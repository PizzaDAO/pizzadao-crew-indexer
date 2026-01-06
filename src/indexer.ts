import { createClient } from "@supabase/supabase-js";
import pLimit from "p-limit";
import { getGoogleClients, extractLinkedSpreadsheetIdsFromGrid } from "./google.js";
import { chunkRowsAsText } from "./chunk.js";
import { embed } from "./embed.js";

/**
 * Simple structured logger (Railway-friendly)
 */
function log(event: string, data: Record<string, any> = {}) {
  // Keep it one-line JSON for easier scanning/filtering in Railway
  console.log(
    JSON.stringify({
      ts: new Date().toISOString(),
      event,
      ...data
    })
  );
}

const SUPABASE_URL = process.env.SUPABASE_URL!;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY!;
const ROOT_SPREADSHEET_ID = process.env.ROOT_SPREADSHEET_ID!;

const CONCURRENCY = Number(process.env.CRAWL_CONCURRENCY || 3);
const SLEEP_MS = Number(process.env.SLEEP_MS || 500);
const GOOGLE_TIMEOUT_MS = Number(process.env.GOOGLE_TIMEOUT_MS || 30_000);

// BOOT BANNER (proves the container is running *your* code)
log("boot", {
  node: process.version,
  concurrency: CONCURRENCY,
  sleepMs: SLEEP_MS,
  googleTimeoutMs: GOOGLE_TIMEOUT_MS,
  rootSpreadsheetId: ROOT_SPREADSHEET_ID,
  impersonate: process.env.GOOGLE_IMPERSONATE_USER ?? null
});

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);
const { sheets, drive } = getGoogleClients();

function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

async function upsertSpreadsheet(id: string) {
  const url = `https://docs.google.com/spreadsheets/d/${id}/edit`;

  // Only insert if new; never overwrite crawl_status for existing rows.
  const { error } = await supabase
    .from("spreadsheets")
    .upsert(
      { spreadsheet_id: id, url },
      { onConflict: "spreadsheet_id", ignoreDuplicates: true }
    );

  if (error) throw error;
}


async function getNextPending() {
  const { data, error } = await supabase
    .from("spreadsheets")
    .select("spreadsheet_id")
    .eq("crawl_status", "pending")
    .order("first_seen_at", { ascending: true })
    .limit(1);

  if (error) throw error;

  const id = data?.[0]?.spreadsheet_id ?? null;
  if (!id) return null;

  // Claim it so other loops don't keep grabbing the same one
  await mark(id, { crawl_status: "in_progress", error: null });

  return id;
}


async function mark(id: string, patch: any) {
  const { error } = await supabase
    .from("spreadsheets")
    .update({ ...patch, last_seen_at: new Date().toISOString() })
    .eq("spreadsheet_id", id);
  if (error) throw error;
}

async function driveModifiedTime(spreadsheetId: string) {
  log("drive.get.start", { spreadsheetId });
  const res = await drive.files.get(
    {
      fileId: spreadsheetId,
      fields: "modifiedTime,name"
    },
    { timeout: GOOGLE_TIMEOUT_MS }
  );
  log("drive.get.done", {
    spreadsheetId,
    modifiedTime: res.data.modifiedTime ?? null,
    name: res.data.name ?? null
  });
  return { modifiedTime: res.data.modifiedTime ?? null, name: res.data.name ?? null };
}

async function ingestOne(spreadsheetId: string) {
  const t0 = Date.now();
  log("ingest.start", { spreadsheetId });

  try {
    const { modifiedTime, name } = await driveModifiedTime(spreadsheetId);

    const { data: existing, error: exErr } = await supabase
      .from("spreadsheets")
      .select("drive_modified_time,last_indexed_at")
      .eq("spreadsheet_id", spreadsheetId)
      .maybeSingle();
    if (exErr) throw exErr;

    const unchanged =
      !!existing?.last_indexed_at && // only skip if we've indexed before
      existing?.drive_modified_time &&
      modifiedTime &&
      new Date(modifiedTime).getTime() <= new Date(existing.drive_modified_time).getTime();

    log("sheets.get.start", { spreadsheetId, includeGridData: true, unchanged });
    const ss = await sheets.spreadsheets.get(
      {
        spreadsheetId,
        includeGridData: true
      },
      { timeout: GOOGLE_TIMEOUT_MS }
    );
    log("sheets.get.done", {
      spreadsheetId,
      sheetCount: ss.data.sheets?.length ?? 0,
      title: ss.data.properties?.title ?? null
    });

    let discoveredLinks = 0;
    let insertedChunks = 0;

    // Discover links + optionally ingest content
    for (const sh of ss.data.sheets || []) {
      const sheetName = sh.properties?.title || "Sheet";
      const gid = sh.properties?.sheetId ?? null;

      // Link discovery from all grid-data blocks
      for (const gd of sh.data || []) {
        const ids = extractLinkedSpreadsheetIdsFromGrid(gd);
        if (ids.length) log("links.found", { spreadsheetId, sheetName, count: ids.length });

        for (const id of ids) {
          discoveredLinks++;
          await upsertSpreadsheet(id);

          // best-effort link record (can add unique constraint later)
          const { error: linkErr } = await supabase.from("links").insert({
            from_spreadsheet_id: spreadsheetId,
            from_sheet_name: sheetName,
            from_a1: null,
            to_spreadsheet_id: id
          });
          if (linkErr) {
            // Don't fail the whole ingest on duplicate inserts etc.
            log("links.insert.warn", {
              spreadsheetId,
              toSpreadsheetId: id,
              message: linkErr.message
            });
          }
        }
      }

      if (unchanged) continue;

      // Build values matrix from grid data
      const grid = sh.data?.[0];
      const rowData = grid?.rowData || [];

      log("tab.values", { spreadsheetId, sheetName, rows: rowData.length });

      const values: (string | null)[][] = rowData.map((row: any) => {
        const cells = row?.values || [];
        return cells.map((cell: any) => cell?.formattedValue ?? null);
      });

      const chunks = chunkRowsAsText({ sheetName, gid, values, maxRowsPerChunk: 25 });
      log("tab.chunks", { spreadsheetId, sheetName, chunkCount: chunks.length });

      // Remove old chunks for this tab (simple approach)
      await supabase
        .from("chunks")
        .delete()
        .eq("spreadsheet_id", spreadsheetId)
        .eq("sheet_name", sheetName);

      for (const ch of chunks) {
        const embedding = await embed(ch.text);
        const { error } = await supabase.from("chunks").insert({
          spreadsheet_id: spreadsheetId,
          sheet_name: ch.sheet_name,
          a1_range: ch.a1_range,
          text: ch.text,
          metadata: ch.metadata,
          embedding
        });
        if (error) throw error;

        insertedChunks++;
        // Avoid log spam: log every 10 chunks
        if (insertedChunks % 10 === 0) {
          log("chunks.insert.progress", { spreadsheetId, insertedChunks });
        }

        await sleep(SLEEP_MS);
      }
    }

    await mark(spreadsheetId, {
      title: name ?? ss.data.properties?.title ?? null,
      drive_modified_time: modifiedTime,
      last_indexed_at: unchanged ? undefined : new Date().toISOString(),
      crawl_status: unchanged ? "skipped" : "indexed",
      error: null
    });

    log("ingest.done", {
      spreadsheetId,
      status: unchanged ? "skipped" : "indexed",
      discoveredLinks,
      insertedChunks,
      ms: Date.now() - t0
    });
  } catch (e: any) {
    const msg = String(e?.message || e);
    log("ingest.error", { spreadsheetId, message: msg });

    // Ensure we never leave it stuck in pending with no error
    try {
      await mark(spreadsheetId, { crawl_status: "error", error: msg });
    } catch (markErr: any) {
      log("mark.error", { spreadsheetId, message: String(markErr?.message || markErr) });
    }
  }
}

async function main() {
  log("main.start");

  // Ensure root exists (in case seed wasn't run)
  await upsertSpreadsheet(ROOT_SPREADSHEET_ID);
  log("root.upserted", { root: ROOT_SPREADSHEET_ID });

  // Heartbeat so logs are never “empty”
  setInterval(() => {
    log("heartbeat");
  }, 30_000);

  const limit = pLimit(CONCURRENCY);

  while (true) {
    const ids: string[] = [];
    for (let i = 0; i < CONCURRENCY; i++) {
      const id = await getNextPending();
      if (!id) break;
      ids.push(id);
    }

    if (ids.length === 0) {
      log("queue.empty.sleep", { ms: 30_000 });
      await sleep(30_000);
      continue;
    }

    log("queue.batch", { count: ids.length, ids });

    await Promise.all(ids.map((id) => limit(() => ingestOne(id))));

    await sleep(2000);
  }
}

main().catch((e) => {
  log("fatal", { message: String((e as any)?.message || e) });
  process.exit(1);
});
