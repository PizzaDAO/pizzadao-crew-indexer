import { createClient } from "@supabase/supabase-js";
import pLimit from "p-limit";
import { getGoogleClients, extractLinkedSpreadsheetIdsFromGrid } from "./google.js";
import { chunkRowsAsText } from "./chunk.js";
import { embed } from "./embed.js";

const SUPABASE_URL = process.env.SUPABASE_URL!;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY!;
const ROOT_SPREADSHEET_ID = process.env.ROOT_SPREADSHEET_ID!;

const CONCURRENCY = Number(process.env.CRAWL_CONCURRENCY || 3);
const SLEEP_MS = Number(process.env.SLEEP_MS || 500);

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);
const { sheets, drive } = getGoogleClients();

function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

async function upsertSpreadsheet(id: string) {
  const url = `https://docs.google.com/spreadsheets/d/${id}/edit`;
  const { error } = await supabase.from("spreadsheets").upsert(
    { spreadsheet_id: id, url, crawl_status: "pending" },
    { onConflict: "spreadsheet_id" }
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
  return data?.[0]?.spreadsheet_id ?? null;
}

async function mark(id: string, patch: any) {
  const { error } = await supabase
    .from("spreadsheets")
    .update({ ...patch, last_seen_at: new Date().toISOString() })
    .eq("spreadsheet_id", id);
  if (error) throw error;
}

async function driveModifiedTime(spreadsheetId: string) {
  const res = await drive.files.get({
    fileId: spreadsheetId,
    fields: "modifiedTime,name"
  });
  return { modifiedTime: res.data.modifiedTime ?? null, name: res.data.name ?? null };
}

async function ingestOne(spreadsheetId: string) {
  const { modifiedTime, name } = await driveModifiedTime(spreadsheetId);

  const { data: existing, error: exErr } = await supabase
    .from("spreadsheets")
    .select("drive_modified_time")
    .eq("spreadsheet_id", spreadsheetId)
    .maybeSingle();
  if (exErr) throw exErr;

  const unchanged =
    existing?.drive_modified_time &&
    modifiedTime &&
    new Date(modifiedTime).getTime() <= new Date(existing.drive_modified_time).getTime();

  const ss = await sheets.spreadsheets.get({
    spreadsheetId,
    includeGridData: true
  });

  // Discover links
  for (const sh of ss.data.sheets || []) {
    const sheetName = sh.properties?.title || "Sheet";
    const gid = sh.properties?.sheetId ?? null;

    for (const gd of sh.data || []) {
      const ids = extractLinkedSpreadsheetIdsFromGrid(gd);
      for (const id of ids) {
        await upsertSpreadsheet(id);
        // best-effort link record (can add unique constraint later)
        await supabase.from("links").insert({
          from_spreadsheet_id: spreadsheetId,
          from_sheet_name: sheetName,
          from_a1: null,
          to_spreadsheet_id: id
        });
      }
    }

    // If unchanged, skip chunk/embeddings work, but still continue link discovery above
    if (unchanged) continue;

    // Build values matrix from grid data
    const grid = sh.data?.[0];
    const rowData = grid?.rowData || [];

    const values: (string | null)[][] = rowData.map((row: any) => {
      const cells = row?.values || [];
      return cells.map((cell: any) => (cell?.formattedValue ?? null));
    });

    const chunks = chunkRowsAsText({ sheetName, gid, values, maxRowsPerChunk: 25 });

    // Remove old chunks for this tab (simple approach)
    await supabase.from("chunks").delete().eq("spreadsheet_id", spreadsheetId).eq("sheet_name", sheetName);

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
}

async function main() {
  // Ensure root exists (in case seed wasn't run)
  await upsertSpreadsheet(ROOT_SPREADSHEET_ID);

  const limit = pLimit(CONCURRENCY);

  while (true) {
    const ids: string[] = [];
    for (let i = 0; i < CONCURRENCY; i++) {
      const id = await getNextPending();
      if (!id) break;
      // optional: mark in progress by setting pending->pending; later you can add a real in_progress state
      ids.push(id);
    }

    if (ids.length === 0) {
      await sleep(30_000);
      continue;
    }

    await Promise.all(
      ids.map((id) =>
        limit(async () => {
          try {
            await ingestOne(id);
          } catch (e: any) {
            await mark(id, { crawl_status: "error", error: String(e?.message || e) });
          }
        })
      )
    );

    await sleep(2000);
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
