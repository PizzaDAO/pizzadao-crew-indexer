// src/indexer.ts
import { createClient } from "@supabase/supabase-js";
import pLimit from "p-limit";
import { getGoogleClients, extractLinkedSpreadsheetIdsFromGrid } from "./google.js";
import { chunkRowsAsText } from "./chunk.js";
import { embed } from "./embed.js";

/**
 * Simple structured logger (Railway-friendly)
 */
function log(event: string, data: Record<string, any> = {}) {
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

// ✅ NEW: seed discovery from the crew website
const ROOT_URL = process.env.ROOT_URL || "https://crew.pizzadao.xyz";
const WEBCRAWL_MAX_PAGES = Number(process.env.WEBCRAWL_MAX_PAGES || 500);

const CONCURRENCY = Number(process.env.CRAWL_CONCURRENCY || 3);
const SLEEP_MS = Number(process.env.SLEEP_MS || 500);
const GOOGLE_TIMEOUT_MS = Number(process.env.GOOGLE_TIMEOUT_MS || 30_000);

log("boot", {
  node: process.version,
  concurrency: CONCURRENCY,
  sleepMs: SLEEP_MS,
  googleTimeoutMs: GOOGLE_TIMEOUT_MS,
  rootSpreadsheetId: ROOT_SPREADSHEET_ID,
  rootUrl: ROOT_URL,
  webcrawlMaxPages: WEBCRAWL_MAX_PAGES,
  impersonate: process.env.GOOGLE_IMPERSONATE_USER ?? null
});

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);
const { sheets, drive } = getGoogleClients();

function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

// Google Sheets ID extraction
const SHEETS_ID_RE =
  /https?:\/\/docs\.google\.com\/spreadsheets\/d\/([a-zA-Z0-9-_]+)/g;

function extractSheetIdsFromText(text: string): string[] {
  const ids = new Set<string>();
  let m: RegExpExecArray | null;
  while ((m = SHEETS_ID_RE.exec(text)) !== null) ids.add(m[1]);
  return [...ids];
}

// Extremely simple href extraction (good enough for typical Next/static sites)
function extractLinksFromHtml(html: string, baseUrl: string): string[] {
  const urls = new Set<string>();
  const re = /href=["']([^"']+)["']/g;
  let m: RegExpExecArray | null;
  while ((m = re.exec(html)) !== null) {
    try {
      const u = new URL(m[1], baseUrl);
      urls.add(u.toString());
    } catch {
      // ignore invalid
    }
  }
  return [...urls];
}

async function crawlRootSiteForSheetIds(opts: {
  rootUrl: string;
  maxPages: number;
  sameOriginOnly?: boolean;
}): Promise<{ sheetIds: string[]; pagesCrawled: number }> {
  const rootUrl = opts.rootUrl.replace(/\/$/, "");
  const maxPages = opts.maxPages;
  const sameOriginOnly = opts.sameOriginOnly ?? true;

  const origin = new URL(rootUrl).origin;

  const queue: string[] = [rootUrl];
  const seen = new Set<string>();
  const sheetIds = new Set<string>();

  while (queue.length && seen.size < maxPages) {
    const url = queue.shift()!;
    if (seen.has(url)) continue;
    seen.add(url);

    let html = "";
    try {
      const res = await fetch(url, { redirect: "follow" });
      if (!res.ok) {
        log("webcrawl.fetch.skip", { url, status: res.status });
        continue;
      }

      const ct = res.headers.get("content-type") || "";
      if (!ct.includes("text/html")) {
        log("webcrawl.fetch.not_html", { url, contentType: ct });
        continue;
      }

      html = await res.text();
    } catch (e: any) {
      log("webcrawl.fetch.error", { url, message: String(e?.message || e) });
      continue;
    }

    // Extract sheet IDs from anywhere in HTML
    for (const id of extractSheetIdsFromText(html)) sheetIds.add(id);

    // Enqueue more same-origin links
    const links = extractLinksFromHtml(html, url);
    for (const link of links) {
      try {
        const u = new URL(link);
        if (sameOriginOnly && u.origin !== origin) continue;
        if (/\.(png|jpg|jpeg|gif|svg|css|js|ico|pdf|zip)$/i.test(u.pathname)) continue;
        // drop hash to reduce duplicates
        u.hash = "";
        queue.push(u.toString());
      } catch {
        // ignore
      }
    }

    if (seen.size % 25 === 0) {
      log("webcrawl.progress", {
        pagesCrawled: seen.size,
        queued: queue.length,
        discoveredSheets: sheetIds.size
      });
    }
  }

  return { sheetIds: [...sheetIds], pagesCrawled: seen.size };
}

async function upsertSpreadsheet(id: string) {
  const url = `https://docs.google.com/spreadsheets/d/${id}/edit`;

  // Only insert if new; never overwrite crawl_status for existing rows.
  const { error } = await supabase
    .from("spreadsheets")
    .upsert({ spreadsheet_id: id, url }, { onConflict: "spreadsheet_id", ignoreDuplicates: true });

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

  // Safety caps to avoid OOM (tune via env if needed)
  const MAX_GRID_ROWS = Number(process.env.MAX_GRID_ROWS || 600); // per tab
  const MAX_GRID_COLS = Number(process.env.MAX_GRID_COLS || 80);  // per row (A..CB)

  try {
    // 0) Drive metadata (cheap)
    const { modifiedTime, name } = await driveModifiedTime(spreadsheetId);

    // 1) Check if we've indexed before + unchanged
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

    // 2) Fetch spreadsheet metadata ONLY (no grid data) — this is the big memory saver
    log("sheets.meta.start", { spreadsheetId });
    const meta = await sheets.spreadsheets.get(
      {
        spreadsheetId,
        includeGridData: false,
        fields: "properties(title),sheets(properties(sheetId,title))"
      },
      { timeout: GOOGLE_TIMEOUT_MS }
    );
    const sheetMetas = meta.data.sheets || [];
    log("sheets.meta.done", {
      spreadsheetId,
      sheetCount: sheetMetas.length,
      title: meta.data.properties?.title ?? null,
      unchanged
    });

    let discoveredLinks = 0;
    let insertedChunks = 0;

    // 3) Process each tab one-by-one (prevents loading entire spreadsheet into memory)
    for (const shMeta of sheetMetas) {
      const sheetName = shMeta.properties?.title || "Sheet";
      const gid = shMeta.properties?.sheetId ?? null;

      // Fetch grid data for this single tab, but restrict fields to the minimum we need.
      // Still potentially big, so we cap how much we *use* below.
      log("tab.grid.start", { spreadsheetId, sheetName });
      const oneTab = await sheets.spreadsheets.get(
        {
          spreadsheetId,
          ranges: [`'${sheetName}'`],
          includeGridData: true,
          fields:
            "sheets(properties(sheetId,title),data(rowData(values(formattedValue,userEnteredValue,textFormatRuns(format(link(uri)))))))"
        },
        { timeout: GOOGLE_TIMEOUT_MS }
      );
      log("tab.grid.done", { spreadsheetId, sheetName });

      const tab = oneTab.data.sheets?.[0];
      const gd = tab?.data?.[0];
      const rowDataAll = gd?.rowData || [];

      // ---- Link discovery (bounded) ----
      // We only scan up to MAX_GRID_ROWS x MAX_GRID_COLS to keep memory bounded.
      // (If you discover links far below, bump MAX_GRID_ROWS.)
      const boundedGridForLinks = {
        rowData: rowDataAll.slice(0, MAX_GRID_ROWS).map((r: any) => ({
          ...r,
          values: Array.isArray(r?.values) ? r.values.slice(0, MAX_GRID_COLS) : r?.values
        }))
      };

      const ids = extractLinkedSpreadsheetIdsFromGrid(boundedGridForLinks);
      if (ids.length) log("links.found", { spreadsheetId, sheetName, count: ids.length });

      for (const id of ids) {
        discoveredLinks++;
        await upsertSpreadsheet(id);

        const { error: linkErr } = await supabase.from("links").insert({
          from_spreadsheet_id: spreadsheetId,
          from_sheet_name: sheetName,
          from_a1: null,
          to_spreadsheet_id: id
        });

        if (linkErr) {
          // Don't fail the whole ingest on duplicates etc.
          log("links.insert.warn", {
            spreadsheetId,
            toSpreadsheetId: id,
            message: linkErr.message
          });
        }
      }

      // If unchanged, we still do link discovery, but we skip chunking/embeddings
      if (unchanged) continue;

      // ---- Content ingest (bounded) ----
      const rowData = rowDataAll.slice(0, MAX_GRID_ROWS);

      log("tab.values", { spreadsheetId, sheetName, rows: rowData.length, maxCols: MAX_GRID_COLS });

      const values: (string | null)[][] = rowData.map((row: any) => {
        const cells = row?.values || [];
        return cells.slice(0, MAX_GRID_COLS).map((cell: any) => cell?.formattedValue ?? null);
      });

      const chunks = chunkRowsAsText({ sheetName, gid, values, maxRowsPerChunk: 25 });
      log("tab.chunks", { spreadsheetId, sheetName, chunkCount: chunks.length });

      // Remove old chunks for this tab
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

        insertedChunks++;
        if (insertedChunks % 10 === 0) {
          log("chunks.insert.progress", { spreadsheetId, insertedChunks });
        }

        await sleep(SLEEP_MS);
      }
    }

    // 4) Mark spreadsheet state
    await mark(spreadsheetId, {
      title: name ?? meta.data.properties?.title ?? null,
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

    try {
      await mark(spreadsheetId, { crawl_status: "error", error: msg });
    } catch (markErr: any) {
      log("mark.error", { spreadsheetId, message: String(markErr?.message || markErr) });
    }
  }
}


async function main() {
  log("main.start");

  // ✅ NEW: seed from the crew website first (this is how we discover 100s of sheets)
  if (ROOT_URL) {
    log("seed.webcrawl.start", { rootUrl: ROOT_URL, maxPages: WEBCRAWL_MAX_PAGES });
    const { sheetIds, pagesCrawled } = await crawlRootSiteForSheetIds({
      rootUrl: ROOT_URL,
      maxPages: WEBCRAWL_MAX_PAGES
    });

    log("seed.webcrawl.done", {
      pagesCrawled,
      discoveredSheets: sheetIds.length
    });

    for (const id of sheetIds) {
      await upsertSpreadsheet(id);
    }
    log("seed.webcrawl.upserted", { count: sheetIds.length });
  } else {
    log("seed.webcrawl.skipped", { reason: "ROOT_URL not set" });
  }

  // Keep spreadsheet root too (can discover deeper links inside sheets)
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
