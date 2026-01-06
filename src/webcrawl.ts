// src/webcrawl.ts
const SHEETS_ID_RE =
  /https?:\/\/docs\.google\.com\/spreadsheets\/d\/([a-zA-Z0-9-_]+)/g;

function extractSheetIds(html: string): string[] {
  const ids = new Set<string>();
  let m: RegExpExecArray | null;
  while ((m = SHEETS_ID_RE.exec(html)) !== null) ids.add(m[1]);
  return [...ids];
}

function extractLinks(html: string, baseUrl: string): string[] {
  const urls = new Set<string>();
  // simple href capture (good enough for your site)
  const re = /href=["']([^"']+)["']/g;
  let m: RegExpExecArray | null;

  while ((m = re.exec(html)) !== null) {
    try {
      const u = new URL(m[1], baseUrl);
      urls.add(u.toString());
    } catch {}
  }
  return [...urls];
}

export async function crawlCrewSiteForSheetIds(opts: {
  rootUrl: string;
  maxPages?: number;
  sameOriginOnly?: boolean;
}): Promise<{ sheetIds: string[]; pagesCrawled: number }> {
  const rootUrl = opts.rootUrl.replace(/\/$/, "");
  const maxPages = opts.maxPages ?? 300;
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
      if (!res.ok) continue;
      const ct = res.headers.get("content-type") || "";
      if (!ct.includes("text/html")) continue;
      html = await res.text();
    } catch {
      continue;
    }

    for (const id of extractSheetIds(html)) sheetIds.add(id);

    const links = extractLinks(html, url);
    for (const link of links) {
      try {
        const u = new URL(link);
        if (sameOriginOnly && u.origin !== origin) continue;
        // avoid obvious non-pages
        if (/\.(png|jpg|jpeg|gif|svg|css|js|ico|pdf)$/i.test(u.pathname)) continue;
        queue.push(u.toString());
      } catch {}
    }
  }

  return { sheetIds: [...sheetIds], pagesCrawled: seen.size };
}
