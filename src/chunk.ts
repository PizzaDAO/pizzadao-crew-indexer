// src/chunk.ts

type Chunk = {
  sheet_name: string;
  a1_range: string;
  text: string;
  metadata: any;
};

type ChunkArgs = {
  sheetName: string;
  gid: number | null;
  values: (string | null)[][];
  maxRowsPerChunk: number;
};

/**
 * Multi-table aware chunking:
 * - Detects table boundaries (blank-row gaps)
 * - Detects header rows heuristically
 * - Applies the correct header row to subsequent data rows
 * - Produces chunks with "Headers:" + "Row:" lines for better retrieval
 */
export function chunkRowsAsText({
  sheetName,
  gid,
  values,
  maxRowsPerChunk
}: ChunkArgs): Chunk[] {
  const chunks: Chunk[] = [];

  // --- Tunables (env optional) ---
  const MIN_HEADER_NONEMPTY = Number(process.env.MIN_HEADER_NONEMPTY || 2);
  const HEADER_STRINGINESS = Number(process.env.HEADER_STRINGINESS || 0.6); // 0..1
  const BLANK_GAP_ROWS = Number(process.env.BLANK_GAP_ROWS || 1); // >=1 blank row => new table
  const MAX_HEADER_COLS = Number(process.env.MAX_HEADER_COLS || 80); // cap header width

  // ---------- Helpers ----------
  const isBlankCell = (v: string | null) => v == null || String(v).trim() === "";

  const rowNonEmptyCount = (row: (string | null)[]) =>
    row.reduce((acc, v) => acc + (isBlankCell(v) ? 0 : 1), 0);

  const normalize = (v: string | null) => (v == null ? "" : String(v).trim());

  const rowToStrings = (row: (string | null)[], maxCols?: number) => {
    const out: string[] = [];
    const limit = Math.min(row.length, maxCols ?? row.length);
    for (let c = 0; c < limit; c++) out.push(normalize(row[c]));
    // Trim trailing empties so headers/rows don’t explode in width
    while (out.length && out[out.length - 1] === "") out.pop();
    return out;
  };

  const isMostlyStrings = (cells: string[]) => {
    const nonEmpty = cells.filter((x) => x !== "");
    if (nonEmpty.length === 0) return false;

    const stringy = nonEmpty.filter((x) => isNaN(Number(x))).length;
    return stringy / nonEmpty.length >= HEADER_STRINGINESS;
  };

  const looksLikeHeader = (row: (string | null)[]) => {
    const cells = rowToStrings(row, MAX_HEADER_COLS);
    const nonEmpty = cells.filter((x) => x !== "");
    if (nonEmpty.length < MIN_HEADER_NONEMPTY) return false;

    // If it’s mostly numbers, not a header
    if (!isMostlyStrings(cells)) return false;

    // Common header hints: short-ish tokens, no huge paragraphs
    const avgLen =
      nonEmpty.reduce((sum, x) => sum + x.length, 0) / Math.max(nonEmpty.length, 1);
    if (avgLen > 80) return false;

    return true;
  };

  const colToA1 = (colIdx0: number) => {
    let n = colIdx0 + 1;
    let s = "";
    while (n > 0) {
      const r = (n - 1) % 26;
      s = String.fromCharCode(65 + r) + s;
      n = Math.floor((n - 1) / 26);
    }
    return s;
  };

  const rangeA1 = (r0: number, r1: number, c0: number, c1: number) => {
    // r0/r1 inclusive, 0-based
    const start = `${colToA1(c0)}${r0 + 1}`;
    const end = `${colToA1(c1)}${r1 + 1}`;
    return `${start}:${end}`;
  };

  const rowHasRealData = (row: (string | null)[]) => rowNonEmptyCount(row) > 0;

  const countLeadingBlankRowsFrom = (startIdx: number) => {
    let k = 0;
    for (let i = startIdx; i < values.length; i++) {
      if (rowHasRealData(values[i])) break;
      k++;
    }
    return k;
  };

  // ---------- Main scan ----------
  let activeHeaders: string[] | null = null;
  let activeHeaderRowIndex: number | null = null;
  let tableIndex = 0;

  // Current chunk buffer (for a single table)
  let bufRows: { rowIndex: number; row: (string | null)[] }[] = [];

  const flush = () => {
    if (bufRows.length === 0) return;

    // Determine width based on buffered rows + headers (for A1)
    const maxColFromRows = bufRows.reduce((mx, r) => {
      const cells = rowToStrings(r.row);
      return Math.max(mx, Math.max(0, cells.length - 1));
    }, 0);

    const maxColFromHeaders =
      activeHeaders && activeHeaders.length ? activeHeaders.length - 1 : 0;

    const maxCol = Math.max(maxColFromRows, maxColFromHeaders);

    // Build chunk text with headers + rows
    const headersLine =
      activeHeaders && activeHeaders.length
        ? `Headers: ${activeHeaders.join(" | ")}`
        : "Headers: (none)";

    const lines: string[] = [headersLine];

    for (const r of bufRows) {
      const cells = rowToStrings(r.row);
      // Skip totally empty (shouldn’t happen, but safe)
      if (cells.every((x) => x === "")) continue;

      lines.push(`Row ${r.rowIndex + 1}: ${cells.join(" | ")}`);
    }

    const startRow = bufRows[0].rowIndex;
    const endRow = bufRows[bufRows.length - 1].rowIndex;

    const a1 = rangeA1(startRow, endRow, 0, Math.max(maxCol, 0));

    chunks.push({
      sheet_name: sheetName,
      a1_range: a1,
      text: lines.join("\n"),
      metadata: {
        gid,
        sheetName,
        table_index: tableIndex,
        header_row: activeHeaderRowIndex != null ? activeHeaderRowIndex + 1 : null,
        headers: activeHeaders ?? null
      }
    });

    bufRows = [];
  };

  const pushRowToBuf = (rowIndex: number, row: (string | null)[]) => {
    bufRows.push({ rowIndex, row });
    if (bufRows.length >= maxRowsPerChunk) flush();
  };

  const startNewTable = () => {
    flush();
    tableIndex++;
    activeHeaders = null;
    activeHeaderRowIndex = null;
  };

  for (let i = 0; i < values.length; i++) {
    const row = values[i];

    // If current row is blank, we may be at a table boundary
    if (!rowHasRealData(row)) {
      // If we have enough blank gap rows, treat as boundary
      const blanks = countLeadingBlankRowsFrom(i);
      if (blanks >= BLANK_GAP_ROWS) {
        // boundary: flush current chunk, reset headers for next table
        // Advance i to just before the first non-blank row (for-loop will i++)
        startNewTable();
        i = i + blanks - 1;
      }
      continue;
    }

    // If we don't have headers yet, attempt to detect header row
    if (!activeHeaders) {
      if (looksLikeHeader(row)) {
        activeHeaders = rowToStrings(row, MAX_HEADER_COLS).filter((x) => x !== "");
        activeHeaderRowIndex = i;

        // After detecting header row, do NOT treat it as data row.
        // Instead, continue to next row(s) as data.
        continue;
      } else {
        // No obvious header: we still want chunks, but mark headers as null
        activeHeaders = null;
        activeHeaderRowIndex = null;
        // Treat as data row
        pushRowToBuf(i, row);
        continue;
      }
    }

    // We already have headers for this table.
    // But tabs sometimes have another header deeper down; detect and switch.
    if (looksLikeHeader(row)) {
      // Heuristic: if it looks like a header AND we currently have buffered data,
      // start a new table, set new headers, and continue.
      if (bufRows.length > 0) flush();
      // New table within same tab (no blank rows)
      tableIndex++;
      activeHeaders = rowToStrings(row, MAX_HEADER_COLS).filter((x) => x !== "");
      activeHeaderRowIndex = i;
      continue;
    }

    // Normal data row
    pushRowToBuf(i, row);
  }

  flush();
  return chunks;
}
