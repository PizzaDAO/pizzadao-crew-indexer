export function chunkRowsAsText(args: {
  sheetName: string;
  gid: number | null;
  values: (string | null)[][];
  maxRowsPerChunk?: number;
}) {
  const { sheetName, gid, values } = args;
  const maxRows = args.maxRowsPerChunk ?? 25;

  // header = first non-empty row
  let headerRowIdx = -1;
  for (let i = 0; i < values.length; i++) {
    if ((values[i] || []).some((v) => (v ?? "").toString().trim() !== "")) {
      headerRowIdx = i;
      break;
    }
  }

  const header = headerRowIdx >= 0 ? values[headerRowIdx] : [];
  const headerText = header.map((h) => (h ?? "").toString().trim()).join(" | ");

  const chunks: {
    sheet_name: string;
    a1_range: string;
    text: string;
    metadata: any;
  }[] = [];

  let start = Math.max(headerRowIdx + 1, 0);

  while (start < values.length) {
    const end = Math.min(start + maxRows, values.length);
    const rows = values.slice(start, end);

    const hasContent = rows.some((row) =>
      row.some((v) => (v ?? "").toString().trim() !== "")
    );
    if (!hasContent) {
      start = end;
      continue;
    }

    const body = rows
      .map((row, idx) => {
        const rowNum = start + idx + 1; // 1-indexed display
        const rowText = row.map((v) => (v ?? "").toString().trim()).join(" | ");
        return `Row ${rowNum}: ${rowText}`;
      })
      .join("\n");

    const text =
      `Tab: ${sheetName}\n` +
      (headerText ? `Headers: ${headerText}\n` : "") +
      body;

    const a1_range = `${sheetName}!R${start + 1}:R${end}`;

    chunks.push({
      sheet_name: sheetName,
      a1_range,
      text,
      metadata: { gid, headerRowIdx, rowStart: start + 1, rowEnd: end }
    });

    start = end;
  }

  return chunks;
}
