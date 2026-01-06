import { google } from "googleapis";

export function getGoogleClients() {
  const raw = process.env.GOOGLE_SERVICE_ACCOUNT_JSON;
  if (!raw) throw new Error("Missing GOOGLE_SERVICE_ACCOUNT_JSON");

  const creds = JSON.parse(raw);

  // Required for domain-wide delegation (service account impersonates this user)
  const subject = process.env.GOOGLE_IMPERSONATE_USER;
  if (!subject) {
    throw new Error(
      "Missing GOOGLE_IMPERSONATE_USER (required for domain-wide delegation). Example: hello@rarepizzas.com"
    );
  }

  const auth = new google.auth.JWT({
    clientEmail: creds.client_email,
    privateKey: creds.private_key,
    scopes: [
      "https://www.googleapis.com/auth/spreadsheets.readonly",
      "https://www.googleapis.com/auth/drive.readonly"
    ],
    subject
  });

  const sheets = google.sheets({ version: "v4", auth });
  const drive = google.drive({ version: "v3", auth });

  return { sheets, drive };
}

export function extractSpreadsheetIdsFromText(text: string): string[] {
  const ids = new Set<string>();
  const re = /https?:\/\/docs\.google\.com\/spreadsheets\/d\/([a-zA-Z0-9-_]+)/g;
  let m: RegExpExecArray | null;
  while ((m = re.exec(text)) !== null) ids.add(m[1]);
  return [...ids];
}

export function extractLinkedSpreadsheetIdsFromGrid(gridData: any): string[] {
  const ids = new Set<string>();

  const push = (v: unknown) => {
    if (typeof v !== "string" || !v) return;
    for (const id of extractSpreadsheetIdsFromText(v)) ids.add(id);
  };

  if (!gridData?.rowData) return [];

  for (const row of gridData.rowData) {
    for (const cell of row?.values || []) {
      push(cell?.formattedValue);

      const uev = cell?.userEnteredValue;
      push(uev?.stringValue);
      push(uev?.formulaValue);

      const tfr = cell?.textFormatRuns;
      if (Array.isArray(tfr)) {
        for (const run of tfr) {
          push(run?.format?.link?.uri);
        }
      }
    }
  }

  return [...ids];
}
