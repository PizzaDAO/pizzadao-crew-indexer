# GitHub Copilot / Agent Instructions

Purpose: help an AI coding agent become productive quickly in this repo by summarizing architecture, workflows, conventions, and integration points.

Big picture
- This is a small TypeScript indexer that discovers Google Sheets (from a root spreadsheet and by crawling a site), converts sheet rows into text chunks, computes embeddings, and stores results in Supabase.
- Core data flows: crawl/seed -> `spreadsheets` table -> fetch sheet grid -> `chunkRowsAsText` -> `embed` -> insert into `chunks` table. Related links are stored in `links`.

Key files
- `src/indexer.ts`: main worker loop, webcrawl, claim/mark spreadsheets, ingestion flow, concurrency control.
- `src/google.ts`: creates Google API clients (JWT impersonation) and extracts spreadsheet IDs from grid data.
- `src/chunk.ts`: converts sheet rows into text chunks and A1 ranges.
- `src/embed.ts`: calls OpenAI embeddings API (model controlled by `EMBED_MODEL`).
- `src/seed.ts`: helper to seed the root spreadsheet into Supabase.

Required environment variables (used at runtime)
- `SUPABASE_URL`, `SUPABASE_SERVICE_ROLE_KEY` — Supabase client with service-role privileges.
- `ROOT_SPREADSHEET_ID` — canonical seed spreadsheet id.
- `ROOT_URL` — site to crawl for sheet links (optional; indexer will seed from it if present).
- `GOOGLE_SERVICE_ACCOUNT_JSON` — JSON string of service account credentials.
- `GOOGLE_IMPERSONATE_USER` — email to impersonate (domain-wide delegation required).
- `OPENAI_API_KEY` — for embeddings.
- Optional tuning: `CRAWL_CONCURRENCY`, `SLEEP_MS`, `WEBCRAWL_MAX_PAGES`, `GOOGLE_TIMEOUT_MS`, `EMBED_MODEL`.

Run & developer workflows
- Dev (fast iteration): `npm run dev` (runs `tsx src/indexer.ts`).
- Build: `npm run build` (TypeScript -> `dist/`).
- Start production: `npm run start` (`node dist/indexer.js`).
- Seed the root spreadsheet: `npm run seed` (calls `src/seed.ts`).

Important project-specific conventions
- Structured logging: all logs are JSON one-per-line via `log()` in `src/indexer.ts`. Use these logs when debugging or writing alerts.
- Claim-and-mark pattern: worker claims a `pending` row then updates `crawl_status` to avoid duplicate work (see `getNextPending()` and `mark()`).
- Immutable inserts: `upsert` is used for spreadsheets; never overwrite `crawl_status` blindly — preserve existing status on conflict.
- Concurrency control: `p-limit` with `CRAWL_CONCURRENCY` governs parallel ingestion; respect `SLEEP_MS` between chunk inserts to avoid rate limits.
- Chunking: `chunkRowsAsText` emits chunks with metadata `{gid, headerRowIdx, rowStart, rowEnd}` and A1 ranges; embeddings are computed per-chunk.

Integration points & external dependencies
- Supabase: expects tables `spreadsheets`, `chunks`, `links` (indexer reads/writes specific columns, see `src/indexer.ts`).
- Google APIs: uses JWT auth + impersonation. `getGoogleClients()` throws if credentials or impersonation not provided.
- OpenAI: uses `openai` client; `embed()` returns numeric vector to be stored in `chunks.embedding`.

Debugging notes
- Logs are JSON; use `jq` or your log viewer to filter `event` fields (e.g. `jq 'select(.event=="ingest.error")'`).
- To reproduce locally: set env vars for Supabase and Google creds, run `npm run dev` and watch logs.
- Use `npm run seed` to ensure `ROOT_SPREADSHEET_ID` exists in the DB before starting workers.

Database expectations (discoverable from code)
- `spreadsheets` table: columns referenced: `spreadsheet_id`, `url`, `crawl_status`, `drive_modified_time`, `last_indexed_at`, `first_seen_at`, `title`, `error`.
- `chunks` table: stores `spreadsheet_id`, `sheet_name`, `a1_range`, `text`, `metadata`, `embedding`.
- `links` table: stores relationships `from_spreadsheet_id`, `from_sheet_name`, `to_spreadsheet_id`, etc.

When editing code
- Preserve the logging shape (JSON) and don't replace the `log()` function with console prints.
- If changing chunking or embedding behavior, update `chunkRowsAsText` and `embed()` together; this affects DB schema/consumers.
- Keep Google client creation in `src/google.ts` and maintain JWT + impersonation behavior for domain-wide delegation.

Next steps for agents
- If asked to add features, update this file with any new runtime env vars and DB column references.
- If modifying DB interactions, include SQL migration notes and which code paths write/read affected columns.

If anything in this summary is unclear or you want more detail for a specific area (DB schema, logs, or auth), tell me which area and I will expand.
