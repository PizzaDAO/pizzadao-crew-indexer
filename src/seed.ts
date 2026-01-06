import { createClient } from "@supabase/supabase-js";

const SUPABASE_URL = process.env.SUPABASE_URL!;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY!;
const ROOT_SPREADSHEET_ID = process.env.ROOT_SPREADSHEET_ID!;

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

async function main() {
  const url = `https://docs.google.com/spreadsheets/d/${ROOT_SPREADSHEET_ID}/edit`;

  const { error } = await supabase.from("spreadsheets").upsert(
    {
      spreadsheet_id: ROOT_SPREADSHEET_ID,
      url,
      crawl_status: "pending"
    },
    { onConflict: "spreadsheet_id" }
  );

  if (error) throw error;

  console.log("Seeded root spreadsheet:", ROOT_SPREADSHEET_ID);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
