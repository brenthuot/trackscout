// api/athletes.js
import { createClient } from "@supabase/supabase-js";

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_ANON_KEY
);

export default async function handler(req, res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET,OPTIONS");
  if (req.method === "OPTIONS") return res.status(200).end();

  try {
    const limit  = Math.min(parseInt(req.query.limit  || "1000"), 1000);
    const offset = parseInt(req.query.offset || "0");

    // Single query with embedded performances join — avoids the .in() URL length limit
    const { data: athletes, error } = await supabase
      .from("athletes")
      .select(`
        id, name, source, source_id,
        hometown, hometown_state, high_school,
        hs_grad_year, college, conference,
        college_year, gender, events, tfrrs_url,
        performances (
          athlete_id, event, mark, mark_display,
          year, season, level, meet_name
        )
      `)
      .eq("source", "tfrrs")
      .neq("field_only", true)
      .range(offset, offset + limit - 1)
      .order("id");

    if (error) {
      console.error("Query error:", error);
      return res.status(500).json({ error: error.message });
    }

    return res.status(200).json(athletes || []);

  } catch (err) {
    console.error("Unhandled error:", err);
    return res.status(500).json({ error: err.message });
  }
}
