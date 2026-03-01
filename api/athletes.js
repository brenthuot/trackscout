// api/athletes.js — Vercel serverless function
// Supports: ?limit=N&offset=N  (paginated)
// Also supports legacy: ?limit=5000 (returns up to 1000, ignores offset)

import { createClient } from "@supabase/supabase-js";

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_KEY
);

export default async function handler(req, res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET,OPTIONS");
  if (req.method === "OPTIONS") return res.status(200).end();

  try {
    const limit  = Math.min(parseInt(req.query.limit  || "1000"), 1000);
    const offset = parseInt(req.query.offset || "0");

    const { data: athletes, error: athErr } = await supabase
      .from("athletes")
      .select("*")
      .eq("source", "tfrrs")
      .range(offset, offset + limit - 1)
      .order("id");

    if (athErr) throw athErr;
    if (!athletes || athletes.length === 0) return res.status(200).json([]);

    const ids = athletes.map(a => a.id);
    const { data: performances, error: perfErr } = await supabase
      .from("performances")
      .select("athlete_id, event, mark, mark_display, year, season, level, meet_name")
      .in("athlete_id", ids);

    if (perfErr) throw perfErr;

    const perfMap = {};
    (performances || []).forEach(p => {
      if (!perfMap[p.athlete_id]) perfMap[p.athlete_id] = [];
      perfMap[p.athlete_id].push(p);
    });

    const result = athletes.map(a => ({
      ...a,
      performances: perfMap[a.id] || [],
    }));

    return res.status(200).json(result);
  } catch (err) {
    console.error("API error:", err);
    return res.status(500).json({ error: err.message });
  }
}
