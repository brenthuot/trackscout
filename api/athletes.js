// api/athletes.js — Vercel serverless function
// Fetches athletes + their performances from Supabase
// Supports: ?limit=1000&offset=0

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
    const limit  = Math.min(parseInt(req.query.limit  || "1000"), 1000); // cap at 1000 per page
    const offset = parseInt(req.query.offset || "0");

    // Fetch athletes page
    const { data: athletes, error: athErr } = await supabase
      .from("athletes")
      .select("*")
      .eq("source", "tfrrs")
      .range(offset, offset + limit - 1)
      .order("id");

    if (athErr) throw athErr;
    if (!athletes || athletes.length === 0) return res.status(200).json([]);

    // Fetch performances for this batch of athletes
    const ids = athletes.map(a => a.id);
    const { data: performances, error: perfErr } = await supabase
      .from("performances")
      .select("athlete_id, event, mark, mark_display, year, season, level, meet_name")
      .in("athlete_id", ids);

    if (perfErr) throw perfErr;

    // Group performances by athlete_id
    const perfMap = {};
    (performances || []).forEach(p => {
      if (!perfMap[p.athlete_id]) perfMap[p.athlete_id] = [];
      perfMap[p.athlete_id].push(p);
    });

    // Attach performances to each athlete
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
