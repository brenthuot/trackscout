// api/athletes.js
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

    // Fetch athlete page
    const { data: athletes, error: athErr } = await supabase
      .from("athletes")
      .select("id,name,source,source_id,hometown,hometown_state,high_school,hs_grad_year,college,conference,college_year,gender,events,tfrrs_url")
      .eq("source", "tfrrs")
      .range(offset, offset + limit - 1)
      .order("id");

    if (athErr) {
      console.error("Athletes query error:", athErr);
      return res.status(500).json({ error: athErr.message });
    }

    if (!athletes || athletes.length === 0) {
      return res.status(200).json([]);
    }

    // Fetch performances for this batch
    const ids = athletes.map(a => a.id);
    const { data: performances, error: perfErr } = await supabase
      .from("performances")
      .select("athlete_id,event,mark,mark_display,year,season,level,meet_name")
      .in("athlete_id", ids);

    if (perfErr) {
      console.error("Performances query error:", perfErr);
      // Return athletes without performances rather than failing entirely
      return res.status(200).json(athletes.map(a => ({ ...a, performances: [] })));
    }

    // Group performances by athlete
    const perfMap = {};
    (performances || []).forEach(p => {
      if (!perfMap[p.athlete_id]) perfMap[p.athlete_id] = [];
      perfMap[p.athlete_id].push(p);
    });

    return res.status(200).json(
      athletes.map(a => ({ ...a, performances: perfMap[a.id] || [] }))
    );

  } catch (err) {
    console.error("Unhandled error:", err);
    return res.status(500).json({ error: err.message });
  }
}
