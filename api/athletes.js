import { createClient } from '@supabase/supabase-js';

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_ANON_KEY
);

export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');

  const { event, state, college, conference, hs_year, college_year, gender, limit = 500 } = req.query;

  let query = supabase
    .from('athletes')
    .select(`
      id, name, source, college, conference, hometown, hometown_state,
      hs_grad_year, college_year, gender, events, tfrrs_url,
      performances (event, mark, mark_display, year, season, level, meet_name)
    `)
    .limit(parseInt(limit));

  if (event)       query = query.contains('events', [event]);
  if (state)       query = query.eq('hometown_state', state);
  if (college)     query = query.eq('college', college);
  if (conference)  query = query.eq('conference', conference);
  if (hs_year)     query = query.eq('hs_grad_year', parseInt(hs_year));
  if (college_year) query = query.eq('college_year', parseInt(college_year));
  if (gender)      query = query.eq('gender', gender);

  const { data, error } = await query;

  if (error) return res.status(500).json({ error: error.message });
  res.status(200).json(data || []);
}
