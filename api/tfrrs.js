import { createClient } from '@supabase/supabase-js';

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_ANON_KEY
);

export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');

  const { athlete_id, college, year } = req.query;

  if (!athlete_id && !college) {
    return res.status(400).json({ error: 'Provide athlete_id or college' });
  }

  try {
    // Build TFRRS URL
    const baseUrl = athlete_id
      ? `https://www.tfrrs.org/athletes/${athlete_id}`
      : `https://www.tfrrs.org/teams/college/${year || 'current'}/${college}.html`;

    const response = await fetch(baseUrl, {
      headers: { 'User-Agent': 'Mozilla/5.0 (compatible; RunStats/1.0)' }
    });

    if (!response.ok) return res.status(404).json({ error: 'TFRRS page not found' });

    const html = await response.text();

    // Parse athlete name
    const nameMatch = html.match(/<h3[^>]*class="[^"]*athlete-name[^"]*"[^>]*>([^<]+)<\/h3>/);
    const name = nameMatch ? nameMatch[1].trim() : 'Unknown';

    // Parse PR table rows — TFRRS uses a standard results table
    const rows = [...html.matchAll(/<tr[^>]*>([\s\S]*?)<\/tr>/g)];
    const performances = [];

    rows.forEach(row => {
      const cells = [...row[1].matchAll(/<td[^>]*>([\s\S]*?)<\/td>/g)].map(c =>
        c[1].replace(/<[^>]+>/g, '').trim()
      );
      if (cells.length >= 2 && cells[0] && cells[1]) {
        performances.push({ event: cells[0], mark: cells[1], year: cells[2] || '' });
      }
    });

    // Cache to Supabase if we got good data
    if (athlete_id && name !== 'Unknown') {
      await supabase.from('athletes').upsert({
        id: `tfrrs_${athlete_id}`,
        name,
        source: 'tfrrs',
        source_id: athlete_id,
        updated_at: new Date().toISOString()
      }, { onConflict: 'id' });
    }

    res.status(200).json({ name, performances, source: 'tfrrs', url: baseUrl });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
}
