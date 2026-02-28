export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');

  const { query, state, event, gender = 'M' } = req.query;
  const API_KEY = process.env.ATHLETIC_NET_API_KEY;

  if (!API_KEY) return res.status(500).json({ error: 'API key not configured' });

  try {
    // Athletic.net athlete search endpoint
    const url = new URL('https://www.athletic.net/api/v1/AthleticNet/Search');
    if (query) url.searchParams.set('q', query);
    if (state) url.searchParams.set('state', state);
    if (event) url.searchParams.set('event', event);
    url.searchParams.set('gender', gender);
    url.searchParams.set('type', 'athlete');

    const response = await fetch(url.toString(), {
      headers: {
        'AthleticNet-API-Key': API_KEY,
        'Content-Type': 'application/json'
      }
    });

    if (!response.ok) {
      return res.status(response.status).json({ error: 'Athletic.net API error' });
    }

    const data = await response.json();
    res.status(200).json(data);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
}
