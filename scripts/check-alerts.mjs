import fs from 'node:fs/promises';
import path from 'node:path';

const ANALYSIS_PATH = path.resolve('data/analysis.json');
const STATE_PATH = path.resolve('data/alert-state.json');
const PROXIMITY_THRESHOLD = 0.001; // 0.1%
const COOLDOWN_MS = 30 * 60 * 1000; // 30 minutes

async function readJsonSafe(filePath, fallback) {
  try {
    const raw = await fs.readFile(filePath, 'utf8');
    return JSON.parse(raw);
  } catch {
    return fallback;
  }
}

async function main() {
  const topic = process.env.NTFY_TOPIC;
  if (!topic) {
    console.log('NTFY_TOPIC secret is not set; skipping alert check.');
    return;
  }

  const analysis = await readJsonSafe(ANALYSIS_PATH, null);
  if (!analysis || !analysis.spot) {
    console.log('No valid data/analysis.json found; skipping.');
    return;
  }

  // Fetch latest PAXGUSDT price
  const res = await fetch('https://data-api.binance.vision/api/v3/ticker/price?symbol=PAXGUSDT');
  if (!res.ok) {
    console.error('Failed to fetch PAXGUSDT price:', res.status);
    return;
  }
  const ticker = await res.json();
  const price = Number(ticker.price);
  if (!(price > 0)) return;

  const state = await readJsonSafe(STATE_PATH, { last_sent: {} });
  if (!state.last_sent || typeof state.last_sent !== 'object') {
    state.last_sent = {};
  }

  const levels = [
    ...(analysis.resistances || []).map((r, i) => ({
      id: `R${i + 1}`,
      zone: 'SELL',
      price: Number(r.price)
    })),
    ...(analysis.supports || []).map((s, i) => ({
      id: `S${i + 1}`,
      zone: 'BUY',
      price: Number(s.price)
    }))
  ];

  const now = Date.now();
  let updated = false;

  for (const lvl of levels) {
    if (!(lvl.price > 0)) continue;
    const dist = Math.abs(price - lvl.price) / lvl.price;
    if (dist <= PROXIMITY_THRESHOLD) {
      const key = `${analysis.generated_at}_${lvl.id}`;
      const lastTs = Number(state.last_sent[key] || 0);
      if (now - lastTs >= COOLDOWN_MS) {
        const formattedPrice = price.toLocaleString('en-US', {
          minimumFractionDigits: 2,
          maximumFractionDigits: 2
        });
        const title = `${lvl.zone} ZONE TOUCHED – GOLD ${formattedPrice}`;
        const body = `GOLD touched ${lvl.id} (${lvl.price.toFixed(2)}). Live: ${formattedPrice} (${(dist * 100).toFixed(3)}% away).`;

        const pushRes = await fetch(`https://ntfy.sh/${encodeURIComponent(topic)}`, {
          method: 'POST',
          headers: {
            'Title': title,
            'Priority': '5',
            'Tags': 'rotating_light'
          },
          body
        });

        if (pushRes.ok) {
          console.log('Sent ntfy alert:', title);
          state.last_sent[key] = now;
          updated = true;
        } else {
          console.error('ntfy POST failed:', pushRes.status);
        }
      }
    }
  }

  if (updated) {
    // Prune entries older than 48h to keep alert-state.json compact
    for (const [k, ts] of Object.entries(state.last_sent)) {
      if (now - Number(ts) > 48 * 3600 * 1000) {
        delete state.last_sent[k];
      }
    }
    await fs.writeFile(STATE_PATH, JSON.stringify(state, null, 2) + '\n', 'utf8');
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
