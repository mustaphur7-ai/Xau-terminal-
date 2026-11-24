import fs from 'node:fs/promises';
import path from 'node:path';

const OUT_PATH = path.resolve('data/analysis.json');
const RETRY_DELAY_MS = 5 * 60 * 1000; // 5 minutes
const MAX_ATTEMPTS = 3;

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function fetchJson(url) {
  const res = await fetch(url, {
    headers: {
      'User-Agent': 'Mozilla/5.0 (compatible; GoldOptionsTerminal/1.0)',
      'Accept': 'application/json'
    }
  });
  if (!res.ok) throw new Error(`HTTP ${res.status} for ${url}`);
  return res.json();
}

async function getGoldSpotAndPrevClose() {
  // Primary: Binance PAXGUSDT
  try {
    const [ticker, klines] = await Promise.all([
      fetchJson('https://data-api.binance.vision/api/v3/ticker/price?symbol=PAXGUSDT'),
      fetchJson('https://data-api.binance.vision/api/v3/klines?symbol=PAXGUSDT&interval=1d&limit=2')
    ]);
    const spot = Number(ticker.price);
    const prevClose = Array.isArray(klines) && klines.length >= 2 ? Number(klines[0][4]) : spot;
    if (spot > 0) return { spot, prevClose, source: 'binance:PAXGUSDT' };
  } catch (err) {
    console.warn('Primary spot fetch failed, falling back to Yahoo GC=F:', err.message);
  }

  // Fallback: Yahoo Finance GC=F
  const yData = await fetchJson('https://query1.finance.yahoo.com/v8/finance/chart/GC=F?interval=1d&range=5d');
  const result = yData?.chart?.result?.[0];
  const meta = result?.meta;
  const spot = Number(meta?.regularMarketPrice);
  const closes = result?.indicators?.quote?.[0]?.close?.filter((v) => typeof v === 'number') || [];
  const prevClose = closes.length >= 2 ? closes[closes.length - 2] : (Number(meta?.previousClose) || spot);
  if (!(spot > 0)) throw new Error('Invalid spot price from fallback');
  return { spot, prevClose, source: 'yahoo:GC=F' };
}

function parseCboeSymbol(sym) {
  // Format: GLD261218C00250000
  const m = String(sym || '').match(/^([A-Z]+)(\d{6})([CP])(\d{8})$/);
  if (!m) return null;
  const yy = Number(m[2].slice(0, 2));
  const mm = Number(m[2].slice(2, 4));
  const dd = Number(m[2].slice(4, 6));
  const expiryIso = `20${String(yy).padStart(2, '0')}-${String(mm).padStart(2, '0')}-${String(dd).padStart(2, '0')}`;
  return {
    expiry: expiryIso,
    type: m[3] === 'C' ? 'call' : 'put',
    strike: Number(m[4]) / 1000
  };
}

function normPdf(x) {
  return Math.exp(-0.5 * x * x) / Math.sqrt(2 * Math.PI);
}

function bsGamma(S, K, T, iv, r = 0.04) {
  if (S <= 0 || K <= 0 || T <= 0 || iv <= 0) return 0;
  const sqrtT = Math.sqrt(T);
  const d1 = (Math.log(S / K) + (r + 0.5 * iv * iv) * T) / (iv * sqrtT);
  return normPdf(d1) / (S * iv * sqrtT);
}

function formatEatTimestamp(date) {
  // Africa/Nairobi is fixed UTC+3 (no DST)
  const eatMs = date.getTime() + 3 * 3600 * 1000;
  const d = new Date(eatMs);
  const yyyy = d.getUTCFullYear();
  const mm = String(d.getUTCMonth() + 1).padStart(2, '0');
  const dd = String(d.getUTCDate()).padStart(2, '0');
  const hh = String(d.getUTCHours()).padStart(2, '0');
  const min = String(d.getUTCMinutes()).padStart(2, '0');
  const sec = String(d.getUTCSeconds()).padStart(2, '0');
  return `${yyyy}-${mm}-${dd} ${hh}:${min}:${sec} EAT`;
}

async function runOnce() {
  const [{ spot, prevClose, source }, cboe] = await Promise.all([
    getGoldSpotAndPrevClose(),
    fetchJson('https://cdn.cboe.com/api/global/delayed_quotes/options/GLD.json')
  ]);

  const gldPrice = Number(cboe?.data?.current_price || cboe?.data?.close || cboe?.data?.last_trade_price);
  const rawOptions = cboe?.data?.options;
  if (!(gldPrice > 0) || !Array.isArray(rawOptions) || rawOptions.length === 0) {
    throw new Error('Invalid CBOE GLD options payload');
  }

  const ratio = spot / gldPrice;
  const now = new Date();
  const nowMs = now.getTime();

  const parsed = [];
  for (const opt of rawOptions) {
    const meta = parseCboeSymbol(opt.option);
    if (!meta) continue;

    const oi = Number(opt.open_interest || 0);
    const iv = Number(opt.iv || 0);
    const gamma = Number(opt.gamma || 0);
    const delta = Number(opt.delta || 0);
    const vega = Number(opt.vega || 0);
    if (oi <= 0 || gamma <= 0) continue;

    const expMs = Date.parse(`${meta.expiry}T21:00:00Z`);
    const dte = Math.max(0, (expMs - nowMs) / 86400000);
    const goldStrike = meta.strike * ratio;
    const expiryWeight = 1 / Math.sqrt(dte + 1);

    // GEX per option = gamma * OI * 100 * spot^2 * 0.01 (calls +, puts -)
    const sign = meta.type === 'call' ? 1 : -1;
    const gex = sign * gamma * oi * 100 * (spot * spot) * 0.01;

    parsed.push({
      strike: goldStrike,
      gldStrike: meta.strike,
      expiry: meta.expiry,
      dte,
      type: meta.type,
      oi,
      iv,
      gamma,
      delta,
      vega,
      gex,
      weightedGexOi: gex * expiryWeight * oi
    });
  }

  if (parsed.length === 0) {
    throw new Error('No valid active options found in CBOE chain');
  }

  // Group by rounded gold strike ($1 increments for clean aggregation)
  const strikeMap = new Map();
  for (const o of parsed) {
    const bucket = Math.round(o.strike);
    if (!strikeMap.has(bucket)) {
      strikeMap.set(bucket, {
        strike: bucket,
        call_gex: 0,
        put_gex: 0,
        net_gex: 0,
        call_score: 0,
        put_score: 0,
        oi: 0,
        iv_sum: 0,
        iv_oi: 0
      });
    }
    const row = strikeMap.get(bucket);
    if (o.type === 'call') {
      row.call_gex += o.gex;
      row.call_score += o.weightedGexOi;
    } else {
      row.put_gex += o.gex;
      row.put_score += Math.abs(o.weightedGexOi);
    }
    row.net_gex = row.call_gex + row.put_gex;
    row.oi += o.oi;
    if (o.iv > 0) {
      row.iv_sum += o.iv * o.oi;
      row.iv_oi += o.oi;
    }
  }

  const strikes = Array.from(strikeMap.values()).map((s) => ({
    strike: s.strike,
    call_gex: Math.round(s.call_gex),
    put_gex: Math.round(s.put_gex),
    net_gex: Math.round(s.net_gex),
    call_score: s.call_score,
    put_score: s.put_score,
    oi: s.oi,
    iv: s.iv_oi > 0 ? Number((s.iv_sum / s.iv_oi).toFixed(4)) : 0
  }));

  const totalAbsGex = strikes.reduce((acc, s) => acc + Math.abs(s.net_gex), 0) || 1;
  const scoreStrength = (absGex) => {
    const share = absGex / totalAbsGex;
    if (share >= 0.08) return 5;
    if (share >= 0.05) return 4;
    if (share >= 0.03) return 3;
    if (share >= 0.015) return 2;
    return 1;
  };

  // R1-R3: strikes above spot ranked by weighted call GEX * OI, top 3, nearest first
  const resistances = strikes
    .filter((s) => s.strike > spot && s.call_score > 0)
    .sort((a, b) => b.call_score - a.call_score)
    .slice(0, 3)
    .sort((a, b) => a.strike - b.strike)
    .map((s) => ({
      price: Number(s.strike.toFixed(2)),
      strength: scoreStrength(Math.abs(s.call_gex)),
      type: 'Call wall'
    }));

  // S1-S3: strikes below spot ranked by weighted put GEX * OI, top 3, nearest first
  const supports = strikes
    .filter((s) => s.strike < spot && s.put_score > 0)
    .sort((a, b) => b.put_score - a.put_score)
    .slice(0, 3)
    .sort((a, b) => b.strike - a.strike)
    .map((s) => ({
      price: Number(s.strike.toFixed(2)),
      strength: scoreStrength(Math.abs(s.put_gex)),
      type: 'Put wall'
    }));

  // Gamma flip: total net GEX over spot grid ±5% in 0.1% steps using BS gamma from each option's IV
  const grid = [];
  for (let step = -50; step <= 50; step++) {
    const testSpot = spot * (1 + step * 0.001);
    let net = 0;
    for (const o of parsed) {
      if (o.iv <= 0) continue;
      const T = Math.max(1 / 365, o.dte / 365);
      const g = bsGamma(testSpot, o.strike, T, o.iv);
      const sign = o.type === 'call' ? 1 : -1;
      net += sign * g * o.oi * 100 * (testSpot * testSpot) * 0.01;
    }
    grid.push({ spot: testSpot, net });
  }

  let gammaFlip = { price: null, status: 'no flip in range' };
  for (let i = 1; i < grid.length; i++) {
    const prev = grid[i - 1];
    const curr = grid[i];
    if ((prev.net <= 0 && curr.net >= 0) || (prev.net >= 0 && curr.net <= 0)) {
      const denom = Math.abs(curr.net - prev.net) || 1;
      const t = Math.abs(prev.net) / denom;
      const flipPrice = prev.spot + t * (curr.spot - prev.spot);
      gammaFlip = { price: Number(flipPrice.toFixed(2)), status: 'active' };
      break;
    }
  }

  // Expected range: ATM IV of nearest expiry (average of 2 strikes around spot, blending call & put IV)
  const expiries = Array.from(new Set(parsed.map((o) => o.expiry))).sort();
  const nearestExpiry = expiries[0];
  const nearOpts = parsed.filter((o) => o.expiry === nearestExpiry && o.iv > 0);
  const uniqueNearStrikes = Array.from(new Set(nearOpts.map((o) => o.strike))).sort(
    (a, b) => Math.abs(a - spot) - Math.abs(b - spot)
  );
  const twoClosest = uniqueNearStrikes.slice(0, 2);
  const atmSlice = nearOpts.filter((o) => twoClosest.includes(o.strike));
  const atmIv = atmSlice.length
    ? atmSlice.reduce((sum, o) => sum + o.iv, 0) / atmSlice.length
    : 0.16;

  const daily1s = spot * atmIv * Math.sqrt(1 / 365);
  const weekly1s = spot * atmIv * Math.sqrt(7 / 365);

  const expectedRange = {
    atm_iv: Number(atmIv.toFixed(4)),
    nearest_expiry: nearestExpiry,
    daily_1s_low: Number((prevClose - daily1s).toFixed(2)),
    daily_1s_high: Number((prevClose + daily1s).toFixed(2)),
    daily_2s_low: Number((prevClose - 2 * daily1s).toFixed(2)),
    daily_2s_high: Number((prevClose + 2 * daily1s).toFixed(2)),
    weekly_1s_low: Number((spot - weekly1s).toFixed(2)),
    weekly_1s_high: Number((spot + weekly1s).toFixed(2)),
    weekly_2s_low: Number((spot - 2 * weekly1s).toFixed(2)),
    weekly_2s_high: Number((spot + 2 * weekly1s).toFixed(2))
  };

  // Bias from net GEX & flip position
  const totalNetGex = strikes.reduce((acc, s) => acc + s.net_gex, 0);
  let bias = 'NEUTRAL';
  if (typeof gammaFlip.price === 'number') {
    if (spot > gammaFlip.price && totalNetGex > 0) bias = 'BULLISH';
    else if (spot < gammaFlip.price && totalNetGex < 0) bias = 'BEARISH';
  } else if (totalNetGex > 0) {
    bias = 'BULLISH';
  } else if (totalNetGex < 0) {
    bias = 'BEARISH';
  }

  const gexByStrike = strikes
    .filter((s) => s.strike >= spot * 0.94 && s.strike <= spot * 1.06)
    .sort((a, b) => a.strike - b.strike)
    .map(({ strike, call_gex, put_gex, net_gex, oi, iv }) => ({
      strike,
      call_gex,
      put_gex,
      net_gex,
      oi,
      iv
    }));

  const output = {
    generated_at: now.toISOString(),
    generated_at_eat: formatEatTimestamp(now),
    spot_source: source,
    spot: Number(spot.toFixed(2)),
    prev_close: Number(prevClose.toFixed(2)),
    gld_price: Number(gldPrice.toFixed(2)),
    gld_gold_ratio: Number(ratio.toFixed(4)),
    resistances,
    supports,
    gamma_flip: gammaFlip,
    expected_range: expectedRange,
    gex_by_strike: gexByStrike,
    bias
  };

  await fs.mkdir(path.dirname(OUT_PATH), { recursive: true });
  await fs.writeFile(OUT_PATH, JSON.stringify(output, null, 2) + '\n', 'utf8');
  console.log('Successfully updated data/analysis.json at', output.generated_at_eat);
}

async function main() {
  for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
    try {
      console.log(`Analysis attempt ${attempt}/${MAX_ATTEMPTS}…`);
      await runOnce();
      process.exit(0);
    } catch (err) {
      console.error(`Attempt ${attempt} failed:`, err.message);
      if (attempt < MAX_ATTEMPTS) {
        console.log('Waiting 5 minutes before retry…');
        await sleep(RETRY_DELAY_MS);
      }
    }
  }
  console.error('All 3 attempts failed. Exiting without overwriting data/analysis.json.');
  process.exit(0);
}

main();
