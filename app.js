(() => {
  'use strict';

  const SYMBOL = 'PAXGUSDT';
  const REST_KLINES = 'https://data-api.binance.vision/api/v3/klines';
  const REST_24HR = 'https://data-api.binance.vision/api/v3/ticker/24hr';
  const WS_BASE = 'wss://stream.binance.com:9443/ws';

  const DEFAULT_OVERLAYS = {
    keyLevels: true,
    gammaFlip: true,
    gexStrike: true,
    expectedRange: true,
    sessionLevels: false,
    signals: true,
    alerts: true
  };

  const OVERLAY_META = [
    { key: 'keyLevels', label: 'Key levels (R1-R3, S1-S3)', cat: 'options' },
    { key: 'gammaFlip', label: 'Gamma flip line', cat: 'options' },
    { key: 'gexStrike', label: 'GEX by strike chart', cat: 'options' },
    { key: 'expectedRange', label: 'Expected range band (1σ/2σ)', cat: 'options' },
    { key: 'sessionLevels', label: 'Session levels (PDH/PDL/PWH/PWL)', cat: 'structure' },
    { key: 'signals', label: 'Buy/Sell signal markers', cat: 'alerts' },
    { key: 'alerts', label: 'Zone proximity & signal alerts', cat: 'alerts' }
  ];

  const state = {
    analysis: null,
    livePrice: null,
    open24h: null,
    tf: '15m',
    candles: [],
    sessionLevels: null,
    chart: null,
    candleSeries: null,
    priceLines: [],
    rangeSeries: [],
    ws: null,
    wsRetryTimer: null,
    overlays: loadJSON('gold_overlays', DEFAULT_OVERLAYS),
    armedLevels: loadJSON('gold_armed_levels', {}),
    globalAlert: loadJSON('gold_global_alert', false),
    sensitivity: Number(localStorage.getItem('gold_sensitivity') || '0.001'),
    cooldownMin: Number(localStorage.getItem('gold_cooldown') || '30'),
    cooldowns: loadJSON('gold_cooldowns', {}),
    snoozedUntil: 0,
    activeSignal: null,
    lastSignaledCandleTime: 0,
    wakeLock: null,
    audioCtx: null,
    alarmInterval: null,
    vibrateInterval: null
  };

  function loadJSON(key, fallback) {
    try {
      const raw = localStorage.getItem(key);
      return raw ? JSON.parse(raw) : fallback;
    } catch {
      return fallback;
    }
  }

  function saveJSON(key, val) {
    try {
      localStorage.setItem(key, JSON.stringify(val));
    } catch {}
  }

  function fmtPrice(v) {
    if (v === null || v === undefined || Number.isNaN(Number(v))) return '--';
    return Number(v).toLocaleString('en-US', {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2
    });
  }

  function fmtSigned(v, digits = 2) {
    const n = Number(v);
    if (Number.isNaN(n)) return '--';
    return (n >= 0 ? '+' : '') + n.toFixed(digits);
  }

  /* -------------------- INITIALIZATION -------------------- */
  async function init() {
    registerSW();
    bindUI();
    renderIndicatorsSheet();
    syncSettingsUI();
    applyOverlayVisibility();

    await loadAnalysis();
    initChart();
    await Promise.all([
      load24hTicker(),
      loadSessionLevels(),
      loadCandles(state.tf)
    ]);
    connectWS(state.tf);
    updateWakeLock();
  }

  function registerSW() {
    if ('serviceWorker' in navigator) {
      navigator.serviceWorker.register('./sw.js').catch(() => {});
    }
  }

  /* -------------------- LOAD FROZEN ANALYSIS -------------------- */
  async function loadAnalysis() {
    const tsEl = document.getElementById('analysis-timestamp');
    try {
      const res = await fetch('data/analysis.json', { cache: 'no-cache' });
      if (!res.ok) throw new Error('HTTP ' + res.status);
      const data = await res.json();
      if (!data || !data.generated_at || !data.spot) {
        throw new Error('Incomplete analysis data');
      }
      state.analysis = data;
      renderAnalysisHeader(data);
      renderLevels();
      renderExpectedRange();
      renderGexChart();
    } catch {
      tsEl.textContent = 'data unavailable';
      document.getElementById('levels-unavailable').classList.remove('hidden');
      document.getElementById('range-unavailable').classList.remove('hidden');
      document.getElementById('range-content').classList.add('hidden');
    }
  }

  function renderAnalysisHeader(data) {
    const tsEl = document.getElementById('analysis-timestamp');
    const eatStr = data.generated_at_eat || '';
    const parts = eatStr.split(' ');
    const datePart = parts[0] || data.generated_at.slice(0, 10);
    const timePart = parts[1] ? parts[1].slice(0, 5) : data.generated_at.slice(11, 16);

    const ageHours = (Date.now() - Date.parse(data.generated_at)) / 3600000;
    const staleHtml = ageHours > 26 ? '<span class="badge badge-stale">stale</span>' : '';
    tsEl.innerHTML = `Analysis for ${datePart} · run at ${timePart} EAT ${staleHtml}`;

    const ivBadge = document.getElementById('badge-iv');
    const atmIv = data.expected_range?.atm_iv;
    ivBadge.textContent = atmIv ? `ATM IV: ${(atmIv * 100).toFixed(1)}%` : 'ATM IV: --';

    const biasBadge = document.getElementById('badge-bias');
    const bias = (data.bias || 'NEUTRAL').toUpperCase();
    biasBadge.textContent = bias;
    biasBadge.className = 'badge ' + (
      bias === 'BULLISH' ? 'badge-bull' : bias === 'BEARISH' ? 'badge-bear' : 'badge-neutral'
    );
  }

  /* -------------------- MARKET DATA & WEBSOCKET -------------------- */
  async function load24hTicker() {
    try {
      const res = await fetch(`${REST_24HR}?symbol=${SYMBOL}`);
      if (!res.ok) return;
      const d = await res.json();
      state.open24h = Number(d.openPrice);
      if (!state.livePrice) updateLivePrice(Number(d.lastPrice));
    } catch {}
  }

  async function loadSessionLevels() {
    try {
      const [dRes, wRes] = await Promise.all([
        fetch(`${REST_KLINES}?symbol=${SYMBOL}&interval=1d&limit=2`),
        fetch(`${REST_KLINES}?symbol=${SYMBOL}&interval=1w&limit=2`)
      ]);
      if (!dRes.ok || !wRes.ok) return;
      const dData = await dRes.json();
      const wData = await wRes.json();
      if (dData.length >= 2 && wData.length >= 2) {
        state.sessionLevels = {
          pdh: Number(dData[0][2]),
          pdl: Number(dData[0][3]),
          pwh: Number(wData[0][2]),
          pwl: Number(wData[0][3])
        };
        drawChartOverlays();
      }
    } catch {}
  }

  async function loadCandles(tf) {
    const errEl = document.getElementById('chart-error');
    try {
      const res = await fetch(`${REST_KLINES}?symbol=${SYMBOL}&interval=${tf}&limit=200`);
      if (!res.ok) throw new Error('Klines error');
      const raw = await res.json();
      state.candles = raw.map(k => ({
        time: Math.floor(k[0] / 1000),
        open: Number(k[1]),
        high: Number(k[2]),
        low: Number(k[3]),
        close: Number(k[4]),
        volume: Number(k[5])
      }));
      errEl.classList.add('hidden');

      if (state.candles.length) {
        const last = state.candles[state.candles.length - 1];
        updateLivePrice(last.close);
      }

      if (state.candleSeries) {
        state.candleSeries.setData(state.candles);
        drawChartOverlays();
        evaluateSignals(true);
      }
    } catch {
      errEl.classList.remove('hidden');
    }
  }

  function connectWS(tf) {
    if (state.ws) {
      state.ws.onclose = null;
      state.ws.close();
    }
    clearTimeout(state.wsRetryTimer);
    const wsBadge = document.getElementById('badge-ws');
    wsBadge.textContent = 'WS: …';

    const stream = `${SYMBOL.toLowerCase()}@kline_${tf}`;
    const ws = new WebSocket(`${WS_BASE}/${stream}`);
    state.ws = ws;

    ws.onopen = () => {
      wsBadge.textContent = 'WS: LIVE';
      wsBadge.className = 'badge badge-bull';
    };

    ws.onmessage = (evt) => {
      try {
        const msg = JSON.parse(evt.data);
        const k = msg.k;
        if (!k) return;
        const bar = {
          time: Math.floor(k.t / 1000),
          open: Number(k.o),
          high: Number(k.h),
          low: Number(k.l),
          close: Number(k.c),
          volume: Number(k.v)
        };
        updateLivePrice(bar.close);

        const len = state.candles.length;
        if (len && state.candles[len - 1].time === bar.time) {
          state.candles[len - 1] = bar;
        } else {
          state.candles.push(bar);
          if (state.candles.length > 300) state.candles.shift();
        }

        if (state.candleSeries) {
          state.candleSeries.update(bar);
        }

        if (k.x) {
          // Candle closed -> evaluate signal
          evaluateSignals(false);
        }
      } catch {}
    };

    ws.onerror = () => ws.close();
    ws.onclose = () => {
      wsBadge.textContent = 'WS: OFF';
      wsBadge.className = 'badge badge-bear';
      state.wsRetryTimer = setTimeout(() => connectWS(state.tf), 4000);
    };
  }

  function updateLivePrice(price) {
    if (!price || Number.isNaN(price)) return;
    state.livePrice = price;

    const priceEl = document.getElementById('live-price');
    priceEl.textContent = fmtPrice(price);

    const chgEl = document.getElementById('live-change');
    const ref = state.open24h || state.analysis?.spot;
    if (ref) {
      const diff = price - ref;
      const pct = (diff / ref) * 100;
      chgEl.textContent = `${fmtSigned(diff)} (${fmtSigned(pct)}%)`;
      chgEl.className = 'live-change ' + (diff >= 0 ? 'up' : 'down');
    }

    renderLevels();
    renderExpectedRange();
    renderGexChart();
    checkProximityAlerts(price);
  }

  /* -------------------- PAGE 1: LEVELS -------------------- */
  function getOrderedLevels() {
    if (!state.analysis) return [];
    const r = (state.analysis.resistances || []).map((lvl, idx) => ({
      id: `R${idx + 1}`,
      side: 'res',
      zoneType: 'SELL',
      ...lvl
    }));
    const s = (state.analysis.supports || []).map((lvl, idx) => ({
      id: `S${idx + 1}`,
      side: 'sup',
      zoneType: 'BUY',
      ...lvl
    }));
    // Display R3 -> R2 -> R1 -> Flip -> S1 -> S2 -> S3
    return {
      resTopDown: [...r].reverse(),
      supTopDown: s
    };
  }

  function renderLevels() {
    const container = document.getElementById('levels-list');
    if (!state.analysis) return;

    const live = state.livePrice ?? state.analysis.spot;
    const { resTopDown, supTopDown } = getOrderedLevels();
    const flip = state.analysis.gamma_flip;

    const makeRowHtml = (lvl) => {
      const diff = lvl.price - live;
      const pct = (diff / live) * 100;
      const armed = !!(state.globalAlert || state.armedLevels[lvl.id]);
      const bars = [1, 2, 3, 4, 5]
        .map(n => `<span class="${n <= (lvl.strength || 1) ? 'on' : ''}"></span>`)
        .join('');

      return `
        <div class="level-row ${lvl.side}" data-lvl-id="${lvl.id}">
          <div class="lvl-left">
            <div class="lvl-title">
              <span class="lvl-tag">${lvl.id}</span>
              <span>${fmtPrice(lvl.price)}</span>
            </div>
            <div class="lvl-sub">${lvl.type || (lvl.side === 'res' ? 'Call wall' : 'Put wall')}</div>
          </div>
          <div class="lvl-right">
            <div class="lvl-dist">
              ${fmtSigned(diff)}
              <small>${fmtSigned(pct)}%</small>
            </div>
            <div class="strength-bars" title="Strength ${lvl.strength}/5">${bars}</div>
            <button class="bell-btn ${armed ? 'armed' : ''}" data-arm-id="${lvl.id}" aria-label="Alert me on ${lvl.id}">🔔</button>
          </div>
        </div>
      `;
    };

    let flipHtml = '';
    if (flip && typeof flip.price === 'number') {
      const diff = flip.price - live;
      const pct = (diff / live) * 100;
      const posGamma = live >= flip.price;
      const regimeText = posGamma
        ? 'Above flip = positive gamma, mean-reverting'
        : 'Below flip = negative gamma, trending';
      flipHtml = `
        <div class="level-row flip">
          <div class="lvl-left">
            <div class="lvl-title">
              <span class="lvl-tag">γ FLIP</span>
              <span>${fmtPrice(flip.price)}</span>
            </div>
            <div class="lvl-sub">${regimeText}</div>
          </div>
          <div class="lvl-right">
            <div class="lvl-dist">
              ${fmtSigned(diff)}
              <small>${fmtSigned(pct)}%</small>
            </div>
          </div>
        </div>
      `;
    } else {
      flipHtml = `
        <div class="level-row flip">
          <div class="lvl-left">
            <div class="lvl-title"><span class="lvl-tag">γ FLIP</span><span>no flip in range</span></div>
            <div class="lvl-sub">Neutral / mixed dealer gamma across ±5% grid</div>
          </div>
        </div>
      `;
    }

    container.innerHTML =
      resTopDown.map(makeRowHtml).join('') +
      flipHtml +
      supTopDown.map(makeRowHtml).join('');
  }

  /* -------------------- PAGE 2A: EXPECTED RANGE -------------------- */
  function renderExpectedRange() {
    const er = state.analysis?.expected_range;
    if (!er) return;

    document.getElementById('range-iv-label').textContent =
      `ATM IV ${(er.atm_iv * 100).toFixed(1)}%`;
    document.getElementById('d1-bounds').textContent =
      `${fmtPrice(er.daily_1s_low)} – ${fmtPrice(er.daily_1s_high)}`;
    document.getElementById('d2-bounds').textContent =
      `${fmtPrice(er.daily_2s_low)} – ${fmtPrice(er.daily_2s_high)}`;
    document.getElementById('w1-bounds').textContent =
      `${fmtPrice(er.weekly_1s_low)} – ${fmtPrice(er.weekly_1s_high)}`;
    document.getElementById('w2-bounds').textContent =
      `${fmtPrice(er.weekly_2s_low)} – ${fmtPrice(er.weekly_2s_high)}`;

    const low = er.daily_1s_low;
    const high = er.daily_1s_high;
    document.getElementById('bar-low').textContent = fmtPrice(low);
    document.getElementById('bar-high').textContent = fmtPrice(high);

    const live = state.livePrice ?? state.analysis.spot;
    const span = high - low;
    const pct = span > 0 ? Math.max(0, Math.min(100, ((live - low) / span) * 100)) : 50;
    document.getElementById('range-bar-fill').style.width = `${pct}%`;
    document.getElementById('range-bar-pin').style.left = `${pct}%`;
  }

  /* -------------------- PAGE 2B: LIGHTWEIGHT CHARTS -------------------- */
  function initChart() {
    const container = document.getElementById('chart-container');
    if (!window.LightweightCharts || !container) return;

    const chart = LightweightCharts.createChart(container, {
      layout: {
        background: { type: 'solid', color: '#0d1117' },
        textColor: '#8b949e',
        fontFamily: 'ui-monospace, SFMono-Regular, Menlo, monospace',
        fontSize: 10
      },
      grid: {
        vertLines: { color: 'rgba(48, 54, 61, 0.45)', style: 0 },
        horzLines: { color: 'rgba(48, 54, 61, 0.45)', style: 0 }
      },
      rightPriceScale: {
        borderColor: '#30363d',
        scaleMargins: { top: 0.08, bottom: 0.08 }
      },
      timeScale: {
        borderColor: '#30363d',
        timeVisible: true,
        secondsVisible: false
      },
      crosshair: {
        mode: LightweightCharts.CrosshairMode.Normal
      }
    });

    const candleSeries = chart.addCandlestickSeries({
      upColor: '#2ea043',
      downColor: '#f85149',
      borderUpColor: '#2ea043',
      borderDownColor: '#f85149',
      wickUpColor: '#2ea043',
      wickDownColor: '#f85149'
    });

    state.chart = chart;
    state.candleSeries = candleSeries;

    const ro = new ResizeObserver(() => {
      chart.applyOptions({ width: container.clientWidth, height: container.clientHeight });
    });
    ro.observe(container);
  }

  function clearChartOverlays() {
    if (!state.candleSeries || !state.chart) return;
    for (const line of state.priceLines) {
      try { state.candleSeries.removePriceLine(line); } catch {}
    }
    state.priceLines = [];

    for (const s of state.rangeSeries) {
      try { state.chart.removeSeries(s); } catch {}
    }
    state.rangeSeries = [];
  }

  function drawChartOverlays() {
    if (!state.candleSeries || !state.chart) return;
    clearChartOverlays();

    const a = state.analysis;
    if (a && state.overlays.keyLevels) {
      (a.resistances || []).forEach((r, idx) => {
        const alpha = 0.25 + (r.strength || 1) * 0.13;
        const line = state.candleSeries.createPriceLine({
          price: r.price,
          color: `rgba(248, 81, 73, ${alpha.toFixed(2)})`,
          lineWidth: Math.min(3, Math.ceil((r.strength || 1) / 2)),
          lineStyle: LightweightCharts.LineStyle.Solid,
          axisLabelVisible: true,
          title: `R${idx + 1} (${r.strength}★)`
        });
        state.priceLines.push(line);
      });

      (a.supports || []).forEach((s, idx) => {
        const alpha = 0.25 + (s.strength || 1) * 0.13;
        const line = state.candleSeries.createPriceLine({
          price: s.price,
          color: `rgba(46, 160, 67, ${alpha.toFixed(2)})`,
          lineWidth: Math.min(3, Math.ceil((s.strength || 1) / 2)),
          lineStyle: LightweightCharts.LineStyle.Solid,
          axisLabelVisible: true,
          title: `S${idx + 1} (${s.strength}★)`
        });
        state.priceLines.push(line);
      });
    }

    if (a && state.overlays.gammaFlip && typeof a.gamma_flip?.price === 'number') {
      const line = state.candleSeries.createPriceLine({
        price: a.gamma_flip.price,
        color: '#d29922',
        lineWidth: 2,
        lineStyle: LightweightCharts.LineStyle.Dashed,
        axisLabelVisible: true,
        title: 'γ FLIP'
      });
      state.priceLines.push(line);
    }

    if (a && state.overlays.expectedRange && a.expected_range && state.candles.length >= 2) {
      const er = a.expected_range;
      const times = state.candles.map(c => c.time);

      // 1σ shaded band via BaselineSeries + border lines
      const mid1s = (er.daily_1s_high + er.daily_1s_low) / 2;
      const bandTop = state.chart.addBaselineSeries({
        baseValue: { type: 'price', price: mid1s },
        topFillColor1: 'rgba(88, 166, 255, 0.14)',
        topFillColor2: 'rgba(88, 166, 255, 0.14)',
        topLineColor: 'rgba(88, 166, 255, 0.55)',
        bottomFillColor1: 'transparent',
        bottomFillColor2: 'transparent',
        bottomLineColor: 'transparent',
        lineWidth: 1,
        priceLineVisible: false,
        lastValueVisible: false
      });
      bandTop.setData(times.map(t => ({ time: t, value: er.daily_1s_high })));

      const bandBot = state.chart.addBaselineSeries({
        baseValue: { type: 'price', price: mid1s },
        topFillColor1: 'transparent',
        topFillColor2: 'transparent',
        topLineColor: 'transparent',
        bottomFillColor1: 'rgba(88, 166, 255, 0.14)',
        bottomFillColor2: 'rgba(88, 166, 255, 0.14)',
        bottomLineColor: 'rgba(88, 166, 255, 0.55)',
        lineWidth: 1,
        priceLineVisible: false,
        lastValueVisible: false
      });
      bandBot.setData(times.map(t => ({ time: t, value: er.daily_1s_low })));
      state.rangeSeries.push(bandTop, bandBot);

      // 2σ dashed lines
      [
        { price: er.daily_2s_high, title: '+2σ' },
        { price: er.daily_2s_low, title: '-2σ' }
      ].forEach(b => {
        const l = state.candleSeries.createPriceLine({
          price: b.price,
          color: 'rgba(88, 166, 255, 0.6)',
          lineWidth: 1,
          lineStyle: LightweightCharts.LineStyle.Dashed,
          axisLabelVisible: true,
          title: b.title
        });
        state.priceLines.push(l);
      });
    }

    if (state.overlays.sessionLevels && state.sessionLevels) {
      const sl = state.sessionLevels;
      [
        { price: sl.pdh, title: 'PDH' },
        { price: sl.pdl, title: 'PDL' },
        { price: sl.pwh, title: 'PWH' },
        { price: sl.pwl, title: 'PWL' }
      ].forEach(item => {
        const l = state.candleSeries.createPriceLine({
          price: item.price,
          color: '#8b949e',
          lineWidth: 1,
          lineStyle: LightweightCharts.LineStyle.Dotted,
          axisLabelVisible: true,
          title: item.title
        });
        state.priceLines.push(l);
      });
    }
  }

  /* -------------------- PAGE 2C: GEX BY STRIKE SVG -------------------- */
  function renderGexChart() {
    const svg = document.getElementById('gex-svg');
    const a = state.analysis;
    if (!svg || !a || !Array.isArray(a.gex_by_strike)) return;

    const spot = a.spot;
    const minP = spot * 0.95;
    const maxP = spot * 1.05;

    // Sort high price at top to low price at bottom
    const rows = a.gex_by_strike
      .filter(d => d.strike >= minP && d.strike <= maxP)
      .sort((x, y) => y.strike - x.strike);

    if (!rows.length) {
      svg.innerHTML = '';
      return;
    }

    const rowH = 22;
    const padTop = 10;
    const padBot = 10;
    const width = 360;
    const height = padTop + padBot + rows.length * rowH;
    const centerX = 185;
    const maxBarW = 115;

    svg.setAttribute('viewBox', `0 0 ${width} ${height}`);
    svg.style.height = `${Math.max(220, height)}px`;

    let maxAbs = 1;
    for (const r of rows) {
      maxAbs = Math.max(maxAbs, Math.abs(r.call_gex || 0), Math.abs(r.put_gex || 0));
    }

    const live = state.livePrice ?? spot;
    const flipP = a.gamma_flip?.price;
    const rPrices = (a.resistances || []).map((x, i) => ({ p: x.price, tag: `R${i + 1}`, col: '#f85149' }));
    const sPrices = (a.supports || []).map((x, i) => ({ p: x.price, tag: `S${i + 1}`, col: '#2ea043' }));

    const priceToY = (p) => {
      const topP = rows[0].strike;
      const botP = rows[rows.length - 1].strike;
      if (topP === botP) return padTop;
      const ratio = (topP - p) / (topP - botP);
      return padTop + ratio * ((rows.length - 1) * rowH) + rowH / 2;
    };

    let out = `<line x1="${centerX}" y1="0" x2="${centerX}" y2="${height}" stroke="#30363d" stroke-width="1"/>`;

    rows.forEach((r, i) => {
      const y = padTop + i * rowH;
      const midY = y + rowH / 2;
      const callW = (Math.max(0, r.call_gex || 0) / maxAbs) * maxBarW;
      const putW = (Math.abs(Math.min(0, r.put_gex || 0)) / maxAbs) * maxBarW;

      out += `
        <g class="gex-bar-row" data-idx="${i}" style="cursor:pointer">
          <rect x="0" y="${y}" width="${width}" height="${rowH}" fill="transparent" />
          <text x="4" y="${midY + 3}" fill="#8b949e" font-family="monospace" font-size="9">${r.strike.toFixed(0)}</text>
          ${putW > 0 ? `<rect x="${centerX - putW}" y="${y + 4}" width="${putW}" height="${rowH - 8}" fill="#f85149" rx="2"/>` : ''}
          ${callW > 0 ? `<rect x="${centerX}" y="${y + 4}" width="${callW}" height="${rowH - 8}" fill="#2ea043" rx="2"/>` : ''}
        </g>
      `;
    });

    // Reference lines: Live, Flip, R1-R3, S1-S3
    const markers = [
      { p: live, tag: `LIVE ${live.toFixed(0)}`, col: '#58a6ff', dash: '2,2' },
      ...(typeof flipP === 'number' ? [{ p: flipP, tag: 'FLIP', col: '#d29922', dash: '4,2' }] : []),
      ...rPrices.map(x => ({ ...x, dash: '' })),
      ...sPrices.map(x => ({ ...x, dash: '' }))
    ];

    markers.forEach(m => {
      if (m.p < rows[rows.length - 1].strike || m.p > rows[0].strike) return;
      const y = priceToY(m.p);
      out += `
        <line x1="48" y1="${y}" x2="${width - 4}" y2="${y}" stroke="${m.col}" stroke-width="1" ${m.dash ? `stroke-dasharray="${m.dash}"` : ''}/>
        <text x="${width - 6}" y="${y - 2}" fill="${m.col}" font-family="monospace" font-size="8" font-weight="bold" text-anchor="end">${m.tag}</text>
      `;
    });

    svg.innerHTML = out;

    svg.querySelectorAll('.gex-bar-row').forEach(el => {
      el.addEventListener('click', () => {
        const idx = Number(el.getAttribute('data-idx'));
        const item = rows[idx];
        if (!item) return;
        const tip = document.getElementById('gex-tooltip');
        tip.classList.remove('hidden');
        tip.textContent =
          `Strike $${fmtPrice(item.strike)} · Call GEX: ${(item.call_gex / 1e6).toFixed(2)}M · Put GEX: ${(item.put_gex / 1e6).toFixed(2)}M · OI: ${item.oi} · IV: ${(item.iv * 100).toFixed(1)}%`;
      });
    });
  }

  /* -------------------- SIGNALS ENGINE -------------------- */
  function computeATR(candles, period = 14) {
    if (candles.length < 2) return 0;
    const trs = [];
    for (let i = 1; i < candles.length; i++) {
      const c = candles[i];
      const pc = candles[i - 1].close;
      trs.push(Math.max(c.high - c.low, Math.abs(c.high - pc), Math.abs(c.low - pc)));
    }
    const slice = trs.slice(-period);
    return slice.reduce((a, b) => a + b, 0) / slice.length;
  }

  function evaluateSignals(isInitialLoad) {
    const a = state.analysis;
    if (!a || state.candles.length < 22) return;

    const atr = computeATR(state.candles, 14);
    const resistances = (a.resistances || []).map((r, i) => ({ id: `R${i + 1}`, ...r }));
    const supports = (a.supports || []).map((s, i) => ({ id: `S${i + 1}`, ...s }));
    const flipPrice = a.gamma_flip?.price;

    const markers = [];
    let latestSignal = null;

    // Evaluate historical closed candles (up to second-to-last bar)
    const closedCount = state.candles.length - 1;
    const startIdx = Math.max(20, closedCount - 60);

    for (let i = startIdx; i < closedCount; i++) {
      const bar = state.candles[i];
      const prevSlice = state.candles.slice(i - 20, i);
      const avgVol = prevSlice.reduce((s, x) => s + x.volume, 0) / prevSlice.length;
      const posGamma = typeof flipPrice === 'number' ? bar.close >= flipPrice : true;
      const regime = posGamma ? 'POSITIVE GAMMA (Fade)' : 'NEGATIVE GAMMA (Breakout)';

      let sig = null;

      // Check BUY bounce at Supports
      for (const sup of supports) {
        const tol = Math.max(sup.price * 0.0015, 0.3 * atr);
        if (bar.low <= sup.price + tol && bar.close > sup.price && bar.open >= sup.price - tol) {
          const stop = sup.price - 0.3 * atr;
          const target = resistances[0]?.price || (bar.close + 2 * atr);
          const rr = Math.abs(target - bar.close) / Math.max(0.01, Math.abs(bar.close - stop));
          sig = {
            direction: 'BUY',
            setup: `Bounce @ ${sup.id}`,
            levelId: sup.id,
            entry: bar.close,
            stop,
            target,
            rr,
            regime,
            confidence: Math.min(5, Math.max(1, (sup.strength || 2) + (posGamma ? 1 : 0))),
            time: bar.time
          };
          break;
        }
      }

      // Check SELL rejection at Resistances
      if (!sig) {
        for (const res of resistances) {
          const tol = Math.max(res.price * 0.0015, 0.3 * atr);
          if (bar.high >= res.price - tol && bar.close < res.price && bar.open <= res.price + tol) {
            const stop = res.price + 0.3 * atr;
            const target = supports[0]?.price || (bar.close - 2 * atr);
            const rr = Math.abs(bar.close - target) / Math.max(0.01, Math.abs(stop - bar.close));
            sig = {
              direction: 'SELL',
              setup: `Rejection @ ${res.id}`,
              levelId: res.id,
              entry: bar.close,
              stop,
              target,
              rr,
              regime,
              confidence: Math.min(5, Math.max(1, (res.strength || 2) + (posGamma ? 1 : 0))),
              time: bar.time
            };
            break;
          }
        }
      }

      // Negative gamma breakout / breakdown with volume > 20-bar avg
      if (!sig && !posGamma && bar.volume > avgVol) {
        const prevBar = state.candles[i - 1];
        for (let rIdx = 0; rIdx < resistances.length; rIdx++) {
          const res = resistances[rIdx];
          if (prevBar.close <= res.price && bar.close > res.price) {
            const stop = res.price - 0.3 * atr;
            const target = resistances[rIdx + 1]?.price || (bar.close + 2 * atr);
            const rr = Math.abs(target - bar.close) / Math.max(0.01, Math.abs(bar.close - stop));
            sig = {
              direction: 'BUY',
              setup: `Breakout > ${res.id}`,
              levelId: res.id,
              entry: bar.close,
              stop,
              target,
              rr,
              regime,
              confidence: Math.min(5, Math.max(1, (res.strength || 2) + 1)),
              time: bar.time
            };
            break;
          }
        }
        if (!sig) {
          for (let sIdx = 0; sIdx < supports.length; sIdx++) {
            const sup = supports[sIdx];
            if (prevBar.close >= sup.price && bar.close < sup.price) {
              const stop = sup.price + 0.3 * atr;
              const target = supports[sIdx + 1]?.price || (bar.close - 2 * atr);
              const rr = Math.abs(bar.close - target) / Math.max(0.01, Math.abs(stop - bar.close));
              sig = {
                direction: 'SELL',
                setup: `Breakdown < ${sup.id}`,
                levelId: sup.id,
                entry: bar.close,
                stop,
                target,
                rr,
                regime,
                confidence: Math.min(5, Math.max(1, (sup.strength || 2) + 1)),
                time: bar.time
              };
              break;
            }
          }
        }
      }

      if (sig) {
        latestSignal = sig;
        markers.push({
          time: bar.time,
          position: sig.direction === 'BUY' ? 'belowBar' : 'aboveBar',
          color: sig.direction === 'BUY' ? '#2ea043' : '#f85149',
          shape: sig.direction === 'BUY' ? 'arrowUp' : 'arrowDown',
          text: sig.direction
        });
      }
    }

    if (state.candleSeries) {
      state.candleSeries.setMarkers(state.overlays.signals ? markers : []);
    }

    state.activeSignal = latestSignal;
    renderSignalCard();

    // Trigger alarm if a brand-new signal occurred on the most recently closed candle
    const lastClosed = state.candles[closedCount - 1];
    if (
      !isInitialLoad &&
      latestSignal &&
      lastClosed &&
      latestSignal.time === lastClosed.time &&
      latestSignal.time !== state.lastSignaledCandleTime
    ) {
      state.lastSignaledCandleTime = latestSignal.time;
      if (state.overlays.alerts && (state.globalAlert || state.armedLevels[latestSignal.levelId])) {
        triggerAlarm(latestSignal.direction, latestSignal.entry, latestSignal.setup);
      }
    }
  }

  function renderSignalCard() {
    const body = document.getElementById('signal-body');
    const pill = document.getElementById('signal-regime-pill');
    document.getElementById('signal-tf-label').textContent = state.tf;

    const a = state.analysis;
    const live = state.livePrice ?? a?.spot;
    const flipP = a?.gamma_flip?.price;
    if (typeof flipP === 'number' && live) {
      pill.textContent = live >= flipP ? '+GAMMA FADE' : '-GAMMA TREND';
    } else {
      pill.textContent = 'NEUTRAL';
    }

    const sig = state.activeSignal;
    if (!sig) {
      body.innerHTML = `<div class="signal-neutral">NEUTRAL — No active level rejection or breakout on ${state.tf} close.</div>`;
      return;
    }

    const isBuy = sig.direction === 'BUY';
    body.innerHTML = `
      <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:6px;">
        <span class="badge ${isBuy ? 'badge-bull' : 'badge-bear'}" style="font-size:12px;padding:4px 8px;">
          ${isBuy ? '▲ BUY' : '▼ SELL'} · ${sig.setup}
        </span>
        <span class="mono-dim">Conf: ${sig.confidence}/5</span>
      </div>
      <div class="signal-grid">
        <div class="sig-cell"><div class="sig-cell-label">ENTRY</div><div class="sig-cell-val">${fmtPrice(sig.entry)}</div></div>
        <div class="sig-cell"><div class="sig-cell-label">STOP (0.3×ATR)</div><div class="sig-cell-val">${fmtPrice(sig.stop)}</div></div>
        <div class="sig-cell"><div class="sig-cell-label">TARGET</div><div class="sig-cell-val">${fmtPrice(sig.target)}</div></div>
        <div class="sig-cell"><div class="sig-cell-label">RISK : REWARD</div><div class="sig-cell-val">1 : ${sig.rr.toFixed(2)}</div></div>
      </div>
      <div class="mono-dim" style="margin-top:6px;">Regime: ${sig.regime}</div>
    `;
  }

  /* -------------------- ALERTS & WEB AUDIO SIREN -------------------- */
  function checkProximityAlerts(price) {
    if (!state.analysis || !state.overlays.alerts) return;
    if (Date.now() < state.snoozedUntil) return;

    const levels = [
      ...(state.analysis.resistances || []).map((r, i) => ({ id: `R${i + 1}`, zone: 'SELL', price: r.price })),
      ...(state.analysis.supports || []).map((s, i) => ({ id: `S${i + 1}`, zone: 'BUY', price: s.price }))
    ];

    const cooldownMs = state.cooldownMin * 60 * 1000;
    const now = Date.now();

    for (const lvl of levels) {
      const armed = state.globalAlert || state.armedLevels[lvl.id];
      if (!armed) continue;

      const distRatio = Math.abs(price - lvl.price) / lvl.price;
      if (distRatio <= state.sensitivity) {
        const lastFired = state.cooldowns[lvl.id] || 0;
        if (now - lastFired >= cooldownMs) {
          state.cooldowns[lvl.id] = now;
          saveJSON('gold_cooldowns', state.cooldowns);
          triggerAlarm(lvl.zone, price, `${lvl.id} (${fmtPrice(lvl.price)})`);
          break;
        }
      }
    }
  }

  function unlockAudio() {
    const AudioCtx = window.AudioContext || window.webkitAudioContext;
    if (!AudioCtx) return;
    if (!state.audioCtx) {
      state.audioCtx = new AudioCtx();
    }
    if (state.audioCtx.state === 'suspended') {
      state.audioCtx.resume().catch(() => {});
    }
  }

  function playSirenPulse() {
    unlockAudio();
    if (!state.audioCtx) return;
    const ctx = state.audioCtx;
    const now = ctx.currentTime;
    const osc = ctx.createOscillator();
    const gain = ctx.createGain();

    osc.type = 'sawtooth';
    osc.frequency.setValueAtTime(660, now);
    osc.frequency.exponentialRampToValueAtTime(1180, now + 0.35);
    osc.frequency.exponentialRampToValueAtTime(660, now + 0.7);

    gain.gain.setValueAtTime(0.28, now);
    gain.gain.exponentialRampToValueAtTime(0.01, now + 0.72);

    osc.connect(gain);
    gain.connect(ctx.destination);
    osc.start(now);
    osc.stop(now + 0.75);
  }

  function triggerAlarm(direction, price, subText) {
    const overlay = document.getElementById('alarm-overlay');
    const title = document.getElementById('alarm-title');
    const sub = document.getElementById('alarm-sub');

    const isBuy = direction === 'BUY';
    overlay.className = `alarm-overlay ${isBuy ? 'buy' : 'sell'}`;
    title.textContent = `🔔 ${isBuy ? 'BUY' : 'SELL'} ZONE TOUCHED — GOLD ${fmtPrice(price)}`;
    sub.textContent = subText;

    stopAlarmMedia();
    playSirenPulse();
    state.alarmInterval = setInterval(playSirenPulse, 850);

    if (navigator.vibrate) {
      navigator.vibrate([400, 200, 400]);
      state.vibrateInterval = setInterval(() => {
        navigator.vibrate([400, 200, 400]);
      }, 1200);
    }
  }

  function stopAlarmMedia() {
    clearInterval(state.alarmInterval);
    clearInterval(state.vibrateInterval);
    state.alarmInterval = null;
    state.vibrateInterval = null;
    if (navigator.vibrate) navigator.vibrate(0);
  }

  function dismissAlarm(snoozeMin = 0) {
    stopAlarmMedia();
    document.getElementById('alarm-overlay').classList.add('hidden');
    if (snoozeMin > 0) {
      state.snoozedUntil = Date.now() + snoozeMin * 60 * 1000;
    }
  }

  async function updateWakeLock() {
    const anyArmed = state.globalAlert || Object.values(state.armedLevels).some(Boolean);
    if (!('wakeLock' in navigator)) return;
    try {
      if (anyArmed && !state.wakeLock) {
        state.wakeLock = await navigator.wakeLock.request('screen');
        state.wakeLock.addEventListener('release', () => { state.wakeLock = null; });
      } else if (!anyArmed && state.wakeLock) {
        await state.wakeLock.release();
        state.wakeLock = null;
      }
    } catch {}
  }

  /* -------------------- UI BINDINGS & SHEETS -------------------- */
  function bindUI() {
    // Bottom tab bar
    document.querySelectorAll('.nav-btn').forEach(btn => {
      btn.addEventListener('click', () => {
        document.querySelectorAll('.nav-btn').forEach(b => b.classList.remove('active'));
        document.querySelectorAll('.tab-page').forEach(p => p.classList.remove('active'));
        btn.classList.add('active');
        const target = document.getElementById(btn.dataset.tab);
        if (target) target.classList.add('active');
        if (btn.dataset.tab === 'page-chart' && state.chart) {
          const c = document.getElementById('chart-container');
          state.chart.applyOptions({ width: c.clientWidth, height: c.clientHeight });
          renderGexChart();
        }
      });
    });

    // Global alert switch
    const globalToggle = document.getElementById('toggle-global-alert');
    globalToggle.checked = state.globalAlert;
    globalToggle.addEventListener('change', (e) => {
      unlockAudio();
      state.globalAlert = e.target.checked;
      saveJSON('gold_global_alert', state.globalAlert);
      renderLevels();
      updateWakeLock();
    });

    // Tap level row or bell icon
    document.getElementById('levels-list').addEventListener('click', (e) => {
      const row = e.target.closest('[data-lvl-id]');
      if (!row) return;
      unlockAudio();
      const id = row.dataset.lvlId;
      state.armedLevels[id] = !state.armedLevels[id];
      saveJSON('gold_armed_levels', state.armedLevels);
      renderLevels();
      updateWakeLock();
    });

    // Timeframe buttons
    document.querySelectorAll('.tf-btn').forEach(btn => {
      btn.addEventListener('click', async () => {
        document.querySelectorAll('.tf-btn').forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        state.tf = btn.dataset.tf;
        await loadCandles(state.tf);
        connectWS(state.tf);
      });
    });

    // Expected range toggle
    const rangeBtn = document.getElementById('btn-toggle-range-band');
    rangeBtn.classList.toggle('active', state.overlays.expectedRange);
    rangeBtn.addEventListener('click', () => {
      state.overlays.expectedRange = !state.overlays.expectedRange;
      rangeBtn.classList.toggle('active', state.overlays.expectedRange);
      saveJSON('gold_overlays', state.overlays);
      drawChartOverlays();
      renderIndicatorsSheet();
    });

    // Modals
    document.getElementById('btn-open-indicators').addEventListener('click', () => {
      document.getElementById('modal-indicators').classList.remove('hidden');
    });
    document.getElementById('btn-alarm-settings').addEventListener('click', () => {
      document.getElementById('modal-settings').classList.remove('hidden');
    });
    document.querySelectorAll('[data-close]').forEach(btn => {
      btn.addEventListener('click', () => {
        document.getElementById(btn.dataset.close).classList.add('hidden');
      });
    });

    // Test alarm & alarm controls
    document.getElementById('btn-test-alarm').addEventListener('click', () => {
      unlockAudio();
      const p = state.livePrice ?? state.analysis?.spot ?? 0;
      triggerAlarm('BUY', p, 'Test Alarm · Audio & Vibration Unlocked');
    });
    document.getElementById('btn-alarm-dismiss').addEventListener('click', () => dismissAlarm(0));
    document.getElementById('btn-alarm-snooze').addEventListener('click', () => dismissAlarm(5));

    // Settings selects
    document.getElementById('set-sensitivity').addEventListener('change', (e) => {
      state.sensitivity = Number(e.target.value);
      localStorage.setItem('gold_sensitivity', String(state.sensitivity));
    });
    document.getElementById('set-cooldown').addEventListener('change', (e) => {
      state.cooldownMin = Number(e.target.value);
      localStorage.setItem('gold_cooldown', String(state.cooldownMin));
    });

    // Indicators search & category filter
    document.getElementById('ind-search').addEventListener('input', () => renderIndicatorsSheet());
    document.getElementById('ind-cats').addEventListener('click', (e) => {
      const btn = e.target.closest('.cat-btn');
      if (!btn) return;
      document.querySelectorAll('.cat-btn').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      renderIndicatorsSheet();
    });

    document.addEventListener('visibilitychange', () => {
      if (document.visibilityState === 'visible') updateWakeLock();
    });
  }

  function syncSettingsUI() {
    document.getElementById('set-sensitivity').value = String(state.sensitivity);
    document.getElementById('set-cooldown').value = String(state.cooldownMin);
  }

  function applyOverlayVisibility() {
    const gexCard = document.getElementById('gex-card');
    if (gexCard) gexCard.classList.toggle('hidden', !state.overlays.gexStrike);
    const rangeBtn = document.getElementById('btn-toggle-range-band');
    if (rangeBtn) rangeBtn.classList.toggle('active', state.overlays.expectedRange);
  }

  function renderIndicatorsSheet() {
    const list = document.getElementById('ind-list');
    const q = (document.getElementById('ind-search').value || '').toLowerCase();
    const activeCat = document.querySelector('#ind-cats .cat-btn.active')?.dataset.cat || 'all';

    const items = OVERLAY_META.filter(item => {
      const catOk = activeCat === 'all' || item.cat === activeCat;
      const textOk = item.label.toLowerCase().includes(q);
      return catOk && textOk;
    });

    list.innerHTML = items.map(item => `
      <label class="ind-row">
        <span>${item.label}</span>
        <input type="checkbox" data-ov-key="${item.key}" ${state.overlays[item.key] ? 'checked' : ''} />
      </label>
    `).join('');

    list.querySelectorAll('input[data-ov-key]').forEach(cb => {
      cb.addEventListener('change', (e) => {
        const k = e.target.dataset.ovKey;
        state.overlays[k] = e.target.checked;
        saveJSON('gold_overlays', state.overlays);
        applyOverlayVisibility();
        drawChartOverlays();
        evaluateSignals(true);
      });
    });
  }

  document.addEventListener('DOMContentLoaded', init);
})();
