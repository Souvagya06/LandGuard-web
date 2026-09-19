const express = require('express');
const cors = require('cors');
const fs = require('fs');
const path = require('path');
require('dotenv').config({ path: path.join(__dirname, '.env') });
const http = require('http');
const WebSocket = require('ws');
const { evaluateTrainedRisk } = require('./services/risk-model');
const { loadMonitoringLocations } = require('./services/model-locations');
const { AlertStore } = require('./services/alert-store');
const { sendFcmAlert, sendFcmAlertStatus, sendFcmSync, firebaseStatus } = require('./services/fcm-sender');
const { RISK_BANDS } = require('./services/risk-config');
const { sign, requireRole, actor, hasRole, authRequired, login } = require('./services/auth');
const { securityHeaders, createRateLimiter, allowedOrigins } = require('./services/security');
const { MonitoringService } = require('./services/monitoring/monitoring-service');
const contract = require('./services/alert-contract');

const VERSION = '4.0.0';
const IS_PRODUCTION = process.env.NODE_ENV === 'production';
const PORT = Number(process.env.PORT || 8000);
const DATA_DIR = process.env.LANDGUARD_DATA_DIR ? path.resolve(process.env.LANDGUARD_DATA_DIR) : path.join(__dirname, 'data');
const FRONTEND_DIST_PATH = path.resolve(__dirname, '..', 'frontend', 'dist');
const SERVE_FRONTEND = process.env.SERVE_FRONTEND !== 'false' && fs.existsSync(FRONTEND_DIST_PATH);
const FEATURE_TABLE_PATH = path.resolve(__dirname, '..', 'ml', 'data', 'processed', 'feature_table.csv');

if (IS_PRODUCTION && (!process.env.AUTH_SECRET || process.env.AUTH_SECRET.length < 32)) {
  console.error('[LandGuard] AUTH_SECRET (≥32 characters) is required in production. Refusing to start.');
  process.exit(1);
}

const app = express();
app.disable('x-powered-by');
// Behind the HTTPS reverse proxy the client IP comes from X-Forwarded-For.
function trustProxySetting(value) {
  if (value === undefined || value === '') return IS_PRODUCTION ? 1 : false;
  if (value === 'true' || value === 'false') return value === 'true';
  return Number.isInteger(Number(value)) ? Number(value) : value;
}
app.set('trust proxy', trustProxySetting(process.env.TRUST_PROXY));
app.use(express.json({ limit: '15mb' }));
app.use(securityHeaders);
app.use(createRateLimiter({ windowMs: 60_000, max: Number(process.env.RATE_LIMIT_PER_MINUTE || 180) }));

const ORIGINS = allowedOrigins();
app.use(cors({
  origin(origin, callback) {
    // Non-browser clients (Android, curl) send no Origin header.
    callback(null, !origin || ORIGINS.has(origin));
  },
  methods: ['GET', 'POST', 'PUT', 'PATCH', 'DELETE', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Authorization'],
  maxAge: 600,
}));

const alertStore = new AlertStore(path.join(DATA_DIR, 'alerts.json'));
const monitoring = new MonitoringService({ dataDir: DATA_DIR, refreshMinutes: Number(process.env.MONITORING_REFRESH_MINUTES || 60) });

// ─────────────────────────────────────────────────────────────
// Realtime channel (WebSocket) — public, read-only broadcast
// ─────────────────────────────────────────────────────────────

let wss = null;

function initializeWebSocket(server) {
  wss = new WebSocket.Server({
    server,
    maxPayload: 16 * 1024,
    verifyClient: ({ origin }) => !origin || ORIGINS.has(origin),
  });

  wss.on('connection', (ws) => {
    ws.isAlive = true;
    ws.on('pong', () => { ws.isAlive = true; });
    ws.send(JSON.stringify({ type: 'connection', message: 'Connected to LandGuard realtime channel', clientId: `client-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`, timestamp: new Date().toISOString() }));
    ws.on('message', (data) => {
      try {
        if (JSON.parse(data)?.type === 'ping') ws.send(JSON.stringify({ type: 'pong', timestamp: new Date().toISOString() }));
      } catch { /* ignore malformed client frames */ }
    });
    ws.on('error', () => ws.terminate());
  });

  // Keep connections alive through proxies and drop dead ones.
  const heartbeat = setInterval(() => {
    wss.clients.forEach((ws) => {
      if (!ws.isAlive) return ws.terminate();
      ws.isAlive = false;
      ws.ping();
    });
  }, 30_000);
  heartbeat.unref?.();
}

function broadcast(type, data) {
  if (!wss) return 0;
  const frame = JSON.stringify({ type, data, timestamp: new Date().toISOString() });
  let sent = 0;
  wss.clients.forEach((client) => {
    if (client.readyState === WebSocket.OPEN) { client.send(frame); sent += 1; }
  });
  return sent;
}

// ─────────────────────────────────────────────────────────────
// Legacy model zones (What-If Studio). Terrain features are a curated static
// baseline; rainfall is live Open-Meteo. Nothing is overridden or simulated.
// ─────────────────────────────────────────────────────────────

const ZONE_SEEDS = loadMonitoringLocations(FEATURE_TABLE_PATH);

const weatherCache = new Map();
const WEATHER_TTL_MS = 10 * 60 * 1000;
let inFlightBatchPromise = null;

function getCacheKey(lat, lng) {
  return `${Number(lat).toFixed(3)},${Number(lng).toFixed(3)}`;
}

async function fetchLiveWeatherBatch(locations) {
  const now = Date.now();
  const results = new Map();
  const missing = [];

  for (const loc of locations) {
    const key = getCacheKey(loc.lat, loc.lng);
    const entry = weatherCache.get(key);
    if (entry && now - entry.timestamp < WEATHER_TTL_MS) {
      results.set(key, entry.data);
      continue;
    }
    missing.push(loc);
  }
  if (missing.length === 0) return results;

  if (inFlightBatchPromise) {
    await inFlightBatchPromise.catch(() => {});
    for (const loc of missing) {
      const key = getCacheKey(loc.lat, loc.lng);
      if (!weatherCache.has(key)) throw Object.assign(new Error('Live rainfall data is unavailable.'), { statusCode: 503 });
      results.set(key, weatherCache.get(key).data);
    }
    return results;
  }

  const params = new URLSearchParams({
    latitude: missing.map((m) => Number(m.lat).toFixed(4)).join(','),
    longitude: missing.map((m) => Number(m.lng).toFixed(4)).join(','),
    hourly: 'precipitation',
    past_hours: '168',
    forecast_hours: '1',
    timezone: 'UTC',
  });

  inFlightBatchPromise = (async () => {
    try {
      const res = await fetch(`https://api.open-meteo.com/v1/forecast?${params}`, { signal: AbortSignal.timeout(8000) });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const rawData = await res.json();
      const dataArray = Array.isArray(rawData) ? rawData : [rawData];
      missing.forEach((loc, idx) => {
        const key = getCacheKey(loc.lat, loc.lng);
        const data = dataArray[idx];
        if (!data?.hourly?.time || !data?.hourly?.precipitation) throw new Error(`Open-Meteo returned no hourly precipitation for ${loc.name || key}`);
        weatherCache.set(key, { timestamp: now, data });
        results.set(key, data);
      });
    } catch (err) {
      // Never invent rainfall: callers surface an explicit "unavailable" error.
      console.error(`[WeatherClient] Batch fetch error (${err.message}). Live rainfall is unavailable.`);
      throw Object.assign(new Error('Live rainfall data is unavailable.'), { statusCode: 503 });
    } finally {
      inFlightBatchPromise = null;
    }
  })();

  await inFlightBatchPromise;
  return results;
}

async function fetchLiveWeather(lat, lng) {
  const key = getCacheKey(lat, lng);
  const batch = await fetchLiveWeatherBatch([{ lat, lng }]);
  const data = batch.get(key);
  if (!data) throw Object.assign(new Error('Live rainfall data is unavailable for this coordinate.'), { statusCode: 503 });
  return data;
}

function computeRainfallFeatures(weatherData) {
  const precip = weatherData?.hourly?.precipitation || [];
  const timestamps = weatherData?.hourly?.time || [];
  if (!precip.length || precip.length !== timestamps.length) throw new Error('Live weather response contains no usable hourly precipitation series.');

  const now = Date.now();
  const observedIndex = timestamps.reduce((latest, timestamp, index) => {
    const time = Date.parse(`${timestamp}Z`);
    return Number.isFinite(time) && time <= now ? index : latest;
  }, -1);
  if (observedIndex < 23) throw new Error('Live weather response does not contain 24 completed hourly observations.');
  const endExclusive = observedIndex + 1;
  const trailing = (hours) => precip.slice(Math.max(0, endExclusive - hours), endExclusive);
  const sum = (values) => values.reduce((a, b) => a + (b || 0), 0);

  const rain1d = Math.round(sum(trailing(24)) * 10) / 10;
  const rain3dSum = Math.round(sum(trailing(72)) * 10) / 10;
  const rain7dSum = Math.round(sum(trailing(168)) * 10) / 10;

  const daily = [];
  const recentHourly = trailing(168);
  for (let i = 0; i < recentHourly.length; i += 24) daily.push(sum(recentHourly.slice(i, i + 24)));
  const recentDays = daily.slice(-7);
  const rainMax7d = Math.round((recentDays.length ? Math.max(...recentDays) : rain1d) * 10) / 10;

  let api7d = 0;
  [...recentDays].reverse().forEach((value, k) => { api7d += Math.pow(0.84, k + 1) * value; });
  api7d = Math.round(api7d * 10) / 10;

  // 14/30-day sums are model inputs extrapolated from the observed 7-day
  // series (the 168 h window is all Open-Meteo returns here); they are never
  // displayed as observations.
  return {
    rain_1d: rain1d,
    rain_3d_sum: rain3dSum,
    rain_7d_sum: rain7dSum,
    rain_14d_sum: Math.round(rain7dSum * 1.8 * 10) / 10,
    rain_30d_sum: Math.round(rain7dSum * 3.4 * 10) / 10,
    rain_max_7d: rainMax7d,
    api_7d: api7d,
  };
}

function computeFactors(zone, rainFeatures) {
  const rainFactor = Math.min(100, Math.round(rainFeatures.rain_1d * 1.2 + rainFeatures.rain_7d_sum * 0.15));
  const slopeFactor = Math.min(100, Math.round((zone.slope_deg / 45.0) * 85));
  const sarFactor = Math.min(100, Math.round(((zone.sar_disturbance || 0.4) / 1.2) * 80));
  const vegLossFactor = Math.min(100, Math.round((1.0 - Math.max(0, Math.min(1, zone.ndvi || 0.5))) * 70));
  return [
    { label: 'Rainfall (24h + 7d)', value: rainFactor },
    { label: 'Slope angle', value: slopeFactor },
    { label: 'Soil / SAR disturbance', value: Math.max(5, sarFactor) },
    { label: 'Vegetation loss', value: Math.max(5, vegLossFactor) },
  ];
}

function generateExplanation(zone, rainFeatures, mlResult) {
  const susc = Math.round(mlResult.susceptibility_score * 100);
  const trig = Math.round(mlResult.trigger_probability * 100);
  if (mlResult.risk_level === 'critical') return `CRITICAL: ${zone.name} (${zone.district}) model score ${mlResult.risk_score}%. Slope ${zone.slope_deg}°, ${susc}% susceptibility and ${rainFeatures.rain_1d}mm 24h rainfall give a ${trig}% trigger probability.`;
  if (mlResult.risk_level === 'high') return `HIGH: ${zone.name} model score ${mlResult.risk_score}%. ${rainFeatures.rain_1d}mm 24h rainfall on a ${zone.slope_deg}° gradient gives a ${trig}% trigger probability.`;
  if (mlResult.risk_level === 'moderate') return `MODERATE: ${zone.name} model score ${mlResult.risk_score}%. Baseline susceptibility ${susc}%, 24h rainfall ${rainFeatures.rain_1d}mm.`;
  return `LOW: ${zone.name} model score ${mlResult.risk_score}%. 24h rainfall ${rainFeatures.rain_1d}mm is within the model's low band.`;
}

async function evaluateZoneWithWeather(seed, weatherData) {
  const rainFeatures = computeRainfallFeatures(weatherData);
  const mlResult = await evaluateTrainedRisk({ ...seed, ...rainFeatures });
  return {
    id: seed.id,
    name: seed.name,
    district: seed.district,
    lat: seed.lat,
    lng: seed.lng,
    riskScore: mlResult.risk_score,
    riskLevel: mlResult.risk_level,
    susceptibilityScore: mlResult.susceptibility_score,
    triggerProbability: mlResult.trigger_probability,
    landslideProbability: Math.round(mlResult.trigger_probability * 100),
    rainfall24h: rainFeatures.rain_1d,
    rainfall7d: rainFeatures.rain_7d_sum,
    roadStatus: seed.roadStatus,
    factors: computeFactors(seed, rainFeatures),
    explanation: generateExplanation(seed, rainFeatures, mlResult),
    modelSource: mlResult.modelSource,
    modelVersion: mlResult.modelVersion,
    dataBasis: { rainfall: 'Open-Meteo (live)', terrain: 'Curated static baseline (not a live observation)' },
    coverageRadiusM: seed.coverageRadiusM,
    updatedAt: new Date().toISOString(),
  };
}

// ─────────────────────────────────────────────────────────────
// Alert dispatch
// ─────────────────────────────────────────────────────────────

/** Resolves an alert target from live monitoring first, then legacy model zones. Never needs a live weather call. */
function resolveZone(zoneId) {
  const zone = monitoring.zone(zoneId);
  if (zone) return { id: zone.id, name: zone.name, lat: zone.lat, lng: zone.lng, riskSnapshot: { score: zone.risk.score, level: zone.risk.level, coverage: zone.risk.coverage, conditionsObservedAt: zone.rainfall?.observedAt || null } };
  const seed = ZONE_SEEDS.find((s) => s.id === zoneId);
  if (seed) return { id: seed.id, name: seed.name, lat: seed.lat, lng: seed.lng, riskSnapshot: null };
  return null;
}

const actorRef = (a) => ({ id: a.sub, name: a.name || a.sub, role: a.role });

async function dispatchAlert(alert) {
  const active = alertStore.updateAlert(alert.alertId, { status: 'active', dispatchedAt: new Date().toISOString() });
  broadcast('alert', contract.toPublicAlert(active));

  const devices = alertStore.matchingDevices(active.zoneId);
  let fcm;
  try {
    fcm = await sendFcmAlert(active, devices);
  } catch (error) {
    console.error('[FCM] Delivery attempt failed:', error.message);
    fcm = { status: 'not_sent', reason: 'firebase_delivery_error', attempted: devices.length, accepted: 0, failed: devices.length, invalidTokens: [], failures: {} };
  }
  const invalidTokensRemoved = fcm.invalidTokens?.length ? alertStore.removeTokens(fcm.invalidTokens) : 0;
  const { invalidTokens, ...fcmSummary } = fcm;
  const updated = alertStore.updateAlert(active.alertId, {
    delivery: { status: fcm.status, attemptedAt: new Date().toISOString(), fcm: { ...fcmSummary, invalidTokensRemoved } },
  });
  console.log(`[Alert] ${updated.alertId} ${updated.level.toUpperCase()} ${updated.zoneName}: FCM ${fcm.status} (${fcm.accepted}/${fcm.attempted} accepted, ${invalidTokensRemoved} invalid tokens pruned)`);
  broadcast('alert.updated', contract.toPublicAlert(updated));
  return updated;
}

// Zone escalations push a silent sync hint to devices (at most every 30 minutes).
let lastMonitoringSyncAt = 0;
monitoring.on('updated', (summary) => broadcast('monitoring.updated', summary));
monitoring.on('escalated', async (escalations) => {
  broadcast('monitoring.escalated', escalations);
  if (Date.now() - lastMonitoringSyncAt < 30 * 60 * 1000) return;
  lastMonitoringSyncAt = Date.now();
  try {
    const result = await sendFcmSync('monitoring', alertStore.listDevices());
    if (result.invalidTokens?.length) alertStore.removeTokens(result.invalidTokens);
  } catch (error) {
    console.error('[FCM] Monitoring sync hint failed:', error.message);
  }
});

// ─────────────────────────────────────────────────────────────
// Validation helpers
// ─────────────────────────────────────────────────────────────

const inRange = (value, min, max) => Number.isFinite(Number(value)) && Number(value) >= min && Number(value) <= max;
const INSTALLATION_ID = /^[A-Za-z0-9-]{8,64}$/;
const badRequest = (res, detail) => res.status(400).json({ detail });

// ─────────────────────────────────────────────────────────────
// System & auth
// ─────────────────────────────────────────────────────────────

app.get('/health', (req, res) => {
  res.json({ status: 'ok', service: 'landguard-api', version: VERSION, time: new Date().toISOString() });
});

app.get('/system/status', (req, res) => {
  const devices = alertStore.listDevices();
  const weekAgo = Date.now() - 7 * 24 * 3600 * 1000;
  const alerts = alertStore.listAlerts();
  res.json({
    status: 'ok',
    version: VERSION,
    time: new Date().toISOString(),
    push: firebaseStatus(),
    realtime: { connectedClients: wss ? wss.clients.size : 0 },
    devices: {
      registered: devices.length,
      android: devices.filter((d) => (d.platform || 'android') === 'android').length,
      activeLast7Days: devices.filter((d) => Date.parse(d.lastSeenAt || d.updatedAt) >= weekAgo).length,
      lastRegistrationAt: devices.map((d) => d.updatedAt).sort().pop() || null,
    },
    alerts: {
      active: alerts.filter((a) => contract.effectiveStatus(a) === 'active').length,
      awaitingApproval: alerts.filter((a) => a.status === 'awaiting_approval').length,
      total: alerts.length,
    },
    monitoring: monitoring.summary(),
    riskBands: RISK_BANDS,
    security: { authenticationRequired: authRequired() },
  });
});

app.get('/auth/config', (req, res) => res.json({ authRequired: authRequired() }));

app.post('/auth/login', createRateLimiter({ windowMs: 60_000, max: 10 }), (req, res) => {
  const { username, password } = req.body || {};
  if (typeof username !== 'string' || typeof password !== 'string') return badRequest(res, 'username and password are required.');
  if (!process.env.AUTH_SECRET) return res.status(503).json({ detail: 'Authority sign-in is not configured on this server.' });
  const session = login(username, password);
  if (!session) return res.status(401).json({ detail: 'Invalid username or password.' });
  alertStore.audit({ action: 'auth.login', actor: { sub: session.user.username, name: session.user.name, role: session.user.role }, entityType: 'session', entityId: session.user.username });
  res.json(session);
});

app.get('/auth/session', actor, (req, res) => {
  res.json({ user: { username: req.actor.sub, name: req.actor.name || req.actor.sub, role: req.actor.role }, development: Boolean(req.actor.demo), expiresAt: req.actor.exp ? new Date(req.actor.exp * 1000).toISOString() : null });
});

// Development-only token minting; production uses /auth/login.
app.post('/auth/dev-token', (req, res) => {
  if (IS_PRODUCTION) return res.status(404).end();
  const role = ['viewer', 'field_officer', 'operator', 'incident_commander', 'admin'].includes(req.body?.role) ? req.body.role : 'admin';
  if (!process.env.AUTH_SECRET) return res.status(503).json({ detail: 'Set AUTH_SECRET to issue development tokens.' });
  const expiresInSeconds = 8 * 60 * 60;
  const token = sign({ sub: `dev-${role}`, name: `Development ${role}`, role, exp: Math.floor(Date.now() / 1000) + expiresInSeconds });
  res.json({ token, role, expiresInSeconds });
});

// ─────────────────────────────────────────────────────────────
// Regional monitoring — the shared source for web and Android
// ─────────────────────────────────────────────────────────────

app.get('/monitoring/summary', (req, res) => res.json(monitoring.summary()));

app.get('/monitoring/zones', (req, res) => {
  const summary = monitoring.summary();
  res.json({ generatedAt: summary.generatedAt, conditions: summary.conditions, catalog: summary.catalog, zones: monitoring.zones() });
});

app.get('/monitoring/zones/:id', (req, res) => {
  const zone = monitoring.zone(req.params.id);
  if (!zone) return res.status(404).json({ detail: 'Monitored area not found.' });
  res.json(zone);
});

app.get('/monitoring/catalog', (req, res) => {
  const catalog = monitoring.catalogPayload();
  if (!catalog) return res.status(503).json({ detail: 'Landslide catalog is not available yet.' });
  res.json(catalog);
});

app.get('/monitoring/analysis', createRateLimiter({ windowMs: 60_000, max: 20 }), async (req, res, next) => {
  try {
    const lat = Number(req.query.lat);
    const lng = Number(req.query.lng);
    // India envelope: the monitoring model and sources are validated for this region.
    if (!inRange(lat, 6, 37.5) || !inRange(lng, 68, 97.5)) return badRequest(res, 'lat/lng must be inside India.');
    res.json(await monitoring.analyze(lat, lng, { force: req.query.force === 'true' }));
  } catch (err) { next(err); }
});

app.post('/monitoring/refresh', requireRole('operator'), async (req, res, next) => {
  try {
    await monitoring.refresh({ force: req.body?.catalog === true });
    res.json(monitoring.summary());
  } catch (err) { next(err); }
});

// ─────────────────────────────────────────────────────────────
// Legacy model zones & What-If simulation
// ─────────────────────────────────────────────────────────────

app.get('/zones', async (req, res, next) => {
  try {
    const weatherMap = await fetchLiveWeatherBatch(ZONE_SEEDS);
    const zones = ZONE_SEEDS.map((seed) => {
      const weather = weatherMap.get(getCacheKey(seed.lat, seed.lng));
      if (!weather) throw Object.assign(new Error('Live rainfall data is unavailable.'), { statusCode: 503 });
      return evaluateZoneWithWeather(seed, weather);
    });
    res.json(await Promise.all(zones));
  } catch (err) { next(err); }
});

app.get('/zones/:id', async (req, res, next) => {
  try {
    const seed = ZONE_SEEDS.find((s) => s.id === req.params.id);
    if (!seed) return res.status(404).json({ detail: 'Zone not found' });
    res.json(await evaluateZoneWithWeather(seed, await fetchLiveWeather(seed.lat, seed.lng)));
  } catch (err) { next(err); }
});

app.post('/predict', createRateLimiter({ windowMs: 60_000, max: 30 }), async (req, res, next) => {
  try {
    const {
      lat = 27.5, lng = 93.8, elevation_m = 1000, slope_deg = 25, aspect_deg = 180, ndvi = 0.55, sar_disturbance = 0.4,
      rain_1d, rain_3d_sum, rain_7d_sum, rain_14d_sum, rain_30d_sum, rain_max_7d, api_7d, roadStatus = 'open',
    } = req.body || {};
    if (!inRange(lat, -90, 90) || !inRange(lng, -180, 180) || !inRange(slope_deg, 0, 90) || !inRange(ndvi, 0, 1)) {
      return badRequest(res, 'Coordinates, slope_deg, and ndvi are outside valid ranges.');
    }
    if (!['open', 'restricted', 'blocked'].includes(roadStatus)) return badRequest(res, 'Invalid roadStatus.');
    let rainFeatures;
    if (rain_1d !== undefined) {
      const r1 = Number(rain_1d) || 0;
      rainFeatures = {
        rain_1d: r1,
        rain_3d_sum: Number(rain_3d_sum) || r1 * 2.2,
        rain_7d_sum: Number(rain_7d_sum) || r1 * 3.5,
        rain_14d_sum: Number(rain_14d_sum) || r1 * 5.0,
        rain_30d_sum: Number(rain_30d_sum) || r1 * 8.0,
        rain_max_7d: Number(rain_max_7d) || r1,
        api_7d: Number(api_7d) || r1 * 0.8,
      };
    } else {
      rainFeatures = computeRainfallFeatures(await fetchLiveWeather(Number(lat), Number(lng)));
    }
    const mlPayload = {
      lat: Number(lat), lng: Number(lng), elevation_m: Number(elevation_m), slope_deg: Number(slope_deg), aspect_deg: Number(aspect_deg),
      ndvi: Number(ndvi), sar_disturbance: Number(sar_disturbance), roadStatus, ...rainFeatures,
    };
    const mlResult = await evaluateTrainedRisk(mlPayload);
    const target = { name: 'Target Coordinate', district: `${Number(lat).toFixed(3)}°N, ${Number(lng).toFixed(3)}°E`, ...mlPayload };
    res.json({
      lat: Number(lat), lng: Number(lng), ...mlResult,
      rainfall24h: rainFeatures.rain_1d, rainfall7d: rainFeatures.rain_7d_sum,
      factors: computeFactors(target, rainFeatures), explanation: generateExplanation(target, rainFeatures, mlResult),
      simulated: rain_1d !== undefined, timestamp: new Date().toISOString(),
    });
  } catch (err) { next(err); }
});

// ─────────────────────────────────────────────────────────────
// Alerts — one lifecycle for web, Android and the mesh
// ─────────────────────────────────────────────────────────────

function listPublicAlerts(req) {
  const since = req.query.updatedSince ? Date.parse(String(req.query.updatedSince)) : NaN;
  const limit = Math.min(500, Math.max(1, Number(req.query.limit) || 100));
  const now = Date.now();
  return alertStore.listAlerts()
    .filter((a) => a.status !== 'awaiting_approval')
    .filter((a) => !Number.isFinite(since) || Date.parse(a.updatedAt) >= since)
    .map((a) => contract.toPublicAlert(a, now))
    .filter((a) => req.query.active !== 'true' || a.status === 'active')
    .slice(0, limit);
}

app.get('/alerts', (req, res, next) => {
  if (SERVE_FRONTEND && req.headers.accept?.includes('text/html')) return res.sendFile(path.join(FRONTEND_DIST_PATH, 'index.html'));
  if (req.query.view === 'authority') {
    return requireRole('operator')(req, res, () => {
      const limit = Math.min(500, Math.max(1, Number(req.query.limit) || 200));
      res.json(alertStore.listAlerts().slice(0, limit).map((a) => contract.toAuthorityAlert(a)));
    });
  }
  try { res.json(listPublicAlerts(req)); } catch (err) { next(err); }
});

app.get('/alerts/:id', (req, res) => {
  const alert = alertStore.getAlert(req.params.id);
  if (!alert || alert.status === 'awaiting_approval') return res.status(404).json({ detail: 'Alert not found.' });
  res.json(contract.toPublicAlert(alert));
});

app.post('/alerts', requireRole('operator'), async (req, res, next) => {
  try {
    const { zoneId, level, message, channel = 'push', expiresInMinutes, origin, clientRequestId } = req.body || {};
    if (typeof zoneId !== 'string' || !zoneId) return badRequest(res, 'zoneId is required.');
    const zone = resolveZone(zoneId);
    if (!zone) return res.status(404).json({ detail: 'Zone not found.' });

    const normalizedLevel = level === 'casual' ? 'low' : level || zone.riskSnapshot?.level;
    if (!contract.LEVELS.includes(normalizedLevel)) return badRequest(res, 'level must be low, moderate, high, or critical.');
    if (message !== undefined && (typeof message !== 'string' || !message.trim() || message.length > 500)) return badRequest(res, 'message must contain 1 to 500 characters.');
    const expiry = expiresInMinutes === undefined ? contract.DEFAULT_EXPIRY_MINUTES : Number(expiresInMinutes);
    if (!inRange(expiry, contract.MIN_EXPIRY_MINUTES, contract.MAX_EXPIRY_MINUTES)) return badRequest(res, `expiresInMinutes must be between ${contract.MIN_EXPIRY_MINUTES} and ${contract.MAX_EXPIRY_MINUTES}.`);
    if (clientRequestId !== undefined && (typeof clientRequestId !== 'string' || clientRequestId.length > 64)) return badRequest(res, 'clientRequestId must be a string of at most 64 characters.');

    const duplicate = alertStore.findRecentByClientRequestId(clientRequestId);
    if (duplicate) return res.status(200).json(contract.toAuthorityAlert(duplicate));

    // Two-person rule: high/critical alerts from operators need an incident commander.
    const requiresApproval = ['high', 'critical'].includes(normalizedLevel) && !hasRole(req.actor.role, 'incident_commander');
    const createdBy = actorRef(req.actor);
    const alert = alertStore.createAlert({
      zoneId: zone.id,
      zoneName: zone.name,
      lat: zone.lat,
      lng: zone.lng,
      level: normalizedLevel,
      message: message?.trim() || `${normalizedLevel.toUpperCase()} LANDSLIDE ALERT: ${zone.name}. Follow instructions from local authorities and avoid steep slopes and road cuts.`,
      channel,
      source: 'authority',
      origin: contract.ORIGINS.includes(origin) ? origin : 'api',
      status: requiresApproval ? 'awaiting_approval' : 'active',
      expiresInMinutes: Math.round(expiry),
      clientRequestId,
      createdBy,
      approvedBy: requiresApproval ? null : createdBy,
      approvedAt: requiresApproval ? null : new Date().toISOString(),
      riskSnapshot: zone.riskSnapshot,
    });
    alertStore.audit({ action: 'alert.created', actor: req.actor, entityType: 'alert', entityId: alert.alertId, metadata: { level: alert.level, status: alert.status } });
    if (requiresApproval) {
      broadcast('alert.pending', { alertId: alert.alertId });
      return res.status(201).json(contract.toAuthorityAlert(alert));
    }
    const dispatched = await dispatchAlert(alert);
    alertStore.audit({ action: 'alert.dispatched', actor: req.actor, entityType: 'alert', entityId: alert.alertId, metadata: { delivery: dispatched.delivery.status } });
    res.status(201).json(contract.toAuthorityAlert(dispatched));
  } catch (err) { next(err); }
});

app.post('/alerts/:id/approve', requireRole('incident_commander'), async (req, res, next) => {
  try {
    const alert = alertStore.getAlert(req.params.id);
    if (!alert) return res.status(404).json({ detail: 'Alert not found.' });
    if (alert.status !== 'awaiting_approval') return res.status(409).json({ detail: 'Alert is not awaiting approval.' });
    const approved = alertStore.updateAlert(alert.alertId, { approvedAt: new Date().toISOString(), approvedBy: actorRef(req.actor) });
    alertStore.audit({ action: 'alert.approved', actor: req.actor, entityType: 'alert', entityId: alert.alertId });
    const dispatched = await dispatchAlert(approved);
    alertStore.audit({ action: 'alert.dispatched', actor: req.actor, entityType: 'alert', entityId: alert.alertId, metadata: { delivery: dispatched.delivery.status } });
    res.json(contract.toAuthorityAlert(dispatched));
  } catch (err) { next(err); }
});

app.post('/alerts/:id/cancel', requireRole('operator'), async (req, res, next) => {
  try {
    const alert = alertStore.getAlert(req.params.id);
    if (!alert) return res.status(404).json({ detail: 'Alert not found.' });
    if (!['awaiting_approval', 'active'].includes(contract.effectiveStatus(alert))) return res.status(409).json({ detail: 'Only pending or active alerts can be cancelled.' });
    const wasPublic = alert.status === 'active';
    const cancelled = alertStore.updateAlert(alert.alertId, { status: 'cancelled', cancelledAt: new Date().toISOString(), cancelledBy: actorRef(req.actor) });
    alertStore.audit({ action: 'alert.cancelled', actor: req.actor, entityType: 'alert', entityId: alert.alertId });
    if (wasPublic) {
      broadcast('alert.updated', contract.toPublicAlert(cancelled));
      sendFcmAlertStatus(cancelled, alertStore.matchingDevices(cancelled.zoneId))
        .then((r) => { if (r.invalidTokens?.length) alertStore.removeTokens(r.invalidTokens); })
        .catch((error) => console.error('[FCM] Cancellation notice failed:', error.message));
    }
    res.json(contract.toAuthorityAlert(cancelled));
  } catch (err) { next(err); }
});

// Device-side delivery confirmation (FCM, sync or offline mesh).
app.post('/alerts/:id/receipts', createRateLimiter({ windowMs: 60_000, max: 120 }), (req, res) => {
  const { installationId, event = 'received', via = 'fcm', hopCount = 0 } = req.body || {};
  if (typeof installationId !== 'string' || !INSTALLATION_ID.test(installationId)) return badRequest(res, 'installationId is required.');
  if (!['received', 'opened', 'acknowledged'].includes(event)) return badRequest(res, 'event must be received, opened or acknowledged.');
  if (!['fcm', 'sync', 'mesh'].includes(via)) return badRequest(res, 'via must be fcm, sync or mesh.');
  if (!inRange(hopCount, 0, contract.MAX_MESH_HOPS)) return badRequest(res, 'hopCount is out of range.');
  const alert = alertStore.getAlert(req.params.id);
  if (!alert || alert.status === 'awaiting_approval') return res.status(404).json({ detail: 'Alert not found.' });
  const updated = alertStore.recordReceipt(alert.alertId, { installationId, event, via, hopCount: Number(hopCount) });
  alertStore.touchDevice(installationId);
  broadcast('alert.receipt', { alertId: alert.alertId, receipts: contract.toAuthorityAlert(updated).receipts });
  res.status(202).json({ ok: true });
});

// ─────────────────────────────────────────────────────────────
// Devices — public registration (the FCM token is the credential), rate limited
// ─────────────────────────────────────────────────────────────

app.post(['/devices', '/api/devices/register'], createRateLimiter({ windowMs: 60_000, max: 30 }), (req, res) => {
  const { token, platform = 'android', zoneIds, appVersion, installationId } = req.body || {};
  if (typeof token !== 'string' || token.trim().length < 20 || token.length > 4096) return badRequest(res, 'A valid FCM registration token is required.');
  if (!['android', 'ios', 'web'].includes(platform)) return badRequest(res, 'platform must be android, ios or web.');
  if (zoneIds !== undefined && (!Array.isArray(zoneIds) || zoneIds.length > 500 || !zoneIds.every((id) => typeof id === 'string' && id.length <= 100))) return badRequest(res, 'zoneIds must be an array of zone identifiers.');
  if (installationId !== undefined && (typeof installationId !== 'string' || !INSTALLATION_ID.test(installationId))) return badRequest(res, 'installationId is malformed.');
  if (appVersion !== undefined && (typeof appVersion !== 'string' || appVersion.length > 40)) return badRequest(res, 'appVersion is malformed.');
  const device = alertStore.registerDevice({ token: token.trim(), platform, zoneIds: zoneIds || [], appVersion, installationId });
  res.status(201).json({ id: device.id, ok: true, registeredAt: device.updatedAt });
});

app.get(['/devices', '/api/devices'], requireRole('operator'), (req, res) => {
  // Tokens are secrets: only aggregate and non-identifying fields are exposed.
  const devices = alertStore.listDevices();
  const weekAgo = Date.now() - 7 * 24 * 3600 * 1000;
  res.json({
    registeredDevices: devices.length,
    androidDevices: devices.filter((d) => (d.platform || 'android') === 'android').length,
    activeLast7Days: devices.filter((d) => Date.parse(d.lastSeenAt || d.updatedAt) >= weekAgo).length,
    devices: devices.map((d) => ({ id: d.id, platform: d.platform, appVersion: d.appVersion, registeredAt: d.createdAt, lastSeenAt: d.lastSeenAt || d.updatedAt, zoneSubscriptions: d.zoneIds?.length || 0 })),
  });
});

// ─────────────────────────────────────────────────────────────
// Field reports & audit
// ─────────────────────────────────────────────────────────────

app.get('/reports', (req, res) => {
  if (SERVE_FRONTEND && req.headers.accept?.includes('text/html')) return res.sendFile(path.join(FRONTEND_DIST_PATH, 'index.html'));
  res.json(alertStore.listReports());
});

app.post('/reports', requireRole('field_officer'), (req, res) => {
  const { zoneId, zoneName, note, photoDataUrl, lat, lng } = req.body || {};
  if (typeof note !== 'string' || note.trim().length < 10 || note.length > 2_000) return badRequest(res, 'note must contain 10 to 2,000 characters.');
  if (!inRange(lat, -90, 90) || !inRange(lng, -180, 180)) return badRequest(res, 'A valid latitude and longitude are required.');
  if (photoDataUrl !== undefined && (typeof photoDataUrl !== 'string' || !/^data:image\/(jpeg|png|webp);base64,/.test(photoDataUrl) || photoDataUrl.length > 10_000_000)) {
    return badRequest(res, 'photoDataUrl must be a JPEG, PNG, or WEBP data URL below 7.5MB.');
  }
  const report = alertStore.createReport({
    zoneId: typeof zoneId === 'string' ? zoneId.slice(0, 100) : 'custom',
    zoneName: typeof zoneName === 'string' ? zoneName.slice(0, 200) : 'Field Location',
    note: note.trim(), photoDataUrl, lat: Number(lat), lng: Number(lng),
    submittedBy: actorRef(req.actor),
  });
  alertStore.audit({ action: 'report.created', actor: req.actor, entityType: 'report', entityId: report.id, metadata: { zoneId: report.zoneId } });
  broadcast('report.created', { id: report.id });
  res.status(201).json({ ok: true, report });
});

app.post('/reports/:id/verify', requireRole('operator'), (req, res) => {
  const verdict = req.body?.verdict;
  if (!['verified', 'rejected'].includes(verdict)) return badRequest(res, 'verdict must be verified or rejected.');
  const report = alertStore.updateReport(req.params.id, { status: verdict, verification: { verdict, note: typeof req.body?.note === 'string' ? req.body.note.slice(0, 500) : '', verifiedAt: new Date().toISOString(), verifiedBy: actorRef(req.actor) } });
  if (!report) return res.status(404).json({ detail: 'Report not found.' });
  alertStore.audit({ action: `report.${verdict}`, actor: req.actor, entityType: 'report', entityId: report.id });
  res.json(report);
});

app.delete('/reports/:id', requireRole('field_officer'), (req, res) => {
  const removed = alertStore.deleteReport(req.params.id);
  if (!removed) return res.status(404).json({ detail: 'Report not found.' });
  alertStore.audit({ action: 'report.deleted', actor: req.actor, entityType: 'report', entityId: req.params.id });
  broadcast('report.deleted', { id: req.params.id });
  res.json({ ok: true, report: removed });
});

app.get('/audit-events', requireRole('operator'), (req, res) => {
  res.json(alertStore.listAuditEvents().slice(0, Math.min(500, Number(req.query.limit) || 100)));
});

// ─────────────────────────────────────────────────────────────
// Optional: serve the built web app from this process (local / single host).
// In production landguard.online is served by the reverse proxy instead.
// ─────────────────────────────────────────────────────────────

if (SERVE_FRONTEND) {
  app.use(express.static(FRONTEND_DIST_PATH, { index: 'index.html' }));
  const pages = ['/dashboard', '/send-alert', '/simulate', '/system', '/login', '/dashboard.html', '/send-alert.html', '/simulate.html', '/reports.html', '/alerts.html'];
  app.get(pages, (req, res) => res.sendFile(path.join(FRONTEND_DIST_PATH, 'index.html')));
}

app.use((req, res) => res.status(404).json({ detail: 'Not found.' }));

// Global error handler — never leaks internals in production.
app.use((err, req, res, next) => {
  const status = err.statusCode || err.status || 500;
  if (status >= 500) console.error('[Server Error]', err);
  const detail = status >= 500 && IS_PRODUCTION && !err.statusCode ? 'Internal server error' : err.message || 'Internal server error';
  res.status(status).json({ detail });
});

const server = http.createServer(app);
initializeWebSocket(server);

server.listen(PORT, () => {
  console.log(`[LandGuard] API ${VERSION} listening on port ${PORT} (${IS_PRODUCTION ? 'production' : 'development'})`);
  console.log(`[LandGuard] CORS origins: ${[...ORIGINS].join(', ') || '(none — browser access disabled)'}`);
  console.log(`[LandGuard] Push delivery: ${firebaseStatus().configured ? 'Firebase configured' : 'NOT configured'}`);
  // MONITORING_DISABLED serves only previously cached data (tests / offline hosts).
  if (process.env.MONITORING_DISABLED === 'true') monitoring.loadDiskCache();
  else monitoring.start();
});

module.exports = { app, server };
