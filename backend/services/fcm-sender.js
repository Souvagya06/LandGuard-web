const fs = require('fs');
const admin = require('firebase-admin');
const { toFcmData } = require('./alert-contract');

/**
 * Firebase Admin credentials are loaded from the environment only
 * (FIREBASE_SERVICE_ACCOUNT_BASE64 | _JSON | _PATH) and never leave the backend.
 */
let initError = null;

function serviceAccount() {
  let raw = (process.env.FIREBASE_SERVICE_ACCOUNT_BASE64 || '').trim();
  // If user copied 'FIREBASE_SERVICE_ACCOUNT_BASE64=...' into the value box, strip the prefix:
  if (raw.startsWith('FIREBASE_SERVICE_ACCOUNT_BASE64=')) {
    raw = raw.slice('FIREBASE_SERVICE_ACCOUNT_BASE64='.length).trim();
  }
  if (raw) {
    // If raw JSON was provided directly in the BASE64 variable, parse it directly:
    if (raw.startsWith('{') || (raw.startsWith("'") && raw.includes('"type"'))) {
      const stripped = raw.replace(/^'([\s\S]*)'$/, '$1').replace(/^"([\s\S]*)"$/, '$1').trim();
      return JSON.parse(stripped);
    }
    // Otherwise decode base64 (stripping any wrapper quotes or spaces):
    const clean = raw.replace(/^['"]|['"]$/g, '').replace(/\s+/g, '');
    const decoded = Buffer.from(clean, 'base64').toString('utf8').trim();
    return JSON.parse(decoded);
  }
  if (process.env.FIREBASE_SERVICE_ACCOUNT_JSON) {
    const jsonStr = process.env.FIREBASE_SERVICE_ACCOUNT_JSON.replace(/^'([\s\S]*)'$/, '$1').replace(/^"([\s\S]*)"$/, '$1').trim();
    return JSON.parse(jsonStr);
  }
  if (process.env.FIREBASE_SERVICE_ACCOUNT_PATH) {
    return JSON.parse(fs.readFileSync(process.env.FIREBASE_SERVICE_ACCOUNT_PATH, 'utf8'));
  }
  return null;
}

function firebaseMessaging() {
  if (!admin.apps.length) {
    let credentials;
    try {
      credentials = serviceAccount();
      initError = null;
    } catch (error) {
      initError = 'firebase_credentials_unreadable';
      console.error(`[FCM] Firebase service account could not be read: ${error.message}`);
      return null;
    }
    if (!credentials) return null;
    admin.initializeApp({ credential: admin.credential.cert(credentials) });
  }
  return admin.messaging();
}

/** Public-safe status: whether push delivery is configured, never the project details. */
function firebaseStatus() {
  const messaging = firebaseMessaging();
  if (messaging) return { configured: true };
  return { configured: false, reason: initError || 'firebase_not_configured' };
}

// Only codes that are specific to one token. Payload/credential errors
// (invalid-argument, mismatched-credential) must never prune every device.
const INVALID_TOKEN_CODES = new Set([
  'messaging/registration-token-not-registered',
  'messaging/invalid-registration-token',
]);
const TRANSIENT_CODES = new Set(['messaging/unavailable', 'messaging/internal-error', 'messaging/server-unavailable', 'messaging/quota-exceeded']);
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

/**
 * Sends one data message to many devices (500 per multicast), retries
 * transient failures once, and reports tokens FCM says are invalid so the
 * caller can prune them.
 */
async function multicast(devices, buildMessage) {
  const messaging = firebaseMessaging();
  if (!devices.length) return { status: 'not_sent', reason: 'no_registered_devices', attempted: 0, accepted: 0, failed: 0, invalidTokens: [], failures: {} };
  if (!messaging) return { status: 'not_sent', reason: initError || 'firebase_not_configured', attempted: devices.length, accepted: 0, failed: devices.length, invalidTokens: [], failures: {} };

  let accepted = 0;
  const invalidTokens = [];
  const failures = {};
  const sendBatch = async (tokens) => {
    const result = await messaging.sendEachForMulticast({ tokens, ...buildMessage() });
    const retry = [];
    result.responses.forEach((response, index) => {
      if (response.success) { accepted += 1; return; }
      const code = response.error?.code || 'unknown';
      if (INVALID_TOKEN_CODES.has(code)) { invalidTokens.push(tokens[index]); failures[code] = (failures[code] || 0) + 1; }
      else if (TRANSIENT_CODES.has(code)) retry.push({ token: tokens[index], code });
      else failures[code] = (failures[code] || 0) + 1;
    });
    return retry;
  };

  for (let offset = 0; offset < devices.length; offset += 500) {
    const tokens = devices.slice(offset, offset + 500).map((d) => d.token);
    const retry = await sendBatch(tokens);
    if (retry.length) {
      await sleep(2_000);
      const stillFailing = await sendBatch(retry.map((r) => r.token));
      stillFailing.forEach((r) => { failures[r.code] = (failures[r.code] || 0) + 1; });
    }
  }
  const failed = devices.length - accepted;
  return {
    status: accepted ? (accepted === devices.length ? 'sent' : 'partially_sent') : 'not_sent',
    reason: accepted ? undefined : 'all_deliveries_failed',
    attempted: devices.length,
    accepted,
    failed,
    invalidTokens,
    failures,
  };
}

/** Alert dispatch. Data-only + high priority, so Android always runs onMessageReceived. */
function sendFcmAlert(alert, devices) {
  const ttlMs = Math.max(60_000, Math.min(28 * 24 * 3600 * 1000, Date.parse(alert.expiresAt) - Date.now()));
  return multicast(devices, () => ({
    data: toFcmData(alert),
    android: { priority: 'high', ttl: ttlMs },
  }));
}

/** Status change (e.g. cancellation) for an alert devices already hold. */
function sendFcmAlertStatus(alert, devices) {
  return multicast(devices, () => ({
    data: { type: 'alert_status', alertId: alert.alertId, status: alert.status, schemaVersion: '1' },
    android: { priority: 'high', ttl: 24 * 3600 * 1000 },
  }));
}

/** Silent "please synchronise" hint, collapsed so devices only keep the latest one. */
function sendFcmSync(scope, devices) {
  return multicast(devices, () => ({
    data: { type: 'sync', scope, schemaVersion: '1' },
    android: { priority: 'normal', collapseKey: `landguard-sync-${scope}`, ttl: 6 * 3600 * 1000 },
  }));
}

module.exports = { sendFcmAlert, sendFcmAlertStatus, sendFcmSync, firebaseStatus };
