# LandGuard AI

> **AI-Assisted Landslide Early-Warning, Monitoring & Resilient Alert Distribution Platform for Northeast India**

LandGuard AI is an integrated landslide risk monitoring and emergency response system engineered for the complex, cloud-prone terrain of Northeast India. By fusing satellite earth observation (Sentinel-1 SAR, Sentinel-2 Optical, SRTM DEM), live Open-Meteo hydrometeorological telemetry, and NASA Global Landslide Catalog historical records, LandGuard delivers continuous hazard assessment, automated risk quantification, and mission-critical alert delivery that bridges internet outages via peer-to-peer mobile mesh networks.

- **Authority Web Console:** [https://landguard.online](https://landguard.online)
- **Production API:** [https://api.landguard.online](https://api.landguard.online)
- **Architecture & Deployment Guide:** [docs/production.md](docs/production.md)

---

## System Architecture

```text
┌────────────────────────────────────────────────────────┐
│              Authority Control Center                  │
│       React 19 · TypeScript · Vite · Leaflet           │
└──────────────────────────┬─────────────────────────────┘
                           │ HTTPS / WebSocket (Broadcasts)
                           ▼
┌────────────────────────────────────────────────────────┐
│                   LandGuard API                        │
│            Node.js · Express · WebSocket               │
│  ├─ Monitoring Service (139 Areas, NASA GLC, Weather)  │
│  ├─ Alert Engine (Store, Two-Person Rule, Lifecycles)  │
│  ├─ Firebase Admin SDK (High-Priority FCM Multicast)   │
│  └─ Telemetry & Field Reports (Receipts, Photos)       │
└────────────┬─────────────────────────────┬─────────────┘
             │ FCM Push (Online Devices)   │ REST API Sync
             ▼                             ▼
┌────────────────────────────────────────────────────────┐
│               LandGuard Android App                    │
│      Kotlin · Jetpack Compose · Room · Nearby API      │
└──────────────────────────┬─────────────────────────────┘
                           │ P2P Nearby Connections
                           │ (Store-and-Forward Mesh, ≤ 6 hops)
                           ▼
┌────────────────────────────────────────────────────────┐
│         Offline Field Devices (No Internet)            │
│  • Receive, display & relay critical warnings          │
│  • Queue delivery receipts until connection restored   │
└────────────────────────────────────────────────────────┘
```

---

## Key Features

### 1. Regional Monitoring Engine (Northeast India)
- **139 Real-World Hazard Hotspots:** Clustered from the NASA Global Landslide Catalog across Sikkim, Assam, Arunachal Pradesh, Meghalaya, Manipur, Mizoram, Nagaland, and Tripura.
- **Continuous Hydrometeorological Fusion:** Live 72-hour precipitation accumulation and 24-hour forecast from Open-Meteo paired with cached SRTM elevation and slope models.
- **Automated Weather API Fallback:** Automatic failover to OpenWeatherMap (OWM) 5-day forecast when Open-Meteo encounters daily cloud IP rate limits (HTTP 429), preserving live 24-hour forecast telemetry without interruption.
- **Bundled Baseline Seed Cache:** Embedded seed dataset (`seed-cache.json`) ensuring fresh cloud deployments instantly boot with complete Copernicus DEM elevation, slope, and catalog records with 100% data coverage.
- **Deterministic Risk Scoring:** Weighted index calculating continuous risk scores (0–100) mapped to four standardized bands: **Low**, **Moderate**, **High**, and **Critical**.
- **Data Integrity & Honesty:** Missing readings are explicitly labeled as unavailable—no synthetic or hallucinated telemetry is ever fed to operators.

### 2. Authority Control Center
- **Interactive Risk Map:** Multi-layer GIS interface displaying monitored areas with severity-coded markers, search filtering, satellite/terrain base styles, and factor breakdowns.
- **Send Alert Console:**
  - Fast, keyboard-accessible area search (fuzzy matching name, state, and coordinates).
  - Four risk levels with standardized guidance templates.
  - Configurable expiration (1 hour to 72 hours).
  - Real-time Android notification preview.
- **Two-Person Rule & RBAC:**
  - Standard operators creating High or Critical alerts place them in `awaiting_approval` status.
  - Incident Commanders or Administrators verify and authorize dispatch.
  - Full audit logging with actor attribution.
- **Delivery Telemetry & Receipts:** Real-time visibility into FCM acceptance, device sync confirmations, and offline mesh hops.
- **What-If Simulation Studio:** Interactive parameter testing (slope, NDVI, SAR disturbance, rainfall windows) for hazard drills and contingency planning without affecting live operations.
- **Citizen & Field Reports:** Geotagged observation reporting with photographic evidence, status tracking (`submitted` → `verified`), and operator verification.
- **System Observability:** Real-time WebSocket connection state, live freshness indicator (`LIVE`, `UPDATED`, `CACHED`, `OFFLINE`), FCM status, and device registry counters.

### 3. Resilient Alert Distribution & Offline Mesh
- **Data-Only High-Priority FCM:** Backend initiates instant multicast push with automatic TTL enforcement matching alert expiry.
- **Android Nearby Connections Mesh:** Phones receiving alerts relay them locally to nearby devices via BLE and Wi-Fi Direct without cellular or internet connectivity (up to 6 hops).
- **Store-and-Forward Receipts:** Offline devices store alert receipts locally and flush delivery confirmations back to the API once connectivity is re-established.

### 4. Cloud Resiliency & Fault-Tolerant Telemetry
- **Dual-Provider Weather Telemetry:** Seamless primary-to-fallback pipeline (Open-Meteo → OpenWeatherMap) with spatial grid aggregation (~0.1° resolution) to minimize API consumption while guaranteeing live 24-hour rainfall forecast feeds.
- **Zero-Cold-Start Seed Architecture:** Eliminates data unavailability during cold container spins by shipping precomputed Copernicus GLO-90 DEM terrain models and NASA GLC clusters within `seed-cache.json`.
- **Adaptive Credential Parser:** Resilient Firebase Admin SDK credential loader that auto-detects and parses raw JSON, quoted Base64, or Base64 formats while stripping common copy-paste formatting anomalies (wrapper quotes, variable prefixes).

---

## Unified Alert Contract

The alert payload structure is standardized across REST endpoints, WebSocket frames, FCM data messages, SQLite/Room databases, and the offline mesh protocol (`backend/services/alert-contract.js`):

| Property | Type | Description |
|---|---|---|
| `alertId` | `string` (UUID) | Unique identifier; permanently preserved across all mesh relays |
| `zoneId` | `string` | Monitored area ID (e.g., `glc_2729_8824`) |
| `zoneName` | `string` | Human-readable location name (e.g., `Near Tista`) |
| `state` | `string` | Indian state (e.g., `Sikkim`) |
| `lat`, `lng` | `number` | Geographical centroid |
| `level` / `severity` | `string` | `low` · `moderate` · `high` · `critical` (normalized upper/lowercase) |
| `message` | `string` | Operational directive (1–500 characters) |
| `timestamp` | `string` (ISO) | Alert creation time in UTC |
| `expiresAt` | `string` (ISO) | UTC expiry timestamp after which devices cease alerts and relay |
| `source` | `string` | `authority` or `risk_engine` |
| `status` | `string` | `awaiting_approval` · `active` · `cancelled` · `expired` |
| `origin` | `string` | `authority_web` · `api` |
| `hopCount` | `number` | `0` at initial dispatch; incremented by `+1` on each peer-to-peer relay |

---

## Technology Stack

### Web & Backend
- **Frontend:** React 19, TypeScript, Vite, Tailwind CSS, TanStack Query, Leaflet, Lucide Icons
- **Backend:** Node.js, Express, WebSocket (`ws`), SQLite/JSON `AlertStore`
- **Push Delivery:** Firebase Admin SDK (FCM HTTP v1 data messaging)
- **Authentication:** JWT, bcrypt, role-based middleware (`incident_commander`, `operator`, `admin`)
- **Reverse Proxy & TLS:** Caddy with automatic Let's Encrypt certificates

### Android Application
- **Language & Framework:** Kotlin, Jetpack Compose, Coroutines, Flow
- **Local Persistence:** Room Database
- **Mesh Networking:** Google Play Services Nearby Connections API
- **Push Notifications:** Firebase Cloud Messaging (`FirebaseMessagingService`)

### Machine Learning & Offline Geospatial Pipeline
- **Languages & Tools:** Python 3.10+, pandas, NumPy, scikit-learn (`HistGradientBoostingClassifier`)
- **Geospatial Processing:** rasterio, shapely, Google Earth Engine, Microsoft Planetary Computer STAC
- **Data Sources:** NASA Global Landslide Catalog, SRTM 30m DEM, Sentinel-1 SAR, Sentinel-2 L2A, Open-Meteo, OpenWeatherMap (fallback)

---

## Repository Layout

```text
landguard-ai/
├── backend/
│   ├── server.js                      # Express API, WebSocket server & routes
│   ├── Dockerfile                     # Container definition for API service
│   ├── scripts/
│   │   └── hash-password.js           # Password hash generator for authority accounts
│   ├── services/
│   │   ├── alert-contract.js          # Shared validation & contract schemas
│   │   ├── alert-store.js             # Alert persistence & delivery tracking
│   │   ├── auth.js                    # JWT issuance, verification & RBAC
│   │   ├── fcm-sender.js              # FCM push notification dispatcher
│   │   ├── risk-model.js              # Portable JavaScript risk scoring logic
│   │   ├── security.js                # Security headers, rate limiting & CORS
│   │   └── monitoring/
│   │       ├── analytics.js           # GLC spatial clustering & risk index
│   │       ├── monitoring-service.js  # 139 areas management & background sync
│   │       ├── seed-cache.json        # Bundled baseline terrain & catalog cache
│   │       └── sources.js             # Bounding boxes, GLC cache & weather endpoints
│   └── test/                          # Comprehensive Node test suite
├── frontend/
│   ├── src/
│   │   ├── components/                # UI design system, RiskMap, DeliveryPanel, ZoneDetail
│   │   ├── pages/                     # Dashboard, SendAlert, Alerts, FieldReports, SimulationStudio, System, Login
│   │   ├── lib/                       # API clients, TanStack queries, WebSocket realtime client
│   │   └── types.ts                   # TypeScript contracts and interfaces
│   └── vite.config.ts                 # Vite bundler configuration
├── deploy/
│   ├── Caddyfile                      # Production reverse-proxy & SSL configuration
│   ├── docker-compose.prod.yml        # Production multi-service orchestration
│   ├── web.Dockerfile                 # Static web production container
│   └── .env.production.example        # Environment variable template
├── ml/
│   ├── data/                          # Raw and processed feature datasets
│   ├── models/                        # Serialized ML model artifacts
│   └── scripts/                       # Terrain, NDVI, SAR and catalog extraction scripts
└── docs/
    └── production.md                  # Complete production & operational guide
```

---

## API Reference

### System & Authentication
| Method | Endpoint | Access | Description |
|---|---|---|---|
| `GET` | `/health` | Public | Liveness check |
| `GET` | `/system/status` | Public | Node health, push status, registered device counts, cache age |
| `GET` | `/auth/config` | Public | Authority authentication settings |
| `POST` | `/auth/login` | Public | Authenticate operator or incident commander |
| `GET` | `/auth/session` | Bearer | Validate session token and retrieve role profile |

### Regional Monitoring
| Method | Endpoint | Access | Description |
|---|---|---|---|
| `GET` | `/monitoring/summary` | Public | Regional overview, zone counts, risk level breakdowns |
| `GET` | `/monitoring/zones` | Public | All 139 monitored areas with live weather and risk scores |
| `GET` | `/monitoring/zones/:id` | Public | Detailed telemetry and historical events for one area |
| `GET` | `/monitoring/catalog` | Public | NASA GLC catalog events and metadata |
| `GET` | `/monitoring/analysis` | Rate-limited | On-demand coordinates point analysis |
| `POST` | `/monitoring/refresh` | Operator | Force refresh of weather and catalog data |

### Alert Lifecycle & Devices
| Method | Endpoint | Access | Description |
|---|---|---|---|
| `GET` | `/alerts` | Public | Public active alerts (Android synchronization) |
| `GET` | `/alerts?view=authority` | Operator | Full alert history with delivery and receipt telemetry |
| `POST` | `/alerts` | Operator | Create alert (triggers two-person rule if High/Critical) |
| `POST` | `/alerts/:id/approve` | Commander | Approve pending alert and dispatch over FCM |
| `POST` | `/alerts/:id/cancel` | Operator | Cancel an active alert across all channels |
| `POST` | `/alerts/:id/receipts` | Device | Record device receipt (FCM, direct sync, or mesh hop count) |
| `POST` | `/devices` | Device | Register FCM token and device model |
| `GET` | `/devices` | Operator | List registered devices (sanitized without tokens) |

### WebSocket Realtime Channel
- **Endpoint:** `wss://api.landguard.online/` (or `ws://localhost:8000/`)
- **Frames:**
  - `alert`: New alert published
  - `alert.updated`: Status change, cancellation, or approval
  - `alert.receipt`: Device delivery confirmation
  - `monitoring.updated`: Weather or risk index recalculation

---

## Quick Start (Local Development)

### Prerequisites
- Node.js 18+ and npm
- (Optional) Docker for containerized run

### 1. Start Backend API
```bash
cd backend
npm install
npm run dev
```
The API server starts at `http://127.0.0.1:8000`.

To run the automated test suite:
```bash
npm test
```

### 2. Start Frontend Web Console
```bash
cd frontend
npm install
npm run dev
```
The development server launches at `http://localhost:5173`.

### 3. Build & Serve Through Backend
To test the production build served directly from the Node.js process:
```bash
cd frontend
npm run build

cd ../backend
npm start
```
Access the console at `http://127.0.0.1:8000`.

---

## Production Deployment

Production orchestration is configured via Docker Compose and Caddy with automatic TLS management.

1. **Configure DNS:** Point `landguard.online` and `api.landguard.online` to your host IP.
2. **Environment Configuration:**
   ```bash
   cp deploy/.env.production.example deploy/.env.production
   ```
   Set `ACME_EMAIL`, `AUTH_SECRET` (≥32 chars), and `FIREBASE_SERVICE_ACCOUNT_BASE64`.
3. **Provision Authority Users:**
   ```bash
   docker compose -f deploy/docker-compose.prod.yml run --rm -it api node backend/scripts/hash-password.js <username> incident_commander "<Full Name>"
   ```
   Copy the output object into `AUTHORITY_USERS` in `deploy/.env.production`.
4. **Deploy Containers:**
   ```bash
   docker compose -f deploy/docker-compose.prod.yml --env-file deploy/.env.production up -d --build
   ```

For comprehensive operational and troubleshooting details, consult [docs/production.md](docs/production.md).

---

## License

This project is developed for academic, disaster-management research, and hackathon demonstration purposes. Ensure compliance with regional data governance and disaster-response protocols prior to public or formal deployment.
