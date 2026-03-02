# Orion MVP — Climate Risk Dashboard

Turkey-focused climate risk intelligence platform. Frontend (Next.js) + Backend (FastAPI).

---

## Quick Start

### 1. Environment variables

Create `.env.local` in the project root:

```env
# Required — server-side only (used by the Next.js proxy, never sent to browser)
ORION_BACKEND_URL=http://localhost:8000
ORION_BACKEND_API_KEY=your-api-key-here

# Optional — only needed if you want the browser to call a different base path
# Defaults to /api/orion (the built-in Next.js proxy)
# NEXT_PUBLIC_API_BASE_URL=/api/orion
```

### 2. Install and run

```bash
# Install dependencies
npm install

# Start frontend dev server (port 3000)
npm run dev
```

Open [http://localhost:3000](http://localhost:3000).

### 3. Start backend (separate terminal)

```bash
cd backend
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # fill in DB / API keys
alembic upgrade head
uvicorn main:app --reload --port 8000
```

Backend API docs: [http://localhost:8000/docs](http://localhost:8000/docs)

---

## Pages

| Route | Description |
|-------|-------------|
| `/` | Legacy province-level dashboard |
| `/portfolio` | Portfolio overview — scores, band distribution, trend chart, top assets |
| `/assets` | Asset table with search + band filter |
| `/assets/[id]` | Asset detail — per-peril scores, explainability drivers, time series, map |
| `/notifications` | Wildfire / alert notifications (available after FIRMS sprint) |

---

## API Contract

All frontend calls route through the Next.js proxy at `/api/orion → ORION_BACKEND_URL`.
Source of truth: [`backend/openapi.yaml`](backend/openapi.yaml)

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/portfolios` | List portfolios |
| `GET` | `/portfolios/{id}/risk-summary?start=YYYY-MM-DD&end=YYYY-MM-DD` | Portfolio risk summary |
| `POST` | `/scores/batch` | Batch asset scoring |
| `POST` | `/export/portfolio` | Queue portfolio CSV export |
| `GET` | `/notifications?portfolio_id=...` | List notifications |
| `POST` | `/notifications/{id}/ack` | Acknowledge notification |

---

## Architecture

```
browser → Next.js (/api/orion proxy) → FastAPI backend
                ↑
        adds Bearer auth header
        (ORION_BACKEND_API_KEY, server-side only)
```

**Key files:**

```
types/index.ts          — TypeScript types mirroring openapi.yaml
lib/orionApi.ts         — Typed fetch wrappers for all endpoints
lib/store.ts            — Zustand global state (portfolio, date range, selected asset)
hooks/useApi.ts         — TanStack Query hooks (caching, retry, loading states)
app/providers.tsx       — QueryClientProvider wrapper
app/api/orion/[...path] — Server-side proxy (adds auth, forwards to backend)
components/AssetMap.tsx — MapLibre GL map (dynamic import, OSM tiles, no token needed)
```

---

## Map

The asset detail map uses **MapLibre GL JS** with OpenStreetMap tiles — no API token required.
To use a custom style (Mapbox, Maptiler, etc.), update the `style` property in [components/AssetMap.tsx](components/AssetMap.tsx).

---

## Build

```bash
npm run build
npm start
```

---

## Deployment (Render)

See [`render.yaml`](render.yaml) for deployment configuration.

Required environment variables on Render:
- `ORION_BACKEND_URL`
- `ORION_BACKEND_API_KEY`
