# Contract Verification

Source of truth: [`backend/openapi.yaml`](/Users/salihdurmus/Desktop/orion-mvp/backend/openapi.yaml)

## Env vars

```bash
export BASE_URL="https://orion-api-126886725893.europe-west1.run.app"
export API_KEY="orion-dev-key-2024"
```

For local backend:

```bash
export ORION_BACKEND_API_KEY="orion-dev-key-2024"
export LOCAL_DEV_AUTH_BYPASS="false"   # set true only for local bypass
```

## Quick smoke

```bash
cd backend
./scripts/smoke_contract.sh
```

## Manual curl checks

### 1) `GET /portfolios`

```bash
curl -s "${BASE_URL}/portfolios" -H "Authorization: Bearer ${API_KEY}" | jq
```

Expected snippet:

```json
[{"portfolio_id":"demo-10-assets","name":"demo-10-assets"}]
```

### 2) `GET /portfolios/{id}/risk-summary`

```bash
curl -s "${BASE_URL}/portfolios/demo-10-assets/risk-summary?start=2024-01-01&end=2024-01-31" \
  -H "Authorization: Bearer ${API_KEY}" | jq
```

Expected keys:

```json
{
  "portfolio_id": "...",
  "period": {"start":"2024-01-01","end":"2024-01-31"},
  "bands": {"minimal":0,"minor":0,"moderate":0,"major":0,"extreme":0},
  "peril_averages": {},
  "top_assets": [],
  "trend": [{"date":"2024-01-01","scores":{"all":17.75,"heat":0}}]
}
```

### 3) `POST /scores/batch`

```bash
curl -s -X POST "${BASE_URL}/scores/batch" \
  -H "Authorization: Bearer ${API_KEY}" \
  -H "Content-Type: application/json" \
  -d '{
    "assets":[{"asset_id":"a1","lat":41.01,"lon":28.97}],
    "start_date":"2024-01-01",
    "end_date":"2024-01-03",
    "climatology_version":"v1_test_2024_jan",
    "include_perils":["heat","rain","wind","drought"]
  }' | jq
```

Expected keys:

```json
{
  "run_id": "...",
  "climatology_version": "...",
  "results": [
    {"asset_id":"a1","series":[{"date":"2024-01-01","scores":{},"bands":{},"drivers":{}}]}
  ]
}
```

### 4) Notifications list + ack

```bash
curl -s "${BASE_URL}/notifications?portfolio_id=demo-10-assets" \
  -H "Authorization: Bearer ${API_KEY}" | jq
```

```bash
curl -s -X POST "${BASE_URL}/notifications/<notification_id>/ack" \
  -H "Authorization: Bearer ${API_KEY}" | jq
```

Ack expected shape:

```json
{"id":"ntf_...","acknowledged_at":"2026-03-02T...Z"}
```

### 5) `POST /export/portfolio`

```bash
curl -s -X POST "${BASE_URL}/export/portfolio" \
  -H "Authorization: Bearer ${API_KEY}" \
  -H "Content-Type: application/json" \
  -d '{
    "portfolio_id":"demo-10-assets",
    "start_date":"2024-01-01",
    "end_date":"2024-01-31",
    "format":"csv",
    "include_drivers":true
  }' | jq
```

Expected snippet:

```json
{
  "export_id":"...",
  "status":"success",
  "path":"gs://.../exports/demo-10-assets/...csv",
  "download_url":null
}
```

## Tests

```bash
pytest -q backend/tests
```

