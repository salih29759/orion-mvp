# OpenAPI Contract Migration Note

The backend now serves contract-first endpoints defined in [`openapi.yaml`](/Users/salihdurmus/Desktop/orion-mvp/backend/openapi.yaml) and keeps legacy `/v1/*` routes for backward compatibility.

## Contract endpoints implemented

- `GET /portfolios`
- `GET /portfolios/{portfolio_id}/risk-summary`
- `POST /scores/batch`
- `GET /notifications`
- `POST /notifications/{notification_id}/ack`
- `POST /export/portfolio`

## Key response-shape guarantees

- Risk summary trend points are nested: `trend[].scores.{peril}`.
- Export status enum is restricted to: `queued | running | success | failed`.
- Notifications use `severity: low|medium|high` and `acknowledged_at` timestamp (nullable).
- Notification ACK response shape is exactly: `{ id, acknowledged_at }`.
- Batch scoring response shape is `run_id`, `climatology_version`, `results[]` with `ScoreSeriesPoint`.

## Legacy compatibility

- Existing `/v1/*` endpoints remain active.
- Legacy handlers log deprecation messages on access.
- New frontend should use only the contract endpoints from `openapi.yaml`.
