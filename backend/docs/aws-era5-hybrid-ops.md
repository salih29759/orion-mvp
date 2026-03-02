# AWS ERA5 Hybrid Ops Runbook

## Strategy
- Bulk history: AWS (`nsf-ncar-era5`)
- Tail / gaps: CDS fallback
- Resolver runs per-month and selects provider (`aws` or `cds`) before child job enqueue.

## Key env vars
- `AWS_ERA5_BUCKET=nsf-ncar-era5`
- `AWS_ERA5_REGION=us-west-2`
- `AWS_ERA5_USE_UNSIGNED=true`
- `AWS_ERA5_MAX_CONCURRENT_DOWNLOADS=3`
- `AWS_ERA5_MODE_DEFAULT=points`
- `AWS_ERA5_POINTS_SET_DEFAULT=assets+provinces`
- `ERA5_HYBRID_ENABLE=true`
- `ERA5_CDS_FALLBACK_ENABLE=true`

## Commands
Catalog sync:
```bash
curl -X POST "$BASE_URL/jobs/aws-era5/catalog/sync" \
  -H "Authorization: Bearer $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"max_keys_per_prefix":2000}'
```

Catalog latest:
```bash
curl -s "$BASE_URL/jobs/aws-era5/catalog/latest" \
  -H "Authorization: Bearer $API_KEY"
```

AWS backfill:
```bash
curl -X POST "$BASE_URL/jobs/aws-era5/backfill" \
  -H "Authorization: Bearer $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "start":"2015-01-01",
    "end":"2024-12-31",
    "mode":"points",
    "points_set":"assets+provinces",
    "bbox":{"north":42,"west":26,"south":36,"east":45},
    "variables":["2m_temperature","total_precipitation","10m_u_component_of_wind","10m_v_component_of_wind","volumetric_soil_water_layer_1"],
    "concurrency":3,
    "force":false
  }'
```

Legacy backfill (now AWS-first hybrid internally):
```bash
curl -X POST "$BASE_URL/jobs/era5/backfill" \
  -H "Authorization: Bearer $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "start_month":"2015-01",
    "end_month":"2024-12",
    "bbox":{"north":42,"west":26,"south":36,"east":45},
    "variables":["2m_temperature","total_precipitation","10m_u_component_of_wind","10m_v_component_of_wind","volumetric_soil_water_layer_1"],
    "mode":"monthly",
    "concurrency":2
  }'
```

Monthly update cron:
```bash
curl -X POST "$BASE_URL/cron/aws-era5/monthly-update" \
  -H "x-cron-secret: $CRON_SECRET"
```

Job status:
```bash
curl -s "$BASE_URL/jobs/$JOB_ID" -H "Authorization: Bearer $API_KEY"
```

## Stuck / retry
- If child month does not progress, re-run same backfill request.
- Completed months are idempotently skipped (`force=false`).
- Use `force=true` to reprocess selected ranges.
