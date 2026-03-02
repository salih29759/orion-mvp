# AWS ERA5 Layout (NSF/NCAR Open Data)

Bucket:
- `s3://nsf-ncar-era5`
- region: `us-west-2`
- anonymous access enabled (`UNSIGNED` / `--no-sign-request`)

Observed top-level groups:
- `e5.oper.an.sfc/`
- `e5.oper.fc.sfc.accumu/`
- `e5.oper.fc.sfc.instan/`

Key pattern used by parser:
- `{dataset_group}/{YYYYMM}/{filename}`
- Example: `e5.oper.an.sfc/194001/e5.oper.an.sfc.128_167_2t.ll025sc.1940010100_1940013123.nc`

Variable mapping used:
- `2t -> 2m_temperature`
- `10u -> 10m_u_component_of_wind`
- `10v -> 10m_v_component_of_wind`
- `swvl1 -> volumetric_soil_water_layer_1`
- `tp -> total_precipitation`
- `lsp -> large_scale_precipitation`
- `cp -> convective_precipitation`

Precipitation handling:
- if `tp` missing, pipeline derives total precipitation from `lsp + cp`.

Catalog tables:
- `aws_era5_objects`
- `aws_era5_catalog_runs`

Latest availability:
- computed from catalog rows via `/jobs/aws-era5/catalog/latest`.
- response includes per-variable latest month and common latest month.
