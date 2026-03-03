from sqlalchemy import func, select, text
from sqlalchemy.orm import Session

from app.orm import ProvinceORM
from app.seed_data import RAW_PROVINCES


def ensure_provinces_seeded(db: Session) -> int:
    count = db.execute(select(func.count()).select_from(ProvinceORM)).scalar_one()
    if count > 0:
        return 0

    rows = [
        ProvinceORM(
            id=str(plate),
            plate=plate,
            name=name,
            region=region,
            lat=lat,
            lng=lng,
            population=pop,
            insured_assets=pop * 18000,
        )
        for plate, name, region, lat, lng, _flood, _drought, pop in RAW_PROVINCES
    ]
    db.add_all(rows)
    db.commit()
    return len(rows)


def ensure_ops_schema(db: Session) -> None:
    # Postgres only: make incremental ops schema changes safe during rollout.
    if db.bind is None or db.bind.dialect.name != "postgresql":
        return

    stmts = [
        "ALTER TABLE era5_ingest_jobs ADD COLUMN IF NOT EXISTS dq_status VARCHAR(32)",
        "ALTER TABLE era5_ingest_jobs ADD COLUMN IF NOT EXISTS dq_report_json TEXT",
        "ALTER TABLE era5_ingest_jobs ADD COLUMN IF NOT EXISTS duration_seconds DOUBLE PRECISION",
        "ALTER TABLE era5_ingest_jobs ADD COLUMN IF NOT EXISTS provider VARCHAR(32) NOT NULL DEFAULT 'cds'",
        "ALTER TABLE era5_ingest_jobs ADD COLUMN IF NOT EXISTS mode VARCHAR(16) NOT NULL DEFAULT 'bbox'",
        "ALTER TABLE era5_ingest_jobs ADD COLUMN IF NOT EXISTS points_set VARCHAR(64)",
        "ALTER TABLE era5_ingest_jobs ADD COLUMN IF NOT EXISTS month_label VARCHAR(7)",
        "ALTER TABLE era5_ingest_jobs ADD COLUMN IF NOT EXISTS source_range_json TEXT",
        "CREATE INDEX IF NOT EXISTS ix_era5_ingest_jobs_dq_status ON era5_ingest_jobs(dq_status)",
        "CREATE INDEX IF NOT EXISTS ix_era5_ingest_jobs_month_label ON era5_ingest_jobs(month_label)",
        "ALTER TABLE era5_artifacts ADD COLUMN IF NOT EXISTS source_uri TEXT",
        "ALTER TABLE era5_artifacts ADD COLUMN IF NOT EXISTS source_etag VARCHAR(255)",
        "ALTER TABLE era5_artifacts ADD COLUMN IF NOT EXISTS cache_hit BOOLEAN NOT NULL DEFAULT FALSE",
        "ALTER TABLE era5_backfill_jobs ADD COLUMN IF NOT EXISTS provider_strategy VARCHAR(32) NOT NULL DEFAULT 'aws_first_hybrid'",
        "ALTER TABLE era5_backfill_jobs ADD COLUMN IF NOT EXISTS force BOOLEAN NOT NULL DEFAULT FALSE",
        "ALTER TABLE era5_backfill_items ADD COLUMN IF NOT EXISTS provider_selected VARCHAR(16) NOT NULL DEFAULT 'cds'",
        "ALTER TABLE era5_backfill_items ADD COLUMN IF NOT EXISTS attempt_count INTEGER NOT NULL DEFAULT 0",
        """
        CREATE TABLE IF NOT EXISTS era5_backfill_jobs (
            backfill_id VARCHAR(64) PRIMARY KEY,
            request_signature VARCHAR(64) UNIQUE NOT NULL,
            status VARCHAR(16) NOT NULL,
            mode VARCHAR(16) NOT NULL,
            dataset VARCHAR(128) NOT NULL,
            variables_csv TEXT NOT NULL,
            bbox_csv VARCHAR(128) NOT NULL,
            start_month VARCHAR(7) NOT NULL,
            end_month VARCHAR(7) NOT NULL,
            months_total INTEGER NOT NULL DEFAULT 0,
            months_success INTEGER NOT NULL DEFAULT 0,
            months_failed INTEGER NOT NULL DEFAULT 0,
            failed_months_json TEXT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            finished_at TIMESTAMPTZ NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS era5_backfill_items (
            id SERIAL PRIMARY KEY,
            backfill_id VARCHAR(64) NOT NULL REFERENCES era5_backfill_jobs(backfill_id) ON DELETE CASCADE,
            month_label VARCHAR(7) NOT NULL,
            start_date DATE NOT NULL,
            end_date DATE NOT NULL,
            job_id VARCHAR(64) NULL REFERENCES era5_ingest_jobs(job_id) ON DELETE SET NULL,
            status VARCHAR(16) NOT NULL DEFAULT 'queued',
            error TEXT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            finished_at TIMESTAMPTZ NULL,
            CONSTRAINT uq_backfill_month UNIQUE (backfill_id, month_label)
        )
        """,
        "CREATE INDEX IF NOT EXISTS ix_era5_backfill_items_backfill_id ON era5_backfill_items(backfill_id)",
        "CREATE INDEX IF NOT EXISTS ix_era5_backfill_items_job_id ON era5_backfill_items(job_id)",
        "CREATE INDEX IF NOT EXISTS ix_era5_backfill_items_month_label ON era5_backfill_items(month_label)",
        "CREATE INDEX IF NOT EXISTS ix_era5_backfill_items_status ON era5_backfill_items(status)",
        """
        CREATE TABLE IF NOT EXISTS export_jobs (
            export_id VARCHAR(64) PRIMARY KEY,
            portfolio_id VARCHAR(128) NOT NULL,
            scenario VARCHAR(32) NOT NULL,
            start_date DATE NOT NULL,
            end_date DATE NOT NULL,
            output_format VARCHAR(16) NOT NULL,
            status VARCHAR(16) NOT NULL,
            row_count INTEGER NOT NULL DEFAULT 0,
            gcs_uri TEXT NULL,
            signed_url TEXT NULL,
            error TEXT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """,
        "CREATE INDEX IF NOT EXISTS ix_export_jobs_portfolio_id ON export_jobs(portfolio_id)",
        "CREATE INDEX IF NOT EXISTS ix_export_jobs_status ON export_jobs(status)",
        """
        CREATE TABLE IF NOT EXISTS climatology_runs (
            run_id VARCHAR(64) PRIMARY KEY,
            climatology_version VARCHAR(128) NOT NULL UNIQUE,
            dataset VARCHAR(128) NOT NULL,
            baseline_start DATE NOT NULL,
            baseline_end DATE NOT NULL,
            level VARCHAR(16) NOT NULL,
            status VARCHAR(16) NOT NULL,
            row_count INTEGER NOT NULL DEFAULT 0,
            thresholds_gcs_uri TEXT NULL,
            error TEXT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """,
        "CREATE INDEX IF NOT EXISTS ix_climatology_runs_status ON climatology_runs(status)",
        """
        CREATE TABLE IF NOT EXISTS climatology_thresholds (
            id SERIAL PRIMARY KEY,
            climatology_version VARCHAR(128) NOT NULL,
            cell_lat DOUBLE PRECISION NOT NULL,
            cell_lng DOUBLE PRECISION NOT NULL,
            month INTEGER NOT NULL,
            temp_max_p95 DOUBLE PRECISION NULL,
            wind_max_p95 DOUBLE PRECISION NULL,
            precip_1d_p95 DOUBLE PRECISION NULL,
            precip_1d_p99 DOUBLE PRECISION NULL,
            precip_7d_p95 DOUBLE PRECISION NULL,
            precip_7d_p99 DOUBLE PRECISION NULL,
            precip_30d_p10 DOUBLE PRECISION NULL,
            soil_moisture_p10 DOUBLE PRECISION NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT uq_clim_version_cell_month UNIQUE (climatology_version, cell_lat, cell_lng, month)
        )
        """,
        "CREATE INDEX IF NOT EXISTS ix_climatology_thresholds_version ON climatology_thresholds(climatology_version)",
        "CREATE INDEX IF NOT EXISTS ix_climatology_thresholds_month ON climatology_thresholds(month)",
        """
        CREATE TABLE IF NOT EXISTS climatology_thresholds_doy (
            id SERIAL PRIMARY KEY,
            climatology_version VARCHAR(128) NOT NULL,
            cell_lat DOUBLE PRECISION NOT NULL,
            cell_lng DOUBLE PRECISION NOT NULL,
            doy INTEGER NOT NULL,
            temp_max_p95 DOUBLE PRECISION NULL,
            wind_max_p95 DOUBLE PRECISION NULL,
            precip_1d_p95 DOUBLE PRECISION NULL,
            precip_1d_p99 DOUBLE PRECISION NULL,
            precip_7d_p95 DOUBLE PRECISION NULL,
            precip_7d_p99 DOUBLE PRECISION NULL,
            precip_30d_p10 DOUBLE PRECISION NULL,
            soil_moisture_p10 DOUBLE PRECISION NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT uq_clim_version_cell_doy UNIQUE (climatology_version, cell_lat, cell_lng, doy)
        )
        """,
        "CREATE INDEX IF NOT EXISTS ix_climatology_thresholds_doy_version ON climatology_thresholds_doy(climatology_version)",
        "CREATE INDEX IF NOT EXISTS ix_climatology_thresholds_doy_doy ON climatology_thresholds_doy(doy)",
        """
        CREATE TABLE IF NOT EXISTS asset_risk_scores (
            id SERIAL PRIMARY KEY,
            asset_id VARCHAR(128) NOT NULL,
            score_date DATE NOT NULL,
            peril VARCHAR(32) NOT NULL,
            scenario VARCHAR(32) NOT NULL,
            horizon VARCHAR(32) NOT NULL,
            likelihood VARCHAR(32) NOT NULL,
            score_0_100 INTEGER NOT NULL,
            band VARCHAR(16) NOT NULL,
            exposure_json TEXT NOT NULL,
            drivers_json TEXT NOT NULL,
            run_id VARCHAR(64) NOT NULL,
            climatology_version VARCHAR(128) NOT NULL,
            data_version VARCHAR(128) NOT NULL DEFAULT 'era5_daily_v1',
            created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT uq_asset_risk_score_dim UNIQUE (asset_id, score_date, peril, scenario, horizon, likelihood)
        )
        """,
        "CREATE INDEX IF NOT EXISTS ix_asset_risk_scores_asset_id ON asset_risk_scores(asset_id)",
        "CREATE INDEX IF NOT EXISTS ix_asset_risk_scores_score_date ON asset_risk_scores(score_date)",
        "CREATE INDEX IF NOT EXISTS ix_asset_risk_scores_peril ON asset_risk_scores(peril)",
        "CREATE INDEX IF NOT EXISTS ix_asset_risk_scores_run_id ON asset_risk_scores(run_id)",
        """
        CREATE TABLE IF NOT EXISTS portfolio_assets (
            id SERIAL PRIMARY KEY,
            portfolio_id VARCHAR(128) NOT NULL,
            asset_id VARCHAR(128) NOT NULL,
            lat DOUBLE PRECISION NOT NULL,
            lon DOUBLE PRECISION NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT uq_portfolio_asset UNIQUE (portfolio_id, asset_id)
        )
        """,
        "CREATE INDEX IF NOT EXISTS ix_portfolio_assets_portfolio_id ON portfolio_assets(portfolio_id)",
        """
        CREATE TABLE IF NOT EXISTS firms_ingest_jobs (
            job_id VARCHAR(64) PRIMARY KEY,
            request_signature VARCHAR(64) NOT NULL UNIQUE,
            status VARCHAR(16) NOT NULL,
            source VARCHAR(64) NOT NULL,
            bbox_csv VARCHAR(128) NOT NULL,
            start_date DATE NOT NULL,
            end_date DATE NOT NULL,
            rows_fetched INTEGER NOT NULL DEFAULT 0,
            rows_inserted INTEGER NOT NULL DEFAULT 0,
            raw_gcs_uri TEXT NULL,
            duration_seconds DOUBLE PRECISION NULL,
            error TEXT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            started_at TIMESTAMPTZ NULL,
            finished_at TIMESTAMPTZ NULL
        )
        """,
        "CREATE INDEX IF NOT EXISTS ix_firms_ingest_jobs_status ON firms_ingest_jobs(status)",
        "CREATE INDEX IF NOT EXISTS ix_firms_ingest_jobs_source ON firms_ingest_jobs(source)",
        """
        CREATE TABLE IF NOT EXISTS earthquake_ingest_jobs (
            job_id VARCHAR(64) PRIMARY KEY,
            request_signature VARCHAR(64) NOT NULL,
            status VARCHAR(32) NOT NULL,
            start_date DATE NOT NULL,
            end_date DATE NOT NULL,
            rows_written INTEGER NOT NULL DEFAULT 0,
            files_written INTEGER NOT NULL DEFAULT 0,
            progress_json TEXT NULL,
            error TEXT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            started_at TIMESTAMPTZ NULL,
            finished_at TIMESTAMPTZ NULL
        )
        """,
        "CREATE INDEX IF NOT EXISTS ix_earthquake_ingest_jobs_status ON earthquake_ingest_jobs(status)",
        "CREATE INDEX IF NOT EXISTS ix_earthquake_ingest_jobs_request_signature ON earthquake_ingest_jobs(request_signature)",
        "CREATE INDEX IF NOT EXISTS ix_earthquake_ingest_jobs_created_at ON earthquake_ingest_jobs(created_at)",
        """
        CREATE TABLE IF NOT EXISTS fires (
            id BIGSERIAL PRIMARY KEY,
            time_utc TIMESTAMPTZ NOT NULL,
            lat DOUBLE PRECISION NOT NULL,
            lon DOUBLE PRECISION NOT NULL,
            lat_round DOUBLE PRECISION NOT NULL,
            lon_round DOUBLE PRECISION NOT NULL,
            geom_wkt TEXT NULL,
            frp DOUBLE PRECISION NULL,
            confidence VARCHAR(32) NULL,
            satellite VARCHAR(32) NULL,
            source VARCHAR(64) NOT NULL,
            raw_job_id VARCHAR(64) NULL REFERENCES firms_ingest_jobs(job_id) ON DELETE SET NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT uq_fire_event_key UNIQUE (source, time_utc, lat_round, lon_round)
        )
        """,
        "CREATE INDEX IF NOT EXISTS ix_fires_time_utc ON fires(time_utc)",
        "CREATE INDEX IF NOT EXISTS ix_fires_lat ON fires(lat)",
        "CREATE INDEX IF NOT EXISTS ix_fires_lon ON fires(lon)",
        "CREATE INDEX IF NOT EXISTS ix_fires_source ON fires(source)",
        "CREATE INDEX IF NOT EXISTS ix_fires_raw_job_id ON fires(raw_job_id)",
        """
        CREATE TABLE IF NOT EXISTS notifications (
            id VARCHAR(64) PRIMARY KEY,
            customer_id VARCHAR(128) NULL,
            portfolio_id VARCHAR(128) NULL,
            asset_id VARCHAR(128) NOT NULL,
            type VARCHAR(64) NOT NULL,
            severity VARCHAR(16) NOT NULL,
            payload_json TEXT NOT NULL,
            dedup_key VARCHAR(255) NOT NULL UNIQUE,
            created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            acknowledged_at TIMESTAMPTZ NULL
        )
        """,
        "CREATE INDEX IF NOT EXISTS ix_notifications_customer_id ON notifications(customer_id)",
        "CREATE INDEX IF NOT EXISTS ix_notifications_portfolio_id ON notifications(portfolio_id)",
        "CREATE INDEX IF NOT EXISTS ix_notifications_asset_id ON notifications(asset_id)",
        "CREATE INDEX IF NOT EXISTS ix_notifications_type ON notifications(type)",
        "CREATE INDEX IF NOT EXISTS ix_notifications_severity ON notifications(severity)",
        """
        CREATE TABLE IF NOT EXISTS aws_era5_objects (
            id SERIAL PRIMARY KEY,
            bucket VARCHAR(128) NOT NULL,
            key TEXT NOT NULL,
            size BIGINT NOT NULL DEFAULT 0,
            etag VARCHAR(255) NULL,
            last_modified TIMESTAMPTZ NULL,
            dataset_group VARCHAR(128) NULL,
            variable VARCHAR(64) NULL,
            year INTEGER NULL,
            month INTEGER NULL,
            day INTEGER NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT uq_aws_era5_bucket_key UNIQUE (bucket, key)
        )
        """,
        "CREATE INDEX IF NOT EXISTS ix_aws_era5_objects_variable ON aws_era5_objects(variable)",
        "CREATE INDEX IF NOT EXISTS ix_aws_era5_objects_year ON aws_era5_objects(year)",
        "CREATE INDEX IF NOT EXISTS ix_aws_era5_objects_month ON aws_era5_objects(month)",
        "CREATE INDEX IF NOT EXISTS ix_aws_era5_objects_last_modified ON aws_era5_objects(last_modified)",
        """
        CREATE TABLE IF NOT EXISTS aws_era5_catalog_runs (
            run_id VARCHAR(64) PRIMARY KEY,
            status VARCHAR(16) NOT NULL,
            objects_scanned INTEGER NOT NULL DEFAULT 0,
            started_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            finished_at TIMESTAMPTZ NULL,
            error TEXT NULL
        )
        """,
        "CREATE INDEX IF NOT EXISTS ix_aws_era5_catalog_runs_status ON aws_era5_catalog_runs(status)",
    ]
    for stmt in stmts:
        db.execute(text(stmt))
    db.commit()
