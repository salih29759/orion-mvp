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
        "CREATE INDEX IF NOT EXISTS ix_era5_ingest_jobs_dq_status ON era5_ingest_jobs(dq_status)",
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
    ]
    for stmt in stmts:
        db.execute(text(stmt))
    db.commit()
