# Orion Backend (Postgres + Open-Meteo)

## Local setup

```bash
cd backend
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

## Migrate + seed

```bash
alembic upgrade head
python scripts/seed_postgres.py
```

## Run API

```bash
uvicorn main:app --reload --port 8000
```

Docs: `http://localhost:8000/docs`

## Run pipeline (30-day backfill)

```bash
python -m pipeline.run_pipeline
```

## Required env vars in production

- `DATABASE_URL`
- `API_KEY`
- `ALLOWED_ORIGINS`
- `MODEL_VERSION`
- `DEFAULT_DATA_SOURCE`
