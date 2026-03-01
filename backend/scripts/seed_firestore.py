"""Deprecated script kept for backward compatibility.

Firestore has been replaced by Postgres. Use:
    python scripts/seed_postgres.py [--dry-run]
"""

from seed_postgres import run


if __name__ == "__main__":
    run(dry_run=False)
