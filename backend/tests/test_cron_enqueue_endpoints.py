from __future__ import annotations

from app.routers import aws_jobs, firms


def test_cron_aws_monthly_update_publish_contract(api_client, monkeypatch):
    monkeypatch.setattr(aws_jobs.settings, "cron_secret", "secret")
    monkeypatch.setattr(
        aws_jobs,
        "enqueue_aws_monthly_update",
        lambda: {
            "status": "accepted",
            "source": "aws_era5",
            "job_type": "monthly",
            "run_id": "run-aws-1",
            "published_count": 1,
            "deduped_count": 0,
            "skipped_count": 0,
            "disabled": False,
            "reason": None,
            "latest_common_month": "2024-01",
        },
    )
    res = api_client.post("/cron/aws-era5/monthly-update", headers={"x-cron-secret": "secret"})
    assert res.status_code == 200
    body = res.json()
    assert body["status"] == "accepted"
    assert body["published_count"] == 1
    assert body["source"] == "aws_era5"


def test_cron_firms_daily_update_publish_contract(api_client, monkeypatch):
    monkeypatch.setattr(firms.settings, "cron_secret", "secret")
    monkeypatch.setattr(
        firms,
        "enqueue_firms_daily_update",
        lambda: {
            "status": "accepted",
            "source": "firms",
            "job_type": "daily",
            "run_id": "run-firms-1",
            "published_count": 1,
            "deduped_count": 0,
            "skipped_count": 0,
            "disabled": False,
            "reason": None,
            "start_date": "2024-01-01",
            "end_date": "2024-01-02",
        },
    )
    res = api_client.post("/cron/firms/daily-update", headers={"x-cron-secret": "secret"})
    assert res.status_code == 200
    body = res.json()
    assert body["status"] == "accepted"
    assert body["published_count"] == 1
    assert body["source"] == "firms"

