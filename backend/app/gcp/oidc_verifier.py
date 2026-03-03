from __future__ import annotations

from google.auth.transport import requests as google_requests
from google.oauth2 import id_token

from app.config import settings
from app.errors import ApiError

GOOGLE_ISSUERS = {"https://accounts.google.com", "accounts.google.com"}


def verify_pubsub_oidc_token(token: str) -> dict:
    audience = settings.pubsub_oidc_audience
    if not audience:
        raise ApiError(
            status_code=503,
            error_code="CONFIG_ERROR",
            message="PUBSUB_OIDC_AUDIENCE is not configured",
        )
    expected_email = settings.pubsub_push_sa_email
    if not expected_email:
        raise ApiError(
            status_code=503,
            error_code="CONFIG_ERROR",
            message="PUBSUB_PUSH_SA_EMAIL is not configured",
        )

    try:
        claims = id_token.verify_oauth2_token(token, google_requests.Request(), audience=audience)
    except Exception as exc:  # noqa: BLE001
        raise ApiError(status_code=401, error_code="UNAUTHORIZED", message="Invalid OIDC token") from exc

    issuer = str(claims.get("iss", ""))
    if issuer not in GOOGLE_ISSUERS:
        raise ApiError(status_code=401, error_code="UNAUTHORIZED", message="Invalid OIDC issuer")

    email = str(claims.get("email", ""))
    if email != expected_email:
        raise ApiError(status_code=401, error_code="UNAUTHORIZED", message="Unauthorized service account")

    return claims

