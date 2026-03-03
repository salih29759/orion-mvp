from __future__ import annotations

from fastapi import APIRouter, Header

from app.errors import ApiError
from app.gcp.oidc_verifier import verify_pubsub_oidc_token
from app.schemas.orchestration import PubSubPushEnvelope
from app.services.orchestration_service import process_pubsub_job_message

router = APIRouter()


def _extract_bearer_token(authorization: str | None) -> str:
    if not authorization:
        raise ApiError(status_code=401, error_code="UNAUTHORIZED", message="Missing Authorization header")
    scheme, _, token = authorization.partition(" ")
    if scheme.lower() != "bearer" or not token:
        raise ApiError(status_code=401, error_code="UNAUTHORIZED", message="Invalid Authorization header")
    return token.strip()


@router.post("/pubsub/worker", summary="Pub/Sub push worker endpoint")
async def pubsub_worker(
    body: PubSubPushEnvelope,
    authorization: str | None = Header(default=None),
):
    token = _extract_bearer_token(authorization)
    verify_pubsub_oidc_token(token)

    try:
        message = body.decode_job_message()
    except Exception as exc:  # noqa: BLE001
        raise ApiError(status_code=422, error_code="VALIDATION_ERROR", message=f"Invalid Pub/Sub envelope: {exc}") from exc

    if body.delivery_attempt and body.delivery_attempt > 0:
        message = message.model_copy(update={"attempt": int(body.delivery_attempt)})

    result = process_pubsub_job_message(message)
    return {"status": "acknowledged", "result": result}

