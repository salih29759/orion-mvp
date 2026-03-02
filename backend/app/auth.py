from fastapi import Security, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from app.config import settings
from app.errors import ApiError

_bearer = HTTPBearer(auto_error=False)


def verify_token(
    credentials: HTTPAuthorizationCredentials | None = Security(_bearer),
) -> str:
    """Validate the Bearer token from the Authorization header."""
    if settings.local_dev_auth_bypass:
        return credentials.credentials if credentials else "dev-bypass"

    expected = settings.orion_backend_api_key or settings.api_key
    if not credentials or credentials.credentials != expected:
        raise ApiError(
            status_code=status.HTTP_401_UNAUTHORIZED,
            error_code="UNAUTHORIZED",
            message="Invalid or missing API key",
            details={"auth_scheme": "Bearer"},
        )
    return credentials.credentials
