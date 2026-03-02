from __future__ import annotations

from fastapi.responses import JSONResponse


class ApiError(Exception):
    def __init__(self, *, status_code: int, error_code: str, message: str, details: dict | None = None):
        self.status_code = status_code
        self.error_code = error_code
        self.message = message
        self.details = details
        super().__init__(message)


def api_error_response(status_code: int, error_code: str, message: str, details: dict | None = None) -> JSONResponse:
    payload: dict = {"error_code": error_code, "message": message}
    if details:
        payload["details"] = details
    return JSONResponse(status_code=status_code, content=payload)

