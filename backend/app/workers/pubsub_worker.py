from __future__ import annotations

import argparse
import base64
import json
from pathlib import Path
from typing import Any

from app.schemas.orchestration import PubSubPushEnvelope
from app.services.orchestration_service import process_pubsub_job_message


def _load_envelope_from_file(path: str) -> dict[str, Any]:
    raw = Path(path).read_text(encoding="utf-8")
    return json.loads(raw)


def _build_envelope_from_message_file(path: str) -> dict[str, Any]:
    raw = Path(path).read_text(encoding="utf-8")
    encoded = base64.b64encode(raw.encode("utf-8")).decode("utf-8")
    return {"message": {"data": encoded}}


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Pub/Sub worker logic locally from JSON payloads.")
    parser.add_argument("--envelope-file", help="Path to Pub/Sub push envelope JSON file.")
    parser.add_argument("--message-file", help="Path to plain Pub/Sub message JSON file.")
    args = parser.parse_args()

    if not args.envelope_file and not args.message_file:
        raise SystemExit("Provide --envelope-file or --message-file")

    if args.envelope_file:
        payload = _load_envelope_from_file(args.envelope_file)
    else:
        payload = _build_envelope_from_message_file(args.message_file)

    envelope = PubSubPushEnvelope.model_validate(payload)
    message = envelope.decode_job_message()
    if envelope.delivery_attempt and envelope.delivery_attempt > 0:
        message = message.model_copy(update={"attempt": int(envelope.delivery_attempt)})

    result = process_pubsub_job_message(message)
    print(json.dumps(result, ensure_ascii=True, default=str))


if __name__ == "__main__":
    main()

