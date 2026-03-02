from __future__ import annotations

import sys
from pathlib import Path
import types


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


# Lightweight stubs so unit tests can run without cloud/CDS client libs.
if "cdsapi" not in sys.modules:
    cdsapi = types.ModuleType("cdsapi")

    class _DummyClient:  # noqa: D401
        def __init__(self, *args, **kwargs):
            pass

        def retrieve(self, *args, **kwargs):
            raise RuntimeError("cdsapi stub should not be called in unit tests")

    cdsapi.Client = _DummyClient
    sys.modules["cdsapi"] = cdsapi

if "google.cloud" not in sys.modules:
    google = types.ModuleType("google")
    cloud = types.ModuleType("google.cloud")
    storage = types.ModuleType("google.cloud.storage")

    class _DummyBlob:
        def upload_from_filename(self, *args, **kwargs):
            pass

        def download_to_filename(self, *args, **kwargs):
            raise RuntimeError("storage stub should not be called in unit tests")

    class _DummyBucket:
        def blob(self, *args, **kwargs):
            return _DummyBlob()

    class _DummyStorageClient:
        def bucket(self, *args, **kwargs):
            return _DummyBucket()

    storage.Client = _DummyStorageClient
    cloud.storage = storage
    google.cloud = cloud
    sys.modules["google"] = google
    sys.modules["google.cloud"] = cloud
    sys.modules["google.cloud.storage"] = storage
