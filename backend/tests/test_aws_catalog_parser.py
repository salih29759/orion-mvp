from pipeline import aws_era5_catalog as catalog
from pipeline.aws_era5_catalog import parse_aws_key


def test_parse_aws_key_surface_temp():
    key = "e5.oper.an.sfc/194001/e5.oper.an.sfc.128_167_2t.ll025sc.1940010100_1940013123.nc"
    out = parse_aws_key(key)
    assert out.dataset_group == "e5.oper.an.sfc"
    assert out.variable == "2m_temperature"
    assert out.year == 1940
    assert out.month == 1


def test_parse_aws_key_precip_component():
    key = "e5.oper.fc.sfc.accumu/194001/e5.oper.fc.sfc.accumu.128_142_lsp.ll025sc.1940010106_1940013118.nc"
    out = parse_aws_key(key)
    assert out.dataset_group == "e5.oper.fc.sfc.accumu"
    assert out.variable == "large_scale_precipitation"
    assert out.year == 1940
    assert out.month == 1


def test_list_objects_uses_start_after_only_on_first_page(monkeypatch):
    class _DummyClient:
        def __init__(self):
            self.calls = []
            self.page = 0

        def list_objects_v2(self, **kwargs):
            self.calls.append(kwargs)
            self.page += 1
            if self.page == 1:
                return {
                    "Contents": [{"Key": "e5.oper.an.sfc/194001/a.nc"}],
                    "IsTruncated": True,
                    "NextContinuationToken": "tok-1",
                }
            return {
                "Contents": [{"Key": "e5.oper.an.sfc/194001/b.nc"}],
                "IsTruncated": False,
            }

    dummy = _DummyClient()
    monkeypatch.setattr(catalog, "_client", lambda: dummy)

    out = catalog.list_objects("e5.oper.an.sfc/", max_keys=2, start_after="resume-key-1")
    assert len(out) == 2
    assert dummy.calls[0]["StartAfter"] == "resume-key-1"
    assert "ContinuationToken" not in dummy.calls[0]
    assert dummy.calls[1]["ContinuationToken"] == "tok-1"
    assert "StartAfter" not in dummy.calls[1]
