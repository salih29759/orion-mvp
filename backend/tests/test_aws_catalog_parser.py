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
