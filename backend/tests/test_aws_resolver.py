from pipeline import aws_era5_resolver


def test_resolver_uses_aws_when_all_vars_present(monkeypatch):
    monkeypatch.setattr(
        aws_era5_resolver,
        "get_month_variables",
        lambda y, m: {
            "2m_temperature",
            "10m_u_component_of_wind",
            "10m_v_component_of_wind",
            "large_scale_precipitation",
            "convective_precipitation",
            "volumetric_soil_water_layer_1",
        },
    )

    out = aws_era5_resolver.resolve_months_provider(
        start_month="2024-01",
        end_month="2024-01",
        variables=[
            "2m_temperature",
            "10m_u_component_of_wind",
            "10m_v_component_of_wind",
            "total_precipitation",
            "volumetric_soil_water_layer_1",
        ],
    )
    assert len(out) == 1
    assert out[0].provider == "aws"


def test_resolver_falls_back_to_cds_when_var_missing(monkeypatch):
    monkeypatch.setattr(
        aws_era5_resolver,
        "get_month_variables",
        lambda y, m: {
            "2m_temperature",
            "10m_u_component_of_wind",
            "10m_v_component_of_wind",
            "large_scale_precipitation",
            "convective_precipitation",
        },
    )

    out = aws_era5_resolver.resolve_months_provider(
        start_month="2024-01",
        end_month="2024-01",
        variables=[
            "2m_temperature",
            "10m_u_component_of_wind",
            "10m_v_component_of_wind",
            "total_precipitation",
            "volumetric_soil_water_layer_1",
        ],
    )
    assert len(out) == 1
    assert out[0].provider == "cds"
