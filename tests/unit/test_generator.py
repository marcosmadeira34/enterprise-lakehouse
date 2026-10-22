from collections import Counter
from pathlib import Path

import pytest

from lakehouse.common.storage import LocalStore
from lakehouse.generator.retail import DROP1_SCENARIOS, GeneratorConfig, generate


def _generate(tmp_path: Path, name: str, drop: int = 1, **kwargs: object) -> dict:
    landing = tmp_path / name
    return generate(LocalStore(), str(landing), GeneratorConfig(scale="tiny", **kwargs), drop=drop)  # type: ignore[arg-type]


def _bytes(root: Path) -> dict[str, bytes]:
    return {str(p.relative_to(root)): p.read_bytes() for p in sorted(root.rglob("*")) if p.is_file()}


def test_same_seed_produces_identical_files(tmp_path: Path) -> None:
    first = _generate(tmp_path, "a", seed=7)
    second = _generate(tmp_path, "b", seed=7)
    data_a = {k: v for k, v in _bytes(tmp_path / "a").items() if not k.startswith("_manifests")}
    data_b = {k: v for k, v in _bytes(tmp_path / "b").items() if not k.startswith("_manifests")}
    assert data_a == data_b and len(data_a) == 16
    assert first["expected"] == second["expected"]


def test_different_seed_changes_data(tmp_path: Path) -> None:
    _generate(tmp_path, "a", seed=1)
    _generate(tmp_path, "b", seed=2)
    a, b = _bytes(tmp_path / "a"), _bytes(tmp_path / "b")
    assert a.keys() == b.keys()
    assert a != b


def test_manifest_lists_every_injected_anomaly(tmp_path: Path) -> None:
    manifest = _generate(tmp_path, "a")
    reasons = Counter(r["reason"] for r in manifest["expected"]["rejections"])
    for reason in ("DUPLICATE_BUSINESS_KEY", "ORPHAN_PRODUCT", "ORPHAN_SALE", "NEGATIVE_AMOUNT_FOR_SALE",
                   "NEGATIVE_UNIT_PRICE", "NEGATIVE_AMOUNT_FOR_PAYMENT", "MISSING_REQUIRED:store_id",
                   "MISSING_REQUIRED:product_name", "INVALID_FORMAT:sale_ts", "INVALID_FORMAT:signup_date",
                   "MALFORMED_RECORD"):  # fmt: skip
        assert reasons[reason] >= 1, reason
    warnings = manifest["expected"]["warnings"]
    assert warnings["SLV-SALES-TOTAL-MISMATCH"] >= 1
    assert warnings["SLV-SALES-RI-CUSTOMER"] >= 1
    assert warnings["SLV-PAYMENTS-TOTAL-MISMATCH"] == 0  # payments follow the (wrong) header total
    # Every injected row is in some file; files hold the world rows plus the injected ones.
    rows = sum(f["rows"] for f in manifest["files"])
    world = sum(manifest["expected"]["silver_counts"].values())
    assert rows == world + len(manifest["expected"]["rejections"])


def test_clean_generation_has_no_anomalies(tmp_path: Path) -> None:
    manifest = _generate(tmp_path, "a", error_rate=0.0)
    assert manifest["expected"]["rejections"] == []
    warnings = manifest["expected"]["warnings"]
    assert warnings["SLV-SALES-TOTAL-MISMATCH"] == warnings["SLV-INVENTORY-NEG-STOCK"] == 0


def test_formats_follow_contracts(tmp_path: Path) -> None:
    manifest = _generate(tmp_path, "a")
    extensions = {f["entity"]: Path(f["uri"]).suffix for f in manifest["files"]}
    assert extensions == {"categories": ".csv", "products": ".json", "customers": ".csv", "stores": ".csv",
                          "sales": ".csv", "sale_items": ".parquet", "payments": ".json", "inventory": ".parquet"}  # fmt: skip


def test_drop2_emits_incremental_scenarios(tmp_path: Path) -> None:
    _generate(tmp_path, "a", drop=1)
    manifest = _generate(tmp_path, "a", drop=2)
    expected = manifest["expected"]
    assert len(expected["duplicate_files"]) == 1
    assert len(expected["quarantined_files"]) == 1
    assert expected["updated_customers"] and expected["stale_customers"]
    resend = Path(expected["duplicate_files"][0]).read_bytes()
    original = (tmp_path / "a" / "sales" / "sales_d001_p001.csv").read_bytes()
    assert resend == original  # byte-identical re-delivery under another name
    header = (tmp_path / "a" / "customers" / "customers_d002_v2_p001.csv").read_text().splitlines()[0]
    assert "loyalty_tier" in header and "phone" not in header


def test_parameters_are_validated() -> None:
    with pytest.raises(ValueError):
        GeneratorConfig(scale="huge")
    with pytest.raises(ValueError):
        GeneratorConfig(scenarios=("nope",))
    with pytest.raises(ValueError):
        GeneratorConfig(error_rate=0.9)
    assert set(DROP1_SCENARIOS) <= set(GeneratorConfig().scenarios)


def test_volume_scales_with_overrides(tmp_path: Path) -> None:
    manifest = _generate(tmp_path, "a", overrides={"sales": 300}, error_rate=0.0)
    assert manifest["expected"]["silver_counts"]["sales"] >= 300
