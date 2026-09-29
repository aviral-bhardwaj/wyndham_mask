import copy
from collections import Counter
import pytest
from wd_datamask import Config, MaskingEngine, UnmaskingEngine, PermissionManager, LocalIdentity
from wd_datamask.exceptions import ConfigurationError, MissingMappingError, IntegrityError
from wd_datamask.spark.pandas_udfs import make_lookup_pandas_udf
from wd_datamask.spark.udf_registry import register_lookup_udf
from wd_datamask.spark.subsetting import subset_related

pytestmark = pytest.mark.spark


def narrow(deps, *columns):
    raw = copy.deepcopy(deps["config"].raw)
    raw["tables"]["customer"]["columns"] = {k: v for k, v in raw["tables"]["customer"]["columns"].items() if k in columns}
    return {**deps, "config": Config(raw)}


def test_dataframe_exact_and_partial_roundtrip(spark, setup):
    _, _, deps, _ = setup
    params = narrow(deps, "email", "phone", "membership")
    mask, unmask = MaskingEngine(**params), UnmaskingEngine(**params)
    rows = [(1, "john@example.net", "(555) 123-4567", "000000001A"),
            (1, "john@example.net", "(555) 123-4567", "000000001A"),
            (2, "bad", "bad", "bad"), (3, None, None, None), (4, "", "", "")]
    original = spark.createDataFrame(rows, "id INT, email STRING, phone STRING, membership STRING")
    masked = mask.mask_dataframe(original.repartition(2), table="customer").cache()
    assert masked.count() == len(rows)
    assert masked.columns == original.columns
    fully = unmask.unmask_dataframe(masked, table="customer", reason="Roundtrip")
    assert Counter(tuple(r) for r in fully.collect()) == Counter(rows)
    partial = unmask.unmask_dataframe(masked, table="customer", columns=["email"], reason="Partial").collect()
    masked_rows = {r.id: r for r in masked.collect()}
    source_rows = {r[0]: r for r in rows}
    for row in partial:
        assert row.email == source_rows[row.id][1]
        assert row.phone == masked_rows[row.id].phone
        assert row.membership == masked_rows[row.id].membership
    statuses = [e["status"] for e in deps["audit"].events()]
    assert "PLAN_READY" in statuses
    assert "SUCCEEDED" not in statuses  # Returning a plan isn't a completed Spark action.
    with pytest.raises(ConfigurationError):
        mask.mask_dataframe(masked, table="customer")
    masked.unpersist()


def test_cross_table_consistency(spark, setup):
    _, _, deps, _ = setup
    params = narrow(deps, "email")
    raw = copy.deepcopy(params["config"].raw)
    raw["tables"]["booking"] = {"columns": {"contact": {"mask_type": "email", "domain": "email"}}}
    mask = MaskingEngine(**{**params, "config": Config(raw)})
    customer = spark.createDataFrame([("a@example.org",), ("b@example.org",)], "email STRING")
    booking = spark.createDataFrame([("a@example.org",), ("a@example.org",)], "contact STRING")
    left = mask.mask_dataframe(customer, table="customer")
    right = mask.mask_dataframe(booking, table="booking")
    assert left.join(right, left.email == right.contact).count() == 2


def test_missing_nulls_and_selection(spark, setup):
    _, _, deps, _ = setup
    unmask = UnmaskingEngine(**narrow(deps, "email"))
    unknown = spark.createDataFrame([("not-mapped",), (None,)], "email STRING")
    with pytest.raises(MissingMappingError):
        unmask.unmask_dataframe(unknown, table="customer", reason="Strict")
    kept = unmask.unmask_dataframe(unknown, table="customer", reason="Inspect", on_missing="keep_masked")
    assert Counter(tuple(r) for r in kept.collect()) == Counter(tuple(r) for r in unknown.collect())
    assert kept.schema["email"].metadata["wd_datamask"]["state"] == "partial"
    assert deps["audit"].events()[-1]["unresolved"] == {"email": 1}
    for columns in [[], ["email", "email"], ["unknown"]]:
        with pytest.raises(ConfigurationError):
            unmask.unmask_dataframe(unknown, table="customer", columns=columns, reason="Invalid selection")
    with pytest.raises(ConfigurationError):
        unmask.unmask_dataframe(spark.createDataFrame([(1,)], "email INT"), table="customer", reason="Invalid type")
    with pytest.raises(ConfigurationError):
        unmask.unmask_dataframe(unknown, table="customer", reason="Invalid policy", on_missing="invent")


def test_column_authorization(spark, setup):
    _, _, deps, grant = setup
    grant["columns"] = ["phone"]
    frame = spark.createDataFrame([("anything",)], "email STRING")
    unmask = UnmaskingEngine(**deps)
    with pytest.raises(PermissionError):
        unmask.unmask_dataframe(frame, table="customer", columns=["email"], reason="Restricted column")


def test_dry_run_has_no_allocations(spark, setup):
    _, _, deps, _ = setup
    params = narrow(deps, "email")
    raw = copy.deepcopy(params["config"].raw)
    raw["defaults"]["invalid_values"] = "error"
    engine = MaskingEngine(**{**params, "config": Config(raw)})
    frame = spark.createDataFrame([("a@example.org",), ("bad",), (None,)], "email STRING")
    report = engine.dry_run(frame, table="customer")
    assert report["records_processed"] == 3
    assert report["columns"]["email"]["missing_mappings"] == 2
    assert report["errors"] == 1
    assert deps["store"].all_for_scope(deps["config"].scope("email")) == []


def test_pure_udfs(spark):
    frame = spark.createDataFrame([("John",), (None,)], "value STRING")
    pandas_udf = make_lookup_pandas_udf({"John": "Michael"})
    assert [r[0] for r in frame.select(pandas_udf("value")).collect()] == ["Michael", None]
    scalar = register_lookup_udf(spark, "mask_test_name", {"John": "Michael"})
    assert [r[0] for r in frame.select(scalar("value")).collect()] == ["Michael", None]
    assert spark.sql("SELECT mask_test_name('John')").first()[0] == "Michael"
    for factory in [make_lookup_pandas_udf, lambda m: register_lookup_udf(spark, "oversize", m)]:
        with pytest.raises(ValueError):
            factory({str(i): str(i) for i in range(10001)})
    with pytest.raises(MissingMappingError):
        scalar.func("unknown")
    assert scalar.func(None) is None
    assert scalar.func("John") == "Michael"


def test_decryption_udf_contract(spark, setup):
    from wd_datamask.spark.udf_registry import decrypt_udf
    from dataclasses import asdict
    from pyspark.sql import Row
    mask, _, deps, _ = setup
    mask.mask_value("a@example.org", domain="email")
    scope = deps["config"].scope("email")
    item = deps["store"].all_for_scope(scope)[0]
    decrypt = decrypt_udf(deps["keys"], scope)
    assert decrypt.func(None) is None
    assert decrypt.func(Row(**asdict(item))) == "a@example.org"


def test_subsetting(spark):
    from pyspark.sql import functions as F
    parents = spark.createDataFrame([(1,), (2,)], "id INT")
    children = spark.createDataFrame([(1, "A"), (1, "B"), (2, "C"), (None, "D")], "parent_id INT, detail STRING")
    p, c = subset_related(parents, children, parent_key="id", child_key="parent_id", predicate=F.col("id") == 1)
    assert p.count() == 1 and c.count() == 2
    orphan = spark.createDataFrame([(3,)], "parent_id INT")
    with pytest.raises(IntegrityError):
        subset_related(parents, orphan, parent_key="id", child_key="parent_id", predicate=F.lit(True))
    with pytest.raises(IntegrityError):
        subset_related(parents.union(parents), children, parent_key="id", child_key="parent_id", predicate=F.lit(True))
