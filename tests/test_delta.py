import copy
import os
import uuid
import pytest
from wd_datamask import Config, DeltaRepository, DeltaAuditManager, MaskingEngine, UnmaskingEngine
from wd_datamask.exceptions import AllocationBusyError, ConfigurationError, IntegrityError, RecoveryRequiredError, AuditError
from wd_datamask.spark.dataframe_masker import _read_manifest, _publish

pytestmark = [pytest.mark.delta, pytest.mark.skipif(not os.environ.get("WD_DELTA_JARS"), reason="Set WD_DELTA_JARS to test actual Delta storage")]


@pytest.fixture
def delta_setup(spark, setup):
    _, _, deps, grant = setup
    name = "dm_" + uuid.uuid4().hex[:12]
    spark.sql(f"CREATE DATABASE {name}")
    raw = copy.deepcopy(deps["config"].raw)
    raw["tables"]["customer"]["columns"] = {"email": {"mask_type": "email", "domain": "email"}}
    repo = DeltaRepository(spark, f"{name}.vault", f"{name}.mutex")
    repo.bootstrap()
    spark.sql(f"CREATE TABLE {name}.audit (event STRING) USING DELTA")
    grant["destinations"] = [f"{name}.masked", f"{name}.restored", f"{name}.restored2"]
    params = {**deps, "spark": spark, "config": Config(raw), "store": repo, "audit": DeltaAuditManager(spark, f"{name}.audit")}
    yield name, params, grant
    spark.sql(f"DROP DATABASE {name} CASCADE")


def test_saved_delta_roundtrip_fresh_engine(spark, delta_setup):
    name, deps, _ = delta_setup
    original = spark.createDataFrame([(1, "john@example.net"), (1, "john@example.net"), (2, "bad"), (3, None)], "id INT, email STRING")
    original.write.format("delta").saveAsTable(f"{name}.source")
    mask = MaskingEngine(**deps)
    result = mask.mask_table(source_table=f"{name}.source", target_table=f"{name}.masked", table="customer")
    assert result["records"] == 4
    manifest, config = _read_manifest(spark, f"{name}.masked")
    assert config.digest == deps["config"].digest
    assert manifest["source_version"] == 0
    # Simulate newer current YAML. Restore must use the dataset's stored v1 manifest.
    new_raw = copy.deepcopy(deps["config"].raw)
    new_raw["mapping_version"] = "v2"
    fresh = DeltaRepository(spark, f"{name}.vault", f"{name}.mutex")
    unmask = UnmaskingEngine(**{**deps, "config": Config(new_raw), "store": fresh})
    restored = unmask.unmask_table(source_table=f"{name}.masked", target_table=f"{name}.restored", table="customer", reason="Saved dataset restoration")
    assert restored["records"] == 4
    actual = spark.table(f"{name}.restored")
    assert actual.exceptAll(original).count() == original.exceptAll(actual).count() == 0
    assert spark.table(f"{name}.audit").where("event LIKE '%SUCCEEDED%'").count() == 2
    fresh.validate(config.scope("email"))
    with pytest.raises(ConfigurationError):
        unmask.unmask_table(source_table=f"{name}.masked", target_table=f"{name}.restored", table="customer", reason="Do not overwrite")
    with pytest.raises(ConfigurationError):
        unmask.unmask_table(source_table=f"{name}.masked", target_table=f"{name}.restored2", table="customer", reason="Do not append", write_mode="append")


def test_mutex_and_recovery(spark, delta_setup):
    name, deps, _ = delta_setup
    other = DeltaRepository(spark, f"{name}.vault", f"{name}.mutex")
    with pytest.raises(IntegrityError):
        other.insert([])
    with deps["store"].allocation():
        with pytest.raises(AllocationBusyError):
            with other.allocation():
                pass
    with other.allocation():
        pass
    # An abandoned lock is not stolen just because it has been held a long time.
    spark.sql(f"UPDATE {name}.mutex SET owner='abandoned-request'")
    with pytest.raises(AllocationBusyError):
        with other.allocation():
            pass
    spark.sql(f"UPDATE {name}.mutex SET owner=NULL")  # operator-only recovery after stopping owner


def test_missing_manifest_and_denied_destination(spark, delta_setup):
    name, deps, _ = delta_setup
    spark.createDataFrame([("value",)], "email STRING").write.format("delta").saveAsTable(f"{name}.source")
    unmask = UnmaskingEngine(**deps)
    with pytest.raises(IntegrityError):
        unmask.unmask_table(source_table=f"{name}.source", target_table=f"{name}.restored", table="customer", reason="No manifest")
    with pytest.raises(PermissionError):
        MaskingEngine(**deps).mask_table(source_table=f"{name}.source", target_table=f"{name}.unauthorized", table="customer")
    assert not spark.catalog.tableExists(f"{name}.unauthorized")


def test_audit_failure_does_not_publish_stage(spark, delta_setup):
    name, deps, _ = delta_setup
    source = spark.createDataFrame([("synthetic@example.org",)], "email STRING")
    source.write.format("delta").saveAsTable(f"{name}.source")
    original_sink = deps["audit"]
    class FailPrepared:
        def emit(self, **event):
            if event["status"] == "PREPARED":
                raise AuditError("Simulated outage")
            original_sink.emit(**event)
    engine = MaskingEngine(**{**deps, "audit": FailPrepared()})
    with pytest.raises(AuditError):
        engine.mask_table(source_table=f"{name}.source", target_table=f"{name}.masked", table="customer")
    assert not spark.catalog.tableExists(f"{name}.masked")
    assert not [t for t in spark.catalog.listTables(name) if t.name.startswith("_wd_stage_")]


def test_post_commit_audit_failure_requires_reconciliation(spark, delta_setup):
    name, deps, _ = delta_setup
    sink = deps["audit"]
    class FailCompletion:
        def emit(self, **event):
            if event["status"] in {"SUCCEEDED", "FAILED"}:
                raise AuditError("Simulated final audit outage")
            sink.emit(**event)
    engine = MaskingEngine(**{**deps, "audit": FailCompletion()})
    source = spark.createDataFrame([("synthetic",)], "email STRING")
    context = {"request_id": "recovery-test", "action": "UNMASK"}
    with pytest.raises(RecoveryRequiredError, match="recovery-test"):
        _publish(engine, source, f"{name}.restored", {"test": True}, context)
    assert spark.catalog.tableExists(f"{name}.restored")
    assert spark.table(f"{name}.audit").where("event LIKE '%PREPARED%'").count() == 1


def test_simultaneous_delta_allocators(spark, delta_setup):
    from concurrent.futures import ThreadPoolExecutor
    import threading
    name, deps, _ = delta_setup
    barrier = threading.Barrier(2)
    loser_done = threading.Event()
    def attempt():
        repo = DeltaRepository(spark, f"{name}.vault", f"{name}.mutex")
        barrier.wait(timeout=15)
        try:
            with repo.allocation():
                # Keep the winner's lock until the contender has observed contention.
                assert loser_done.wait(timeout=30)
                return "owner"
        except AllocationBusyError:
            loser_done.set()
            return "contended"
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: attempt(), range(2)))
    assert sorted(results) == ["contended", "owner"]
    assert spark.table(f"{name}.mutex").first().owner is None


def test_delta_rotation_and_corruption_detection(spark, delta_setup):
    from wd_datamask import KeyRing
    from wd_datamask.exceptions import AmbiguousMappingError
    name, deps, _ = delta_setup
    mask = MaskingEngine(**deps)
    masked = mask.mask_value("a@example.org", domain="email")
    scope = deps["config"].scope("email")
    old = deps["keys"]
    keys = KeyRing({**old.encryption_keys, "e2": os.urandom(32)}, old.fingerprint_keys, "e2", "f1")
    assert MaskingEngine(**{**deps, "keys": keys}).rotate_encryption("email", reason="Rotation integration") == 1
    assert UnmaskingEngine(**{**deps, "keys": keys}).unmask_value(masked, domain="email", reason="Rotated") == "a@example.org"
    # Simulate an operator bypassing the allocator. The readers must refuse ambiguity.
    vault_copy = spark.table(f"{name}.vault").collect()
    spark.createDataFrame(vault_copy, spark.table(f"{name}.vault").schema).write.format("delta").mode("append").saveAsTable(f"{name}.vault")
    with pytest.raises(AmbiguousMappingError):
        deps["store"].validate(scope)
    with pytest.raises(AmbiguousMappingError):
        UnmaskingEngine(**deps).unmask_value(masked, domain="email", reason="Corrupt store")
