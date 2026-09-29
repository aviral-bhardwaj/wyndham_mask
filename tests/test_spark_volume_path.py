"""Distributed allocation path (the one used for large volumes) on a Parquet-backed vault."""
from collections import Counter
import copy
import os
import uuid
import pytest
from wd_datamask import Config, KeyRing, MaskingEngine, UnmaskingEngine, ParquetRepository
from wd_datamask.exceptions import (AllocationBusyError, ConfigurationError, IntegrityError,
                                    MappingCapacityError, MissingMappingError)
from wd_datamask.maskers.base import NameMasker

pytestmark = pytest.mark.spark

COLUMNS = ["first_name", "last_name", "full_name", "email", "phone", "address", "membership"]


@pytest.fixture
def parquet_setup(spark, setup, tmp_path):
    _, _, deps, grant = setup
    name = "pq_" + uuid.uuid4().hex[:12]
    spark.sql(f"CREATE DATABASE {name}")
    repo = ParquetRepository(spark, f"{name}.vault", tmp_path / "locks")
    repo.bootstrap()
    params = {**deps, "spark": spark, "store": repo}
    yield name, params, grant
    spark.sql(f"DROP DATABASE {name} CASCADE")


def narrow(deps, *columns):
    raw = copy.deepcopy(deps["config"].raw)
    raw["tables"]["customer"]["columns"] = {k: v for k, v in raw["tables"]["customer"]["columns"].items() if k in columns}
    return {**deps, "config": Config(raw)}


def sample_rows(n):
    rows = []
    for i in range(n):
        c = i % (n // 3)  # every customer appears three times
        if i % 17 == 0:
            rows.append((i, None, None, None, None, None, None, None))
        elif i % 23 == 0:
            rows.append((i, f"F{c}", f"L{c}", f"F{c} L{c}", "not-an-email", "bad phone", f"{c} Src St", "BAD"))
        else:
            rows.append((i, f"F{c}", f"L{c}", f"F{c} L{c}", f"user{c}@source.org", f"({c % 800 + 200:03d}) 555-{c % 10000:04d}",
                         f"{c} Src St", f"{c:09d}{'AZ'[c % 2]}"))
    return rows


@pytest.mark.parametrize("broadcast_threshold", [0, 500_000])
def test_distributed_roundtrip_all_types(spark, parquet_setup, broadcast_threshold):
    from pyspark.sql import functions as F
    name, deps, _ = parquet_setup
    rows = sample_rows(600)
    schema = "id INT, " + ", ".join(f"{c} STRING" for c in COLUMNS)
    source = spark.createDataFrame(rows, schema)
    mask = MaskingEngine(**deps, broadcast_threshold=broadcast_threshold)
    unmask = UnmaskingEngine(**deps, broadcast_threshold=broadcast_threshold)
    # Force multiple probing rounds and the compound tiers with a tiny pool.
    mask.maskers["first_name"] = NameMasker(["Ann", "Bob", "Cal", "Dee", "Eve", "Fay", "Gus", "Hal", "Ivy", "Jon"])
    masked = mask.mask_dataframe(source, table="customer")
    masked.write.mode("overwrite").parquet(f"/tmp/{name}_masked")
    mask.release()
    masked = spark.read.parquet(f"/tmp/{name}_masked")
    assert masked.count() == len(rows)
    # Nulls preserved, non-null values changed, distinct cardinalities preserved.
    both = source.alias("s").join(masked.alias("m"), "id")
    for column in COLUMNS:
        assert both.where(F.col(f"s.{column}").isNull() != F.col(f"m.{column}").isNull()).count() == 0
        assert both.where(F.col(f"s.{column}") == F.col(f"m.{column}")).count() == 0
        assert masked.select(column).distinct().count() == source.select(column).distinct().count()
    # Referential integrity: the same original always gets the same substitute.
    assert both.groupBy("s.email").agg(F.countDistinct("m.email").alias("n")).where("n > 1").count() == 0
    firsts = [r[0] for r in masked.select("first_name").distinct().collect() if r[0]]
    assert all(part in {"Ann", "Bob", "Cal", "Dee", "Eve", "Fay", "Gus", "Hal", "Ivy", "Jon"} for f in firsts for part in f.split("-"))
    assert any("-" in f for f in firsts)  # pool of 10 plain names, 200 distinct originals
    assert all(r[0].endswith("@example.com") for r in masked.select("email").where("email is not null").collect())
    restored = unmask.unmask_dataframe(masked, table="customer", reason="Volume path round trip")
    assert Counter(tuple(r) for r in restored.collect()) == Counter(rows)
    unmask.release()
    # A second masking call reuses the vault and allocates nothing new.
    before = spark.table(f"{name}.vault").count()
    again = mask.mask_dataframe(source, table="customer")
    assert again.exceptAll(masked).count() == 0
    assert spark.table(f"{name}.vault").count() == before
    assert deps["store"].validate(deps["config"].scope("email")) is None


def test_capacity_exhaustion_is_atomic_and_invalid_values_rejected(spark, parquet_setup):
    name, deps, _ = parquet_setup
    mask = MaskingEngine(**narrow(deps, "first_name"))
    mask.maskers["first_name"] = NameMasker(["Ann", "Bob"], compound=False)
    frame = spark.createDataFrame([(f"F{i}",) for i in range(5)], "first_name STRING")
    with pytest.raises(MappingCapacityError):
        mask.mask_dataframe(frame, table="customer")
    assert spark.table(f"{name}.vault").count() == 0
    strict = copy.deepcopy(narrow(deps, "email")["config"].raw)
    strict["defaults"]["invalid_values"] = "error"
    engine = MaskingEngine(**{**deps, "config": Config(strict)})
    with pytest.raises(ConfigurationError, match="email"):
        engine.mask_dataframe(spark.createDataFrame([("bad",)], "email STRING"), table="customer")
    assert spark.table(f"{name}.vault").count() == 0
    assert deps["audit"].events()[-1]["status"] == "FAILED"


def test_parquet_lock_and_rotation(spark, parquet_setup, tmp_path):
    name, deps, _ = parquet_setup
    repo = deps["store"]
    other = ParquetRepository(spark, f"{name}.vault", tmp_path / "locks")
    with pytest.raises(IntegrityError):
        other.insert([])
    with repo.allocation():
        with pytest.raises(AllocationBusyError):
            with other.allocation():
                pass
        with pytest.raises(AllocationBusyError):
            with repo.allocation():
                pass
    with other.allocation():
        other.insert([])
    mask = MaskingEngine(**deps)
    masked = mask.mask_value("rotate@example.org", domain="email")
    old = deps["keys"]
    keys = KeyRing({**old.encryption_keys, "e2": os.urandom(32)}, old.fingerprint_keys, "e2", "f1")
    assert MaskingEngine(**{**deps, "keys": keys}).rotate_encryption("email", reason="Parquet rotation") == 1
    only_new = KeyRing({"e2": keys.encryption_keys["e2"]}, old.fingerprint_keys, "e2", "f1")
    assert UnmaskingEngine(**{**deps, "keys": only_new}).unmask_value(masked, domain="email", reason="After rotation") == "rotate@example.org"
    assert spark.table(f"{name}.vault").count() == 1
    with pytest.raises(ValueError):
        ParquetRepository(spark, "bad name", tmp_path)


def test_dry_run_summary_and_missing_policy(spark, parquet_setup):
    name, deps, _ = parquet_setup
    mask, unmask = MaskingEngine(**deps), UnmaskingEngine(**deps)
    frame = spark.createDataFrame([("John", "a@example.org", "(555) 123-4567"), ("Jane", "bad", None), (None, None, None)],
                                  "first_name STRING, email STRING, phone STRING")
    raw = copy.deepcopy(deps["config"].raw)
    raw["tables"]["customer"]["columns"] = {k: v for k, v in raw["tables"]["customer"]["columns"].items() if k in {"first_name", "email", "phone"}}
    engine = MaskingEngine(**{**deps, "config": Config(raw)})
    report = engine.dry_run(frame, table="customer")
    assert report["records_processed"] == 3 and report["names_masked"] == 2
    assert report["emails_masked"] == 2 and report["phones_masked"] == 1 and report["errors"] == 0
    assert report["columns"]["email"]["missing_mappings"] == 2
    assert spark.table(f"{name}.vault").count() == 0
    masked = engine.mask_dataframe(frame, table="customer")
    unknown = masked.union(spark.createDataFrame([("Zed", "nobody@example.com", "000")], masked.schema))
    restorer = UnmaskingEngine(**{**deps, "config": Config(raw)})
    with pytest.raises(MissingMappingError):
        restorer.unmask_dataframe(unknown, table="customer", reason="Strict")
    kept = restorer.unmask_dataframe(unknown, table="customer", reason="Lenient", on_missing="keep_masked").collect()
    assert sorted(r.first_name or "" for r in kept) == ["", "Jane", "John", "Zed"]
    assert deps["audit"].events()[-1]["unresolved"] == {"first_name": 1, "email": 1, "phone": 1}
    assert kept[0].__fields__ == ["first_name", "email", "phone"]
    engine.release()
    assert engine._tracked == []


def test_reliable_checkpoint_dir_is_used(spark, parquet_setup, tmp_path):
    name, deps, _ = parquet_setup
    # Keep this the last Spark test: the checkpoint directory stays set for the session.
    spark.sparkContext.setCheckpointDir(str(tmp_path / "checkpoints"))
    mask = MaskingEngine(**narrow(deps, "email"))
    frame = spark.createDataFrame([(f"user{i}@example.org",) for i in range(50)], "email STRING")
    assert mask.mask_dataframe(frame, table="customer").select("email").distinct().count() == 50
    assert any(p.is_dir() for p in (tmp_path / "checkpoints").iterdir())
