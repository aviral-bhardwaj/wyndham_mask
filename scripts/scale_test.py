"""End-to-end volume qualification: mask and unmask tens of millions of rows in one call.

Synthetic customer-activity rows (no real data) are generated deterministically from a
customer id, so the same customer repeats across rows and referential integrity can be
verified after masking. All seven mask types are exercised in a single
``mask_dataframe`` call, the result is written, then restored with ``unmask_dataframe``
and compared with the source using row counts and column hashes.

    python scripts/scale_test.py --rows 60000000 --customers 6000000 --backend parquet
    WD_DELTA_JARS=... python scripts/scale_test.py --rows 60000000 --backend delta

The parquet backend needs no Delta jars and is meant for single-node qualification;
the delta backend exercises DeltaRepository exactly as on Databricks.
"""
import argparse
import json
import os
from pathlib import Path
import shutil
import tempfile
import time
from wd_datamask import (Config, KeyRing, AuditManager, PermissionManager, LocalIdentity,
                         MaskingEngine, UnmaskingEngine, ParquetRepository, DeltaRepository)

STREETS = ["Main Street", "High Street", "Broadway", "Park Avenue", "First Street", "Elm Street",
           "Washington Avenue", "Lake Road", "Hill Road", "Mill Lane", "Church Street", "Station Road"]
DOMAINS = ["gmail.com", "yahoo.com", "outlook.com", "hotmail.com", "example.org"]

CONFIG = {
    "namespace": "scale_test", "mapping_version": "v1",
    "defaults": {"invalid_values": "replace", "email_domain": "example.com"},
    "tables": {"activity": {"columns": {
        "first_name": {"mask_type": "first_name", "domain": "first_name"},
        "last_name": {"mask_type": "last_name", "domain": "last_name"},
        "full_name": {"mask_type": "full_name", "domain": "full_name"},
        "email": {"mask_type": "email", "domain": "email"},
        "phone": {"mask_type": "phone", "domain": "phone"},
        "address_line_1": {"mask_type": "address", "domain": "address"},
        "membership_no": {"mask_type": "membership", "domain": "membership"},
    }}},
}
MASKED = list(CONFIG["tables"]["activity"]["columns"])


def synthetic(spark, rows, customers, partitions):
    """Activity rows keyed by a customer id; every attribute is a pure function of it."""
    from pyspark.sql import functions as F
    df = spark.range(0, rows, 1, partitions).withColumnRenamed("id", "activity_id")
    c = ((F.col("activity_id") * F.lit(2654435761)) % F.lit(customers)).alias("customer_id")
    df = df.withColumn("customer_id", c)
    cid = F.col("customer_id")
    first = F.concat(F.lit("First"), (cid % 25000).cast("string"))
    last = F.concat(F.lit("Last"), (cid % 120000).cast("string"))
    area = (F.lit(200) + (cid * 7) % 800).cast("string")
    exchange = (F.lit(200) + (cid * 13) % 800).cast("string")
    subscriber = F.lpad((cid % 10000).cast("string"), 4, "0")
    phone = (F.when(cid % 4 == 0, F.concat(F.lit("("), area, F.lit(") "), exchange, F.lit("-"), subscriber))
             .when(cid % 4 == 1, F.concat(area, F.lit("-"), exchange, F.lit("-"), subscriber))
             .when(cid % 4 == 2, F.concat(F.lit("+1 "), area, F.lit("."), exchange, F.lit("."), subscriber))
             .otherwise(F.concat(area, exchange, subscriber)))
    street = F.element_at(F.array(*[F.lit(s) for s in STREETS]), (cid % len(STREETS) + 1).cast("int"))
    domain = F.element_at(F.array(*[F.lit(s) for s in DOMAINS]), (cid % len(DOMAINS) + 1).cast("int"))
    letter = F.element_at(F.array(*[F.lit(chr(65 + i)) for i in range(26)]), (cid % 26 + 1).cast("int"))
    null_every = F.col("activity_id") % 101 == 0          # ~1% nulls in every masked column
    invalid_every = F.col("activity_id") % 997 == 0       # invalid formats that must still round-trip
    return (df.withColumn("first_name", F.when(null_every, None).otherwise(first))
              .withColumn("last_name", F.when(null_every, None).otherwise(last))
              .withColumn("full_name", F.when(null_every, None).otherwise(F.concat(first, F.lit(" "), last)))
              .withColumn("email", F.when(null_every, None).when(invalid_every, F.lit("not an email"))
                          .otherwise(F.concat(F.lower(first), F.lit("."), F.lower(last), cid.cast("string"), F.lit("@"), domain)))
              .withColumn("phone", F.when(null_every, None).when(invalid_every, F.lit("N/A")).otherwise(phone))
              .withColumn("address_line_1", F.when(null_every, None)
                          .otherwise(F.concat(((cid % 999999) + 1).cast("string"), F.lit(" "), street)))
              .withColumn("membership_no", F.when(null_every, None).when(invalid_every, F.lit("BAD-ID"))
                          .otherwise(F.concat(F.lpad(((cid * 31) % 1000000000).cast("string"), 9, "0"), letter)))
              .withColumn("amount", (F.col("activity_id") % 5000).cast("decimal(10,2)"))
              .withColumn("event_ts", F.timestamp_seconds(F.lit(1700000000) + F.col("activity_id") % 31536000)))


def column_hash(df, columns):
    from pyspark.sql import functions as F
    return df.select(F.sum(F.xxhash64(*[F.col(c) for c in columns])).alias("h"), F.count("*").alias("n")).first()


def main():
    from pyspark.sql import SparkSession, functions as F
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=60_000_000)
    parser.add_argument("--customers", type=int, default=6_000_000)
    parser.add_argument("--backend", choices=["parquet", "delta"], default="parquet")
    parser.add_argument("--workdir", default=None, help="directory for warehouse, data and spark temp files")
    parser.add_argument("--partitions", type=int, default=96)
    parser.add_argument("--driver-memory", default="4g")
    parser.add_argument("--cores", default="*")
    parser.add_argument("--keep", action="store_true", help="keep generated data after the run")
    parser.add_argument("--write-restored", action="store_true", help="also write the restored rows to parquet")
    parser.add_argument("--drop-source-early", action="store_true", help="delete the source parquet once masked (disk-bound hosts)")
    parser.add_argument("--codec", default="zstd", help="spark.io.compression.codec for shuffle/spill files")
    parser.add_argument("--broadcast-threshold", type=int, default=1_000_000)
    parser.add_argument("--report", default=None, help="write the JSON report to this path")
    args = parser.parse_args()
    workdir = Path(args.workdir or tempfile.mkdtemp(prefix="wd_scale_"))
    workdir.mkdir(parents=True, exist_ok=True)
    # local mode: the JVM is launched by PySpark, so driver memory must be passed at launch.
    os.environ.setdefault("PYSPARK_SUBMIT_ARGS", f"--driver-memory {args.driver_memory} pyspark-shell")
    builder = (SparkSession.builder.master(f"local[{args.cores}]").appName("wd-datamask-scale")
               .config("spark.driver.memory", args.driver_memory)
               .config("spark.sql.shuffle.partitions", str(args.partitions))
               .config("spark.sql.warehouse.dir", str(workdir / "warehouse"))
               .config("spark.local.dir", str(workdir / "tmp"))
               .config("spark.ui.enabled", "false")
               .config("spark.io.compression.codec", args.codec)
               .config("spark.sql.execution.arrow.maxRecordsPerBatch", "20000"))
    jars = os.environ.get("WD_DELTA_JARS")
    if args.backend == "delta":
        if not jars:
            parser.error("--backend delta requires WD_DELTA_JARS")
        builder = (builder.config("spark.jars", jars)
                   .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                   .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    report = {"rows": args.rows, "customers": args.customers, "backend": args.backend,
              "spark": spark.version, "cores": os.cpu_count(), "driver_memory": args.driver_memory, "timings": {}}
    timer = {}

    def stage(name):
        class _Timer:
            def __enter__(self):
                timer[name] = time.perf_counter()
            def __exit__(self, *exc):
                report["timings"][name] = round(time.perf_counter() - timer[name], 1)
                print(f"[{name}] {report['timings'][name]}s", flush=True)
        return _Timer()

    identity = LocalIdentity()
    config = Config(CONFIG)
    grant = {"principals": [identity.current().requester], "actions": ["MASK", "UNMASK"],
             "namespace": config.namespace, "domains": list(config.domains), "tables": ["*"], "columns": ["*"]}
    spark.sql("CREATE DATABASE IF NOT EXISTS scale")
    if args.backend == "delta":
        store = DeltaRepository(spark, "scale.vault", "scale.mutex")
    else:
        store = ParquetRepository(spark, "scale.vault", workdir / "locks")
    store.bootstrap()
    audit = AuditManager(workdir / "audit.sqlite")
    keys = KeyRing({"e1": os.urandom(32)}, {"f1": os.urandom(32)}, "e1", "f1")
    deps = dict(config=config, store=store, keys=keys, audit=audit, spark=spark,
                permissions=PermissionManager(identity, [grant]))
    masker = MaskingEngine(**deps, broadcast_threshold=args.broadcast_threshold)
    unmasker = UnmaskingEngine(**deps, broadcast_threshold=args.broadcast_threshold)
    report["broadcast_threshold"] = args.broadcast_threshold

    source_path, masked_path, restored_path = (str(workdir / n) for n in ("source", "masked", "restored"))
    with stage("generate_source"):
        synthetic(spark, args.rows, args.customers, args.partitions).write.mode("overwrite").parquet(source_path)
    source = spark.read.parquet(source_path)
    sample_path = str(workdir / "source_sample")
    with stage("source_stats"):
        stats = source.agg(F.count("*").alias("rows"), F.countDistinct("customer_id").alias("customers"),
                           *[F.countDistinct(c).alias(f"distinct_{c}") for c in MASKED],
                           *[F.sum(F.col(c).isNull().cast("int")).alias(f"nulls_{c}") for c in MASKED]).first().asDict()
        report["source"] = stats
        print(stats, flush=True)
        # Everything needed after masking that must not depend on the full source stays small.
        columns = ["activity_id", "customer_id", *MASKED, "amount", "event_ts"]
        source_hash = column_hash(source, columns)
        per_customer = lambda frame: frame.groupBy("customer_id").agg(*[F.countDistinct(c).alias(c) for c in MASKED]).agg(
            *[F.sum(c).alias(f"sum_{c}") for c in MASKED], *[F.max(c).alias(f"max_{c}") for c in MASKED]).first().asDict()
        source_per_customer = per_customer(source)
        source.where(F.col("activity_id") % 30 == 0).write.mode("overwrite").parquet(sample_path)

    with stage("mask_dataframe_and_write"):
        masked = masker.mask_dataframe(source, table="activity")
        masked.write.mode("overwrite").parquet(masked_path)
        masker.release()
    masked = spark.read.parquet(masked_path)

    with stage("verify_masked"):
        checks = {}
        masked_stats = masked.agg(F.count("*").alias("rows"), *[F.countDistinct(c).alias(f"distinct_{c}") for c in MASKED],
                                  *[F.sum(F.col(c).isNull().cast("int")).alias(f"nulls_{c}") for c in MASKED]).first().asDict()
        report["masked"] = masked_stats
        checks["row_count_preserved"] = masked_stats["rows"] == stats["rows"]
        checks["columns_preserved"] = masked.columns == source.columns
        checks["null_counts_preserved"] = all(masked_stats[f"nulls_{c}"] == stats[f"nulls_{c}"] for c in MASKED)
        checks["distinct_counts_preserved"] = all(masked_stats[f"distinct_{c}"] == stats[f"distinct_{c}"] for c in MASKED)
        # Row-level checks on the retained 1-in-30 sample (the full wide join would need
        # more scratch disk than the qualification host has): every non-null value changed,
        # null positions are unchanged and valid phones keep their separator layout.
        paired = (spark.read.parquet(sample_path).alias("s")
                  .join(masked.where(F.col("activity_id") % 30 == 0).alias("m"), "activity_id"))
        unchanged = [(F.col(f"s.{c}").isNull() != F.col(f"m.{c}").isNull()) | (F.col(f"s.{c}") == F.col(f"m.{c}")) for c in MASKED]
        checks["values_changed_nulls_kept"] = paired.where(unchanged[0] | unchanged[1] | unchanged[2] | unchanged[3]
                                                           | unchanged[4] | unchanged[5] | unchanged[6]).limit(1).count() == 0
        checks["phone_shape_preserved"] = paired.where(
            F.col("s.phone").isNotNull() & (F.col("s.phone") != "N/A")
            & (F.regexp_replace(F.col("s.phone"), "[0-9]", "D") != F.regexp_replace(F.col("m.phone"), "[0-9]", "D"))).limit(1).count() == 0
        # Referential integrity: per-customer distinct-value profile is identical to the source
        # (one value per customer; invalid placeholder rows add a second) and cardinalities match.
        checks["one_masked_value_per_customer"] = per_customer(masked) == source_per_customer
        sample = masked.where(F.col("email").isNotNull()).limit(200000)
        checks["email_domain_applied"] = sample.where(~F.col("email").endswith("@example.com")).count() == 0
        checks["membership_format"] = sample.where(~F.col("membership_no").rlike(r"^[0-9]{9}[A-Za-z]$")).count() == 0
        checks["names_are_readable"] = sample.where(~F.col("first_name").rlike(r"^[A-Z][a-z]+(-[A-Z][a-z]+){0,2}$")).count() == 0
        report["checks_masked"] = checks
        print(checks, flush=True)
        examples = masked.select(*MASKED).where(F.col("email").isNotNull()).limit(50000).distinct().limit(5).collect()
        report["masked_examples"] = [r.asDict() for r in examples]
        print(examples, flush=True)
    if args.drop_source_early:
        shutil.rmtree(source_path, ignore_errors=True)  # disk-bound hosts: the hash and sample were kept

    with stage("unmask_dataframe_and_verify"):
        restored = unmasker.unmask_dataframe(masked, table="activity", reason="Scale qualification round trip")
        if args.write_restored:
            restored.write.mode("overwrite").parquet(restored_path)
            restored = spark.read.parquet(restored_path)
        a, b = source_hash, column_hash(restored, columns)
        unmasker.release()
        report["checks_restored"] = {"row_count": a.n == b.n, "content_hash": a.h == b.h,
                                     "source_hash": a.h, "restored_hash": b.h}
        print(report["checks_restored"], flush=True)

    with stage("repeat_mask_is_idempotent"):
        again = masker.mask_dataframe(spark.read.parquet(sample_path), table="activity")
        first = masked.alias("m").join(again.alias("a"), "activity_id")
        report["repeat_mask_consistent"] = first.where(" OR ".join(f"m.{c} <=> a.{c} = false" for c in MASKED)).limit(1).count() == 0
        masker.release()
        print({"repeat_mask_consistent": report["repeat_mask_consistent"]}, flush=True)

    vault = spark.table("scale.vault")
    report["vault_rows"] = vault.count()
    report["vault_duplicate_keys"] = vault.groupBy("scope", "masked_value").count().where("count > 1").count() \
        + vault.groupBy("scope", "fingerprint").count().where("count > 1").count()
    report["audit_events"] = len(audit.events())
    report["passed"] = (all(report["checks_masked"].values()) and report["checks_restored"]["row_count"]
                        and report["checks_restored"]["content_hash"] and report["repeat_mask_consistent"]
                        and report["vault_duplicate_keys"] == 0)
    print(json.dumps(report, indent=2, default=str), flush=True)
    if args.report:
        Path(args.report).write_text(json.dumps(report, indent=2, default=str))
    audit.close()
    spark.stop()
    if not args.keep:
        shutil.rmtree(workdir, ignore_errors=True)
    raise SystemExit(0 if report["passed"] else 1)


if __name__ == "__main__":
    main()
