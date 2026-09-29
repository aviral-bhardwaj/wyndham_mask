"""Synthetic Spark benchmark of the distributed masking path on a single machine.

Run from the repository, e.g.:
    python scripts/benchmark.py --rows 5000000 --distinct 1000000
It uses ParquetRepository (no Delta jars needed); use the Delta backend on Databricks
for production qualification. scripts/scale_test.py is the full 60M-row round trip.
"""
import argparse
import os
from pathlib import Path
import tempfile
import time
import json
from wd_datamask import (Config, KeyRing, ParquetRepository, AuditManager, PermissionManager,
                         LocalIdentity, MaskingEngine, UnmaskingEngine)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=1_000_000)
    parser.add_argument("--distinct", type=int, default=100_000)
    parser.add_argument("--driver-memory", default="4g")
    args = parser.parse_args()
    if args.rows < 1 or args.distinct < 1:
        parser.error("Use positive rows and distinct values")
    os.environ.setdefault("PYSPARK_SUBMIT_ARGS", f"--driver-memory {args.driver_memory} pyspark-shell")
    from pyspark.sql import SparkSession, functions as F
    with tempfile.TemporaryDirectory() as tmp:
        spark = (SparkSession.builder.master("local[*]").appName("wd-datamask-benchmark")
                 .config("spark.sql.warehouse.dir", str(Path(tmp) / "warehouse"))
                 .config("spark.ui.enabled", "false").getOrCreate())
        spark.sparkContext.setLogLevel("ERROR")
        config = Config({"namespace": "benchmark", "mapping_version": "v1", "tables": {
            "customer": {"columns": {"email": {"mask_type": "email", "domain": "email"}}}}})
        identity = LocalIdentity()
        grant = {"principals": [identity.current().requester], "namespace": "benchmark",
                 "domains": ["email"], "tables": ["customer"], "columns": ["email"], "actions": ["MASK", "UNMASK"]}
        spark.sql("CREATE DATABASE IF NOT EXISTS benchmark")
        store = ParquetRepository(spark, "benchmark.vault", Path(tmp) / "locks")
        store.bootstrap()
        audit = AuditManager(Path(tmp) / "audit.sqlite")
        deps = dict(config=config, keys=KeyRing({"e": os.urandom(32)}, {"f": os.urandom(32)}, "e", "f"),
                    store=store, audit=audit, permissions=PermissionManager(identity, [grant]), spark=spark)
        mask, unmask = MaskingEngine(**deps), UnmaskingEngine(**deps)
        source = spark.range(args.rows).select("id", F.concat(F.lit("guest"), (F.col("id") % args.distinct).cast("string"), F.lit("@example.org")).alias("email"))
        report = {"rows": args.rows, "distinct": min(args.rows, args.distinct), "spark": spark.version,
                  "cores": os.cpu_count(), "backend": "ParquetRepository (single machine)"}

        def materialize(frame):
            # count() alone can prune the projected masking/decryption expressions.
            return frame.select(F.sum(F.length("email"))).first()[0]

        start = time.perf_counter()
        masked = mask.mask_dataframe(source, table="customer").cache()
        materialize(masked)
        report["initial_mask_seconds"] = round(time.perf_counter() - start, 1)
        start = time.perf_counter()
        materialize(mask.mask_dataframe(source, table="customer"))
        report["repeat_mask_seconds"] = round(time.perf_counter() - start, 1)
        start = time.perf_counter()
        materialize(unmask.unmask_dataframe(masked, table="customer", reason="Synthetic benchmark"))
        report["unmask_seconds"] = round(time.perf_counter() - start, 1)
        report["vault_rows"] = spark.table("benchmark.vault").count()
        print(json.dumps(report, indent=2))
        masked.unpersist()
        mask.release()
        unmask.release()
        audit.close()
        spark.stop()


if __name__ == "__main__":
    main()
