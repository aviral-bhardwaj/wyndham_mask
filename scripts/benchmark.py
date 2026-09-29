"""Synthetic Spark benchmark; use the Delta backend in Databricks for scale qualification.

Run from the repository: python scripts/benchmark.py --rows 100000 --distinct 1000
The local SQLite backend is intentionally bounded; this measures Spark transform
costs, not production Delta allocation throughput.
"""
import argparse
import os
from pathlib import Path
import tempfile
import time
import json
from wd_datamask import (Config, KeyRing, MappingStore, AuditManager, PermissionManager,
                        LocalIdentity, MaskingEngine, UnmaskingEngine)


def main():
    from pyspark.sql import SparkSession, functions as F
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=100000)
    parser.add_argument("--distinct", type=int, default=1000)
    args = parser.parse_args()
    if args.rows < 1 or not 1 <= args.distinct <= 10000:
        parser.error("Use positive rows and 1..10000 distinct values for the local backend")
    spark = SparkSession.builder.master("local[2]").appName("wd-datamask-benchmark").getOrCreate()
    config = Config({"namespace": "benchmark", "mapping_version": "v1", "tables": {
        "customer": {"columns": {"email": {"mask_type": "email", "domain": "email"}}}}})
    identity = LocalIdentity()
    grant = {"principals": [identity.current().requester], "namespace": "benchmark",
             "domains": ["email"], "tables": ["customer"], "columns": ["email"], "actions": ["MASK", "UNMASK"]}
    with tempfile.TemporaryDirectory() as tmp:
        store, audit = MappingStore(Path(tmp) / "vault.sqlite"), AuditManager(Path(tmp) / "audit.sqlite")
        deps = dict(config=config, keys=KeyRing({"e": os.urandom(32)}, {"f": os.urandom(32)}, "e", "f"),
                    store=store, audit=audit, permissions=PermissionManager(identity, [grant]), spark=spark)
        mask, unmask = MaskingEngine(**deps), UnmaskingEngine(**deps)
        source = spark.range(args.rows).select("id", F.concat(F.lit("guest"), (F.col("id") % args.distinct).cast("string"), F.lit("@example.org")).alias("email"))
        report = {"rows": args.rows, "distinct": min(args.rows, args.distinct), "spark": spark.version,
                  "backend": "SQLite reference; not a Delta scale qualification"}
        def materialize(frame):
            # count() alone can prune the projected masking/decryption expressions.
            return frame.select(F.sum(F.length("email"))).first()[0]
        start = time.perf_counter()
        masked = mask.mask_dataframe(source, table="customer").cache()
        materialize(masked)
        report["initial_mask_seconds"] = time.perf_counter() - start
        start = time.perf_counter()
        materialize(mask.mask_dataframe(source, table="customer"))
        report["repeat_mask_seconds"] = time.perf_counter() - start
        start = time.perf_counter()
        materialize(unmask.unmask_dataframe(masked, table="customer", reason="Synthetic benchmark"))
        report["unmask_seconds"] = time.perf_counter() - start
        print(json.dumps(report, indent=2))
        masked.unpersist()
        store.close()
        audit.close()
    spark.stop()


if __name__ == "__main__":
    main()
