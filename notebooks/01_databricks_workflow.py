# Databricks notebook source
# MAGIC %md
# MAGIC # Mask and unmask synthetic customer data
# MAGIC Run only on trusted compute with access to the restricted vault and secrets.
# MAGIC Follow docs/SECURITY.md first. Edit the constants and policy below as an operator.
# MAGIC This notebook never prints original or restored records.

# COMMAND ----------

# MAGIC %pip install /Workspace/Users/ardb40@gmail.com/wyndham_mask/ --force-reinstall --no-deps

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import hashlib
from wd_datamask import (Config, KeyRing, DeltaRepository, DeltaAuditManager,
                        DatabricksIdentity, PermissionManager, MaskingEngine, UnmaskingEngine)

# Provision these catalogs/schemas in the operator bootstrap step first.
CATALOG = "dont_touch_this_catalog"
SECURITY_SCHEMA = f"{CATALOG}.security"
DATA_SCHEMA = f"{CATALOG}.restricted"
VAULT = f"{SECURITY_SCHEMA}.value_mappings"
LOCK = f"{SECURITY_SCHEMA}.allocation_lock"
AUDIT = f"{SECURITY_SCHEMA}.audit_events"
SOURCE = f"{DATA_SCHEMA}.source_customer"
MASKED = f"{DATA_SCHEMA}.masked_customer"
RESTORED = f"{DATA_SCHEMA}.restored_customer"
# Resolve the current session principal for the permission grant.
APPROVED_PRINCIPAL = spark.sql("SELECT session_user()").first()[0]

config = Config({
    "namespace": "wyndham_shared_test", "mapping_version": "v1",
    "defaults": {"invalid_values": "replace", "email_domain": "example.com"},
    "tables": {"customer": {"columns": {
        "first_name": {"mask_type": "first_name", "domain": "customer_first_name"},
        "email": {"mask_type": "email", "domain": "customer_email"},
        "phone": {"mask_type": "phone", "domain": "customer_phone"},
    }}},
})
keys = KeyRing(
    {"enc-v1": hashlib.sha256(b"demo-encryption-key-v1").digest()},
    {"fp-v1": hashlib.sha256(b"demo-fingerprint-key-v1").digest()},
    "enc-v1", "fp-v1",
)
permissions = PermissionManager(DatabricksIdentity(spark), [{
    "principals": [APPROVED_PRINCIPAL], "actions": ["MASK", "UNMASK"],
    "namespace": config.namespace, "domains": list(config.domains),
    "allow_values": True, "tables": ["customer"], "columns": ["first_name", "email", "phone"],
    "destinations": [MASKED, RESTORED],
}])
store = DeltaRepository(spark, VAULT, LOCK)
audit = DeltaAuditManager(spark, AUDIT)
deps = dict(spark=spark, config=config, keys=keys, store=store, permissions=permissions, audit=audit)
masker, unmasker = MaskingEngine(**deps), UnmaskingEngine(**deps)

# COMMAND ----------

# DBTITLE 1,Create catalog and schemas
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SECURITY_SCHEMA}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {DATA_SCHEMA}")

# COMMAND ----------

# Operator-only, first run: bootstrap tables before allowing allocation jobs.
store.bootstrap()
spark.sql(f"CREATE TABLE IF NOT EXISTS {AUDIT} (event STRING) USING DELTA")

# COMMAND ----------

source_df = spark.createDataFrame([
    (1, "John", "john.smith@example.org", "(555) 123-4567"),
    (2, "José", "invalid email", "invalid phone"),
    (3, None, None, None),
], "customer_id INT, first_name STRING, email STRING, phone STRING")
# Skip source write if table already exists (idempotent re-run).
if not spark.catalog.tableExists(SOURCE):
    source_df.write.format("delta").saveAsTable(SOURCE)
report = masker.dry_run(source_df, table="customer")
assert report["records_processed"] == 3
print({k: report[k] for k in ("records_processed", "names_masked", "emails_masked", "phones_masked", "errors")})
# Skip masking if target already exists (deterministic keys make this safe).
if not spark.catalog.tableExists(MASKED):
    mask_result = masker.mask_table(source_table=SOURCE, target_table=MASKED, table="customer")
    assert mask_result["records"] == 3
else:
    print(f"{MASKED} already exists, skipping masking.")

# COMMAND ----------

# Selected-column unmasking. Other columns keep their masked values.
selected_df = unmasker.unmask_dataframe(
    spark.table(MASKED), table="customer", columns=["email", "phone"],
    reason="Synthetic selected-column validation",
)
expected = source_df.select("customer_id", "email", "phone")
actual = selected_df.select("customer_id", "email", "phone")
assert actual.exceptAll(expected).count() == expected.exceptAll(actual).count() == 0
unmasker.release()  # drop the cached lookups once the DataFrame has been consumed

# COMMAND ----------

# Fresh engine; no in-memory mapping cache is required. This writes a new restricted
# table, using the persisted dataset manifest and historical mapping version.
fresh_unmasker = UnmaskingEngine(**deps)
if not spark.catalog.tableExists(RESTORED):
    restore_result = fresh_unmasker.unmask_table(
        source_table=MASKED, target_table=RESTORED, table="customer", columns=None,
        reason="Synthetic complete restoration validation",
    )
    assert restore_result["records"] == 3
else:
    print(f"{RESTORED} already exists, skipping unmask_table.")
restored = spark.table(RESTORED)
assert restored.exceptAll(source_df).count() == source_df.exceptAll(restored).count() == 0

# COMMAND ----------

# Negative demonstration: a policy without any grants cannot unmask.
denied = UnmaskingEngine(**{**deps, "permissions": PermissionManager(DatabricksIdentity(spark), [])})
try:
    denied.unmask_dataframe(spark.table(MASKED), table="customer", columns=["email"], reason="Denied demo")
    raise AssertionError("Unauthorized request unexpectedly succeeded")
except PermissionError:
    pass

# COMMAND ----------

# MAGIC %md
# MAGIC For large tables (tens of millions of rows) the same calls apply unchanged: allocation
# MAGIC runs as distributed Spark joins over the distinct values of each column and the vault
# MAGIC receives one append per column. Optionally call `spark.sparkContext.setCheckpointDir(...)`
# MAGIC with a reliable location so allocation rounds use durable checkpoints on autoscaling
# MAGIC clusters. See docs/VALIDATION.md for the measured 60M-row qualification.

# COMMAND ----------

# MAGIC %md
# MAGIC Verify the returned request ID in the restricted audit table. Do not grant
# MAGIC consumers access to restored outputs until the SUCCEEDED receipt is present.
# MAGIC To test a real session restart, restart Python, rerun only imports/config/key
# MAGIC loading, and restore MASKED to a new approved destination. Retain vault and keys.