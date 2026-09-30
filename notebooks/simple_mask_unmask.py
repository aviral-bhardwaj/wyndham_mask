# Databricks notebook source
# DBTITLE 1,Title
# MAGIC %md
# MAGIC # Simple Mask / Unmask
# MAGIC
# MAGIC Minimal two-step workflow: **mask a DataFrame → unmask it back**. All setup lives in one cell.

# COMMAND ----------

# DBTITLE 1,Install library
# MAGIC %pip install /Workspace/Users/ardb40@gmail.com/wyndham_mask/ --force-reinstall --no-deps -q
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,One-time setup (run once per session)
import hashlib
from wd_datamask import (
    Config, KeyRing, DeltaRepository, DeltaAuditManager,
    DatabricksIdentity, PermissionManager, LookupManager,
    MaskingEngine, UnmaskingEngine,
)

# ── 1. WHERE to store mappings ──────────────────────────────────────────────
CATALOG_SCHEMA = "dont_touch_this_catalog.masking"       # change to your catalog.schema
VAULT = f"{CATALOG_SCHEMA}.value_mappings"
LOCK  = f"{CATALOG_SCHEMA}.allocation_lock"
AUDIT = f"{CATALOG_SCHEMA}.audit_events"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_SCHEMA}")

# ── 2. WHAT to mask (column → mask type + domain) ───────────────────────────
#    Supported mask_types: first_name, last_name, full_name, email, phone, address, membership
#    Columns sharing the same domain get consistent substitutions across tables.
config = Config({
    "namespace": "my_project",
    "mapping_version": "v1",
    "defaults": {"invalid_values": "replace", "email_domain": "masked.example.com"},
    "tables": {
        "my_table": {"columns": {
            # ↓↓↓ EDIT THESE to match YOUR DataFrame columns ↓↓↓
            "first_name":  {"mask_type": "first_name", "domain": "person_first"},
            "last_name":   {"mask_type": "last_name",  "domain": "person_last"},
            "email":       {"mask_type": "email",      "domain": "person_email"},
            "phone":       {"mask_type": "phone",      "domain": "person_phone"},
        }},
    },
})

# ── 3. KEYS (deterministic demo keys — use secrets in production) ───────────
keys = KeyRing(
    {"enc-v1": hashlib.sha256(b"demo-encryption-key-v1").digest()},
    {"fp-v1":  hashlib.sha256(b"demo-fingerprint-key-v1").digest()},
    "enc-v1", "fp-v1",
)

# ── 4. PERMISSIONS (grant current user full mask/unmask) ────────────────────
identity = DatabricksIdentity(spark)
principal = identity.current().requester
permissions = PermissionManager(identity, [{
    "principals": [principal],
    "actions": ["MASK", "UNMASK"],
    "namespace": config.namespace,
    "domains": list(config.domains),
    "tables": ["my_table"], "columns": ["*"], "allow_values": True,
    "destinations": [],   # add target table names here if using mask_table
}])

# ── 5. VAULT + AUDIT (idempotent bootstrap) ─────────────────────────────────
store = DeltaRepository(spark, VAULT, LOCK)
store.bootstrap()
spark.sql(f"CREATE TABLE IF NOT EXISTS {AUDIT} (event STRING) USING DELTA")
audit = DeltaAuditManager(spark, AUDIT)

# ── 6. BUILD ENGINES ────────────────────────────────────────────────────────
masker   = MaskingEngine(spark=spark, config=config, keys=keys, store=store,
                         permissions=permissions, audit=audit, lookups=LookupManager())
unmasker = UnmaskingEngine(spark=spark, config=config, keys=keys, store=store,
                           permissions=permissions, audit=audit)

print("✓ Ready. Use  masker.mask_dataframe(df, table='my_table')  to mask.")

# COMMAND ----------

# DBTITLE 1,Mask a DataFrame
# Load your DataFrame (replace with your actual table/source)
df = spark.createDataFrame([
    (1, "John",  "Smith",  "john.smith@gmail.com",    "(555) 123-4567"),
    (2, "Maria", "Garcia", "maria.garcia@yahoo.com",  "212-555-0199"),
    (3, "Alex",  "Chen",   "alex.chen@outlook.com",   "415.555.0100"),
    (4, None,    None,      None,                      None),
], "id INT, first_name STRING, last_name STRING, email STRING, phone STRING")

# ──── MASK ────
masked_df = masker.mask_dataframe(df, table="my_table")
masker.release()   # free cached lookups after write

display(masked_df)

# COMMAND ----------

# DBTITLE 1,Unmask back to original
# ──── UNMASK (full restoration) ────
restored_df = unmasker.unmask_dataframe(masked_df, table="my_table", reason="Restore original data")
unmasker.release()

display(restored_df)

# COMMAND ----------

# DBTITLE 1,Verify round-trip
# Confirm restored matches original exactly
assert restored_df.exceptAll(df).count() == 0
assert df.exceptAll(restored_df).count() == 0
print("✓ Round-trip verified: restored == original")