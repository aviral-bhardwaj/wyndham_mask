# Databricks notebook source
# MAGIC %md
# MAGIC # End-to-end masking and unmasking on synthetic Faker data
# MAGIC
# MAGIC Generates **12,000 customers and 30,000 bookings** with the `faker` library (no real
# MAGIC data), then exercises every public function of `wd_datamask`:
# MAGIC
# MAGIC | Area | Functions |
# MAGIC | --- | --- |
# MAGIC | Setup | `Config`/`load_config`, `KeyRing`, `LookupManager`, `DeltaRepository.bootstrap`, `DeltaAuditManager`, `PermissionManager`, `DatabricksIdentity` |
# MAGIC | Masking | `mask_value`, `mask_first_name`, `mask_last_name`, `dry_run`, `mask_dataframe`, `mask_table`, `rotate_encryption`, `release` |
# MAGIC | Unmasking | `unmask_value`, `unmask_dataframe` (all columns, selected columns, `on_missing="keep_masked"`), `unmask_table` |
# MAGIC | Integrity | referential integrity across customer/booking, repeat-mask consistency, `DeltaRepository.validate`, `subset_related`, permission denial, audit trail |
# MAGIC
# MAGIC Run on trusted compute only. The vault, audit and output tables are created in the
# MAGIC catalog/schema below; change the constants first. The notebook also runs locally
# MAGIC (`python notebooks/02_faker_end_to_end.py`) using a Parquet vault for development.

# COMMAND ----------

# MAGIC %pip install faker /Workspace/Users/ardb40@gmail.com/wyndham_mask/ --force-reinstall --no-deps

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import base64
import os
import random
import tempfile
import time
from pathlib import Path

from faker import Faker
from pyspark.sql import functions as F
from wd_datamask import (Config, KeyRing, LookupManager, DeltaRepository, ParquetRepository,
                         DeltaAuditManager, AuditManager, DatabricksIdentity, LocalIdentity,
                         PermissionManager, MaskingEngine, UnmaskingEngine)
from wd_datamask.exceptions import MissingMappingError, ConfigurationError
from wd_datamask.spark.subsetting import subset_related

# ---- environment: Databricks (Delta vault) or local development (Parquet vault) ----
ON_DATABRICKS = "DATABRICKS_RUNTIME_VERSION" in os.environ
if not ON_DATABRICKS:
    from pyspark.sql import SparkSession
    WORKDIR = Path(tempfile.mkdtemp(prefix="wd_faker_"))
    spark = (SparkSession.builder.master("local[*]").appName("wd-datamask-faker")
             .config("spark.sql.warehouse.dir", str(WORKDIR / "warehouse"))
             .config("spark.sql.shuffle.partitions", "8").config("spark.ui.enabled", "false").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")

CATALOG_SCHEMA = "dont_touch_this_catalog.faker" if ON_DATABRICKS else "faker_demo"   # <catalog>.<schema> on Databricks
VAULT, LOCK, AUDIT = (f"{CATALOG_SCHEMA}.{t}" for t in ("value_mappings", "allocation_lock", "audit_events"))
SRC_CUSTOMER, SRC_BOOKING = f"{CATALOG_SCHEMA}.source_customer", f"{CATALOG_SCHEMA}.source_booking"
MASKED_CUSTOMER, MASKED_BOOKING = f"{CATALOG_SCHEMA}.masked_customer", f"{CATALOG_SCHEMA}.masked_booking"
RESTORED_CUSTOMER = f"{CATALOG_SCHEMA}.restored_customer"
N_CUSTOMERS, N_BOOKINGS = 12_000, 30_000

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_SCHEMA}")
print("Databricks" if ON_DATABRICKS else "local", "->", CATALOG_SCHEMA)

# COMMAND ----------

# MAGIC %md ## 1. Generate synthetic source data with Faker

# COMMAND ----------

fake = Faker("en_US")
Faker.seed(42)
random.seed(42)

PHONE_FORMATS = ["(###) ###-####", "###-###-####", "###.###.####", "+1 ###-###-####", "##########"]


def phone():
    digits = f"{random.randint(2, 9)}{random.randint(0, 9)}{random.randint(0, 9)}" \
             f"{random.randint(2, 9)}{random.randint(0, 9)}{random.randint(0, 9)}{random.randint(0, 9999):04d}"
    layout = random.choice(PHONE_FORMATS)
    it = iter(digits)
    return "".join(next(it) if c == "#" else c for c in layout)


def membership():
    return f"{random.randint(0, 999_999_999):09d}{random.choice('ABCDEFGHJKLMNPQRSTUVWXYZ')}"


def customer(i):
    first, last = fake.first_name(), fake.last_name()
    r = random.random()
    if r < 0.01:      # ~1 % rows with nulls in every sensitive column
        return (i, None, None, None, None, None, None, None, None, fake.city(), fake.state_abbr(), fake.postcode())
    if r < 0.02:      # ~1 % rows with invalid formats that must still round-trip exactly
        return (i, first, last, f"{first} {last}", "not-an-email", "N/A", fake.street_address(), None, "BAD-ID",
                fake.city(), fake.state_abbr(), fake.postcode())
    email = f"{first}.{last}{random.randint(1, 999)}@{random.choice(['gmail.com', 'yahoo.com', 'outlook.com', 'example.org'])}".lower()
    line2 = fake.secondary_address() if random.random() < 0.3 else None
    return (i, first, last, f"{first} {last}", email, phone(), fake.street_address(), line2, membership(),
            fake.city(), fake.state_abbr(), fake.postcode())


t0 = time.time()
customers = [customer(i) for i in range(1, N_CUSTOMERS + 1)]
CUSTOMER_SCHEMA = ("customer_id INT, first_name STRING, last_name STRING, full_name STRING, email STRING, "
                   "phone STRING, address_line_1 STRING, address_line_2 STRING, membership_no STRING, "
                   "city STRING, state STRING, zip STRING")
customer_df = spark.createDataFrame(customers, CUSTOMER_SCHEMA)

# Bookings reference customers by id AND repeat the customer's email/membership (denormalised),
# which is exactly where referential integrity of the masking matters.
by_id = {c[0]: c for c in customers}
bookings = []
for b in range(1, N_BOOKINGS + 1):
    c = by_id[random.randint(1, N_CUSTOMERS)]
    bookings.append((b, c[0], c[4], c[8], fake.date_between("-2y", "today").isoformat(),
                     round(random.uniform(80, 900), 2), random.choice(["WEB", "APP", "CALL", "OTA"])))
booking_df = spark.createDataFrame(bookings, "booking_id INT, customer_id INT, guest_email STRING, "
                                             "membership_no STRING, checkin_date STRING, amount DOUBLE, channel STRING")
fmt = "delta" if ON_DATABRICKS else "parquet"
if not spark.catalog.tableExists(SRC_CUSTOMER):
    customer_df.write.format(fmt).saveAsTable(SRC_CUSTOMER)
if not spark.catalog.tableExists(SRC_BOOKING):
    booking_df.write.format(fmt).saveAsTable(SRC_BOOKING)
print(f"generated {customer_df.count():,} customers and {booking_df.count():,} bookings in {time.time() - t0:.1f}s")
display(customer_df.limit(5)) if ON_DATABRICKS else customer_df.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md ## 2. Configuration, keys, vault, permissions, audit

# COMMAND ----------

config = Config({
    "namespace": "wyndham_faker_demo",
    "mapping_version": "v1",
    "defaults": {"invalid_values": "replace", "email_domain": "testmail.com", "phone_pool": "synthetic"},
    "tables": {
        "customer": {"columns": {
            "first_name":     {"mask_type": "first_name", "domain": "customer_first_name"},
            "last_name":      {"mask_type": "last_name",  "domain": "customer_last_name"},
            "full_name":      {"mask_type": "full_name",  "domain": "customer_full_name"},
            "email":          {"mask_type": "email",      "domain": "customer_email"},
            "phone":          {"mask_type": "phone",      "domain": "customer_phone"},
            "address_line_1": {"mask_type": "address",    "domain": "customer_address"},
            "address_line_2": {"mask_type": "address",    "domain": "customer_address"},
            "membership_no":  {"mask_type": "membership", "domain": "membership_number"},
        }},
        # Same domains => the same original gets the same substitute in both tables.
        "booking": {"columns": {
            "guest_email":   {"mask_type": "email",      "domain": "customer_email"},
            "membership_no": {"mask_type": "membership", "domain": "membership_number"},
        }},
    },
})
# load_config also accepts a YAML path: config = load_config("/Workspace/.../masking.yaml")

import hashlib
if ON_DATABRICKS:
    keys = KeyRing({"enc-v1": hashlib.sha256(b"demo-encryption-key-v1").digest()},
                   {"fp-v1": hashlib.sha256(b"demo-fingerprint-key-v1").digest()},
                   "enc-v1", "fp-v1")
    identity = DatabricksIdentity(spark)
    store = DeltaRepository(spark, VAULT, LOCK)
    store.bootstrap()                                   # operator-only, idempotent
    spark.sql(f"CREATE TABLE IF NOT EXISTS {AUDIT} (event STRING) USING DELTA")
    audit = DeltaAuditManager(spark, AUDIT)
else:
    keys = KeyRing({"enc-v1": os.urandom(32)}, {"fp-v1": os.urandom(32)}, "enc-v1", "fp-v1")  # demo-only keys
    identity = LocalIdentity()
    store = ParquetRepository(spark, VAULT, WORKDIR / "locks")
    store.bootstrap()
    audit = AuditManager(WORKDIR / "audit.sqlite")

principal = identity.current().requester
print("execution principal:", principal)

# Operator-installed policy. Replace `principal` with the approved job/service principal in production.
grants = [{
    "principals": [principal], "actions": ["MASK", "UNMASK", "ROTATE"],
    "namespace": config.namespace, "domains": list(config.domains),
    "tables": ["customer", "booking"], "columns": ["*"], "allow_values": True,
    "destinations": [MASKED_CUSTOMER, MASKED_BOOKING, RESTORED_CUSTOMER],
}]
permissions = PermissionManager(identity, grants)
deps = dict(spark=spark, config=config, keys=keys, store=store, permissions=permissions, audit=audit,
            lookups=LookupManager())          # LookupManager(path) to supply your own approved pools
masker, unmasker = MaskingEngine(**deps), UnmaskingEngine(**deps)
print("pool sizes:", {k: len(masker.lookups[k]) for k in ("first_names", "last_names", "streets")})

# COMMAND ----------

# MAGIC %md ## 3. Scalar API: `mask_value`, `mask_first_name`, `mask_last_name`, `unmask_value`

# COMMAND ----------

samples = {
    "customer_first_name": "John", "customer_last_name": "Smith", "customer_full_name": "John Smith",
    "customer_email": "john.smith@gmail.com", "customer_phone": "(555) 123-4567",
    "customer_address": "123 Main Street", "membership_number": "123456789A",
}
for domain, original in samples.items():
    masked = masker.mask_value(original, domain=domain)
    again = masker.mask_value(original, domain=domain)                      # deterministic
    restored = unmasker.unmask_value(masked, domain=domain, reason="Notebook scalar validation")
    assert again == masked and restored == original and masked != original
    print(f"{domain:22s} {original!r:28s} -> {masked!r:34s} -> {restored!r}")

assert masker.mask_first_name("Adam", domain="customer_first_name") != "Adam"
assert masker.mask_last_name("Brown", domain="customer_last_name") != "Brown"
assert masker.mask_value(None, domain="customer_email") is None               # nulls stay null
try:
    masker.mask_first_name("Adam", domain="customer_email")                   # wrong helper for the domain
except ConfigurationError as e:
    print("expected:", e)

# COMMAND ----------

# MAGIC %md ## 4. `dry_run`: what would be masked, nothing allocated

# COMMAND ----------

source_customer = spark.table(SRC_CUSTOMER)
report = masker.dry_run(source_customer, table="customer")
print({k: report[k] for k in ("records_processed", "names_masked", "emails_masked", "phones_masked",
                              "addresses_masked", "memberships_masked", "errors")})
for column, stats in report["columns"].items():
    print(f"  {column:15s} non_null={stats['non_null_rows']:6d} distinct={stats['distinct_values']:6d} "
          f"missing_mappings={stats['missing_mappings']:6d} capacity={stats['candidate_pool_capacity']:,}")

# COMMAND ----------

# MAGIC %md ## 5. `mask_dataframe` on customers and bookings (shared domains keep joins intact)

# COMMAND ----------

t0 = time.time()
masked_customer = masker.mask_dataframe(source_customer, table="customer")
if not spark.catalog.tableExists(MASKED_CUSTOMER):
    masked_customer.write.format(fmt).saveAsTable(MASKED_CUSTOMER)
masked_booking = masker.mask_dataframe(spark.table(SRC_BOOKING), table="booking")
if not spark.catalog.tableExists(MASKED_BOOKING):
    masked_booking.write.format(fmt).saveAsTable(MASKED_BOOKING)
masker.release()                                    # drop cached lookups once written
masked_customer, masked_booking = spark.table(MASKED_CUSTOMER), spark.table(MASKED_BOOKING)
print(f"masked both tables in {time.time() - t0:.1f}s")
display(masked_customer.limit(5)) if ON_DATABRICKS else masked_customer.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md ## 6. Checks: row counts, nulls, readability, format preservation, referential integrity

# COMMAND ----------

MASKED_COLS = list(config.columns("customer"))
src, msk = source_customer.alias("s"), masked_customer.alias("m")
paired = src.join(msk, "customer_id")
checks = {}
checks["row_count"] = masked_customer.count() == source_customer.count()
checks["unmasked_columns_untouched"] = paired.where(
    (F.col("s.city") != F.col("m.city")) | (F.col("s.state") != F.col("m.state")) | (F.col("s.zip") != F.col("m.zip"))).count() == 0
checks["null_positions_preserved"] = all(
    paired.where(F.col(f"s.{c}").isNull() != F.col(f"m.{c}").isNull()).count() == 0 for c in MASKED_COLS)
checks["every_value_changed"] = all(
    paired.where(F.col(f"s.{c}") == F.col(f"m.{c}")).count() == 0 for c in MASKED_COLS)
checks["distinct_counts_preserved"] = all(
    masked_customer.select(c).distinct().count() == source_customer.select(c).distinct().count() for c in MASKED_COLS)
checks["email_domain"] = masked_customer.where(F.col("email").isNotNull() & ~F.col("email").endswith("@testmail.com")).count() == 0
checks["membership_format"] = masked_customer.where(F.col("membership_no").isNotNull()
                                                    & ~F.col("membership_no").rlike(r"^[0-9]{9}[A-Z]$")).count() == 0
checks["phone_layout_preserved"] = paired.where(
    F.col("s.phone").isNotNull() & (F.col("s.phone") != "N/A")
    & (F.regexp_replace("s.phone", "[0-9]", "D") != F.regexp_replace("m.phone", "[0-9]", "D"))).count() == 0
checks["names_readable"] = masked_customer.where(F.col("first_name").isNotNull()
                                                 & ~F.col("first_name").rlike(r"^[A-Z][a-z]+(-[A-Z][a-z]+)*$")).count() == 0
# Referential integrity: booking.guest_email joins masked_customer.email exactly as the sources join.
source_joins = spark.table(SRC_BOOKING).join(source_customer, F.col("guest_email") == F.col("email")).count()
masked_joins = masked_booking.join(masked_customer, F.col("guest_email") == F.col("email")).count()
checks["referential_integrity_email"] = source_joins == masked_joins
checks["referential_integrity_membership"] = (
    masked_booking.alias("b").join(masked_customer.alias("c"), "customer_id")
    .where(F.col("b.membership_no").isNotNull() & (F.col("b.membership_no") != F.col("c.membership_no"))).count() == 0)
print(checks)
assert all(checks.values()), "a masking check failed"

# COMMAND ----------

# MAGIC %md ## 7. `unmask_dataframe`: all columns, selected columns, unknown values

# COMMAND ----------

restored = unmasker.unmask_dataframe(masked_customer, table="customer", reason="Notebook full restoration")
assert restored.exceptAll(source_customer).count() == 0 and source_customer.exceptAll(restored).count() == 0
print("full restoration: exact match on all", source_customer.count(), "rows")

partial = unmasker.unmask_dataframe(masked_customer, table="customer", columns=["email", "phone"],
                                    reason="Notebook selected-column restoration")
p = partial.alias("p").join(source_customer.alias("s"), "customer_id").join(masked_customer.alias("m"), "customer_id")
assert p.where(~F.col("p.email").eqNullSafe(F.col("s.email")) | ~F.col("p.phone").eqNullSafe(F.col("s.phone"))).count() == 0
assert p.where(~F.col("p.first_name").eqNullSafe(F.col("m.first_name"))).count() == 0     # other columns stay masked
print("selected-column restoration: email/phone restored, first_name still masked")

unknown = masked_customer.limit(3).withColumn("email", F.lit("nobody@testmail.com"))
try:
    unmasker.unmask_dataframe(unknown, table="customer", columns=["email"], reason="Strict").collect()
except MissingMappingError as e:
    print("strict policy raised as expected:", e)
kept = unmasker.unmask_dataframe(unknown, table="customer", columns=["email"], reason="Lenient", on_missing="keep_masked")
assert kept.where(F.col("email") == "nobody@testmail.com").count() == 3
unmasker.release()

# COMMAND ----------

# MAGIC %md ## 8. Repeat masking is idempotent and the vault is consistent

# COMMAND ----------

vault_rows = spark.table(VAULT).count()
again = masker.mask_dataframe(source_customer, table="customer")
assert again.exceptAll(masked_customer).count() == 0
masker.release()
assert spark.table(VAULT).count() == vault_rows, "repeat masking must not allocate new mappings"
for domain in config.domains:
    store.validate(config.scope(domain))               # raises AmbiguousMappingError on duplicate keys
print(f"vault rows: {vault_rows:,}; no duplicate fingerprints or substitutes in any scope")
vault_scopes = spark.table(VAULT).groupBy("scope").count().collect()
for r in vault_scopes:
    print(f"  {r['scope']:60s} {r['count']:7,d}")

# COMMAND ----------

# MAGIC %md ## 9. Table API (Delta only): `mask_table`, `unmask_table` with manifests

# COMMAND ----------

if ON_DATABRICKS:
    # Use separate targets for the table API test (mask_dataframe already wrote MASKED_CUSTOMER without a manifest).
    TABLE_API_MASKED = f"{CATALOG_SCHEMA}.table_api_masked_customer"
    TABLE_API_RESTORED = f"{CATALOG_SCHEMA}.table_api_restored_customer"
    table_api_grants = [{**grants[0], "destinations": [*grants[0]['destinations'], TABLE_API_MASKED, TABLE_API_RESTORED]}]
    table_api_perms = PermissionManager(identity, table_api_grants)
    if not spark.catalog.tableExists(TABLE_API_MASKED):
        table_api_masker = MaskingEngine(**{**deps, "permissions": table_api_perms})
        result = table_api_masker.mask_table(source_table=SRC_CUSTOMER, target_table=TABLE_API_MASKED, table="customer")
        print("mask_table:", result)
    else:
        print(f"{TABLE_API_MASKED} already exists; skipping mask_table.")
    if not spark.catalog.tableExists(TABLE_API_RESTORED):
        fresh = UnmaskingEngine(**{**deps, "permissions": table_api_perms})
        result = fresh.unmask_table(source_table=TABLE_API_MASKED, target_table=TABLE_API_RESTORED,
                                    table="customer", columns=None, reason="Notebook table restoration")
        print("unmask_table:", result)
    else:
        print(f"{TABLE_API_RESTORED} already exists; skipping unmask_table.")
    r = spark.table(TABLE_API_RESTORED)
    assert r.exceptAll(source_customer).count() == 0 and source_customer.exceptAll(r).count() == 0
    print(spark.sql(f"SHOW TBLPROPERTIES {TABLE_API_MASKED} ('wd_datamask.manifest')").first().value[:200], "...")
else:
    print("mask_table / unmask_table need Delta tables; run this cell on Databricks")

# COMMAND ----------

# MAGIC %md ## 10. Authorization: an identity without grants cannot unmask

# COMMAND ----------

denied = UnmaskingEngine(**{**deps, "permissions": PermissionManager(identity, [])})
try:
    denied.unmask_value(masker.mask_value("secret@gmail.com", domain="customer_email"),
                        domain="customer_email", reason="Should be denied")
    raise AssertionError("denied engine unexpectedly succeeded")
except PermissionError as e:
    print("PermissionError:", e)
column_locked = PermissionManager(identity, [{**grants[0], "columns": ["phone"]}])
try:
    UnmaskingEngine(**{**deps, "permissions": column_locked}).unmask_dataframe(
        masked_customer, table="customer", columns=["email"], reason="Column not granted")
    raise AssertionError("column restriction not enforced")
except PermissionError as e:
    print("PermissionError (column):", e)

# COMMAND ----------

# MAGIC %md ## 11. Key rotation and data subsetting

# COMMAND ----------

rotated_keys = KeyRing({**keys.encryption_keys, "enc-v2": os.urandom(32)}, keys.fingerprint_keys, "enc-v2", "fp-v1")
count = MaskingEngine(**{**deps, "keys": rotated_keys}).rotate_encryption("customer_email", reason="Notebook rotation")
print(f"re-encrypted {count:,} email mappings under enc-v2")
check = UnmaskingEngine(**{**deps, "keys": rotated_keys}).unmask_dataframe(
    masked_customer, table="customer", columns=["email"], reason="Verify after rotation")
assert check.join(source_customer.alias("s"), "customer_id").where(~check.email.eqNullSafe(F.col("s.email"))).count() == 0
keys = rotated_keys      # keep using the rotated ring from here on

parents, children = subset_related(masked_customer, masked_booking, parent_key="customer_id", child_key="customer_id",
                                   predicate=F.col("state").isin("CA", "NY", "TX"))
print(f"subset: {parents.count():,} customers in CA/NY/TX with {children.count():,} bookings")

# COMMAND ----------

# MAGIC %md ## 12. Audit trail (no original or restored values are ever logged)

# COMMAND ----------

if ON_DATABRICKS:
    events = spark.table(AUDIT)
    print("audit events:", events.count())
    display(spark.sql(f"""SELECT get_json_object(event,'$.timestamp') ts, get_json_object(event,'$.action') action,
                                 get_json_object(event,'$.status') status, get_json_object(event,'$.requester') requester,
                                 get_json_object(event,'$.table') tbl, get_json_object(event,'$.reason') reason
                          FROM {AUDIT} ORDER BY ts DESC LIMIT 20"""))
else:
    events = audit.events()
    print("audit events:", len(events))
    for e in events[-8:]:
        print(f"  {e['timestamp'][:19]} {e['action']:6s} {e['status']:11s} table={e.get('table')} reason={e.get('reason')!r}")
    import json
    assert "john.smith@gmail.com" not in json.dumps(events)

print("\nALL CHECKS PASSED")