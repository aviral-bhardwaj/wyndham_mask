# Validation and operational limits

## Verified locally

Latest run (cloud Linux sandbox, 2 vCPU, 8 GB RAM, Python 3.11.15, Java 17, PySpark
3.5.9, pandas 2.3, PyArrow 19): the full suite passed, **72 tests, 7
skipped (Delta jars unavailable offline), 86% statement coverage** in
241 s. The uncovered statements are the Delta-only paths (`DeltaRepository`
mutex/MERGE, `mask_table`/`unmask_table` publication, `DeltaAuditManager`), which the
skipped Delta suite covers; the remaining modules are at 93-100%. The Spark suite now includes the distributed volume path on a
Parquet-backed vault (`tests/test_spark_volume_path.py`): all seven mask types, forced
multi-round allocation with compound name tiers, broadcast and shuffled lookups,
atomic capacity exhaustion, invalid-value rejection, dry-run summaries, missing-
mapping policies, file-lock contention, encryption rotation and reliable checkpoints.
The Delta tests (`tests/test_delta.py`) were last executed with Delta 3.2.1 jars on
macOS as recorded in the previous revision; rerun them with `WD_DELTA_JARS` before
deploying. The wheel built with `python -m build` and was installed into a clean
virtual environment and smoke-tested without the repository sources.

## 60 million row qualification

`scripts/scale_test.py` generated 60,000,000 synthetic activity rows for 5,000,000
customers (each customer's attributes repeat across rows; ~1% nulls and ~0.1%
invalid placeholders in every masked column) and ran **one** `mask_dataframe` call over
all seven configured columns, wrote the result, verified it, restored it with one
`unmask_dataframe` call and re-masked a sample. Backend: `ParquetRepository`
(the same distributed code path as `DeltaRepository`, minus Delta's mutex/MERGE), local
Spark `local[2]`, 4 GB driver, zstd shuffle compression.

| Step | Result |
| --- | --- |
| Distinct values per column | first 25,000; last 120,000; full name 600,000; email 5,000,001; phone 20,001; address 3,999,996; membership 5,000,001 (plus 594,060 nulls per column) |
| `mask_dataframe` + parquet write (7 columns, one call) | 1,779 s |
| New vault mappings written | 14,764,999 (one append per column; 0 duplicate keys) |
| Row count, null positions, distinct cardinalities preserved | all true (60,000,000 rows; null and distinct counts identical per column) |
| One substitute per customer per column (referential integrity) | true (per-customer distinct-value profile identical to the source) |
| `unmask_dataframe` + full-content hash equals source | true (638 s) |
| Re-masking a 2M-row sample reproduces the same substitutes | true (332 s for the sample, no new vault rows) |

Sample substitutes from the run: `Tia-Nelda Siggers-Ebbert`, `jeri.preciado118@example.com`, `(835) 883-1376`, `4495 Chambers Street`, `355998568E`; full report in `docs/scale_test_report_60m.json`.

The host's 30 GB scratch disk, not the package, bounded this run: Spark shuffle files
for the three shuffled lookup joins plus the 5 GB source, the masked output and
2 GB vault fill it, so the source parquet was deleted after masking (its content
hash and a 1-in-30 sample had been retained) and the restored rows were hashed
rather than written. On a Databricks cluster none of these accommodations apply.
Throughput scales with cluster cores: every step is a Spark job over distinct values
or a join over rows, with no driver-side loop over data.

## Test commands

```bash
python -m pip install -e '.[dev,spark,delta]'
python -m pytest -m 'not spark and not delta'
# Requires Java 17, matching Python on Spark workers, and local loopback networking:
python -m pytest -m spark
```

For actual local Delta integration, set `WD_DELTA_JARS` to a comma-separated list of
Delta Spark 3.2.1, Delta Storage 3.2.1, and ANTLR 4.9.3 jar paths. Then run:

```bash
python -m pytest --cov=wd_datamask --cov-report=term-missing
```

The Spark fixture enables Delta extensions only when those jars are supplied.
Without them, Delta tests are explicitly skipped; they do not count as passed.
Run the Databricks notebook separately for Unity Catalog permissions, secrets,
session identity, and the workspace's actual runtime. Local tests cannot certify
those integrations.

Tests cover exact restoration with duplicates, nulls, invalid strings, Unicode and
formatting; cross-table joins; selected-column restoration; concurrent SQLite
allocation; encryption rotation and tampering; denied/spoofed identity requests;
missing mappings; dry runs; pure UDFs; subsetting; and actual Delta persistence,
manifest version recovery, mutex contention, and audit-failure staging cleanup.

## Compatibility

The local validation target is Python 3.11, Java 17, PySpark 3.5.3, Delta Lake 3.2.1,
pandas 2.3, and PyArrow 19. The package requires Python 3.11+ and uses optional Spark
3.5 extras; other combinations require qualification. It does not bundle a JVM,
Spark, Delta jars, credentials, or runtime-specific dependencies into the wheel.
Databricks execution uses its bundled Spark/Delta libraries and `session_user()`.
No Databricks Runtime or Unity Catalog environment is claimed as tested locally.

## Capacity and scale

Built-in pools (public US Census name frequency files via `scripts/build_lookups.py`,
plus generated street names) and their candidate spaces:

| Type | Default candidate space |
| --- | --- |
| First name | 5,130 plain, then 26.3 M hyphenated pairs, then 135 G triples |
| Last name | 20,000 plain, then 400 M pairs, then 8 T triples |
| Independent full name | first-name space x last-name space per tier |
| Email | 102.6 M `first.last@domain`, then 102.6 G numbered |
| Phone (`synthetic`) | 6.26 G well-formed NANP numbers per separator layout |
| Phone (`fictitious`) | 100 reserved 555-01XX numbers per separator layout |
| Address | 127.6 M number/street combinations |
| Membership | 1 G numbers per preserved final letter |

The allocator skips an unchanged original, rejects persisted and in-round collisions,
and gives up after 64 probes per value (`MappingCapacityError`; nothing is written for
that column). Plain-tier probes come first (2), then pairs (22), then triples, so
compound names appear only once the plain pool is crowded. Different phone format
strings retain their own exact representations and can share the same digits with
different separators; normalized phone uniqueness is not promised. Synthetic phone
numbers are well-formed but not guaranteed unassigned: never dial masked numbers.

Expand or replace pools using a JSON file passed to `LookupManager(path)` with
`first_names`, `last_names`, and `streets`. Existing mappings stay authoritative after
expansion. The built-in phone masker supports NANP-style 10-digit or leading-1
11-digit layouts; other locales need a reviewed custom masker/pool. Invalid-value
replacement must be enabled explicitly when such source strings are present.

Use `scripts/benchmark.py` for quick single-column timings and `scripts/scale_test.py`
for the full round trip on trusted compute; pass `--backend delta` with
`WD_DELTA_JARS` (or run on Databricks) to qualify the Delta vault. Allocation is
serialized per vault by the mutex; concurrent table jobs on the same vault wait for
each other, while jobs on different vaults run in parallel. Lookups with at most
`broadcast_threshold` distinct values (default 500,000) are broadcast; larger ones
use shuffled joins. Cached lookups are released by `engine.release()` or the table
APIs. Protect caching/spill of original or decrypted data using platform controls.

## Deliberate limits

- Sensitive columns must be strings. The library never silently converts numeric
  identifiers and loses leading zeros.
- Derived full-name mode, coherent multi-line address tuples, general locale phone
  pools, and automatic relationship-graph subsetting are not implemented; no silent
  promise of those behaviors is made.
- Caller identity is the OS account locally or Databricks execution principal.
  Human delegation/SSO gateway and dynamic enterprise-group resolution are deployment
  integrations, not included services.
- Encryption keys are available to trusted Spark executors; non-exportable KMS/HSM
  cryptographic operations are not implemented.
- Audit and output publication are not one atomic transaction. Keep restored outputs
  restricted until the completed audit receipt is verified.
- General DataFrame actions are lazy and not intercepted. Authorization occurs when
  constructing the plan; `unmask_table` manages execution and output receipts.
