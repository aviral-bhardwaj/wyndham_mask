# Validation and operational limits

## Verified locally

The complete suite passed: **70 tests**, including the actual local Spark and Delta
integration tests, with **96% statement coverage** (898 of 932 statements). The run
completed in approximately 75 seconds after the package rename on macOS with Python 3.11.16, Java 17,
PySpark 3.5.3, and Delta Lake 3.2.1. Twelve warnings were upstream PySpark deprecations
for `distutils` version checks in its pandas/Arrow integration.

Both wheel and source distribution built successfully. The wheel was installed into
a separate directory and passed the synthetic mask/unmask demo without importing
the repository source. All 38 Python files parsed successfully. The Delta suite
verified simultaneous mutex contention, persisted manifest recovery with newer
current YAML, encryption rotation, corruption detection, and both pre-publication
and post-commit audit failures. No live Databricks workspace was used.
The rebuilt distribution and its import namespace were verified as `wd_datamask`;
the wheel contains no superseded package modules.

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

The supplied pools are demonstration data, not approved company datasets:

| Type | Default candidate space |
| --- | --- |
| First name | 32 |
| Last name | 32 |
| Independent full name | 1,024 combinations |
| Email | 1 billion numbered synthetic usernames |
| Phone | 100 reserved NANP substitutes per supported separator pattern |
| Address | 159,984 number/street combinations |
| Membership | 1 billion numbers per preserved final letter |

The allocator skips an unchanged original, checks persisted and in-batch collisions,
and stops after the smaller of pool capacity and 10,000 probes. A probe-limit error
does not prove the entire pool is full. Imported lookup names must be unique strings;
the generated full-name domain still undergoes collision detection. Different phone
format strings retain their own exact representations and can share the same digits
with different separators; normalized phone uniqueness is not promised.

Expand pools using a JSON file passed to `LookupManager(path)` with `first_names`,
`last_names`, and `streets`. Existing mappings stay authoritative after expansion.
The built-in phone masker supports NANP-style 10-digit or leading-1 11-digit layouts;
other locales need a reviewed custom masker/pool. Invalid-value replacement must be
enabled explicitly when such source strings are present.

Use `scripts/benchmark.py` on trusted compute to measure row count, distinct value
count, initial allocation, repeated masking, and unmasking. There is no verified
10M-row/100-table SLA. Driver batch allocation, global mutex contention, repeated
uniqueness scans, and mapping cardinality are known scaling constraints. The local
SQLite backend materializes scope mappings and is intended only for bounded demos.

No forced broadcast joins, Delta caching, or partition tuning is enabled universally.
Let Spark choose join plans; tune after examining representative execution plans.
Protect any caching/spill of original or decrypted data using platform controls.

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
