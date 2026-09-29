# How to Apply Unmasking

## Prerequisites

Keep the mapping Delta table, encryption keys, fingerprint keys, and dataset manifest
from masking. Use an approved execution identity on trusted compute. Grant that
identity only the required domains, tables, columns, and destination names through
an operator-controlled `PermissionManager` policy. Configure a durable audit sink.

For a complete dependency setup, import the Databricks source notebook
`notebooks/01_databricks_workflow.py`. Source notebooks can be imported directly into
a Databricks workspace. The local example is executable without Spark.

## One value

```python
restored = unmasker.unmask_value(
    masked_email,
    domain="customer_email",
    mapping_version="v1",
    reason="Approved investigation INC-12345",
)
```

The namespace comes from the engine's configuration. Always specify the historical
version if it differs from the configured default. No caller-supplied `user` or role
argument exists. A null returns null after authorization and auditing. Unknown
values or wrong scopes raise `MissingMappingError`; the package never guesses an
original. Do not print `restored` in notebook outputs or logs.

## Selected DataFrame columns

```python
restored_df = unmasker.unmask_dataframe(
    masked_df, table="customer", columns=["email", "phone"],
    reason="Approved investigation INC-12345",
)
```

Names, membership numbers, and other columns remain exactly as supplied. Use
`columns=None` to restore every configured column. Non-configured columns are
preserved. Configured columns must be strings; cast deliberately upstream when
necessary, preserving leading zeros and the original representation.

The default `on_missing="error"` refuses unresolved values. An explicit
`on_missing="keep_masked"` keeps unknown values, emits per-column unresolved counts,
and marks the resulting column as partially restored. This mode can produce a
mixture of masked and original strings and must not be treated as a complete result.
Persisted-table restoration always uses strict mode.

Spark is lazy. This method records REQUESTED, AUTHORIZED, and PLAN_READY, not a
completed restoration. A later `.collect()` or `.write()` runs under the same trusted
compute boundary, outside this API's execution audit. Authorization is not rechecked
for every downstream action. Use `unmask_table` for a managed write and completion
receipt. Do not share a restored DataFrame or its cached contents with other users.

## Restore a saved table in a new session

Use `masker.mask_table` when persisting masked data so the table includes the dataset
manifest. After restarting Python or moving to a new authorized cluster, reload the
same key ring and vault connection; no in-memory cache is needed.

```python
receipt = unmasker.unmask_table(
    source_table="restricted.qa.customer",
    target_table="restricted.restore.customer_inc12345",
    table="customer",
    columns=["email", "phone"],
    reason="Approved investigation INC-12345",
)
```

The engine uses the saved manifest's configuration, verifies its digest and column
provenance, reads a pinned Delta source version, authorizes each requested column and
the exact destination, writes a staging table, records PREPARED, and publishes a new
destination. It returns a request ID and row count only after recording SUCCEEDED.
Existing destinations cause an error. It never overwrites production data.

If you persist a DataFrame yourself, column metadata alone is insufficient for this
table API. Do not manually fabricate a manifest; use `mask_table`, or explicitly
configure historical scopes and use the DataFrame API inside an approved workflow.

Keep staging and restored destinations in a schema without inherited consumer read
access. Do not grant access until the success receipt and audit record are verified.
An audit failure after the destination commit raises `RecoveryRequiredError`; the
output remains restricted until an operator reconciles the request. Delta cannot
atomically commit the destination and a separate audit table.

## Validate without exposing values

For synthetic acceptance data only:

```python
assert restored_df.exceptAll(original_df).count() == 0
assert original_df.exceptAll(restored_df).count() == 0
```

Comparing both row multisets verifies duplicates as well as values and counts.
Spark does not guarantee row ordering. Do not use `show()`, `display()`, or debugging
logs on real original/restored data during validation.

## Errors and recovery

| Error | Action |
| --- | --- |
| `PermissionError` | Have an operator verify authenticated identity, required reason, and policy scope. |
| `MissingMappingError` | Check namespace/version and restore the correct vault backup. |
| `AmbiguousMappingError` | Stop use and repair duplicate vault keys under an operator-controlled maintenance procedure. |
| `IntegrityError` | Verify the retained keys and mapping/manifest integrity; never bypass authentication checks. |
| `AllocationBusyError` | Retry later; follow lock recovery only if the owning job has stopped. |
| `MappingCapacityError` | Supply a larger approved lookup pool or a different explicit format/domain. |
| `AuditError` | Restore the audit sink before retrying. |
| `RecoveryRequiredError` | Keep destination restricted and reconcile staging/output and audit state using the request ID. |

Losing the mapping or required keys makes restoration impossible. Deleting mappings
is irreversible from the masked output alone.
