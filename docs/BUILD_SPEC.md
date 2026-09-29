# Build an Internal Data Masking and Unmasking Python Package for Databricks

Build an internal Python package named `va_datamask` for Wyndham's data masking initiative. Deliver source code and a private wheel; do not publish it to PyPI.

The package must mask sensitive production data for development, QA, UAT, and testing, using realistic substitute values and consistent mappings. It must also let authorized users apply unmasking to individual values, selected DataFrame columns, and persisted masked Delta datasets, restoring the exact originals.

This is reversible pseudonymization. Its mapping vault and decryption capability are sensitive resources. Clearly distinguish implemented and tested functionality from deployment prerequisites and unverified scale targets.

## 1. Technology and packaging

- Python 3.11+ with a documented, tested Databricks Runtime/Python/Spark compatibility matrix.
- PySpark, Spark SQL, Delta Lake, and Unity Catalog.
- Prefer Spark expressions and distributed joins. Provide Pandas UDFs and Spark UDFs for transformations where appropriate; do not require UDFs where native operations work better.
- Package with `pyproject.toml`, a `src/` layout, and `python -m build`. Include `setup.py` only if required by the chosen build tooling.
- Produce `dist/va_datamask-1.0.0-py3-none-any.whl` if the implementation is pure Python. Document dependencies and do not bundle a conflicting Spark runtime.
- Include private installation instructions using a Unity Catalog volume or workspace files, for example:

```python
%pip install /Volumes/<catalog>/<schema>/<volume>/wheels/va_datamask-1.0.0-py3-none-any.whl
```

## 2. Package structure

Include `__init__.py` files and these modules:

```text
va_datamask/
  pyproject.toml
  README.md
  src/va_datamask/
    __init__.py
    masking_engine.py
    unmasking_engine.py
    exceptions.py
    maskers/
      name_masker.py
      full_name_masker.py
      email_masker.py
      phone_masker.py
      address_masker.py
      membership_masker.py
    spark/
      udf_registry.py
      pandas_udfs.py
      dataframe_masker.py
    storage/
      lookup_manager.py
      mapping_store.py
      delta_repository.py
    security/
      encryption.py
      permissions.py
      auditing.py
    config/
      yaml_loader.py
  tests/
  sample_lookup_files/
  notebooks/
  examples/
```

Implement and expose `MaskingEngine`, `UnmaskingEngine`, `NameMasker`, `FullNameMasker`, `EmailMasker`, `PhoneMasker`, `AddressMasker`, `MembershipMasker`, `LookupManager`, `MappingStore`, `DeltaRepository`, `AuditManager`, and `PermissionManager`.

## 3. Mapping contract: required for reliable unmasking

Use persistent Delta mapping tables shared by all participating databases. Define a mapping scope using `namespace`, `domain`, and `mapping_version`. A domain identifies the semantic field, such as `customer_email` or `customer_first_name`, independently of its table or column name.

Within a scope:

- Every distinct original has one stable masked value.
- Every masked value identifies exactly one original.
- The same original must receive the same substitute across tables, databases, reruns, and Spark repartitioning.
- Mappings become authoritative when committed and must survive cluster restarts. Do not promise reproducibility after deleting the mapping vault.
- Commit and validate mappings before publishing masked output.
- Distinct originals must not silently share a substitute. Detect collisions and allocate a different realistic substitute before committing.
- A finite lookup pool cannot represent unlimited distinct originals reversibly. Support approved pool expansion or raise `MappingCapacityError`; never silently recycle values.
- Use exact input values by default. Lossy normalization, such as trimming or case folding, must not prevent exact restoration. If normalized matching is enabled, retain sufficient protected variant information and document how it is carried through persisted datasets.
- Require scope metadata when unmasking. Do not implement an ambiguous global `unmask("Michael")` lookup.
- Keep versions explicit and retain old mappings/keys for as long as corresponding masked datasets must remain reversible.
- Treat source and masked inputs as explicitly identified states. Do not guess whether an arbitrary string has already been masked.

Preserve row counts, nulls, column order, supported data types, duplicate rows, and relationships. Relationship columns in different tables must share a compatible mapping scope. Nulls remain null; empty strings are handled explicitly and remain distinguishable from nulls. Reject unsupported types instead of silently casting them.

## 4. Human-readable masking rules

Masked values should resemble business data rather than exposed hashes, token IDs, or random gibberish. Internal keyed fingerprints are allowed in the protected mapping store.

### Names

Provide `mask_first_name()` and `mask_last_name()` and realistic lookup datasets. Example: `John` becomes `Michael`, and `Smith` becomes `Johnson`. Examples are illustrative; collision-free mapping takes precedence over any particular substitute.

When first name, last name, and full name exist, support constructing the masked full name from the masked components. Validate that this is compatible with exact restoration of the source full name, including spacing, punctuation, and inconsistent source values. Store a separate reversible full-name mapping where needed. If distinct originals would produce the same constructed full name, reject that configuration or use a documented independent full-name mapping mode; do not claim both guarantees have been met silently.

When only a full name exists, use a full-name domain and persistent full-name mappings. Do not assume all names have exactly two parts.

### Addresses

Mask `ADDRESS_LINE_1`, `ADDRESS_LINE_2`, and `ADDRESS_LINE_3` using realistic replacements. Leave city, state, country, ZIP, and postal code unchanged by default. Support related address lines as a coherent tuple when configured. Document that retaining geographic fields preserves potentially identifying information.

### Email

Produce valid addresses with synthetic usernames, using `example.com` as the default configurable domain. Example: `john.smith@gmail.com` becomes `michael.johnson@example.com`. Resolve username collisions. Do not assume two originals with the same local part have the same identity.

### Phone

For supported valid formats, preserve separators and digit positions while substituting digits. Configure locale-specific test-number pools and document their capacity. Do not assume arbitrary generated numbers are safe to call; examples and tests must not send messages or make calls.

### Membership number

For the configured format of nine digits followed by one letter, mask the digits and preserve the final letter, including its case where applicable. Preserve leading zeros using a string column. Ensure uniqueness within each suffix and detect capacity exhaustion.

### Invalid source values

Make invalid-value behavior configurable as `error` or `replace`. With `replace`, create a format-valid masked substitute and retain the exact invalid original for later unmasking. This replacement changes the masked representation only; it must not repair or overwrite the source. Unmasking must return the original invalid value, not a cleaned version.

## 5. Secure storage and concurrency

Use mapping records containing at least:

```text
namespace, domain, mapping_version
original_fingerprint, fingerprint_key_id
encrypted_original, encryption_key_id, encryption_metadata
masked_value, original_type
created_at, created_by, allocation_batch_id
```

- Encrypt exact original values using authenticated encryption, such as AES-256-GCM through a maintained cryptography library. Use correct nonce generation and bind mapping context as authenticated additional data.
- Use a keyed HMAC fingerprint over an unambiguous encoding of scope, type, and exact original for forward lookup. Plain hashes are insufficient for low-entropy values such as names and phone numbers. Validate potential fingerprint collisions rather than silently merging originals.
- Manage keys outside source code, YAML, wheels, and ordinary data tables. Define a secrets/KMS integration and key-rotation procedure; separate encryption and fingerprinting keys.
- Protect both the vault and fingerprint indexes with Unity Catalog permissions and trusted execution boundaries. Ordinary masked-data consumers must not receive vault access or decryption keys.
- Implement a concrete mapping allocator with serialized writes per scope, or another justified coordination mechanism, including ownership and recovery after crashes. Batch allocation rather than issuing writes per row.
- Delta `MERGE` alone does not establish uniqueness. Databricks primary-key, foreign-key, and unique constraints are not enforced. Enforce allocation invariants explicitly, validate them before output publication, and test simultaneous allocations, retries, and partial failures.
- Mapping writes must not happen inside UDFs. Worker retries and speculative execution must not create duplicate or conflicting mappings.

## 6. Authorization and audit

Authenticate through the actual execution identity and platform permissions. A caller-supplied `user="admin@company.com"` or role string must never grant permission.

Support policy grants for Admin, Compliance Team, and Data Steward roles, with namespace, domain, table, and column restrictions. Membership alone must not imply unrestricted access unless the configured policy explicitly grants it.

Explain the trusted boundary: Python library checks alone cannot restrict a user who already has arbitrary code execution with access to both the vault and keys. For privileged Databricks jobs, distinguish the job's Run as identity from the requester. Enforce requester authorization through a trusted entry point before a service principal decrypts anything, and restrict who can modify that job.

Require a non-empty reason for every unmask operation. Reject unauthorized access with `PermissionError` before exposing originals. Define and enforce allowed restoration destinations.

Audit requests, denials, authorization decisions, execution outcomes, and failures with request ID, timestamps, verified requester, execution identity, scope, source table/columns, destination when applicable, reason, and counts when available. Do not log sensitive field values, decrypted samples, or keys. Protect audit records from ordinary-user modification.

Account for Spark's lazy evaluation: a returned DataFrame is not proof that restoration executed. Record authorization separately from execution. For persisted restoration, record success only after the write and required validation complete. Document the limits of auditing arbitrary downstream actions on returned DataFrames. Audit unavailability must prevent release of restored data, with a documented recovery path for partial failures.

## 7. Public APIs, including usable unmasking

Implement the following contracts consistently and provide working examples. The names below are requirements for the package to be built, not claims about an existing installed library.

```python
config = load_config("masking.yaml")
masker = MaskingEngine(spark=spark, config=config)
unmasker = UnmaskingEngine(spark=spark, config=config)

masked_value = masker.mask_value(
    "john.smith@gmail.com",
    domain="customer_email",
    mapping_version="v1",
)
original_value = unmasker.unmask_value(
    masked_value,
    domain="customer_email",
    mapping_version="v1",
    reason="Approved investigation INC-12345",
)
assert original_value == "john.smith@gmail.com"

masked_df = masker.mask_dataframe(source_df, table="customer")

# Restore only the selected columns. All other columns retain their input values.
restored_df = unmasker.unmask_dataframe(
    masked_df,
    table="customer",
    columns=["email", "phone"],
    reason="Approved investigation INC-12345",
    on_missing="error",
)

# Restore all configured masked columns; still require authorization per column.
fully_restored_df = unmasker.unmask_dataframe(
    masked_df,
    table="customer",
    columns=None,
    reason="Approved round-trip validation",
    on_missing="error",
)

# Apply unmasking to an already persisted dataset in a new approved destination.
result = unmasker.unmask_table(
    source_table="qa.masked.customer",
    target_table="restricted.restore.customer_inc12345",
    table="customer",
    columns=["email", "phone"],
    reason="Approved investigation INC-12345",
    write_mode="errorifexists",
)
```

Namespace comes from configuration; value-level calls must resolve an explicit configured domain and version. DataFrame calls must validate compatible schema and column mappings. Persist a dataset manifest containing column scopes, mapping versions, configuration digest, and run ID without source values. Persisted-table restoration must validate the manifest instead of assuming that today's YAML matches historical output.

Default to `MissingMappingError` for an unknown non-null masked value. If an explicit `on_missing="keep_masked"` option is supported, report unresolved counts and never label partial restoration as complete. Raise `AmbiguousMappingError` for duplicate reverse matches; do not choose the first match. Wrong namespace/version, unavailable keys, or corrupt ciphertext must produce clear errors without exposing values.

Unmasking must never invent a lost original. Mapping and key backups, retention, recovery, and deletion consequences must be documented. A masked string by itself is insufficient without its scope and retained mapping.

## 8. YAML configuration

Provide a validated schema and a complete configuration, including security policies and key-provider settings without secret values. Example subset:

```yaml
namespace: wyndham_shared_test
mapping_store: security.masking.value_mappings
audit_store: security.masking.audit_events
mapping_version: v1
defaults:
  on_missing: error
  invalid_values: replace
  email_domain: example.com
tables:
  customer:
    columns:
      first_name:
        mask_type: first_name
        domain: customer_first_name
      last_name:
        mask_type: last_name
        domain: customer_last_name
      full_name:
        mask_type: full_name
        domain: customer_full_name
        mode: independent_mapping
      email:
        mask_type: email
        domain: customer_email
      phone:
        mask_type: phone
        domain: customer_phone
      address:
        mask_type: address
        domain: customer_address
      membership_no:
        mask_type: membership
        domain: membership_number
```

Show how a second table reuses domains to preserve joins. Validate unknown fields, missing columns, incompatible formats, insufficient pools, and unsupported types before processing.

## 9. Execution, scale, and dry run

Target 10M+ records and 100+ tables, with distributed processing and parallel table jobs governed by the allocator's concurrency model. Provide representative benchmarks with runtime, compute configuration, data cardinalities, and limitations; do not present unmeasured targets as verified performance.

Compute distinct source values in Spark, resolve existing mappings, allocate missing mappings in controlled batches, and join mappings back without multiplying rows. Do not collect complete datasets or large mapping tables to the driver. Broadcast only when measured size permits; otherwise use distributed joins. Tune partitioning and caching based on workload and runtime support.

Support on-demand DataFrame masking and relationship-aware subsetting. Define parent/child relationships and validate subset referential integrity before masking. Row-count preservation applies to the selected subset.

Implement `masker.dry_run(df, table="customer")` to report examined rows, fields affected, existing/missing distinct mappings, validation errors, and estimated pool capacity. Clearly label estimates and sampled counts. Dry runs must not write business outputs or allocate mappings; authorized request auditing may still occur. Do not expose original samples.

## 10. Tests and acceptance criteria

Use pytest, with a target of 90%+ coverage for package code. Include meaningful local tests and separately documented Databricks integration tests.

Required coverage:

- Every masker, valid and invalid values, nulls, empty strings, Unicode, separators, leading zeros, and type preservation.
- Exact round-trip restoration for values and full DataFrames, including duplicates and invalid originals. Compare full row multisets rather than relying on row order or counts alone.
- Partial-column unmasking and persisted-table unmasking in a fresh session using retained mappings and manifest metadata.
- Stable cross-table mappings and unchanged relationship joins.
- Collisions, lookup exhaustion, full-name conflicts, repeated runs, and concurrent allocation.
- Missing/ambiguous mappings, wrong versions, corrupt ciphertext, missing keys, and rotation/recovery behavior.
- Unauthorized users, spoofed caller identities, restricted columns/destinations, and missing reasons.
- Audit request/denial/execution records, audit outages, Spark lazy evaluation, retries, and partial-write recovery.
- No sensitive values in logs, exception messages, or audit payloads.

Deliver notebooks that create synthetic source data, configure the vault, mask and persist it, reload it in a new session, unmask selected columns to a restricted destination, and validate exact restoration. Include setup grants, secrets configuration, and an unauthorized-access demonstration. Use synthetic data only in distributed examples and test fixtures.

## 11. Deliverables

Deliver complete source code, buildable wheel, tests, sample lookup files, validated YAML, security bootstrap instructions, architecture diagram, README, installation guide, benchmarks or a runnable benchmark harness, and end-to-end Databricks notebooks.

Include a dedicated **How to Apply Unmasking** guide covering prerequisites, single-value restoration, selected columns, all configured columns, persisted tables, audit verification, and failure handling. Explain that only data masked with retained compatible mappings and keys can be restored.

Do not leave placeholder implementations for allocation, encryption, authorization, audit, or unmasking. Clearly list environment-specific adapters and any integration tests that could not be run. Require deployment-specific security review and measured validation before describing the result as production-ready.

## Reference notes

- Databricks documents that primary-key, foreign-key, and unique constraints are not enforced: https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-create-table-constraint
- Databricks recommends workspace files or Unity Catalog volumes instead of DBFS root for libraries: https://docs.databricks.com/aws/en/libraries/object-storage-libraries
- Databricks jobs use Run as privileges, which must be considered separately from the user triggering a job: https://docs.databricks.com/aws/en/jobs/privileges
