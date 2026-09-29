# Security and operator setup

This package implements reversible pseudonymization, not anonymization. Repeated
substitutes reveal equality, and unmasked city/state/postal-code fields can remain
identifying. Assess the full dataset and permitted uses before granting access.

## Trusted boundary

Run masking and unmasking on operator-controlled compute or a job whose source,
libraries, policy, and parameters cannot be edited by ordinary users. Only its
execution principal may access mapping tables, lock tables, key secrets, and audit
writes. Users receiving masked tables must not receive vault or secret access.

`DatabricksIdentity` uses `session_user()`. In a Databricks Job, that is the configured
execution principal, not necessarily the human who triggered the job. This package
does not supply a human-requester gateway or interpret a claimed requester field.
Until a separately authenticated gateway is deployed, restrict job triggering to
the approved operator group and treat the job principal as the audited requester.
Never expose a broadly runnable privileged job with arbitrary source/destination
parameters. A future gateway must authenticate and authorize the human before
calling this library and preserve the trusted requester identity in the audit.

Policy grants are explicit principal allowlists. Admin, Compliance, and Data Steward
are organizational roles: resolve their authorized members through your identity
administration process and install their grants centrally. Role labels alone confer
no permission. Do not let callers submit or replace policy/configuration/key objects.
Policy restrictions are additive; omitting `allow_values` denies scalar operations.
Table/column grants support explicit `*`; domain/namespace/destination grants are exact.
Use ticket identifiers or non-sensitive business reasons in audit requests; do not
include customer values or secrets in the reason text.

## Bootstrap

1. Create a restricted Unity Catalog security schema for vault, mutex, and audit, and
   a separate restricted schema for source/staging/restored data. Ensure no broad
   schema/catalog-level SELECT grants are inherited.
2. Create two independent 32-byte random keys with your approved secrets/KMS process.
   Save their base64 values in the `wd-datamask` Databricks secret scope as `enc-v1`
   and `fp-v1`, matching the example notebook.
   Do not paste keys into source control, notebooks, YAML, or terminal history.
3. Install the wheel on trusted compute. Load key bytes through
   `dbutils.secrets.get` into `KeyRing`; the example notebook shows this adapter.
4. As an operator, call `DeltaRepository.bootstrap()` once before enabling jobs.
   Create the audit table with `event STRING`. Do not bootstrap while writers run.
5. Grant the service principal `USE CATALOG`, `USE SCHEMA`, and the required table
   `SELECT`/`MODIFY` privileges. Give it `CREATE TABLE` in approved destination schemas.
   Keep consumers out of those schemas until outputs are explicitly released.
6. Provision `PermissionManager` with the approved execution principal and exact
   domains, columns, and destination table names. Pin and review the job definition.
7. Run the synthetic notebook and negative authorization tests on the selected runtime.

Example grants, with placeholders to replace by an administrator:

```sql
GRANT USE CATALOG ON CATALOG masking_demo TO `approved-service-principal`;
GRANT USE SCHEMA ON SCHEMA masking_demo.security TO `approved-service-principal`;
GRANT USE SCHEMA ON SCHEMA masking_demo.restricted TO `approved-service-principal`;
GRANT SELECT, MODIFY ON TABLE masking_demo.security.value_mappings TO `approved-service-principal`;
GRANT SELECT, MODIFY ON TABLE masking_demo.security.allocation_lock TO `approved-service-principal`;
GRANT MODIFY ON TABLE masking_demo.security.audit_events TO `approved-service-principal`;
GRANT CREATE TABLE ON SCHEMA masking_demo.restricted TO `approved-service-principal`;
```

Source SELECT, audit-review SELECT, ownership needed to rename staging tables, and
secret access must be granted separately. Confirm effective inherited privileges.
These examples do not revoke pre-existing access or configure your workspace for you.

For local deployments, `KeyRing.from_env()` reads the base64-encoded keys from
`WD_DATAMASK_ENCRYPTION_KEY` and `WD_DATAMASK_FINGERPRINT_KEY`. Supply them through
your secret-injection mechanism, rather than committing them in configuration files.

## Encryption and lookup

Exact UTF-8 strings are encrypted using AES-256-GCM with a random 96-bit nonce per
encryption. Associated data binds scope, masked value, fingerprint, and string type.
The nonce and authentication tag are stored with ciphertext. HMAC-SHA256 over an
unambiguous encoding of scope, type, and original supplies a keyed lookup index.
Use different keys for encryption and fingerprints. The key ring has no repr that
prints key bytes, and exceptions/logging never include original values.

Spark fingerprinting/decryption serializes keys to trusted executors. This is not
an HSM-only or non-exportable-key design. Executors, their service accounts, logs,
storage, notebooks, and administrators are part of the trust boundary. Secure Spark
spill/shuffle/cache storage and executor isolation using your platform controls.
Do not export decrypted UDF results to untrusted compute. The small-map UDF helpers
ship plaintext forward mapping inputs to workers; use them only on trusted compute.

## Allocation and crash recovery

SQLite uses a transaction plus enforced unique indexes. Delta uses a single
pre-provisioned global mutex row with Serializable isolation. A conditional Delta
UPDATE changes a null owner to a unique allocation ID. All library mutations require
ownership, and batches resolve collisions before appending records. No executor UDF
writes mappings. The mutex serializes allocations across table jobs; reads and Spark
transforms can execute concurrently. Contention returns `AllocationBusyError` for a
bounded job-level retry with backoff. Do not bypass the repository with direct writes.

No lock expiration or automatic takeover exists. On a crashed writer:

1. Identify the mutex owner and stop/terminate its job and any still-running tasks.
2. Validate uniqueness of both fingerprint and masked value for affected scopes with
   `repo.validate(scope)`, and review any completed batch records.
3. Only after confirming that the old owner cannot resume, clear the mutex owner with
   an operator-controlled UPDATE. Never reset a live owner's lock.
4. Retry; persisted mappings are reused. Previously allocated but unpublished mappings
   are harmless and must not be recycled while any masked data may refer to them.

Delta does not enforce unique/primary-key constraints. Direct vault writes, duplicate
mutex rows, tampering, or uncontrolled operators invalidate the coordination model.
The library detects duplicate join keys and refuses ambiguous restoration.

## Rotation, retention, and release

Load old and new encryption keys in a `KeyRing`, make the new ID active, grant ROTATE,
and call `engine.rotate_encryption(domain, reason=...)`. Delta maintenance processes
bounded batches; a crash can leave mixed key IDs, so retain both keys and retry. Old
Delta snapshots/backups still require old keys. Rotation is not key deletion.

Fingerprint-key changes are rejected within a populated scope. Use an explicit new
mapping version and retain the old fingerprint key for old data, or implement and
review a separate controlled index migration. Do not silently switch active keys.

Back up mappings, manifests, and key versions with aligned retention. Protect audit
storage against ordinary-user modification; export receipts to your centralized
immutable audit service if required by policy. The built-in Delta audit sink is not
tamper-proof against privileged administrators.

For table restoration, staging and destination schemas must remain restricted until
the success receipt is verified. A durable PREPARED event precedes publication.
There is no atomic transaction across the output and audit tables: a post-publication
audit outage requires operator reconciliation. No automatic consumer GRANT is issued.

## Primary platform references

- [Databricks job execution identity](https://docs.databricks.com/aws/en/jobs/privileges)
- [Non-enforced table constraints](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-create-table-constraint)
- [Delta isolation levels](https://docs.databricks.com/aws/en/optimizations/isolation/isolation-levels)
- [Private library installation locations](https://docs.databricks.com/aws/en/libraries/object-storage-libraries)
