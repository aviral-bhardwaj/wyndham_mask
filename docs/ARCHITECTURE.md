# Architecture

```mermaid
flowchart TD
    R[Approved execution identity] --> P[PermissionManager]
    C[YAML configuration and scope] --> P
    P --> A[Audit request and authorization]
    A --> M[MaskingEngine]
    A --> U[UnmaskingEngine]
    M --> D[Distinct values per column]
    D --> B[Distributed probing rounds: candidate, collision, retry]
    B --> L[Exclusive allocation mutex]
    L --> V[Delta mapping vault: one append per column]
    K[Secrets-managed key ring] --> V
    V --> J[Distributed joins]
    M --> J
    U --> J
    J --> F[Lazy DataFrame with provenance]
    F --> S[Restricted Delta staging table]
    S --> Q[Durable PREPARED audit]
    Q --> T[New destination and manifest]
    T --> Z[SUCCEEDED receipt]
```

A scope is `(namespace, domain, mapping_version)`. It is independent of a physical
table/column name so the same email in customer and booking can share a substitute.
Exact-string equality determines source identity; no normalization is performed.
Nulls bypass allocation and remain null. Existing mapping records are authoritative.

The mapping schema contains scope, keyed fingerprint and its key ID, substitute,
encrypted original and encryption key ID, UTC creation timestamp, execution identity,
and allocation batch ID. Nonce/tag are encoded in `encrypted_original`. String is the
only supported sensitive-field type; unsupported types fail before processing.

DataFrame masking is fully distributed (`spark/dataframe_masker.py`). For each
configured column the engine takes the distinct non-null values, fingerprints them with
an Arrow (pandas) UDF, anti-joins the vault to find unmapped values and then runs a
bounded probing loop: every round proposes one deterministic candidate per pending value
(`masker.candidate(original, fingerprint_seed, attempt)`), drops candidates already
present in the vault or equal to the original, resolves in-round collisions with a
window `row_number`, and carries the losers into the next round with `attempt + 1`.
Round results are checkpointed so the plan does not nest; typically two to five rounds
are needed. Survivors are encrypted on executors and appended to the vault in a single
write per column, all under the allocation mutex, so a capacity error leaves the vault
untouched. The source rows are then joined once per column with a persisted
`value -> substitute` lookup (broadcast when it has at most
`Engine.broadcast_threshold` distinct values). Unmasking mirrors this with a join on the
substitute and executor-side authenticated decryption of the distinct matches. Workers
therefore process distinct values rather than rows, and the driver never holds source
strings for DataFrame operations. Scalar `mask_value`/`unmask_value` calls keep a
small driver-side allocator that uses the same candidate contract.

Lookups stay cached until `engine.release()` (called automatically by the table APIs)
so that the lazy DataFrame returned to the caller does not recompute fingerprints.
Allocation is serialized per vault for correctness; the loop's cost is proportional
to the number of *new* distinct values, not to the row count. Source data must be
deterministic during a DataFrame call. Table APIs pin the input Delta version.

The built-in lookups (US Census first/last names, generated street names) produce
realistic names/addresses and synthetic email addresses. Pool maskers are tiered: plain
entries first (`Michael`), then hyphenated pairs (`Anna-Marie`, `Smith-Parker`) and
triples, which extends capacity to hundreds of millions of readable values. Phone
substitutes preserve the original separators and, by default, use well-formed synthetic
NANP numbers; a reserved 555-01XX pool is available for demos.
Pool exhaustion raises an error rather than merging source identities. Full names
use an independent reversible mapping; they are not reconstructed from first/last
name mappings, because that could lose punctuation or collapse distinct originals.
Address lines are independently mapped; coherent multi-line address tuples are not
implemented. Relationships between tables require shared domains.

The parent/child subsetting helper verifies unique non-null parent keys and rejects
orphan non-null child references before using a semi-join to select related rows.
Null child references are excluded from the chosen subset. Compose multiple edges
explicitly; automatic cyclic graph traversal is not implemented.

Table output manifests retain the logical table, full configuration/digest, per-column
scope/state, source Delta version, and request ID. Treat manifests as controlled table
metadata; the digest detects accidental configuration changes, not malicious edits
by a privileged writer. The table API verifies provenance and loads historical
configuration on restoration, avoiding accidental use of a newer mapping version.
