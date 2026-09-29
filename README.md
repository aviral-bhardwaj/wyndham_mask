# va_datamask

Private Python package for human-readable, reversible data masking. Mask and restore
individual strings, selected Spark DataFrame columns, or persisted Delta tables.
Original values are encrypted with AES-256-GCM; mappings are scoped, versioned, and
unique in both directions. This repository must not be published to PyPI.

## Get started locally

```bash
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install -e '.[dev]'
python examples/local_demo.py
python -m pytest -m 'not spark and not delta'
python -m build
```

The demo uses synthetic records, temporary keys, and an encrypted SQLite mapping
vault. It verifies an exact round trip without printing sensitive values. SQLite is
the reference backend for local development; use `DeltaRepository` on Databricks.

The build produces `dist/va_datamask-1.0.0-py3-none-any.whl`. Upload the wheel to a
Unity Catalog volume and install it on trusted Databricks compute:

```python
%pip install /Volumes/<catalog>/<schema>/<volume>/wheels/va_datamask-1.0.0-py3-none-any.whl
```

Restart Python after installation when required. Do not install the `spark` or
`delta` extras on Databricks: use the Spark and Delta versions bundled with its
runtime. Runtime qualification is required before deployment.

## Apply masking and unmasking

Initialize `masker` and `unmasker` with a validated configuration, vault, key ring,
trusted identity provider, operator-installed permission policy, and audit sink.
See the [complete Databricks notebook](notebooks/01_databricks_workflow.py) for setup.

```python
masked = masker.mask_value("john.smith@example.org", domain="customer_email")
original = unmasker.unmask_value(
    masked, domain="customer_email", reason="Approved investigation INC-12345"
)

masked_df = masker.mask_dataframe(source_df, table="customer")
restored_df = unmasker.unmask_dataframe(
    masked_df,
    table="customer",
    columns=["email", "phone"],
    reason="Approved investigation INC-12345",
)
# columns=None restores every configured column. Other columns stay unchanged.

masker.mask_table(
    source_table="restricted.source.customer",
    target_table="restricted.qa.customer",
    table="customer",
)
unmasker.unmask_table(
    source_table="restricted.qa.customer",
    target_table="restricted.restore.customer_inc12345",
    table="customer",
    columns=["email", "phone"],
    reason="Approved investigation INC-12345",
)
```

Both destination names must be explicitly permitted by policy. Table operations
create new destinations and refuse to overwrite or append. They preserve a manifest
with the original mapping configuration and versions. Retain the mapping vault and
keys: substitutes alone cannot reconstruct originals.

Read [How to Apply Unmasking](docs/UNMASKING.md), [security and bootstrap](docs/SECURITY.md),
[architecture](docs/ARCHITECTURE.md), and [validation and limitations](docs/VALIDATION.md).
The [reviewed build specification](docs/BUILD_SPEC.md) is retained for reference;
the validation guide explicitly identifies implementation limits.

## Supported behavior

- First/last names, independent full names, emails, phones, address lines, and
  membership numbers, with realistic substitute values.
- Exact string restoration, including whitespace, Unicode, invalid originals, empty
  strings, and nulls. Masked strings can be format-valid even when their originals
  were invalid. No normalization is applied to the protected originals.
- Consistent cross-table values through shared `namespace`, `domain`, and version.
  See [the customer/booking YAML example](examples/masking.yaml).
- Collision checks, capacity errors, authenticated encryption, HMAC lookup indexes,
  encryption-key rotation, and a non-expiring Delta allocation mutex.
- Scoped principal/table/column/destination authorization and value-free auditing.
- Distributed Spark joins; pure scalar and Pandas lookup UDFs for bounded snapshots.
- Read-only dry runs and explicit parent/child relationship subsetting.

This is an initial implementation, not a claim of production certification. Built-in
lookup files are demo-sized: first/last names have 32 entries each and the default
phone pool has 100 numbers per formatting pattern. They cannot support arbitrary
high-cardinality source values. Expand and validate lookup pools for your workload.
The 10M-row/100-table targets require benchmarking in your Databricks environment.

The package cannot enforce security against arbitrary code running with vault and
key access. Keep that access inside a trusted operator-controlled job or service.
Ordinary developers should receive only masked output tables.
