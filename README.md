# wd_datamask

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
python -m pytest                 # Spark tests too (Java 17 and local loopback networking)
python -m build
```

The demo uses synthetic records, temporary keys, and an encrypted SQLite mapping
vault. It verifies an exact round trip without printing sensitive values. SQLite is
the reference backend for local development; use `DeltaRepository` on Databricks.

The build produces `dist/wd_datamask-1.0.0-py3-none-any.whl`. Upload the wheel to a
Unity Catalog volume and install it on trusted Databricks compute:

```python
%pip install /Volumes/<catalog>/<schema>/<volume>/wheels/wd_datamask-1.0.0-py3-none-any.whl
```

Restart Python after installation when required. Do not install the `spark` or
`delta` extras on Databricks: use the Spark and Delta versions bundled with its
runtime. Runtime qualification is required before deployment.

Import the installed package in your notebook before initializing the engines:

```python
from wd_datamask import MaskingEngine, UnmaskingEngine
```

## Apply masking and unmasking

Initialize `masker` and `unmasker` with a validated configuration, vault, key ring,
trusted identity provider, operator-installed permission policy, and audit sink.
See the [complete Databricks notebook](notebooks/01_databricks_workflow.py) for setup, and
[notebooks/02_faker_end_to_end.py](notebooks/02_faker_end_to_end.py) for a runnable walkthrough of every
function on 12,000 Faker-generated customers and 30,000 bookings (`pip install faker`; also runs locally).

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

Built-in lookup pools come from public US Census name data and generated street names:
5,130 first names, 20,000 last names and 12,760 streets. Pool maskers are tiered, so
after the plain entries are used up they continue with readable hyphenated compounds
(`Anna-Marie`, `Smith-Parker`), giving each name domain hundreds of millions of
distinct substitutes; emails use `first.last@domain` then `first.last<n>@domain`;
phones keep the original separators and draw from ~6.3 billion well-formed synthetic
NANP numbers (`phone_pool: fictitious` restricts them to the reserved 555-01XX range).
Existing mappings stay authoritative when pools are expanded via `LookupManager(path)`.

DataFrame masking and unmasking are fully distributed: workers only see the distinct
values of one column at a time, new mappings are allocated by bounded rounds of Spark
joins and appended to the vault once per column, and the rows are joined with a
broadcast or shuffled lookup. The end-to-end volume qualification in
`scripts/scale_test.py` masks and restores 60 million rows (all seven mask types, ~5
million distinct customers) in one `mask_dataframe` call; see
[validation](docs/VALIDATION.md) for the measured run. `ParquetRepository` provides the
same distributed vault on a single machine without Delta jars for such qualification;
use `DeltaRepository` on Databricks. Call `engine.release()` after the returned
DataFrame has been written to drop cached lookups.

The package cannot enforce security against arbitrary code running with vault and
key access. Keep that access inside a trusted operator-controlled job or service.
Ordinary developers should receive only masked output tables.
