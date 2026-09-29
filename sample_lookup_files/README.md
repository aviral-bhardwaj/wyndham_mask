# Lookup format

The bundled example is `src/wd_datamask/data/lookups.json`. Copy it into your approved
data location and expand its `first_names`, `last_names`, and `streets` arrays for the
actual number of distinct values. Use:

```python
lookups = LookupManager("/Volumes/catalog/schema/volume/approved-lookups.json")
masker = MaskingEngine(..., lookups=lookups)
```

The JSON must contain non-empty unique strings in all three arrays. Do not populate
these files with production customer records. Changing or expanding a lookup pool
does not replace existing mappings. Retain the vault to preserve consistency.
