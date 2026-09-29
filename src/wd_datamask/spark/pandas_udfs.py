"""Arrow-backed lookup UDF for bounded, already allocated mapping snapshots."""
from ..exceptions import MissingMappingError


def make_lookup_pandas_udf(mappings):
    import pandas as pd
    from pyspark.sql.functions import pandas_udf
    from pyspark.sql.types import StringType
    if len(mappings) > 10000:
        raise ValueError("Use distributed joins for large maps")
    snapshot = dict(mappings)

    @pandas_udf(StringType())
    def lookup(values: pd.Series) -> pd.Series:
        missing = values.notna() & ~values.isin(snapshot)
        if missing.any():
            raise MissingMappingError("Preallocated mapping is missing")
        return values.map(snapshot).where(values.notna(), None)

    return lookup
