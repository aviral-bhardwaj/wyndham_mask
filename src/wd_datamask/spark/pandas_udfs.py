"""Arrow-backed (pandas) UDFs used by the distributed masking and unmasking paths.

All functions here are pure: they never read or write the vault. The allocator in
``dataframe_masker`` composes them with distributed joins so that Spark workers only
ever see the distinct values of the column they are processing, and the vault is
appended exactly once per column.
"""
from ..exceptions import MissingMappingError, ConfigurationError
from ..models import Mapping
from ..storage.mapping_store import MAPPING_FIELDS


def fingerprint_pandas_udf(keys, scope):
    import pandas as pd
    from pyspark.sql.functions import pandas_udf
    from pyspark.sql.types import StringType

    @pandas_udf(StringType())
    def fingerprint(values: pd.Series) -> pd.Series:
        return values.map(lambda v: None if v is None else keys.fingerprint(scope, v))

    return fingerprint


def candidate_pandas_udf(masker):
    import pandas as pd
    from pyspark.sql.functions import pandas_udf
    from pyspark.sql.types import StringType

    @pandas_udf(StringType())
    def candidate(values: pd.Series, fingerprints: pd.Series, attempts: pd.Series) -> pd.Series:
        out = []
        for value, fingerprint, attempt in zip(values, fingerprints, attempts):
            if value is None:
                out.append(None)
            else:
                out.append(masker.candidate(value, int(fingerprint, 16), int(attempt)))
        return pd.Series(out, dtype="object")

    return candidate


def validate_pandas_udf(masker):
    import pandas as pd
    from pyspark.sql.functions import pandas_udf
    from pyspark.sql.types import BooleanType

    def ok(value):
        if value is None:
            return True
        try:
            masker.validate(value)
            return True
        except ConfigurationError:
            return False

    @pandas_udf(BooleanType())
    def valid(values: pd.Series) -> pd.Series:
        return values.map(ok).astype("boolean")

    return valid


def encrypt_pandas_udf(keys, scope):
    import pandas as pd
    from pyspark.sql.functions import pandas_udf
    from pyspark.sql.types import StringType

    @pandas_udf(StringType())
    def encrypt(values: pd.Series, masked: pd.Series, fingerprints: pd.Series) -> pd.Series:
        return pd.Series([keys.encrypt(scope, v, m, f)[0] for v, m, f in zip(values, masked, fingerprints)], dtype="object")

    return encrypt


def decrypt_pandas_udf(keys, scope):
    """Takes the mapping columns in MAPPING_FIELDS order and returns the original."""
    import pandas as pd
    from pyspark.sql.functions import pandas_udf
    from pyspark.sql.types import StringType

    @pandas_udf(StringType())
    def decrypt(*columns: pd.Series) -> pd.Series:
        out = []
        for row in zip(*columns):
            if row[1] is None:  # fingerprint absent: no mapping was joined
                out.append(None)
            else:
                out.append(keys.decrypt(scope, Mapping(**dict(zip(MAPPING_FIELDS, row)))))
        return pd.Series(out, dtype="object")

    return decrypt


def make_lookup_pandas_udf(mappings):
    """Bounded, preallocated forward map for already allocated, non-sensitive values."""
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
