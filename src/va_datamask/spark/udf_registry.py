"""Pure UDFs only. Never perform vault writes from a Spark worker."""
from ..models import Mapping
from ..exceptions import MissingMappingError


def fingerprint_udf(keys, scope):
    from pyspark.sql.functions import udf
    from pyspark.sql.types import StringType
    return udf(lambda v: None if v is None else keys.fingerprint(scope, v), StringType())


def decrypt_udf(keys, scope):
    from pyspark.sql.functions import udf
    from pyspark.sql.types import StringType

    def decrypt(row):
        if row is None or row.fingerprint is None:
            return None
        return keys.decrypt(scope, Mapping(**row.asDict()))

    return udf(decrypt, StringType())


def register_lookup_udf(spark, name, mappings):
    """For small preallocated, non-sensitive forward maps only. All masker types work."""
    from pyspark.sql.types import StringType
    if len(mappings) > 10000:
        raise ValueError("Use distributed joins for large maps")
    snapshot = dict(mappings)

    def lookup(value):
        if value is None:
            return None
        if value not in snapshot:
            raise MissingMappingError("Preallocated mapping is missing")
        return snapshot[value]

    return spark.udf.register(name, lookup, StringType())
