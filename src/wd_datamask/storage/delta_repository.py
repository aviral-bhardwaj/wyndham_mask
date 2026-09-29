"""Delta vault with a non-expiring, atomic global allocation mutex.

Bootstrap once with an operator, before enabling writers. Recovery requires stopping
the abandoned owner first; leases are deliberately not stolen on a timer.
"""
from contextlib import contextmanager
from dataclasses import asdict
import os
import re
import uuid
from ..exceptions import AllocationBusyError, AmbiguousMappingError, IntegrityError
from ..models import Mapping
from .mapping_store import MAPPING_FIELDS


def identifier(name):
    if not isinstance(name, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*){0,2}", name):
        raise ValueError("Expected a simple catalog.schema.table identifier")
    return name


class DeltaRepository:
    distributed = True
    format = "delta"

    def __init__(self, spark, table, lock_table):
        self.spark = spark
        self.table, self.lock_table = identifier(table), identifier(lock_table)
        if self.table == self.lock_table:
            raise ValueError("Vault and lock tables must differ")
        self._owner = None

    def bootstrap(self):
        """Operator-only setup. Do not call concurrently with live writers."""
        schema = ", ".join(f"{f} STRING NOT NULL" for f in MAPPING_FIELDS)
        self.spark.sql(f"CREATE TABLE IF NOT EXISTS {self.table} ({schema}) USING DELTA")
        self.spark.sql(f"CREATE TABLE IF NOT EXISTS {self.lock_table} USING DELTA AS SELECT 1 AS id, CAST(NULL AS STRING) AS owner")
        self.spark.sql(f"ALTER TABLE {self.lock_table} SET TBLPROPERTIES ('delta.isolationLevel'='Serializable')")

    @contextmanager
    def allocation(self):
        from delta.tables import DeltaTable
        from pyspark.sql import functions as F
        if self._owner is not None:
            raise AllocationBusyError("This repository already holds an allocation lock")
        rows = self.spark.table(self.lock_table).limit(2).collect()
        if len(rows) != 1 or rows[0].id != 1:
            raise IntegrityError("Allocation mutex must contain exactly one provisioned row")
        owner = str(uuid.uuid4())
        lock = DeltaTable.forName(self.spark, self.lock_table)
        try:
            lock.update((F.col("id") == 1) & F.col("owner").isNull(), {"owner": F.lit(owner)})
        except Exception:
            raise AllocationBusyError("Concurrent allocation; retry the batch later") from None
        rows = self.spark.table(self.lock_table).limit(2).collect()
        if len(rows) != 1 or rows[0].owner != owner:
            raise AllocationBusyError("Allocation mutex is held; retry later or follow crash recovery")
        self._owner = owner
        try:
            yield
        finally:
            # Retaining the lock on an uncertain failure is safer than stealing it.
            try:
                lock.update((F.col("id") == 1) & (F.col("owner") == owner), {"owner": F.lit(None).cast("string")})
            finally:
                self._owner = None

    def _require_lock(self):
        if self._owner is None:
            raise IntegrityError("Mapping mutations require an allocation lock")
        rows = self.spark.table(self.lock_table).limit(2).collect()
        if len(rows) != 1 or rows[0].owner != self._owner:
            raise IntegrityError("Allocation ownership was lost; stop the writer")

    def dataframe(self, spark, scope):
        from pyspark.sql import functions as F
        return spark.table(self.table).where(F.col("scope") == scope.key())

    def find(self, scope, field, values):
        from pyspark.sql import functions as F
        if field not in {"fingerprint", "masked_value"}:
            raise ValueError("Unsupported lookup field")
        if not values:
            return {}
        rows = self.dataframe(self.spark, scope).where(F.col(field).isin(values)).collect()
        result = {}
        for row in rows:
            item = Mapping(**row.asDict())
            key = getattr(item, field)
            if key in result:
                raise AmbiguousMappingError("Duplicate mapping key")
            result[key] = item
        return result

    def key_ids(self, scope):
        rows = self.dataframe(self.spark, scope).select("fingerprint_key_id").distinct().limit(2).collect()
        return {row.fingerprint_key_id for row in rows}

    def insert(self, records):
        self._require_lock()
        if not records:
            return
        rows = [tuple(asdict(r).values()) for r in records]
        self._append(self.spark.createDataFrame(rows, ", ".join(f"{f} STRING" for f in MAPPING_FIELDS)))

    def insert_dataframe(self, frame):
        """Append a distributed batch of mappings (columns MAPPING_FIELDS) under the lock."""
        self._require_lock()
        self._append(frame.select(*MAPPING_FIELDS))

    def _append(self, frame):
        frame.write.format(self.format).mode("append").saveAsTable(self.table)

    def all_for_scope(self, scope):
        # Streaming is used for maintenance; never collect the vault into the driver.
        return (Mapping(**r.asDict()) for r in self.dataframe(self.spark, scope).toLocalIterator())

    def replace_encryption(self, records):
        from delta.tables import DeltaTable
        self._require_lock()
        if not records:
            return
        rows = [(r.scope, r.fingerprint, r.encrypted_original, r.encryption_key_id) for r in records]
        source = self.spark.createDataFrame(rows, "scope STRING, fingerprint STRING, encrypted_original STRING, encryption_key_id STRING")
        DeltaTable.forName(self.spark, self.table).alias("t").merge(
            source.alias("s"), "t.scope=s.scope AND t.fingerprint=s.fingerprint"
        ).whenMatchedUpdate(set={"encrypted_original": "s.encrypted_original", "encryption_key_id": "s.encryption_key_id"}).execute()

    def validate(self, scope):
        from pyspark.sql import functions as F
        frame = self.dataframe(self.spark, scope)
        for field in ("fingerprint", "masked_value"):
            if frame.groupBy(field).count().where(F.col("count") > 1).limit(1).count():
                raise AmbiguousMappingError("Vault contains duplicate mapping keys")


class ParquetRepository(DeltaRepository):
    """Single-node development/benchmark vault backed by Parquet tables.

    It keeps the distributed code path of ``DeltaRepository`` (Spark joins, one append
    per column) but replaces the Delta mutex with an OS file lock in ``lock_dir`` and
    ``MERGE`` with a full rewrite. Use it to qualify large volumes on a local Spark
    installation without Delta jars. It is not for shared or production use: the file
    lock only coordinates processes on one machine.
    """
    format = "parquet"

    def __init__(self, spark, table, lock_dir):
        self.spark, self.table = spark, identifier(table)
        self.lock_table = None
        self.lock_dir = str(lock_dir)
        self._owner = None
        self._handle = None

    def bootstrap(self):
        schema = ", ".join(f"{f} STRING" for f in MAPPING_FIELDS)
        self.spark.sql(f"CREATE TABLE IF NOT EXISTS {self.table} ({schema}) USING PARQUET")
        os.makedirs(self.lock_dir, exist_ok=True)

    @contextmanager
    def allocation(self):
        import fcntl
        if self._owner is not None:
            raise AllocationBusyError("This repository already holds an allocation lock")
        path = os.path.join(self.lock_dir, f"{self.table}.lock")
        handle = open(path, "a+")
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            handle.close()
            raise AllocationBusyError("Allocation mutex is held; retry later") from None
        self._owner, self._handle = str(uuid.uuid4()), handle
        try:
            yield
        finally:
            self._owner = None
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            handle.close()
            self._handle = None

    def _require_lock(self):
        if self._owner is None:
            raise IntegrityError("Mapping mutations require an allocation lock")

    def replace_encryption(self, records):
        from pyspark.sql import functions as F
        self._require_lock()
        if not records:
            return
        rows = [(r.scope, r.fingerprint, r.encrypted_original, r.encryption_key_id) for r in records]
        source = self.spark.createDataFrame(rows, "scope STRING, fingerprint STRING, new_encrypted STRING, new_key STRING")
        current = self.spark.table(self.table)
        merged = current.join(source, ["scope", "fingerprint"], "left").select(
            *[F.coalesce("new_encrypted", "encrypted_original").alias(f) if f == "encrypted_original"
              else F.coalesce("new_key", "encryption_key_id").alias(f) if f == "encryption_key_id"
              else F.col(f) for f in MAPPING_FIELDS])
        staged = self.table + "_rotation_stage"
        merged.write.format(self.format).mode("overwrite").saveAsTable(staged)
        self.spark.table(staged).write.format(self.format).mode("overwrite").saveAsTable(self.table)
        self.spark.sql(f"DROP TABLE IF EXISTS {staged}")
