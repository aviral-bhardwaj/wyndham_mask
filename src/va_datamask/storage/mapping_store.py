"""SQLite reference vault. Database constraints and BEGIN IMMEDIATE serialize allocation."""
from contextlib import contextmanager
from dataclasses import asdict
import sqlite3
import threading
from ..models import Mapping
from ..exceptions import AmbiguousMappingError, IntegrityError

MAPPING_FIELDS = tuple(Mapping.__dataclass_fields__)


class MappingStore:
    distributed = False

    def __init__(self, path):
        self._lock = threading.RLock()
        self._db = sqlite3.connect(str(path), timeout=30, check_same_thread=False)
        self._db.row_factory = sqlite3.Row
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("CREATE TABLE IF NOT EXISTS mappings (scope TEXT NOT NULL, fingerprint TEXT NOT NULL, fingerprint_key_id TEXT NOT NULL, masked_value TEXT NOT NULL, encrypted_original TEXT NOT NULL, encryption_key_id TEXT NOT NULL, created_at TEXT NOT NULL, created_by TEXT NOT NULL, batch_id TEXT NOT NULL, PRIMARY KEY(scope, fingerprint), UNIQUE(scope, masked_value))")
        self._db.commit()

    @contextmanager
    def allocation(self):
        with self._lock:
            try:
                self._db.execute("BEGIN IMMEDIATE")
                yield
                self._db.commit()
            except Exception:
                self._db.rollback()
                raise

    def all_for_scope(self, scope):
        with self._lock:
            return [Mapping(**dict(r)) for r in self._db.execute("SELECT * FROM mappings WHERE scope=?", (scope.key(),))]

    def find(self, scope, field, values):
        if field not in {"fingerprint", "masked_value"}:
            raise ValueError("Unsupported lookup field")
        results = {}
        with self._lock:
            for start in range(0, len(values), 400):
                part = values[start:start + 400]
                rows = self._db.execute(f"SELECT * FROM mappings WHERE scope=? AND {field} IN ({','.join('?' for _ in part)})", (scope.key(), *part))
                for row in rows:
                    item = Mapping(**dict(row))
                    key = getattr(item, field)
                    if key in results and results[key] != item:
                        raise AmbiguousMappingError("Duplicate mapping key")
                    results[key] = item
        return results

    def key_ids(self, scope):
        with self._lock:
            return {r[0] for r in self._db.execute("SELECT DISTINCT fingerprint_key_id FROM mappings WHERE scope=?", (scope.key(),))}

    def insert(self, records):
        try:
            self._db.executemany(f"INSERT INTO mappings VALUES ({','.join('?' for _ in MAPPING_FIELDS)})", [tuple(asdict(r).values()) for r in records])
        except sqlite3.IntegrityError:
            raise IntegrityError("Mapping uniqueness violation") from None

    def replace_encryption(self, records):
        self._db.executemany("UPDATE mappings SET encrypted_original=?, encryption_key_id=? WHERE scope=? AND fingerprint=?", [(r.encrypted_original, r.encryption_key_id, r.scope, r.fingerprint) for r in records])

    def dataframe(self, spark, scope):
        # This local backend is for bounded tests/demo data. Production uses DeltaRepository.
        rows = [tuple(asdict(r).values()) for r in self.all_for_scope(scope)]
        return spark.createDataFrame(rows, ", ".join(f"{f} STRING" for f in MAPPING_FIELDS))

    def close(self):
        self._db.close()
