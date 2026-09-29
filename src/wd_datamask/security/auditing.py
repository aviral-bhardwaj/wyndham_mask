from datetime import datetime, timezone
import json
import sqlite3
import threading
from ..exceptions import AuditError


def utcnow():
    return datetime.now(timezone.utc).isoformat()


class AuditManager:
    """Durable local audit sink. Store its file in an operator-controlled directory."""
    def __init__(self, path):
        self._lock = threading.RLock()
        self._db = sqlite3.connect(str(path), check_same_thread=False)
        self._db.execute("CREATE TABLE IF NOT EXISTS audit (id INTEGER PRIMARY KEY, event TEXT NOT NULL)")
        self._db.commit()

    def emit(self, **event):
        event = {"timestamp": utcnow(), **event}
        try:
            with self._lock, self._db:
                self._db.execute("INSERT INTO audit(event) VALUES (?)", (json.dumps(event, sort_keys=True),))
        except Exception:
            raise AuditError("Audit persistence failed; operation stopped") from None

    def events(self):
        with self._lock:
            return [json.loads(r[0]) for r in self._db.execute("SELECT event FROM audit ORDER BY id")]

    def close(self):
        self._db.close()


class DeltaAuditManager:
    def __init__(self, spark, table):
        from ..storage.delta_repository import identifier
        self.spark, self.table = spark, identifier(table)

    def emit(self, **event):
        try:
            payload = json.dumps({"timestamp": utcnow(), **event}, sort_keys=True)
            self.spark.createDataFrame([(payload,)], "event STRING").write.format("delta").mode("append").saveAsTable(self.table)
        except Exception:
            raise AuditError("Audit persistence failed; operation stopped") from None
