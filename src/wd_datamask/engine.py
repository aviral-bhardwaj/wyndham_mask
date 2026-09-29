from dataclasses import replace
import itertools
import uuid
from .config.yaml_loader import load_config
from .exceptions import ConfigurationError, IntegrityError, MappingCapacityError, AuditError, RecoveryRequiredError
from .maskers.base import make_masker
from .models import Mapping
from .security.auditing import utcnow
from .storage.lookup_manager import LookupManager


def batches(values, size=1000):
    iterator = iter(values)
    while batch := list(itertools.islice(iterator, size)):
        yield batch


class Engine:
    def __init__(self, *, config, store, keys, permissions, audit, spark=None, lookups=None, batch_size=1000,
                 broadcast_threshold=500_000):
        self.config = load_config(config)
        self.store, self.keys, self.permissions, self.audit = store, keys, permissions, audit
        self.spark = spark
        self.lookups = lookups or LookupManager()
        if not isinstance(batch_size, int) or not 1 <= batch_size <= 10000:
            raise ConfigurationError("Batch size must be between 1 and 10000")
        if not isinstance(broadcast_threshold, int) or broadcast_threshold < 0:
            raise ConfigurationError("Broadcast threshold must be a non-negative integer")
        self.batch_size = batch_size
        # Lookups with at most this many distinct values are broadcast to every executor.
        self.broadcast_threshold = broadcast_threshold
        self.maskers = {d: make_masker(spec, self.lookups) for d, spec in self.config.domains.items()}
        self._tracked = []

    def _track(self, frame):
        """Remember a persisted lookup frame referenced by a returned lazy plan."""
        self._tracked.append(frame)

    def release(self):
        """Unpersist cached lookup frames from earlier DataFrame operations.

        Call it once the DataFrames returned by ``mask_dataframe``/``unmask_dataframe``
        have been written or are no longer needed. Table operations call it themselves.
        """
        while self._tracked:
            try:
                self._tracked.pop().unpersist()
            except Exception:
                pass

    def _begin(self, action, scopes, *, reason, table=None, columns=None, destination=None):
        identity = self.permissions.identity()
        context = dict(request_id=str(uuid.uuid4()), action=action, requester=identity.requester,
                       executor=identity.executor, reason=reason, table=table,
                       columns=columns, scopes=[s.key() for s in scopes], destination=destination)
        self.audit.emit(**context, status="REQUESTED")
        try:
            if not isinstance(reason, str) or not reason.strip():
                raise PermissionError("A non-empty reason is required")
            for i, scope in enumerate(scopes):
                self.permissions.require(identity, action, scope, table,
                                         columns[i] if columns else None, destination)
        except PermissionError:
            self.audit.emit(**context, status="DENIED")
            raise
        self.audit.emit(**context, status="AUTHORIZED")
        return context

    def _failed(self, context, error):
        try:
            self.audit.emit(**context, status="FAILED", error_type=type(error).__name__)
        except AuditError:
            if isinstance(error, RecoveryRequiredError):
                raise error from None
            raise AuditError(f"Request {context['request_id']} failed and failure auditing is unavailable") from None

    def _rejected(self, action, reason, error, table=None):
        identity = self.permissions.identity()
        self.audit.emit(request_id=str(uuid.uuid4()), action=action, status="REJECTED",
                        requester=identity.requester, executor=identity.executor,
                        reason=reason, table=table, error_type=type(error).__name__)

    @staticmethod
    def _value(value):
        if value is not None and not isinstance(value, str):
            raise ConfigurationError("Masking supports string columns and nulls only")

    def _key_check(self, scope):
        ids = self.store.key_ids(scope)
        if ids and ids != {self.keys.fingerprint_key_id}:
            raise IntegrityError("Fingerprint key changes require a new mapping version or an explicit migration")

    def _resolve_many(self, originals, scope, actor):
        for value in originals:
            self._value(value)
        originals = list(dict.fromkeys(v for v in originals if v is not None))
        if not originals:
            return {}
        masker = self.maskers[scope.domain]
        for value in originals:
            masker.validate(value)
        with self.store.allocation():
            self._key_check(scope)
            fingerprints = {v: self.keys.fingerprint(scope, v) for v in originals}
            if len(set(fingerprints.values())) != len(originals):
                raise IntegrityError("Fingerprint collision detected")
            existing = self.store.find(scope, "fingerprint", list(fingerprints.values()))
            resolved = {}
            for original, fingerprint in fingerprints.items():
                if fingerprint in existing:
                    item = existing[fingerprint]
                    if self.keys.decrypt(scope, item) != original:
                        raise IntegrityError("Fingerprint collision detected")
                    resolved[original] = item.masked_value
            pending = {v: 0 for v in originals if v not in resolved}
            allocated, reserved = [], set()
            batch_id = str(uuid.uuid4())
            while pending:
                candidates = {}
                for original, attempt in pending.items():
                    if attempt >= masker.max_attempts:
                        raise MappingCapacityError("No free substitute within the pool/probe limit; expand the lookup pool")
                    seed = int(fingerprints[original], 16)
                    candidates[original] = masker.candidate(original, seed, attempt)
                used = self.store.find(scope, "masked_value", list(set(candidates.values())))
                for original, candidate in candidates.items():
                    if candidate == original or candidate in used or candidate in reserved:
                        pending[original] += 1
                        continue
                    encrypted, key_id = self.keys.encrypt(scope, original, candidate, fingerprints[original])
                    allocated.append(Mapping(scope.key(), fingerprints[original], self.keys.fingerprint_key_id,
                                             candidate, encrypted, key_id, utcnow(), actor, batch_id))
                    reserved.add(candidate)
                    resolved[original] = candidate
                    del pending[original]
            self.store.insert(allocated)
            return resolved

    def rotate_encryption(self, domain, *, reason, mapping_version=None):
        scope = self.config.scope(domain, mapping_version)
        context = self._begin("ROTATE", [scope], reason=reason)
        count = 0
        try:
            with self.store.allocation():
                for batch in batches(self.store.all_for_scope(scope), self.batch_size):
                    rotated = []
                    for item in batch:
                        original = self.keys.decrypt(scope, item)
                        cipher, key_id = self.keys.encrypt(scope, original, item.masked_value, item.fingerprint)
                        rotated.append(replace(item, encrypted_original=cipher, encryption_key_id=key_id))
                    self.store.replace_encryption(rotated)
                    count += len(rotated)
            self.audit.emit(**context, status="SUCCEEDED", records=count)
            return count
        except Exception as error:
            self._failed(context, error)
            raise
