"""Authenticated encryption and keyed exact-value lookup; no plaintext persistence."""
import base64
import hashlib
import hmac
import os
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from ..exceptions import ConfigurationError, IntegrityError
from ..models import encode


class KeyRing:
    def __init__(self, encryption_keys, fingerprint_keys, encryption_key_id, fingerprint_key_id):
        self.encryption_keys = dict(encryption_keys)
        self.fingerprint_keys = dict(fingerprint_keys)
        self.encryption_key_id = encryption_key_id
        self.fingerprint_key_id = fingerprint_key_id
        if encryption_key_id not in self.encryption_keys or fingerprint_key_id not in self.fingerprint_keys:
            raise ConfigurationError("Active key IDs must exist in the key ring")
        if any(len(k) != 32 for k in [*self.encryption_keys.values(), *self.fingerprint_keys.values()]):
            raise ConfigurationError("Keys must be 32 bytes")
        if set(self.encryption_keys.values()) & set(self.fingerprint_keys.values()):
            raise ConfigurationError("Use separate encryption and fingerprint keys")

    @classmethod
    def from_env(cls):
        try:
            return cls(
                {"enc-v1": base64.b64decode(os.environ["WD_DATAMASK_ENCRYPTION_KEY"], validate=True)},
                {"fp-v1": base64.b64decode(os.environ["WD_DATAMASK_FINGERPRINT_KEY"], validate=True)},
                "enc-v1", "fp-v1",
            )
        except (KeyError, ValueError):
            raise ConfigurationError("Two base64 key environment variables are required") from None

    def fingerprint(self, scope, original, key_id=None):
        key_id = key_id or self.fingerprint_key_id
        try:
            key = self.fingerprint_keys[key_id]
        except KeyError:
            raise IntegrityError("Fingerprint key is unavailable") from None
        return hmac.new(key, encode(scope.key(), "string", original), hashlib.sha256).hexdigest()

    def encrypt(self, scope, original, masked, fingerprint):
        nonce = os.urandom(12)
        aad = encode(scope.key(), masked, fingerprint, "string")
        payload = AESGCM(self.encryption_keys[self.encryption_key_id]).encrypt(nonce, original.encode(), aad)
        return base64.b64encode(nonce + payload).decode(), self.encryption_key_id

    def decrypt(self, scope, mapping):
        try:
            if mapping.scope != scope.key():
                raise ValueError()
            payload = base64.b64decode(mapping.encrypted_original, validate=True)
            aad = encode(scope.key(), mapping.masked_value, mapping.fingerprint, "string")
            original = AESGCM(self.encryption_keys[mapping.encryption_key_id]).decrypt(
                payload[:12], payload[12:], aad
            ).decode()
            if not hmac.compare_digest(self.fingerprint(scope, original, mapping.fingerprint_key_id), mapping.fingerprint):
                raise ValueError()
            return original
        except Exception:
            raise IntegrityError("Mapping authentication failed or a required key is unavailable") from None
