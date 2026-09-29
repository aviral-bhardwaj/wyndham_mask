from dataclasses import dataclass
import json


def encode(*parts):
    return json.dumps(parts, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


@dataclass(frozen=True)
class Scope:
    namespace: str
    domain: str
    version: str

    def key(self):
        return encode(self.namespace, self.domain, self.version).decode()


@dataclass(frozen=True)
class Mapping:
    scope: str
    fingerprint: str
    fingerprint_key_id: str
    masked_value: str
    encrypted_original: str
    encryption_key_id: str
    created_at: str
    created_by: str
    batch_id: str
