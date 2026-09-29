from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
import json
import os
import pytest
from va_datamask import KeyRing, MaskingEngine, UnmaskingEngine, MappingStore, PermissionManager, LocalIdentity
from va_datamask.exceptions import (MappingCapacityError, MissingMappingError, IntegrityError,
                                    AuditError, ConfigurationError)
from va_datamask.models import Scope


@pytest.mark.parametrize("domain,original", [
    ("first_name", "John"), ("first_name", "  José  "), ("last_name", "Smith"),
    ("full_name", "Dr. John  Smith Jr."), ("email", "john@gmail.com"), ("email", "invalid@"),
    ("phone", "(555) 123-4567"), ("phone", "+1 555.123.4567"), ("phone", "bad"),
    ("address", "123 Main St\nApt 4"), ("membership", "000000123a"),
    ("membership", "invalid"), ("first_name", ""), ("first_name", None),
])
def test_exact_roundtrip(setup, domain, original):
    mask, unmask, deps, _ = setup
    masked = mask.mask_value(original, domain=domain)
    assert unmask.unmask_value(masked, domain=domain, reason="Test exact restoration") == original
    assert mask.mask_value(original, domain=domain) == masked
    if original is not None:
        assert masked != original
        mappings = deps["store"].all_for_scope(deps["config"].scope(domain))
        assert len(mappings) == 1
        assert original == deps["keys"].decrypt(deps["config"].scope(domain), mappings[0])


def test_persistence_and_no_plaintext(setup, tmp_path):
    mask, _, deps, _ = setup
    secret = "unique.secret.customer@production.invalid"
    masked = mask.mask_value(secret, domain="email")
    fresh = MappingStore(tmp_path / "vault.sqlite")
    unmask = UnmaskingEngine(**{**deps, "store": fresh})
    assert unmask.unmask_value(masked, domain="email", reason="Fresh session") == secret
    assert secret.encode() not in (tmp_path / "vault.sqlite").read_bytes()
    assert secret not in json.dumps(deps["audit"].events())
    fresh.close()


def test_finite_pool_exhaustion_is_atomic(setup):
    mask, _, deps, _ = setup
    from va_datamask.maskers.base import NameMasker
    mask.maskers["first_name"] = NameMasker(["Alice", "Bob"])
    scope = deps["config"].scope("first_name")
    with pytest.raises(MappingCapacityError):
        mask._resolve_many(["one", "two", "three"], scope, "test")
    assert deps["store"].all_for_scope(scope) == []
    assert len(set(mask._resolve_many(["one", "two"], scope, "test").values())) == 2


def test_concurrent_connections_unique_allocations(setup, tmp_path):
    _, _, deps, _ = setup
    def allocate(index):
        store = MappingStore(tmp_path / "vault.sqlite")
        engine = MaskingEngine(**{**deps, "store": store})
        try:
            return engine.mask_value(f"person{index % 8}@production.invalid", domain="email")
        finally:
            store.close()
    with ThreadPoolExecutor(max_workers=8) as pool:
        values = list(pool.map(allocate, range(48)))
    assert len(set(values)) == 8
    assert values[:8] == values[8:16]
    assert len(deps["store"].all_for_scope(deps["config"].scope("email"))) == 8


def test_permissions_and_reason(setup):
    mask, unmask, deps, _ = setup
    masked = mask.mask_value("secret@example.net", domain="email")
    denied = UnmaskingEngine(**{**deps, "permissions": PermissionManager(LocalIdentity(), [])})
    with pytest.raises(PermissionError):
        denied.unmask_value(masked, domain="email", reason="Unauthorized request")
    with pytest.raises(PermissionError):
        unmask.unmask_value(masked, domain="email", reason="   ")
    with pytest.raises(TypeError):
        unmask.unmask_value(masked, domain="email", reason="Spoof", user="admin")
    assert sum(e["status"] == "DENIED" for e in deps["audit"].events()) == 2


def test_unknown_scope_and_version(setup):
    mask, unmask, _, _ = setup
    value = mask.mask_value("John", domain="first_name")
    for domain, version in [("first_name", "v2"), ("last_name", "v1")]:
        with pytest.raises(MissingMappingError):
            unmask.unmask_value(value, domain=domain, mapping_version=version, reason="Check scope")
    with pytest.raises(ConfigurationError):
        mask.mask_value("John", domain="unknown")


def test_ciphertext_and_context_tampering(setup):
    mask, _, deps, _ = setup
    mask.mask_value("john@example.net", domain="email")
    scope = deps["config"].scope("email")
    item = deps["store"].all_for_scope(scope)[0]
    for damaged in [replace(item, masked_value="tampered"), replace(item, encrypted_original="broken"),
                    replace(item, fingerprint="0" * 64), replace(item, encryption_key_id="unknown")]:
        with pytest.raises(IntegrityError):
            deps["keys"].decrypt(scope, damaged)
    with pytest.raises(IntegrityError):
        deps["keys"].decrypt(Scope("other", "email", "v1"), item)


def test_encryption_rotation_and_fingerprint_protection(setup):
    mask, _, deps, _ = setup
    value = mask.mask_value("John", domain="first_name")
    old = deps["keys"]
    keys = KeyRing({**old.encryption_keys, "e2": os.urandom(32)}, old.fingerprint_keys, "e2", "f1")
    updated = {**deps, "keys": keys}
    assert MaskingEngine(**updated).rotate_encryption("first_name", reason="Scheduled rotation") == 1
    fresh_keys = KeyRing({"e2": keys.encryption_keys["e2"]}, old.fingerprint_keys, "e2", "f1")
    assert UnmaskingEngine(**{**deps, "keys": fresh_keys}).unmask_value(value, domain="first_name", reason="Restore") == "John"
    changed_fp = KeyRing(keys.encryption_keys, {"f2": os.urandom(32)}, "e2", "f2")
    with pytest.raises(IntegrityError):
        MaskingEngine(**{**deps, "keys": changed_fp}).mask_value("John", domain="first_name")


def test_audit_outage_blocks_release(setup):
    mask, unmask, deps, _ = setup
    value = mask.mask_value("John", domain="first_name")
    deps["audit"].close()
    with pytest.raises(AuditError):
        unmask.unmask_value(value, domain="first_name", reason="Outage")


def test_source_type_rejected(setup):
    mask, unmask, _, _ = setup
    with pytest.raises(ConfigurationError):
        mask.mask_value(123, domain="membership")
    with pytest.raises(ConfigurationError):
        unmask.unmask_value(123, domain="membership", reason="Bad type")


def test_key_environment(monkeypatch):
    import base64
    for name in ("VA_DATAMASK_ENCRYPTION_KEY", "VA_DATAMASK_FINGERPRINT_KEY"):
        monkeypatch.setenv(name, base64.b64encode(os.urandom(32)).decode())
    assert KeyRing.from_env().encryption_key_id == "enc-v1"
    monkeypatch.setenv("VA_DATAMASK_ENCRYPTION_KEY", "invalid")
    with pytest.raises(ConfigurationError):
        KeyRing.from_env()


def test_invalid_keys():
    for args in [({}, {}, "e", "f"), ({"e": b"short"}, {"f": os.urandom(32)}, "e", "f"),
                 ({"e": b"x" * 32}, {"f": b"x" * 32}, "e", "f")]:
        with pytest.raises(ConfigurationError):
            KeyRing(*args)
