from dataclasses import replace
import os
import pytest
from wd_datamask import KeyRing, MaskingEngine, UnmaskingEngine, Config, PermissionManager, LocalIdentity
from wd_datamask.exceptions import ConfigurationError, IntegrityError, AuditError, RecoveryRequiredError
from wd_datamask.models import Scope
from wd_datamask.spark.pandas_udfs import make_lookup_pandas_udf
from wd_datamask.exceptions import MissingMappingError


def test_typed_name_helpers(setup):
    mask, unmask, _, _ = setup
    first = mask.mask_first_name("John", domain="first_name")
    last = mask.mask_last_name("Smith", domain="last_name")
    assert unmask.unmask_value(first, domain="first_name", reason="Helper") == "John"
    assert unmask.unmask_value(last, domain="last_name", reason="Helper") == "Smith"
    with pytest.raises(ConfigurationError):
        mask.mask_first_name("John", domain="email")
    with pytest.raises(ConfigurationError):
        mask.mask_last_name("John", domain="first_name")


def test_fingerprint_collision_is_not_merged(setup, monkeypatch):
    mask, _, deps, _ = setup
    scope = deps["config"].scope("email")
    monkeypatch.setattr(deps["keys"], "fingerprint", lambda *args: "1" * 64)
    with pytest.raises(IntegrityError):
        mask._resolve_many(["a@example.org", "b@example.org"], scope, "test")
    assert deps["store"].all_for_scope(scope) == []
    # A collision with an existing original must not reuse another source's mapping.
    mask.mask_value("a@example.org", domain="email")
    with pytest.raises(IntegrityError):
        mask.mask_value("b@example.org", domain="email")


def test_audit_failure_on_success_blocks_scalar_return(setup):
    mask, _, deps, _ = setup
    masked = mask.mask_value("John", domain="first_name")
    sink = deps["audit"]
    class FailSuccess:
        def emit(self, **event):
            if event["status"] == "SUCCEEDED":
                raise AuditError("Simulated")
            sink.emit(**event)
    engine = UnmaskingEngine(**{**deps, "audit": FailSuccess()})
    with pytest.raises(AuditError):
        engine.unmask_value(masked, domain="first_name", reason="Audit outage")
    assert sink.events()[-1]["status"] == "FAILED"


def test_failure_reporting_preserves_recovery_requirement(setup):
    mask, _, _, _ = setup
    class Broken:
        def emit(self, **event):
            raise AuditError("Unavailable")
    mask.audit = Broken()
    with pytest.raises(RecoveryRequiredError):
        mask._failed({"request_id": "test-request"}, RecoveryRequiredError("Restricted output needs reconciliation"))
    with pytest.raises(AuditError, match="test-request"):
        mask._failed({"request_id": "test-request"}, IntegrityError("Integrity failure"))


def test_rotation_failure_keeps_old_ciphertext(setup):
    mask, unmask, deps, _ = setup
    masked = mask.mask_value("John", domain="first_name")
    missing_old_key = KeyRing({"new": os.urandom(32)}, deps["keys"].fingerprint_keys, "new", "f1")
    with pytest.raises(IntegrityError):
        MaskingEngine(**{**deps, "keys": missing_old_key}).rotate_encryption("first_name", reason="Missing old key")
    assert unmask.unmask_value(masked, domain="first_name", reason="Old key retained") == "John"


def test_policy_all_dimensions(setup):
    _, _, deps, grant = setup
    manager = deps["permissions"]
    identity = manager.identity()
    scope = deps["config"].scope("email")
    for action, candidate, table, column, destination in [
        ("DELETE", scope, "customer", "email", None),
        ("UNMASK", Scope("other", "email", "v1"), "customer", "email", None),
        ("UNMASK", scope, "customer", "email", "not.allowed.destination"),
    ]:
        with pytest.raises(PermissionError):
            manager.require(identity, action, candidate, table, column, destination)
    grant["tables"] = ["customer"]
    with pytest.raises(PermissionError):
        manager.require(identity, "UNMASK", scope, "booking", "email")
    grant["allow_values"] = False
    with pytest.raises(PermissionError):
        manager.require(identity, "UNMASK", scope)


def test_bad_batch_config_and_version(setup, raw_config):
    _, _, deps, _ = setup
    for size in [0, 10001, "100"]:
        with pytest.raises(ConfigurationError):
            MaskingEngine(**deps, batch_size=size)
    for value in ["", 1, []]:
        with pytest.raises(ConfigurationError):
            deps["config"].scope("email", value)
    with pytest.raises(ConfigurationError):
        deps["config"].columns("not_configured")
    raw_config["tables"]["customer"]["columns"]["email"]["mask_type"] = []
    with pytest.raises(ConfigurationError):
        Config(raw_config)


def test_mapping_insert_constraints(setup):
    mask, _, deps, _ = setup
    mask.mask_value("John", domain="first_name")
    scope = deps["config"].scope("first_name")
    item = deps["store"].all_for_scope(scope)[0]
    with pytest.raises(IntegrityError):
        with deps["store"].allocation():
            deps["store"].insert([replace(item, fingerprint="different")])
    assert len(deps["store"].all_for_scope(scope)) == 1
    with pytest.raises(ValueError):
        deps["store"].find(scope, "unsupported", [])


def test_pandas_lookup_missing_raises():
    import pandas as pd
    lookup = make_lookup_pandas_udf({"John": "Michael"})
    with pytest.raises(MissingMappingError):
        lookup.func(pd.Series(["unknown"]))
    assert lookup.func(pd.Series(["John", None])).tolist() == ["Michael", None]


def test_identity_adapter_contract():
    from types import SimpleNamespace
    from wd_datamask import DatabricksIdentity
    class PlatformSession:
        def sql(self, query):
            assert query == "SELECT session_user() AS principal"
            return SimpleNamespace(first=lambda: SimpleNamespace(principal="verified-job-principal"))
    identity = DatabricksIdentity(PlatformSession()).current()
    assert identity.requester == identity.executor == "verified-job-principal"


def test_invalid_unmask_scope_is_audited(setup):
    _, unmask, deps, _ = setup
    with pytest.raises(ConfigurationError):
        unmask.unmask_value("value", domain="not_configured", reason="Invalid domain")
    assert deps["audit"].events()[-1]["status"] == "REJECTED"
