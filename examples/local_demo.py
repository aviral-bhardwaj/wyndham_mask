"""Runnable without Spark. Uses only synthetic data and a temporary encrypted vault."""
from pathlib import Path
import os
import tempfile
from wd_datamask import (load_config, KeyRing, MappingStore, AuditManager, LocalIdentity,
                        PermissionManager, MaskingEngine, UnmaskingEngine)


def main():
    config = load_config(Path(__file__).with_name("masking.yaml"))
    identity = LocalIdentity()
    # Demo-only ephemeral keys. Persistent deployments must retain externally managed keys.
    keys = KeyRing({"demo-e1": os.urandom(32)}, {"demo-f1": os.urandom(32)}, "demo-e1", "demo-f1")
    permissions = PermissionManager(identity, [{
        "principals": [identity.current().requester], "actions": ["MASK", "UNMASK"],
        "namespace": config.namespace, "domains": list(config.domains),
        "allow_values": True, "tables": ["customer", "booking"], "columns": ["*"],
    }])
    with tempfile.TemporaryDirectory() as directory:
        store = MappingStore(Path(directory) / "vault.sqlite")
        audit = AuditManager(Path(directory) / "audit.sqlite")
        deps = dict(config=config, store=store, keys=keys, permissions=permissions, audit=audit)
        mask, unmask = MaskingEngine(**deps), UnmaskingEngine(**deps)
        original = "john.smith@example.org"
        masked = mask.mask_value(original, domain="customer_email")
        restored = unmask.unmask_value(masked, domain="customer_email", reason="Synthetic demo validation")
        assert restored == original
        print("Mask/unmask round trip passed. Audit events:", len(audit.events()))
        store.close()
        audit.close()


if __name__ == "__main__":
    main()
