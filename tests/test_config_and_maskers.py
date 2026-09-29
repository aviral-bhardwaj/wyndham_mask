import copy
import json
import re
import pytest
from wd_datamask import Config, load_config, LookupManager
from wd_datamask.exceptions import ConfigurationError
from wd_datamask.maskers.base import EmailMasker, PhoneMasker, MembershipMasker
from wd_datamask.storage.delta_repository import identifier


def test_yaml(tmp_path, raw_config):
    import yaml
    path = tmp_path / "config.yml"
    path.write_text(yaml.safe_dump(raw_config))
    config = load_config(path)
    assert config.digest == Config(raw_config).digest
    assert load_config(config) is config
    with pytest.raises(ConfigurationError):
        load_config(tmp_path / "absent")
    path.write_text("a: [broken")
    with pytest.raises(ConfigurationError):
        load_config(path)


@pytest.mark.parametrize("change", [
    lambda c: c.update(extra=True), lambda c: c.update(namespace=""), lambda c: c.update(tables={}),
    lambda c: c.update(defaults={"unknown": True}),
    lambda c: c["tables"]["customer"]["columns"]["email"].update(mask_type="nope"),
    lambda c: c["tables"]["customer"]["columns"]["email"].update(invalid_values="ignore"),
    lambda c: c["tables"]["customer"]["columns"]["full_name"].update(mode="derived"),
    lambda c: c["tables"]["customer"]["columns"]["email"].update(foo="x"),
    lambda c: c["tables"].update(customer={"wrong": {}}),
])
def test_bad_config(raw_config, change):
    change(raw_config)
    with pytest.raises(ConfigurationError):
        Config(raw_config)


def test_shared_domain_rules(raw_config):
    raw_config["tables"]["booking"] = copy.deepcopy(raw_config["tables"]["customer"])
    assert Config(raw_config).scope("email").domain == "email"
    raw_config["tables"]["booking"]["columns"]["email"]["email_domain"] = "example.org"
    with pytest.raises(ConfigurationError):
        Config(raw_config)


def test_invalid_lookup(tmp_path):
    path = tmp_path / "lookup.json"
    path.write_text(json.dumps({"first_names": ["A", "A"]}))
    with pytest.raises(ConfigurationError):
        LookupManager(path)
    path.write_text(json.dumps({"first_names": []}))
    with pytest.raises(ConfigurationError):
        LookupManager(path)


def test_phone_and_membership_patterns():
    phone = PhoneMasker()
    for value in ["(555) 123-4567", "+1 555-123-4567", "555.123.4567"]:
        phone.validate(value)
        result = phone.candidate(value, 32)
        assert re.sub(r"[0-9]", "D", value) == re.sub(r"[0-9]", "D", result)
        assert "555" in result
    with pytest.raises(ConfigurationError):
        phone.validate("invalid")
    membership = MembershipMasker()
    assert membership.candidate("123456789a", 12) == "000000012a"
    with pytest.raises(ConfigurationError):
        membership.validate("invalid")


def test_email_validation():
    masker = EmailMasker(LookupManager())
    with pytest.raises(ConfigurationError):
        masker.validate("invalid")
    with pytest.raises(ConfigurationError):
        EmailMasker(LookupManager(), "bad domain")
    assert masker.candidate("a@example.org", 12).endswith("@example.com")


@pytest.mark.parametrize("bad", ["x; DROP TABLE y", "a.b.c.d", "", "a b", "`a`"])
def test_identifier_rejects_sql(bad):
    with pytest.raises(ValueError):
        identifier(bad)
