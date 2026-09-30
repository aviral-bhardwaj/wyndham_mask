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
        result = phone.candidate(value, 32, 0)
        assert re.sub(r"[0-9]", "D", value) == re.sub(r"[0-9]", "D", result)
        digits = re.sub(r"[^0-9]", "", result)[-10:]
        assert digits[0] in "23456789" and digits[3] in "23456789"
        assert digits[1:3] != "11" and digits[4:6] != "11" and digits[3:6] != "555"
    assert phone.candidate("invalid", 7, 0).count("-") == 1 and phone.candidate("invalid", 7, 0).startswith("(")
    assert len({phone.candidate("5551234567", seed, 0) for seed in range(5000)}) == 5000
    with pytest.raises(ConfigurationError):
        phone.validate("invalid")
    fictitious = PhoneMasker(pool="fictitious")
    assert fictitious.capacity == 100
    assert "555-01" in fictitious.candidate("555-123-4567", 32, 0)
    with pytest.raises(ConfigurationError):
        PhoneMasker(pool="unknown")
    membership = MembershipMasker()
    assert membership.candidate("123456789a", 12, 0) == "000000012a"
    assert membership.candidate("bad", 12, 1) == "000000013A"
    with pytest.raises(ConfigurationError):
        membership.validate("invalid")


def test_pool_tiers_are_readable_and_unique():
    from wd_datamask.maskers.base import NameMasker, FullNameMasker, AddressMasker
    pool = NameMasker(["Ann", "Bob", "Cal"])
    assert pool.tier_capacity == (3, 6, 12) and pool.capacity == 21
    plain = {pool.candidate("x", seed, 0) for seed in range(3)}
    assert plain == {"Ann", "Bob", "Cal"}
    pairs = {pool.pick(i, 1) for i in range(6)}
    assert pairs == {"Ann-Bob", "Ann-Cal", "Bob-Ann", "Bob-Cal", "Cal-Ann", "Cal-Bob"}
    triples = {pool.pick(i, 2) for i in range(12)}
    assert len(triples) == 12 and all(len(set(t.split("-"))) >= 2 and "Ann-Ann" not in t for t in triples)
    assert all(a != b for a, b in (t.split("-")[:2] for t in triples))
    plain_only = NameMasker(["Ann", "Bob"], compound=False)
    assert plain_only.capacity == 2 and plain_only.candidate("x", 5, 30) in {"Ann", "Bob"}
    full = FullNameMasker(["Ann", "Bob"], ["Lee", "Ray"])
    first, last = full.candidate("x", 1 << 70, 0).split(" ")
    assert first in {"Ann", "Bob"} and last in {"Lee", "Ray"}
    address = AddressMasker(["Oak Street"])
    assert address.candidate("x", 0, 0) == "1 Oak Street" and address.candidate("x", 9999, 0) == "1 Oak Street"
    assert address.capacity == 9999
    with pytest.raises(ConfigurationError):
        AddressMasker([])
    with pytest.raises(ConfigurationError):
        NameMasker([])


def test_builtin_pools_are_large():
    lookups = LookupManager()
    assert len(lookups["first_names"]) >= 5000
    assert len(lookups["last_names"]) >= 20000
    assert len(lookups["streets"]) >= 10000
    masker = EmailMasker(lookups)
    assert masker.candidate("a@example.org", 0, 0) == "james.smith@example.com"
    assert masker.candidate("a@example.org", masker.plain, 0) == "james.smith1@example.com"
    assert masker.capacity == masker.plain * 1000


def test_email_validation():
    masker = EmailMasker(LookupManager())
    with pytest.raises(ConfigurationError):
        masker.validate("invalid")
    with pytest.raises(ConfigurationError):
        EmailMasker(LookupManager(), "bad domain")
    assert masker.candidate("a@example.org", 12, 0).endswith("@example.com")


@pytest.mark.parametrize("bad", ["x; DROP TABLE y", "a.b.c.d", "", "a b", "`a`"])
def test_identifier_rejects_sql(bad):
    with pytest.raises(ValueError):
        identifier(bad)
