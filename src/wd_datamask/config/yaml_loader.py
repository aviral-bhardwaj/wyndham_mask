import copy
import hashlib
import json
import re
from pathlib import Path
import yaml
from ..exceptions import ConfigurationError
from ..models import Scope

TYPES = {"first_name", "last_name", "full_name", "email", "phone", "address", "membership"}


class Config:
    def __init__(self, raw):
        self.raw = copy.deepcopy(raw)
        raw = self.raw
        if not isinstance(raw, dict) or set(raw) - {"namespace", "mapping_version", "tables", "defaults"}:
            raise ConfigurationError("Invalid configuration root")
        for k in ("namespace", "mapping_version"):
            if not isinstance(raw.get(k), str) or not raw[k]:
                raise ConfigurationError("Namespace and mapping version are required")
        self.namespace = raw["namespace"]
        self.version = raw["mapping_version"]
        self.defaults = raw.get("defaults", {})
        if not isinstance(self.defaults, dict) or set(self.defaults) - {"invalid_values", "email_domain"}:
            raise ConfigurationError("Unsupported defaults")
        self.tables = raw.get("tables", {})
        if not isinstance(self.tables, dict) or not self.tables:
            raise ConfigurationError("At least one table is required")
        self.domains = {}
        for table, body in self.tables.items():
            if not isinstance(body, dict) or set(body) != {"columns"} or not isinstance(body["columns"], dict) or not body["columns"]:
                raise ConfigurationError("Each table must contain a non-empty columns mapping")
            for column, definition in body["columns"].items():
                if not all(isinstance(x, str) and re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", x) for x in (table, column)):
                    raise ConfigurationError("Invalid table or column identifier")
                if not isinstance(definition, dict) or set(definition) - {"mask_type", "domain", "invalid_values", "email_domain", "mode"}:
                    raise ConfigurationError("Unsupported column configuration")
                spec = {**self.defaults, **definition}
                if not isinstance(spec.get("mask_type"), str) or spec["mask_type"] not in TYPES or not isinstance(spec.get("domain"), str) or not spec["domain"]:
                    raise ConfigurationError("Mask type and domain are required")
                if spec.get("invalid_values", "error") not in {"error", "replace"}:
                    raise ConfigurationError("Invalid source-value policy")
                if spec.get("mode", "independent_mapping") != "independent_mapping":
                    raise ConfigurationError("Only independent reversible full-name mappings are supported")
                prior = self.domains.setdefault(spec["domain"], spec)
                if prior != spec:
                    raise ConfigurationError("A domain must have the same masking rules across tables")
        self.digest = hashlib.sha256(json.dumps(self.raw, sort_keys=True).encode()).hexdigest()

    def columns(self, table):
        if table not in self.tables:
            raise ConfigurationError("Table is not configured")
        return self.tables[table]["columns"]

    def scope(self, domain, version=None):
        if domain not in self.domains:
            raise ConfigurationError("Unknown domain")
        if version is not None and (not isinstance(version, str) or not version):
            raise ConfigurationError("Mapping version must be a non-empty string")
        return Scope(self.namespace, domain, self.version if version is None else version)


def load_config(source):
    if isinstance(source, Config):
        return source
    if isinstance(source, dict):
        return Config(source)
    try:
        raw = yaml.safe_load(Path(source).read_text())
    except (OSError, yaml.YAMLError):
        raise ConfigurationError("Unable to read YAML configuration") from None
    return Config(raw)
