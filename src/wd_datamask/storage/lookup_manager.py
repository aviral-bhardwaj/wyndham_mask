import json
from importlib.resources import files
from pathlib import Path
from ..exceptions import ConfigurationError


class LookupManager:
    def __init__(self, path=None):
        source = Path(path) if path else files("wd_datamask").joinpath("data/lookups.json")
        self.data = json.loads(source.read_text())
        for name in ("first_names", "last_names", "streets"):
            values = self.data.get(name)
            if not isinstance(values, list) or not values or any(not isinstance(v, str) or not v.strip() for v in values):
                raise ConfigurationError("Lookup pools must contain non-empty strings")
            if len(values) != len(set(values)):
                raise ConfigurationError("Lookup pools must not contain duplicates")

    def __getitem__(self, name):
        return self.data[name]
