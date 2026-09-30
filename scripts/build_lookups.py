"""Regenerate src/wd_datamask/data/lookups.json from public-domain US Census name data.

The `names` package (MIT) redistributes the 1990 US Census first/last name frequency
files. Run: python -m pip install names && python scripts/build_lookups.py
The output is committed, so the package itself does not depend on `names`.
"""
import json
import re
from pathlib import Path

TARGET = Path(__file__).resolve().parents[1] / "src" / "wd_datamask" / "data" / "lookups.json"
FIRST_NAME_LIMIT = 6000
LAST_NAME_LIMIT = 20000
STREET_SURNAME_LIMIT = 1200
STREET_BASES = [
    "Oak", "Maple", "Cedar", "Willow", "Birch", "Pine", "Elm", "Aspen", "Spruce", "Chestnut",
    "Walnut", "Hickory", "Magnolia", "Sycamore", "Poplar", "Juniper", "Laurel", "Linden", "Cypress",
    "Hazel", "Holly", "Ivy", "Rose", "Lilac", "Jasmine", "Meadow", "Prairie", "Valley", "Ridge",
    "Summit", "Highland", "Hillcrest", "Lakeview", "Riverside", "Bayside", "Seaside", "Harbor",
    "Park", "Garden", "Orchard", "Vineyard", "Mill", "Bridge", "Canal", "Spring", "Brook", "Creek",
    "River", "Lake", "Forest", "Woodland", "Glen", "Dale", "Heather", "Clover", "Fern", "Sage",
    "Main", "Center", "Market", "Church", "School", "Union", "Liberty", "Franklin", "Washington",
    "Jefferson", "Lincoln", "Madison", "Monroe", "Jackson", "Adams", "Grant", "Hamilton", "Harrison",
    "Sunset", "Sunrise", "Morning", "Evening", "Northern", "Southern", "Eastern", "Western",
    "Colonial", "Heritage", "Pioneer", "Frontier", "Prospect", "Commerce", "Industrial", "Station",
]
SUFFIXES = ["Street", "Avenue", "Road", "Lane", "Drive", "Court", "Boulevard", "Way", "Place", "Terrace"]


def census(name):
    import names
    path = Path(names.__file__).with_name(name)
    return [line.split()[0] for line in path.read_text().splitlines() if line.strip()]


def usable(value):
    return re.fullmatch(r"[A-Z]{3,}", value) is not None


def ranked(*files, limit):
    seen, result = set(), []
    # Interleave male/female first names so both are represented in the plain tier.
    columns = [[v.title() for v in census(f) if usable(v)] for f in files]
    for i in range(max(map(len, columns))):
        for column in columns:
            if i < len(column) and column[i] not in seen:
                seen.add(column[i])
                result.append(column[i])
    return result[:limit]


def main():
    first = ranked("dist.male.first", "dist.female.first", limit=FIRST_NAME_LIMIT)
    last = ranked("dist.all.last", limit=LAST_NAME_LIMIT)
    bases = STREET_BASES + [n for n in last[:STREET_SURNAME_LIMIT] if n not in STREET_BASES]
    streets = [f"{base} {suffix}" for base in bases for suffix in SUFFIXES]
    payload = {"first_names": first, "last_names": last, "streets": streets}
    for key, values in payload.items():
        assert len(values) == len(set(values)), key
    body = ",\n".join(f'"{key}": {json.dumps(values, ensure_ascii=False, separators=(",", ":"))}' for key, values in payload.items())
    TARGET.write_text("{\n" + body + "\n}\n")
    print({k: len(v) for k, v in payload.items()}, TARGET)


if __name__ == "__main__":
    main()
