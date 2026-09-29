"""Deterministic, human-readable candidate generators.

Every masker exposes the same contract:

* ``validate(original)`` raises ``ConfigurationError`` for unsupported source values
  when the ``invalid_values`` policy is ``error``.
* ``candidate(original, seed, attempt)`` returns the substitute to try for probe number
  ``attempt`` (0, 1, 2, ...). ``seed`` is the integer value of the keyed fingerprint,
  so the first candidate is stable for a given original, scope and fingerprint key.
* ``capacity`` is the total number of distinct substitutes the masker can produce and
  ``max_attempts`` bounds the probing loop; the allocator raises
  ``MappingCapacityError`` once it is exceeded.

Pool maskers are tiered so that plain lookup entries are used first (``Michael``), then
hyphenated compounds of two entries (``Anna-Marie``, ``Smith-Parker``) and finally three
entries. Compound tiers keep values readable while extending capacity far beyond the
pool size, which is what makes tens of millions of distinct originals representable.
"""
import re
from ..exceptions import ConfigurationError

# attempt ranges: [0, 2) plain, [2, 24) pairs, [24, 64) triples
TIER_STARTS = (0, 2, 24)
MAX_ATTEMPTS = 64


def tier_for(attempt):
    return 2 if attempt >= TIER_STARTS[2] else 1 if attempt >= TIER_STARTS[1] else 0


class PoolMasker:
    max_attempts = MAX_ATTEMPTS

    def __init__(self, values, separator="-", compound=True):
        self.values = tuple(values)
        if not self.values:
            raise ConfigurationError("Lookup pools must not be empty")
        n = len(self.values)
        self.separator = separator
        self.compound = compound and n > 1
        self.tier_capacity = (n, n * (n - 1), n * (n - 1) * (n - 1)) if self.compound else (n, n, n)
        self.capacity = sum(self.tier_capacity) if self.compound else n

    def validate(self, original):
        pass

    def pick(self, index, tier):
        n, values = len(self.values), self.values
        index %= self.tier_capacity[tier]
        if tier == 0 or not self.compound:
            return values[index]
        first, rest = divmod(index, n - 1)
        if tier == 1:
            second = rest + (rest >= first)
            return values[first] + self.separator + values[second]
        first, rest = divmod(index, (n - 1) * (n - 1))
        second, third = divmod(rest, n - 1)
        second += second >= first
        third += third >= second
        return self.separator.join((values[first], values[second], values[third]))

    def candidate(self, original, seed, attempt):
        return self.pick(seed + attempt, tier_for(attempt))


class NameMasker(PoolMasker):
    pass


class FullNameMasker:
    """Independent full-name substitutes: a first-name pick and a last-name pick."""
    max_attempts = MAX_ATTEMPTS

    def __init__(self, first_names, last_names):
        self.first, self.last = NameMasker(first_names), NameMasker(last_names)
        self.capacity = self.first.capacity * self.last.capacity

    def validate(self, original):
        pass

    def candidate(self, original, seed, attempt):
        tier = tier_for(attempt)
        return f"{self.first.pick(seed + attempt, tier)} {self.last.pick((seed >> 64) + attempt, tier)}"


class EmailMasker:
    """``first.last@domain`` from the name pools, then ``first.last<n>@domain``."""
    max_attempts = MAX_ATTEMPTS
    NUMBER_SUFFIXES = 1000

    def __init__(self, lookups, domain="example.com", invalid_values="error"):
        if (not isinstance(domain, str) or len(domain) > 253 or "." not in domain
                or not all(re.fullmatch(r"[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?", label) for label in domain.split("."))
                or not re.fullmatch(r"[A-Za-z]{2,}", domain.split(".")[-1])):
            raise ConfigurationError("Invalid synthetic email domain")
        self.first = tuple(re.sub(r"[^a-z]", "", v.lower()) or "guest" for v in lookups["first_names"])
        self.last = tuple(re.sub(r"[^a-z]", "", v.lower()) or "visitor" for v in lookups["last_names"])
        self.domain, self.invalid = domain.lower(), invalid_values
        self.plain = len(self.first) * len(self.last)
        self.capacity = self.plain * self.NUMBER_SUFFIXES

    @staticmethod
    def valid(original):
        return re.fullmatch(r"[^\s@]+@[^\s@]+\.[^\s@]+", original) is not None

    def validate(self, original):
        if self.invalid == "error" and not self.valid(original):
            raise ConfigurationError("Invalid email source value")

    def candidate(self, original, seed, attempt):
        index = (seed + attempt) % self.capacity
        number, base = divmod(index, self.plain)
        first, last = self.first[base % len(self.first)], self.last[base // len(self.first)]
        suffix = "" if number == 0 else str(number)
        return f"{first}.{last}{suffix}@{self.domain}"


class PhoneMasker:
    """NANP-shaped substitutes that preserve the original separators.

    ``pool="synthetic"`` (default) draws area codes and exchanges from [2-9][0-9]{2}
    (excluding N11 service codes and the 555 exchange) for about 5.7 billion numbers.
    These are well-formed but not guaranteed unassigned; never dial masked numbers.
    ``pool="fictitious"`` uses the reserved 555-0100..0199 range only (100 numbers
    per formatting pattern), which is safe for demos but not for large datasets.
    """
    max_attempts = MAX_ATTEMPTS
    POOLS = {"synthetic", "fictitious"}

    def __init__(self, invalid_values="error", pool="synthetic"):
        if pool not in self.POOLS:
            raise ConfigurationError("Unsupported phone pool")
        self.invalid, self.pool = invalid_values, pool
        codes = [f"{a}{b}{c}" for a in range(2, 10) for b in range(10) for c in range(10) if not (b == 1 and c == 1)]
        self.areas = tuple(codes)  # 792 area codes without N11
        self.exchanges = tuple(code for code in codes if code != "555")  # 791 exchanges
        self.capacity = 100 if pool == "fictitious" else len(self.areas) * len(self.exchanges) * 10000

    @staticmethod
    def valid(original):
        digits = re.sub(r"[^0-9]", "", original)
        return bool(re.fullmatch(r"[+() .0-9-]+", original)) and (len(digits) == 10 or (len(digits) == 11 and digits[0] == "1"))

    def validate(self, original):
        if self.invalid == "error" and not self.valid(original):
            raise ConfigurationError("Phone format is unsupported; configure replacement or a custom locale pool")

    def _digits(self, index):
        if self.pool == "fictitious":
            return f"20255501{index % 100:02d}"
        index %= self.capacity
        rest, subscriber = divmod(index, 10000)
        area, exchange = divmod(rest, len(self.exchanges))
        return f"{self.areas[area]}{self.exchanges[exchange]}{subscriber:04d}"

    def candidate(self, original, seed, attempt):
        digits = self._digits(seed + attempt)
        if not self.valid(original):
            return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
        if len(re.sub(r"[^0-9]", "", original)) == 11:
            digits = "1" + digits
        values = iter(digits)
        return "".join(next(values) if c in "0123456789" else c for c in original)


class AddressMasker:
    """``<number> <street>`` using the street pool; numbers 1..9999."""
    max_attempts = MAX_ATTEMPTS

    def __init__(self, streets):
        self.streets = tuple(streets)
        if not self.streets:
            raise ConfigurationError("Lookup pools must not be empty")
        self.capacity = len(self.streets) * 9999

    def validate(self, original):
        pass

    def candidate(self, original, seed, attempt):
        index = (seed + attempt) % self.capacity
        return f"{index // len(self.streets) + 1} {self.streets[index % len(self.streets)]}"


class MembershipMasker:
    """Nine digits plus a preserved final letter."""
    max_attempts = MAX_ATTEMPTS
    capacity = 1_000_000_000

    def __init__(self, invalid_values="error"):
        self.invalid = invalid_values

    @staticmethod
    def valid(original):
        return re.fullmatch(r"[0-9]{9}[A-Za-z]", original) is not None

    def validate(self, original):
        if self.invalid == "error" and not self.valid(original):
            raise ConfigurationError("Invalid membership source value")

    def candidate(self, original, seed, attempt):
        suffix = original[-1] if self.valid(original) else "A"
        return f"{(seed + attempt) % self.capacity:09d}{suffix}"


def make_masker(spec, lookup):
    kind = spec["mask_type"]
    invalid = spec.get("invalid_values", "error")
    factories = {
        "first_name": lambda: NameMasker(lookup["first_names"]),
        "last_name": lambda: NameMasker(lookup["last_names"]),
        "full_name": lambda: FullNameMasker(lookup["first_names"], lookup["last_names"]),
        "email": lambda: EmailMasker(lookup, spec.get("email_domain", "example.com"), invalid),
        "phone": lambda: PhoneMasker(invalid, spec.get("phone_pool", "synthetic")),
        "address": lambda: AddressMasker(lookup["streets"]),
        "membership": lambda: MembershipMasker(invalid),
    }
    return factories[kind]()
