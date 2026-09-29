import re
from ..exceptions import ConfigurationError


class PoolMasker:
    def __init__(self, values):
        self.values = tuple(values)
        self.capacity = len(self.values)

    def candidate(self, original, index):
        return self.values[index % self.capacity]

    def validate(self, original):
        pass


class NameMasker(PoolMasker):
    pass


class FullNameMasker(PoolMasker):
    def __init__(self, first_names, last_names):
        super().__init__([f"{f} {l}" for f in first_names for l in last_names])


class EmailMasker:
    capacity = 1_000_000_000

    def __init__(self, lookups, domain="example.com", invalid_values="error"):
        if (not isinstance(domain, str) or len(domain) > 253 or "." not in domain
                or not all(re.fullmatch(r"[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?", label) for label in domain.split("."))
                or not re.fullmatch(r"[A-Za-z]{2,}", domain.split(".")[-1])):
            raise ConfigurationError("Invalid synthetic email domain")
        self.first, self.last = lookups["first_names"], lookups["last_names"]
        self.domain, self.invalid = domain.lower(), invalid_values

    def validate(self, original):
        if self.invalid == "error" and not re.fullmatch(r"[^\s@]+@[^\s@]+\.[^\s@]+", original):
            raise ConfigurationError("Invalid email source value")

    def candidate(self, original, index):
        index %= self.capacity
        first = re.sub(r"[^a-z]", "", self.first[index % len(self.first)].lower()) or "guest"
        last = re.sub(r"[^a-z]", "", self.last[(index // len(self.first)) % len(self.last)].lower()) or "visitor"
        return f"{first}.{last}{index}@{self.domain}"


class PhoneMasker:
    # NANP fictitious 202-555-0100 through 0199; explicitly finite pool.
    capacity = 100

    def __init__(self, invalid_values="error"):
        self.invalid = invalid_values

    @staticmethod
    def valid(original):
        digits = re.sub(r"[^0-9]", "", original)
        return bool(re.fullmatch(r"[+() .0-9-]+", original)) and (len(digits) == 10 or (len(digits) == 11 and digits[0] == "1"))

    def validate(self, original):
        if self.invalid == "error" and not self.valid(original):
            raise ConfigurationError("Phone format is unsupported; configure replacement or a custom locale pool")

    def candidate(self, original, index):
        digits = f"20255501{index % self.capacity:02d}"
        if not self.valid(original):
            return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
        if len(re.sub(r"[^0-9]", "", original)) == 11:
            digits = "1" + digits
        values = iter(digits)
        return "".join(next(values) if c in "0123456789" else c for c in original)


class AddressMasker:
    def __init__(self, streets):
        self.streets = streets
        self.capacity = len(streets) * 9999

    def validate(self, original):
        pass

    def candidate(self, original, index):
        index %= self.capacity
        return f"{index // len(self.streets) + 1} {self.streets[index % len(self.streets)]}"


class MembershipMasker:
    capacity = 1_000_000_000

    def __init__(self, invalid_values="error"):
        self.invalid = invalid_values

    def validate(self, original):
        if self.invalid == "error" and not re.fullmatch(r"[0-9]{9}[A-Za-z]", original):
            raise ConfigurationError("Invalid membership source value")

    def candidate(self, original, index):
        suffix = original[-1] if re.fullmatch(r"[0-9]{9}[A-Za-z]", original) else "A"
        return f"{index % self.capacity:09d}{suffix}"


def make_masker(spec, lookup):
    kind = spec["mask_type"]
    invalid = spec.get("invalid_values", "error")
    factories = {
        "first_name": lambda: NameMasker(lookup["first_names"]),
        "last_name": lambda: NameMasker(lookup["last_names"]),
        "full_name": lambda: FullNameMasker(lookup["first_names"], lookup["last_names"]),
        "email": lambda: EmailMasker(lookup, spec.get("email_domain", "example.com"), invalid),
        "phone": lambda: PhoneMasker(invalid),
        "address": lambda: AddressMasker(lookup["streets"]),
        "membership": lambda: MembershipMasker(invalid),
    }
    return factories[kind]()
