"""Errors deliberately omit source and restored values."""


class DataMaskError(Exception):
    pass


class ConfigurationError(DataMaskError):
    pass


class MappingCapacityError(DataMaskError):
    pass


class MissingMappingError(DataMaskError):
    pass


class AmbiguousMappingError(DataMaskError):
    pass


class IntegrityError(DataMaskError):
    pass


class AllocationBusyError(DataMaskError):
    pass


class AuditError(DataMaskError):
    pass


class RecoveryRequiredError(DataMaskError):
    pass
