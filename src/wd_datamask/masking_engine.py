from .engine import Engine
from .exceptions import ConfigurationError


class MaskingEngine(Engine):
    def mask_value(self, value, *, domain, mapping_version=None):
        scope = self.config.scope(domain, mapping_version)
        context = self._begin("MASK", [scope], reason="Data masking")
        try:
            self._value(value)
            result = None if value is None else self._resolve_many([value], scope, context["executor"])[value]
            self.audit.emit(**context, status="SUCCEEDED", records=1)
            return result
        except Exception as error:
            self._failed(context, error)
            raise

    def mask_first_name(self, value, *, domain, mapping_version=None):
        if self.config.domains.get(domain, {}).get("mask_type") != "first_name":
            raise ConfigurationError("mask_first_name requires a first_name domain")
        return self.mask_value(value, domain=domain, mapping_version=mapping_version)

    def mask_last_name(self, value, *, domain, mapping_version=None):
        if self.config.domains.get(domain, {}).get("mask_type") != "last_name":
            raise ConfigurationError("mask_last_name requires a last_name domain")
        return self.mask_value(value, domain=domain, mapping_version=mapping_version)

    def mask_dataframe(self, df, *, table):
        from .spark.dataframe_masker import transform
        return transform(self, df, table=table, action="MASK", reason="Data masking")

    def mask_table(self, *, source_table, target_table, table):
        from .spark.dataframe_masker import persist_masked
        return persist_masked(self, source_table, target_table, table)

    def dry_run(self, df, *, table):
        from .spark.dataframe_masker import dry_run
        return dry_run(self, df, table)
