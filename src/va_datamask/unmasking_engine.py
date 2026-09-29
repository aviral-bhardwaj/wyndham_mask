from .engine import Engine
from .exceptions import MissingMappingError, ConfigurationError


class UnmaskingEngine(Engine):
    def unmask_value(self, value, *, domain, reason, mapping_version=None):
        try:
            scope = self.config.scope(domain, mapping_version)
        except ConfigurationError as error:
            self._rejected("UNMASK", reason, error)
            raise
        context = self._begin("UNMASK", [scope], reason=reason)
        try:
            self._value(value)
            result = None
            if value is not None:
                mapping = self.store.find(scope, "masked_value", [value]).get(value)
                if mapping is None:
                    raise MissingMappingError("No mapping in the requested scope/version")
                result = self.keys.decrypt(scope, mapping)
            self.audit.emit(**context, status="SUCCEEDED", records=1)
            return result
        except Exception as error:
            self._failed(context, error)
            raise

    def unmask_dataframe(self, df, *, table, columns=None, reason, on_missing="error"):
        from .spark.dataframe_masker import transform
        return transform(self, df, table=table, columns=columns, reason=reason,
                         action="UNMASK", on_missing=on_missing)

    def unmask_table(self, *, source_table, target_table, table, columns=None, reason,
                     write_mode="errorifexists"):
        from .spark.dataframe_masker import persist_unmasked
        return persist_unmasked(self, source_table, target_table, table, columns, reason, write_mode)
