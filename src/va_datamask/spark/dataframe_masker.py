import copy
import json
import uuid
from ..config.yaml_loader import load_config
from ..engine import batches
from ..exceptions import (ConfigurationError, MissingMappingError, AmbiguousMappingError,
                          IntegrityError, RecoveryRequiredError)
from ..storage.delta_repository import identifier
from ..storage.mapping_store import MAPPING_FIELDS
from .udf_registry import fingerprint_udf, decrypt_udf

TAG = "va_datamask"
MANIFEST_PROPERTY = "va_datamask.manifest"


def quoted(column):
    return "`" + column.replace("`", "``") + "`"


def selection(engine, df, table, columns=None):
    from pyspark.sql.types import StringType
    definitions = engine.config.columns(table)
    chosen = list(definitions) if columns is None else list(columns)
    if not chosen or len(set(chosen)) != len(chosen) or len(set(df.columns)) != len(df.columns):
        raise ConfigurationError("Columns must be non-empty and unique")
    if any(c not in definitions or c not in df.columns for c in chosen):
        raise ConfigurationError("A selected column is missing or not configured")
    if any(not isinstance(df.schema[c].dataType, StringType) for c in chosen):
        raise ConfigurationError("Configured masking columns must be strings")
    scopes = [engine.config.scope(definitions[c]["domain"]) for c in chosen]
    return chosen, scopes


def transform(engine, df, *, table, action, reason, columns=None, on_missing="error", context=None):
    from pyspark.sql import functions as F
    try:
        chosen, scopes = selection(engine, df, table, columns)
        if on_missing not in {"error", "keep_masked"}:
            raise ConfigurationError("Unsupported missing-mapping policy")
    except ConfigurationError as error:
        if context is None:
            engine._rejected(action, reason, error, table)
        raise
    context = context or engine._begin(action, scopes, reason=reason, table=table, columns=chosen)
    try:
        # Prevent accidental repeated transformation when package metadata is present.
        for column, scope in zip(chosen, scopes):
            tag = df.schema[column].metadata.get(TAG)
            if tag:
                if action == "MASK" or tag.get("state") != "masked" or tag.get("scope") != scope.key():
                    raise ConfigurationError("Column state or mapping scope is incompatible")
        if action == "MASK":
            for column, scope in zip(chosen, scopes):
                distinct = df.select(F.col(quoted(column)).alias("value")).where(F.col("value").isNotNull()).distinct()
                for values in batches((r.value for r in distinct.toLocalIterator()), engine.batch_size):
                    engine._resolve_many(values, scope, context["executor"])
        out = df
        unresolved = {}
        for column, scope in zip(chosen, scopes):
            mappings = engine.store.dataframe(df.sparkSession, scope)
            for key in ("fingerprint", "masked_value"):
                if mappings.groupBy(key).count().where(F.col("count") > 1).limit(1).count():
                    raise AmbiguousMappingError("Vault contains duplicate mapping keys")
            if action == "MASK":
                engine._key_check(scope)
            left, right = out.alias("d"), mappings.alias("m")
            source = F.col("d." + quoted(column))
            match = fingerprint_udf(engine.keys, scope)(source) == F.col("m.fingerprint") if action == "MASK" else source == F.col("m.masked_value")
            joined = left.join(right, match, "left")
            missing = source.isNotNull() & F.col("m.fingerprint").isNull()
            if on_missing == "error":
                if joined.where(missing).limit(1).count():
                    raise MissingMappingError("At least one non-null value has no mapping in the selected scope")
                unresolved[column] = 0
            else:
                unresolved[column] = joined.where(missing).count()
            if action == "MASK":
                replacement = F.col("m.masked_value")
            else:
                record = F.struct(*[F.col("m." + f).alias(f) for f in MAPPING_FIELDS])
                replacement = decrypt_udf(engine.keys, scope)(record)
                if on_missing == "keep_masked":
                    replacement = F.when(missing, source).otherwise(replacement)
            metadata = dict(out.schema[column].metadata)
            metadata[TAG] = {"scope": scope.key(), "state": "masked" if action == "MASK" else ("partial" if unresolved[column] else "restored")}
            out = joined.select(*[replacement.alias(c, metadata=metadata) if c == column else F.col("d." + quoted(c)) for c in out.columns])
        engine.audit.emit(**context, status="PLAN_READY", unresolved=unresolved,
                          execution="lazy; downstream actions are not tracked by this API")
        return out
    except Exception as error:
        engine._failed(context, error)
        raise


def dry_run(engine, df, table):
    from pyspark.sql import functions as F
    from pyspark.sql.types import BooleanType
    chosen, scopes = selection(engine, df, table)
    context = engine._begin("MASK", scopes, reason="Dry run", table=table, columns=chosen)
    try:
        result = {"records_processed": df.count(), "columns": {}, "errors": 0, "estimates": False}
        for column, scope in zip(chosen, scopes):
            engine._key_check(scope)
            masker = engine.maskers[scope.domain]

            def valid(value):
                try:
                    masker.validate(value)
                    return True
                except ConfigurationError:
                    return False

            source = df.select(F.col(quoted(column)).alias("value")).where(F.col("value").isNotNull())
            distinct = source.distinct()
            existing = engine.store.dataframe(df.sparkSession, scope).select("fingerprint").distinct()
            missing = distinct.withColumn("fingerprint", fingerprint_udf(engine.keys, scope)(F.col("value"))).join(existing, "fingerprint", "left_anti").count()
            errors = source.where(~F.udf(valid, BooleanType())("value")).count()
            result["columns"][column] = {"non_null_rows": source.count(), "distinct_values": distinct.count(),
                                         "missing_mappings": missing, "invalid_rows": errors,
                                         "candidate_pool_capacity": masker.capacity,
                                         "capacity_note": "Per format/suffix for phones/memberships; existing collisions may reduce availability"}
            result["errors"] += errors
        engine.audit.emit(**context, status="SUCCEEDED", records=result["records_processed"], errors=result["errors"])
        return result
    except Exception as error:
        engine._failed(context, error)
        raise


def _snapshot(spark, table):
    from delta.tables import DeltaTable
    version = DeltaTable.forName(spark, table).history(1).select("version").first().version
    return spark.read.option("versionAsOf", version).table(table), version


def _manifest(df, engine, table, request_id):
    return {"format_version": 1, "config": engine.config.raw, "config_digest": engine.config.digest,
            "logical_table": table, "request_id": request_id,
            "columns": {f.name: f.metadata.get(TAG) for f in df.schema.fields if TAG in f.metadata}}


def _set_manifest(spark, table, manifest):
    # SQL literal quoting: escape backslashes before quotes for Spark's parser.
    value = json.dumps(manifest, ensure_ascii=True, sort_keys=True).replace("\\", "\\\\").replace("'", "\\'")
    spark.sql(f"ALTER TABLE {identifier(table)} SET TBLPROPERTIES ('{MANIFEST_PROPERTY}' = '{value}')")


def _read_manifest(spark, table):
    rows = spark.sql(f"SHOW TBLPROPERTIES {identifier(table)} ('{MANIFEST_PROPERTY}')").collect()
    try:
        manifest = json.loads(rows[0].value)
        config = load_config(manifest["config"])
        if manifest["format_version"] != 1 or config.digest != manifest["config_digest"]:
            raise ValueError()
        return manifest, config
    except Exception:
        raise IntegrityError("A valid dataset manifest is required for table restoration") from None


def _publish(engine, frame, target, manifest, context):
    spark = frame.sparkSession
    identifier(target)
    if spark.catalog.tableExists(target):
        raise ConfigurationError("Destination already exists; overwriting is disabled")
    stage = target.rsplit(".", 1)
    stage[-1] = "_va_stage_" + uuid.uuid4().hex
    stage = ".".join(stage)
    published = False
    try:
        frame.write.format("delta").mode("errorifexists").saveAsTable(stage)
        rows = spark.table(stage).count()
        _set_manifest(spark, stage, manifest)
        engine.audit.emit(**context, status="PREPARED", records=rows, staging_table=stage)
        spark.sql(f"ALTER TABLE {stage} RENAME TO {target}")
        published = True
        engine.audit.emit(**context, status="SUCCEEDED", records=rows)
        return {"request_id": context["request_id"], "target_table": target, "records": rows}
    except Exception as error:
        if published:
            # No cross-table atomic transaction exists between audit and destination.
            # Keep data in an operator-only schema until the success receipt is returned.
            raise RecoveryRequiredError(f"Request {context['request_id']}: destination committed but completion audit failed; retain restricted access and reconcile the request") from None
        try:
            spark.sql(f"DROP TABLE IF EXISTS {stage}")
        except Exception:
            raise RecoveryRequiredError(f"Request {context['request_id']}: staging table {stage} requires operator cleanup") from None
        raise error


def persist_masked(engine, source, target, table):
    spark = engine.spark
    if spark is None:
        raise ConfigurationError("A Spark session is required")
    identifier(source), identifier(target)
    if source == target:
        raise ConfigurationError("Source and destination must differ")
    df, version = _snapshot(spark, source)
    chosen, scopes = selection(engine, df, table)
    context = engine._begin("MASK", scopes, reason="Persist masked dataset", table=table, columns=chosen, destination=target)
    try:
        result = transform(engine, df, table=table, action="MASK", reason=context["reason"], context=context)
        manifest = _manifest(result, engine, table, context["request_id"])
        manifest.update(source_table=source, source_version=version)
        return _publish(engine, result, target, manifest, context)
    except Exception as error:
        engine._failed(context, error)
        raise


def persist_unmasked(engine, source, target, table, columns, reason, write_mode):
    spark = engine.spark
    if spark is None:
        raise ConfigurationError("A Spark session is required")
    identifier(source), identifier(target)
    if source == target or write_mode != "errorifexists":
        raise ConfigurationError("Restoration requires a new destination; overwrite and append are disabled")
    manifest, config = _read_manifest(spark, source)
    if manifest["logical_table"] != table or config.namespace != engine.config.namespace:
        raise IntegrityError("Dataset manifest does not match the requested table/namespace")
    historical = copy.copy(engine)
    historical.config = config
    df, version = _snapshot(spark, source)
    if manifest["columns"] != {f.name: f.metadata.get(TAG) for f in df.schema.fields if TAG in f.metadata}:
        raise IntegrityError("Schema provenance does not match the dataset manifest")
    chosen, scopes = selection(historical, df, table, columns)
    context = historical._begin("UNMASK", scopes, reason=reason, table=table, columns=chosen, destination=target)
    try:
        restored = transform(historical, df, table=table, action="UNMASK", reason=reason, columns=chosen, context=context)
        output = _manifest(restored, historical, table, context["request_id"])
        output.update(source_table=source, source_version=version)
        return _publish(historical, restored, target, output, context)
    except Exception as error:
        historical._failed(context, error)
        raise
