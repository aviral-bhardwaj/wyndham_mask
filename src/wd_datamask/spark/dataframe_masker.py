"""Distributed masking and unmasking of Spark DataFrames.

Scale design (validated locally at 60M rows, see docs/VALIDATION.md):

* Workers only ever process the *distinct* non-null values of one column at a time.
  Fingerprinting, candidate generation, validation, encryption and decryption run as
  pandas (Arrow) UDFs over that distinct set, never over every source row.
* Allocation of new mappings is a bounded probing loop of Spark joins: each round
  proposes one candidate per pending value, rejects candidates already taken in the
  vault or by another value in the same round (window ``row_number``), and appends the
  survivors. The vault receives exactly one append per column per call.
* The source DataFrame is joined once per column with a ``value -> replacement``
  lookup. Lookups below ``engine.broadcast_threshold`` distinct values are broadcast.
"""
import copy
import json
import uuid
from ..config.yaml_loader import load_config
from ..exceptions import (ConfigurationError, MissingMappingError, AmbiguousMappingError,
                          IntegrityError, RecoveryRequiredError, MappingCapacityError)
from ..security.auditing import utcnow
from ..storage.delta_repository import identifier
from ..storage.mapping_store import MAPPING_FIELDS
from .pandas_udfs import (fingerprint_pandas_udf, candidate_pandas_udf, validate_pandas_udf,
                          encrypt_pandas_udf, decrypt_pandas_udf)

TAG = "wd_datamask"
MANIFEST_PROPERTY = "wd_datamask.manifest"
NAME_TYPES = {"first_name", "last_name", "full_name"}


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


def _distinct(df, column):
    from pyspark.sql import functions as F
    return df.select(F.col(quoted(column)).alias("value")).where(F.col("value").isNotNull()).distinct()


def _persist(frame):
    frame.persist()
    return frame


def _materialize(frame):
    """Compute ``frame`` now and truncate its lineage.

    Each allocation round builds on the previous one; without truncation the logical
    plan nests every earlier round and grows exponentially. A reliable checkpoint is
    used when ``spark.sparkContext.setCheckpointDir`` was called, otherwise a local
    checkpoint (executor storage; the round is retried if an executor is lost).
    """
    try:
        if frame.sparkSession.sparkContext.getCheckpointDir() is not None:
            return frame.checkpoint(eager=True)
    except Exception:
        pass  # sparkContext unavailable on Spark Connect; fall back to local checkpoint
    return frame.localCheckpoint(eager=True)


def allocate(engine, distinct, scope, actor):
    """Allocate mappings for every value of ``distinct`` (column ``value``) missing from the vault.

    Returns the number of new mappings. Nothing is written unless every pending value
    receives a unique substitute; capacity exhaustion aborts the whole column.
    """
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window
    spark = distinct.sparkSession
    keys, masker = engine.keys, engine.maskers[scope.domain]
    fingerprint = fingerprint_pandas_udf(keys, scope)
    with engine.store.allocation():
        engine._key_check(scope)
        vault = engine.store.dataframe(spark, scope)
        pending = _materialize(distinct.withColumn("fingerprint", fingerprint("value"))
                               .join(vault.select("fingerprint"), "fingerprint", "left_anti")
                               .withColumn("attempt", F.lit(0)))
        remaining = pending.count()
        if remaining == 0:
            pending.unpersist()
            return 0
        if pending.where(~validate_pandas_udf(masker)("value")).limit(1).count():
            pending.unpersist()
            raise ConfigurationError(f"Invalid source value in domain {scope.domain}; "
                                     "set invalid_values: replace or correct the source")
        if pending.groupBy("fingerprint").count().where(F.col("count") > 1).limit(1).count():
            pending.unpersist()
            raise IntegrityError("Fingerprint collision detected")
        propose = candidate_pandas_udf(masker)
        taken = vault.select("masked_value").withColumnRenamed("masked_value", "candidate")
        accepted, cached = [], [pending]
        window = Window.partitionBy("candidate").orderBy("fingerprint")
        for attempt in range(masker.max_attempts):
            if remaining == 0:
                break
            proposals = _materialize(pending.withColumn("candidate", propose("value", "fingerprint", "attempt")))
            winners = _materialize(proposals.join(taken, "candidate", "left_anti")
                                   .where(F.col("candidate") != F.col("value"))
                                   .withColumn("rank", F.row_number().over(window))
                                   .where(F.col("rank") == 1).drop("rank", "attempt"))
            if winners.count():
                accepted.append(winners)
                taken = taken.union(winners.select("candidate"))
            pending = _materialize(proposals.join(winners.select("fingerprint"), "fingerprint", "left_anti")
                                   .withColumn("attempt", F.col("attempt") + 1).drop("candidate"))
            remaining = pending.count()
            cached.extend([proposals, winners, pending])
        try:
            if remaining:
                raise MappingCapacityError("No free substitute within the pool/probe limit; expand the lookup pool")
            batch = accepted[0]
            for extra in accepted[1:]:
                batch = batch.union(extra)
            encrypt = encrypt_pandas_udf(keys, scope)
            rows = batch.select(
                F.lit(scope.key()).alias("scope"), F.col("fingerprint"),
                F.lit(keys.fingerprint_key_id).alias("fingerprint_key_id"),
                F.col("candidate").alias("masked_value"),
                encrypt("value", "candidate", "fingerprint").alias("encrypted_original"),
                F.lit(keys.encryption_key_id).alias("encryption_key_id"),
                F.lit(utcnow()).alias("created_at"), F.lit(actor).alias("created_by"),
                F.lit(str(uuid.uuid4())).alias("batch_id"))
            engine.store.insert_dataframe(rows)
            return sum(w.count() for w in accepted)
        finally:
            for frame in cached:
                frame.unpersist()


def _lookup(engine, df, column, scope, action):
    """Build a persisted ``value -> replacement`` frame over the column's distinct values."""
    from pyspark.sql import functions as F
    spark = df.sparkSession
    distinct = _persist(_distinct(df, column))
    if action == "MASK":
        allocate(engine, distinct, scope, engine.permissions.identity().executor)
        vault = engine.store.dataframe(spark, scope).select("fingerprint", "masked_value")
        lookup = (distinct.withColumn("fingerprint", fingerprint_pandas_udf(engine.keys, scope)("value"))
                  .join(vault, "fingerprint", "left")
                  .select("value", F.col("masked_value").alias("replacement")))
    else:
        vault = engine.store.dataframe(spark, scope).alias("m")
        joined = distinct.alias("d").join(vault, F.col("d.value") == F.col("m.masked_value"), "left")
        decrypt = decrypt_pandas_udf(engine.keys, scope)
        lookup = joined.select(F.col("d.value").alias("value"),
                               decrypt(*[F.col("m." + f) for f in MAPPING_FIELDS]).alias("replacement"))
    lookup = _persist(lookup)
    size = lookup.count()
    distinct.unpersist()
    missing = lookup.where(F.col("replacement").isNull())
    return lookup, size, missing


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
        for column, scope in zip(chosen, scopes):
            tag = df.schema[column].metadata.get(TAG)
            if tag:
                if action == "MASK" or tag.get("state") != "masked" or tag.get("scope") != scope.key():
                    raise ConfigurationError("Column state or mapping scope is incompatible")
        out = df
        unresolved = {}
        checked = set()
        for column, scope in zip(chosen, scopes):
            if scope.key() not in checked:
                mappings = engine.store.dataframe(df.sparkSession, scope)
                for key in ("fingerprint", "masked_value"):
                    if mappings.groupBy(key).count().where(F.col("count") > 1).limit(1).count():
                        raise AmbiguousMappingError("Vault contains duplicate mapping keys")
                checked.add(scope.key())
            if action == "MASK":
                engine._key_check(scope)
            lookup, size, missing = _lookup(engine, df, column, scope, action)
            engine._track(lookup)
            if action == "MASK":
                if missing.limit(1).count():
                    raise IntegrityError("Allocation did not cover every distinct value")
                unresolved[column] = 0
            elif on_missing == "error":
                if missing.limit(1).count():
                    raise MissingMappingError("At least one non-null value has no mapping in the selected scope")
                unresolved[column] = 0
            else:
                unresolved[column] = df.join(missing, F.col(quoted(column)) == missing.value, "left_semi").count()
            right = F.broadcast(lookup) if size <= engine.broadcast_threshold else lookup
            source = F.col("d." + quoted(column))
            joined = out.alias("d").join(right.alias("m"), source == F.col("m.value"), "left")
            replacement = F.col("m.replacement")
            if action == "UNMASK" and on_missing == "keep_masked":
                replacement = F.coalesce(replacement, source)
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
    chosen, scopes = selection(engine, df, table)
    context = engine._begin("MASK", scopes, reason="Dry run", table=table, columns=chosen)
    try:
        result = {"records_processed": df.count(), "columns": {}, "errors": 0, "estimates": False,
                  "names_masked": 0, "emails_masked": 0, "phones_masked": 0, "addresses_masked": 0,
                  "memberships_masked": 0}
        definitions = engine.config.columns(table)
        for column, scope in zip(chosen, scopes):
            engine._key_check(scope)
            masker = engine.maskers[scope.domain]
            source = _persist(df.select(F.col(quoted(column)).alias("value")).where(F.col("value").isNotNull()))
            distinct = _persist(source.distinct())
            existing = engine.store.dataframe(df.sparkSession, scope).select("fingerprint").distinct()
            missing = (distinct.withColumn("fingerprint", fingerprint_pandas_udf(engine.keys, scope)("value"))
                       .join(existing, "fingerprint", "left_anti").count())
            errors = source.where(~validate_pandas_udf(masker)("value")).count()
            non_null = source.count()
            result["columns"][column] = {"non_null_rows": non_null, "distinct_values": distinct.count(),
                                         "missing_mappings": missing, "invalid_rows": errors,
                                         "candidate_pool_capacity": masker.capacity,
                                         "capacity_note": "Per format/suffix for phones/memberships; existing collisions may reduce availability"}
            result["errors"] += errors
            kind = definitions[column]["mask_type"]
            bucket = "names_masked" if kind in NAME_TYPES else {"email": "emails_masked", "phone": "phones_masked",
                                                                 "address": "addresses_masked", "membership": "memberships_masked"}[kind]
            result[bucket] += non_null
            source.unpersist()
            distinct.unpersist()
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
    stage[-1] = "_wd_stage_" + uuid.uuid4().hex
    stage = ".".join(stage)
    published = False
    try:
        frame.write.format("delta").mode("error").saveAsTable(stage)
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
    finally:
        engine.release()


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
    if source == target or write_mode not in ("error", "errorifexists"):
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
