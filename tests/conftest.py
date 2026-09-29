import os
from pathlib import Path
import pytest
from wd_datamask import (Config, KeyRing, MaskingEngine, UnmaskingEngine, MappingStore,
                        AuditManager, PermissionManager, LocalIdentity)


@pytest.fixture
def raw_config():
    types = {"first_name": "first_name", "last_name": "last_name", "full_name": "full_name",
             "email": "email", "phone": "phone", "address": "address", "membership": "membership"}
    return {"namespace": "test", "mapping_version": "v1", "defaults": {"invalid_values": "replace"},
            "tables": {"customer": {"columns": {k: {"mask_type": v, "domain": k} for k, v in types.items()}}}}


@pytest.fixture
def setup(tmp_path, raw_config):
    config = Config(raw_config)
    keys = KeyRing({"e1": os.urandom(32)}, {"f1": os.urandom(32)}, "e1", "f1")
    identity = LocalIdentity()
    grant = {"principals": [identity.current().requester], "actions": ["MASK", "UNMASK", "ROTATE"],
             "namespace": "test", "domains": list(config.domains), "tables": ["*"], "columns": ["*"],
             "allow_values": True, "destinations": []}
    store = MappingStore(tmp_path / "vault.sqlite")
    audit = AuditManager(tmp_path / "audit.sqlite")
    dependencies = dict(config=config, store=store, keys=keys, audit=audit,
                        permissions=PermissionManager(identity, [grant]))
    yield MaskingEngine(**dependencies), UnmaskingEngine(**dependencies), dependencies, grant
    store.close()
    audit.close()


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    from pyspark.sql import SparkSession
    builder = SparkSession.builder.master("local[2]").appName("wd-datamask-tests").config(
        "spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "2").config(
        "spark.sql.warehouse.dir", str(tmp_path_factory.mktemp("warehouse")))
    jars = os.environ.get("WD_DELTA_JARS")
    if jars:
        builder = builder.config("spark.jars", jars).config(
            "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension").config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    session = builder.getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
