from __future__ import annotations

import fnmatch
import importlib.util
import sys
from pathlib import Path

from pyspark.sql import SparkSession


NULL_SENTINELS = {"__NULL__", "<null>", "None"}


def parse_context(argv: list[str]) -> dict[str, str | None]:
    context: dict[str, str | None] = {}
    index = 0
    while index < len(argv):
        token = argv[index]

        if token in {"--context_param", "--context_type"}:
            index += 1
            if index < len(argv) and "=" in argv[index]:
                key, value = argv[index].split("=", 1)
                if token == "--context_param":
                    context[key] = None if value in NULL_SENTINELS else value
            index += 1
            continue

        if token.startswith("--"):
            body = token[2:]
            if "=" in body:
                key, value = body.split("=", 1)
                context[key] = None if value in NULL_SENTINELS else value
                index += 1
                continue

            key = body
            if index + 1 < len(argv) and not argv[index + 1].startswith("--"):
                index += 1
                value = argv[index]
                context[key] = None if value in NULL_SENTINELS else value
            else:
                context[key] = "true"

        index += 1

    return context


def require(context: dict[str, str | None], key: str) -> str:
    value = context.get(key)
    if value is None or value == "":
        raise ValueError(f"Missing required context value: {key}")
    return value


def configure_hadoop(spark: SparkSession, context: dict[str, str | None]) -> None:
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    mappings = {
        "EDH_CLUSTER_NameNodeUri": "fs.defaultFS",
        "EDH_CLUSTER_NameNodeUri": "fs.default.name",
        "EDH_CLUSTER_NameNodePrin": "dfs.namenode.kerberos.principal",
        "EDH_CLUSTER_dfs_nameservices": "dfs.nameservices",
        "EDH_CLUSTER_dfs_client_failover_proxy_provider_edhcluster": "dfs.client.failover.proxy.provider.edhcluster",
        "EDH_CLUSTER_ha_zookeeper_quorum": "ha.zookeeper.quorum",
        "EDH_CLUSTER_dfs_ha_namenodes_edhcluster": "dfs.ha.namenodes.edhcluster",
        "EDH_CLUSTER_dfs_namenode_rpc_address_edhcluster_namenode209": "dfs.namenode.rpc-address.edhcluster.namenode209",
        "EDH_CLUSTER_dfs_namenode_rpc_address_edhcluster_namenode264": "dfs.namenode.rpc-address.edhcluster.namenode264",
    }
    for context_key, hadoop_key in mappings.items():
        value = context.get(context_key)
        if value:
            conf.set(hadoop_key, value)

    conf.set("dfs.client.use.datanode.hostname", "true")


def build_spark(app_name: str, context: dict[str, str | None]) -> SparkSession:
    spark = SparkSession.builder.appName(app_name).enableHiveSupport().getOrCreate()
    configure_hadoop(spark, context)
    return spark


def get_hdfs_fs(spark: SparkSession):
    jvm = spark._jvm
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    return jvm.org.apache.hadoop.fs.FileSystem.get(conf), jvm.org.apache.hadoop.fs.Path


def hdfs_delete_if_exists(spark: SparkSession, path_value: str) -> None:
    fs, jpath = get_hdfs_fs(spark)
    path = jpath(path_value)
    if fs.exists(path):
        fs.delete(path, True)


def hdfs_create_empty_file(spark: SparkSession, path_value: str) -> None:
    fs, jpath = get_hdfs_fs(spark)
    path = jpath(path_value)
    parent = path.getParent()
    if parent is not None:
        fs.mkdirs(parent)
    stream = fs.create(path, True)
    stream.close()


def load_job_module(subfolder: str, filename: str):
    root = Path(__file__).resolve().parents[1]
    module_path = root / subfolder / f"{filename}.py"
    spec = importlib.util.spec_from_file_location(filename, module_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Unable to load module from {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def list_matching_files(context: dict[str, str | None]) -> list[Path]:
    source_dir = Path(require(context, "W_PATH_IN"))
    file_mask = require(context, "W_F_FILEMASK")
    return sorted(
        path
        for path in source_dir.iterdir()
        if path.is_file() and fnmatch.fnmatch(path.name, file_mask)
    )


def run(context: dict[str, str | None], spark: SparkSession | None = None) -> dict[str, str | int]:
    owns_spark = spark is None
    if spark is None:
        spark = build_spark("S_INGESTION_IMPORT_FILE_1", context)

    business_name = require(context, "W_BUSINESS_NAME")
    hdfs_base_path = require(context, "W_HDFS_ETL_PATH").rstrip("/")
    landing_dir = f"{hdfs_base_path}/in/{business_name.lower()}"
    placeholder = f"{landing_dir}/{business_name.lower()}"
    staging_module = load_job_module(
        "s_ingestion_import_file_sta_0_1", "S_INGESTION_IMPORT_FILE_STA"
    )

    try:
        hdfs_delete_if_exists(spark, landing_dir)
        hdfs_create_empty_file(spark, placeholder)

        processed_files = 0
        last_query = ""

        for current_file in list_matching_files(context):
            child_context = dict(context)
            child_context["W_FILENAME"] = current_file.name
            staging_module.run(child_context, spark=spark)

            last_query = (
                f"INSERT INTO TABLE {require(context, 'W_EDH_DB_H')}.H_FILE_INGESTION "
                f"SELECT '{current_file.name}' nome_file, {require(context, 'W_PARTITION_FIELD')} DATA_PRODUZIONE"
            )
            spark.sql(last_query)
            spark.sql(f"REFRESH TABLE {require(context, 'W_EDH_DB_H')}.H_FILE_INGESTION")
            processed_files += 1

        return {
            "processed_files": processed_files,
            "landing_dir": landing_dir,
            "last_query": last_query,
        }
    finally:
        if owns_spark:
            spark.stop()


def main(argv: list[str] | None = None) -> int:
    arguments = sys.argv[1:] if argv is None else argv
    context = parse_context(arguments)
    result = run(context)
    print(f"Processed files: {result['processed_files']}")
    print(f"Landing dir: {result['landing_dir']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())