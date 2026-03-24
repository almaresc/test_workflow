from __future__ import annotations

import sys
import shutil
from datetime import datetime
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


def is_truthy(value: str | None) -> bool:
    if value is None:
        return False
    return value.strip().lower() in {"1", "t", "true", "y", "yes"}


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


def hdfs_copy_from_local(spark: SparkSession, local_path: str, remote_path: str) -> None:
    fs, jpath = get_hdfs_fs(spark)
    remote = jpath(remote_path)
    parent = remote.getParent()
    if parent is not None:
        fs.mkdirs(parent)
    fs.copyFromLocalFile(False, True, jpath(local_path), remote)


def normalise_input_file(source_path: Path, temp_path: Path, has_header: bool) -> None:
    header_rows = 1 if has_header else 0
    with source_path.open("r", encoding="ISO-8859-15") as source_handle, temp_path.open(
        "w", encoding="ISO-8859-15", newline="\n"
    ) as temp_handle:
        for line_number, raw_line in enumerate(source_handle):
            if line_number < header_rows:
                continue

            row = raw_line.rstrip("\r\n")
            if row == "":
                continue

            temp_handle.write(row)
            temp_handle.write("\n")


def archive_original_file(source_path: Path, archive_dir: Path) -> Path:
    archive_dir.mkdir(parents=True, exist_ok=True)
    archive_name = f"{source_path.name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    archive_path = archive_dir / archive_name
    shutil.move(str(source_path), str(archive_path))
    return archive_path


def run(context: dict[str, str | None], spark: SparkSession | None = None) -> dict[str, str]:
    owns_spark = spark is None
    if spark is None:
        spark = build_spark("S_INGESTION_IMPORT_FILE_STA", context)

    source_dir = Path(require(context, "W_PATH_IN"))
    archive_dir = Path(require(context, "W_PATH_ARCHIN"))
    file_name = require(context, "W_FILENAME")
    business_name = require(context, "W_BUSINESS_NAME")
    hdfs_base_path = require(context, "W_HDFS_ETL_PATH").rstrip("/")

    source_path = source_dir / file_name
    temp_path = source_dir / f"{file_name}_tmp"
    target_path = f"{hdfs_base_path}/in/{business_name.lower()}/{business_name}"

    if not source_path.is_file():
        raise FileNotFoundError(f"Source file not found: {source_path}")

    try:
        normalise_input_file(source_path, temp_path, is_truthy(context.get("W_FLAG_HEADER")))
        hdfs_copy_from_local(spark, str(temp_path), target_path)
        archived_path = archive_original_file(source_path, archive_dir)
        if temp_path.exists():
            temp_path.unlink()
        return {
            "source_file": str(source_path),
            "archived_file": str(archived_path),
            "hdfs_target": target_path,
        }
    finally:
        if owns_spark:
            spark.stop()


def main(argv: list[str] | None = None) -> int:
    arguments = sys.argv[1:] if argv is None else argv
    context = parse_context(arguments)
    result = run(context)
    print(f"Uploaded to HDFS: {result['hdfs_target']}")
    print(f"Archived source file: {result['archived_file']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())