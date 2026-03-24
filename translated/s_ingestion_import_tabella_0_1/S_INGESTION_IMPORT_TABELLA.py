from __future__ import annotations

import sys

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
        "EDH_CLUSTER_JTOrRMPrin": "yarn.resourcemanager.principal",
        "EDH_CLUSTER_JobHistroyPrin": "mapreduce.jobhistory.principal",
        "EDH_CLUSTER_dfs_nameservices": "dfs.nameservices",
        "EDH_CLUSTER_dfs_client_failover_proxy_provider_edhcluster": "dfs.client.failover.proxy.provider.edhcluster",
        "EDH_CLUSTER_ha_zookeeper_quorum": "ha.zookeeper.quorum",
        "EDH_CLUSTER_dfs_ha_namenodes_edhcluster": "dfs.ha.namenodes.edhcluster",
        "EDH_CLUSTER_dfs_namenode_rpc_address_edhcluster_namenode209": "dfs.namenode.rpc-address.edhcluster.namenode209",
        "EDH_CLUSTER_dfs_namenode_rpc_address_edhcluster_namenode264": "dfs.namenode.rpc-address.edhcluster.namenode264",
        "EDH_CLUSTER_ResourceManager": "yarn.resourcemanager.address",
        "EDH_CLUSTER_ResourceManagerScheduler": "yarn.resourcemanager.scheduler.address",
        "EDH_CLUSTER_JobHistory": "mapreduce.jobhistory.address",
        "EDH_CLUSTER_StagingDirectory": "yarn.app.mapreduce.am.staging-dir",
        "EDH_CLUSTER_yarn_resourcemanager_ha_enabled": "yarn.resourcemanager.ha.enabled",
        "EDH_CLUSTER_yarn_resourcemanager_ha_rm_ids": "yarn.resourcemanager.ha.rm-ids",
        "EDH_CLUSTER_yarn_resourcemanager_address_rm1": "yarn.resourcemanager.address.rm1",
        "EDH_CLUSTER_yarn_resourcemanager_scheduler_address_rm1": "yarn.resourcemanager.scheduler.address.rm1",
        "EDH_CLUSTER_yarn_resourcemanager_address_rm2": "yarn.resourcemanager.address.rm2",
        "EDH_CLUSTER_yarn_resourcemanager_scheduler_address_rm2": "yarn.resourcemanager.scheduler.address.rm2",
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


def build_where_clause(context: dict[str, str | None]) -> str:
    where_clause = "1 = 1"
    last_partition = context.get("W_LAST_PARTITION")
    import_type = (context.get("W_IMPORT_TYPE") or "").lower()
    partition_field = require(context, "W_PARTITION_FIELD")

    if last_partition and import_type == "delta":
        where_clause += (
            f" AND {partition_field} > to_date('{last_partition[:10]}', 'yyyy-mm-dd')"
        )

    context["where_clause"] = where_clause
    return where_clause


def run(context: dict[str, str | None], spark: SparkSession | None = None) -> dict[str, str]:
    owns_spark = spark is None
    if spark is None:
        spark = build_spark("S_INGESTION_IMPORT_TABELLA", context)

    hdfs_base_path = require(context, "W_HDFS_ETL_PATH").rstrip("/")
    business_name = require(context, "W_BUSINESS_NAME")
    target_dir = f"{hdfs_base_path}/in/{business_name.lower()}"
    placeholder_file = f"{target_dir}/empty"

    where_clause = build_where_clause(context)
    table_name = f"{require(context, 'W_T_SOURCE_DATABASE')}.{require(context, 'W_T_SOURCE_TABLE')}"
    jdbc_query = f"SELECT * FROM {table_name} WHERE {where_clause}"
    jdbc_url = (
        f"jdbc:oracle:thin:@{require(context, 'DB_CONNECTION_Server')}:"
        f"{require(context, 'DB_CONNECTION_Port')}/{require(context, 'DB_CONNECTION_Database')}"
    )

    try:
        hdfs_delete_if_exists(spark, target_dir)
        hdfs_create_empty_file(spark, placeholder_file)

        dataframe = (
            spark.read.format("jdbc")
            .option("url", jdbc_url)
            .option("query", jdbc_query)
            .option("user", require(context, "DB_CONNECTION_Login"))
            .option("password", require(context, "DB_CONNECTION_Password"))
            .option("driver", "oracle.jdbc.driver.OracleDriver")
            .load()
        )

        (
            dataframe.coalesce(1)
            .write.mode("overwrite")
            .option("sep", "|")
            .option("header", "false")
            .option("nullValue", "\\N")
            .option("emptyValue", "")
            .csv(target_dir)
        )

        return {
            "where_clause": where_clause,
            "jdbc_query": jdbc_query,
            "target_dir": target_dir,
        }
    finally:
        if owns_spark:
            spark.stop()


def main(argv: list[str] | None = None) -> int:
    arguments = sys.argv[1:] if argv is None else argv
    context = parse_context(arguments)
    result = run(context)
    print(f"Where clause: {result['where_clause']}")
    print(f"Written to: {result['target_dir']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())