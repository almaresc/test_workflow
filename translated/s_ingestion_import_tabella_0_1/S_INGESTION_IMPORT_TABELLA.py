"""
Job: S_INGESTION_IMPORT_TABELLA
Purpose: Ingest a database table from an Oracle source into HDFS using Sqoop (via PySpark orchestration).
Author: Pietrini, Luca
Translated from: to_translate/s_ingestion_import_tabella_0_1/S_INGESTION_IMPORT_TABELLA.java

Flow:
  1. Load context parameters from a CSV configuration file.
  2. Build a WHERE clause for delta or full imports.
  3. Establish a SparkSession with Hive support and Kerberos authentication.
  4. Delete the HDFS landing directory for the current business entity.
  5. Create an empty marker file in the landing directory.
  6. Read the Oracle table via JDBC and write to HDFS as pipe-delimited text.
"""

import argparse
import logging
import re
import sys
from typing import Optional

from pyspark.sql import SparkSession

logging.basicConfig(
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger("S_INGESTION_IMPORT_TABELLA")

# ---------------------------------------------------------------------------
# Context – key/value pairs loaded from the CSV configuration file
# ---------------------------------------------------------------------------

SENSITIVE_KEYS = {
    "DB_POSTGRES_Password",
    "EDH_CLUSTER_HIVE_hiveSSLTrustStorePassword",
    "EDH_CLUSTER_HIVE_Password",
}

CONTEXT_CSV_PATH = "/talend/properties/DB_CONTEXT/Context/DB_CONTEXT.csv"


def load_context(csv_path: str) -> dict:
    """Load context properties from a semicolon-separated CSV file.

    Lines starting with '#' or '!' are treated as comments and ignored.
    Each non-empty, non-comment line is expected to match the pattern:
        key;value
    """
    context: dict = {}
    try:
        with open(csv_path, encoding="iso-8859-15") as fh:
            for raw_line in fh:
                line = raw_line.rstrip("\n")
                if not line:
                    continue
                stripped = line.strip()
                if stripped.startswith("#") or stripped.startswith("!"):
                    continue
                match = re.match(r"^([^;]*);(.*)$", line)
                if match:
                    key = match.group(1)
                    value = match.group(2)
                    context[key] = value
                    if key in SENSITIVE_KEYS:
                        logger.debug("Loaded context key: %s = ****", key)
                    else:
                        logger.debug("Loaded context key: %s = %s", key, value)
                else:
                    logger.warning(
                        "Implicit_Context_Regex - Line doesn't match: %s", line
                    )
    except FileNotFoundError:
        logger.warning(
            "Implicit_Context_Regex - Context file not found: %s", csv_path
        )
    logger.info(
        "Implicit_Context_Context - Loaded contexts count: %d.", len(context)
    )
    return context


def override_context(context: dict, overrides: dict) -> dict:
    """Apply command-line context_param overrides to the loaded context."""
    context.update(overrides)
    return context


# ---------------------------------------------------------------------------
# WHERE clause builder (tJava_1)
# ---------------------------------------------------------------------------


def build_where_clause(context: dict) -> str:
    """Build the WHERE clause used by the Sqoop import.

    For a delta import with a non-empty W_LAST_PARTITION, a date filter is
    appended: <partition_field> > to_date('<last_partition_date>', 'yyyy-mm-dd')
    """
    where_clause = "1 = 1"
    last_partition: Optional[str] = context.get("W_LAST_PARTITION")
    import_type: str = (context.get("W_IMPORT_TYPE") or "").lower()
    partition_field: str = context.get("W_PARTITION_FIELD") or ""

    if last_partition and import_type == "delta":
        if len(last_partition) < 10:
            raise ValueError(
                f"W_LAST_PARTITION '{last_partition}' is too short; "
                "expected at least 10 characters (yyyy-mm-dd)."
            )
        date_str = last_partition[:10]
        where_clause += (
            f" AND {partition_field} > to_date('{date_str}', 'yyyy-mm-dd')"
        )

    logger.info("tJava_1 - where_clause: %s", where_clause)
    return where_clause


# ---------------------------------------------------------------------------
# Spark / Hive / HDFS helpers
# ---------------------------------------------------------------------------


def build_spark_session(context: dict) -> SparkSession:
    """Create a SparkSession configured for Kerberos, Hive and HDFS HA.

    Mirrors the tHiveConnection_1 and tHDFSConnection_1 Talend components.
    """
    hive_principal = context.get("EDH_CLUSTER_HIVE_HivePrincipal", "")
    hive_keytab = context.get("EDH_CLUSTER_HIVE_HiveKeyTab", "")
    hive_keytab_principal = context.get("EDH_CLUSTER_HIVE_HiveKeyTabPrincipal", "")
    hive_server = context.get("EDH_CLUSTER_HIVE_Server", "")
    hive_port = context.get("EDH_CLUSTER_HIVE_Port", "10000")
    hive_database = context.get("EDH_CLUSTER_HIVE_Database", "default")
    hive_ssl_store = context.get("EDH_CLUSTER_HIVE_hiveSSLTrustStorePath", "")
    hive_ssl_pwd = context.get("EDH_CLUSTER_HIVE_hiveSSLTrustStorePassword", "")
    hive_additional_jdbc = context.get(
        "EDH_CLUSTER_HIVE_hiveAdditionalJDBCParameters", ""
    )

    namenode_uri = context.get("EDH_CLUSTER_NameNodeUri", "")
    namenode_prin = context.get("EDH_CLUSTER_NameNodePrin", "")
    principal = context.get("EDH_CLUSTER_Principal", "")
    keytab = context.get("EDH_CLUSTER_KeyTab", "")
    dfs_nameservices = context.get("EDH_CLUSTER_dfs_nameservices", "")
    dfs_failover = context.get(
        "EDH_CLUSTER_dfs_client_failover_proxy_provider_edhcluster", ""
    )
    ha_zk = context.get("EDH_CLUSTER_ha_zookeeper_quorum", "")
    dfs_ha_namenodes = context.get("EDH_CLUSTER_dfs_ha_namenodes_edhcluster", "")
    nn_rpc_209 = context.get(
        "EDH_CLUSTER_dfs_namenode_rpc_address_edhcluster_namenode209", ""
    )
    nn_rpc_264 = context.get(
        "EDH_CLUSTER_dfs_namenode_rpc_address_edhcluster_namenode264", ""
    )

    dynamic_part = context.get("EDH_CLUSTER_HIVE_dynamicPart", "nonstrict")
    exec_engine = context.get("EDH_CLUSTER_HIVE_executionEngine", "mr")
    dyn_part_max = context.get("EDH_CLUSTER_HIVE_dynamicPartMax", "1000")
    dyn_part_max_node = context.get("EDH_CLUSTER_HIVE_dynamicPartMaxPerNode", "100")

    # Build HiveServer2 JDBC URL
    hive_url = (
        f"jdbc:hive2://{hive_server}:{hive_port}/{hive_database}"
        f";principal={hive_principal}"
        ";ssl=true"
        f";sslTrustStore={hive_ssl_store}"
        f";trustStorePassword={hive_ssl_pwd}"
    )
    if hive_additional_jdbc.strip():
        if not hive_additional_jdbc.startswith(";"):
            hive_additional_jdbc = ";" + hive_additional_jdbc
        hive_url += hive_additional_jdbc

    logger.debug("tHiveConnection_1 - Connection URL: %s", hive_url)

    builder = (
        SparkSession.builder.appName("S_INGESTION_IMPORT_TABELLA")
        .enableHiveSupport()
        # HDFS HA configuration
        .config("spark.hadoop.fs.default.name", namenode_uri)
        .config("spark.hadoop.dfs.nameservices", dfs_nameservices)
        .config(
            "spark.hadoop.dfs.client.failover.proxy.provider.edhcluster",
            dfs_failover,
        )
        .config("spark.hadoop.ha.zookeeper.quorum", ha_zk)
        .config("spark.hadoop.dfs.ha.namenodes.edhcluster", dfs_ha_namenodes)
        .config(
            "spark.hadoop.dfs.namenode.rpc-address.edhcluster.namenode209",
            nn_rpc_209,
        )
        .config(
            "spark.hadoop.dfs.namenode.rpc-address.edhcluster.namenode264",
            nn_rpc_264,
        )
        .config("spark.hadoop.dfs.namenode.kerberos.principal", namenode_prin)
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
        # Kerberos
        .config("spark.kerberos.principal", hive_keytab_principal or principal)
        .config("spark.kerberos.keytab", hive_keytab or keytab)
        # Hive settings
        .config("hive.exec.dynamic.partition.mode", dynamic_part)
        .config("hive.execution.engine", exec_engine)
        .config("hive.exec.max.dynamic.partitions", dyn_part_max)
        .config("hive.exec.max.dynamic.partitions.pernode", dyn_part_max_node)
    )

    spark = builder.getOrCreate()

    # Apply Hadoop configuration on existing SparkContext
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.default.name", namenode_uri)
    hadoop_conf.set("dfs.nameservices", dfs_nameservices)
    hadoop_conf.set(
        "dfs.client.failover.proxy.provider.edhcluster", dfs_failover
    )
    hadoop_conf.set("ha.zookeeper.quorum", ha_zk)
    hadoop_conf.set("dfs.ha.namenodes.edhcluster", dfs_ha_namenodes)
    hadoop_conf.set(
        "dfs.namenode.rpc-address.edhcluster.namenode209", nn_rpc_209
    )
    hadoop_conf.set(
        "dfs.namenode.rpc-address.edhcluster.namenode264", nn_rpc_264
    )
    hadoop_conf.set("dfs.namenode.kerberos.principal", namenode_prin)
    hadoop_conf.set("dfs.client.use.datanode.hostname", "true")

    logger.debug("tHiveConnection_1 - Done.")
    logger.debug("tHDFSConnection_1 - Done.")

    return spark


def _get_filesystem(spark: SparkSession, namenode_uri: str):
    """Return the Hadoop FileSystem object for the given URI."""
    jvm = spark._jvm
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    uri = jvm.java.net.URI(namenode_uri)
    return jvm.org.apache.hadoop.fs.FileSystem.get(uri, conf)


def hdfs_delete(spark: SparkSession, hdfs_path: str, context: dict) -> None:
    """Delete an HDFS directory recursively if it exists (tHDFSDelete_1).

    Mirrors the behaviour of the Talend tHDFSDelete component.
    """
    namenode_uri = context.get("EDH_CLUSTER_NameNodeUri", "")
    fs = _get_filesystem(spark, namenode_uri)
    jvm = spark._jvm
    path = jvm.org.apache.hadoop.fs.Path(hdfs_path)

    if fs.exists(path):
        if fs.delete(path, True):
            logger.info(
                "tHDFSDelete_1 - directory or file : %s is deleted.", hdfs_path
            )
        else:
            logger.info(
                "tHDFSDelete_1 - fail to delete directory or file : %s.",
                hdfs_path,
            )
    else:
        logger.warning(
            "tHDFSDelete_1 - directory or file : %s does not exist.", hdfs_path
        )


def hdfs_write_empty_file(spark: SparkSession, hdfs_path: str, context: dict) -> None:
    """Create an empty marker file in HDFS (tHDFSOutput_1 / tFixedFlowInput_3).

    The Java job uses a fixed-flow input with 0 records, so the resulting
    output file is always empty.  We replicate that by creating a zero-byte
    file at the given path.
    """
    namenode_uri = context.get("EDH_CLUSTER_NameNodeUri", "")
    fs = _get_filesystem(spark, namenode_uri)
    jvm = spark._jvm
    path = jvm.org.apache.hadoop.fs.Path(hdfs_path)

    # Overwrite the file (create or replace)
    out = fs.create(path, True)
    out.close()
    logger.debug("tHDFSOutput_1 - Written records count: 0 .")
    logger.debug("tHDFSOutput_1 - Done.")


# ---------------------------------------------------------------------------
# Oracle JDBC → HDFS import  (tSqoopImport_3)
# ---------------------------------------------------------------------------


def sqoop_import(
    spark: SparkSession,
    context: dict,
    where_clause: str,
) -> None:
    """Import a table from Oracle into HDFS as pipe-delimited text files.

    This function replicates the behaviour of the Talend tSqoopImport_3
    component.  Instead of invoking Sqoop directly, it uses PySpark's JDBC
    reader to read from Oracle and writes the result to HDFS as a CSV-style
    text file with '|' as the field delimiter and '\\N' as the null string.

    Key settings (mirrors Sqoop component configuration):
      - File format  : TextFile
      - Delimiter    : |
      - Null string  : \\N  (Sqoop convention)
      - Num mappers  : 1  (spark parallelism set accordingly)
      - Delete target: True (handled by hdfs_delete before this call)

    Note: The WHERE clause and JDBC URL are constructed from context
    parameters that originate from a trusted configuration file, matching
    the security model of the original Talend job.
    """
    jdbc_url = (
        "jdbc:oracle:thin:@"
        + context.get("DB_CONNECTION_Server", "")
        + ":"
        + context.get("DB_CONNECTION_Port", "")
        + "/"
        + context.get("DB_CONNECTION_Database", "")
    )
    db_user = context.get("DB_CONNECTION_Login", "")
    db_password = context.get("DB_CONNECTION_Password", "")
    source_table = (
        context.get("W_T_SOURCE_DATABASE", "")
        + "."
        + context.get("W_T_SOURCE_TABLE", "")
    )
    target_dir = (
        context.get("W_HDFS_ETL_PATH", "")
        + "/in/"
        + (context.get("W_BUSINESS_NAME") or "").lower()
    )

    logger.info("tSqoopImport_3 - Start to work.")
    logger.debug("tSqoopImport_3 - JDBC URL: %s", jdbc_url)
    logger.debug("tSqoopImport_3 - Source table: %s", source_table)
    logger.debug("tSqoopImport_3 - Target dir: %s", target_dir)
    logger.debug("tSqoopImport_3 - WHERE clause: %s", where_clause)

    # Use a single partition to mimic Sqoop with numMappers=1.
    # The dbtable sub-query applies the WHERE clause server-side so only
    # matching rows are transferred over the network.
    df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option(
            "dbtable",
            f"(SELECT * FROM {source_table} WHERE {where_clause}) t",
        )
        .option("user", db_user)
        .option("password", db_password)
        .option("driver", "oracle.jdbc.driver.OracleDriver")
        .option("numPartitions", 1)
        .load()
    )

    # Use the DataFrame CSV writer for better performance.  The null value
    # is set to '\N' (Sqoop convention) and the field separator to '|'.
    df.write.mode("overwrite").option("delimiter", "|").option(
        "nullValue", r"\N"
    ).option("emptyValue", "").csv(target_dir)

    logger.info("tSqoopImport_3 - Done.")


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="S_INGESTION_IMPORT_TABELLA PySpark job"
    )
    parser.add_argument(
        "--context_param",
        action="append",
        metavar="KEY=VALUE",
        default=[],
        help="Override a context parameter (can be specified multiple times).",
    )
    parser.add_argument(
        "--context",
        default="LOCAL",
        help="Context name (default: LOCAL).",
    )
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)

    # Parse --context_param KEY=VALUE overrides
    param_overrides: dict = {}
    for item in args.context_param:
        if "=" in item:
            k, v = item.split("=", 1)
            param_overrides[k] = v

    logger.info("TalendJob: 'S_INGESTION_IMPORT_TABELLA' - Start.")

    # Step 1 – Load context from configuration CSV
    context = load_context(CONTEXT_CSV_PATH)
    context = override_context(context, param_overrides)

    # Step 2 – Build WHERE clause (tJava_1)
    where_clause = build_where_clause(context)

    # Step 3 – Create SparkSession (tHiveConnection_1 + tHDFSConnection_1)
    spark = build_spark_session(context)

    try:
        landing_dir = (
            context.get("W_HDFS_ETL_PATH", "")
            + "/in/"
            + (context.get("W_BUSINESS_NAME") or "").lower()
        )
        empty_file_path = landing_dir + "/empty"

        # Step 4 – Delete existing HDFS landing directory (tHDFSDelete_1)
        hdfs_delete(spark, landing_dir, context)

        # Step 5 – Write empty marker file (tFixedFlowInput_3 + tHDFSOutput_1)
        hdfs_write_empty_file(spark, empty_file_path, context)

        # Step 6 – Import table from Oracle to HDFS (tSqoopImport_3)
        sqoop_import(spark, context, where_clause)

    finally:
        spark.stop()

    logger.info("TalendJob: 'S_INGESTION_IMPORT_TABELLA' - Done.")


if __name__ == "__main__":
    main(sys.argv[1:])
