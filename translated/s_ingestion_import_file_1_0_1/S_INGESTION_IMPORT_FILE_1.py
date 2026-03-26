"""
Job: S_INGESTION_IMPORT_FILE_1
Purpose: File ingestion orchestrator – deletes the staging HDFS landing area,
         recreates an empty sentinel file, lists matching input files and, for
         each one, delegates the actual staging load to the child job
         S_INGESTION_IMPORT_FILE_STA before logging the ingestion event in the
         Hive table H_FILE_INGESTION and invalidating the corresponding Impala
         metadata.
Author:  Pietrini, Luca  (original Talend job)
Version: 7.3.1.20241003_1446-patch (translated to PySpark)
"""

import argparse
import fnmatch
import logging
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Optional

from pyspark.sql import SparkSession

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
log = logging.getLogger("S_INGESTION_IMPORT_FILE_1")


# ---------------------------------------------------------------------------
# Context helpers
# ---------------------------------------------------------------------------

class Context:
    """Holds all context variables, mirroring the Talend ContextProperties."""

    # Workflow parameters
    W_BUSINESS_GROUP: Optional[str] = None
    W_BUSINESS_NAME: Optional[str] = None
    W_DATA_TYPE: Optional[str] = None
    W_DEFAULT_PARTITION: Optional[str] = None
    W_EDH_DB_ARCHIN: Optional[str] = None
    W_EDH_DB_H: Optional[str] = None
    W_EDH_DB_IN: Optional[str] = None
    W_EDH_TABLE_ARCHIN: Optional[str] = None
    W_EDH_TABLE_H: Optional[str] = None
    W_EDH_TABLE_IN: Optional[str] = None
    W_EXTRACTION_FIELDS: Optional[str] = None
    W_F_FILEMASK: Optional[str] = None
    W_FILENAME: Optional[str] = None
    W_FLAG_ABILITATA: Optional[str] = None
    W_FLAG_HEADER: Optional[str] = None
    W_HDFS_ETL_PATH: Optional[str] = None
    W_IMPORT_TYPE: Optional[str] = None
    W_LAST_PARTITION: Optional[str] = None
    W_MAPPER: Optional[str] = None
    W_PARTITION_FIELD: Optional[str] = None
    W_PATH_ARCHIN: Optional[str] = None
    W_PATH_IN: Optional[str] = None
    W_SOURCE_NAME: Optional[str] = None
    W_T_SOURCE_DATABASE: Optional[str] = None
    W_T_SOURCE_TABLE: Optional[str] = None

    # Postgres / log DB parameters
    DB_POSTGRES_CUSTOM_LOG_TABLE: Optional[str] = None
    DB_POSTGRES_Database: Optional[str] = None
    DB_POSTGRES_FILE_METADATA_TABLE: Optional[str] = None
    DB_POSTGRES_LOG_TABLE: Optional[str] = None
    DB_POSTGRES_Login: Optional[str] = None
    DB_POSTGRES_LOGTABLE: Optional[str] = None
    DB_POSTGRES_LOGVIEW: Optional[str] = None
    DB_POSTGRES_METADATA_FACT_RECUPERO_PARTIZIONI: Optional[str] = None
    DB_POSTGRES_METADATA_FACT_TABLE: Optional[str] = None
    DB_POSTGRES_Password: Optional[str] = None
    DB_POSTGRES_Port: Optional[str] = None
    DB_POSTGRES_Schema: Optional[str] = None
    DB_POSTGRES_Server: Optional[str] = None
    DB_POSTGRES_SQOOP_METADATA_TABLE: Optional[str] = None
    DB_POSTGRES_STATTABLE: Optional[str] = None
    DB_POSTGRES_configuration_metadata: Optional[str] = None

    # EDH Cluster / HDFS parameters
    EDH_CLUSTER_dfs_client_failover_proxy_provider_edhcluster: Optional[str] = None
    EDH_CLUSTER_dfs_ha_namenodes_edhcluster: Optional[str] = None
    EDH_CLUSTER_dfs_namenode_rpc_address_edhcluster_namenode209: Optional[str] = None
    EDH_CLUSTER_dfs_namenode_rpc_address_edhcluster_namenode264: Optional[str] = None
    EDH_CLUSTER_dfs_nameservices: Optional[str] = None
    EDH_CLUSTER_ha_zookeeper_quorum: Optional[str] = None
    EDH_CLUSTER_JobHistory: Optional[str] = None
    EDH_CLUSTER_JobHistroyPrin: Optional[str] = None
    EDH_CLUSTER_JTOrRMPrin: Optional[str] = None
    EDH_CLUSTER_KeyTab: Optional[str] = None
    EDH_CLUSTER_NameNodePrin: Optional[str] = None
    EDH_CLUSTER_NameNodeUri: Optional[str] = None
    EDH_CLUSTER_Principal: Optional[str] = None
    EDH_CLUSTER_ResourceManager: Optional[str] = None
    EDH_CLUSTER_ResourceManagerScheduler: Optional[str] = None
    EDH_CLUSTER_StagingDirectory: Optional[str] = None
    EDH_CLUSTER_username: Optional[str] = None
    EDH_CLUSTER_yarn_resourcemanager_address_rm1: Optional[str] = None
    EDH_CLUSTER_yarn_resourcemanager_address_rm2: Optional[str] = None
    EDH_CLUSTER_yarn_resourcemanager_ha_enabled: Optional[str] = None
    EDH_CLUSTER_yarn_resourcemanager_ha_rm_ids: Optional[str] = None
    EDH_CLUSTER_yarn_resourcemanager_scheduler_address_rm1: Optional[str] = None
    EDH_CLUSTER_yarn_resourcemanager_scheduler_address_rm2: Optional[str] = None
    EDH_CLUSTER_HDFS_HdfsFileSeparator: Optional[str] = None
    EDH_CLUSTER_HDFS_HdfsRowSeparator: Optional[str] = None

    # Hive parameters
    EDH_CLUSTER_HIVE_Database: Optional[str] = None
    EDH_CLUSTER_HIVE_dynamicPart: Optional[str] = None
    EDH_CLUSTER_HIVE_dynamicPartMax: Optional[str] = None
    EDH_CLUSTER_HIVE_dynamicPartMaxPerNode: Optional[str] = None
    EDH_CLUSTER_HIVE_executionEngine: Optional[str] = None
    EDH_CLUSTER_HIVE_hiveAdditionalJDBCParameters: Optional[str] = None
    EDH_CLUSTER_HIVE_HiveKeyTab: Optional[str] = None
    EDH_CLUSTER_HIVE_HiveKeyTabPrincipal: Optional[str] = None
    EDH_CLUSTER_HIVE_HivePrincipal: Optional[str] = None
    EDH_CLUSTER_HIVE_hiveSSLTrustStorePassword: Optional[str] = None
    EDH_CLUSTER_HIVE_hiveSSLTrustStorePath: Optional[str] = None
    EDH_CLUSTER_HIVE_Login: Optional[str] = None
    EDH_CLUSTER_HIVE_Password: Optional[str] = None
    EDH_CLUSTER_HIVE_Port: Optional[str] = None
    EDH_CLUSTER_HIVE_Server: Optional[str] = None

    # Impala parameters
    EDH_CLUSTER_IMPALA_Database: Optional[str] = None
    EDH_CLUSTER_IMPALA_ImpalaPrincipal: Optional[str] = None
    EDH_CLUSTER_IMPALA_Login: Optional[str] = None
    EDH_CLUSTER_IMPALA_Port: Optional[str] = None
    EDH_CLUSTER_IMPALA_Server: Optional[str] = None

    def set(self, key: str, value: str) -> None:
        if hasattr(self, key):
            setattr(self, key, value)
        else:
            log.warning("Context key not found: %s", key)

    def get(self, key: str) -> Optional[str]:
        return getattr(self, key, None)


def load_context_from_file(context_file: str, context: Context) -> None:
    """
    Load context properties from a key=value file, mirroring the behaviour of
    the Talend tFileInputRegex / tContextLoad component pair
    (Implicit_Context_Regex → Implicit_Context_Context).
    Lines starting with '#' are treated as comments and ignored.
    """
    pattern = re.compile(r"^\s*([^#=\s][^=]*?)\s*=\s*(.*?)\s*$")
    try:
        with open(context_file, "r", encoding="utf-8") as fh:
            for line in fh:
                match = pattern.match(line)
                if match:
                    key, value = match.group(1), match.group(2)
                    context.set(key, value)
        log.info("Context loaded from: %s", context_file)
    except FileNotFoundError:
        log.warning("Context file not found: %s – using defaults / CLI params", context_file)


def apply_cli_overrides(context: Context, overrides: dict) -> None:
    """Apply individual --context_param KEY=VALUE overrides (highest priority)."""
    for key, value in overrides.items():
        context.set(key, value)


# ---------------------------------------------------------------------------
# HDFS helpers (via Spark / hadoop fs commands)
# ---------------------------------------------------------------------------

def hdfs_delete(spark: SparkSession, path: str) -> None:
    """
    Recursively delete an HDFS path if it exists.
    Mirrors tHDFSDelete_1: path = W_HDFS_ETL_PATH + "/in/" + W_BUSINESS_NAME.lower()
    """
    sc = spark.sparkContext
    hadoop_conf = sc._jsc.hadoopConfiguration()
    fs_class = sc._jvm.org.apache.hadoop.fs.FileSystem
    hadoop_path = sc._jvm.org.apache.hadoop.fs.Path(path)
    fs = fs_class.get(hadoop_conf)

    if fs.exists(hadoop_path):
        if fs.delete(hadoop_path, True):
            log.info("tHDFSDelete_1 - directory or file: %s is deleted.", path)
        else:
            log.info("tHDFSDelete_1 - failed to delete directory or file: %s.", path)
    else:
        log.warning("tHDFSDelete_1 - directory or file: %s does not exist.", path)


def hdfs_write_empty_file(spark: SparkSession, path: str) -> None:
    """
    Create (or overwrite) an empty file at the given HDFS path.
    Mirrors tFixedFlowInput_1 (0 rows) → tHDFSOutput_1 (TEXT, OVERWRITE).
    The Talend component writes a single empty string followed by a newline for
    each input row; since the fixed-flow input produces 0 rows the resulting
    file is truly empty.
    """
    sc = spark.sparkContext
    hadoop_conf = sc._jsc.hadoopConfiguration()
    fs_class = sc._jvm.org.apache.hadoop.fs.FileSystem
    hadoop_path = sc._jvm.org.apache.hadoop.fs.Path(path)
    fs = fs_class.get(hadoop_conf)

    out_stream = fs.create(hadoop_path, True)  # overwrite=True
    out_stream.close()
    log.debug("tHDFSOutput_1 - Written records count: 0.")
    log.debug("tHDFSOutput_1 - Done.")


def hdfs_list_files(spark: SparkSession, directory: str, filemask: str):
    """
    List files in *directory* whose names match *filemask* (glob pattern).
    Mirrors tFileList_1 with FILES=[{FILEMASK=context.W_F_FILEMASK}].
    Returns a list of (filename, filepath) tuples sorted by name.
    """
    sc = spark.sparkContext
    hadoop_conf = sc._jsc.hadoopConfiguration()
    fs_class = sc._jvm.org.apache.hadoop.fs.FileSystem
    hadoop_path = sc._jvm.org.apache.hadoop.fs.Path(directory)
    fs = fs_class.get(hadoop_conf)

    matched = []
    if not fs.exists(hadoop_path):
        log.warning("tFileList_1 - directory does not exist: %s", directory)
        return matched

    statuses = fs.listStatus(hadoop_path)
    for status in statuses:
        if status.isFile():
            name = status.getPath().getName()
            if fnmatch.fnmatch(name, filemask):
                full_path = status.getPath().toString()
                matched.append((name, full_path))

    matched.sort(key=lambda t: t[0])
    log.info("tFileList_1 - File or directory count: %d", len(matched))
    return matched


# ---------------------------------------------------------------------------
# Child job invocation
# ---------------------------------------------------------------------------

def run_child_job(
    context: Context,
    w_filename: str,
    child_job_script: Optional[str] = None,
) -> None:
    """
    Invoke the child job S_INGESTION_IMPORT_FILE_STA for a single file.
    Mirrors tRunJob_1 (TRANSMIT_WHOLE_CONTEXT=true, DIE_ON_CHILD_ERROR=true).

    The child job is executed as a subprocess; its path can be supplied via
    the *child_job_script* argument or derived automatically relative to this
    file.
    """
    if child_job_script is None:
        base_dir = Path(__file__).resolve().parent.parent
        child_job_script = str(
            base_dir / "s_ingestion_import_file_sta_0_1" / "S_INGESTION_IMPORT_FILE_STA.py"
        )

    cmd = [sys.executable, child_job_script]

    # Transmit the full context
    ctx_attrs = [a for a in vars(Context) if not a.startswith("_") and not callable(getattr(Context, a))]
    for attr in ctx_attrs:
        value = context.get(attr)
        if value is not None:
            cmd += ["--context_param", f"{attr}={value}"]

    # Override W_FILENAME with the current file
    cmd += ["--context_param", f"W_FILENAME={w_filename}"]

    log.info(
        "tRunJob_1 - The child job 'S_INGESTION_IMPORT_FILE_STA' starts with W_FILENAME=%s",
        w_filename,
    )
    result = subprocess.run(cmd, check=False)
    if result.returncode != 0:
        raise RuntimeError(
            f"Child job S_INGESTION_IMPORT_FILE_STA failed for file '{w_filename}' "
            f"(exit code {result.returncode})"
        )
    log.info("tRunJob_1 - The child job 'S_INGESTION_IMPORT_FILE_STA' is done.")


# ---------------------------------------------------------------------------
# Hive / Impala helpers
# ---------------------------------------------------------------------------

def build_hive_insert_query(context: Context, current_file: str) -> str:
    """
    Build the Hive INSERT query, mirroring tJava_1.
    INSERT INTO TABLE {W_EDH_DB_H}.H_FILE_INGESTION
      SELECT '{current_file}' nome_file, {W_PARTITION_FIELD} DATA_PRODUZIONE

    Single quotes inside *current_file* are escaped to prevent SQL injection.
    """
    safe_file = current_file.replace("'", "\\'")
    query = (
        f"INSERT INTO TABLE {context.W_EDH_DB_H}.H_FILE_INGESTION "
        f"SELECT '{safe_file}' nome_file, "
        f"{context.W_PARTITION_FIELD} DATA_PRODUZIONE"
    )
    log.info("tJava_1 - hive_query_file: %s", query)
    return query


def execute_hive_query(spark: SparkSession, query: str) -> None:
    """
    Execute a Hive statement via SparkSQL, mirroring tHiveRow_1.
    SparkSession with Hive support provides direct access to the Hive metastore.
    """
    log.info("tHiveRow_1 - executing query: %s", query)
    spark.sql(query)
    log.debug("tHiveRow_1 - Done.")


def invalidate_impala_metadata(spark: SparkSession, context: Context) -> None:
    """
    Invalidate Impala metadata for H_FILE_INGESTION via a JDBC connection to
    the Impala daemon, mirroring tImpalaRow_1.

    The INVALIDATE METADATA statement is Impala-specific; it is executed
    through a raw JDBC connection to avoid routing it through Hive.
    """
    query = f"INVALIDATE METADATA {context.W_EDH_DB_H}.H_FILE_INGESTION"
    log.info("tImpalaRow_1 - executing: %s", query)

    jdbc_url = (
        f"jdbc:hive2://{context.EDH_CLUSTER_IMPALA_Server}:"
        f"{context.EDH_CLUSTER_IMPALA_Port}/{context.EDH_CLUSTER_IMPALA_Database}"
        f";principal={context.EDH_CLUSTER_IMPALA_ImpalaPrincipal};ssl=true"
    )

    sc = spark.sparkContext
    driver_class = "org.apache.hive.jdbc.HiveDriver"

    # Use the JVM JDBC layer available in Spark's JVM gateway
    jvm = sc._jvm
    jvm.Class.forName(driver_class)
    conn = jvm.java.sql.DriverManager.getConnection(
        jdbc_url,
        context.EDH_CLUSTER_IMPALA_Login or "",
        "",
    )
    try:
        stmt = conn.createStatement()
        stmt.execute(query)
        stmt.close()
        log.info("tImpalaRow_1 - Done.")
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Main orchestration
# ---------------------------------------------------------------------------

def run(context: Context, spark: SparkSession, child_job_script: Optional[str] = None) -> None:
    """
    Main job execution, following the Talend component chain:

    Implicit_Context_Regex / tContextLoad
      → tImpalaConnection_1 (connection established via Spark JDBC)
      → tHiveConnection_1   (connection established via SparkSession)
      → tHDFSConnection_1   (connection established via Spark SC)
      → tJava_3             (debug logging)
      → tHDFSDelete_1       (delete landing HDFS directory)
      → tFixedFlowInput_1 + tHDFSOutput_1  (write empty sentinel file)
      → tFileList_1         (iterate over matching input files)
          → [per file] tRunJob_1  (call child job S_INGESTION_IMPORT_FILE_STA)
          → [per file] tJava_1 + tHiveRow_1   (log file in H_FILE_INGESTION)
          → [per file] tImpalaRow_1            (INVALIDATE METADATA)
    """

    # ------------------------------------------------------------------
    # tJava_3 – debug logging
    # ------------------------------------------------------------------
    log.info(
        "tJava_3 - rimuovo da: W_HDFS_ETL_PATH = %s/in/%s",
        context.W_HDFS_ETL_PATH,
        (context.W_BUSINESS_NAME or "").lower(),
    )
    log.info(
        "tJava_3 - scrivo file vuoto su: W_HDFS_ETL_PATH = %s/in/%s/%s",
        context.W_HDFS_ETL_PATH,
        (context.W_BUSINESS_NAME or "").lower(),
        (context.W_BUSINESS_NAME or "").lower(),
    )
    log.info("tJava_3 - faccio la list file su: W_PATH_IN = %s", context.W_PATH_IN)
    log.info("tJava_3 - W_F_FILEMASK = %s", context.W_F_FILEMASK)
    log.info("tJava_3 - PARTITION FIELD = %s", context.W_PARTITION_FIELD)

    # ------------------------------------------------------------------
    # tHDFSDelete_1 – delete the HDFS landing directory
    # ------------------------------------------------------------------
    if not context.W_HDFS_ETL_PATH:
        raise ValueError("Required context variable W_HDFS_ETL_PATH is not set.")
    if not context.W_BUSINESS_NAME:
        raise ValueError("Required context variable W_BUSINESS_NAME is not set.")
    hdfs_in_dir = f"{context.W_HDFS_ETL_PATH}/in/{context.W_BUSINESS_NAME.lower()}"
    hdfs_delete(spark, hdfs_in_dir)

    # ------------------------------------------------------------------
    # tFixedFlowInput_1 + tHDFSOutput_1 – create empty sentinel file
    # ------------------------------------------------------------------
    hdfs_sentinel = (
        f"{context.W_HDFS_ETL_PATH}/in/"
        f"{context.W_BUSINESS_NAME.lower()}/"
        f"{context.W_BUSINESS_NAME.lower()}"
    )
    hdfs_write_empty_file(spark, hdfs_sentinel)

    # ------------------------------------------------------------------
    # tFileList_1 – iterate over input files
    # ------------------------------------------------------------------
    if not context.W_PATH_IN:
        raise ValueError("Required context variable W_PATH_IN is not set.")
    if not context.W_F_FILEMASK:
        raise ValueError("Required context variable W_F_FILEMASK is not set.")
    files = hdfs_list_files(spark, context.W_PATH_IN, context.W_F_FILEMASK)

    for current_file, current_filepath in files:
        log.info("tFileList_1 - Current file or directory path: %s", current_filepath)

        # tRunJob_1 – delegate staging to child job
        run_child_job(context, current_file, child_job_script=child_job_script)

        # tJava_1 – build Hive query
        hive_query = build_hive_insert_query(context, current_file)

        # tHiveRow_1 – execute Hive INSERT
        execute_hive_query(spark, hive_query)

        # tImpalaRow_1 – invalidate Impala metadata
        invalidate_impala_metadata(spark, context)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def _parse_args():
    parser = argparse.ArgumentParser(
        description="S_INGESTION_IMPORT_FILE_1 – file ingestion orchestrator"
    )
    parser.add_argument(
        "--context_file",
        default=None,
        help="Path to a key=value context properties file (Talend tContextLoad equivalent)",
    )
    parser.add_argument(
        "--context_param",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="Override a single context variable (can be specified multiple times)",
    )
    parser.add_argument(
        "--child_job_script",
        default=None,
        help="Explicit path to the S_INGESTION_IMPORT_FILE_STA.py child job script",
    )
    return parser.parse_args()


def main():
    args = _parse_args()

    context = Context()

    if args.context_file:
        load_context_from_file(args.context_file, context)

    overrides = {}
    for param in args.context_param:
        if "=" in param:
            k, v = param.split("=", 1)
            overrides[k.strip()] = v.strip()
    apply_cli_overrides(context, overrides)

    spark = (
        SparkSession.builder
        .appName("S_INGESTION_IMPORT_FILE_1")
        .config("spark.sql.catalogImplementation", "hive")
        .enableHiveSupport()
        .getOrCreate()
    )

    try:
        run(context, spark, child_job_script=args.child_job_script)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
