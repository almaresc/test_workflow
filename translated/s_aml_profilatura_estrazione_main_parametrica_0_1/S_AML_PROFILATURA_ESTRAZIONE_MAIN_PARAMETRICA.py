"""
Job: S_AML_PROFILATURA_ESTRAZIONE_MAIN_PARAMETRICA
Purpose: Extract data for the "universo partecipazioni" profiling – reads one or
         more parametric CSV configuration files, loads their rows into the Impala
         table andc_etl.profilatura_main_tab_parametrica (INSERT OVERWRITE for the
         first file / first batch, INSERT INTO for subsequent ones), and then
         invalidates the corresponding Impala metadata.
Author:  Pietrini, Luca  (original Talend job)
Version: 0.1 (translated to PySpark)
"""

import argparse
import fnmatch
import logging
import os
import re
import sys
from pathlib import Path
from typing import Optional

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
log = logging.getLogger("S_AML_PROFILATURA_ESTRAZIONE_MAIN_PARAMETRICA")

JOB_NAME = "S_AML_PROFILATURA_ESTRAZIONE_MAIN_PARAMETRICA"

# ---------------------------------------------------------------------------
# Row schema – mirrors row1Struct in the Talend job
# ---------------------------------------------------------------------------

ROW1_SCHEMA = StructType(
    [
        StructField("data_inizio_trim", StringType(), True),
        StructField("data_fine_trim", StringType(), True),
        StructField("filtro_data", StringType(), True),
        StructField("an_fine_validita_filtro_a", StringType(), True),
        StructField("an_modulo_pervenuto_filtro_a", StringType(), True),
        StructField("data_modulo_av_filtro_a", StringType(), True),
        StructField("an_sottogruppo_sae_filtro_a", StringType(), True),
    ]
)

# Target Impala table – mirrors tableName_tImpalaOutput_1
TARGET_TABLE = "andc_etl.profilatura_main_tab_parametrica"


# ---------------------------------------------------------------------------
# Context
# ---------------------------------------------------------------------------


class Context:
    """Holds all context variables, mirroring the Talend ContextProperties."""

    # Job / workflow parameters
    W_JOB_PRINCIPALE: Optional[str] = None
    W_PROJECT_NAME: Optional[str] = None
    CONTEXT_SALE_FILE_NAME: Optional[str] = None
    W_INIZIO_DATE: Optional[str] = None
    W_FINE_DATE: Optional[str] = None
    data_riferimento: Optional[str] = None
    W_FILE_NAME_ESITI_CONTROLLO: Optional[str] = None
    W_DATA_RIFERIMENTO: Optional[str] = None
    W_FLAG_REG_AUI: Optional[str] = None
    W_CRAP: Optional[str] = None
    W_NDG: Optional[str] = None
    W_ESITI_PRECEDENTI: Optional[str] = None

    # DB_CONNECTION parameters
    DB_CONNECTION_Database: Optional[str] = None
    DB_CONNECTION_Login: Optional[str] = None
    DB_CONNECTION_Password: Optional[str] = None
    DB_CONNECTION_Port: Optional[str] = None
    DB_CONNECTION_Server: Optional[str] = None

    # Postgres parameters
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

    # Postgres POC parameters
    DB_POSTGRES_POC_Database: Optional[str] = None
    DB_POSTGRES_POC_Login: Optional[str] = None
    DB_POSTGRES_POC_Password: Optional[str] = None
    DB_POSTGRES_POC_Port: Optional[str] = None
    DB_POSTGRES_POC_Schema: Optional[str] = None
    DB_POSTGRES_POC_Server: Optional[str] = None

    # EDH Cluster parameters
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

    # Workflow-specific parameters
    W_CONTEXT_TYPE: Optional[str] = None
    W_DAVINCI_TABELLA_TARGET: Optional[str] = None
    W_DB_ANDC_DH: Optional[str] = None
    W_DB_ANDC_ETL: Optional[str] = None
    W_DB_DAVINCI_POSTGRES_CUSTOM_LOG_TABLE: Optional[str] = None
    W_DB_DAVINCI_POSTGRES_FILE_METADATA_TABLE: Optional[str] = None
    W_DB_DAVINCI_POSTGRES_STATEMENT_TABLE: Optional[str] = None
    W_FOLDER_CSV: Optional[str] = None
    W_FOLDER_CSV_EXPORT: Optional[str] = None
    W_FOLDER_IN_AUTOMAZIONI_CONTROLLI_II_LIV: Optional[str] = None
    W_FOLDER_IN_AUTOMAZIONI_CONTROLLI_II_LIV_PROFILATURA: Optional[str] = None
    W_HDFS_DAVINCI_ETL: Optional[str] = None
    W_HDFS_DH: Optional[str] = None
    W_HDFS_ETL: Optional[str] = None
    W_HIVE_DAVINCI_SANDBOX_DB: Optional[str] = None
    W_HIVE_DAVINCI_SANDBOX_TABLE: Optional[str] = None
    W_NAME_TFILE_LIST: Optional[str] = None
    W_MAIN_PARAMETRICA: Optional[str] = None
    W_ESITI_PARAMETRICA: Optional[str] = None

    def set(self, key: str, value: str) -> None:
        if hasattr(self, key):
            setattr(self, key, value)
        else:
            log.warning("Context key not found: %s", key)

    def get(self, key: str) -> Optional[str]:
        return getattr(self, key, None)


def load_context_from_file(context_file: str, context: Context) -> None:
    """
    Load context properties from a key=value file (Implicit_Context_Regex /
    tContextLoad equivalent). Lines starting with '#' are treated as comments.
    """
    pattern = re.compile(r"^\s*([^#=\s][^=]*?)\s*=\s*(.*?)\s*$")
    try:
        with open(context_file, "r", encoding="utf-8") as fh:
            for line in fh:
                match = pattern.match(line)
                if match:
                    context.set(match.group(1), match.group(2))
        log.info("Context loaded from: %s", context_file)
    except FileNotFoundError:
        log.warning(
            "Context file not found: %s – using defaults / CLI params", context_file
        )


def apply_cli_overrides(context: Context, overrides: dict) -> None:
    """Apply individual --context_param KEY=VALUE overrides (highest priority)."""
    for key, value in overrides.items():
        context.set(key, value)


# ---------------------------------------------------------------------------
# Kerberos setup  (tJava_2 – KerberosSetup)
# ---------------------------------------------------------------------------


def kerberos_setup(spark: SparkSession, context: Context) -> None:
    """
    Perform Kerberos authentication using the cluster keytab, mirroring tJava_2.
    Sets SSL truststore system properties and calls loginUserFromKeytab.
    """
    jvm = spark.sparkContext._jvm
    if context.EDH_CLUSTER_HIVE_hiveSSLTrustStorePath:
        jvm.System.setProperty(
            "javax.net.ssl.trustStore",
            context.EDH_CLUSTER_HIVE_hiveSSLTrustStorePath,
        )
    if context.EDH_CLUSTER_HIVE_hiveSSLTrustStorePassword:
        jvm.System.setProperty(
            "javax.net.ssl.trustStorePassword",
            context.EDH_CLUSTER_HIVE_hiveSSLTrustStorePassword,
        )

    if context.EDH_CLUSTER_Principal and context.EDH_CLUSTER_KeyTab:
        ugi = jvm.org.apache.hadoop.security.UserGroupInformation
        ugi.loginUserFromKeytab(
            context.EDH_CLUSTER_Principal, context.EDH_CLUSTER_KeyTab
        )
        log.info(
            "tJava_2 (KerberosSetup) - logged in as principal: %s",
            context.EDH_CLUSTER_Principal,
        )
    else:
        log.warning(
            "tJava_2 (KerberosSetup) - EDH_CLUSTER_Principal or EDH_CLUSTER_KeyTab "
            "not set; skipping Kerberos login."
        )


# ---------------------------------------------------------------------------
# Impala connection helpers
# ---------------------------------------------------------------------------


def _build_impala_jdbc_url(context: Context) -> str:
    """Build the Impala JDBC URL, mirroring tImpalaConnection_1."""
    return (
        f"jdbc:hive2://{context.EDH_CLUSTER_IMPALA_Server}:"
        f"{context.EDH_CLUSTER_IMPALA_Port}/{context.EDH_CLUSTER_IMPALA_Database}"
        f";principal={context.EDH_CLUSTER_IMPALA_ImpalaPrincipal};ssl=true"
    )


def _get_impala_connection(spark: SparkSession, context: Context):
    """
    Return a raw JDBC connection to Impala by reusing the JVM gateway,
    mirroring tImpalaConnection_1.
    """
    jdbc_url = _build_impala_jdbc_url(context)
    jvm = spark.sparkContext._jvm
    driver_class = "org.apache.hive.jdbc.HiveDriver"
    jvm.Class.forName(driver_class)
    conn = jvm.java.sql.DriverManager.getConnection(
        jdbc_url,
        context.EDH_CLUSTER_IMPALA_Login or "",
        "",
    )
    log.debug(
        "tImpalaConnection_1 - Connection to '%s' has succeeded.", jdbc_url
    )
    return conn


# ---------------------------------------------------------------------------
# File listing  (tFileList_1)
# ---------------------------------------------------------------------------


def list_parametric_files(context: Context):
    """
    List files in W_FOLDER_IN_AUTOMAZIONI_CONTROLLI_II_LIV_PROFILATURA whose
    names match the glob pattern "*<W_MAIN_PARAMETRICA>*.csv", mirroring
    tFileList_1. Files are sorted by name (case-sensitive). Raises
    RuntimeError if the directory contains no matching files.
    """
    directory = context.W_FOLDER_IN_AUTOMAZIONI_CONTROLLI_II_LIV_PROFILATURA
    if not directory:
        raise ValueError(
            "Context variable W_FOLDER_IN_AUTOMAZIONI_CONTROLLI_II_LIV_PROFILATURA "
            "is not set."
        )
    if not context.W_MAIN_PARAMETRICA:
        raise ValueError("Context variable W_MAIN_PARAMETRICA is not set.")

    filemask = f"*{context.W_MAIN_PARAMETRICA}*.csv"
    log.info("tFileList_1 - Starting to search for matching entries.")
    log.debug("tFileList_1 - DIRECTORY = %s", directory)
    log.debug("tFileList_1 - FILEMASK  = %s", filemask)

    try:
        entries = os.listdir(directory)
    except FileNotFoundError:
        raise RuntimeError(f"No file found in directory {directory}")

    matched = sorted(
        f
        for f in entries
        if os.path.isfile(os.path.join(directory, f))
        and fnmatch.fnmatchcase(f, filemask)
    )

    log.info("tFileList_1 - Start to list files.")
    if not matched:
        raise RuntimeError(f"No file found in directory {directory}")

    log.info("tFileList_1 - File or directory count: %d", len(matched))
    return matched


# ---------------------------------------------------------------------------
# CSV reading  (tFileInputDelimited_1)
# ---------------------------------------------------------------------------


def read_csv_file(spark: SparkSession, filepath: str):
    """
    Read the parametric CSV file, mirroring tFileInputDelimited_1:
      - field separator  : ';'
      - row separator    : '\\n'
      - header rows      : 1
      - encoding         : ISO-8859-15
      - remove empty rows: true
      - schema           : ROW1_SCHEMA (7 String columns)
    Returns a Spark DataFrame.
    """
    log.info("tFileInputDelimited_1 - Retrieving records from the datasource.")
    df = (
        spark.read.format("csv")
        .option("sep", ";")
        .option("header", "true")
        .option("encoding", "ISO-8859-15")
        .option("lineSep", "\n")
        .option("mode", "PERMISSIVE")
        .schema(ROW1_SCHEMA)
        .load(filepath)
    )
    row_count = df.count()
    log.info("tFileInputDelimited_1 - Retrieved records count: %d.", row_count)
    log.debug("tFileInputDelimited_1 - Done.")
    return df


# ---------------------------------------------------------------------------
# Impala output  (tImpalaOutput_1)
# ---------------------------------------------------------------------------


def write_to_impala(
    spark: SparkSession,
    context: Context,
    df,
    is_first_file: bool,
) -> None:
    """
    Write the DataFrame rows to the Impala target table via JDBC, mirroring
    tImpalaOutput_1.

    The first write for the first file uses INSERT OVERWRITE semantics
    (SaveMode.Overwrite); all subsequent writes use INSERT INTO semantics
    (SaveMode.Append).  This mirrors the Talend behaviour where the first
    batch of the first file uses "INSERT OVERWRITE" and every other batch /
    file uses "INSERT INTO".
    """
    jdbc_url = _build_impala_jdbc_url(context)
    write_mode = "overwrite" if is_first_file else "append"

    log.info(
        "tImpalaOutput_1 - writing to %s (mode=%s).", TARGET_TABLE, write_mode
    )
    (
        df.write.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", TARGET_TABLE)
        .option("driver", "org.apache.hive.jdbc.HiveDriver")
        .option("user", context.EDH_CLUSTER_IMPALA_Login or "")
        .option("password", "")
        .mode(write_mode)
        .save()
    )
    log.debug("tImpalaOutput_1 - Done.")


# ---------------------------------------------------------------------------
# Impala metadata invalidation  (tImpalaRow_1)
# ---------------------------------------------------------------------------


def invalidate_impala_metadata(spark: SparkSession, context: Context) -> None:
    """
    Execute INVALIDATE METADATA against the target table via a raw Impala
    JDBC connection, mirroring tImpalaRow_1.
    """
    query = f"INVALIDATE METADATA {TARGET_TABLE}"
    log.info("tImpalaRow_1 - executing: %s", query)
    conn = _get_impala_connection(spark, context)
    try:
        stmt = conn.createStatement()
        try:
            stmt.execute(query)
        except Exception as e:
            log.error("tImpalaRow_1 - %s", e)
            print(f"Exception in the component tImpalaRow_1: {e}", file=sys.stderr)
        finally:
            stmt.close()
        log.info("tImpalaRow_1 - Done.")
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Main orchestration
# ---------------------------------------------------------------------------


def run(context: Context, spark: SparkSession) -> None:
    """
    Main job execution, following the Talend component chain:

    Implicit_Context_Regex / tContextLoad  (context already loaded by caller)
      → tJava_2             (Kerberos setup)
      → tImpalaConnection_1 (connection via Spark JVM gateway)
      → tJava_1             (print START + set W_JOB_PRINCIPALE)
      → tHiveConnection_1   (connection via SparkSession)
      → tFileList_1         (list matching CSV files in local FS)
          → [per file] tJava_3               (print "Inizio estrazione")
          → [per file] tFileInputDelimited_1  (read CSV)
          → [per file] tImpalaOutput_1        (INSERT OVERWRITE / INTO)
          → [per file] tImpalaRow_1           (INVALIDATE METADATA)
          → [per file] tJava_4               (print "END OF JOB")
    """

    # ------------------------------------------------------------------
    # tJava_2 – Kerberos setup
    # ------------------------------------------------------------------
    kerberos_setup(spark, context)

    # ------------------------------------------------------------------
    # tJava_1 – initialisation / startup logging
    # ------------------------------------------------------------------
    context.W_JOB_PRINCIPALE = JOB_NAME
    print("***************************************")
    print("***************START*******************")
    print(f"ESECUZIONE JOB: {context.W_JOB_PRINCIPALE}")
    print(f"context.W_MAIN_PARAMETRICA: {context.W_MAIN_PARAMETRICA}")
    log.info("tJava_1 - ESECUZIONE JOB: %s", context.W_JOB_PRINCIPALE)
    log.info("tJava_1 - context.W_MAIN_PARAMETRICA: %s", context.W_MAIN_PARAMETRICA)

    # ------------------------------------------------------------------
    # tFileList_1 – list matching CSV files on the local filesystem
    # ------------------------------------------------------------------
    matched_files = list_parametric_files(context)
    directory = context.W_FOLDER_IN_AUTOMAZIONI_CONTROLLI_II_LIV_PROFILATURA

    for file_index, filename in enumerate(matched_files):
        filepath = os.path.join(directory, filename)
        log.info("tFileList_1 - Current file or directory path: %s", filepath)

        # --------------------------------------------------------------
        # tJava_3 – log extraction start
        # --------------------------------------------------------------
        print(f"Inizio estrazione:{filepath}")
        log.info("tJava_3 - Inizio estrazione: %s", filepath)

        # --------------------------------------------------------------
        # tFileInputDelimited_1 – read the CSV file
        # --------------------------------------------------------------
        df = read_csv_file(spark, filepath)

        # --------------------------------------------------------------
        # tImpalaOutput_1 – write rows to Impala
        # --------------------------------------------------------------
        is_first_file = file_index == 0
        write_to_impala(spark, context, df, is_first_file=is_first_file)

        # --------------------------------------------------------------
        # tImpalaRow_1 – invalidate Impala metadata
        # --------------------------------------------------------------
        invalidate_impala_metadata(spark, context)

        # --------------------------------------------------------------
        # tJava_4 – log job end (once per file, matching original)
        # --------------------------------------------------------------
        print(f" END OF JOB: {context.W_JOB_PRINCIPALE}")
        log.info("tJava_4 - END OF JOB: %s", context.W_JOB_PRINCIPALE)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def _parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "S_AML_PROFILATURA_ESTRAZIONE_MAIN_PARAMETRICA – "
            "parametric extraction for AML profiling"
        )
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
        SparkSession.builder.appName(JOB_NAME)
        .config("spark.sql.catalogImplementation", "hive")
        .enableHiveSupport()
        .getOrCreate()
    )

    try:
        run(context, spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
