"""
Job: S_AML_PROFILATURA_ESTRAZIONE_UTENTI_AML
Purpose: estrarre i dati per universo partecipazioni
         (Extract data for the AML user profiling universe.)
         Iterates over CSV files matching the AML user pattern in the
         configured input folder and, for each file, delegates the actual
         copy-to-EDH work to the child job S_AML_PROFILATURA_COPY_FILE_TO_EDH.
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
log = logging.getLogger("S_AML_PROFILATURA_ESTRAZIONE_UTENTI_AML")

JOB_NAME = "S_AML_PROFILATURA_ESTRAZIONE_UTENTI_AML"


# ---------------------------------------------------------------------------
# Context helpers
# ---------------------------------------------------------------------------

class Context:
    """Holds all context variables, mirroring the Talend ContextProperties."""

    # AML / job-specific parameters
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
    W_UTENTI_AML: Optional[str] = None
    W_NAME_TFILE_LIST: Optional[str] = None

    # Generic DB connection parameters
    DB_CONNECTION_Database: Optional[str] = None
    DB_CONNECTION_Login: Optional[str] = None
    DB_CONNECTION_Password: Optional[str] = None
    DB_CONNECTION_Port: Optional[str] = None
    DB_CONNECTION_Server: Optional[str] = None

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

    # Postgres POC parameters
    DB_POSTGRES_POC_Database: Optional[str] = None
    DB_POSTGRES_POC_Login: Optional[str] = None
    DB_POSTGRES_POC_Password: Optional[str] = None
    DB_POSTGRES_POC_Port: Optional[str] = None
    DB_POSTGRES_POC_Schema: Optional[str] = None
    DB_POSTGRES_POC_Server: Optional[str] = None

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

    # Additional context / DaVinci parameters
    W_CONTEXT_TYPE: Optional[str] = None
    W_DAVINCI_TABELLA_TARGET: Optional[str] = None
    W_DB_ANDC_DH: Optional[str] = None
    W_DB_ANDC_ETL: Optional[str] = None
    W_DB_DAVINCI_POSTGRES_CUSTOM_LOG_TABLE: Optional[str] = None
    W_DB_DAVINCI_POSTGRES_FILE_METADATA_TABLE: Optional[str] = None
    W_DB_DAVINCI_POSTGRES_STATEMENT_TABLE: Optional[str] = None
    W_FOLDER_CSV: Optional[str] = None
    W_FOLDER_CSV_EXPORT: Optional[str] = None
    W_FOLDER_IN_AUTOMAZIONI_CONTROLLI_II_LIV_PROFILATURA: Optional[str] = None
    W_FOLDER_OUT_AUTOMAZIONI_CONTROLLI_II_LIV_PROFILATURA: Optional[str] = None
    W_HDFS_DAVINCI_ETL: Optional[str] = None
    W_HDFS_DH: Optional[str] = None
    W_HDFS_ETL: Optional[str] = None
    W_HIVE_DAVINCI_SANDBOX_DB: Optional[str] = None
    W_HIVE_DAVINCI_SANDBOX_TABLE: Optional[str] = None

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
        log.warning(
            "Context file not found: %s – using defaults / CLI params",
            context_file,
        )


def apply_cli_overrides(context: Context, overrides: dict) -> None:
    """Apply individual --context_param KEY=VALUE overrides (highest priority)."""
    for key, value in overrides.items():
        context.set(key, value)


# ---------------------------------------------------------------------------
# Local file-listing helper (mirrors tFileList_1)
# ---------------------------------------------------------------------------

def list_local_files(directory: str, filemask: str):
    """
    List files in *directory* whose names match *filemask* (glob pattern).
    Mirrors tFileList_1 (LIST_MODE=FILES, no subdirectories, case-sensitive,
    GLOBEXPRESSIONS=true, ORDER_BY_NOTHING=true).

    Returns a list of (filename, filepath) tuples in filesystem order.
    Raises RuntimeError if the directory contains no matching files,
    mirroring the Talend component's "Error if no match" behaviour.
    """
    dir_path = Path(directory)
    if not dir_path.is_dir():
        raise RuntimeError(
            f"tFileList_1 - directory does not exist or is not a directory: {directory}"
        )

    matched = []
    for entry in dir_path.iterdir():
        if entry.is_file() and fnmatch.fnmatch(entry.name, filemask):
            matched.append((entry.name, str(entry.resolve())))

    log.info("tFileList_1 - File or directory count: %d", len(matched))

    if len(matched) == 0:
        raise RuntimeError(
            f"No files matching pattern '{filemask}' found in directory {directory}"
        )

    return matched


# ---------------------------------------------------------------------------
# Child job invocation (mirrors tRunJob_1)
# ---------------------------------------------------------------------------

def run_child_job(
    context: Context,
    child_job_script: Optional[str] = None,
) -> None:
    """
    Invoke the child job S_AML_PROFILATURA_COPY_FILE_TO_EDH for the current
    file.  Mirrors tRunJob_1 (TRANSMIT_WHOLE_CONTEXT=true, DIE_ON_CHILD_ERROR=true).

    The child job is executed as a subprocess; its path can be supplied via
    the *child_job_script* argument or derived automatically relative to this
    file.
    """
    if child_job_script is None:
        base_dir = Path(__file__).resolve().parent.parent
        child_job_script = str(
            base_dir
            / "s_aml_profilatura_copy_file_to_edh_0_1"
            / "S_AML_PROFILATURA_COPY_FILE_TO_EDH.py"
        )

    cmd = [sys.executable, child_job_script]

    # Transmit the full context (TRANSMIT_WHOLE_CONTEXT=true)
    ctx_attrs = [
        a for a in vars(Context)
        if not a.startswith("_") and not callable(getattr(Context, a))
    ]
    for attr in ctx_attrs:
        value = context.get(attr)
        if value is not None:
            cmd += ["--context_param", f"{attr}={value}"]

    log.info(
        "tRunJob_1 - The child job 'S_AML_PROFILATURA_COPY_FILE_TO_EDH' starts "
        "on the version '0.1' with the context 'Default'."
    )
    result = subprocess.run(cmd, check=False)
    if result.returncode != 0:
        raise RuntimeError(
            f"Child job S_AML_PROFILATURA_COPY_FILE_TO_EDH failed "
            f"(exit code {result.returncode})"
        )
    log.info(
        "tRunJob_1 - The child job 'S_AML_PROFILATURA_COPY_FILE_TO_EDH' is done."
    )


# ---------------------------------------------------------------------------
# Main orchestration
# ---------------------------------------------------------------------------

def run(
    context: Context,
    spark: SparkSession,
    child_job_script: Optional[str] = None,
) -> None:
    """
    Main job execution, following the Talend component chain:

    Implicit_Context_Regex / tContextLoad
      → tJava_1      (set W_JOB_PRINCIPALE, print start banner)
      → tFileList_1  (list CSV files matching *{W_UTENTI_AML}*.csv)
          → [per file] tJava_3  (set W_NAME_TFILE_LIST, W_DAVINCI_TABELLA_TARGET,
                                  print debug info)
          → [per file] tRunJob_1 (run child job S_AML_PROFILATURA_COPY_FILE_TO_EDH
                                   with full context)
          → [per file] tJava_5  (print end-of-job message)
    """

    # ------------------------------------------------------------------
    # tJava_1 – initialise job name and print start banner
    # ------------------------------------------------------------------
    context.W_JOB_PRINCIPALE = JOB_NAME
    print("***************************************")
    print("***************START*******************")
    print(f"ESECUZIONE JOB: {context.W_JOB_PRINCIPALE}")

    # ------------------------------------------------------------------
    # tFileList_1 – list input files matching the AML user pattern
    # ------------------------------------------------------------------
    if not context.W_FOLDER_IN_AUTOMAZIONI_CONTROLLI_II_LIV_PROFILATURA:
        raise ValueError(
            "Required context variable "
            "W_FOLDER_IN_AUTOMAZIONI_CONTROLLI_II_LIV_PROFILATURA is not set."
        )
    if not context.W_UTENTI_AML:
        raise ValueError("Required context variable W_UTENTI_AML is not set.")

    input_dir = context.W_FOLDER_IN_AUTOMAZIONI_CONTROLLI_II_LIV_PROFILATURA
    filemask = f"*{context.W_UTENTI_AML}*.csv"

    log.info("tFileList_1 - Starting to search for matching entries.")
    log.info("tFileList_1 - Start to list files.")
    files = list_local_files(input_dir, filemask)

    for current_filename, current_filepath in files:
        log.info(
            "tFileList_1 - Current file or directory path : %s",
            current_filepath,
        )

        # ------------------------------------------------------------------
        # tJava_3 – per-file setup and debug output
        # ------------------------------------------------------------------
        context.W_NAME_TFILE_LIST = context.W_UTENTI_AML
        context.W_DAVINCI_TABELLA_TARGET = "aut_cont_2_livello_ar_prof_utenti_aml"

        print(f"Inizio estrazione:{current_filepath}")
        print(context.W_FOLDER_IN_AUTOMAZIONI_CONTROLLI_II_LIV_PROFILATURA)

        # ------------------------------------------------------------------
        # tRunJob_1 – delegate copy-to-EDH to child job
        # ------------------------------------------------------------------
        run_child_job(context, child_job_script=child_job_script)

        # ------------------------------------------------------------------
        # tJava_5 – end-of-job marker
        # ------------------------------------------------------------------
        print(f" END OF JOB: {context.W_JOB_PRINCIPALE}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def _parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "S_AML_PROFILATURA_ESTRAZIONE_UTENTI_AML – "
            "AML user profiling extraction orchestrator"
        )
    )
    parser.add_argument(
        "--context_file",
        default=None,
        help=(
            "Path to a key=value context properties file "
            "(Talend tContextLoad equivalent)"
        ),
    )
    parser.add_argument(
        "--context_param",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help=(
            "Override a single context variable "
            "(can be specified multiple times)"
        ),
    )
    parser.add_argument(
        "--child_job_script",
        default=None,
        help=(
            "Explicit path to the S_AML_PROFILATURA_COPY_FILE_TO_EDH.py "
            "child job script"
        ),
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
        .appName(JOB_NAME)
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
