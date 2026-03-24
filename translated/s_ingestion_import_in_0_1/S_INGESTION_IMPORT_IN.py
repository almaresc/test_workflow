"""
Job: S_INGESTION_IMPORT_IN
Purpose: Ingestion import orchestrator – routes execution to the appropriate
         child job depending on whether the data source is a file or a table.
Author:  Pietrini, Luca
Version: 0.1
"""

import argparse
import importlib
import logging
import sys
from dataclasses import dataclass, field
from typing import Optional

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    level=logging.INFO,
)
log = logging.getLogger("S_INGESTION_IMPORT_IN")


# ---------------------------------------------------------------------------
# Context – holds all job parameters (mirrors Talend ContextProperties)
# ---------------------------------------------------------------------------
@dataclass
class Context:
    # Workload parameters
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

    # Generic DB connection
    DB_CONNECTION_Database: Optional[str] = None
    DB_CONNECTION_Login: Optional[str] = None
    DB_CONNECTION_Password: Optional[str] = None
    DB_CONNECTION_Port: Optional[str] = None
    DB_CONNECTION_Server: Optional[str] = None

    # PostgreSQL metadata / log tables
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

    # EDH cluster settings
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

    # HDFS settings
    EDH_CLUSTER_HDFS_HdfsFileSeparator: Optional[str] = None
    EDH_CLUSTER_HDFS_HdfsRowSeparator: Optional[str] = None

    # Hive settings
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

    # Impala settings
    EDH_CLUSTER_IMPALA_Database: Optional[str] = None
    EDH_CLUSTER_IMPALA_ImpalaPrincipal: Optional[str] = None
    EDH_CLUSTER_IMPALA_Login: Optional[str] = None
    EDH_CLUSTER_IMPALA_Port: Optional[str] = None
    EDH_CLUSTER_IMPALA_Server: Optional[str] = None

    def to_dict(self) -> dict:
        """Return a plain dict representation of the context."""
        return {k: v for k, v in self.__dict__.items()}

    @classmethod
    def from_dict(cls, params: dict) -> "Context":
        """Build a Context from a dictionary, ignoring unknown keys."""
        known = {f.name for f in cls.__dataclass_fields__.values()}  # type: ignore[attr-defined]
        return cls(**{k: v for k, v in params.items() if k in known})


# ---------------------------------------------------------------------------
# Main job class
# ---------------------------------------------------------------------------
class S_INGESTION_IMPORT_IN:
    """
    Orchestrator job that delegates to either the table-based or the
    file-based ingestion child job, depending on the W_DATA_TYPE context
    parameter.

    Equivalent child jobs:
      * W_DATA_TYPE != 'file'  →  S_INGESTION_IMPORT_TABELLA  (tRunJob_1)
      * W_DATA_TYPE == 'file'  →  S_INGESTION_IMPORT_FILE_1   (tRunJob_2)
    """

    JOB_NAME = "S_INGESTION_IMPORT_IN"
    JOB_VERSION = "0.1"
    PROJECT_NAME = "DATAHUB_AML"

    def __init__(self, context: Optional[Context] = None):
        self.context: Context = context if context is not None else Context()
        self.error_code: Optional[int] = None
        self.status: str = ""

    # ------------------------------------------------------------------
    # tJava_1 equivalent – print diagnostic message, then branch
    # ------------------------------------------------------------------
    def _t_java_1(self) -> None:
        print("File o Tabella? ")

    # ------------------------------------------------------------------
    # tRunJob_1 – delegate to S_INGESTION_IMPORT_TABELLA
    # ------------------------------------------------------------------
    def _run_job_tabella(self) -> None:
        log.info(
            "tRunJob_1 - The child job "
            "'S_INGESTION_IMPORT_TABELLA' starts on version '0.1'."
        )
        try:
            child_module = importlib.import_module(
                "translated.s_ingestion_import_tabella_0_1"
                ".S_INGESTION_IMPORT_TABELLA"
            )
            child_job = child_module.S_INGESTION_IMPORT_TABELLA(
                context=Context.from_dict(self.context.to_dict())
            )
            child_job.run()
            if child_job.status == "failure" or child_job.error_code not in (None, 0):
                raise RuntimeError(
                    "Child job S_INGESTION_IMPORT_TABELLA running failed."
                )
        except ImportError:
            log.warning(
                "Could not import S_INGESTION_IMPORT_TABELLA; "
                "ensure the translated module is available."
            )
            raise
        log.info("tRunJob_1 - The child job 'S_INGESTION_IMPORT_TABELLA' is done.")

    # ------------------------------------------------------------------
    # tRunJob_2 – delegate to S_INGESTION_IMPORT_FILE_1
    # ------------------------------------------------------------------
    def _run_job_file(self) -> None:
        log.info(
            "tRunJob_2 - The child job "
            "'S_INGESTION_IMPORT_FILE_1' starts on version '0.1'."
        )
        try:
            child_module = importlib.import_module(
                "translated.s_ingestion_import_file_1_0_1"
                ".S_INGESTION_IMPORT_FILE_1"
            )
            child_job = child_module.S_INGESTION_IMPORT_FILE_1(
                context=Context.from_dict(self.context.to_dict())
            )
            child_job.run()
            if child_job.status == "failure" or child_job.error_code not in (None, 0):
                raise RuntimeError(
                    "Child job S_INGESTION_IMPORT_FILE_1 running failed."
                )
        except ImportError:
            log.warning(
                "Could not import S_INGESTION_IMPORT_FILE_1; "
                "ensure the translated module is available."
            )
            raise
        log.info("tRunJob_2 - The child job 'S_INGESTION_IMPORT_FILE_1' is done.")

    # ------------------------------------------------------------------
    # Main execution entry point
    # ------------------------------------------------------------------
    def run(self) -> int:
        """
        Execute the job.

        Returns 0 on success, non-zero on failure.
        """
        log.info("TalendJob: '%s' - Start.", self.JOB_NAME)
        try:
            # tJava_1: diagnostic print + branching logic
            self._t_java_1()

            data_type = (self.context.W_DATA_TYPE or "").lower()

            # Branch: table-based vs file-based ingestion
            if data_type != "file":
                self._run_job_tabella()
            else:
                self._run_job_file()

            self.status = "success"
            self.error_code = 0
            log.info("TalendJob: '%s' - Done.", self.JOB_NAME)
            return 0

        except Exception as exc:
            log.fatal("Exception in job '%s': %s", self.JOB_NAME, exc, exc_info=True)
            self.status = "failure"
            self.error_code = 1
            return 1


# ---------------------------------------------------------------------------
# CLI parameter parsing (mirrors Talend runJobInTOS argument handling)
# ---------------------------------------------------------------------------
def _parse_args(argv: list) -> Context:
    parser = argparse.ArgumentParser(
        description=f"Run the {S_INGESTION_IMPORT_IN.JOB_NAME} Spark job."
    )
    parser.add_argument(
        "--context_param",
        metavar="KEY=VALUE",
        action="append",
        default=[],
        help="Context parameter in KEY=VALUE format (may be repeated).",
    )
    args, _ = parser.parse_known_args(argv)

    params: dict = {}
    for kv in args.context_param:
        if "=" in kv:
            key, value = kv.split("=", 1)
            params[key.strip()] = value.strip()

    return Context.from_dict(params)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main(argv: Optional[list] = None) -> int:
    if argv is None:
        argv = sys.argv[1:]
    context = _parse_args(argv)
    job = S_INGESTION_IMPORT_IN(context=context)
    return job.run()


if __name__ == "__main__":
    sys.exit(main())
