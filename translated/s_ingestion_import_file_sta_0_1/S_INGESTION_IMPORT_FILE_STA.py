"""
Job: S_INGESTION_IMPORT_FILE_STA
Purpose: Ingestion of a static import file into HDFS.
Description:
  1. Load context properties from a context file (key=value pairs).
  2. Establish a Spark session with Hive support and HDFS Kerberos configuration.
  3. Read the source file (ISO-8859-15, with optional header skip) and write a
     temporary copy to the local input directory.
  4. Upload the temporary copy to HDFS under
     <W_HDFS_ETL_PATH>/in/<W_BUSINESS_NAME_lower>/<W_BUSINESS_NAME>.
  5. Archive the original source file to <W_PATH_ARCHIN> with a timestamp suffix.
  6. Delete the temporary file.

Author: Pietrini, Luca (original Java)
Translated to PySpark
"""

import logging
import os
import re
import shutil

from datetime import datetime

from pyspark.sql import SparkSession

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s - %(message)s",
)
log = logging.getLogger("S_INGESTION_IMPORT_FILE_STA")


# ---------------------------------------------------------------------------
# Context helpers
# ---------------------------------------------------------------------------

def load_context(context_file: str) -> dict:
    """
    Load context properties from a *key=value* file.
    Lines that do not contain '=' are silently ignored.
    """
    ctx: dict = {}
    with open(context_file, encoding="utf-8") as fh:
        for raw in fh:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                key, _, value = line.partition("=")
                ctx[key.strip()] = value.strip()
    return ctx


def get_context(ctx: dict, key: str, default: str = "") -> str:
    return ctx.get(key, default)


# ---------------------------------------------------------------------------
# Spark / Hive session
# ---------------------------------------------------------------------------

def create_spark_session(ctx: dict) -> SparkSession:
    """
    Build a SparkSession with Hive support and HDFS HA / Kerberos settings
    derived from the job context, mirroring tHiveConnection_1 and
    tHDFSConnection_1 configuration.
    """
    hive_server = get_context(ctx, "EDH_CLUSTER_HIVE_Server")
    hive_port = get_context(ctx, "EDH_CLUSTER_HIVE_Port", "10000")
    hive_db = get_context(ctx, "EDH_CLUSTER_HIVE_Database", "default")
    hive_principal = get_context(ctx, "EDH_CLUSTER_HIVE_HivePrincipal")
    hive_keytab_principal = get_context(ctx, "EDH_CLUSTER_HIVE_HiveKeyTabPrincipal")
    hive_keytab = get_context(ctx, "EDH_CLUSTER_HIVE_HiveKeyTab")
    hive_ssl_trust_store = get_context(ctx, "EDH_CLUSTER_HIVE_hiveSSLTrustStorePath")
    hive_ssl_trust_store_pw = get_context(ctx, "EDH_CLUSTER_HIVE_hiveSSLTrustStorePassword")
    hive_additional_jdbc = get_context(ctx, "EDH_CLUSTER_HIVE_hiveAdditionalJDBCParameters", "")

    namenode_uri = get_context(ctx, "EDH_CLUSTER_NameNodeUri")
    namenode_prin = get_context(ctx, "EDH_CLUSTER_NameNodePrin")
    cluster_principal = get_context(ctx, "EDH_CLUSTER_Principal")
    cluster_keytab = get_context(ctx, "EDH_CLUSTER_KeyTab")

    dfs_nameservices = get_context(ctx, "EDH_CLUSTER_dfs_nameservices")
    dfs_failover_provider = get_context(
        ctx, "EDH_CLUSTER_dfs_client_failover_proxy_provider_edhcluster"
    )
    ha_zookeeper = get_context(ctx, "EDH_CLUSTER_ha_zookeeper_quorum")
    dfs_ha_namenodes = get_context(ctx, "EDH_CLUSTER_dfs_ha_namenodes_edhcluster")
    nn_rpc_209 = get_context(
        ctx, "EDH_CLUSTER_dfs_namenode_rpc_address_edhcluster_namenode209"
    )
    nn_rpc_264 = get_context(
        ctx, "EDH_CLUSTER_dfs_namenode_rpc_address_edhcluster_namenode264"
    )

    dynamic_part = get_context(ctx, "EDH_CLUSTER_HIVE_dynamicPart", "nonstrict")
    execution_engine = get_context(ctx, "EDH_CLUSTER_HIVE_executionEngine", "mr")
    dynamic_part_max = get_context(ctx, "EDH_CLUSTER_HIVE_dynamicPartMax", "1000")
    dynamic_part_max_per_node = get_context(
        ctx, "EDH_CLUSTER_HIVE_dynamicPartMaxPerNode", "100"
    )

    # Build JDBC URL (mirrors tHiveConnection_1)
    jdbc_url = (
        f"jdbc:hive2://{hive_server}:{hive_port}/{hive_db}"
        f";principal={hive_principal}"
        f";ssl=true"
        f";sslTrustStore={hive_ssl_trust_store}"
        f";trustStorePassword={hive_ssl_trust_store_pw}"
    )
    if hive_additional_jdbc.strip():
        sep = hive_additional_jdbc if hive_additional_jdbc.startswith(";") else f";{hive_additional_jdbc}"
        jdbc_url += sep

    log.info("Building SparkSession with Hive JDBC URL: %s", jdbc_url)

    builder = (
        SparkSession.builder
        .appName("S_INGESTION_IMPORT_FILE_STA")
        .enableHiveSupport()
        # HDFS HA settings
        .config("spark.hadoop.fs.default.name", namenode_uri)
        .config("spark.hadoop.dfs.nameservices", dfs_nameservices)
        .config(
            f"spark.hadoop.dfs.client.failover.proxy.provider.{dfs_nameservices}",
            dfs_failover_provider,
        )
        .config("spark.hadoop.ha.zookeeper.quorum", ha_zookeeper)
        .config(f"spark.hadoop.dfs.ha.namenodes.{dfs_nameservices}", dfs_ha_namenodes)
        .config(
            f"spark.hadoop.dfs.namenode.rpc-address.{dfs_nameservices}.namenode209",
            nn_rpc_209,
        )
        .config(
            f"spark.hadoop.dfs.namenode.rpc-address.{dfs_nameservices}.namenode264",
            nn_rpc_264,
        )
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
        # Kerberos
        .config("spark.hadoop.dfs.namenode.kerberos.principal", namenode_prin)
        .config("spark.kerberos.principal", cluster_principal)
        .config("spark.kerberos.keytab", cluster_keytab)
        # Hive settings
        .config("spark.hadoop.hive.exec.dynamic.partition.mode", dynamic_part)
        .config("spark.hadoop.hive.execution.engine", execution_engine)
        .config("spark.hadoop.hive.exec.max.dynamic.partitions", dynamic_part_max)
        .config(
            "spark.hadoop.hive.exec.max.dynamic.partitions.pernode",
            dynamic_part_max_per_node,
        )
    )

    spark = builder.getOrCreate()
    log.info("SparkSession created successfully.")
    return spark


# ---------------------------------------------------------------------------
# Step implementations
# ---------------------------------------------------------------------------

def step_tJava_3(ctx: dict) -> dict:
    """
    Mirror tJava_3: derive the flag_header value and log diagnostic info.
    Returns a dict with the computed flag.
    """
    w_filename = get_context(ctx, "W_FILENAME")
    w_flag_header = get_context(ctx, "W_FLAG_HEADER", "f")
    flag_header = "1" if w_flag_header == "t" else "0"

    log.info("Processing file: %s", w_filename)
    log.info("W_FLAG_HEADER: %s  ->  flag_header: %s", w_flag_header, flag_header)
    log.info("PATH ARCHIN: %s", get_context(ctx, "W_PATH_ARCHIN"))
    log.info(
        "file header HDFS_put: %s",
        get_context(ctx, "W_F_FILEMASK") + "_tmp",
    )

    return {"flag_header": flag_header}


def step_file_input_output(ctx: dict, flags: dict) -> int:
    """
    Mirror tFileInputFullRow_1 -> tFileOutputDelimited_1:
    Read the source file (ISO-8859-15, skip header lines according to
    flag_header) and write it to a temporary file in the same directory.

    Returns the number of lines written.
    """
    src_path = os.path.join(get_context(ctx, "W_PATH_IN"), get_context(ctx, "W_FILENAME"))
    tmp_path = src_path + "_tmp"
    skip_lines = int(flags.get("flag_header", "0"))

    log.info(
        "Reading source file '%s' (encoding=ISO-8859-15, skip=%d header lines)",
        src_path,
        skip_lines,
    )

    nb_lines = 0
    with (
        open(src_path, encoding="iso-8859-15", newline="") as fin,
        open(tmp_path, "w", encoding="iso-8859-15", newline="") as fout,
    ):
        for idx, raw_line in enumerate(fin):
            if idx < skip_lines:
                continue
            line = raw_line.rstrip("\n").rstrip("\r")
            if not line:
                continue
            fout.write(line + "\n")
            nb_lines += 1

    log.info("Wrote %d lines to temporary file '%s'.", nb_lines, tmp_path)
    return nb_lines


def step_hdfs_put(ctx: dict, spark: SparkSession) -> None:
    """
    Mirror tHDFSPut_1: upload the temporary file to HDFS, overwriting any
    existing file, renaming it to W_BUSINESS_NAME.

    Local:  <W_PATH_IN>/<W_FILENAME>_tmp
    Remote: <W_HDFS_ETL_PATH>/in/<W_BUSINESS_NAME_lower>/<W_BUSINESS_NAME>
    """
    w_path_in = get_context(ctx, "W_PATH_IN")
    w_filename = get_context(ctx, "W_FILENAME")
    w_hdfs_etl_path = get_context(ctx, "W_HDFS_ETL_PATH")
    w_business_name = get_context(ctx, "W_BUSINESS_NAME")

    local_tmp = os.path.join(w_path_in, w_filename + "_tmp")
    hdfs_dir = f"{w_hdfs_etl_path}/in/{w_business_name.lower()}"
    hdfs_target = f"{hdfs_dir}/{w_business_name}"

    log.info("Uploading '%s' -> HDFS '%s'", local_tmp, hdfs_target)

    sc = spark.sparkContext
    hadoop_conf = sc._jsc.hadoopConfiguration()
    fs_uri = hadoop_conf.get("fs.default.name", "hdfs:///")

    jvm = sc._jvm
    path_cls = jvm.org.apache.hadoop.fs.Path
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(
        jvm.java.net.URI(fs_uri), hadoop_conf
    )

    # Create remote directory if needed
    fs.mkdirs(path_cls(hdfs_dir))

    # Copy local file to HDFS (overwrite=True)
    fs.copyFromLocalFile(False, True, path_cls(local_tmp), path_cls(hdfs_target))

    log.info("Upload complete: '%s' -> '%s'", local_tmp, hdfs_target)


def step_file_copy(ctx: dict) -> str:
    """
    Mirror tFileCopy_1: copy the original source file to W_PATH_ARCHIN,
    renaming it with a timestamp suffix, and remove the source file.

    Returns the destination file path.
    """
    w_path_in = get_context(ctx, "W_PATH_IN")
    w_filename = get_context(ctx, "W_FILENAME")
    w_path_archin = get_context(ctx, "W_PATH_ARCHIN")

    src_path = os.path.join(w_path_in, w_filename)
    if not os.path.isfile(src_path):
        raise RuntimeError(
            f"The source file \"{src_path}\" does not exist or is not a regular file."
        )

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    dest_filename = f"{w_filename}_{timestamp}"
    dest_path = os.path.join(w_path_archin, dest_filename)

    os.makedirs(w_path_archin, exist_ok=True)
    shutil.copy2(src_path, dest_path)
    os.remove(src_path)

    if os.path.exists(src_path):
        raise RuntimeError(
            f"The source file \"{src_path}\" could not be removed."
        )

    log.info("Archived '%s' -> '%s' (source removed).", src_path, dest_path)
    return dest_path


def step_file_delete(ctx: dict) -> None:
    """
    Mirror tFileDelete_1: delete the temporary file
    <W_PATH_IN>/<W_FILENAME>_tmp.
    """
    w_path_in = get_context(ctx, "W_PATH_IN")
    w_filename = get_context(ctx, "W_FILENAME")
    tmp_path = os.path.join(w_path_in, w_filename + "_tmp")

    if not os.path.isfile(tmp_path):
        raise RuntimeError(
            f"File '{tmp_path}' does not exist or is not a regular file."
        )

    os.remove(tmp_path)
    if os.path.exists(tmp_path):
        raise RuntimeError(f"File '{tmp_path}' could not be deleted.")

    log.info("Deleted temporary file '%s'.", tmp_path)


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run(context_file: str) -> None:
    """
    Execute the full S_INGESTION_IMPORT_FILE_STA job pipeline.

    Pipeline order (mirrors the Talend job):
      Implicit_Context_Regex  -> context load
      tLibraryLoad_1          -> (handled by imports)
      tHiveConnection_1       -> SparkSession with Hive support
      tHDFSConnection_1       -> SparkSession Hadoop config
      tJava_3                 -> flag_header derivation
      tFileInputFullRow_1 / tFileOutputDelimited_1  -> local file copy (strip header)
      tHDFSPut_1              -> upload to HDFS
      tFileCopy_1             -> archive original with timestamp, remove source
      tFileDelete_1           -> delete temporary file
    """
    log.info("Job S_INGESTION_IMPORT_FILE_STA starting.")

    # 1. Load context
    ctx = load_context(context_file)
    log.info("Context loaded from '%s' (%d keys).", context_file, len(ctx))

    # 2. Create Spark session (Hive + HDFS connections)
    spark = create_spark_session(ctx)

    try:
        # 3. tJava_3: derive flag_header and log diagnostic info
        flags = step_tJava_3(ctx)

        # 4. tFileInputFullRow_1 -> tFileOutputDelimited_1: read source, write tmp
        step_file_input_output(ctx, flags)

        # 5. tHDFSPut_1: upload tmp file to HDFS
        step_hdfs_put(ctx, spark)

        # 6. tFileCopy_1: archive original file with timestamp, remove source
        step_file_copy(ctx)

        # 7. tFileDelete_1: delete the tmp file
        step_file_delete(ctx)

        log.info("Job S_INGESTION_IMPORT_FILE_STA completed successfully.")
    finally:
        spark.stop()
        log.info("SparkSession stopped.")


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print(
            "Usage: spark-submit S_INGESTION_IMPORT_FILE_STA.py <context_file>",
            file=sys.stderr,
        )
        sys.exit(1)

    run(sys.argv[1])
