
package datahub_aml.s_ingestion_import_file_1_0_1;

import routines.DataOperation;
import routines.TalendDataGenerator;
import routines.DataQuality;
import routines.Relational;
import routines.Mathematical;
import routines.DataQualityDependencies;
import routines.SQLike;
import routines.Numeric;
import routines.TalendStringUtil;
import routines.TalendString;
import routines.DQTechnical;
import routines.StringHandling;
import routines.TalendDate;
import routines.DataMasking;
import routines.DqStringHandling;
import routines.system.*;
import routines.system.api.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.math.BigDecimal;
import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;
import java.io.IOException;
import java.util.Comparator;
 




	//the import part of tJava_3
	//import java.util.List;

	//the import part of tJava_1
	//import java.util.List;


@SuppressWarnings("unused")

/**
 * Job: S_INGESTION_IMPORT_FILE_1 Purpose: <br>
 * Description:  <br>
 * @author Pietrini, Luca
 * @version 7.3.1.20241003_1446-patch
 * @status 
 */
public class S_INGESTION_IMPORT_FILE_1 implements TalendJob {
	static {System.setProperty("TalendJob.log", "S_INGESTION_IMPORT_FILE_1.log");}

	

	
	private static org.apache.logging.log4j.Logger log = org.apache.logging.log4j.LogManager.getLogger(S_INGESTION_IMPORT_FILE_1.class);
	

protected static void logIgnoredError(String message, Throwable cause) {
       log.error(message, cause);

}


	public final Object obj = new Object();

	// for transmiting parameters purpose
	private Object valueObject = null;

	public Object getValueObject() {
		return this.valueObject;
	}

	public void setValueObject(Object valueObject) {
		this.valueObject = valueObject;
	}
	
	private final static String defaultCharset = java.nio.charset.Charset.defaultCharset().name();

	
	private final static String utf8Charset = "UTF-8";
	//contains type for every context property
	public class PropertiesWithType extends java.util.Properties {
		private static final long serialVersionUID = 1L;
		private java.util.Map<String,String> propertyTypes = new java.util.HashMap<>();
		
		public PropertiesWithType(java.util.Properties properties){
			super(properties);
		}
		public PropertiesWithType(){
			super();
		}
		
		public void setContextType(String key, String type) {
			propertyTypes.put(key,type);
		}
	
		public String getContextType(String key) {
			return propertyTypes.get(key);
		}
	}
	
	// create and load default properties
	private java.util.Properties defaultProps = new java.util.Properties();
	// create application properties with default
	public class ContextProperties extends PropertiesWithType {

		private static final long serialVersionUID = 1L;

		public ContextProperties(java.util.Properties properties){
			super(properties);
		}
		public ContextProperties(){
			super();
		}

		public void synchronizeContext(){
			
			if(W_BUSINESS_GROUP != null){
				
					this.setProperty("W_BUSINESS_GROUP", W_BUSINESS_GROUP.toString());
				
			}
			
			if(W_BUSINESS_NAME != null){
				
					this.setProperty("W_BUSINESS_NAME", W_BUSINESS_NAME.toString());
				
			}
			
			if(W_DATA_TYPE != null){
				
					this.setProperty("W_DATA_TYPE", W_DATA_TYPE.toString());
				
			}
			
			if(W_DEFAULT_PARTITION != null){
				
					this.setProperty("W_DEFAULT_PARTITION", W_DEFAULT_PARTITION.toString());
				
			}
			
			if(W_EDH_DB_ARCHIN != null){
				
					this.setProperty("W_EDH_DB_ARCHIN", W_EDH_DB_ARCHIN.toString());
				
			}
			
			if(W_EDH_DB_H != null){
				
					this.setProperty("W_EDH_DB_H", W_EDH_DB_H.toString());
				
			}
			
			if(W_EDH_DB_IN != null){
				
					this.setProperty("W_EDH_DB_IN", W_EDH_DB_IN.toString());
				
			}
			
			if(W_EDH_TABLE_ARCHIN != null){
				
					this.setProperty("W_EDH_TABLE_ARCHIN", W_EDH_TABLE_ARCHIN.toString());
				
			}
			
			if(W_EDH_TABLE_H != null){
				
					this.setProperty("W_EDH_TABLE_H", W_EDH_TABLE_H.toString());
				
			}
			
			if(W_EDH_TABLE_IN != null){
				
					this.setProperty("W_EDH_TABLE_IN", W_EDH_TABLE_IN.toString());
				
			}
			
			if(W_EXTRACTION_FIELDS != null){
				
					this.setProperty("W_EXTRACTION_FIELDS", W_EXTRACTION_FIELDS.toString());
				
			}
			
			if(W_F_FILEMASK != null){
				
					this.setProperty("W_F_FILEMASK", W_F_FILEMASK.toString());
				
			}
			
			if(W_FILENAME != null){
				
					this.setProperty("W_FILENAME", W_FILENAME.toString());
				
			}
			
			if(W_FLAG_ABILITATA != null){
				
					this.setProperty("W_FLAG_ABILITATA", W_FLAG_ABILITATA.toString());
				
			}
			
			if(W_FLAG_HEADER != null){
				
					this.setProperty("W_FLAG_HEADER", W_FLAG_HEADER.toString());
				
			}
			
			if(W_HDFS_ETL_PATH != null){
				
					this.setProperty("W_HDFS_ETL_PATH", W_HDFS_ETL_PATH.toString());
				
			}
			
			if(W_IMPORT_TYPE != null){
				
					this.setProperty("W_IMPORT_TYPE", W_IMPORT_TYPE.toString());
				
			}
			
			if(W_LAST_PARTITION != null){
				
					this.setProperty("W_LAST_PARTITION", W_LAST_PARTITION.toString());
				
			}
			
			if(W_MAPPER != null){
				
					this.setProperty("W_MAPPER", W_MAPPER.toString());
				
			}
			
			if(W_PARTITION_FIELD != null){
				
					this.setProperty("W_PARTITION_FIELD", W_PARTITION_FIELD.toString());
				
			}
			
			if(W_PATH_ARCHIN != null){
				
					this.setProperty("W_PATH_ARCHIN", W_PATH_ARCHIN.toString());
				
			}
			
			if(W_PATH_IN != null){
				
					this.setProperty("W_PATH_IN", W_PATH_IN.toString());
				
			}
			
			if(W_SOURCE_NAME != null){
				
					this.setProperty("W_SOURCE_NAME", W_SOURCE_NAME.toString());
				
			}
			
			if(W_T_SOURCE_DATABASE != null){
				
					this.setProperty("W_T_SOURCE_DATABASE", W_T_SOURCE_DATABASE.toString());
				
			}
			
			if(W_T_SOURCE_TABLE != null){
				
					this.setProperty("W_T_SOURCE_TABLE", W_T_SOURCE_TABLE.toString());
				
			}
			
			if(DB_POSTGRES_CUSTOM_LOG_TABLE != null){
				
					this.setProperty("DB_POSTGRES_CUSTOM_LOG_TABLE", DB_POSTGRES_CUSTOM_LOG_TABLE.toString());
				
			}
			
			if(DB_POSTGRES_Database != null){
				
					this.setProperty("DB_POSTGRES_Database", DB_POSTGRES_Database.toString());
				
			}
			
			if(DB_POSTGRES_FILE_METADATA_TABLE != null){
				
					this.setProperty("DB_POSTGRES_FILE_METADATA_TABLE", DB_POSTGRES_FILE_METADATA_TABLE.toString());
				
			}
			
			if(DB_POSTGRES_LOG_TABLE != null){
				
					this.setProperty("DB_POSTGRES_LOG_TABLE", DB_POSTGRES_LOG_TABLE.toString());
				
			}
			
			if(DB_POSTGRES_Login != null){
				
					this.setProperty("DB_POSTGRES_Login", DB_POSTGRES_Login.toString());
				
			}
			
			if(DB_POSTGRES_LOGTABLE != null){
				
					this.setProperty("DB_POSTGRES_LOGTABLE", DB_POSTGRES_LOGTABLE.toString());
				
			}
			
			if(DB_POSTGRES_LOGVIEW != null){
				
					this.setProperty("DB_POSTGRES_LOGVIEW", DB_POSTGRES_LOGVIEW.toString());
				
			}
			
			if(DB_POSTGRES_METADATA_FACT_RECUPERO_PARTIZIONI != null){
				
					this.setProperty("DB_POSTGRES_METADATA_FACT_RECUPERO_PARTIZIONI", DB_POSTGRES_METADATA_FACT_RECUPERO_PARTIZIONI.toString());
				
			}
			
			if(DB_POSTGRES_METADATA_FACT_TABLE != null){
				
					this.setProperty("DB_POSTGRES_METADATA_FACT_TABLE", DB_POSTGRES_METADATA_FACT_TABLE.toString());
				
			}
			
			if(DB_POSTGRES_Password != null){
				
					this.setProperty("DB_POSTGRES_Password", DB_POSTGRES_Password.toString());
				
			}
			
			if(DB_POSTGRES_Port != null){
				
					this.setProperty("DB_POSTGRES_Port", DB_POSTGRES_Port.toString());
				
			}
			
			if(DB_POSTGRES_Schema != null){
				
					this.setProperty("DB_POSTGRES_Schema", DB_POSTGRES_Schema.toString());
				
			}
			
			if(DB_POSTGRES_Server != null){
				
					this.setProperty("DB_POSTGRES_Server", DB_POSTGRES_Server.toString());
				
			}
			
			if(DB_POSTGRES_SQOOP_METADATA_TABLE != null){
				
					this.setProperty("DB_POSTGRES_SQOOP_METADATA_TABLE", DB_POSTGRES_SQOOP_METADATA_TABLE.toString());
				
			}
			
			if(DB_POSTGRES_STATTABLE != null){
				
					this.setProperty("DB_POSTGRES_STATTABLE", DB_POSTGRES_STATTABLE.toString());
				
			}
			
			if(EDH_CLUSTER_dfs_client_failover_proxy_provider_edhcluster != null){
				
					this.setProperty("EDH_CLUSTER_dfs_client_failover_proxy_provider_edhcluster", EDH_CLUSTER_dfs_client_failover_proxy_provider_edhcluster.toString());
				
			}
			
			if(EDH_CLUSTER_dfs_ha_namenodes_edhcluster != null){
				
					this.setProperty("EDH_CLUSTER_dfs_ha_namenodes_edhcluster", EDH_CLUSTER_dfs_ha_namenodes_edhcluster.toString());
				
			}
			
			if(EDH_CLUSTER_dfs_namenode_rpc_address_edhcluster_namenode209 != null){
				
					this.setProperty("EDH_CLUSTER_dfs_namenode_rpc_address_edhcluster_namenode209", EDH_CLUSTER_dfs_namenode_rpc_address_edhcluster_namenode209.toString());
				
			}
			
			if(EDH_CLUSTER_dfs_namenode_rpc_address_edhcluster_namenode264 != null){
				
					this.setProperty("EDH_CLUSTER_dfs_namenode_rpc_address_edhcluster_namenode264", EDH_CLUSTER_dfs_namenode_rpc_address_edhcluster_namenode264.toString());
				
			}
			
			if(EDH_CLUSTER_dfs_nameservices != null){
				
					this.setProperty("EDH_CLUSTER_dfs_nameservices", EDH_CLUSTER_dfs_nameservices.toString());
				
			}
			
			if(EDH_CLUSTER_ha_zookeeper_quorum != null){
				
					this.setProperty("EDH_CLUSTER_ha_zookeeper_quorum", EDH_CLUSTER_ha_zookeeper_quorum.toString());
				
			}
			
			if(EDH_CLUSTER_JobHistory != null){
				
					this.setProperty("EDH_CLUSTER_JobHistory", EDH_CLUSTER_JobHistory.toString());
				
			}
			
			if(EDH_CLUSTER_JobHistroyPrin != null){
				
					this.setProperty("EDH_CLUSTER_JobHistroyPrin", EDH_CLUSTER_JobHistroyPrin.toString());
				
			}
			
			if(EDH_CLUSTER_JTOrRMPrin != null){
				
					this.setProperty("EDH_CLUSTER_JTOrRMPrin", EDH_CLUSTER_JTOrRMPrin.toString());
				
			}
			
			if(EDH_CLUSTER_KeyTab != null){
				
					this.setProperty("EDH_CLUSTER_KeyTab", EDH_CLUSTER_KeyTab.toString());
				
			}
			
			if(EDH_CLUSTER_NameNodePrin != null){
				
					this.setProperty("EDH_CLUSTER_NameNodePrin", EDH_CLUSTER_NameNodePrin.toString());
				
			}
			
			if(EDH_CLUSTER_NameNodeUri != null){
				
					this.setProperty("EDH_CLUSTER_NameNodeUri", EDH_CLUSTER_NameNodeUri.toString());
				
			}
			
			if(EDH_CLUSTER_Principal != null){
				
					this.setProperty("EDH_CLUSTER_Principal", EDH_CLUSTER_Principal.toString());
				
			}
			
			if(EDH_CLUSTER_ResourceManager != null){
				
					this.setProperty("EDH_CLUSTER_ResourceManager", EDH_CLUSTER_ResourceManager.toString());
				
			}
			
			if(EDH_CLUSTER_ResourceManagerScheduler != null){
				
					this.setProperty("EDH_CLUSTER_ResourceManagerScheduler", EDH_CLUSTER_ResourceManagerScheduler.toString());
				
			}
			
			if(EDH_CLUSTER_StagingDirectory != null){
				
					this.setProperty("EDH_CLUSTER_StagingDirectory", EDH_CLUSTER_StagingDirectory.toString());
				
			}
			
			if(EDH_CLUSTER_username != null){
				
					this.setProperty("EDH_CLUSTER_username", EDH_CLUSTER_username.toString());
				
			}
			
			if(EDH_CLUSTER_yarn_resourcemanager_address_rm1 != null){
				
					this.setProperty("EDH_CLUSTER_yarn_resourcemanager_address_rm1", EDH_CLUSTER_yarn_resourcemanager_address_rm1.toString());
				
			}
			
			if(EDH_CLUSTER_yarn_resourcemanager_address_rm2 != null){
				
					this.setProperty("EDH_CLUSTER_yarn_resourcemanager_address_rm2", EDH_CLUSTER_yarn_resourcemanager_address_rm2.toString());
				
			}
			
			if(EDH_CLUSTER_yarn_resourcemanager_ha_enabled != null){
				
					this.setProperty("EDH_CLUSTER_yarn_resourcemanager_ha_enabled", EDH_CLUSTER_yarn_resourcemanager_ha_enabled.toString());
				
			}
			
			if(EDH_CLUSTER_yarn_resourcemanager_ha_rm_ids != null){
				
					this.setProperty("EDH_CLUSTER_yarn_resourcemanager_ha_rm_ids", EDH_CLUSTER_yarn_resourcemanager_ha_rm_ids.toString());
				
			}
			
			if(EDH_CLUSTER_yarn_resourcemanager_scheduler_address_rm1 != null){
				
					this.setProperty("EDH_CLUSTER_yarn_resourcemanager_scheduler_address_rm1", EDH_CLUSTER_yarn_resourcemanager_scheduler_address_rm1.toString());
				
			}
			
			if(EDH_CLUSTER_yarn_resourcemanager_scheduler_address_rm2 != null){
				
					this.setProperty("EDH_CLUSTER_yarn_resourcemanager_scheduler_address_rm2", EDH_CLUSTER_yarn_resourcemanager_scheduler_address_rm2.toString());
				
			}
			
			if(EDH_CLUSTER_HDFS_HdfsFileSeparator != null){
				
					this.setProperty("EDH_CLUSTER_HDFS_HdfsFileSeparator", EDH_CLUSTER_HDFS_HdfsFileSeparator.toString());
				
			}
			
			if(EDH_CLUSTER_HDFS_HdfsRowSeparator != null){
				
					this.setProperty("EDH_CLUSTER_HDFS_HdfsRowSeparator", EDH_CLUSTER_HDFS_HdfsRowSeparator.toString());
				
			}
			
			if(EDH_CLUSTER_HIVE_Database != null){
				
					this.setProperty("EDH_CLUSTER_HIVE_Database", EDH_CLUSTER_HIVE_Database.toString());
				
			}
			
			if(EDH_CLUSTER_HIVE_dynamicPart != null){
				
					this.setProperty("EDH_CLUSTER_HIVE_dynamicPart", EDH_CLUSTER_HIVE_dynamicPart.toString());
				
			}
			
			if(EDH_CLUSTER_HIVE_dynamicPartMax != null){
				
					this.setProperty("EDH_CLUSTER_HIVE_dynamicPartMax", EDH_CLUSTER_HIVE_dynamicPartMax.toString());
				
			}
			
			if(EDH_CLUSTER_HIVE_dynamicPartMaxPerNode != null){
				
					this.setProperty("EDH_CLUSTER_HIVE_dynamicPartMaxPerNode", EDH_CLUSTER_HIVE_dynamicPartMaxPerNode.toString());
				
			}
			
			if(EDH_CLUSTER_HIVE_executionEngine != null){
				
					this.setProperty("EDH_CLUSTER_HIVE_executionEngine", EDH_CLUSTER_HIVE_executionEngine.toString());
				
			}
			
			if(EDH_CLUSTER_HIVE_hiveAdditionalJDBCParameters != null){
				
					this.setProperty("EDH_CLUSTER_HIVE_hiveAdditionalJDBCParameters", EDH_CLUSTER_HIVE_hiveAdditionalJDBCParameters.toString());
				
			}
			
			if(EDH_CLUSTER_HIVE_HiveKeyTab != null){
				
					this.setProperty("EDH_CLUSTER_HIVE_HiveKeyTab", EDH_CLUSTER_HIVE_HiveKeyTab.toString());
				
			}
			
			if(EDH_CLUSTER_HIVE_HiveKeyTabPrincipal != null){
				
					this.setProperty("EDH_CLUSTER_HIVE_HiveKeyTabPrincipal", EDH_CLUSTER_HIVE_HiveKeyTabPrincipal.toString());
				
			}
			
			if(EDH_CLUSTER_HIVE_HivePrincipal != null){
				
					this.setProperty("EDH_CLUSTER_HIVE_HivePrincipal", EDH_CLUSTER_HIVE_HivePrincipal.toString());
				
			}
			
			if(EDH_CLUSTER_HIVE_hiveSSLTrustStorePassword != null){
				
					this.setProperty("EDH_CLUSTER_HIVE_hiveSSLTrustStorePassword", EDH_CLUSTER_HIVE_hiveSSLTrustStorePassword.toString());
				
			}
			
			if(EDH_CLUSTER_HIVE_hiveSSLTrustStorePath != null){
				
					this.setProperty("EDH_CLUSTER_HIVE_hiveSSLTrustStorePath", EDH_CLUSTER_HIVE_hiveSSLTrustStorePath.toString());
				
			}
			
			if(EDH_CLUSTER_HIVE_Login != null){
				
					this.setProperty("EDH_CLUSTER_HIVE_Login", EDH_CLUSTER_HIVE_Login.toString());
				
			}
			
			if(EDH_CLUSTER_HIVE_Password != null){
				
					this.setProperty("EDH_CLUSTER_HIVE_Password", EDH_CLUSTER_HIVE_Password.toString());
				
			}
			
			if(EDH_CLUSTER_HIVE_Port != null){
				
					this.setProperty("EDH_CLUSTER_HIVE_Port", EDH_CLUSTER_HIVE_Port.toString());
				
			}
			
			if(EDH_CLUSTER_HIVE_Server != null){
				
					this.setProperty("EDH_CLUSTER_HIVE_Server", EDH_CLUSTER_HIVE_Server.toString());
				
			}
			
			if(EDH_CLUSTER_IMPALA_Database != null){
				
					this.setProperty("EDH_CLUSTER_IMPALA_Database", EDH_CLUSTER_IMPALA_Database.toString());
				
			}
			
			if(EDH_CLUSTER_IMPALA_ImpalaPrincipal != null){
				
					this.setProperty("EDH_CLUSTER_IMPALA_ImpalaPrincipal", EDH_CLUSTER_IMPALA_ImpalaPrincipal.toString());
				
			}
			
			if(EDH_CLUSTER_IMPALA_Login != null){
				
					this.setProperty("EDH_CLUSTER_IMPALA_Login", EDH_CLUSTER_IMPALA_Login.toString());
				
			}
			
			if(EDH_CLUSTER_IMPALA_Port != null){
				
					this.setProperty("EDH_CLUSTER_IMPALA_Port", EDH_CLUSTER_IMPALA_Port.toString());
				
			}
			
			if(EDH_CLUSTER_IMPALA_Server != null){
				
					this.setProperty("EDH_CLUSTER_IMPALA_Server", EDH_CLUSTER_IMPALA_Server.toString());
				
			}
			
			if(DB_POSTGRES_configuration_metadata != null){
				
					this.setProperty("DB_POSTGRES_configuration_metadata", DB_POSTGRES_configuration_metadata.toString());
				
			}
			
		}
		
		//if the stored or passed value is "<TALEND_NULL>" string, it mean null
		public String getStringValue(String key) {
			String origin_value = this.getProperty(key);
			if(NULL_VALUE_EXPRESSION_IN_COMMAND_STRING_FOR_CHILD_JOB_ONLY.equals(origin_value)) {
				return null;
			}
			return origin_value;
		}

public String W_BUSINESS_GROUP;
public String getW_BUSINESS_GROUP(){
	return this.W_BUSINESS_GROUP;
}
public String W_BUSINESS_NAME;
public String getW_BUSINESS_NAME(){
	return this.W_BUSINESS_NAME;
}
public String W_DATA_TYPE;
public String getW_DATA_TYPE(){
	return this.W_DATA_TYPE;
}
public String W_DEFAULT_PARTITION;
public String getW_DEFAULT_PARTITION(){
	return this.W_DEFAULT_PARTITION;
}
public String W_EDH_DB_ARCHIN;
public String getW_EDH_DB_ARCHIN(){
	return this.W_EDH_DB_ARCHIN;
}
public String W_EDH_DB_H;
public String getW_EDH_DB_H(){
	return this.W_EDH_DB_H;
}
public String W_EDH_DB_IN;
public String getW_EDH_DB_IN(){
	return this.W_EDH_DB_IN;
}
public String W_EDH_TABLE_ARCHIN;
public String getW_EDH_TABLE_ARCHIN(){
	return this.W_EDH_TABLE_ARCHIN;
}
public String W_EDH_TABLE_H;
public String getW_EDH_TABLE_H(){
	return this.W_EDH_TABLE_H;
}
public String W_EDH_TABLE_IN;
public String getW_EDH_TABLE_IN(){
	return this.W_EDH_TABLE_IN;
}
public String W_EXTRACTION_FIELDS;
public String getW_EXTRACTION_FIELDS(){
	return this.W_EXTRACTION_FIELDS;
}
public String W_F_FILEMASK;
public String getW_F_FILEMASK(){
	return this.W_F_FILEMASK;
}
public String W_FILENAME;
public String getW_FILENAME(){
	return this.W_FILENAME;
}
public String W_FLAG_ABILITATA;
public String getW_FLAG_ABILITATA(){
	return this.W_FLAG_ABILITATA;
}
public String W_FLAG_HEADER;
public String getW_FLAG_HEADER(){
	return this.W_FLAG_HEADER;
}
public String W_HDFS_ETL_PATH;
public String getW_HDFS_ETL_PATH(){
	return this.W_HDFS_ETL_PATH;
}
public String W_IMPORT_TYPE;
public String getW_IMPORT_TYPE(){
	return this.W_IMPORT_TYPE;
}
public String W_LAST_PARTITION;
public String getW_LAST_PARTITION(){
	return this.W_LAST_PARTITION;
}
public String W_MAPPER;
public String getW_MAPPER(){
	return this.W_MAPPER;
}
public String W_PARTITION_FIELD;
public String getW_PARTITION_FIELD(){
	return this.W_PARTITION_FIELD;
}
public String W_PATH_ARCHIN;
public String getW_PATH_ARCHIN(){
	return this.W_PATH_ARCHIN;
}
public String W_PATH_IN;
public String getW_PATH_IN(){
	return this.W_PATH_IN;
}
public String W_SOURCE_NAME;
public String getW_SOURCE_NAME(){
	return this.W_SOURCE_NAME;
}
public String W_T_SOURCE_DATABASE;
public String getW_T_SOURCE_DATABASE(){
	return this.W_T_SOURCE_DATABASE;
}
public String W_T_SOURCE_TABLE;
public String getW_T_SOURCE_TABLE(){
	return this.W_T_SOURCE_TABLE;
}
public String DB_POSTGRES_CUSTOM_LOG_TABLE;
public String getDB_POSTGRES_CUSTOM_LOG_TABLE(){
	return this.DB_POSTGRES_CUSTOM_LOG_TABLE;
}
public String DB_POSTGRES_Database;
public String getDB_POSTGRES_Database(){
	return this.DB_POSTGRES_Database;
}
public String DB_POSTGRES_FILE_METADATA_TABLE;
public String getDB_POSTGRES_FILE_METADATA_TABLE(){
	return this.DB_POSTGRES_FILE_METADATA_TABLE;
}
public String DB_POSTGRES_LOG_TABLE;
public String getDB_POSTGRES_LOG_TABLE(){
	return this.DB_POSTGRES_LOG_TABLE;
}
public String DB_POSTGRES_Login;
public String getDB_POSTGRES_Login(){
	return this.DB_POSTGRES_Login;
}
public String DB_POSTGRES_LOGTABLE;
public String getDB_POSTGRES_LOGTABLE(){
	return this.DB_POSTGRES_LOGTABLE;
}
public String DB_POSTGRES_LOGVIEW;
public String getDB_POSTGRES_LOGVIEW(){
	return this.DB_POSTGRES_LOGVIEW;
}
public String DB_POSTGRES_METADATA_FACT_RECUPERO_PARTIZIONI;
public String getDB_POSTGRES_METADATA_FACT_RECUPERO_PARTIZIONI(){
	return this.DB_POSTGRES_METADATA_FACT_RECUPERO_PARTIZIONI;
}
public String DB_POSTGRES_METADATA_FACT_TABLE;
public String getDB_POSTGRES_METADATA_FACT_TABLE(){
	return this.DB_POSTGRES_METADATA_FACT_TABLE;
}
public java.lang.String DB_POSTGRES_Password;
public java.lang.String getDB_POSTGRES_Password(){
	return this.DB_POSTGRES_Password;
}
public String DB_POSTGRES_Port;
public String getDB_POSTGRES_Port(){
	return this.DB_POSTGRES_Port;
}
public String DB_POSTGRES_Schema;
public String getDB_POSTGRES_Schema(){
	return this.DB_POSTGRES_Schema;
}
public String DB_POSTGRES_Server;
public String getDB_POSTGRES_Server(){
	return this.DB_POSTGRES_Server;
}
public String DB_POSTGRES_SQOOP_METADATA_TABLE;
public String getDB_POSTGRES_SQOOP_METADATA_TABLE(){
	return this.DB_POSTGRES_SQOOP_METADATA_TABLE;
}
public String DB_POSTGRES_STATTABLE;
public String getDB_POSTGRES_STATTABLE(){
	return this.DB_POSTGRES_STATTABLE;
}
public String EDH_CLUSTER_dfs_client_failover_proxy_provider_edhcluster;
public String getEDH_CLUSTER_dfs_client_failover_proxy_provider_edhcluster(){
	return this.EDH_CLUSTER_dfs_client_failover_proxy_provider_edhcluster;
}
public String EDH_CLUSTER_dfs_ha_namenodes_edhcluster;
public String getEDH_CLUSTER_dfs_ha_namenodes_edhcluster(){
	return this.EDH_CLUSTER_dfs_ha_namenodes_edhcluster;
}
public String EDH_CLUSTER_dfs_namenode_rpc_address_edhcluster_namenode209;
public String getEDH_CLUSTER_dfs_namenode_rpc_address_edhcluster_namenode209(){
	return this.EDH_CLUSTER_dfs_namenode_rpc_address_edhcluster_namenode209;
}
public String EDH_CLUSTER_dfs_namenode_rpc_address_edhcluster_namenode264;
public String getEDH_CLUSTER_dfs_namenode_rpc_address_edhcluster_namenode264(){
	return this.EDH_CLUSTER_dfs_namenode_rpc_address_edhcluster_namenode264;
}
public String EDH_CLUSTER_dfs_nameservices;
public String getEDH_CLUSTER_dfs_nameservices(){
	return this.EDH_CLUSTER_dfs_nameservices;
}
public String EDH_CLUSTER_ha_zookeeper_quorum;
public String getEDH_CLUSTER_ha_zookeeper_quorum(){
	return this.EDH_CLUSTER_ha_zookeeper_quorum;
}
public String EDH_CLUSTER_JobHistory;
public String getEDH_CLUSTER_JobHistory(){
	return this.EDH_CLUSTER_JobHistory;
}
public String EDH_CLUSTER_JobHistroyPrin;
public String getEDH_CLUSTER_JobHistroyPrin(){
	return this.EDH_CLUSTER_JobHistroyPrin;
}
public String EDH_CLUSTER_JTOrRMPrin;
public String getEDH_CLUSTER_JTOrRMPrin(){
	return this.EDH_CLUSTER_JTOrRMPrin;
}
public String EDH_CLUSTER_KeyTab;
public String getEDH_CLUSTER_KeyTab(){
	return this.EDH_CLUSTER_KeyTab;
}
public String EDH_CLUSTER_NameNodePrin;
public String getEDH_CLUSTER_NameNodePrin(){
	return this.EDH_CLUSTER_NameNodePrin;
}
public String EDH_CLUSTER_NameNodeUri;
public String getEDH_CLUSTER_NameNodeUri(){
	return this.EDH_CLUSTER_NameNodeUri;
}
public String EDH_CLUSTER_Principal;
public String getEDH_CLUSTER_Principal(){
	return this.EDH_CLUSTER_Principal;
}
public String EDH_CLUSTER_ResourceManager;
public String getEDH_CLUSTER_ResourceManager(){
	return this.EDH_CLUSTER_ResourceManager;
}
public String EDH_CLUSTER_ResourceManagerScheduler;
public String getEDH_CLUSTER_ResourceManagerScheduler(){
	return this.EDH_CLUSTER_ResourceManagerScheduler;
}
public String EDH_CLUSTER_StagingDirectory;
public String getEDH_CLUSTER_StagingDirectory(){
	return this.EDH_CLUSTER_StagingDirectory;
}
public String EDH_CLUSTER_username;
public String getEDH_CLUSTER_username(){
	return this.EDH_CLUSTER_username;
}
public String EDH_CLUSTER_yarn_resourcemanager_address_rm1;
public String getEDH_CLUSTER_yarn_resourcemanager_address_rm1(){
	return this.EDH_CLUSTER_yarn_resourcemanager_address_rm1;
}
public String EDH_CLUSTER_yarn_resourcemanager_address_rm2;
public String getEDH_CLUSTER_yarn_resourcemanager_address_rm2(){
	return this.EDH_CLUSTER_yarn_resourcemanager_address_rm2;
}
public String EDH_CLUSTER_yarn_resourcemanager_ha_enabled;
public String getEDH_CLUSTER_yarn_resourcemanager_ha_enabled(){
	return this.EDH_CLUSTER_yarn_resourcemanager_ha_enabled;
}
public String EDH_CLUSTER_yarn_resourcemanager_ha_rm_ids;
public String getEDH_CLUSTER_yarn_resourcemanager_ha_rm_ids(){
	return this.EDH_CLUSTER_yarn_resourcemanager_ha_rm_ids;
}
public String EDH_CLUSTER_yarn_resourcemanager_scheduler_address_rm1;
public String getEDH_CLUSTER_yarn_resourcemanager_scheduler_address_rm1(){
	return this.EDH_CLUSTER_yarn_resourcemanager_scheduler_address_rm1;
}
public String EDH_CLUSTER_yarn_resourcemanager_scheduler_address_rm2;
public String getEDH_CLUSTER_yarn_resourcemanager_scheduler_address_rm2(){
	return this.EDH_CLUSTER_yarn_resourcemanager_scheduler_address_rm2;
}
public String EDH_CLUSTER_HDFS_HdfsFileSeparator;
public String getEDH_CLUSTER_HDFS_HdfsFileSeparator(){
	return this.EDH_CLUSTER_HDFS_HdfsFileSeparator;
}
public String EDH_CLUSTER_HDFS_HdfsRowSeparator;
public String getEDH_CLUSTER_HDFS_HdfsRowSeparator(){
	return this.EDH_CLUSTER_HDFS_HdfsRowSeparator;
}
public String EDH_CLUSTER_HIVE_Database;
public String getEDH_CLUSTER_HIVE_Database(){
	return this.EDH_CLUSTER_HIVE_Database;
}
public String EDH_CLUSTER_HIVE_dynamicPart;
public String getEDH_CLUSTER_HIVE_dynamicPart(){
	return this.EDH_CLUSTER_HIVE_dynamicPart;
}
public String EDH_CLUSTER_HIVE_dynamicPartMax;
public String getEDH_CLUSTER_HIVE_dynamicPartMax(){
	return this.EDH_CLUSTER_HIVE_dynamicPartMax;
}
public String EDH_CLUSTER_HIVE_dynamicPartMaxPerNode;
public String getEDH_CLUSTER_HIVE_dynamicPartMaxPerNode(){
	return this.EDH_CLUSTER_HIVE_dynamicPartMaxPerNode;
}
public String EDH_CLUSTER_HIVE_executionEngine;
public String getEDH_CLUSTER_HIVE_executionEngine(){
	return this.EDH_CLUSTER_HIVE_executionEngine;
}
public String EDH_CLUSTER_HIVE_hiveAdditionalJDBCParameters;
public String getEDH_CLUSTER_HIVE_hiveAdditionalJDBCParameters(){
	return this.EDH_CLUSTER_HIVE_hiveAdditionalJDBCParameters;
}
public String EDH_CLUSTER_HIVE_HiveKeyTab;
public String getEDH_CLUSTER_HIVE_HiveKeyTab(){
	return this.EDH_CLUSTER_HIVE_HiveKeyTab;
}
public String EDH_CLUSTER_HIVE_HiveKeyTabPrincipal;
public String getEDH_CLUSTER_HIVE_HiveKeyTabPrincipal(){
	return this.EDH_CLUSTER_HIVE_HiveKeyTabPrincipal;
}
public String EDH_CLUSTER_HIVE_HivePrincipal;
public String getEDH_CLUSTER_HIVE_HivePrincipal(){
	return this.EDH_CLUSTER_HIVE_HivePrincipal;
}
public java.lang.String EDH_CLUSTER_HIVE_hiveSSLTrustStorePassword;
public java.lang.String getEDH_CLUSTER_HIVE_hiveSSLTrustStorePassword(){
	return this.EDH_CLUSTER_HIVE_hiveSSLTrustStorePassword;
}
public String EDH_CLUSTER_HIVE_hiveSSLTrustStorePath;
public String getEDH_CLUSTER_HIVE_hiveSSLTrustStorePath(){
	return this.EDH_CLUSTER_HIVE_hiveSSLTrustStorePath;
}
public String EDH_CLUSTER_HIVE_Login;
public String getEDH_CLUSTER_HIVE_Login(){
	return this.EDH_CLUSTER_HIVE_Login;
}
public java.lang.String EDH_CLUSTER_HIVE_Password;
public java.lang.String getEDH_CLUSTER_HIVE_Password(){
	return this.EDH_CLUSTER_HIVE_Password;
}
public String EDH_CLUSTER_HIVE_Port;
public String getEDH_CLUSTER_HIVE_Port(){
	return this.EDH_CLUSTER_HIVE_Port;
}
public String EDH_CLUSTER_HIVE_Server;
public String getEDH_CLUSTER_HIVE_Server(){
	return this.EDH_CLUSTER_HIVE_Server;
}
public String EDH_CLUSTER_IMPALA_Database;
public String getEDH_CLUSTER_IMPALA_Database(){
	return this.EDH_CLUSTER_IMPALA_Database;
}
public String EDH_CLUSTER_IMPALA_ImpalaPrincipal;
public String getEDH_CLUSTER_IMPALA_ImpalaPrincipal(){
	return this.EDH_CLUSTER_IMPALA_ImpalaPrincipal;
}
public String EDH_CLUSTER_IMPALA_Login;
public String getEDH_CLUSTER_IMPALA_Login(){
	return this.EDH_CLUSTER_IMPALA_Login;
}
public String EDH_CLUSTER_IMPALA_Port;
public String getEDH_CLUSTER_IMPALA_Port(){
	return this.EDH_CLUSTER_IMPALA_Port;
}
public String EDH_CLUSTER_IMPALA_Server;
public String getEDH_CLUSTER_IMPALA_Server(){
	return this.EDH_CLUSTER_IMPALA_Server;
}
public String DB_POSTGRES_configuration_metadata;
public String getDB_POSTGRES_configuration_metadata(){
	return this.DB_POSTGRES_configuration_metadata;
}
	}
	protected ContextProperties context = new ContextProperties(); // will be instanciated by MS.
	public ContextProperties getContext() {
		return this.context;
	}
	private final String jobVersion = "0.1";
	private final String jobName = "S_INGESTION_IMPORT_FILE_1";
	private final String projectName = "DATAHUB_AML";
	public Integer errorCode = null;
	private String currentComponent = "";
	
		private final java.util.Map<String, Object> globalMap = new java.util.HashMap<String, Object>();
        private final static java.util.Map<String, Object> junitGlobalMap = new java.util.HashMap<String, Object>();
	
		private final java.util.Map<String, Long> start_Hash = new java.util.HashMap<String, Long>();
		private final java.util.Map<String, Long> end_Hash = new java.util.HashMap<String, Long>();
		private final java.util.Map<String, Boolean> ok_Hash = new java.util.HashMap<String, Boolean>();
		public  final java.util.List<String[]> globalBuffer = new java.util.ArrayList<String[]>();
	

private final JobStructureCatcherUtils talendJobLog = new JobStructureCatcherUtils(jobName, "_DPuAkE3TEeynKZ4FBgAbDA", "0.1");
private org.talend.job.audit.JobAuditLogger auditLogger_talendJobLog = null;

private RunStat runStat = new RunStat(talendJobLog, System.getProperty("audit.interval"));

	// OSGi DataSource
	private final static String KEY_DB_DATASOURCES = "KEY_DB_DATASOURCES";
	
	private final static String KEY_DB_DATASOURCES_RAW = "KEY_DB_DATASOURCES_RAW";

	public void setDataSources(java.util.Map<String, javax.sql.DataSource> dataSources) {
		java.util.Map<String, routines.system.TalendDataSource> talendDataSources = new java.util.HashMap<String, routines.system.TalendDataSource>();
		for (java.util.Map.Entry<String, javax.sql.DataSource> dataSourceEntry : dataSources.entrySet()) {
			talendDataSources.put(dataSourceEntry.getKey(), new routines.system.TalendDataSource(dataSourceEntry.getValue()));
		}
		globalMap.put(KEY_DB_DATASOURCES, talendDataSources);
		globalMap.put(KEY_DB_DATASOURCES_RAW, new java.util.HashMap<String, javax.sql.DataSource>(dataSources));
	}
	
	public void setDataSourceReferences(List serviceReferences) throws Exception{
		
		java.util.Map<String, routines.system.TalendDataSource> talendDataSources = new java.util.HashMap<String, routines.system.TalendDataSource>();
		java.util.Map<String, javax.sql.DataSource> dataSources = new java.util.HashMap<String, javax.sql.DataSource>();
		
		for (java.util.Map.Entry<String, javax.sql.DataSource> entry : BundleUtils.getServices(serviceReferences,  javax.sql.DataSource.class).entrySet()) {
                    dataSources.put(entry.getKey(), entry.getValue());
                    talendDataSources.put(entry.getKey(), new routines.system.TalendDataSource(entry.getValue()));
		}

		globalMap.put(KEY_DB_DATASOURCES, talendDataSources);
		globalMap.put(KEY_DB_DATASOURCES_RAW, new java.util.HashMap<String, javax.sql.DataSource>(dataSources));
	}


private final java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
private final java.io.PrintStream errorMessagePS = new java.io.PrintStream(new java.io.BufferedOutputStream(baos));

public String getExceptionStackTrace() {
	if ("failure".equals(this.getStatus())) {
		errorMessagePS.flush();
		return baos.toString();
	}
	return null;
}

private Exception exception;

public Exception getException() {
	if ("failure".equals(this.getStatus())) {
		return this.exception;
	}
	return null;
}

private class TalendException extends Exception {

	private static final long serialVersionUID = 1L;

	private java.util.Map<String, Object> globalMap = null;
	private Exception e = null;
	private String currentComponent = null;
	private String virtualComponentName = null;
	
	public void setVirtualComponentName (String virtualComponentName){
		this.virtualComponentName = virtualComponentName;
	}

	private TalendException(Exception e, String errorComponent, final java.util.Map<String, Object> globalMap) {
		this.currentComponent= errorComponent;
		this.globalMap = globalMap;
		this.e = e;
	}

	public Exception getException() {
		return this.e;
	}

	public String getCurrentComponent() {
		return this.currentComponent;
	}

	
    public String getExceptionCauseMessage(Exception e){
        Throwable cause = e;
        String message = null;
        int i = 10;
        while (null != cause && 0 < i--) {
            message = cause.getMessage();
            if (null == message) {
                cause = cause.getCause();
            } else {
                break;          
            }
        }
        if (null == message) {
            message = e.getClass().getName();
        }   
        return message;
    }

	@Override
	public void printStackTrace() {
		if (!(e instanceof TalendException || e instanceof TDieException)) {
			if(virtualComponentName!=null && currentComponent.indexOf(virtualComponentName+"_")==0){
				globalMap.put(virtualComponentName+"_ERROR_MESSAGE",getExceptionCauseMessage(e));
			}
			globalMap.put(currentComponent+"_ERROR_MESSAGE",getExceptionCauseMessage(e));
			System.err.println("Exception in component " + currentComponent + " (" + jobName + ")");
		}
		if (!(e instanceof TDieException)) {
			if(e instanceof TalendException){
				e.printStackTrace();
			} else {
				e.printStackTrace();
				e.printStackTrace(errorMessagePS);
				S_INGESTION_IMPORT_FILE_1.this.exception = e;
			}
		}
		if (!(e instanceof TalendException)) {
		try {
			for (java.lang.reflect.Method m : this.getClass().getEnclosingClass().getMethods()) {
				if (m.getName().compareTo(currentComponent + "_error") == 0) {
					m.invoke(S_INGESTION_IMPORT_FILE_1.this, new Object[] { e , currentComponent, globalMap});
					break;
				}
			}

			if(!(e instanceof TDieException)){
			}
		} catch (Exception e) {
			this.e.printStackTrace();
		}
		}
	}
}

			public void Implicit_Context_Regex_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
							Implicit_Context_Context_error(exception, errorComponent, globalMap);
						
						}
					
			public void Implicit_Context_Context_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					Implicit_Context_Regex_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tImpalaConnection_1_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tImpalaConnection_1_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tHiveConnection_1_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tHiveConnection_1_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tHDFSConnection_1_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tHDFSConnection_1_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tJava_3_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tJava_3_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tHDFSDelete_1_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tHDFSDelete_1_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tFixedFlowInput_1_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tFixedFlowInput_1_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tHDFSOutput_1_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tFixedFlowInput_1_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tFileList_1_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tFileList_1_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tRunJob_1_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tFileList_1_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tJava_1_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tJava_1_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tHiveRow_1_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tHiveRow_1_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tImpalaRow_1_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tImpalaRow_1_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void talendJobLog_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					talendJobLog_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void Implicit_Context_Regex_onSubJobError(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {

resumeUtil.addLog("SYSTEM_LOG", "NODE:"+ errorComponent, "", Thread.currentThread().getId()+ "", "FATAL", "", exception.getMessage(), ResumeUtil.getExceptionStackTrace(exception),"");

			}
			public void tImpalaConnection_1_onSubJobError(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {

resumeUtil.addLog("SYSTEM_LOG", "NODE:"+ errorComponent, "", Thread.currentThread().getId()+ "", "FATAL", "", exception.getMessage(), ResumeUtil.getExceptionStackTrace(exception),"");

			}
			public void tHiveConnection_1_onSubJobError(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {

resumeUtil.addLog("SYSTEM_LOG", "NODE:"+ errorComponent, "", Thread.currentThread().getId()+ "", "FATAL", "", exception.getMessage(), ResumeUtil.getExceptionStackTrace(exception),"");

			}
			public void tHDFSConnection_1_onSubJobError(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {

resumeUtil.addLog("SYSTEM_LOG", "NODE:"+ errorComponent, "", Thread.currentThread().getId()+ "", "FATAL", "", exception.getMessage(), ResumeUtil.getExceptionStackTrace(exception),"");

			}
			public void tJava_3_onSubJobError(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {

resumeUtil.addLog("SYSTEM_LOG", "NODE:"+ errorComponent, "", Thread.currentThread().getId()+ "", "FATAL", "", exception.getMessage(), ResumeUtil.getExceptionStackTrace(exception),"");

			}
			public void tHDFSDelete_1_onSubJobError(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {

resumeUtil.addLog("SYSTEM_LOG", "NODE:"+ errorComponent, "", Thread.currentThread().getId()+ "", "FATAL", "", exception.getMessage(), ResumeUtil.getExceptionStackTrace(exception),"");

			}
			public void tFixedFlowInput_1_onSubJobError(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {

resumeUtil.addLog("SYSTEM_LOG", "NODE:"+ errorComponent, "", Thread.currentThread().getId()+ "", "FATAL", "", exception.getMessage(), ResumeUtil.getExceptionStackTrace(exception),"");

			}
			public void tFileList_1_onSubJobError(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {

resumeUtil.addLog("SYSTEM_LOG", "NODE:"+ errorComponent, "", Thread.currentThread().getId()+ "", "FATAL", "", exception.getMessage(), ResumeUtil.getExceptionStackTrace(exception),"");

			}
			public void tJava_1_onSubJobError(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {

resumeUtil.addLog("SYSTEM_LOG", "NODE:"+ errorComponent, "", Thread.currentThread().getId()+ "", "FATAL", "", exception.getMessage(), ResumeUtil.getExceptionStackTrace(exception),"");

			}
			public void tHiveRow_1_onSubJobError(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {

resumeUtil.addLog("SYSTEM_LOG", "NODE:"+ errorComponent, "", Thread.currentThread().getId()+ "", "FATAL", "", exception.getMessage(), ResumeUtil.getExceptionStackTrace(exception),"");

			}
			public void tImpalaRow_1_onSubJobError(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {

resumeUtil.addLog("SYSTEM_LOG", "NODE:"+ errorComponent, "", Thread.currentThread().getId()+ "", "FATAL", "", exception.getMessage(), ResumeUtil.getExceptionStackTrace(exception),"");

			}
			public void talendJobLog_onSubJobError(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {

resumeUtil.addLog("SYSTEM_LOG", "NODE:"+ errorComponent, "", Thread.currentThread().getId()+ "", "FATAL", "", exception.getMessage(), ResumeUtil.getExceptionStackTrace(exception),"");

			}
	






	

public static class row_Implicit_Context_RegexStruct implements routines.system.IPersistableRow<row_Implicit_Context_RegexStruct> {
    final static byte[] commonByteArrayLock_DATAHUB_AML_S_INGESTION_IMPORT_FILE_1 = new byte[0];
    static byte[] commonByteArray_DATAHUB_AML_S_INGESTION_IMPORT_FILE_1 = new byte[0];

	
			    public String key;

				public String getKey () {
					return this.key;
				}

				public Boolean keyIsNullable(){
				    return true;
				}
				public Boolean keyIsKey(){
				    return false;
				}
				public Integer keyLength(){
				    return 255;
				}
				public Integer keyPrecision(){
				    return 0;
				}
				public String keyDefault(){
				
					return "";
				
				}
				public String keyComment(){
				
				    return null;
				
				}
				public String keyPattern(){
				
				    return null;
				
				}
				public String keyOriginalDbColumnName(){
				
					return "key";
				
				}

				
			    public String value;

				public String getValue () {
					return this.value;
				}

				public Boolean valueIsNullable(){
				    return true;
				}
				public Boolean valueIsKey(){
				    return false;
				}
				public Integer valueLength(){
				    return 255;
				}
				public Integer valuePrecision(){
				    return 0;
				}
				public String valueDefault(){
				
					return "";
				
				}
				public String valueComment(){
				
				    return null;
				
				}
				public String valuePattern(){
				
				    return null;
				
				}
				public String valueOriginalDbColumnName(){
				
					return "value";
				
				}

				



	private String readString(ObjectInputStream dis) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_DATAHUB_AML_S_INGESTION_IMPORT_FILE_1.length) {
				if(length < 1024 && commonByteArray_DATAHUB_AML_S_INGESTION_IMPORT_FILE_1.length == 0) {
   					commonByteArray_DATAHUB_AML_S_INGESTION_IMPORT_FILE_1 = new byte[1024];
				} else {
   					commonByteArray_DATAHUB_AML_S_INGESTION_IMPORT_FILE_1 = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_DATAHUB_AML_S_INGESTION_IMPORT_FILE_1, 0, length);
			strReturn = new String(commonByteArray_DATAHUB_AML_S_INGESTION_IMPORT_FILE_1, 0, length, utf8Charset);
		}
		return strReturn;
	}
	
	private String readString(org.jboss.marshalling.Unmarshaller unmarshaller) throws IOException{
		String strReturn = null;
		int length = 0;
        length = unmarshaller.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_DATAHUB_AML_S_INGESTION_IMPORT_FILE_1.length) {
				if(length < 1024 && commonByteArray_DATAHUB_AML_S_INGESTION_IMPORT_FILE_1.length == 0) {
   					commonByteArray_DATAHUB_AML_S_INGESTION_IMPORT_FILE_1 = new byte[1024];
				} else {
   					commonByteArray_DATAHUB_AML_S_INGESTION_IMPORT_FILE_1 = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_DATAHUB_AML_S_INGESTION_IMPORT_FILE_1, 0, length);
			strReturn = new String(commonByteArray_DATAHUB_AML_S_INGESTION_IMPORT_FILE_1, 0, length, utf8Charset);
		}
		return strReturn;
	}

    private void writeString(String str, ObjectOutputStream dos) throws IOException{
		if(str == null) {
            dos.writeInt(-1);
		} else {
            byte[] byteArray = str.getBytes(utf8Charset);
	    	dos.writeInt(byteArray.length);
			dos.write(byteArray);
    	}
    }
    
    private void writeString(String str, org.jboss.marshalling.Marshaller marshaller) throws IOException{
		if(str == null) {
			marshaller.writeInt(-1);
		} else {
            byte[] byteArray = str.getBytes(utf8Charset);
            marshaller.writeInt(byteArray.length);
            marshaller.write(byteArray);
    	}
    }

    public void readData(ObjectInputStream dis) {

		synchronized(commonByteArrayLock_DATAHUB_AML_S_INGESTION_IMPORT_FILE_1) {

        	try {

        		int length = 0;
		
					this.key = readString(dis);
					
					this.value = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_DATAHUB_AML_S_INGESTION_IMPORT_FILE_1) {

        	try {

        		int length = 0;
		
					this.key = readString(dis);
					
					this.value = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }

    public void writeData(ObjectOutputStream dos) {
        try {

		
					// String
				
						writeString(this.key,dos);
					
					// String
				
						writeString(this.value,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }
    
    public void writeData(org.jboss.marshalling.Marshaller dos) {
        try {

		
					// String
				
						writeString(this.key,dos);
					
					// String
				
						writeString(this.value,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }


    public String toString() {

		StringBuilder sb = new StringBuilder();
		sb.append(super.toString());
		sb.append("[");
		sb.append("key="+key);
		sb.append(",value="+value);
	    sb.append("]");

	    return sb.toString();
    }
        public String toLogString(){
        	StringBuilder sb = new StringBuilder();
        	
        				if(key == null){
        					sb.append("<null>");
        				}else{
            				sb.append(key);
            			}
            		
        			sb.append("|");
        		
        				if(value == null){
        					sb.append("<null>");
        				}else{
            				sb.append(value);
            			}
            		
        			sb.append("|");
        		
        	return sb.toString();
        }

    /**
     * Compare keys
     */
    public int compareTo(row_Implicit_Context_RegexStruct other) {

		int returnValue = -1;
		
	    return returnValue;
    }


    private int checkNullsAndCompare(Object object1, Object object2) {
        int returnValue = 0;
		if (object1 instanceof Comparable && object2 instanceof Comparable) {
            returnValue = ((Comparable) object1).compareTo(object2);
        } else if (object1 != null && object2 != null) {
            returnValue = compareStrings(object1.toString(), object2.toString());
        } else if (object1 == null && object2 != null) {
            returnValue = 1;
        } else if (object1 != null && object2 == null) {
            returnValue = -1;
        } else {
            returnValue = 0;
        }

        return returnValue;
    }

    private int compareStrings(String string1, String string2) {
        return string1.compareTo(string2);
    }


}
public void Implicit_Context_RegexProcess(final java.util.Map<String, Object> globalMap) throws TalendException {
	globalMap.put("Implicit_Context_Regex_SUBPROCESS_STATE", 0);

 final boolean execStat = this.execStat;
		String currentVirtualComponent = null;
	
		String iterateId = "";
	
	
	String currentComponent = "";
	java.util.Map<String, Object> resourceMap = new java.util.HashMap<String, Object>();

	try {
			// TDI-39566 avoid throwing an useless Exception
			boolean resumeIt = true;
			if (globalResumeTicket == false && resumeEntryMethodName != null) {
				String currentMethodName = new java.lang.Exception().getStackTrace()[0].getMethodName();
				resumeIt = resumeEntryMethodName.equals(currentMethodName);
			}
			if (resumeIt || globalResumeTicket) { //start the resume
				globalResumeTicket = true;



		row_Implicit_Context_RegexStruct row_Implicit_Context_Regex = new row_Implicit_Context_RegexStruct();




	
	/**
	 * [Implicit_Context_Context begin ] start
	 */

	

	
		
		ok_Hash.put("Implicit_Context_Context", false);
		start_Hash.put("Implicit_Context_Context", System.currentTimeMillis());
		
	
		currentVirtualComponent = "Implicit_Context_Context";
	
	currentComponent="Implicit_Context_Context";

	
			runStat.updateStatAndLog(execStat,enableLogStash,resourceMap,iterateId,0,0,"Main");
			
		int tos_count_Implicit_Context_Context = 0;
		
                if(log.isDebugEnabled())
            log.debug("Implicit_Context_Context - "  + ("Start to work.") );
            if (log.isDebugEnabled()) {
                class BytesLimit65535_Implicit_Context_Context{
                    public void limitLog4jByte() throws Exception{
                    StringBuilder log4jParamters_Implicit_Context_Context = new StringBuilder();
                    log4jParamters_Implicit_Context_Context.append("Parameters:");
                            log4jParamters_Implicit_Context_Context.append("LOAD_NEW_VARIABLE" + " = " + "Warning");
                        log4jParamters_Implicit_Context_Context.append(" | ");
                            log4jParamters_Implicit_Context_Context.append("NOT_LOAD_OLD_VARIABLE" + " = " + "Warning");
                        log4jParamters_Implicit_Context_Context.append(" | ");
                            log4jParamters_Implicit_Context_Context.append("PRINT_OPERATIONS" + " = " + "false");
                        log4jParamters_Implicit_Context_Context.append(" | ");
                            log4jParamters_Implicit_Context_Context.append("DISABLE_ERROR" + " = " + "false");
                        log4jParamters_Implicit_Context_Context.append(" | ");
                            log4jParamters_Implicit_Context_Context.append("DISABLE_WARNINGS" + " = " + "false");
                        log4jParamters_Implicit_Context_Context.append(" | ");
                            log4jParamters_Implicit_Context_Context.append("DISABLE_INFO" + " = " + "false");
                        log4jParamters_Implicit_Context_Context.append(" | ");
                            log4jParamters_Implicit_Context_Context.append("DIEONERROR" + " = " + "false");
                        log4jParamters_Implicit_Context_Context.append(" | ");
                if(log.isDebugEnabled())
            log.debug("Implicit_Context_Context - "  + (log4jParamters_Implicit_Context_Context) );
                    } 
                } 
            new BytesLimit65535_Implicit_Context_Context().limitLog4jByte();
            }
			if(enableLogStash) {
				talendJobLog.addCM("Implicit_Context_Context", "Implicit_Context_Context", "tContextLoad");
				talendJobLogProcess(globalMap);
			}
			
	java.util.List<String> assignList_Implicit_Context_Context = new java.util.ArrayList<String>();
	java.util.List<String> newPropertyList_Implicit_Context_Context = new java.util.ArrayList<String>();
	java.util.List<String> noAssignList_Implicit_Context_Context = new java.util.ArrayList<String>();
	int nb_line_Implicit_Context_Context = 0;

 



/**
 * [Implicit_Context_Context begin ] stop
 */



	
	/**
	 * [Implicit_Context_Regex begin ] start
	 */

	

	
		
		ok_Hash.put("Implicit_Context_Regex", false);
		start_Hash.put("Implicit_Context_Regex", System.currentTimeMillis());
		
	
		currentVirtualComponent = "Implicit_Context_Regex";
	
	currentComponent="Implicit_Context_Regex";

	
		int tos_count_Implicit_Context_Regex = 0;
		
                if(log.isDebugEnabled())
            log.debug("Implicit_Context_Regex - "  + ("Start to work.") );
            if (log.isDebugEnabled()) {
                class BytesLimit65535_Implicit_Context_Regex{
                    public void limitLog4jByte() throws Exception{
                    StringBuilder log4jParamters_Implicit_Context_Regex = new StringBuilder();
                    log4jParamters_Implicit_Context_Regex.append("Parameters:");
                            log4jParamters_Implicit_Context_Regex.append("FILENAME" + " = " + "\"/talend/properties/DB_CONTEXT/Context/DB_CONTEXT.csv\"");
                        log4jParamters_Implicit_Context_Regex.append(" | ");
                            log4jParamters_Implicit_Context_Regex.append("ROWSEPARATOR" + " = " + "\"\\n\"");
                        log4jParamters_Implicit_Context_Regex.append(" | ");
                            log4jParamters_Implicit_Context_Regex.append("REGEX" + " = " + "\"^([^\"+\";\"+\"]*)\"+\";\"+\"(.*)$\"");
                        log4jParamters_Implicit_Context_Regex.append(" | ");
                            log4jParamters_Implicit_Context_Regex.append("HEADER" + " = " + "0");
                        log4jParamters_Implicit_Context_Regex.append(" | ");
                            log4jParamters_Implicit_Context_Regex.append("FOOTER" + " = " + "0");
                        log4jParamters_Implicit_Context_Regex.append(" | ");
                            log4jParamters_Implicit_Context_Regex.append("LIMIT" + " = " + "");
                        log4jParamters_Implicit_Context_Regex.append(" | ");
                            log4jParamters_Implicit_Context_Regex.append("IGNORE_ERROR_MESSAGE" + " = " + "true");
                        log4jParamters_Implicit_Context_Regex.append(" | ");
                            log4jParamters_Implicit_Context_Regex.append("REMOVE_EMPTY_ROW" + " = " + "true");
                        log4jParamters_Implicit_Context_Regex.append(" | ");
                            log4jParamters_Implicit_Context_Regex.append("DIE_ON_ERROR" + " = " + "false");
                        log4jParamters_Implicit_Context_Regex.append(" | ");
                            log4jParamters_Implicit_Context_Regex.append("ENCODING" + " = " + "\"ISO-8859-15\"");
                        log4jParamters_Implicit_Context_Regex.append(" | ");
                if(log.isDebugEnabled())
            log.debug("Implicit_Context_Regex - "  + (log4jParamters_Implicit_Context_Regex) );
                    } 
                } 
            new BytesLimit65535_Implicit_Context_Regex().limitLog4jByte();
            }
			if(enableLogStash) {
				talendJobLog.addCM("Implicit_Context_Regex", "Implicit_Context_Regex", "tFileInputRegex");
				talendJobLogProcess(globalMap);
			}
			

	
	
				final StringBuffer log4jSb_Implicit_Context_Regex = new StringBuffer();
			
		int nb_line_Implicit_Context_Regex = 0;
        
        int footer_Implicit_Context_Regex  = 0;
        boolean removeEmptyRowImplicit_Context_Regex = true;
        Object source_Implicit_Context_Regex = /** Start field Implicit_Context_Regex:FILENAME */"/talend/properties/DB_CONTEXT/Context/DB_CONTEXT.csv"/** End field Implicit_Context_Regex:FILENAME */;
        
        org.talend.fileprocess.TOSDelimitedReader inImplicit_Context_Regex=null;
        if(source_Implicit_Context_Regex instanceof String || source_Implicit_Context_Regex instanceof java.io.InputStream){
        	inImplicit_Context_Regex = new org.talend.fileprocess.TOSDelimitedReader(/** Start field Implicit_Context_Regex:FILENAME */"/talend/properties/DB_CONTEXT/Context/DB_CONTEXT.csv"/** End field Implicit_Context_Regex:FILENAME */, "ISO-8859-15", "", "\n", removeEmptyRowImplicit_Context_Regex);
        }else{
        	throw new java.lang.Exception("The source data should be specified as File Path or InputStream or java.io.Reader!");
        }
        String strImplicit_Context_Regex;
        int totalLineImplicit_Context_Regex=0,currentLineImplicit_Context_Regex=0,beginLineImplicit_Context_Regex=0,lastLineImplicit_Context_Regex=-1,validRowCountImplicit_Context_Regex=0;
        int limitImplicit_Context_Regex=-1;
        
		int headerImplicit_Context_Regex = 0;
		if(headerImplicit_Context_Regex > 0){
			beginLineImplicit_Context_Regex=headerImplicit_Context_Regex+1;
		}
    	
        if(footer_Implicit_Context_Regex > 0){
			while (inImplicit_Context_Regex.readRecord()) {
                strImplicit_Context_Regex =inImplicit_Context_Regex.getRowRecord();        
				totalLineImplicit_Context_Regex++;
			}
			int lastLineTempImplicit_Context_Regex = totalLineImplicit_Context_Regex - footer_Implicit_Context_Regex   < 0? 0 : totalLineImplicit_Context_Regex - footer_Implicit_Context_Regex ;
			if(lastLineImplicit_Context_Regex > 0){
				lastLineImplicit_Context_Regex = lastLineImplicit_Context_Regex < lastLineTempImplicit_Context_Regex ? lastLineImplicit_Context_Regex : lastLineTempImplicit_Context_Regex; 
			}else {
				lastLineImplicit_Context_Regex = lastLineTempImplicit_Context_Regex;
			}
		  	inImplicit_Context_Regex.close();
        	inImplicit_Context_Regex = new org.talend.fileprocess.TOSDelimitedReader(/** Start field Implicit_Context_Regex:FILENAME */"/talend/properties/DB_CONTEXT/Context/DB_CONTEXT.csv"/** End field Implicit_Context_Regex:FILENAME */, "ISO-8859-15", "", "\n", removeEmptyRowImplicit_Context_Regex);
		}
        java.util.StringTokenizer strTokenImplicit_Context_Regex;
        java.util.regex.Pattern patternImplicit_Context_Regex = java.util.regex.Pattern.compile("^([^"+";"+"]*)"+";"+"(.*)$");
        java.util.regex.Matcher matcherImplicit_Context_Regex = null;
        
				log.debug("Implicit_Context_Regex - Retrieving records from the datasource.");
			
        
        while (inImplicit_Context_Regex.readRecord()) {
            strImplicit_Context_Regex =inImplicit_Context_Regex.getRowRecord(); 
        	
       		currentLineImplicit_Context_Regex++;
        	if(currentLineImplicit_Context_Regex < beginLineImplicit_Context_Regex) {
        		continue;
        	}
        	if(lastLineImplicit_Context_Regex > -1 && currentLineImplicit_Context_Regex > lastLineImplicit_Context_Regex) {
        		break;
        	}
        	if(removeEmptyRowImplicit_Context_Regex && ("").equals(strImplicit_Context_Regex)){
        		continue;
        	}
        	if(limitImplicit_Context_Regex!=-1&& validRowCountImplicit_Context_Regex >= limitImplicit_Context_Regex){
        		break;
        	}
        	
        	matcherImplicit_Context_Regex = patternImplicit_Context_Regex.matcher(strImplicit_Context_Regex);
        	int groupCountImplicit_Context_Regex = 0;
        	boolean isMatchImplicit_Context_Regex = matcherImplicit_Context_Regex.find(); 
        	if(isMatchImplicit_Context_Regex){
        	groupCountImplicit_Context_Regex=matcherImplicit_Context_Regex.groupCount();
			}
    		row_Implicit_Context_Regex = null;						
			
			boolean lineIsEmptyImplicit_Context_Regex = strImplicit_Context_Regex.length() == 0;
			
			String[] valueImplicit_Context_Regex = new String[2];
			String frontCharImplicit_Context_Regex,behindCharImplicit_Context_Regex;
			for(int i=0;i<2;i++){
				valueImplicit_Context_Regex[i] = "";
				if(lineIsEmptyImplicit_Context_Regex){
					continue;
				}
				if(i < groupCountImplicit_Context_Regex){
					valueImplicit_Context_Regex[i] = matcherImplicit_Context_Regex.group(i+1);
				}
			}
			validRowCountImplicit_Context_Regex++;
			
			boolean whetherReject_Implicit_Context_Regex = false;
			row_Implicit_Context_Regex = new row_Implicit_Context_RegexStruct();
			try {			
			if(!isMatchImplicit_Context_Regex){//line data not matched with given regex parameter
        		throw new java.lang.Exception("Line doesn't match: " + strImplicit_Context_Regex);
        	}
								
						if(valueImplicit_Context_Regex[0]!=null && valueImplicit_Context_Regex[0].length() > 0) {
							row_Implicit_Context_Regex.key = valueImplicit_Context_Regex[0];					
						} else {
						row_Implicit_Context_Regex.key = "";}
						
						if(valueImplicit_Context_Regex[1]!=null && valueImplicit_Context_Regex[1].length() > 0) {
							row_Implicit_Context_Regex.value = valueImplicit_Context_Regex[1];					
						} else {
						row_Implicit_Context_Regex.value = "";}
	
				log.debug("Implicit_Context_Regex - Retrieving the record " + (nb_line_Implicit_Context_Regex+1) + ".");
			
										
					
    } catch (java.lang.Exception e) {
globalMap.put("Implicit_Context_Regex_ERROR_MESSAGE",e.getMessage());
        whetherReject_Implicit_Context_Regex = true;
                row_Implicit_Context_Regex = null;
    }					
					
					
			
			nb_line_Implicit_Context_Regex++;

 



/**
 * [Implicit_Context_Regex begin ] stop
 */
	
	/**
	 * [Implicit_Context_Regex main ] start
	 */

	

	
	
		currentVirtualComponent = "Implicit_Context_Regex";
	
	currentComponent="Implicit_Context_Regex";

	

 


	tos_count_Implicit_Context_Regex++;

/**
 * [Implicit_Context_Regex main ] stop
 */
	
	/**
	 * [Implicit_Context_Regex process_data_begin ] start
	 */

	

	
	
		currentVirtualComponent = "Implicit_Context_Regex";
	
	currentComponent="Implicit_Context_Regex";

	

 



/**
 * [Implicit_Context_Regex process_data_begin ] stop
 */
// Start of branch "row_Implicit_Context_Regex"
if(row_Implicit_Context_Regex != null) { 



	
	/**
	 * [Implicit_Context_Context main ] start
	 */

	

	
	
		currentVirtualComponent = "Implicit_Context_Context";
	
	currentComponent="Implicit_Context_Context";

	
			if(runStat.update(execStat,enableLogStash,iterateId,1,1
				
					,"Main","Implicit_Context_Regex","Implicit_Context_Regex","tFileInputRegex","Implicit_Context_Context","Implicit_Context_Context","tContextLoad"
				
			)) {
				talendJobLogProcess(globalMap);
			}
			
        //////////////////////////
        String tmp_key_Implicit_Context_Context = null;
                    String key_Implicit_Context_Context = null;
                      if (row_Implicit_Context_Regex.key != null){
                          tmp_key_Implicit_Context_Context = row_Implicit_Context_Regex.key.trim();
                        if ((tmp_key_Implicit_Context_Context.startsWith("#") || tmp_key_Implicit_Context_Context.startsWith("!") )){
                          tmp_key_Implicit_Context_Context = null;
                        } else {
                          row_Implicit_Context_Regex.key = tmp_key_Implicit_Context_Context;
                        }
                      }
                        if(row_Implicit_Context_Regex.key != null) {
                    key_Implicit_Context_Context =
                        row_Implicit_Context_Regex.key;
                        }
                    String value_Implicit_Context_Context = null;
                        if(row_Implicit_Context_Regex.value != null) {
                    value_Implicit_Context_Context =
                        row_Implicit_Context_Regex.value;
                        }
				
				String currentValue_Implicit_Context_Context = value_Implicit_Context_Context;
										
						if ((key_Implicit_Context_Context != null) && ("DB_POSTGRES_Password".equals(key_Implicit_Context_Context)) && (currentValue_Implicit_Context_Context != null)) 
							currentValue_Implicit_Context_Context = currentValue_Implicit_Context_Context.replaceAll(".", "*");						
						if ((key_Implicit_Context_Context != null) && ("EDH_CLUSTER_HIVE_hiveSSLTrustStorePassword".equals(key_Implicit_Context_Context)) && (currentValue_Implicit_Context_Context != null)) 
							currentValue_Implicit_Context_Context = currentValue_Implicit_Context_Context.replaceAll(".", "*");						
						if ((key_Implicit_Context_Context != null) && ("EDH_CLUSTER_HIVE_Password".equals(key_Implicit_Context_Context)) && (currentValue_Implicit_Context_Context != null)) 
							currentValue_Implicit_Context_Context = currentValue_Implicit_Context_Context.replaceAll(".", "*");

  if (tmp_key_Implicit_Context_Context != null){
  try{
        if(key_Implicit_Context_Context!=null && "W_BUSINESS_GROUP".equals(key_Implicit_Context_Context))
        {
           context.W_BUSINESS_GROUP=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "W_BUSINESS_NAME".equals(key_Implicit_Context_Context))
        {
           context.W_BUSINESS_NAME=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "W_DATA_TYPE".equals(key_Implicit_Context_Context))
        {
           context.W_DATA_TYPE=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "W_DEFAULT_PARTITION".equals(key_Implicit_Context_Context))
        {
           context.W_DEFAULT_PARTITION=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "W_EDH_DB_ARCHIN".equals(key_Implicit_Context_Context))
        {
           context.W_EDH_DB_ARCHIN=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "W_EDH_DB_H".equals(key_Implicit_Context_Context))
        {
           context.W_EDH_DB_H=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "W_EDH_DB_IN".equals(key_Implicit_Context_Context))
        {
           context.W_EDH_DB_IN=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "W_EDH_TABLE_ARCHIN".equals(key_Implicit_Context_Context))
        {
           context.W_EDH_TABLE_ARCHIN=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "W_EDH_TABLE_H".equals(key_Implicit_Context_Context))
        {
           context.W_EDH_TABLE_H=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "W_EDH_TABLE_IN".equals(key_Implicit_Context_Context))
        {
           context.W_EDH_TABLE_IN=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "W_EXTRACTION_FIELDS".equals(key_Implicit_Context_Context))
        {
           context.W_EXTRACTION_FIELDS=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "W_F_FILEMASK".equals(key_Implicit_Context_Context))
        {
           context.W_F_FILEMASK=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "W_FILENAME".equals(key_Implicit_Context_Context))
        {
           context.W_FILENAME=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "W_FLAG_ABILITATA".equals(key_Implicit_Context_Context))
        {
           context.W_FLAG_ABILITATA=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "W_FLAG_HEADER".equals(key_Implicit_Context_Context))
        {
           context.W_FLAG_HEADER=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "W_HDFS_ETL_PATH".equals(key_Implicit_Context_Context))
        {
           context.W_HDFS_ETL_PATH=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "W_IMPORT_TYPE".equals(key_Implicit_Context_Context))
        {
           context.W_IMPORT_TYPE=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "W_LAST_PARTITION".equals(key_Implicit_Context_Context))
        {
           context.W_LAST_PARTITION=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "W_MAPPER".equals(key_Implicit_Context_Context))
        {
           context.W_MAPPER=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "W_PARTITION_FIELD".equals(key_Implicit_Context_Context))
        {
           context.W_PARTITION_FIELD=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "W_PATH_ARCHIN".equals(key_Implicit_Context_Context))
        {
           context.W_PATH_ARCHIN=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "W_PATH_IN".equals(key_Implicit_Context_Context))
        {
           context.W_PATH_IN=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "W_SOURCE_NAME".equals(key_Implicit_Context_Context))
        {
           context.W_SOURCE_NAME=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "W_T_SOURCE_DATABASE".equals(key_Implicit_Context_Context))
        {
           context.W_T_SOURCE_DATABASE=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "W_T_SOURCE_TABLE".equals(key_Implicit_Context_Context))
        {
           context.W_T_SOURCE_TABLE=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "DB_POSTGRES_CUSTOM_LOG_TABLE".equals(key_Implicit_Context_Context))
        {
           context.DB_POSTGRES_CUSTOM_LOG_TABLE=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "DB_POSTGRES_Database".equals(key_Implicit_Context_Context))
        {
           context.DB_POSTGRES_Database=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "DB_POSTGRES_FILE_METADATA_TABLE".equals(key_Implicit_Context_Context))
        {
           context.DB_POSTGRES_FILE_METADATA_TABLE=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "DB_POSTGRES_LOG_TABLE".equals(key_Implicit_Context_Context))
        {
           context.DB_POSTGRES_LOG_TABLE=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "DB_POSTGRES_Login".equals(key_Implicit_Context_Context))
        {
           context.DB_POSTGRES_Login=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "DB_POSTGRES_LOGTABLE".equals(key_Implicit_Context_Context))
        {
           context.DB_POSTGRES_LOGTABLE=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "DB_POSTGRES_LOGVIEW".equals(key_Implicit_Context_Context))
        {
           context.DB_POSTGRES_LOGVIEW=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "DB_POSTGRES_METADATA_FACT_RECUPERO_PARTIZIONI".equals(key_Implicit_Context_Context))
        {
           context.DB_POSTGRES_METADATA_FACT_RECUPERO_PARTIZIONI=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "DB_POSTGRES_METADATA_FACT_TABLE".equals(key_Implicit_Context_Context))
        {
           context.DB_POSTGRES_METADATA_FACT_TABLE=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "DB_POSTGRES_Password".equals(key_Implicit_Context_Context))
        {
           context.DB_POSTGRES_Password=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "DB_POSTGRES_Port".equals(key_Implicit_Context_Context))
        {
           context.DB_POSTGRES_Port=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "DB_POSTGRES_Schema".equals(key_Implicit_Context_Context))
        {
           context.DB_POSTGRES_Schema=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "DB_POSTGRES_Server".equals(key_Implicit_Context_Context))
        {
           context.DB_POSTGRES_Server=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "DB_POSTGRES_SQOOP_METADATA_TABLE".equals(key_Implicit_Context_Context))
        {
           context.DB_POSTGRES_SQOOP_METADATA_TABLE=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "DB_POSTGRES_STATTABLE".equals(key_Implicit_Context_Context))
        {
           context.DB_POSTGRES_STATTABLE=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "EDH_CLUSTER_dfs_client_failover_proxy_provider_edhcluster".equals(key_Implicit_Context_Context))
        {
           context.EDH_CLUSTER_dfs_client_failover_proxy_provider_edhcluster=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "EDH_CLUSTER_dfs_ha_namenodes_edhcluster".equals(key_Implicit_Context_Context))
        {
           context.EDH_CLUSTER_dfs_ha_namenodes_edhcluster=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "EDH_CLUSTER_dfs_namenode_rpc_address_edhcluster_namenode209".equals(key_Implicit_Context_Context))
        {
           context.EDH_CLUSTER_dfs_namenode_rpc_address_edhcluster_namenode209=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "EDH_CLUSTER_dfs_namenode_rpc_address_edhcluster_namenode264".equals(key_Implicit_Context_Context))
        {
           context.EDH_CLUSTER_dfs_namenode_rpc_address_edhcluster_namenode264=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "EDH_CLUSTER_dfs_nameservices".equals(key_Implicit_Context_Context))
        {
           context.EDH_CLUSTER_dfs_nameservices=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "EDH_CLUSTER_ha_zookeeper_quorum".equals(key_Implicit_Context_Context))
        {
           context.EDH_CLUSTER_ha_zookeeper_quorum=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "EDH_CLUSTER_JobHistory".equals(key_Implicit_Context_Context))
        {
           context.EDH_CLUSTER_JobHistory=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "EDH_CLUSTER_JobHistroyPrin".equals(key_Implicit_Context_Context))
        {
           context.EDH_CLUSTER_JobHistroyPrin=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "EDH_CLUSTER_JTOrRMPrin".equals(key_Implicit_Context_Context))
        {
           context.EDH_CLUSTER_JTOrRMPrin=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "EDH_CLUSTER_KeyTab".equals(key_Implicit_Context_Context))
        {
           context.EDH_CLUSTER_KeyTab=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "EDH_CLUSTER_NameNodePrin".equals(key_Implicit_Context_Context))
        {
           context.EDH_CLUSTER_NameNodePrin=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "EDH_CLUSTER_NameNodeUri".equals(key_Implicit_Context_Context))
        {
           context.EDH_CLUSTER_NameNodeUri=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "EDH_CLUSTER_Principal".equals(key_Implicit_Context_Context))
        {
           context.EDH_CLUSTER_Principal=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "EDH_CLUSTER_ResourceManager".equals(key_Implicit_Context_Context))
        {
           context.EDH_CLUSTER_ResourceManager=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "EDH_CLUSTER_ResourceManagerScheduler".equals(key_Implicit_Context_Context))
        {
           context.EDH_CLUSTER_ResourceManagerScheduler=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "EDH_CLUSTER_StagingDirectory".equals(key_Implicit_Context_Context))
        {
           context.EDH_CLUSTER_StagingDirectory=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "EDH_CLUSTER_username".equals(key_Implicit_Context_Context))
        {
           context.EDH_CLUSTER_username=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "EDH_CLUSTER_yarn_resourcemanager_address_rm1".equals(key_Implicit_Context_Context))
        {
           context.EDH_CLUSTER_yarn_resourcemanager_address_rm1=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "EDH_CLUSTER_yarn_resourcemanager_address_rm2".equals(key_Implicit_Context_Context))
        {
           context.EDH_CLUSTER_yarn_resourcemanager_address_rm2=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "EDH_CLUSTER_yarn_resourcemanager_ha_enabled".equals(key_Implicit_Context_Context))
        {
           context.EDH_CLUSTER_yarn_resourcemanager_ha_enabled=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "EDH_CLUSTER_yarn_resourcemanager_ha_rm_ids".equals(key_Implicit_Context_Context))
        {
           context.EDH_CLUSTER_yarn_resourcemanager_ha_rm_ids=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "EDH_CLUSTER_yarn_resourcemanager_scheduler_address_rm1".equals(key_Implicit_Context_Context))
        {
           context.EDH_CLUSTER_yarn_resourcemanager_scheduler_address_rm1=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "EDH_CLUSTER_yarn_resourcemanager_scheduler_address_rm2".equals(key_Implicit_Context_Context))
        {
           context.EDH_CLUSTER_yarn_resourcemanager_scheduler_address_rm2=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "EDH_CLUSTER_HDFS_HdfsFileSeparator".equals(key_Implicit_Context_Context))
        {
           context.EDH_CLUSTER_HDFS_HdfsFileSeparator=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "EDH_CLUSTER_HDFS_HdfsRowSeparator".equals(key_Implicit_Context_Context))
        {
           context.EDH_CLUSTER_HDFS_HdfsRowSeparator=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "EDH_CLUSTER_HIVE_Database".equals(key_Implicit_Context_Context))
        {
           context.EDH_CLUSTER_HIVE_Database=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "EDH_CLUSTER_HIVE_dynamicPart".equals(key_Implicit_Context_Context))
        {
           context.EDH_CLUSTER_HIVE_dynamicPart=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "EDH_CLUSTER_HIVE_dynamicPartMax".equals(key_Implicit_Context_Context))
        {
           context.EDH_CLUSTER_HIVE_dynamicPartMax=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "EDH_CLUSTER_HIVE_dynamicPartMaxPerNode".equals(key_Implicit_Context_Context))
        {
           context.EDH_CLUSTER_HIVE_dynamicPartMaxPerNode=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "EDH_CLUSTER_HIVE_executionEngine".equals(key_Implicit_Context_Context))
        {
           context.EDH_CLUSTER_HIVE_executionEngine=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "EDH_CLUSTER_HIVE_hiveAdditionalJDBCParameters".equals(key_Implicit_Context_Context))
        {
           context.EDH_CLUSTER_HIVE_hiveAdditionalJDBCParameters=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "EDH_CLUSTER_HIVE_HiveKeyTab".equals(key_Implicit_Context_Context))
        {
           context.EDH_CLUSTER_HIVE_HiveKeyTab=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "EDH_CLUSTER_HIVE_HiveKeyTabPrincipal".equals(key_Implicit_Context_Context))
        {
           context.EDH_CLUSTER_HIVE_HiveKeyTabPrincipal=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "EDH_CLUSTER_HIVE_HivePrincipal".equals(key_Implicit_Context_Context))
        {
           context.EDH_CLUSTER_HIVE_HivePrincipal=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "EDH_CLUSTER_HIVE_hiveSSLTrustStorePassword".equals(key_Implicit_Context_Context))
        {
           context.EDH_CLUSTER_HIVE_hiveSSLTrustStorePassword=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "EDH_CLUSTER_HIVE_hiveSSLTrustStorePath".equals(key_Implicit_Context_Context))
        {
           context.EDH_CLUSTER_HIVE_hiveSSLTrustStorePath=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "EDH_CLUSTER_HIVE_Login".equals(key_Implicit_Context_Context))
        {
           context.EDH_CLUSTER_HIVE_Login=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "EDH_CLUSTER_HIVE_Password".equals(key_Implicit_Context_Context))
        {
           context.EDH_CLUSTER_HIVE_Password=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "EDH_CLUSTER_HIVE_Port".equals(key_Implicit_Context_Context))
        {
           context.EDH_CLUSTER_HIVE_Port=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "EDH_CLUSTER_HIVE_Server".equals(key_Implicit_Context_Context))
        {
           context.EDH_CLUSTER_HIVE_Server=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "EDH_CLUSTER_IMPALA_Database".equals(key_Implicit_Context_Context))
        {
           context.EDH_CLUSTER_IMPALA_Database=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "EDH_CLUSTER_IMPALA_ImpalaPrincipal".equals(key_Implicit_Context_Context))
        {
           context.EDH_CLUSTER_IMPALA_ImpalaPrincipal=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "EDH_CLUSTER_IMPALA_Login".equals(key_Implicit_Context_Context))
        {
           context.EDH_CLUSTER_IMPALA_Login=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "EDH_CLUSTER_IMPALA_Port".equals(key_Implicit_Context_Context))
        {
           context.EDH_CLUSTER_IMPALA_Port=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "EDH_CLUSTER_IMPALA_Server".equals(key_Implicit_Context_Context))
        {
           context.EDH_CLUSTER_IMPALA_Server=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "DB_POSTGRES_configuration_metadata".equals(key_Implicit_Context_Context))
        {
           context.DB_POSTGRES_configuration_metadata=value_Implicit_Context_Context;
        }


        if (context.getProperty(key_Implicit_Context_Context)!=null)
        {
            assignList_Implicit_Context_Context.add(key_Implicit_Context_Context);
        }else  {
            newPropertyList_Implicit_Context_Context.add(key_Implicit_Context_Context);
        }
        if(value_Implicit_Context_Context == null){
            context.setProperty(key_Implicit_Context_Context, "");
        }else{
            context.setProperty(key_Implicit_Context_Context,value_Implicit_Context_Context);
        }
    }catch(java.lang.Exception e){
globalMap.put("Implicit_Context_Context_ERROR_MESSAGE",e.getMessage());
            log.error("Implicit_Context_Context - Setting a value for the key \"" + key_Implicit_Context_Context + "\" has failed. Error message: " + e.getMessage());
        System.err.println("Setting a value for the key \"" + key_Implicit_Context_Context + "\" has failed. Error message: " + e.getMessage());
    }
        nb_line_Implicit_Context_Context++;
    }
        //////////////////////////

 


	tos_count_Implicit_Context_Context++;

/**
 * [Implicit_Context_Context main ] stop
 */
	
	/**
	 * [Implicit_Context_Context process_data_begin ] start
	 */

	

	
	
		currentVirtualComponent = "Implicit_Context_Context";
	
	currentComponent="Implicit_Context_Context";

	

 



/**
 * [Implicit_Context_Context process_data_begin ] stop
 */
	
	/**
	 * [Implicit_Context_Context process_data_end ] start
	 */

	

	
	
		currentVirtualComponent = "Implicit_Context_Context";
	
	currentComponent="Implicit_Context_Context";

	

 



/**
 * [Implicit_Context_Context process_data_end ] stop
 */

} // End of branch "row_Implicit_Context_Regex"




	
	/**
	 * [Implicit_Context_Regex process_data_end ] start
	 */

	

	
	
		currentVirtualComponent = "Implicit_Context_Regex";
	
	currentComponent="Implicit_Context_Regex";

	

 



/**
 * [Implicit_Context_Regex process_data_end ] stop
 */
	
	/**
	 * [Implicit_Context_Regex end ] start
	 */

	

	
	
		currentVirtualComponent = "Implicit_Context_Regex";
	
	currentComponent="Implicit_Context_Regex";

	

	
	
    		}
			if(!(source_Implicit_Context_Regex instanceof java.io.InputStream)){
            	inImplicit_Context_Regex.close();
            }
            inImplicit_Context_Regex = null;
            globalMap.put("Implicit_Context_Regex_NB_LINE",nb_line_Implicit_Context_Regex);
				log.debug("Implicit_Context_Regex - Retrieved records count: "+ nb_line_Implicit_Context_Regex + " .");
			      
 
                if(log.isDebugEnabled())
            log.debug("Implicit_Context_Regex - "  + ("Done.") );

ok_Hash.put("Implicit_Context_Regex", true);
end_Hash.put("Implicit_Context_Regex", System.currentTimeMillis());




/**
 * [Implicit_Context_Regex end ] stop
 */

	
	/**
	 * [Implicit_Context_Context end ] start
	 */

	

	
	
		currentVirtualComponent = "Implicit_Context_Context";
	
	currentComponent="Implicit_Context_Context";

	
	
	java.util.Enumeration<?> enu_Implicit_Context_Context = context.propertyNames();
    while(enu_Implicit_Context_Context.hasMoreElements())
    {           
    	String key_Implicit_Context_Context = (String)enu_Implicit_Context_Context.nextElement();
        if(!assignList_Implicit_Context_Context.contains(key_Implicit_Context_Context) && !newPropertyList_Implicit_Context_Context.contains(key_Implicit_Context_Context))
        {
            noAssignList_Implicit_Context_Context.add(key_Implicit_Context_Context);
        }          
    }
	for(Object obj_Implicit_Context_Context :newPropertyList_Implicit_Context_Context){
		
			String newLog_Implicit_Context_Context = "Implicit_Context_Context: Parameter \"" + obj_Implicit_Context_Context + "\" is a new parameter of Implicit_Context_Context";
			
				log.warn(newLog_Implicit_Context_Context);
			
		
		System.out.println("Warning: Parameter \"" + obj_Implicit_Context_Context + "\" is a new parameter of Implicit_Context_Context");        
	}
	for(Object obj_Implicit_Context_Context :noAssignList_Implicit_Context_Context){
		
			String oldLog_Implicit_Context_Context = "Implicit_Context_Context: Parameter \"" + obj_Implicit_Context_Context + "\" has not been set by Implicit_Context_Context";
			
				log.warn(oldLog_Implicit_Context_Context);
			
		
		System.out.println("Warning: Parameter \"" + obj_Implicit_Context_Context + "\" has not been set by Implicit_Context_Context");
		
	} 

    String newPropertyStr_Implicit_Context_Context = newPropertyList_Implicit_Context_Context.toString();
    String newProperty_Implicit_Context_Context = newPropertyStr_Implicit_Context_Context.substring(1, newPropertyStr_Implicit_Context_Context.length() - 1);
    
    String noAssignStr_Implicit_Context_Context = noAssignList_Implicit_Context_Context.toString();
    String noAssign_Implicit_Context_Context = noAssignStr_Implicit_Context_Context.substring(1, noAssignStr_Implicit_Context_Context.length() - 1);
    
    globalMap.put("Implicit_Context_Context_KEY_NOT_INCONTEXT", newProperty_Implicit_Context_Context);
    globalMap.put("Implicit_Context_Context_KEY_NOT_LOADED", noAssign_Implicit_Context_Context);

    globalMap.put("Implicit_Context_Context_NB_LINE",nb_line_Implicit_Context_Context);

	List<String> parametersToEncrypt_Implicit_Context_Context = new java.util.ArrayList<String>();
	
		parametersToEncrypt_Implicit_Context_Context.add("DB_POSTGRES_Password");
		
		parametersToEncrypt_Implicit_Context_Context.add("EDH_CLUSTER_HIVE_hiveSSLTrustStorePassword");
		
		parametersToEncrypt_Implicit_Context_Context.add("EDH_CLUSTER_HIVE_Password");
		
	
	resumeUtil.addLog("NODE", "NODE:Implicit_Context_Context", "", Thread.currentThread().getId() + "", "","","","",resumeUtil.convertToJsonText(context,ContextProperties.class,parametersToEncrypt_Implicit_Context_Context));    
    	log.info("Implicit_Context_Context - Loaded contexts count: " + nb_line_Implicit_Context_Context + ".");
    
			 		if(runStat.updateStatAndLog(execStat,enableLogStash,resourceMap,iterateId,"Main",2,0,
			 			"Implicit_Context_Regex","Implicit_Context_Regex","tFileInputRegex","Implicit_Context_Context","Implicit_Context_Context","tContextLoad","output")) {
						talendJobLogProcess(globalMap);
					}
				
 
                if(log.isDebugEnabled())
            log.debug("Implicit_Context_Context - "  + ("Done.") );

ok_Hash.put("Implicit_Context_Context", true);
end_Hash.put("Implicit_Context_Context", System.currentTimeMillis());




/**
 * [Implicit_Context_Context end ] stop
 */



				}//end the resume

				



	
			}catch(java.lang.Exception e){	
				
				    if(!(e instanceof TalendException)){
					   log.fatal(currentComponent + " " + e.getMessage(),e);
					}
				
				TalendException te = new TalendException(e, currentComponent, globalMap);
				
					te.setVirtualComponentName(currentVirtualComponent);
				
				throw te;
			}catch(java.lang.Error error){	
				
					runStat.stopThreadStat();
				
				throw error;
			}finally{
				
				try{
					
	
	/**
	 * [Implicit_Context_Regex finally ] start
	 */

	

	
	
		currentVirtualComponent = "Implicit_Context_Regex";
	
	currentComponent="Implicit_Context_Regex";

	

 



/**
 * [Implicit_Context_Regex finally ] stop
 */

	
	/**
	 * [Implicit_Context_Context finally ] start
	 */

	

	
	
		currentVirtualComponent = "Implicit_Context_Context";
	
	currentComponent="Implicit_Context_Context";

	

 



/**
 * [Implicit_Context_Context finally ] stop
 */



				}catch(java.lang.Exception e){	
					//ignore
				}catch(java.lang.Error error){
					//ignore
				}
				resourceMap = null;
			}
		

		globalMap.put("Implicit_Context_Regex_SUBPROCESS_STATE", 1);
	}
	

public void tImpalaConnection_1Process(final java.util.Map<String, Object> globalMap) throws TalendException {
	globalMap.put("tImpalaConnection_1_SUBPROCESS_STATE", 0);

 final boolean execStat = this.execStat;
	
		String iterateId = "";
	
	
	String currentComponent = "";
	java.util.Map<String, Object> resourceMap = new java.util.HashMap<String, Object>();

	try {
			// TDI-39566 avoid throwing an useless Exception
			boolean resumeIt = true;
			if (globalResumeTicket == false && resumeEntryMethodName != null) {
				String currentMethodName = new java.lang.Exception().getStackTrace()[0].getMethodName();
				resumeIt = resumeEntryMethodName.equals(currentMethodName);
			}
			if (resumeIt || globalResumeTicket) { //start the resume
				globalResumeTicket = true;



		


	
	/**
	 * [tImpalaConnection_1 begin ] start
	 */

	

	
		
		ok_Hash.put("tImpalaConnection_1", false);
		start_Hash.put("tImpalaConnection_1", System.currentTimeMillis());
		
	
	currentComponent="tImpalaConnection_1";

	
		int tos_count_tImpalaConnection_1 = 0;
		
			if(enableLogStash) {
				talendJobLog.addCM("tImpalaConnection_1", "tImpalaConnection_1", "tImpalaConnection");
				talendJobLogProcess(globalMap);
			}
			
	
	



	
          globalMap.put("HADOOP_USER_NAME_tImpalaConnection_1", System.getProperty("HADOOP_USER_NAME"));
			
							String url_tImpalaConnection_1 = "jdbc:hive2://" + context.EDH_CLUSTER_IMPALA_Server + ":" + context.EDH_CLUSTER_IMPALA_Port + "/" + context.EDH_CLUSTER_IMPALA_Database + ";principal=" + context.EDH_CLUSTER_IMPALA_ImpalaPrincipal+ ";ssl=true";
				String additionalJdbcSettings_tImpalaConnection_1 = "";
				if (!"".equals(additionalJdbcSettings_tImpalaConnection_1.trim())) {
					if (!additionalJdbcSettings_tImpalaConnection_1.startsWith(";")) {
						additionalJdbcSettings_tImpalaConnection_1 = ";" + additionalJdbcSettings_tImpalaConnection_1;
					}
					url_tImpalaConnection_1 += additionalJdbcSettings_tImpalaConnection_1;
				}
	String dbUser_tImpalaConnection_1 = context.EDH_CLUSTER_IMPALA_Login;
	
	
		 
	final String decryptedPassword_tImpalaConnection_1 = routines.system.PasswordEncryptUtil.decryptPassword("enc:routine.encryption.key.v1:bTLC6dNdoIVUEYZ4QodM7ME3W8lPIyts8T9P0Q==");
		String dbPwd_tImpalaConnection_1 = decryptedPassword_tImpalaConnection_1;
	
	
	java.sql.Connection conn_tImpalaConnection_1 = null;
	
		
			String driverClass_tImpalaConnection_1 = "org.apache.hive.jdbc.HiveDriver";
			java.lang.Class jdbcclazz_tImpalaConnection_1 = java.lang.Class.forName(driverClass_tImpalaConnection_1);
			globalMap.put("driverClass_tImpalaConnection_1", driverClass_tImpalaConnection_1);
		
	    		log.debug("tImpalaConnection_1 - Driver ClassName: "+driverClass_tImpalaConnection_1+".");
			
	    		log.debug("tImpalaConnection_1 - Connection attempt to '" + url_tImpalaConnection_1 + "' with the username '" + dbUser_tImpalaConnection_1 + "'.");
			
			if ("" != null && false){
				conn_tImpalaConnection_1 = java.sql.DriverManager.getConnection(url_tImpalaConnection_1+";user="+dbUser_tImpalaConnection_1+";password="+dbPwd_tImpalaConnection_1 );
			} else {
				conn_tImpalaConnection_1 = java.sql.DriverManager.getConnection(url_tImpalaConnection_1, dbUser_tImpalaConnection_1 , dbPwd_tImpalaConnection_1 );
			}
	    		log.debug("tImpalaConnection_1 - Connection to '" + url_tImpalaConnection_1 + "' has succeeded.");
			

		globalMap.put("conn_tImpalaConnection_1", conn_tImpalaConnection_1);
	if (null != conn_tImpalaConnection_1) {
		
	}

	globalMap.put("current_client_path_separator", System.getProperty("path.separator"));
	System.setProperty("path.separator", "");

	java.sql.Statement init_tImpalaConnection_1 = conn_tImpalaConnection_1.createStatement();

	init_tImpalaConnection_1.close();

	
	globalMap.put("conn_tImpalaConnection_1",conn_tImpalaConnection_1);

	globalMap.put("db_tImpalaConnection_1",context.EDH_CLUSTER_IMPALA_Database);

	String currentClientPathSeparator_tImpalaConnection_1 = (String)globalMap.get("current_client_path_separator");
	if(currentClientPathSeparator_tImpalaConnection_1!=null) {
		System.setProperty("path.separator", currentClientPathSeparator_tImpalaConnection_1);
		globalMap.put("current_client_path_separator", null);
	}

	String currentClientUsername_tImpalaConnection_1 = (String)globalMap.get("current_client_user_name");
	if(currentClientUsername_tImpalaConnection_1!=null) {
		System.setProperty("user.name", currentClientUsername_tImpalaConnection_1);
		globalMap.put("current_client_user_name", null);
	}

	String originalHadoopUsername_tImpalaConnection_1 = (String)globalMap.get("HADOOP_USER_NAME_tImpalaConnection_1");
	if(originalHadoopUsername_tImpalaConnection_1!=null) {
		System.setProperty("HADOOP_USER_NAME", originalHadoopUsername_tImpalaConnection_1);
		globalMap.put("HADOOP_USER_NAME_tImpalaConnection_1", null);
	} else {
		System.clearProperty("HADOOP_USER_NAME");
	}

 



/**
 * [tImpalaConnection_1 begin ] stop
 */
	
	/**
	 * [tImpalaConnection_1 main ] start
	 */

	

	
	
	currentComponent="tImpalaConnection_1";

	

 


	tos_count_tImpalaConnection_1++;

/**
 * [tImpalaConnection_1 main ] stop
 */
	
	/**
	 * [tImpalaConnection_1 process_data_begin ] start
	 */

	

	
	
	currentComponent="tImpalaConnection_1";

	

 



/**
 * [tImpalaConnection_1 process_data_begin ] stop
 */
	
	/**
	 * [tImpalaConnection_1 process_data_end ] start
	 */

	

	
	
	currentComponent="tImpalaConnection_1";

	

 



/**
 * [tImpalaConnection_1 process_data_end ] stop
 */
	
	/**
	 * [tImpalaConnection_1 end ] start
	 */

	

	
	
	currentComponent="tImpalaConnection_1";

	

 

ok_Hash.put("tImpalaConnection_1", true);
end_Hash.put("tImpalaConnection_1", System.currentTimeMillis());

				if(execStat){   
   	 				runStat.updateStatOnConnection("OnComponentOk7", 0, "ok");
				}
				tHiveConnection_1Process(globalMap);



/**
 * [tImpalaConnection_1 end ] stop
 */
				}//end the resume

				



	
			}catch(java.lang.Exception e){	
				
				    if(!(e instanceof TalendException)){
					   log.fatal(currentComponent + " " + e.getMessage(),e);
					}
				
				TalendException te = new TalendException(e, currentComponent, globalMap);
				
				throw te;
			}catch(java.lang.Error error){	
				
					runStat.stopThreadStat();
				
				throw error;
			}finally{
				
				try{
					
	
	/**
	 * [tImpalaConnection_1 finally ] start
	 */

	

	
	
	currentComponent="tImpalaConnection_1";

	

 



/**
 * [tImpalaConnection_1 finally ] stop
 */
				}catch(java.lang.Exception e){	
					//ignore
				}catch(java.lang.Error error){
					//ignore
				}
				resourceMap = null;
			}
		

		globalMap.put("tImpalaConnection_1_SUBPROCESS_STATE", 1);
	}
	

public void tHiveConnection_1Process(final java.util.Map<String, Object> globalMap) throws TalendException {
	globalMap.put("tHiveConnection_1_SUBPROCESS_STATE", 0);

 final boolean execStat = this.execStat;
	
		String iterateId = "";
	
	
	String currentComponent = "";
	java.util.Map<String, Object> resourceMap = new java.util.HashMap<String, Object>();

	try {
			// TDI-39566 avoid throwing an useless Exception
			boolean resumeIt = true;
			if (globalResumeTicket == false && resumeEntryMethodName != null) {
				String currentMethodName = new java.lang.Exception().getStackTrace()[0].getMethodName();
				resumeIt = resumeEntryMethodName.equals(currentMethodName);
			}
			if (resumeIt || globalResumeTicket) { //start the resume
				globalResumeTicket = true;



		


	
	/**
	 * [tHiveConnection_1 begin ] start
	 */

	

	
		
		ok_Hash.put("tHiveConnection_1", false);
		start_Hash.put("tHiveConnection_1", System.currentTimeMillis());
		
	
	currentComponent="tHiveConnection_1";

	
		int tos_count_tHiveConnection_1 = 0;
		
                if(log.isDebugEnabled())
            log.debug("tHiveConnection_1 - "  + ("Start to work.") );
            if (log.isDebugEnabled()) {
                class BytesLimit65535_tHiveConnection_1{
                    public void limitLog4jByte() throws Exception{
                    StringBuilder log4jParamters_tHiveConnection_1 = new StringBuilder();
                    log4jParamters_tHiveConnection_1.append("Parameters:");
                            log4jParamters_tHiveConnection_1.append("DISTRIBUTION" + " = " + "CLOUDERA");
                        log4jParamters_tHiveConnection_1.append(" | ");
                            log4jParamters_tHiveConnection_1.append("HIVE_VERSION" + " = " + "Cloudera_CDH5_8");
                        log4jParamters_tHiveConnection_1.append(" | ");
                            log4jParamters_tHiveConnection_1.append("HIVE" + " = " + "");
                        log4jParamters_tHiveConnection_1.append(" | ");
                            log4jParamters_tHiveConnection_1.append("CONNECTION_MODE" + " = " + "STANDALONE");
                        log4jParamters_tHiveConnection_1.append(" | ");
                            log4jParamters_tHiveConnection_1.append("HIVE_SERVER" + " = " + "HIVE2");
                        log4jParamters_tHiveConnection_1.append(" | ");
                            log4jParamters_tHiveConnection_1.append("HOST" + " = " + "context.EDH_CLUSTER_HIVE_Server");
                        log4jParamters_tHiveConnection_1.append(" | ");
                            log4jParamters_tHiveConnection_1.append("PORT" + " = " + "context.EDH_CLUSTER_HIVE_Port");
                        log4jParamters_tHiveConnection_1.append(" | ");
                            log4jParamters_tHiveConnection_1.append("DBNAME" + " = " + "context.EDH_CLUSTER_HIVE_Database");
                        log4jParamters_tHiveConnection_1.append(" | ");
                            log4jParamters_tHiveConnection_1.append("USER" + " = " + "context.EDH_CLUSTER_HIVE_Login");
                        log4jParamters_tHiveConnection_1.append(" | ");
                            log4jParamters_tHiveConnection_1.append("PASS" + " = " + String.valueOf(routines.system.PasswordEncryptUtil.encryptPassword(context.EDH_CLUSTER_HIVE_Password)).substring(0, 4) + "...");     
                        log4jParamters_tHiveConnection_1.append(" | ");
                            log4jParamters_tHiveConnection_1.append("HIVE_ADDITIONAL_JDBC" + " = " + "context.EDH_CLUSTER_HIVE_hiveAdditionalJDBCParameters");
                        log4jParamters_tHiveConnection_1.append(" | ");
                            log4jParamters_tHiveConnection_1.append("CONFIGURATIONS_FROM_CLASSPATH" + " = " + "false");
                        log4jParamters_tHiveConnection_1.append(" | ");
                            log4jParamters_tHiveConnection_1.append("USE_KRB" + " = " + "true");
                        log4jParamters_tHiveConnection_1.append(" | ");
                            log4jParamters_tHiveConnection_1.append("HIVE_PRINCIPAL" + " = " + "context.EDH_CLUSTER_HIVE_HivePrincipal");
                        log4jParamters_tHiveConnection_1.append(" | ");
                            log4jParamters_tHiveConnection_1.append("NAMENODE_PRINCIPAL" + " = " + "context.EDH_CLUSTER_NameNodePrin");
                        log4jParamters_tHiveConnection_1.append(" | ");
                            log4jParamters_tHiveConnection_1.append("RESOURCEMANAGER_PRINCIPAL" + " = " + "context.EDH_CLUSTER_JTOrRMPrin");
                        log4jParamters_tHiveConnection_1.append(" | ");
                            log4jParamters_tHiveConnection_1.append("USE_KEYTAB" + " = " + "true");
                        log4jParamters_tHiveConnection_1.append(" | ");
                            log4jParamters_tHiveConnection_1.append("PRINCIPAL" + " = " + "context.EDH_CLUSTER_HIVE_HiveKeyTabPrincipal");
                        log4jParamters_tHiveConnection_1.append(" | ");
                            log4jParamters_tHiveConnection_1.append("KEYTAB_PATH" + " = " + "context.EDH_CLUSTER_HIVE_HiveKeyTab");
                        log4jParamters_tHiveConnection_1.append(" | ");
                            log4jParamters_tHiveConnection_1.append("USE_SSL" + " = " + "true");
                        log4jParamters_tHiveConnection_1.append(" | ");
                            log4jParamters_tHiveConnection_1.append("SSL_TRUST_STORE" + " = " + "context.EDH_CLUSTER_HIVE_hiveSSLTrustStorePath");
                        log4jParamters_tHiveConnection_1.append(" | ");
                            log4jParamters_tHiveConnection_1.append("SSL_TRUST_STORE_PASSWORD" + " = " + String.valueOf(routines.system.PasswordEncryptUtil.encryptPassword(context.EDH_CLUSTER_HIVE_hiveSSLTrustStorePassword)).substring(0, 4) + "...");     
                        log4jParamters_tHiveConnection_1.append(" | ");
                            log4jParamters_tHiveConnection_1.append("SET_RESOURCE_MANAGER" + " = " + "false");
                        log4jParamters_tHiveConnection_1.append(" | ");
                            log4jParamters_tHiveConnection_1.append("SET_FS_DEFAULT_NAME" + " = " + "true");
                        log4jParamters_tHiveConnection_1.append(" | ");
                            log4jParamters_tHiveConnection_1.append("FS_DEFAULT_NAME" + " = " + "context.EDH_CLUSTER_NameNodeUri");
                        log4jParamters_tHiveConnection_1.append(" | ");
                            log4jParamters_tHiveConnection_1.append("SET_SCHEDULER_ADDRESS" + " = " + "false");
                        log4jParamters_tHiveConnection_1.append(" | ");
                            log4jParamters_tHiveConnection_1.append("SET_JOBHISTORY_ADDRESS" + " = " + "false");
                        log4jParamters_tHiveConnection_1.append(" | ");
                            log4jParamters_tHiveConnection_1.append("SET_HADOOP_USER" + " = " + "false");
                        log4jParamters_tHiveConnection_1.append(" | ");
                            log4jParamters_tHiveConnection_1.append("USE_DATANODE_HOSTNAME" + " = " + "true");
                        log4jParamters_tHiveConnection_1.append(" | ");
                            log4jParamters_tHiveConnection_1.append("USE_SHARED_CONNECTION" + " = " + "false");
                        log4jParamters_tHiveConnection_1.append(" | ");
                            log4jParamters_tHiveConnection_1.append("HADOOP_ADVANCED_PROPERTIES" + " = " + "[{PROPERTY="+("\"dfs.nameservices\"")+", VALUE="+("context.EDH_CLUSTER_dfs_nameservices")+"}, {PROPERTY="+("\"dfs.client.failover.proxy.provider.edhcluster\"")+", VALUE="+("context.EDH_CLUSTER_dfs_client_failover_proxy_provider_edhcluster")+"}, {PROPERTY="+("\"ha.zookeeper.quorum\"")+", VALUE="+("context.EDH_CLUSTER_ha_zookeeper_quorum")+"}, {PROPERTY="+("\"dfs.ha.namenodes.edhcluster\"")+", VALUE="+("context.EDH_CLUSTER_dfs_ha_namenodes_edhcluster")+"}, {PROPERTY="+("\"dfs.namenode.rpc-address.edhcluster.namenode209\"")+", VALUE="+("context.EDH_CLUSTER_dfs_namenode_rpc_address_edhcluster_namenode209")+"}, {PROPERTY="+("\"dfs.namenode.rpc-address.edhcluster.namenode264\"")+", VALUE="+("context.EDH_CLUSTER_dfs_namenode_rpc_address_edhcluster_namenode264")+"}]");
                        log4jParamters_tHiveConnection_1.append(" | ");
                            log4jParamters_tHiveConnection_1.append("ADVANCED_PROPERTIES" + " = " + "[{PROPERTY="+("\"hive.exec.dynamic.partition.mode\"")+", VALUE="+("context.EDH_CLUSTER_HIVE_dynamicPart")+"}, {PROPERTY="+("\"hive.execution.engine\"")+", VALUE="+("context.EDH_CLUSTER_HIVE_executionEngine")+"}, {PROPERTY="+("\"hive.exec.max.dynamic.partitions\"")+", VALUE="+("context.EDH_CLUSTER_HIVE_dynamicPartMax")+"}, {PROPERTY="+("\"hive.exec.max.dynamic.partitions.pernode\"")+", VALUE="+("context.EDH_CLUSTER_HIVE_dynamicPartMaxPerNode")+"}]");
                        log4jParamters_tHiveConnection_1.append(" | ");
                            log4jParamters_tHiveConnection_1.append("SET_MEMORY" + " = " + "false");
                        log4jParamters_tHiveConnection_1.append(" | ");
                            log4jParamters_tHiveConnection_1.append("CLASSPATH_SEPARATOR" + " = " + "\":\"");
                        log4jParamters_tHiveConnection_1.append(" | ");
                if(log.isDebugEnabled())
            log.debug("tHiveConnection_1 - "  + (log4jParamters_tHiveConnection_1) );
                    } 
                } 
            new BytesLimit65535_tHiveConnection_1().limitLog4jByte();
            }
			if(enableLogStash) {
				talendJobLog.addCM("tHiveConnection_1", "Hive Conn", "tHiveConnection");
				talendJobLogProcess(globalMap);
			}
			

		
	

	
          org.apache.hadoop.conf.Configuration conf_tHiveConnection_1 = new org.apache.hadoop.conf.Configuration();
          System.setProperty("dfs.nameservices" ,context.EDH_CLUSTER_dfs_nameservices);
            conf_tHiveConnection_1.set("dfs.nameservices" ,context.EDH_CLUSTER_dfs_nameservices);
          System.setProperty("dfs.client.failover.proxy.provider.edhcluster" ,context.EDH_CLUSTER_dfs_client_failover_proxy_provider_edhcluster);
            conf_tHiveConnection_1.set("dfs.client.failover.proxy.provider.edhcluster" ,context.EDH_CLUSTER_dfs_client_failover_proxy_provider_edhcluster);
          System.setProperty("ha.zookeeper.quorum" ,context.EDH_CLUSTER_ha_zookeeper_quorum);
            conf_tHiveConnection_1.set("ha.zookeeper.quorum" ,context.EDH_CLUSTER_ha_zookeeper_quorum);
          System.setProperty("dfs.ha.namenodes.edhcluster" ,context.EDH_CLUSTER_dfs_ha_namenodes_edhcluster);
            conf_tHiveConnection_1.set("dfs.ha.namenodes.edhcluster" ,context.EDH_CLUSTER_dfs_ha_namenodes_edhcluster);
          System.setProperty("dfs.namenode.rpc-address.edhcluster.namenode209" ,context.EDH_CLUSTER_dfs_namenode_rpc_address_edhcluster_namenode209);
            conf_tHiveConnection_1.set("dfs.namenode.rpc-address.edhcluster.namenode209" ,context.EDH_CLUSTER_dfs_namenode_rpc_address_edhcluster_namenode209);
          System.setProperty("dfs.namenode.rpc-address.edhcluster.namenode264" ,context.EDH_CLUSTER_dfs_namenode_rpc_address_edhcluster_namenode264);
            conf_tHiveConnection_1.set("dfs.namenode.rpc-address.edhcluster.namenode264" ,context.EDH_CLUSTER_dfs_namenode_rpc_address_edhcluster_namenode264);
          org.apache.hadoop.security.UserGroupInformation.setConfiguration(conf_tHiveConnection_1);
          org.apache.hadoop.security.UserGroupInformation.getLoginUser();
				System.setProperty("fs.default.name", context.EDH_CLUSTER_NameNodeUri);
			globalMap.put("HADOOP_USER_NAME_tHiveConnection_1", System.getProperty("HADOOP_USER_NAME"));
			
							org.apache.hadoop.security.UserGroupInformation.loginUserFromKeytab(context.EDH_CLUSTER_HIVE_HiveKeyTabPrincipal, context.EDH_CLUSTER_HIVE_HiveKeyTab);
									String decryptedSslStorePassword_tHiveConnection_1 = context.EDH_CLUSTER_HIVE_hiveSSLTrustStorePassword;
								String url_tHiveConnection_1 = "jdbc:hive2://" + context.EDH_CLUSTER_HIVE_Server + ":" + context.EDH_CLUSTER_HIVE_Port + "/" + context.EDH_CLUSTER_HIVE_Database + ";principal=" + context.EDH_CLUSTER_HIVE_HivePrincipal+";ssl=true" +";sslTrustStore=" + context.EDH_CLUSTER_HIVE_hiveSSLTrustStorePath + ";trustStorePassword=" + decryptedSslStorePassword_tHiveConnection_1;
				String additionalJdbcSettings_tHiveConnection_1 = context.EDH_CLUSTER_HIVE_hiveAdditionalJDBCParameters;
				if(!"".equals(additionalJdbcSettings_tHiveConnection_1.trim())) {
					if(!additionalJdbcSettings_tHiveConnection_1.startsWith(";")) {
						additionalJdbcSettings_tHiveConnection_1 = ";" + additionalJdbcSettings_tHiveConnection_1;
					}
					url_tHiveConnection_1 += additionalJdbcSettings_tHiveConnection_1;
				}
	String dbUser_tHiveConnection_1 = context.EDH_CLUSTER_HIVE_Login;
	
	
		
	final String decryptedPassword_tHiveConnection_1 = context.EDH_CLUSTER_HIVE_Password; 
		String dbPwd_tHiveConnection_1 = decryptedPassword_tHiveConnection_1;
	
	
	java.sql.Connection conn_tHiveConnection_1 = null;
	
		
			String driverClass_tHiveConnection_1 = "org.apache.hive.jdbc.HiveDriver";
			java.lang.Class jdbcclazz_tHiveConnection_1 = java.lang.Class.forName(driverClass_tHiveConnection_1);
			globalMap.put("driverClass_tHiveConnection_1", driverClass_tHiveConnection_1);
		
	    		log.debug("tHiveConnection_1 - Driver ClassName: "+driverClass_tHiveConnection_1+".");
			
	    		log.debug("tHiveConnection_1 - Connection attempt to '" + url_tHiveConnection_1 + "' with the username '" + dbUser_tHiveConnection_1 + "'.");
			
				conn_tHiveConnection_1 = java.sql.DriverManager.getConnection(url_tHiveConnection_1);
	    		log.debug("tHiveConnection_1 - Connection to '" + url_tHiveConnection_1 + "' has succeeded.");
			

		globalMap.put("conn_tHiveConnection_1", conn_tHiveConnection_1);
	if (null != conn_tHiveConnection_1) {
		
	}

	globalMap.put("current_client_path_separator", System.getProperty("path.separator"));
	System.setProperty("path.separator", ":");

	java.sql.Statement init_tHiveConnection_1 = conn_tHiveConnection_1.createStatement();
			init_tHiveConnection_1.execute("SET dfs.client.use.datanode.hostname=true");
			init_tHiveConnection_1.execute("SET fs.default.name=" + context.EDH_CLUSTER_NameNodeUri);
			init_tHiveConnection_1.execute("SET "+"hive.exec.dynamic.partition.mode"+"="+context.EDH_CLUSTER_HIVE_dynamicPart);
			init_tHiveConnection_1.execute("SET "+"hive.execution.engine"+"="+context.EDH_CLUSTER_HIVE_executionEngine);
			init_tHiveConnection_1.execute("SET "+"hive.exec.max.dynamic.partitions"+"="+context.EDH_CLUSTER_HIVE_dynamicPartMax);
			init_tHiveConnection_1.execute("SET "+"hive.exec.max.dynamic.partitions.pernode"+"="+context.EDH_CLUSTER_HIVE_dynamicPartMaxPerNode);

	

	

	init_tHiveConnection_1.close();

	
	globalMap.put("conn_tHiveConnection_1",conn_tHiveConnection_1);

	globalMap.put("db_tHiveConnection_1",context.EDH_CLUSTER_HIVE_Database);

	String currentClientPathSeparator_tHiveConnection_1 = (String)globalMap.get("current_client_path_separator");
	if(currentClientPathSeparator_tHiveConnection_1!=null) {
		System.setProperty("path.separator", currentClientPathSeparator_tHiveConnection_1);
		globalMap.put("current_client_path_separator", null);
	}

	String currentClientUsername_tHiveConnection_1 = (String)globalMap.get("current_client_user_name");
	if(currentClientUsername_tHiveConnection_1!=null) {
		System.setProperty("user.name", currentClientUsername_tHiveConnection_1);
		globalMap.put("current_client_user_name", null);
	}

	String originalHadoopUsername_tHiveConnection_1 = (String)globalMap.get("HADOOP_USER_NAME_tHiveConnection_1");
	if(originalHadoopUsername_tHiveConnection_1!=null) {
		System.setProperty("HADOOP_USER_NAME", originalHadoopUsername_tHiveConnection_1);
		globalMap.put("HADOOP_USER_NAME_tHiveConnection_1", null);
	} else {
		System.clearProperty("HADOOP_USER_NAME");
	}

 



/**
 * [tHiveConnection_1 begin ] stop
 */
	
	/**
	 * [tHiveConnection_1 main ] start
	 */

	

	
	
	currentComponent="tHiveConnection_1";

	

 


	tos_count_tHiveConnection_1++;

/**
 * [tHiveConnection_1 main ] stop
 */
	
	/**
	 * [tHiveConnection_1 process_data_begin ] start
	 */

	

	
	
	currentComponent="tHiveConnection_1";

	

 



/**
 * [tHiveConnection_1 process_data_begin ] stop
 */
	
	/**
	 * [tHiveConnection_1 process_data_end ] start
	 */

	

	
	
	currentComponent="tHiveConnection_1";

	

 



/**
 * [tHiveConnection_1 process_data_end ] stop
 */
	
	/**
	 * [tHiveConnection_1 end ] start
	 */

	

	
	
	currentComponent="tHiveConnection_1";

	

 
                if(log.isDebugEnabled())
            log.debug("tHiveConnection_1 - "  + ("Done.") );

ok_Hash.put("tHiveConnection_1", true);
end_Hash.put("tHiveConnection_1", System.currentTimeMillis());

				if(execStat){   
   	 				runStat.updateStatOnConnection("OnComponentOk5", 0, "ok");
				}
				tHDFSConnection_1Process(globalMap);



/**
 * [tHiveConnection_1 end ] stop
 */
				}//end the resume

				



	
			}catch(java.lang.Exception e){	
				
				    if(!(e instanceof TalendException)){
					   log.fatal(currentComponent + " " + e.getMessage(),e);
					}
				
				TalendException te = new TalendException(e, currentComponent, globalMap);
				
				throw te;
			}catch(java.lang.Error error){	
				
					runStat.stopThreadStat();
				
				throw error;
			}finally{
				
				try{
					
	
	/**
	 * [tHiveConnection_1 finally ] start
	 */

	

	
	
	currentComponent="tHiveConnection_1";

	

 



/**
 * [tHiveConnection_1 finally ] stop
 */
				}catch(java.lang.Exception e){	
					//ignore
				}catch(java.lang.Error error){
					//ignore
				}
				resourceMap = null;
			}
		

		globalMap.put("tHiveConnection_1_SUBPROCESS_STATE", 1);
	}
	

public void tHDFSConnection_1Process(final java.util.Map<String, Object> globalMap) throws TalendException {
	globalMap.put("tHDFSConnection_1_SUBPROCESS_STATE", 0);

 final boolean execStat = this.execStat;
	
		String iterateId = "";
	
	
	String currentComponent = "";
	java.util.Map<String, Object> resourceMap = new java.util.HashMap<String, Object>();

	try {
			// TDI-39566 avoid throwing an useless Exception
			boolean resumeIt = true;
			if (globalResumeTicket == false && resumeEntryMethodName != null) {
				String currentMethodName = new java.lang.Exception().getStackTrace()[0].getMethodName();
				resumeIt = resumeEntryMethodName.equals(currentMethodName);
			}
			if (resumeIt || globalResumeTicket) { //start the resume
				globalResumeTicket = true;



		


	
	/**
	 * [tHDFSConnection_1 begin ] start
	 */

	

	
		
		ok_Hash.put("tHDFSConnection_1", false);
		start_Hash.put("tHDFSConnection_1", System.currentTimeMillis());
		
	
	currentComponent="tHDFSConnection_1";

	
		int tos_count_tHDFSConnection_1 = 0;
		
                if(log.isDebugEnabled())
            log.debug("tHDFSConnection_1 - "  + ("Start to work.") );
            if (log.isDebugEnabled()) {
                class BytesLimit65535_tHDFSConnection_1{
                    public void limitLog4jByte() throws Exception{
                    StringBuilder log4jParamters_tHDFSConnection_1 = new StringBuilder();
                    log4jParamters_tHDFSConnection_1.append("Parameters:");
                            log4jParamters_tHDFSConnection_1.append("DISTRIBUTION" + " = " + "CLOUDERA");
                        log4jParamters_tHDFSConnection_1.append(" | ");
                            log4jParamters_tHDFSConnection_1.append("DB_VERSION" + " = " + "Cloudera_CDH5_8");
                        log4jParamters_tHDFSConnection_1.append(" | ");
                            log4jParamters_tHDFSConnection_1.append("HDFS" + " = " + "");
                        log4jParamters_tHDFSConnection_1.append(" | ");
                            log4jParamters_tHDFSConnection_1.append("SCHEME" + " = " + "HDFS");
                        log4jParamters_tHDFSConnection_1.append(" | ");
                            log4jParamters_tHDFSConnection_1.append("FS_DEFAULT_NAME" + " = " + "context.EDH_CLUSTER_NameNodeUri");
                        log4jParamters_tHDFSConnection_1.append(" | ");
                            log4jParamters_tHDFSConnection_1.append("CONFIGURATIONS_FROM_CLASSPATH" + " = " + "false");
                        log4jParamters_tHDFSConnection_1.append(" | ");
                            log4jParamters_tHDFSConnection_1.append("USE_KRB" + " = " + "true");
                        log4jParamters_tHDFSConnection_1.append(" | ");
                            log4jParamters_tHDFSConnection_1.append("NAMENODE_PRINCIPAL" + " = " + "context.EDH_CLUSTER_NameNodePrin");
                        log4jParamters_tHDFSConnection_1.append(" | ");
                            log4jParamters_tHDFSConnection_1.append("USE_KEYTAB" + " = " + "true");
                        log4jParamters_tHDFSConnection_1.append(" | ");
                            log4jParamters_tHDFSConnection_1.append("PRINCIPAL" + " = " + "context.EDH_CLUSTER_Principal");
                        log4jParamters_tHDFSConnection_1.append(" | ");
                            log4jParamters_tHDFSConnection_1.append("KEYTAB_PATH" + " = " + "context.EDH_CLUSTER_KeyTab");
                        log4jParamters_tHDFSConnection_1.append(" | ");
                            log4jParamters_tHDFSConnection_1.append("HADOOP_ADVANCED_PROPERTIES" + " = " + "[{PROPERTY="+("\"dfs.nameservices\"")+", VALUE="+("context.EDH_CLUSTER_dfs_nameservices")+"}, {PROPERTY="+("\"dfs.client.failover.proxy.provider.edhcluster\"")+", VALUE="+("context.EDH_CLUSTER_dfs_client_failover_proxy_provider_edhcluster")+"}, {PROPERTY="+("\"ha.zookeeper.quorum\"")+", VALUE="+("context.EDH_CLUSTER_ha_zookeeper_quorum")+"}, {PROPERTY="+("\"dfs.ha.namenodes.edhcluster\"")+", VALUE="+("context.EDH_CLUSTER_dfs_ha_namenodes_edhcluster")+"}, {PROPERTY="+("\"dfs.namenode.rpc-address.edhcluster.namenode209\"")+", VALUE="+("context.EDH_CLUSTER_dfs_namenode_rpc_address_edhcluster_namenode209")+"}, {PROPERTY="+("\"dfs.namenode.rpc-address.edhcluster.namenode264\"")+", VALUE="+("context.EDH_CLUSTER_dfs_namenode_rpc_address_edhcluster_namenode264")+"}]");
                        log4jParamters_tHDFSConnection_1.append(" | ");
                            log4jParamters_tHDFSConnection_1.append("USE_DATANODE_HOSTNAME" + " = " + "true");
                        log4jParamters_tHDFSConnection_1.append(" | ");
                            log4jParamters_tHDFSConnection_1.append("USE_HDFS_ENCRYPTION" + " = " + "false");
                        log4jParamters_tHDFSConnection_1.append(" | ");
                if(log.isDebugEnabled())
            log.debug("tHDFSConnection_1 - "  + (log4jParamters_tHDFSConnection_1) );
                    } 
                } 
            new BytesLimit65535_tHDFSConnection_1().limitLog4jByte();
            }
			if(enableLogStash) {
				talendJobLog.addCM("tHDFSConnection_1", "HDFS Conn", "tHDFSConnection");
				talendJobLogProcess(globalMap);
			}
			

	
	
	
				final StringBuffer log4jSb_tHDFSConnection_1 = new StringBuffer();
			
		org.apache.hadoop.conf.Configuration conf_tHDFSConnection_1 = new org.apache.hadoop.conf.Configuration();
		conf_tHDFSConnection_1.set("fs.default.name", context.EDH_CLUSTER_NameNodeUri);
			conf_tHDFSConnection_1.set("fs.default.name", context.EDH_CLUSTER_NameNodeUri);
		
				conf_tHDFSConnection_1.set("dfs.client.use.datanode.hostname", "true");
	
				conf_tHDFSConnection_1.set("dfs.namenode.kerberos.principal", context.EDH_CLUSTER_NameNodePrin);
	
					org.apache.hadoop.security.UserGroupInformation.loginUserFromKeytab(context.EDH_CLUSTER_Principal, context.EDH_CLUSTER_KeyTab);
	
			conf_tHDFSConnection_1.set("dfs.nameservices" ,context.EDH_CLUSTER_dfs_nameservices);
			conf_tHDFSConnection_1.set("dfs.client.failover.proxy.provider.edhcluster" ,context.EDH_CLUSTER_dfs_client_failover_proxy_provider_edhcluster);
			conf_tHDFSConnection_1.set("ha.zookeeper.quorum" ,context.EDH_CLUSTER_ha_zookeeper_quorum);
			conf_tHDFSConnection_1.set("dfs.ha.namenodes.edhcluster" ,context.EDH_CLUSTER_dfs_ha_namenodes_edhcluster);
			conf_tHDFSConnection_1.set("dfs.namenode.rpc-address.edhcluster.namenode209" ,context.EDH_CLUSTER_dfs_namenode_rpc_address_edhcluster_namenode209);
			conf_tHDFSConnection_1.set("dfs.namenode.rpc-address.edhcluster.namenode264" ,context.EDH_CLUSTER_dfs_namenode_rpc_address_edhcluster_namenode264);
	org.apache.hadoop.security.UserGroupInformation.setConfiguration(conf_tHDFSConnection_1);
	globalMap.put("conn_tHDFSConnection_1",conf_tHDFSConnection_1);

 



/**
 * [tHDFSConnection_1 begin ] stop
 */
	
	/**
	 * [tHDFSConnection_1 main ] start
	 */

	

	
	
	currentComponent="tHDFSConnection_1";

	

 


	tos_count_tHDFSConnection_1++;

/**
 * [tHDFSConnection_1 main ] stop
 */
	
	/**
	 * [tHDFSConnection_1 process_data_begin ] start
	 */

	

	
	
	currentComponent="tHDFSConnection_1";

	

 



/**
 * [tHDFSConnection_1 process_data_begin ] stop
 */
	
	/**
	 * [tHDFSConnection_1 process_data_end ] start
	 */

	

	
	
	currentComponent="tHDFSConnection_1";

	

 



/**
 * [tHDFSConnection_1 process_data_end ] stop
 */
	
	/**
	 * [tHDFSConnection_1 end ] start
	 */

	

	
	
	currentComponent="tHDFSConnection_1";

	

 
                if(log.isDebugEnabled())
            log.debug("tHDFSConnection_1 - "  + ("Done.") );

ok_Hash.put("tHDFSConnection_1", true);
end_Hash.put("tHDFSConnection_1", System.currentTimeMillis());




/**
 * [tHDFSConnection_1 end ] stop
 */
				}//end the resume

				
				    			if(resumeEntryMethodName == null || globalResumeTicket){
				    				resumeUtil.addLog("CHECKPOINT", "CONNECTION:SUBJOB_OK:tHDFSConnection_1:OnSubjobOk", "", Thread.currentThread().getId() + "", "", "", "", "", "");
								}	    				    			
					    	
								if(execStat){    	
									runStat.updateStatOnConnection("OnSubjobOk1", 0, "ok");
								} 
							
							tJava_3Process(globalMap); 
						



	
			}catch(java.lang.Exception e){	
				
				    if(!(e instanceof TalendException)){
					   log.fatal(currentComponent + " " + e.getMessage(),e);
					}
				
				TalendException te = new TalendException(e, currentComponent, globalMap);
				
				throw te;
			}catch(java.lang.Error error){	
				
					runStat.stopThreadStat();
				
				throw error;
			}finally{
				
				try{
					
	
	/**
	 * [tHDFSConnection_1 finally ] start
	 */

	

	
	
	currentComponent="tHDFSConnection_1";

	

 



/**
 * [tHDFSConnection_1 finally ] stop
 */
				}catch(java.lang.Exception e){	
					//ignore
				}catch(java.lang.Error error){
					//ignore
				}
				resourceMap = null;
			}
		

		globalMap.put("tHDFSConnection_1_SUBPROCESS_STATE", 1);
	}
	

public void tJava_3Process(final java.util.Map<String, Object> globalMap) throws TalendException {
	globalMap.put("tJava_3_SUBPROCESS_STATE", 0);

 final boolean execStat = this.execStat;
	
		String iterateId = "";
	
	
	String currentComponent = "";
	java.util.Map<String, Object> resourceMap = new java.util.HashMap<String, Object>();

	try {
			// TDI-39566 avoid throwing an useless Exception
			boolean resumeIt = true;
			if (globalResumeTicket == false && resumeEntryMethodName != null) {
				String currentMethodName = new java.lang.Exception().getStackTrace()[0].getMethodName();
				resumeIt = resumeEntryMethodName.equals(currentMethodName);
			}
			if (resumeIt || globalResumeTicket) { //start the resume
				globalResumeTicket = true;



		


	
	/**
	 * [tJava_3 begin ] start
	 */

	

	
		
		ok_Hash.put("tJava_3", false);
		start_Hash.put("tJava_3", System.currentTimeMillis());
		
	
	currentComponent="tJava_3";

	
		int tos_count_tJava_3 = 0;
		
			if(enableLogStash) {
				talendJobLog.addCM("tJava_3", "tJava_3", "tJava");
				talendJobLogProcess(globalMap);
			}
			


String foo = "bar";

System.out.println("rimuovo da :  " );
System.out.println("Variabile W_HDFS_ETL_PATH :  " +context.W_HDFS_ETL_PATH + "/in/" + context.W_BUSINESS_NAME.toLowerCase());

System.out.println("scrivo file vuoto su :  " );
System.out.println("Variabile W_HDFS_ETL_PATH :  " +
context.W_HDFS_ETL_PATH+ "/in/" + context.W_BUSINESS_NAME.toLowerCase()+"/"+context.W_BUSINESS_NAME.toLowerCase() );

System.out.println("faccio la list file su :  " );
System.out.println("Variabile W_PATH_IN:\t" +context.W_PATH_IN );
System.out.println("Variabile W_F_FILEMASK :\t" +context.W_F_FILEMASK );

System.out.println("PARTITION FIELD:\t"+context.W_PARTITION_FIELD);


 



/**
 * [tJava_3 begin ] stop
 */
	
	/**
	 * [tJava_3 main ] start
	 */

	

	
	
	currentComponent="tJava_3";

	

 


	tos_count_tJava_3++;

/**
 * [tJava_3 main ] stop
 */
	
	/**
	 * [tJava_3 process_data_begin ] start
	 */

	

	
	
	currentComponent="tJava_3";

	

 



/**
 * [tJava_3 process_data_begin ] stop
 */
	
	/**
	 * [tJava_3 process_data_end ] start
	 */

	

	
	
	currentComponent="tJava_3";

	

 



/**
 * [tJava_3 process_data_end ] stop
 */
	
	/**
	 * [tJava_3 end ] start
	 */

	

	
	
	currentComponent="tJava_3";

	

 

ok_Hash.put("tJava_3", true);
end_Hash.put("tJava_3", System.currentTimeMillis());




/**
 * [tJava_3 end ] stop
 */
				}//end the resume

				
				    			if(resumeEntryMethodName == null || globalResumeTicket){
				    				resumeUtil.addLog("CHECKPOINT", "CONNECTION:SUBJOB_OK:tJava_3:OnSubjobOk", "", Thread.currentThread().getId() + "", "", "", "", "", "");
								}	    				    			
					    	
								if(execStat){    	
									runStat.updateStatOnConnection("OnSubjobOk2", 0, "ok");
								} 
							
							tHDFSDelete_1Process(globalMap); 
						



	
			}catch(java.lang.Exception e){	
				
				    if(!(e instanceof TalendException)){
					   log.fatal(currentComponent + " " + e.getMessage(),e);
					}
				
				TalendException te = new TalendException(e, currentComponent, globalMap);
				
				throw te;
			}catch(java.lang.Error error){	
				
					runStat.stopThreadStat();
				
				throw error;
			}finally{
				
				try{
					
	
	/**
	 * [tJava_3 finally ] start
	 */

	

	
	
	currentComponent="tJava_3";

	

 



/**
 * [tJava_3 finally ] stop
 */
				}catch(java.lang.Exception e){	
					//ignore
				}catch(java.lang.Error error){
					//ignore
				}
				resourceMap = null;
			}
		

		globalMap.put("tJava_3_SUBPROCESS_STATE", 1);
	}
	

public void tHDFSDelete_1Process(final java.util.Map<String, Object> globalMap) throws TalendException {
	globalMap.put("tHDFSDelete_1_SUBPROCESS_STATE", 0);

 final boolean execStat = this.execStat;
	
		String iterateId = "";
	
	
	String currentComponent = "";
	java.util.Map<String, Object> resourceMap = new java.util.HashMap<String, Object>();

	try {
			// TDI-39566 avoid throwing an useless Exception
			boolean resumeIt = true;
			if (globalResumeTicket == false && resumeEntryMethodName != null) {
				String currentMethodName = new java.lang.Exception().getStackTrace()[0].getMethodName();
				resumeIt = resumeEntryMethodName.equals(currentMethodName);
			}
			if (resumeIt || globalResumeTicket) { //start the resume
				globalResumeTicket = true;



		


	
	/**
	 * [tHDFSDelete_1 begin ] start
	 */

	

	
		
		ok_Hash.put("tHDFSDelete_1", false);
		start_Hash.put("tHDFSDelete_1", System.currentTimeMillis());
		
	
	currentComponent="tHDFSDelete_1";

	
		int tos_count_tHDFSDelete_1 = 0;
		
                if(log.isDebugEnabled())
            log.debug("tHDFSDelete_1 - "  + ("Start to work.") );
            if (log.isDebugEnabled()) {
                class BytesLimit65535_tHDFSDelete_1{
                    public void limitLog4jByte() throws Exception{
                    StringBuilder log4jParamters_tHDFSDelete_1 = new StringBuilder();
                    log4jParamters_tHDFSDelete_1.append("Parameters:");
                            log4jParamters_tHDFSDelete_1.append("USE_EXISTING_CONNECTION" + " = " + "true");
                        log4jParamters_tHDFSDelete_1.append(" | ");
                            log4jParamters_tHDFSDelete_1.append("CONNECTION" + " = " + "tHDFSConnection_1");
                        log4jParamters_tHDFSDelete_1.append(" | ");
                            log4jParamters_tHDFSDelete_1.append("PATH" + " = " + "context.W_HDFS_ETL_PATH + \"/in/\" + context.W_BUSINESS_NAME.toLowerCase()");
                        log4jParamters_tHDFSDelete_1.append(" | ");
                if(log.isDebugEnabled())
            log.debug("tHDFSDelete_1 - "  + (log4jParamters_tHDFSDelete_1) );
                    } 
                } 
            new BytesLimit65535_tHDFSDelete_1().limitLog4jByte();
            }
			if(enableLogStash) {
				talendJobLog.addCM("tHDFSDelete_1", "rimuovi in", "tHDFSDelete");
				talendJobLogProcess(globalMap);
			}
			

 



/**
 * [tHDFSDelete_1 begin ] stop
 */
	
	/**
	 * [tHDFSDelete_1 main ] start
	 */

	

	
	
	currentComponent="tHDFSDelete_1";

	
	


				final StringBuffer log4jSb_tHDFSDelete_1 = new StringBuffer();
			
String username_tHDFSDelete_1 = "";
org.apache.hadoop.fs.FileSystem fs_tHDFSDelete_1 = null;
	org.apache.hadoop.conf.Configuration conf_tHDFSDelete_1 = (org.apache.hadoop.conf.Configuration)globalMap.get("conn_tHDFSConnection_1");
	
						conf_tHDFSDelete_1.set("dfs.namenode.kerberos.principal", context.EDH_CLUSTER_NameNodePrin);					
					username_tHDFSDelete_1 = null;
				if(username_tHDFSDelete_1 == null || "".equals(username_tHDFSDelete_1)){
					fs_tHDFSDelete_1 = org.apache.hadoop.fs.FileSystem.get(conf_tHDFSDelete_1);
				}else{
					System.setProperty("HADOOP_USER_NAME", username_tHDFSDelete_1);
					fs_tHDFSDelete_1 = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(conf_tHDFSDelete_1.get("fs.default.name")),conf_tHDFSDelete_1,username_tHDFSDelete_1);
				}			  		
		  	
	
	org.apache.hadoop.fs.Path path_tHDFSDelete_1 = new org.apache.hadoop.fs.Path(context.W_HDFS_ETL_PATH + "/in/" + context.W_BUSINESS_NAME.toLowerCase());
	if(fs_tHDFSDelete_1.exists(path_tHDFSDelete_1)){
		if(fs_tHDFSDelete_1.delete(path_tHDFSDelete_1, true)){
			globalMap.put("tHDFSDelete_1_CURRENT_STATUS", "Path deleted.");
			
	    	
            log.info("tHDFSDelete_1 - directory or file : " + path_tHDFSDelete_1 + " is deleted.");
		}else{
			globalMap.put("tHDFSDelete_1_CURRENT_STATUS", "No path deleted.");
			
			
            log.info("tHDFSDelete_1 - fail to delete directory or file : " + path_tHDFSDelete_1 + ".");
		}
	}else{
		globalMap.put("tHDFSDelete_1_CURRENT_STATUS", "Path does not exist.");
		
		
       	log.warn("tHDFSDelete_1 - directory or file : " + path_tHDFSDelete_1 + " does not exist.");
	}
	globalMap.put("tHDFSDelete_1_DELETE_PATH",context.W_HDFS_ETL_PATH + "/in/" + context.W_BUSINESS_NAME.toLowerCase());
	



	

 


	tos_count_tHDFSDelete_1++;

/**
 * [tHDFSDelete_1 main ] stop
 */
	
	/**
	 * [tHDFSDelete_1 process_data_begin ] start
	 */

	

	
	
	currentComponent="tHDFSDelete_1";

	

 



/**
 * [tHDFSDelete_1 process_data_begin ] stop
 */
	
	/**
	 * [tHDFSDelete_1 process_data_end ] start
	 */

	

	
	
	currentComponent="tHDFSDelete_1";

	

 



/**
 * [tHDFSDelete_1 process_data_end ] stop
 */
	
	/**
	 * [tHDFSDelete_1 end ] start
	 */

	

	
	
	currentComponent="tHDFSDelete_1";

	

 
                if(log.isDebugEnabled())
            log.debug("tHDFSDelete_1 - "  + ("Done.") );

ok_Hash.put("tHDFSDelete_1", true);
end_Hash.put("tHDFSDelete_1", System.currentTimeMillis());




/**
 * [tHDFSDelete_1 end ] stop
 */
				}//end the resume

				
				    			if(resumeEntryMethodName == null || globalResumeTicket){
				    				resumeUtil.addLog("CHECKPOINT", "CONNECTION:SUBJOB_OK:tHDFSDelete_1:OnSubjobOk", "", Thread.currentThread().getId() + "", "", "", "", "", "");
								}	    				    			
					    	
								if(execStat){    	
									runStat.updateStatOnConnection("OnSubjobOk3", 0, "ok");
								} 
							
							tFixedFlowInput_1Process(globalMap); 
						



	
			}catch(java.lang.Exception e){	
				
				    if(!(e instanceof TalendException)){
					   log.fatal(currentComponent + " " + e.getMessage(),e);
					}
				
				TalendException te = new TalendException(e, currentComponent, globalMap);
				
				throw te;
			}catch(java.lang.Error error){	
				
					runStat.stopThreadStat();
				
				throw error;
			}finally{
				
				try{
					
	
	/**
	 * [tHDFSDelete_1 finally ] start
	 */

	

	
	
	currentComponent="tHDFSDelete_1";

	

 



/**
 * [tHDFSDelete_1 finally ] stop
 */
				}catch(java.lang.Exception e){	
					//ignore
				}catch(java.lang.Error error){
					//ignore
				}
				resourceMap = null;
			}
		

		globalMap.put("tHDFSDelete_1_SUBPROCESS_STATE", 1);
	}
	


public static class row5Struct implements routines.system.IPersistableRow<row5Struct> {
    final static byte[] commonByteArrayLock_DATAHUB_AML_S_INGESTION_IMPORT_FILE_1 = new byte[0];
    static byte[] commonByteArray_DATAHUB_AML_S_INGESTION_IMPORT_FILE_1 = new byte[0];

	
			    public String empty;

				public String getEmpty () {
					return this.empty;
				}

				public Boolean emptyIsNullable(){
				    return true;
				}
				public Boolean emptyIsKey(){
				    return false;
				}
				public Integer emptyLength(){
				    return null;
				}
				public Integer emptyPrecision(){
				    return null;
				}
				public String emptyDefault(){
				
					return null;
				
				}
				public String emptyComment(){
				
				    return "";
				
				}
				public String emptyPattern(){
				
					return "";
				
				}
				public String emptyOriginalDbColumnName(){
				
					return "empty";
				
				}

				



	private String readString(ObjectInputStream dis) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_DATAHUB_AML_S_INGESTION_IMPORT_FILE_1.length) {
				if(length < 1024 && commonByteArray_DATAHUB_AML_S_INGESTION_IMPORT_FILE_1.length == 0) {
   					commonByteArray_DATAHUB_AML_S_INGESTION_IMPORT_FILE_1 = new byte[1024];
				} else {
   					commonByteArray_DATAHUB_AML_S_INGESTION_IMPORT_FILE_1 = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_DATAHUB_AML_S_INGESTION_IMPORT_FILE_1, 0, length);
			strReturn = new String(commonByteArray_DATAHUB_AML_S_INGESTION_IMPORT_FILE_1, 0, length, utf8Charset);
		}
		return strReturn;
	}
	
	private String readString(org.jboss.marshalling.Unmarshaller unmarshaller) throws IOException{
		String strReturn = null;
		int length = 0;
        length = unmarshaller.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_DATAHUB_AML_S_INGESTION_IMPORT_FILE_1.length) {
				if(length < 1024 && commonByteArray_DATAHUB_AML_S_INGESTION_IMPORT_FILE_1.length == 0) {
   					commonByteArray_DATAHUB_AML_S_INGESTION_IMPORT_FILE_1 = new byte[1024];
				} else {
   					commonByteArray_DATAHUB_AML_S_INGESTION_IMPORT_FILE_1 = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_DATAHUB_AML_S_INGESTION_IMPORT_FILE_1, 0, length);
			strReturn = new String(commonByteArray_DATAHUB_AML_S_INGESTION_IMPORT_FILE_1, 0, length, utf8Charset);
		}
		return strReturn;
	}

    private void writeString(String str, ObjectOutputStream dos) throws IOException{
		if(str == null) {
            dos.writeInt(-1);
		} else {
            byte[] byteArray = str.getBytes(utf8Charset);
	    	dos.writeInt(byteArray.length);
			dos.write(byteArray);
    	}
    }
    
    private void writeString(String str, org.jboss.marshalling.Marshaller marshaller) throws IOException{
		if(str == null) {
			marshaller.writeInt(-1);
		} else {
            byte[] byteArray = str.getBytes(utf8Charset);
            marshaller.writeInt(byteArray.length);
            marshaller.write(byteArray);
    	}
    }

    public void readData(ObjectInputStream dis) {

		synchronized(commonByteArrayLock_DATAHUB_AML_S_INGESTION_IMPORT_FILE_1) {

        	try {

        		int length = 0;
		
					this.empty = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_DATAHUB_AML_S_INGESTION_IMPORT_FILE_1) {

        	try {

        		int length = 0;
		
					this.empty = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }

    public void writeData(ObjectOutputStream dos) {
        try {

		
					// String
				
						writeString(this.empty,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }
    
    public void writeData(org.jboss.marshalling.Marshaller dos) {
        try {

		
					// String
				
						writeString(this.empty,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }


    public String toString() {

		StringBuilder sb = new StringBuilder();
		sb.append(super.toString());
		sb.append("[");
		sb.append("empty="+empty);
	    sb.append("]");

	    return sb.toString();
    }
        public String toLogString(){
        	StringBuilder sb = new StringBuilder();
        	
        				if(empty == null){
        					sb.append("<null>");
        				}else{
            				sb.append(empty);
            			}
            		
        			sb.append("|");
        		
        	return sb.toString();
        }

    /**
     * Compare keys
     */
    public int compareTo(row5Struct other) {

		int returnValue = -1;
		
	    return returnValue;
    }


    private int checkNullsAndCompare(Object object1, Object object2) {
        int returnValue = 0;
		if (object1 instanceof Comparable && object2 instanceof Comparable) {
            returnValue = ((Comparable) object1).compareTo(object2);
        } else if (object1 != null && object2 != null) {
            returnValue = compareStrings(object1.toString(), object2.toString());
        } else if (object1 == null && object2 != null) {
            returnValue = 1;
        } else if (object1 != null && object2 == null) {
            returnValue = -1;
        } else {
            returnValue = 0;
        }

        return returnValue;
    }

    private int compareStrings(String string1, String string2) {
        return string1.compareTo(string2);
    }


}
public void tFixedFlowInput_1Process(final java.util.Map<String, Object> globalMap) throws TalendException {
	globalMap.put("tFixedFlowInput_1_SUBPROCESS_STATE", 0);

 final boolean execStat = this.execStat;
	
		String iterateId = "";
	
	
	String currentComponent = "";
	java.util.Map<String, Object> resourceMap = new java.util.HashMap<String, Object>();

	try {
			// TDI-39566 avoid throwing an useless Exception
			boolean resumeIt = true;
			if (globalResumeTicket == false && resumeEntryMethodName != null) {
				String currentMethodName = new java.lang.Exception().getStackTrace()[0].getMethodName();
				resumeIt = resumeEntryMethodName.equals(currentMethodName);
			}
			if (resumeIt || globalResumeTicket) { //start the resume
				globalResumeTicket = true;



		row5Struct row5 = new row5Struct();




	
	/**
	 * [tHDFSOutput_1 begin ] start
	 */

	

	
		
		ok_Hash.put("tHDFSOutput_1", false);
		start_Hash.put("tHDFSOutput_1", System.currentTimeMillis());
		
	
	currentComponent="tHDFSOutput_1";

	
			runStat.updateStatAndLog(execStat,enableLogStash,resourceMap,iterateId,0,0,"row5");
			
		int tos_count_tHDFSOutput_1 = 0;
		
                if(log.isDebugEnabled())
            log.debug("tHDFSOutput_1 - "  + ("Start to work.") );
            if (log.isDebugEnabled()) {
                class BytesLimit65535_tHDFSOutput_1{
                    public void limitLog4jByte() throws Exception{
                    StringBuilder log4jParamters_tHDFSOutput_1 = new StringBuilder();
                    log4jParamters_tHDFSOutput_1.append("Parameters:");
                            log4jParamters_tHDFSOutput_1.append("USE_EXISTING_CONNECTION" + " = " + "true");
                        log4jParamters_tHDFSOutput_1.append(" | ");
                            log4jParamters_tHDFSOutput_1.append("CONNECTION" + " = " + "tHDFSConnection_1");
                        log4jParamters_tHDFSOutput_1.append(" | ");
                            log4jParamters_tHDFSOutput_1.append("FILENAME" + " = " + "context.W_HDFS_ETL_PATH+ \"/in/\" + context.W_BUSINESS_NAME.toLowerCase()+\"/\"+context.W_BUSINESS_NAME.toLowerCase()");
                        log4jParamters_tHDFSOutput_1.append(" | ");
                            log4jParamters_tHDFSOutput_1.append("TYPEFILE" + " = " + "TEXT");
                        log4jParamters_tHDFSOutput_1.append(" | ");
                            log4jParamters_tHDFSOutput_1.append("FILE_ACTION" + " = " + "OVERWRITE");
                        log4jParamters_tHDFSOutput_1.append(" | ");
                            log4jParamters_tHDFSOutput_1.append("ROWSEPARATOR" + " = " + "\"\\n\"");
                        log4jParamters_tHDFSOutput_1.append(" | ");
                            log4jParamters_tHDFSOutput_1.append("FIELDSEPARATOR" + " = " + "\";\"");
                        log4jParamters_tHDFSOutput_1.append(" | ");
                            log4jParamters_tHDFSOutput_1.append("CUSTOM_ENCODING" + " = " + "false");
                        log4jParamters_tHDFSOutput_1.append(" | ");
                            log4jParamters_tHDFSOutput_1.append("COMPRESS" + " = " + "false");
                        log4jParamters_tHDFSOutput_1.append(" | ");
                            log4jParamters_tHDFSOutput_1.append("INCLUDEHEADER" + " = " + "false");
                        log4jParamters_tHDFSOutput_1.append(" | ");
                if(log.isDebugEnabled())
            log.debug("tHDFSOutput_1 - "  + (log4jParamters_tHDFSOutput_1) );
                    } 
                } 
            new BytesLimit65535_tHDFSOutput_1().limitLog4jByte();
            }
			if(enableLogStash) {
				talendJobLog.addCM("tHDFSOutput_1", "scrivi file vuoto in in", "tHDFSOutput");
				talendJobLogProcess(globalMap);
			}
			

	


				final StringBuffer log4jSb_tHDFSOutput_1 = new StringBuffer();
			
String username_tHDFSOutput_1 = "";
org.apache.hadoop.fs.FileSystem fs_tHDFSOutput_1 = null;
	org.apache.hadoop.conf.Configuration conf_tHDFSOutput_1 = (org.apache.hadoop.conf.Configuration)globalMap.get("conn_tHDFSConnection_1");
	
						conf_tHDFSOutput_1.set("dfs.namenode.kerberos.principal", context.EDH_CLUSTER_NameNodePrin);					
					username_tHDFSOutput_1 = null;
				if(username_tHDFSOutput_1 == null || "".equals(username_tHDFSOutput_1)){
					fs_tHDFSOutput_1 = org.apache.hadoop.fs.FileSystem.get(conf_tHDFSOutput_1);
				}else{
					System.setProperty("HADOOP_USER_NAME", username_tHDFSOutput_1);
					fs_tHDFSOutput_1 = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(conf_tHDFSOutput_1.get("fs.default.name")),conf_tHDFSOutput_1,username_tHDFSOutput_1);
				}			  		
		  	

	
	org.apache.hadoop.fs.Path path_tHDFSOutput_1 = new org.apache.hadoop.fs.Path(context.W_HDFS_ETL_PATH+ "/in/" + context.W_BUSINESS_NAME.toLowerCase()+"/"+context.W_BUSINESS_NAME.toLowerCase());
	int nb_line_tHDFSOutput_1 = 0;
				
		org.apache.hadoop.fs.FSDataOutputStream fsDataOutputStream_tHDFSOutput_1 = null;
		
			fsDataOutputStream_tHDFSOutput_1 = fs_tHDFSOutput_1.create(path_tHDFSOutput_1, true);
		
		
			java.io.Writer outtHDFSOutput_1 = null;
			outtHDFSOutput_1=new java.io.BufferedWriter(new java.io.OutputStreamWriter(fsDataOutputStream_tHDFSOutput_1));
		

 



/**
 * [tHDFSOutput_1 begin ] stop
 */



	
	/**
	 * [tFixedFlowInput_1 begin ] start
	 */

	

	
		
		ok_Hash.put("tFixedFlowInput_1", false);
		start_Hash.put("tFixedFlowInput_1", System.currentTimeMillis());
		
	
	currentComponent="tFixedFlowInput_1";

	
		int tos_count_tFixedFlowInput_1 = 0;
		
			if(enableLogStash) {
				talendJobLog.addCM("tFixedFlowInput_1", "input vuoto", "tFixedFlowInput");
				talendJobLogProcess(globalMap);
			}
			

	    for (int i_tFixedFlowInput_1 = 0 ; i_tFixedFlowInput_1 < 0 ; i_tFixedFlowInput_1++) {
	                	            	
    	            		row5.empty = null;        	            	
    	            	

 



/**
 * [tFixedFlowInput_1 begin ] stop
 */
	
	/**
	 * [tFixedFlowInput_1 main ] start
	 */

	

	
	
	currentComponent="tFixedFlowInput_1";

	

 


	tos_count_tFixedFlowInput_1++;

/**
 * [tFixedFlowInput_1 main ] stop
 */
	
	/**
	 * [tFixedFlowInput_1 process_data_begin ] start
	 */

	

	
	
	currentComponent="tFixedFlowInput_1";

	

 



/**
 * [tFixedFlowInput_1 process_data_begin ] stop
 */

	
	/**
	 * [tHDFSOutput_1 main ] start
	 */

	

	
	
	currentComponent="tHDFSOutput_1";

	
			if(runStat.update(execStat,enableLogStash,iterateId,1,1
				
					,"row5","tFixedFlowInput_1","input vuoto","tFixedFlowInput","tHDFSOutput_1","scrivi file vuoto in in","tHDFSOutput"
				
			)) {
				talendJobLogProcess(globalMap);
			}
			
    			if(log.isTraceEnabled()){
    				log.trace("row5 - " + (row5==null? "": row5.toLogString()));
    			}
    		

	
					StringBuilder sb_tHDFSOutput_1 = new StringBuilder();
					
					
								if(row5.empty != null) {
							
									sb_tHDFSOutput_1.append(
										
											row5.empty
										
									);
							
								}
							
					sb_tHDFSOutput_1.append("\n");
					
						outtHDFSOutput_1.write(sb_tHDFSOutput_1.toString());
					
				nb_line_tHDFSOutput_1++;
				
				log.debug("tHDFSOutput_1 - Writing the record " + nb_line_tHDFSOutput_1 + " to the file.");
			

	
 


	tos_count_tHDFSOutput_1++;

/**
 * [tHDFSOutput_1 main ] stop
 */
	
	/**
	 * [tHDFSOutput_1 process_data_begin ] start
	 */

	

	
	
	currentComponent="tHDFSOutput_1";

	

 



/**
 * [tHDFSOutput_1 process_data_begin ] stop
 */
	
	/**
	 * [tHDFSOutput_1 process_data_end ] start
	 */

	

	
	
	currentComponent="tHDFSOutput_1";

	

 



/**
 * [tHDFSOutput_1 process_data_end ] stop
 */



	
	/**
	 * [tFixedFlowInput_1 process_data_end ] start
	 */

	

	
	
	currentComponent="tFixedFlowInput_1";

	

 



/**
 * [tFixedFlowInput_1 process_data_end ] stop
 */
	
	/**
	 * [tFixedFlowInput_1 end ] start
	 */

	

	
	
	currentComponent="tFixedFlowInput_1";

	

        }
        globalMap.put("tFixedFlowInput_1_NB_LINE", 0);        

 

ok_Hash.put("tFixedFlowInput_1", true);
end_Hash.put("tFixedFlowInput_1", System.currentTimeMillis());




/**
 * [tFixedFlowInput_1 end ] stop
 */

	
	/**
	 * [tHDFSOutput_1 end ] start
	 */

	

	
	
	currentComponent="tHDFSOutput_1";

	


		if(outtHDFSOutput_1!=null){
			outtHDFSOutput_1.close();
		}
				log.debug("tHDFSOutput_1 - Written records count: " + nb_line_tHDFSOutput_1 + " .");
			

	
			 		if(runStat.updateStatAndLog(execStat,enableLogStash,resourceMap,iterateId,"row5",2,0,
			 			"tFixedFlowInput_1","input vuoto","tFixedFlowInput","tHDFSOutput_1","scrivi file vuoto in in","tHDFSOutput","output")) {
						talendJobLogProcess(globalMap);
					}
				
 
                if(log.isDebugEnabled())
            log.debug("tHDFSOutput_1 - "  + ("Done.") );

ok_Hash.put("tHDFSOutput_1", true);
end_Hash.put("tHDFSOutput_1", System.currentTimeMillis());

				if(execStat){   
   	 				runStat.updateStatOnConnection("OnComponentOk2", 0, "ok");
				}
				tFileList_1Process(globalMap);



/**
 * [tHDFSOutput_1 end ] stop
 */



				}//end the resume

				



	
			}catch(java.lang.Exception e){	
				
				    if(!(e instanceof TalendException)){
					   log.fatal(currentComponent + " " + e.getMessage(),e);
					}
				
				TalendException te = new TalendException(e, currentComponent, globalMap);
				
				throw te;
			}catch(java.lang.Error error){	
				
					runStat.stopThreadStat();
				
				throw error;
			}finally{
				
				try{
					
	
	/**
	 * [tFixedFlowInput_1 finally ] start
	 */

	

	
	
	currentComponent="tFixedFlowInput_1";

	

 



/**
 * [tFixedFlowInput_1 finally ] stop
 */

	
	/**
	 * [tHDFSOutput_1 finally ] start
	 */

	

	
	
	currentComponent="tHDFSOutput_1";

	

 



/**
 * [tHDFSOutput_1 finally ] stop
 */



				}catch(java.lang.Exception e){	
					//ignore
				}catch(java.lang.Error error){
					//ignore
				}
				resourceMap = null;
			}
		

		globalMap.put("tFixedFlowInput_1_SUBPROCESS_STATE", 1);
	}
	

public void tFileList_1Process(final java.util.Map<String, Object> globalMap) throws TalendException {
	globalMap.put("tFileList_1_SUBPROCESS_STATE", 0);

 final boolean execStat = this.execStat;
	
		String iterateId = "";
	
	
	String currentComponent = "";
	java.util.Map<String, Object> resourceMap = new java.util.HashMap<String, Object>();

	try {
			// TDI-39566 avoid throwing an useless Exception
			boolean resumeIt = true;
			if (globalResumeTicket == false && resumeEntryMethodName != null) {
				String currentMethodName = new java.lang.Exception().getStackTrace()[0].getMethodName();
				resumeIt = resumeEntryMethodName.equals(currentMethodName);
			}
			if (resumeIt || globalResumeTicket) { //start the resume
				globalResumeTicket = true;



		


	
	/**
	 * [tFileList_1 begin ] start
	 */

				
			int NB_ITERATE_tRunJob_1 = 0; //for statistics
			

	
		
		ok_Hash.put("tFileList_1", false);
		start_Hash.put("tFileList_1", System.currentTimeMillis());
		
	
	currentComponent="tFileList_1";

	
		int tos_count_tFileList_1 = 0;
		
                if(log.isDebugEnabled())
            log.debug("tFileList_1 - "  + ("Start to work.") );
            if (log.isDebugEnabled()) {
                class BytesLimit65535_tFileList_1{
                    public void limitLog4jByte() throws Exception{
                    StringBuilder log4jParamters_tFileList_1 = new StringBuilder();
                    log4jParamters_tFileList_1.append("Parameters:");
                            log4jParamters_tFileList_1.append("DIRECTORY" + " = " + "context.W_PATH_IN");
                        log4jParamters_tFileList_1.append(" | ");
                            log4jParamters_tFileList_1.append("LIST_MODE" + " = " + "FILES");
                        log4jParamters_tFileList_1.append(" | ");
                            log4jParamters_tFileList_1.append("INCLUDSUBDIR" + " = " + "false");
                        log4jParamters_tFileList_1.append(" | ");
                            log4jParamters_tFileList_1.append("CASE_SENSITIVE" + " = " + "YES");
                        log4jParamters_tFileList_1.append(" | ");
                            log4jParamters_tFileList_1.append("ERROR" + " = " + "false");
                        log4jParamters_tFileList_1.append(" | ");
                            log4jParamters_tFileList_1.append("GLOBEXPRESSIONS" + " = " + "true");
                        log4jParamters_tFileList_1.append(" | ");
                            log4jParamters_tFileList_1.append("FILES" + " = " + "[{FILEMASK="+("context.W_F_FILEMASK")+"}]");
                        log4jParamters_tFileList_1.append(" | ");
                            log4jParamters_tFileList_1.append("ORDER_BY_NOTHING" + " = " + "false");
                        log4jParamters_tFileList_1.append(" | ");
                            log4jParamters_tFileList_1.append("ORDER_BY_FILENAME" + " = " + "false");
                        log4jParamters_tFileList_1.append(" | ");
                            log4jParamters_tFileList_1.append("ORDER_BY_FILESIZE" + " = " + "false");
                        log4jParamters_tFileList_1.append(" | ");
                            log4jParamters_tFileList_1.append("ORDER_BY_MODIFIEDDATE" + " = " + "true");
                        log4jParamters_tFileList_1.append(" | ");
                            log4jParamters_tFileList_1.append("ORDER_ACTION_ASC" + " = " + "true");
                        log4jParamters_tFileList_1.append(" | ");
                            log4jParamters_tFileList_1.append("ORDER_ACTION_DESC" + " = " + "false");
                        log4jParamters_tFileList_1.append(" | ");
                            log4jParamters_tFileList_1.append("IFEXCLUDE" + " = " + "false");
                        log4jParamters_tFileList_1.append(" | ");
                            log4jParamters_tFileList_1.append("FORMAT_FILEPATH_TO_SLASH" + " = " + "false");
                        log4jParamters_tFileList_1.append(" | ");
                if(log.isDebugEnabled())
            log.debug("tFileList_1 - "  + (log4jParamters_tFileList_1) );
                    } 
                } 
            new BytesLimit65535_tFileList_1().limitLog4jByte();
            }
			if(enableLogStash) {
				talendJobLog.addCM("tFileList_1", "tFileList_1", "tFileList");
				talendJobLogProcess(globalMap);
			}
			
	
 
  
				final StringBuffer log4jSb_tFileList_1 = new StringBuffer();
			   
    
  String directory_tFileList_1 = context.W_PATH_IN;
  final java.util.List<String> maskList_tFileList_1 = new java.util.ArrayList<String>();
  final java.util.List<java.util.regex.Pattern> patternList_tFileList_1 = new java.util.ArrayList<java.util.regex.Pattern>(); 
    maskList_tFileList_1.add(context.W_F_FILEMASK);  
  for (final String filemask_tFileList_1 : maskList_tFileList_1) {
	String filemask_compile_tFileList_1 = filemask_tFileList_1;
	
		filemask_compile_tFileList_1 = org.apache.oro.text.GlobCompiler.globToPerl5(filemask_tFileList_1.toCharArray(), org.apache.oro.text.GlobCompiler.DEFAULT_MASK);
	
		java.util.regex.Pattern fileNamePattern_tFileList_1 = java.util.regex.Pattern.compile(filemask_compile_tFileList_1);
	patternList_tFileList_1.add(fileNamePattern_tFileList_1);
  }
  int NB_FILEtFileList_1 = 0;

  final boolean case_sensitive_tFileList_1 = true;
	
	
		log.info("tFileList_1 - Starting to search for matching entries.");
	
	
    final java.util.List<java.io.File> list_tFileList_1 = new java.util.ArrayList<java.io.File>();
    final java.util.Set<String> filePath_tFileList_1 = new java.util.HashSet<String>();
	java.io.File file_tFileList_1 = new java.io.File(directory_tFileList_1);
     
		file_tFileList_1.listFiles(new java.io.FilenameFilter() {
			public boolean accept(java.io.File dir, String name) {
				java.io.File file = new java.io.File(dir, name);
                if (!file.isDirectory()) {
                	
    	String fileName_tFileList_1 = file.getName();
		for (final java.util.regex.Pattern fileNamePattern_tFileList_1 : patternList_tFileList_1) {
          	if (fileNamePattern_tFileList_1.matcher(fileName_tFileList_1).matches()){
					if(!filePath_tFileList_1.contains(file.getAbsolutePath())) {
			          list_tFileList_1.add(file);
			          filePath_tFileList_1.add(file.getAbsolutePath());
			        }
			}
		}
                }
              return true;
            }
          }
      ); 
        Comparator<java.io.File> lastModifiedASC_tFileList_1 = new Comparator<java.io.File>() {
        
          public int compare(java.io.File o1, java.io.File o2) {
            boolean bO1IsFile = o1.isFile();
            boolean bO2IsFile = o2.isFile();
            
            if ((bO1IsFile && bO2IsFile) || (!bO1IsFile && !bO2IsFile)) {
                if (o1.lastModified() == o2.lastModified()) {
                    return (o1.getName()).compareTo(o2.getName());
                } else if (o1.lastModified() > o2.lastModified()) {
                    return 1;
                } else {
                    return -1;
                }
            } else if (bO1IsFile && (!bO2IsFile)) {
                return 1;
            } else if ((!bO1IsFile) && bO2IsFile) {
                return -1;
            } else {
                return 0;
            }
          }
        };
      java.util.Collections.sort(list_tFileList_1, lastModifiedASC_tFileList_1);
    
		log.info("tFileList_1 - Start to list files.");
	
    for (int i_tFileList_1 = 0; i_tFileList_1 < list_tFileList_1.size(); i_tFileList_1++){
      java.io.File files_tFileList_1 = list_tFileList_1.get(i_tFileList_1);
      String fileName_tFileList_1 = files_tFileList_1.getName();
      
      String currentFileName_tFileList_1 = files_tFileList_1.getName(); 
      String currentFilePath_tFileList_1 = files_tFileList_1.getAbsolutePath();
      String currentFileDirectory_tFileList_1 = files_tFileList_1.getParent();
      String currentFileExtension_tFileList_1 = null;
      
      if (files_tFileList_1.getName().contains(".") && files_tFileList_1.isFile()){
        currentFileExtension_tFileList_1 = files_tFileList_1.getName().substring(files_tFileList_1.getName().lastIndexOf(".") + 1);
      } else{
        currentFileExtension_tFileList_1 = "";
      }
      
      NB_FILEtFileList_1 ++;
      globalMap.put("tFileList_1_CURRENT_FILE", currentFileName_tFileList_1);
      globalMap.put("tFileList_1_CURRENT_FILEPATH", currentFilePath_tFileList_1);
      globalMap.put("tFileList_1_CURRENT_FILEDIRECTORY", currentFileDirectory_tFileList_1);
      globalMap.put("tFileList_1_CURRENT_FILEEXTENSION", currentFileExtension_tFileList_1);
      globalMap.put("tFileList_1_NB_FILE", NB_FILEtFileList_1);
      
		log.info("tFileList_1 - Current file or directory path : " + currentFilePath_tFileList_1);
	  
 



/**
 * [tFileList_1 begin ] stop
 */
	
	/**
	 * [tFileList_1 main ] start
	 */

	

	
	
	currentComponent="tFileList_1";

	

 


	tos_count_tFileList_1++;

/**
 * [tFileList_1 main ] stop
 */
	
	/**
	 * [tFileList_1 process_data_begin ] start
	 */

	

	
	
	currentComponent="tFileList_1";

	

 



/**
 * [tFileList_1 process_data_begin ] stop
 */
	NB_ITERATE_tRunJob_1++;
	
	
					if(execStat){				
	       				runStat.updateStatOnConnection("OnComponentOk3", 3, 0);
					}           			
				
					if(execStat){				
	       				runStat.updateStatOnConnection("OnComponentOk8", 3, 0);
					}           			
				
					if(execStat){				
	       				runStat.updateStatOnConnection("OnComponentOk4", 3, 0);
					}           			
				
				if(execStat){
					runStat.updateStatOnConnection("iterate1", 1, "exec" + NB_ITERATE_tRunJob_1);
					//Thread.sleep(1000);
				}				
			

	
	/**
	 * [tRunJob_1 begin ] start
	 */

	

	
		
		ok_Hash.put("tRunJob_1", false);
		start_Hash.put("tRunJob_1", System.currentTimeMillis());
		
	
	currentComponent="tRunJob_1";

	
		int tos_count_tRunJob_1 = 0;
		
                if(log.isDebugEnabled())
            log.debug("tRunJob_1 - "  + ("Start to work.") );
            if (log.isDebugEnabled()) {
                class BytesLimit65535_tRunJob_1{
                    public void limitLog4jByte() throws Exception{
                    StringBuilder log4jParamters_tRunJob_1 = new StringBuilder();
                    log4jParamters_tRunJob_1.append("Parameters:");
                            log4jParamters_tRunJob_1.append("USE_DYNAMIC_JOB" + " = " + "false");
                        log4jParamters_tRunJob_1.append(" | ");
                            log4jParamters_tRunJob_1.append("PROCESS" + " = " + "S_INGESTION_IMPORT_FILE_STA");
                        log4jParamters_tRunJob_1.append(" | ");
                            log4jParamters_tRunJob_1.append("USE_INDEPENDENT_PROCESS" + " = " + "false");
                        log4jParamters_tRunJob_1.append(" | ");
                            log4jParamters_tRunJob_1.append("DIE_ON_CHILD_ERROR" + " = " + "true");
                        log4jParamters_tRunJob_1.append(" | ");
                            log4jParamters_tRunJob_1.append("TRANSMIT_WHOLE_CONTEXT" + " = " + "true");
                        log4jParamters_tRunJob_1.append(" | ");
                            log4jParamters_tRunJob_1.append("CONTEXTPARAMS" + " = " + "[{PARAM_NAME_COLUMN="+("W_FILENAME")+", PARAM_VALUE_COLUMN="+("(String)globalMap.get(\"tFileList_1_CURRENT_FILE\")")+"}]");
                        log4jParamters_tRunJob_1.append(" | ");
                            log4jParamters_tRunJob_1.append("PROPAGATE_CHILD_RESULT" + " = " + "false");
                        log4jParamters_tRunJob_1.append(" | ");
                            log4jParamters_tRunJob_1.append("PRINT_PARAMETER" + " = " + "false");
                        log4jParamters_tRunJob_1.append(" | ");
                            log4jParamters_tRunJob_1.append("USE_DYNAMIC_CONTEXT" + " = " + "false");
                        log4jParamters_tRunJob_1.append(" | ");
                if(log.isDebugEnabled())
            log.debug("tRunJob_1 - "  + (log4jParamters_tRunJob_1) );
                    } 
                } 
            new BytesLimit65535_tRunJob_1().limitLog4jByte();
            }
			if(enableLogStash) {
				talendJobLog.addCM("tRunJob_1", "__PROCESS__", "tRunJob");
				talendJobLogProcess(globalMap);
			}
			


 



/**
 * [tRunJob_1 begin ] stop
 */
	
	/**
	 * [tRunJob_1 main ] start
	 */

	

	
	
	currentComponent="tRunJob_1";

	
	java.util.List<String> paraList_tRunJob_1 = new java.util.ArrayList<String>();
	
	        				paraList_tRunJob_1.add("--father_pid="+pid);
	      			
	        				paraList_tRunJob_1.add("--root_pid="+rootPid);
	      			
	        				paraList_tRunJob_1.add("--father_node=tRunJob_1");
	      			
			if(!"".equals(log4jLevel)){
				paraList_tRunJob_1.add("--log4jLevel="+log4jLevel);
			}
		
		if(enableLogStash){
			paraList_tRunJob_1.add("--audit.enabled="+enableLogStash);
		}
		
	//for feature:10589
	
		paraList_tRunJob_1.add("--stat_port=" + portStats);
	

	if(resuming_logs_dir_path != null){
		paraList_tRunJob_1.add("--resuming_logs_dir_path=" + resuming_logs_dir_path);
	}
	String childResumePath_tRunJob_1 = ResumeUtil.getChildJobCheckPointPath(resuming_checkpoint_path);
	String tRunJobName_tRunJob_1 = ResumeUtil.getRighttRunJob(resuming_checkpoint_path);
	if("tRunJob_1".equals(tRunJobName_tRunJob_1) && childResumePath_tRunJob_1 != null){
		paraList_tRunJob_1.add("--resuming_checkpoint_path=" + ResumeUtil.getChildJobCheckPointPath(resuming_checkpoint_path));
	}
	paraList_tRunJob_1.add("--parent_part_launcher=JOB:" + jobName + "/NODE:tRunJob_1");
	
	java.util.Map<String, Object> parentContextMap_tRunJob_1 = new java.util.HashMap<String, Object>();

	
		
		context.synchronizeContext();
            class ContextProcessor_tRunJob_1 {
                    private void transmitContext_0() {
                    parentContextMap_tRunJob_1.put("W_BUSINESS_GROUP", context.W_BUSINESS_GROUP);
                    paraList_tRunJob_1.add("--context_type " + "W_BUSINESS_GROUP" + "=" + "id_String");
                    parentContextMap_tRunJob_1.put("W_BUSINESS_NAME", context.W_BUSINESS_NAME);
                    paraList_tRunJob_1.add("--context_type " + "W_BUSINESS_NAME" + "=" + "id_String");
                    parentContextMap_tRunJob_1.put("W_DATA_TYPE", context.W_DATA_TYPE);
                    paraList_tRunJob_1.add("--context_type " + "W_DATA_TYPE" + "=" + "id_String");
                    parentContextMap_tRunJob_1.put("W_DEFAULT_PARTITION", context.W_DEFAULT_PARTITION);
                    paraList_tRunJob_1.add("--context_type " + "W_DEFAULT_PARTITION" + "=" + "id_String");
                    parentContextMap_tRunJob_1.put("W_EDH_DB_ARCHIN", context.W_EDH_DB_ARCHIN);
                    paraList_tRunJob_1.add("--context_type " + "W_EDH_DB_ARCHIN" + "=" + "id_String");
                    parentContextMap_tRunJob_1.put("W_EDH_DB_H", context.W_EDH_DB_H);
                    paraList_tRunJob_1.add("--context_type " + "W_EDH_DB_H" + "=" + "id_String");
                    parentContextMap_tRunJob_1.put("W_EDH_DB_IN", context.W_EDH_DB_IN);
                    paraList_tRunJob_1.add("--context_type " + "W_EDH_DB_IN" + "=" + "id_String");
                    parentContextMap_tRunJob_1.put("W_EDH_TABLE_ARCHIN", context.W_EDH_TABLE_ARCHIN);
                    paraList_tRunJob_1.add("--context_type " + "W_EDH_TABLE_ARCHIN" + "=" + "id_String");
                    parentContextMap_tRunJob_1.put("W_EDH_TABLE_H", context.W_EDH_TABLE_H);
                    paraList_tRunJob_1.add("--context_type " + "W_EDH_TABLE_H" + "=" + "id_String");
                    parentContextMap_tRunJob_1.put("W_EDH_TABLE_IN", context.W_EDH_TABLE_IN);
                    paraList_tRunJob_1.add("--context_type " + "W_EDH_TABLE_IN" + "=" + "id_String");
                    parentContextMap_tRunJob_1.put("W_EXTRACTION_FIELDS", context.W_EXTRACTION_FIELDS);
                    paraList_tRunJob_1.add("--context_type " + "W_EXTRACTION_FIELDS" + "=" + "id_String");
                    parentContextMap_tRunJob_1.put("W_F_FILEMASK", context.W_F_FILEMASK);
                    paraList_tRunJob_1.add("--context_type " + "W_F_FILEMASK" + "=" + "id_String");
                    parentContextMap_tRunJob_1.put("W_FILENAME", context.W_FILENAME);
                    paraList_tRunJob_1.add("--context_type " + "W_FILENAME" + "=" + "id_String");
                    parentContextMap_tRunJob_1.put("W_FLAG_ABILITATA", context.W_FLAG_ABILITATA);
                    paraList_tRunJob_1.add("--context_type " + "W_FLAG_ABILITATA" + "=" + "id_String");
                    parentContextMap_tRunJob_1.put("W_FLAG_HEADER", context.W_FLAG_HEADER);
                    paraList_tRunJob_1.add("--context_type " + "W_FLAG_HEADER" + "=" + "id_String");
                    parentContextMap_tRunJob_1.put("W_HDFS_ETL_PATH", context.W_HDFS_ETL_PATH);
                    paraList_tRunJob_1.add("--context_type " + "W_HDFS_ETL_PATH" + "=" + "id_String");
                    parentContextMap_tRunJob_1.put("W_IMPORT_TYPE", context.W_IMPORT_TYPE);
                    paraList_tRunJob_1.add("--context_type " + "W_IMPORT_TYPE" + "=" + "id_String");
                    parentContextMap_tRunJob_1.put("W_LAST_PARTITION", context.W_LAST_PARTITION);
                    paraList_tRunJob_1.add("--context_type " + "W_LAST_PARTITION" + "=" + "id_String");
                    parentContextMap_tRunJob_1.put("W_MAPPER", context.W_MAPPER);
                    paraList_tRunJob_1.add("--context_type " + "W_MAPPER" + "=" + "id_String");
                    parentContextMap_tRunJob_1.put("W_PARTITION_FIELD", context.W_PARTITION_FIELD);
                    paraList_tRunJob_1.add("--context_type " + "W_PARTITION_FIELD" + "=" + "id_String");
                    parentContextMap_tRunJob_1.put("W_PATH_ARCHIN", context.W_PATH_ARCHIN);
                    paraList_tRunJob_1.add("--context_type " + "W_PATH_ARCHIN" + "=" + "id_String");
                    parentContextMap_tRunJob_1.put("W_PATH_IN", context.W_PATH_IN);
                    paraList_tRunJob_1.add("--context_type " + "W_PATH_IN" + "=" + "id_String");
                    parentContextMap_tRunJob_1.put("W_SOURCE_NAME", context.W_SOURCE_NAME);
                    paraList_tRunJob_1.add("--context_type " + "W_SOURCE_NAME" + "=" + "id_String");
                    parentContextMap_tRunJob_1.put("W_T_SOURCE_DATABASE", context.W_T_SOURCE_DATABASE);
                    paraList_tRunJob_1.add("--context_type " + "W_T_SOURCE_DATABASE" + "=" + "id_String");
                    parentContextMap_tRunJob_1.put("W_T_SOURCE_TABLE", context.W_T_SOURCE_TABLE);
                    paraList_tRunJob_1.add("--context_type " + "W_T_SOURCE_TABLE" + "=" + "id_String");
                    parentContextMap_tRunJob_1.put("DB_POSTGRES_CUSTOM_LOG_TABLE", context.DB_POSTGRES_CUSTOM_LOG_TABLE);
                    paraList_tRunJob_1.add("--context_type " + "DB_POSTGRES_CUSTOM_LOG_TABLE" + "=" + "id_String");
                    parentContextMap_tRunJob_1.put("DB_POSTGRES_Database", context.DB_POSTGRES_Database);
                    paraList_tRunJob_1.add("--context_type " + "DB_POSTGRES_Database" + "=" + "id_String");
                    parentContextMap_tRunJob_1.put("DB_POSTGRES_FILE_METADATA_TABLE", context.DB_POSTGRES_FILE_METADATA_TABLE);
                    paraList_tRunJob_1.add("--context_type " + "DB_POSTGRES_FILE_METADATA_TABLE" + "=" + "id_String");
                    parentContextMap_tRunJob_1.put("DB_POSTGRES_LOG_TABLE", context.DB_POSTGRES_LOG_TABLE);
                    paraList_tRunJob_1.add("--context_type " + "DB_POSTGRES_LOG_TABLE" + "=" + "id_String");
                    parentContextMap_tRunJob_1.put("DB_POSTGRES_Login", context.DB_POSTGRES_Login);
                    paraList_tRunJob_1.add("--context_type " + "DB_POSTGRES_Login" + "=" + "id_String");
                    parentContextMap_tRunJob_1.put("DB_POSTGRES_LOGTABLE", context.DB_POSTGRES_LOGTABLE);
                    paraList_tRunJob_1.add("--context_type " + "DB_POSTGRES_LOGTABLE" + "=" + "id_String");
                    parentContextMap_tRunJob_1.put("DB_POSTGRES_LOGVIEW", context.DB_POSTGRES_LOGVIEW);
                    paraList_tRunJob_1.add("--context_type " + "DB_POSTGRES_LOGVIEW" + "=" + "id_String");
                    parentContextMap_tRunJob_1.put("DB_POSTGRES_METADATA_FACT_RECUPERO_PARTIZIONI", context.DB_POSTGRES_METADATA_FACT_RECUPERO_PARTIZIONI);
                    paraList_tRunJob_1.add("--context_type " + "DB_POSTGRES_METADATA_FACT_RECUPERO_PARTIZIONI" + "=" + "id_String");
                    parentContextMap_tRunJob_1.put("DB_POSTGRES_METADATA_FACT_TABLE", context.DB_POSTGRES_METADATA_FACT_TABLE);
                    paraList_tRunJob_1.add("--context_type " + "DB_POSTGRES_METADATA_FACT_TABLE" + "=" + "id_String");
                    parentContextMap_tRunJob_1.put("DB_POSTGRES_Password", context.DB_POSTGRES_Password);
                    paraList_tRunJob_1.add("--context_type " + "DB_POSTGRES_Password" + "=" + "id_Password");
                    parentContextMap_tRunJob_1.put("DB_POSTGRES_Port", context.DB_POSTGRES_Port);
                    paraList_tRunJob_1.add("--context_type " + "DB_POSTGRES_Port" + "=" + "id_String");
                    parentContextMap_tRunJob_1.put("DB_POSTGRES_Schema", context.DB_POSTGRES_Schema);
                    paraList_tRunJob_1.add("--context_type " + "DB_POSTGRES_Schema" + "=" + "id_String");
                    parentContextMap_tRunJob_1.put("DB_POSTGRES_Server", context.DB_POSTGRES_Server);
                    paraList_tRunJob_1.add("--context_type " + "DB_POSTGRES_Server" + "=" + "id_String");
                    parentContextMap_tRunJob_1.put("DB_POSTGRES_SQOOP_METADATA_TABLE", context.DB_POSTGRES_SQOOP_METADATA_TABLE);
                    paraList_tRunJob_1.add("--context_type " + "DB_POSTGRES_SQOOP_METADATA_TABLE" + "=" + "id_String");
                    parentContextMap_tRunJob_1.put("DB_POSTGRES_STATTABLE", context.DB_POSTGRES_STATTABLE);
                    paraList_tRunJob_1.add("--context_type " + "DB_POSTGRES_STATTABLE" + "=" + "id_String");
                    parentContextMap_tRunJob_1.put("EDH_CLUSTER_dfs_client_failover_proxy_provider_edhcluster", context.EDH_CLUSTER_dfs_client_failover_proxy_provider_edhcluster);
                    paraList_tRunJob_1.add("--context_type " + "EDH_CLUSTER_dfs_client_failover_proxy_provider_edhcluster" + "=" + "id_String");
                    parentContextMap_tRunJob_1.put("EDH_CLUSTER_dfs_ha_namenodes_edhcluster", context.EDH_CLUSTER_dfs_ha_namenodes_edhcluster);
                    paraList_tRunJob_1.add("--context_type " + "EDH_CLUSTER_dfs_ha_namenodes_edhcluster" + "=" + "id_String");
                    parentContextMap_tRunJob_1.put("EDH_CLUSTER_dfs_namenode_rpc_address_edhcluster_namenode209", context.EDH_CLUSTER_dfs_namenode_rpc_address_edhcluster_namenode209);
                    paraList_tRunJob_1.add("--context_type " + "EDH_CLUSTER_dfs_namenode_rpc_address_edhcluster_namenode209" + "=" + "id_String");
                    parentContextMap_tRunJob_1.put("EDH_CLUSTER_dfs_namenode_rpc_address_edhcluster_namenode264", context.EDH_CLUSTER_dfs_namenode_rpc_address_edhcluster_namenode264);
                    paraList_tRunJob_1.add("--context_type " + "EDH_CLUSTER_dfs_namenode_rpc_address_edhcluster_namenode264" + "=" + "id_String");
                    parentContextMap_tRunJob_1.put("EDH_CLUSTER_dfs_nameservices", context.EDH_CLUSTER_dfs_nameservices);
                    paraList_tRunJob_1.add("--context_type " + "EDH_CLUSTER_dfs_nameservices" + "=" + "id_String");
                    parentContextMap_tRunJob_1.put("EDH_CLUSTER_ha_zookeeper_quorum", context.EDH_CLUSTER_ha_zookeeper_quorum);
                    paraList_tRunJob_1.add("--context_type " + "EDH_CLUSTER_ha_zookeeper_quorum" + "=" + "id_String");
                    parentContextMap_tRunJob_1.put("EDH_CLUSTER_JobHistory", context.EDH_CLUSTER_JobHistory);
                    paraList_tRunJob_1.add("--context_type " + "EDH_CLUSTER_JobHistory" + "=" + "id_String");
                    parentContextMap_tRunJob_1.put("EDH_CLUSTER_JobHistroyPrin", context.EDH_CLUSTER_JobHistroyPrin);
                    paraList_tRunJob_1.add("--context_type " + "EDH_CLUSTER_JobHistroyPrin" + "=" + "id_String");
                    parentContextMap_tRunJob_1.put("EDH_CLUSTER_JTOrRMPrin", context.EDH_CLUSTER_JTOrRMPrin);
                    paraList_tRunJob_1.add("--context_type " + "EDH_CLUSTER_JTOrRMPrin" + "=" + "id_String");
                    parentContextMap_tRunJob_1.put("EDH_CLUSTER_KeyTab", context.EDH_CLUSTER_KeyTab);
                    paraList_tRunJob_1.add("--context_type " + "EDH_CLUSTER_KeyTab" + "=" + "id_String");
                    parentContextMap_tRunJob_1.put("EDH_CLUSTER_NameNodePrin", context.EDH_CLUSTER_NameNodePrin);
                    paraList_tRunJob_1.add("--context_type " + "EDH_CLUSTER_NameNodePrin" + "=" + "id_String");
                    parentContextMap_tRunJob_1.put("EDH_CLUSTER_NameNodeUri", context.EDH_CLUSTER_NameNodeUri);
                    paraList_tRunJob_1.add("--context_type " + "EDH_CLUSTER_NameNodeUri" + "=" + "id_String");
                    parentContextMap_tRunJob_1.put("EDH_CLUSTER_Principal", context.EDH_CLUSTER_Principal);
                    paraList_tRunJob_1.add("--context_type " + "EDH_CLUSTER_Principal" + "=" + "id_String");
                    parentContextMap_tRunJob_1.put("EDH_CLUSTER_ResourceManager", context.EDH_CLUSTER_ResourceManager);
                    paraList_tRunJob_1.add("--context_type " + "EDH_CLUSTER_ResourceManager" + "=" + "id_String");
                    parentContextMap_tRunJob_1.put("EDH_CLUSTER_ResourceManagerScheduler", context.EDH_CLUSTER_ResourceManagerScheduler);
                    paraList_tRunJob_1.add("--context_type " + "EDH_CLUSTER_ResourceManagerScheduler" + "=" + "id_String");
                    parentContextMap_tRunJob_1.put("EDH_CLUSTER_StagingDirectory", context.EDH_CLUSTER_StagingDirectory);
                    paraList_tRunJob_1.add("--context_type " + "EDH_CLUSTER_StagingDirectory" + "=" + "id_String");
                    parentContextMap_tRunJob_1.put("EDH_CLUSTER_username", context.EDH_CLUSTER_username);
                    paraList_tRunJob_1.add("--context_type " + "EDH_CLUSTER_username" + "=" + "id_String");
                    parentContextMap_tRunJob_1.put("EDH_CLUSTER_yarn_resourcemanager_address_rm1", context.EDH_CLUSTER_yarn_resourcemanager_address_rm1);
                    paraList_tRunJob_1.add("--context_type " + "EDH_CLUSTER_yarn_resourcemanager_address_rm1" + "=" + "id_String");
                    parentContextMap_tRunJob_1.put("EDH_CLUSTER_yarn_resourcemanager_address_rm2", context.EDH_CLUSTER_yarn_resourcemanager_address_rm2);
                    paraList_tRunJob_1.add("--context_type " + "EDH_CLUSTER_yarn_resourcemanager_address_rm2" + "=" + "id_String");
                    parentContextMap_tRunJob_1.put("EDH_CLUSTER_yarn_resourcemanager_ha_enabled", context.EDH_CLUSTER_yarn_resourcemanager_ha_enabled);
                    paraList_tRunJob_1.add("--context_type " + "EDH_CLUSTER_yarn_resourcemanager_ha_enabled" + "=" + "id_String");
                    parentContextMap_tRunJob_1.put("EDH_CLUSTER_yarn_resourcemanager_ha_rm_ids", context.EDH_CLUSTER_yarn_resourcemanager_ha_rm_ids);
                    paraList_tRunJob_1.add("--context_type " + "EDH_CLUSTER_yarn_resourcemanager_ha_rm_ids" + "=" + "id_String");
                    parentContextMap_tRunJob_1.put("EDH_CLUSTER_yarn_resourcemanager_scheduler_address_rm1", context.EDH_CLUSTER_yarn_resourcemanager_scheduler_address_rm1);
                    paraList_tRunJob_1.add("--context_type " + "EDH_CLUSTER_yarn_resourcemanager_scheduler_address_rm1" + "=" + "id_String");
                    parentContextMap_tRunJob_1.put("EDH_CLUSTER_yarn_resourcemanager_scheduler_address_rm2", context.EDH_CLUSTER_yarn_resourcemanager_scheduler_address_rm2);
                    paraList_tRunJob_1.add("--context_type " + "EDH_CLUSTER_yarn_resourcemanager_scheduler_address_rm2" + "=" + "id_String");
                    parentContextMap_tRunJob_1.put("EDH_CLUSTER_HDFS_HdfsFileSeparator", context.EDH_CLUSTER_HDFS_HdfsFileSeparator);
                    paraList_tRunJob_1.add("--context_type " + "EDH_CLUSTER_HDFS_HdfsFileSeparator" + "=" + "id_String");
                    parentContextMap_tRunJob_1.put("EDH_CLUSTER_HDFS_HdfsRowSeparator", context.EDH_CLUSTER_HDFS_HdfsRowSeparator);
                    paraList_tRunJob_1.add("--context_type " + "EDH_CLUSTER_HDFS_HdfsRowSeparator" + "=" + "id_String");
                    parentContextMap_tRunJob_1.put("EDH_CLUSTER_HIVE_Database", context.EDH_CLUSTER_HIVE_Database);
                    paraList_tRunJob_1.add("--context_type " + "EDH_CLUSTER_HIVE_Database" + "=" + "id_String");
                    parentContextMap_tRunJob_1.put("EDH_CLUSTER_HIVE_dynamicPart", context.EDH_CLUSTER_HIVE_dynamicPart);
                    paraList_tRunJob_1.add("--context_type " + "EDH_CLUSTER_HIVE_dynamicPart" + "=" + "id_String");
                    parentContextMap_tRunJob_1.put("EDH_CLUSTER_HIVE_dynamicPartMax", context.EDH_CLUSTER_HIVE_dynamicPartMax);
                    paraList_tRunJob_1.add("--context_type " + "EDH_CLUSTER_HIVE_dynamicPartMax" + "=" + "id_String");
                    parentContextMap_tRunJob_1.put("EDH_CLUSTER_HIVE_dynamicPartMaxPerNode", context.EDH_CLUSTER_HIVE_dynamicPartMaxPerNode);
                    paraList_tRunJob_1.add("--context_type " + "EDH_CLUSTER_HIVE_dynamicPartMaxPerNode" + "=" + "id_String");
                    parentContextMap_tRunJob_1.put("EDH_CLUSTER_HIVE_executionEngine", context.EDH_CLUSTER_HIVE_executionEngine);
                    paraList_tRunJob_1.add("--context_type " + "EDH_CLUSTER_HIVE_executionEngine" + "=" + "id_String");
                    parentContextMap_tRunJob_1.put("EDH_CLUSTER_HIVE_hiveAdditionalJDBCParameters", context.EDH_CLUSTER_HIVE_hiveAdditionalJDBCParameters);
                    paraList_tRunJob_1.add("--context_type " + "EDH_CLUSTER_HIVE_hiveAdditionalJDBCParameters" + "=" + "id_String");
                    parentContextMap_tRunJob_1.put("EDH_CLUSTER_HIVE_HiveKeyTab", context.EDH_CLUSTER_HIVE_HiveKeyTab);
                    paraList_tRunJob_1.add("--context_type " + "EDH_CLUSTER_HIVE_HiveKeyTab" + "=" + "id_String");
                    parentContextMap_tRunJob_1.put("EDH_CLUSTER_HIVE_HiveKeyTabPrincipal", context.EDH_CLUSTER_HIVE_HiveKeyTabPrincipal);
                    paraList_tRunJob_1.add("--context_type " + "EDH_CLUSTER_HIVE_HiveKeyTabPrincipal" + "=" + "id_String");
                    parentContextMap_tRunJob_1.put("EDH_CLUSTER_HIVE_HivePrincipal", context.EDH_CLUSTER_HIVE_HivePrincipal);
                    paraList_tRunJob_1.add("--context_type " + "EDH_CLUSTER_HIVE_HivePrincipal" + "=" + "id_String");
                    parentContextMap_tRunJob_1.put("EDH_CLUSTER_HIVE_hiveSSLTrustStorePassword", context.EDH_CLUSTER_HIVE_hiveSSLTrustStorePassword);
                    paraList_tRunJob_1.add("--context_type " + "EDH_CLUSTER_HIVE_hiveSSLTrustStorePassword" + "=" + "id_Password");
                    parentContextMap_tRunJob_1.put("EDH_CLUSTER_HIVE_hiveSSLTrustStorePath", context.EDH_CLUSTER_HIVE_hiveSSLTrustStorePath);
                    paraList_tRunJob_1.add("--context_type " + "EDH_CLUSTER_HIVE_hiveSSLTrustStorePath" + "=" + "id_String");
                    parentContextMap_tRunJob_1.put("EDH_CLUSTER_HIVE_Login", context.EDH_CLUSTER_HIVE_Login);
                    paraList_tRunJob_1.add("--context_type " + "EDH_CLUSTER_HIVE_Login" + "=" + "id_String");
                    parentContextMap_tRunJob_1.put("EDH_CLUSTER_HIVE_Password", context.EDH_CLUSTER_HIVE_Password);
                    paraList_tRunJob_1.add("--context_type " + "EDH_CLUSTER_HIVE_Password" + "=" + "id_Password");
                    parentContextMap_tRunJob_1.put("EDH_CLUSTER_HIVE_Port", context.EDH_CLUSTER_HIVE_Port);
                    paraList_tRunJob_1.add("--context_type " + "EDH_CLUSTER_HIVE_Port" + "=" + "id_String");
                    parentContextMap_tRunJob_1.put("EDH_CLUSTER_HIVE_Server", context.EDH_CLUSTER_HIVE_Server);
                    paraList_tRunJob_1.add("--context_type " + "EDH_CLUSTER_HIVE_Server" + "=" + "id_String");
                    parentContextMap_tRunJob_1.put("EDH_CLUSTER_IMPALA_Database", context.EDH_CLUSTER_IMPALA_Database);
                    paraList_tRunJob_1.add("--context_type " + "EDH_CLUSTER_IMPALA_Database" + "=" + "id_String");
                    parentContextMap_tRunJob_1.put("EDH_CLUSTER_IMPALA_ImpalaPrincipal", context.EDH_CLUSTER_IMPALA_ImpalaPrincipal);
                    paraList_tRunJob_1.add("--context_type " + "EDH_CLUSTER_IMPALA_ImpalaPrincipal" + "=" + "id_String");
                    parentContextMap_tRunJob_1.put("EDH_CLUSTER_IMPALA_Login", context.EDH_CLUSTER_IMPALA_Login);
                    paraList_tRunJob_1.add("--context_type " + "EDH_CLUSTER_IMPALA_Login" + "=" + "id_String");
                    parentContextMap_tRunJob_1.put("EDH_CLUSTER_IMPALA_Port", context.EDH_CLUSTER_IMPALA_Port);
                    paraList_tRunJob_1.add("--context_type " + "EDH_CLUSTER_IMPALA_Port" + "=" + "id_String");
                    parentContextMap_tRunJob_1.put("EDH_CLUSTER_IMPALA_Server", context.EDH_CLUSTER_IMPALA_Server);
                    paraList_tRunJob_1.add("--context_type " + "EDH_CLUSTER_IMPALA_Server" + "=" + "id_String");
                    parentContextMap_tRunJob_1.put("DB_POSTGRES_configuration_metadata", context.DB_POSTGRES_configuration_metadata);
                    paraList_tRunJob_1.add("--context_type " + "DB_POSTGRES_configuration_metadata" + "=" + "id_String");
                        }
                    public void transmitAllContext() {
                        transmitContext_0();
                    }
            }
            new ContextProcessor_tRunJob_1().transmitAllContext();
		java.util.Enumeration<?> propertyNames_tRunJob_1 = context.propertyNames();
		while (propertyNames_tRunJob_1.hasMoreElements()) {
			String key_tRunJob_1 = (String) propertyNames_tRunJob_1.nextElement();
			Object value_tRunJob_1 = (Object) context.get(key_tRunJob_1);
			if(value_tRunJob_1!=null) {  
				paraList_tRunJob_1.add("--context_param " + key_tRunJob_1 + "=" + value_tRunJob_1);
			} else {
				paraList_tRunJob_1.add("--context_param " + key_tRunJob_1 + "=" + NULL_VALUE_EXPRESSION_IN_COMMAND_STRING_FOR_CHILD_JOB_ONLY);
			}
			
		}
		

	Object obj_tRunJob_1 = null;

	
		obj_tRunJob_1 = (String)globalMap.get("tFileList_1_CURRENT_FILE");
		if(obj_tRunJob_1!=null) {
			if (obj_tRunJob_1.getClass().getName().equals("java.util.Date")) {
				paraList_tRunJob_1.add("--context_param W_FILENAME=" + ((java.util.Date) obj_tRunJob_1).getTime());
			} else {
				paraList_tRunJob_1.add("--context_param W_FILENAME=" + RuntimeUtils.tRunJobConvertContext(obj_tRunJob_1));
			}
		} else {
			paraList_tRunJob_1.add("--context_param W_FILENAME=" + NULL_VALUE_EXPRESSION_IN_COMMAND_STRING_FOR_CHILD_JOB_ONLY);
		}
		
		parentContextMap_tRunJob_1.put("W_FILENAME", obj_tRunJob_1);
	
	
		datahub_aml.s_ingestion_import_file_sta_0_1.S_INGESTION_IMPORT_FILE_STA childJob_tRunJob_1 = new datahub_aml.s_ingestion_import_file_sta_0_1.S_INGESTION_IMPORT_FILE_STA();
	    // pass DataSources
	    java.util.Map<String, routines.system.TalendDataSource> talendDataSources_tRunJob_1 = (java.util.Map<String, routines.system.TalendDataSource>) globalMap
	            .get(KEY_DB_DATASOURCES);
	    if (null != talendDataSources_tRunJob_1) {
	        java.util.Map<String, javax.sql.DataSource> dataSources_tRunJob_1 = new java.util.HashMap<String, javax.sql.DataSource>();
	        for (java.util.Map.Entry<String, routines.system.TalendDataSource> talendDataSourceEntry_tRunJob_1 : talendDataSources_tRunJob_1
			        .entrySet()) {
	            dataSources_tRunJob_1.put(talendDataSourceEntry_tRunJob_1.getKey(),
	                    talendDataSourceEntry_tRunJob_1.getValue().getRawDataSource());
	        }
	        childJob_tRunJob_1.setDataSources(dataSources_tRunJob_1);
	    }
		  
			childJob_tRunJob_1.parentContextMap = parentContextMap_tRunJob_1;
		  
		
			log.info("tRunJob_1 - The child job 'datahub_aml.s_ingestion_import_file_sta_0_1.S_INGESTION_IMPORT_FILE_STA' starts on the version '0.1' with the context 'LOCAL'.");
		
		String[][] childReturn_tRunJob_1 = childJob_tRunJob_1.runJob((String[]) paraList_tRunJob_1.toArray(new String[paraList_tRunJob_1.size()]));
		
			log.info("tRunJob_1 - The child job 'datahub_aml.s_ingestion_import_file_sta_0_1.S_INGESTION_IMPORT_FILE_STA' is done.");
		
            if(childJob_tRunJob_1.getErrorCode() == null){
                globalMap.put("tRunJob_1_CHILD_RETURN_CODE", childJob_tRunJob_1.getStatus() != null && ("failure").equals(childJob_tRunJob_1.getStatus()) ? 1 : 0);
            }else{
                globalMap.put("tRunJob_1_CHILD_RETURN_CODE", childJob_tRunJob_1.getErrorCode());
            }
            if (childJob_tRunJob_1.getExceptionStackTrace() != null) {
                globalMap.put("tRunJob_1_CHILD_EXCEPTION_STACKTRACE", childJob_tRunJob_1.getExceptionStackTrace());
            }
                    errorCode = childJob_tRunJob_1.getErrorCode();
                if (childJob_tRunJob_1.getErrorCode() != null || ("failure").equals(childJob_tRunJob_1.getStatus())) {
                    java.lang.Exception ce_tRunJob_1 = childJob_tRunJob_1.getException();
                    throw new RuntimeException("Child job running failed.\n" + ((ce_tRunJob_1!=null) ? (ce_tRunJob_1.getClass().getName() + ": " + ce_tRunJob_1.getMessage()) : ""));
                }

 


	tos_count_tRunJob_1++;

/**
 * [tRunJob_1 main ] stop
 */
	
	/**
	 * [tRunJob_1 process_data_begin ] start
	 */

	

	
	
	currentComponent="tRunJob_1";

	

 



/**
 * [tRunJob_1 process_data_begin ] stop
 */
	
	/**
	 * [tRunJob_1 process_data_end ] start
	 */

	

	
	
	currentComponent="tRunJob_1";

	

 



/**
 * [tRunJob_1 process_data_end ] stop
 */
	
	/**
	 * [tRunJob_1 end ] start
	 */

	

	
	
	currentComponent="tRunJob_1";

	

 
                if(log.isDebugEnabled())
            log.debug("tRunJob_1 - "  + ("Done.") );

ok_Hash.put("tRunJob_1", true);
end_Hash.put("tRunJob_1", System.currentTimeMillis());

				if(execStat){   
   	 				runStat.updateStatOnConnection("OnComponentOk3", 0, "ok");
				}
				tJava_1Process(globalMap);



/**
 * [tRunJob_1 end ] stop
 */
						if(execStat){
							runStat.updateStatOnConnection("iterate1", 2, "exec" + NB_ITERATE_tRunJob_1);
						}				
					




	
	/**
	 * [tFileList_1 process_data_end ] start
	 */

	

	
	
	currentComponent="tFileList_1";

	

 



/**
 * [tFileList_1 process_data_end ] stop
 */
	
	/**
	 * [tFileList_1 end ] start
	 */

	

	
	
	currentComponent="tFileList_1";

	

  
    }
  globalMap.put("tFileList_1_NB_FILE", NB_FILEtFileList_1);
  
    log.info("tFileList_1 - File or directory count : " + NB_FILEtFileList_1);

  
 

 
                if(log.isDebugEnabled())
            log.debug("tFileList_1 - "  + ("Done.") );

ok_Hash.put("tFileList_1", true);
end_Hash.put("tFileList_1", System.currentTimeMillis());




/**
 * [tFileList_1 end ] stop
 */
				}//end the resume

				



	
			}catch(java.lang.Exception e){	
				
				    if(!(e instanceof TalendException)){
					   log.fatal(currentComponent + " " + e.getMessage(),e);
					}
				
				TalendException te = new TalendException(e, currentComponent, globalMap);
				
				throw te;
			}catch(java.lang.Error error){	
				
					runStat.stopThreadStat();
				
				throw error;
			}finally{
				
				try{
					
	
	/**
	 * [tFileList_1 finally ] start
	 */

	

	
	
	currentComponent="tFileList_1";

	

 



/**
 * [tFileList_1 finally ] stop
 */

	
	/**
	 * [tRunJob_1 finally ] start
	 */

	

	
	
	currentComponent="tRunJob_1";

	

 



/**
 * [tRunJob_1 finally ] stop
 */



				}catch(java.lang.Exception e){	
					//ignore
				}catch(java.lang.Error error){
					//ignore
				}
				resourceMap = null;
			}
		

		globalMap.put("tFileList_1_SUBPROCESS_STATE", 1);
	}
	

public void tJava_1Process(final java.util.Map<String, Object> globalMap) throws TalendException {
	globalMap.put("tJava_1_SUBPROCESS_STATE", 0);

 final boolean execStat = this.execStat;
	
		String iterateId = "";
	
	
	String currentComponent = "";
	java.util.Map<String, Object> resourceMap = new java.util.HashMap<String, Object>();

	try {
			// TDI-39566 avoid throwing an useless Exception
			boolean resumeIt = true;
			if (globalResumeTicket == false && resumeEntryMethodName != null) {
				String currentMethodName = new java.lang.Exception().getStackTrace()[0].getMethodName();
				resumeIt = resumeEntryMethodName.equals(currentMethodName);
			}
			if (resumeIt || globalResumeTicket) { //start the resume
				globalResumeTicket = true;



		


	
	/**
	 * [tJava_1 begin ] start
	 */

	

	
		
		ok_Hash.put("tJava_1", false);
		start_Hash.put("tJava_1", System.currentTimeMillis());
		
	
	currentComponent="tJava_1";

	
		int tos_count_tJava_1 = 0;
		
			if(enableLogStash) {
				talendJobLog.addCM("tJava_1", "tJava_1", "tJava");
				talendJobLogProcess(globalMap);
			}
			


String hive_query_file="INSERT INTO TABLE  "  + context.W_EDH_DB_H+".H_FILE_INGESTION SELECT '"+((String)globalMap.get("tFileList_1_CURRENT_FILE"))+"' nome_file, "+context.W_PARTITION_FIELD+" DATA_PRODUZIONE";

System.out.println(hive_query_file);

context.setProperty("hive_query_file", hive_query_file);
 



/**
 * [tJava_1 begin ] stop
 */
	
	/**
	 * [tJava_1 main ] start
	 */

	

	
	
	currentComponent="tJava_1";

	

 


	tos_count_tJava_1++;

/**
 * [tJava_1 main ] stop
 */
	
	/**
	 * [tJava_1 process_data_begin ] start
	 */

	

	
	
	currentComponent="tJava_1";

	

 



/**
 * [tJava_1 process_data_begin ] stop
 */
	
	/**
	 * [tJava_1 process_data_end ] start
	 */

	

	
	
	currentComponent="tJava_1";

	

 



/**
 * [tJava_1 process_data_end ] stop
 */
	
	/**
	 * [tJava_1 end ] start
	 */

	

	
	
	currentComponent="tJava_1";

	

 

ok_Hash.put("tJava_1", true);
end_Hash.put("tJava_1", System.currentTimeMillis());

				if(execStat){   
   	 				runStat.updateStatOnConnection("OnComponentOk4", 0, "ok");
				}
				tHiveRow_1Process(globalMap);



/**
 * [tJava_1 end ] stop
 */
				}//end the resume

				



	
			}catch(java.lang.Exception e){	
				
				    if(!(e instanceof TalendException)){
					   log.fatal(currentComponent + " " + e.getMessage(),e);
					}
				
				TalendException te = new TalendException(e, currentComponent, globalMap);
				
				throw te;
			}catch(java.lang.Error error){	
				
					runStat.stopThreadStat();
				
				throw error;
			}finally{
				
				try{
					
	
	/**
	 * [tJava_1 finally ] start
	 */

	

	
	
	currentComponent="tJava_1";

	

 



/**
 * [tJava_1 finally ] stop
 */
				}catch(java.lang.Exception e){	
					//ignore
				}catch(java.lang.Error error){
					//ignore
				}
				resourceMap = null;
			}
		

		globalMap.put("tJava_1_SUBPROCESS_STATE", 1);
	}
	

public void tHiveRow_1Process(final java.util.Map<String, Object> globalMap) throws TalendException {
	globalMap.put("tHiveRow_1_SUBPROCESS_STATE", 0);

 final boolean execStat = this.execStat;
	
		String iterateId = "";
	
	
	String currentComponent = "";
	java.util.Map<String, Object> resourceMap = new java.util.HashMap<String, Object>();

	try {
			// TDI-39566 avoid throwing an useless Exception
			boolean resumeIt = true;
			if (globalResumeTicket == false && resumeEntryMethodName != null) {
				String currentMethodName = new java.lang.Exception().getStackTrace()[0].getMethodName();
				resumeIt = resumeEntryMethodName.equals(currentMethodName);
			}
			if (resumeIt || globalResumeTicket) { //start the resume
				globalResumeTicket = true;



		


	
	/**
	 * [tHiveRow_1 begin ] start
	 */

	

	
		
		ok_Hash.put("tHiveRow_1", false);
		start_Hash.put("tHiveRow_1", System.currentTimeMillis());
		
	
	currentComponent="tHiveRow_1";

	
		int tos_count_tHiveRow_1 = 0;
		
                if(log.isDebugEnabled())
            log.debug("tHiveRow_1 - "  + ("Start to work.") );
            if (log.isDebugEnabled()) {
                class BytesLimit65535_tHiveRow_1{
                    public void limitLog4jByte() throws Exception{
                    StringBuilder log4jParamters_tHiveRow_1 = new StringBuilder();
                    log4jParamters_tHiveRow_1.append("Parameters:");
                            log4jParamters_tHiveRow_1.append("USE_EXISTING_CONNECTION" + " = " + "true");
                        log4jParamters_tHiveRow_1.append(" | ");
                            log4jParamters_tHiveRow_1.append("CONNECTION" + " = " + "tHiveConnection_1");
                        log4jParamters_tHiveRow_1.append(" | ");
                            log4jParamters_tHiveRow_1.append("QUERYSTORE" + " = " + "\"\"");
                        log4jParamters_tHiveRow_1.append(" | ");
                            log4jParamters_tHiveRow_1.append("USE_PARQUET" + " = " + "false");
                        log4jParamters_tHiveRow_1.append(" | ");
                            log4jParamters_tHiveRow_1.append("QUERY" + " = " + "context.getProperty(\"hive_query_file\");");
                        log4jParamters_tHiveRow_1.append(" | ");
                            log4jParamters_tHiveRow_1.append("DIE_ON_ERROR" + " = " + "true");
                        log4jParamters_tHiveRow_1.append(" | ");
                            log4jParamters_tHiveRow_1.append("SET_TEMP_PATH" + " = " + "false");
                        log4jParamters_tHiveRow_1.append(" | ");
                            log4jParamters_tHiveRow_1.append("PROPAGATE_RECORD_SET" + " = " + "false");
                        log4jParamters_tHiveRow_1.append(" | ");
                            log4jParamters_tHiveRow_1.append("CLASSPATH_SEPARATOR" + " = " + "\":\"");
                        log4jParamters_tHiveRow_1.append(" | ");
                if(log.isDebugEnabled())
            log.debug("tHiveRow_1 - "  + (log4jParamters_tHiveRow_1) );
                    } 
                } 
            new BytesLimit65535_tHiveRow_1().limitLog4jByte();
            }
			if(enableLogStash) {
				talendJobLog.addCM("tHiveRow_1", "tHiveRow_1", "tHiveRow");
				talendJobLogProcess(globalMap);
			}
			


		


java.sql.Connection conn_tHiveRow_1 = null;
	globalMap.put("current_client_path_separator", System.getProperty("path.separator"));
	System.setProperty("path.separator", ":");
		conn_tHiveRow_1 = (java.sql.Connection)globalMap.get("conn_tHiveConnection_1");

		String dbname_tHiveRow_1 = (String)globalMap.get("db_tHiveConnection_1");
    	if(dbname_tHiveRow_1!=null && !"".equals(dbname_tHiveRow_1.trim()) && !"default".equals(dbname_tHiveRow_1.trim())) {
        	java.sql.Statement goToDatabase_tHiveRow_1 = conn_tHiveRow_1.createStatement();
        	goToDatabase_tHiveRow_1.execute("use " + dbname_tHiveRow_1);
        	goToDatabase_tHiveRow_1.close();
    	}

    	String dbUser_tHiveRow_1 = (String)globalMap.get("dbUser_tHiveConnection_1");

    	globalMap.put("HADOOP_USER_NAME_tHiveRow_1", System.getProperty("HADOOP_USER_NAME"));
		if(dbUser_tHiveRow_1!=null && !"".equals(dbUser_tHiveRow_1.trim())) {
			System.setProperty("HADOOP_USER_NAME",dbUser_tHiveRow_1);
			//make relative file path work for hive
			globalMap.put("current_client_user_name", System.getProperty("user.name"));
			System.setProperty("user.name",dbUser_tHiveRow_1);
		}
		
		java.sql.Statement stmt_tHiveRow_1 = conn_tHiveRow_1.createStatement();
		
	String query_tHiveRow_1 = "";
	boolean whetherReject_tHiveRow_1 = false;

 



/**
 * [tHiveRow_1 begin ] stop
 */
	
	/**
	 * [tHiveRow_1 main ] start
	 */

	

	
	
	currentComponent="tHiveRow_1";

	

	query_tHiveRow_1 = context.getProperty("hive_query_file");;
	whetherReject_tHiveRow_1 = false;

	
	globalMap.put("tHiveRow_1_QUERY",query_tHiveRow_1);
	
	try {
		

java.text.DateFormat dateStrFormat_tHiveRow_1 = new java.text.SimpleDateFormat("yyyyMMddHHmmss");
final String queryIdentifier_tHiveRow_1 = projectName + "_" + jobName + "_" + jobVersion.replace(".", "_") + "_tHiveRow_1_" + dateStrFormat_tHiveRow_1.format(new Date(startTime));
// For MapReduce Mode
stmt_tHiveRow_1.execute("set mapred.job.name=" + queryIdentifier_tHiveRow_1);
		
		  	stmt_tHiveRow_1.execute(query_tHiveRow_1);
	    	
		
			log.info("Query sent to HiveServer2 to be executed with [Application Name(MapReduce)/DAG Name(Tez): " + queryIdentifier_tHiveRow_1 + "]");
		
	    } catch (java.lang.Exception e) {
globalMap.put("tHiveRow_1_ERROR_MESSAGE",e.getMessage());
	        whetherReject_tHiveRow_1 = true;
	        
	            throw(e);
	            
	    }
		
	    if(!whetherReject_tHiveRow_1) {
	        
	    }
	    

 


	tos_count_tHiveRow_1++;

/**
 * [tHiveRow_1 main ] stop
 */
	
	/**
	 * [tHiveRow_1 process_data_begin ] start
	 */

	

	
	
	currentComponent="tHiveRow_1";

	

 



/**
 * [tHiveRow_1 process_data_begin ] stop
 */
	
	/**
	 * [tHiveRow_1 process_data_end ] start
	 */

	

	
	
	currentComponent="tHiveRow_1";

	

 



/**
 * [tHiveRow_1 process_data_end ] stop
 */
	
	/**
	 * [tHiveRow_1 end ] start
	 */

	

	
	
	currentComponent="tHiveRow_1";

	

		stmt_tHiveRow_1.close();	
		
		

		String currentClientPathSeparator_tHiveRow_1 = (String)globalMap.get("current_client_path_separator");
		if(currentClientPathSeparator_tHiveRow_1!=null) {
			System.setProperty("path.separator", currentClientPathSeparator_tHiveRow_1);
			globalMap.put("current_client_path_separator", null);
		}
		
		String currentClientUsername_tHiveRow_1 = (String)globalMap.get("current_client_user_name");
		if(currentClientUsername_tHiveRow_1!=null) {
			System.setProperty("user.name", currentClientUsername_tHiveRow_1);
			globalMap.put("current_client_user_name", null);
		}
		
		String originalHadoopUsername_tHiveRow_1 = (String)globalMap.get("HADOOP_USER_NAME_tHiveRow_1");
		if(originalHadoopUsername_tHiveRow_1!=null) {
			System.setProperty("HADOOP_USER_NAME", originalHadoopUsername_tHiveRow_1);
			globalMap.put("HADOOP_USER_NAME_tHiveRow_1", null);
		} else {
			System.clearProperty("HADOOP_USER_NAME");
		}

 
                if(log.isDebugEnabled())
            log.debug("tHiveRow_1 - "  + ("Done.") );

ok_Hash.put("tHiveRow_1", true);
end_Hash.put("tHiveRow_1", System.currentTimeMillis());

				if(execStat){   
   	 				runStat.updateStatOnConnection("OnComponentOk8", 0, "ok");
				}
				tImpalaRow_1Process(globalMap);



/**
 * [tHiveRow_1 end ] stop
 */
				}//end the resume

				



	
			}catch(java.lang.Exception e){	
				
				    if(!(e instanceof TalendException)){
					   log.fatal(currentComponent + " " + e.getMessage(),e);
					}
				
				TalendException te = new TalendException(e, currentComponent, globalMap);
				
				throw te;
			}catch(java.lang.Error error){	
				
					runStat.stopThreadStat();
				
				throw error;
			}finally{
				
				try{
					
	
	/**
	 * [tHiveRow_1 finally ] start
	 */

	

	
	
	currentComponent="tHiveRow_1";

	

 



/**
 * [tHiveRow_1 finally ] stop
 */
				}catch(java.lang.Exception e){	
					//ignore
				}catch(java.lang.Error error){
					//ignore
				}
				resourceMap = null;
			}
		

		globalMap.put("tHiveRow_1_SUBPROCESS_STATE", 1);
	}
	

public void tImpalaRow_1Process(final java.util.Map<String, Object> globalMap) throws TalendException {
	globalMap.put("tImpalaRow_1_SUBPROCESS_STATE", 0);

 final boolean execStat = this.execStat;
	
		String iterateId = "";
	
	
	String currentComponent = "";
	java.util.Map<String, Object> resourceMap = new java.util.HashMap<String, Object>();

	try {
			// TDI-39566 avoid throwing an useless Exception
			boolean resumeIt = true;
			if (globalResumeTicket == false && resumeEntryMethodName != null) {
				String currentMethodName = new java.lang.Exception().getStackTrace()[0].getMethodName();
				resumeIt = resumeEntryMethodName.equals(currentMethodName);
			}
			if (resumeIt || globalResumeTicket) { //start the resume
				globalResumeTicket = true;





	
	/**
	 * [tImpalaRow_1 begin ] start
	 */

	

	
		
		ok_Hash.put("tImpalaRow_1", false);
		start_Hash.put("tImpalaRow_1", System.currentTimeMillis());
		
	
	currentComponent="tImpalaRow_1";

	
		int tos_count_tImpalaRow_1 = 0;
		
			if(enableLogStash) {
				talendJobLog.addCM("tImpalaRow_1", "inv. metadata", "tImpalaRow");
				talendJobLogProcess(globalMap);
			}
			
		java.sql.Connection conn_tImpalaRow_1 = (java.sql.Connection)globalMap.get("conn_tImpalaConnection_1");
		String dbname_tImpalaRow_1 = (String)globalMap.get("db_tImpalaConnection_1");
	if(dbname_tImpalaRow_1!=null && !"".equals(dbname_tImpalaRow_1.trim()) && !"default".equals(dbname_tImpalaRow_1.trim())) {
		java.sql.Statement goToDatabase_tImpalaRow_1 = conn_tImpalaRow_1.createStatement();
		goToDatabase_tImpalaRow_1.execute("use " + dbname_tImpalaRow_1);
		goToDatabase_tImpalaRow_1.close();
    }
    
	java.sql.Statement stmt_tImpalaRow_1 = conn_tImpalaRow_1.createStatement();

	String query_tImpalaRow_1 = "";
	boolean whetherReject_tImpalaRow_1 = false;

 



/**
 * [tImpalaRow_1 begin ] stop
 */
	
	/**
	 * [tImpalaRow_1 main ] start
	 */

	

	
	
	currentComponent="tImpalaRow_1";

	

	query_tImpalaRow_1 = "INVALIDATE METADATA "+ context.W_EDH_DB_H+".H_FILE_INGESTION;"
;
	whetherReject_tImpalaRow_1 = false;

	
	globalMap.put("tImpalaRow_1_QUERY",query_tImpalaRow_1);
	try {
	    		stmt_tImpalaRow_1.execute(query_tImpalaRow_1);
	} catch (java.lang.Exception e) {
		whetherReject_tImpalaRow_1 = true;
				log.fatal("tImpalaRow_1 - " + e.getMessage());
			throw e;
    }
 


	tos_count_tImpalaRow_1++;

/**
 * [tImpalaRow_1 main ] stop
 */
	
	/**
	 * [tImpalaRow_1 process_data_begin ] start
	 */

	

	
	
	currentComponent="tImpalaRow_1";

	

 



/**
 * [tImpalaRow_1 process_data_begin ] stop
 */
	
	/**
	 * [tImpalaRow_1 process_data_end ] start
	 */

	

	
	
	currentComponent="tImpalaRow_1";

	

 



/**
 * [tImpalaRow_1 process_data_end ] stop
 */
	
	/**
	 * [tImpalaRow_1 end ] start
	 */

	

	
	
	currentComponent="tImpalaRow_1";

	

		stmt_tImpalaRow_1.close();	
		log.info("tImpalaRow_1 - Done.");
 

ok_Hash.put("tImpalaRow_1", true);
end_Hash.put("tImpalaRow_1", System.currentTimeMillis());




/**
 * [tImpalaRow_1 end ] stop
 */
				}//end the resume

				



	
			}catch(java.lang.Exception e){	
				
				    if(!(e instanceof TalendException)){
					   log.fatal(currentComponent + " " + e.getMessage(),e);
					}
				
				TalendException te = new TalendException(e, currentComponent, globalMap);
				
				throw te;
			}catch(java.lang.Error error){	
				
					runStat.stopThreadStat();
				
				throw error;
			}finally{
				
				try{
					
	
	/**
	 * [tImpalaRow_1 finally ] start
	 */

	

	
	
	currentComponent="tImpalaRow_1";

	

 



/**
 * [tImpalaRow_1 finally ] stop
 */
				}catch(java.lang.Exception e){	
					//ignore
				}catch(java.lang.Error error){
					//ignore
				}
				resourceMap = null;
			}
		

		globalMap.put("tImpalaRow_1_SUBPROCESS_STATE", 1);
	}
	

public void talendJobLogProcess(final java.util.Map<String, Object> globalMap) throws TalendException {
	globalMap.put("talendJobLog_SUBPROCESS_STATE", 0);

 final boolean execStat = this.execStat;
	
		String iterateId = "";
	
	
	String currentComponent = "";
	java.util.Map<String, Object> resourceMap = new java.util.HashMap<String, Object>();

	try {
			// TDI-39566 avoid throwing an useless Exception
			boolean resumeIt = true;
			if (globalResumeTicket == false && resumeEntryMethodName != null) {
				String currentMethodName = new java.lang.Exception().getStackTrace()[0].getMethodName();
				resumeIt = resumeEntryMethodName.equals(currentMethodName);
			}
			if (resumeIt || globalResumeTicket) { //start the resume
				globalResumeTicket = true;





	
	/**
	 * [talendJobLog begin ] start
	 */

	

	
		
		ok_Hash.put("talendJobLog", false);
		start_Hash.put("talendJobLog", System.currentTimeMillis());
		
	
	currentComponent="talendJobLog";

	
		int tos_count_talendJobLog = 0;
		

	for (JobStructureCatcherUtils.JobStructureCatcherMessage jcm : talendJobLog.getMessages()) {
		org.talend.job.audit.JobContextBuilder builder_talendJobLog = org.talend.job.audit.JobContextBuilder.create().jobName(jcm.job_name).jobId(jcm.job_id).jobVersion(jcm.job_version)
			.custom("process_id", jcm.pid).custom("thread_id", jcm.tid).custom("pid", pid).custom("father_pid", fatherPid).custom("root_pid", rootPid);
		org.talend.logging.audit.Context log_context_talendJobLog = null;
		
		
		if(jcm.log_type == JobStructureCatcherUtils.LogType.PERFORMANCE){
			long timeMS = jcm.end_time - jcm.start_time;
			String duration = String.valueOf(timeMS);
			
			log_context_talendJobLog = builder_talendJobLog
				.sourceId(jcm.sourceId).sourceLabel(jcm.sourceLabel).sourceConnectorType(jcm.sourceComponentName)
				.targetId(jcm.targetId).targetLabel(jcm.targetLabel).targetConnectorType(jcm.targetComponentName)
				.connectionName(jcm.current_connector).rows(jcm.row_count).duration(duration).build();
			auditLogger_talendJobLog.flowExecution(log_context_talendJobLog);
		} else if(jcm.log_type == JobStructureCatcherUtils.LogType.JOBSTART) {
			log_context_talendJobLog = builder_talendJobLog.timestamp(jcm.moment).build();
			auditLogger_talendJobLog.jobstart(log_context_talendJobLog);
		} else if(jcm.log_type == JobStructureCatcherUtils.LogType.JOBEND) {
			long timeMS = jcm.end_time - jcm.start_time;
			String duration = String.valueOf(timeMS);
		
			log_context_talendJobLog = builder_talendJobLog
				.timestamp(jcm.moment).duration(duration).status(jcm.status).build();
			auditLogger_talendJobLog.jobstop(log_context_talendJobLog);
		} else if(jcm.log_type == JobStructureCatcherUtils.LogType.RUNCOMPONENT) {
			log_context_talendJobLog = builder_talendJobLog.timestamp(jcm.moment)
				.connectorType(jcm.component_name).connectorId(jcm.component_id).connectorLabel(jcm.component_label).build();
			auditLogger_talendJobLog.runcomponent(log_context_talendJobLog);
		} else if(jcm.log_type == JobStructureCatcherUtils.LogType.FLOWINPUT) {//log current component input line
			long timeMS = jcm.end_time - jcm.start_time;
			String duration = String.valueOf(timeMS);
			
			log_context_talendJobLog = builder_talendJobLog
				.connectorType(jcm.component_name).connectorId(jcm.component_id).connectorLabel(jcm.component_label)
				.connectionName(jcm.current_connector).connectionType(jcm.current_connector_type)
				.rows(jcm.total_row_number).duration(duration).build();
			auditLogger_talendJobLog.flowInput(log_context_talendJobLog);
		} else if(jcm.log_type == JobStructureCatcherUtils.LogType.FLOWOUTPUT) {//log current component output/reject line
			long timeMS = jcm.end_time - jcm.start_time;
			String duration = String.valueOf(timeMS);
			
			log_context_talendJobLog = builder_talendJobLog
				.connectorType(jcm.component_name).connectorId(jcm.component_id).connectorLabel(jcm.component_label)
				.connectionName(jcm.current_connector).connectionType(jcm.current_connector_type)
				.rows(jcm.total_row_number).duration(duration).build();
			auditLogger_talendJobLog.flowOutput(log_context_talendJobLog);
		}
		
		
		
	}

 



/**
 * [talendJobLog begin ] stop
 */
	
	/**
	 * [talendJobLog main ] start
	 */

	

	
	
	currentComponent="talendJobLog";

	

 


	tos_count_talendJobLog++;

/**
 * [talendJobLog main ] stop
 */
	
	/**
	 * [talendJobLog process_data_begin ] start
	 */

	

	
	
	currentComponent="talendJobLog";

	

 



/**
 * [talendJobLog process_data_begin ] stop
 */
	
	/**
	 * [talendJobLog process_data_end ] start
	 */

	

	
	
	currentComponent="talendJobLog";

	

 



/**
 * [talendJobLog process_data_end ] stop
 */
	
	/**
	 * [talendJobLog end ] start
	 */

	

	
	
	currentComponent="talendJobLog";

	

 

ok_Hash.put("talendJobLog", true);
end_Hash.put("talendJobLog", System.currentTimeMillis());




/**
 * [talendJobLog end ] stop
 */
				}//end the resume

				



	
			}catch(java.lang.Exception e){	
				
				    if(!(e instanceof TalendException)){
					   log.fatal(currentComponent + " " + e.getMessage(),e);
					}
				
				TalendException te = new TalendException(e, currentComponent, globalMap);
				
				throw te;
			}catch(java.lang.Error error){	
				
					runStat.stopThreadStat();
				
				throw error;
			}finally{
				
				try{
					
	
	/**
	 * [talendJobLog finally ] start
	 */

	

	
	
	currentComponent="talendJobLog";

	

 



/**
 * [talendJobLog finally ] stop
 */
				}catch(java.lang.Exception e){	
					//ignore
				}catch(java.lang.Error error){
					//ignore
				}
				resourceMap = null;
			}
		

		globalMap.put("talendJobLog_SUBPROCESS_STATE", 1);
	}
	
    public String resuming_logs_dir_path = null;
    public String resuming_checkpoint_path = null;
    public String parent_part_launcher = null;
    private String resumeEntryMethodName = null;
    private boolean globalResumeTicket = false;

    public boolean watch = false;
    // portStats is null, it means don't execute the statistics
    public Integer portStats = null;
    public int portTraces = 4334;
    public String clientHost;
    public String defaultClientHost = "localhost";
    public String contextStr = "LOCAL";
    public boolean isDefaultContext = true;
    public String pid = "0";
    public String rootPid = null;
    public String fatherPid = null;
    public String fatherNode = null;
    public long startTime = 0;
    public boolean isChildJob = false;
    public String log4jLevel = "";
    
    private boolean enableLogStash;

    private boolean execStat = true;

    private ThreadLocal<java.util.Map<String, String>> threadLocal = new ThreadLocal<java.util.Map<String, String>>() {
        protected java.util.Map<String, String> initialValue() {
            java.util.Map<String,String> threadRunResultMap = new java.util.HashMap<String, String>();
            threadRunResultMap.put("errorCode", null);
            threadRunResultMap.put("status", "");
            return threadRunResultMap;
        };
    };


    protected PropertiesWithType context_param = new PropertiesWithType();
    public java.util.Map<String, Object> parentContextMap = new java.util.HashMap<String, Object>();

    public String status= "";
    

    public static void main(String[] args){
        final S_INGESTION_IMPORT_FILE_1 S_INGESTION_IMPORT_FILE_1Class = new S_INGESTION_IMPORT_FILE_1();

        int exitCode = S_INGESTION_IMPORT_FILE_1Class.runJobInTOS(args);
	        if(exitCode==0){
		        log.info("TalendJob: 'S_INGESTION_IMPORT_FILE_1' - Done.");
	        }

        System.exit(exitCode);
    }


    public String[][] runJob(String[] args) {

        int exitCode = runJobInTOS(args);
        String[][] bufferValue = new String[][] { { Integer.toString(exitCode) } };

        return bufferValue;
    }

    public boolean hastBufferOutputComponent() {
		boolean hastBufferOutput = false;
    	
        return hastBufferOutput;
    }

    public int runJobInTOS(String[] args) {
	   	// reset status
	   	status = "";
	   	
        String lastStr = "";
        for (String arg : args) {
            if (arg.equalsIgnoreCase("--context_param")) {
                lastStr = arg;
            } else if (lastStr.equals("")) {
                evalParam(arg);
            } else {
                evalParam(lastStr + " " + arg);
                lastStr = "";
            }
        }
        enableLogStash = "true".equalsIgnoreCase(System.getProperty("audit.enabled"));

	        if(!"".equals(log4jLevel)){
	        	
				
				
				if("trace".equalsIgnoreCase(log4jLevel)){
					org.apache.logging.log4j.core.config.Configurator.setLevel(log.getName(), org.apache.logging.log4j.Level.TRACE);
				}else if("debug".equalsIgnoreCase(log4jLevel)){
					org.apache.logging.log4j.core.config.Configurator.setLevel(log.getName(), org.apache.logging.log4j.Level.DEBUG);
				}else if("info".equalsIgnoreCase(log4jLevel)){
					org.apache.logging.log4j.core.config.Configurator.setLevel(log.getName(), org.apache.logging.log4j.Level.INFO);
				}else if("warn".equalsIgnoreCase(log4jLevel)){
					org.apache.logging.log4j.core.config.Configurator.setLevel(log.getName(), org.apache.logging.log4j.Level.WARN);
				}else if("error".equalsIgnoreCase(log4jLevel)){
					org.apache.logging.log4j.core.config.Configurator.setLevel(log.getName(), org.apache.logging.log4j.Level.ERROR);
				}else if("fatal".equalsIgnoreCase(log4jLevel)){
					org.apache.logging.log4j.core.config.Configurator.setLevel(log.getName(), org.apache.logging.log4j.Level.FATAL);
				}else if ("off".equalsIgnoreCase(log4jLevel)){
					org.apache.logging.log4j.core.config.Configurator.setLevel(log.getName(), org.apache.logging.log4j.Level.OFF);
				}
				org.apache.logging.log4j.core.config.Configurator.setLevel(org.apache.logging.log4j.LogManager.getRootLogger().getName(), log.getLevel());
				
			}
			log.info("TalendJob: 'S_INGESTION_IMPORT_FILE_1' - Start.");
		
		
		
			if(enableLogStash) {
				java.util.Properties properties_talendJobLog = new java.util.Properties();
				properties_talendJobLog.setProperty("root.logger", "audit");
				properties_talendJobLog.setProperty("encoding", "UTF-8");
				properties_talendJobLog.setProperty("application.name", "Talend Studio");
				properties_talendJobLog.setProperty("service.name", "Talend Studio Job");
				properties_talendJobLog.setProperty("instance.name", "Talend Studio Job Instance");
				properties_talendJobLog.setProperty("propagate.appender.exceptions", "none");
				properties_talendJobLog.setProperty("log.appender", "file");
				properties_talendJobLog.setProperty("appender.file.path", "audit.json");
				properties_talendJobLog.setProperty("appender.file.maxsize", "52428800");
				properties_talendJobLog.setProperty("appender.file.maxbackup", "20");
				properties_talendJobLog.setProperty("host", "false");

				System.getProperties().stringPropertyNames().stream()
					.filter(it -> it.startsWith("audit.logger."))
					.forEach(key -> properties_talendJobLog.setProperty(key.substring("audit.logger.".length()), System.getProperty(key)));

				
				
				
				org.apache.logging.log4j.core.config.Configurator.setLevel(properties_talendJobLog.getProperty("root.logger"), org.apache.logging.log4j.Level.DEBUG);
				
				auditLogger_talendJobLog = org.talend.job.audit.JobEventAuditLoggerFactory.createJobAuditLogger(properties_talendJobLog);
			}
		

        if(clientHost == null) {
            clientHost = defaultClientHost;
        }

        if(pid == null || "0".equals(pid)) {
            pid = TalendString.getAsciiRandomString(6);
        }

        if (rootPid==null) {
            rootPid = pid;
        }
        if (fatherPid==null) {
            fatherPid = pid;
        }else{
            isChildJob = true;
        }

        if (portStats != null) {
            // portStats = -1; //for testing
            if (portStats < 0 || portStats > 65535) {
                // issue:10869, the portStats is invalid, so this client socket can't open
                System.err.println("The statistics socket port " + portStats + " is invalid.");
                execStat = false;
            }
        } else {
            execStat = false;
        }

        boolean inOSGi = routines.system.BundleUtils.inOSGi();

        try {
            java.util.Dictionary<String, Object> jobProperties = null;
            if (inOSGi) {
                jobProperties = routines.system.BundleUtils.getJobProperties(jobName);
    
                if (jobProperties != null && jobProperties.get("context") != null) {
                    contextStr = (String)jobProperties.get("context");
                }
            }
            //call job/subjob with an existing context, like: --context=production. if without this parameter, there will use the default context instead.
            java.io.InputStream inContext = S_INGESTION_IMPORT_FILE_1.class.getClassLoader().getResourceAsStream("datahub_aml/s_ingestion_import_file_1_0_1/contexts/" + contextStr + ".properties");
            if (inContext == null) {
                inContext = S_INGESTION_IMPORT_FILE_1.class.getClassLoader().getResourceAsStream("config/contexts/" + contextStr + ".properties");
            }
            if (inContext != null) {
                try {
                    //defaultProps is in order to keep the original context value
                    if(context != null && context.isEmpty()) {
    	                defaultProps.load(inContext);
    	                if (inOSGi && jobProperties != null) {
                             java.util.Enumeration<String> keys = jobProperties.keys();
                             while (keys.hasMoreElements()) {
                                 String propKey = keys.nextElement();
                                 if (defaultProps.containsKey(propKey)) {
                                     defaultProps.put(propKey, (String) jobProperties.get(propKey));
                                 }
                             }
    	                }
    	                context = new ContextProperties(defaultProps);
                    }
                } finally {
                    inContext.close();
                }
            } else if (!isDefaultContext) {
                //print info and job continue to run, for case: context_param is not empty.
                System.err.println("Could not find the context " + contextStr);
            }

            if(!context_param.isEmpty()) {
                context.putAll(context_param);
				//set types for params from parentJobs
				for (Object key: context_param.keySet()){
					String context_key = key.toString();
					String context_type = context_param.getContextType(context_key);
					context.setContextType(context_key, context_type);

				}
            }
            class ContextProcessing {
                private void processContext_0() {
                        context.setContextType("W_BUSINESS_GROUP", "id_String");
                        if(context.getStringValue("W_BUSINESS_GROUP") == null) {
                            context.W_BUSINESS_GROUP = null;
                        } else {
                            context.W_BUSINESS_GROUP=(String) context.getProperty("W_BUSINESS_GROUP");
                        }
                        context.setContextType("W_BUSINESS_NAME", "id_String");
                        if(context.getStringValue("W_BUSINESS_NAME") == null) {
                            context.W_BUSINESS_NAME = null;
                        } else {
                            context.W_BUSINESS_NAME=(String) context.getProperty("W_BUSINESS_NAME");
                        }
                        context.setContextType("W_DATA_TYPE", "id_String");
                        if(context.getStringValue("W_DATA_TYPE") == null) {
                            context.W_DATA_TYPE = null;
                        } else {
                            context.W_DATA_TYPE=(String) context.getProperty("W_DATA_TYPE");
                        }
                        context.setContextType("W_DEFAULT_PARTITION", "id_String");
                        if(context.getStringValue("W_DEFAULT_PARTITION") == null) {
                            context.W_DEFAULT_PARTITION = null;
                        } else {
                            context.W_DEFAULT_PARTITION=(String) context.getProperty("W_DEFAULT_PARTITION");
                        }
                        context.setContextType("W_EDH_DB_ARCHIN", "id_String");
                        if(context.getStringValue("W_EDH_DB_ARCHIN") == null) {
                            context.W_EDH_DB_ARCHIN = null;
                        } else {
                            context.W_EDH_DB_ARCHIN=(String) context.getProperty("W_EDH_DB_ARCHIN");
                        }
                        context.setContextType("W_EDH_DB_H", "id_String");
                        if(context.getStringValue("W_EDH_DB_H") == null) {
                            context.W_EDH_DB_H = null;
                        } else {
                            context.W_EDH_DB_H=(String) context.getProperty("W_EDH_DB_H");
                        }
                        context.setContextType("W_EDH_DB_IN", "id_String");
                        if(context.getStringValue("W_EDH_DB_IN") == null) {
                            context.W_EDH_DB_IN = null;
                        } else {
                            context.W_EDH_DB_IN=(String) context.getProperty("W_EDH_DB_IN");
                        }
                        context.setContextType("W_EDH_TABLE_ARCHIN", "id_String");
                        if(context.getStringValue("W_EDH_TABLE_ARCHIN") == null) {
                            context.W_EDH_TABLE_ARCHIN = null;
                        } else {
                            context.W_EDH_TABLE_ARCHIN=(String) context.getProperty("W_EDH_TABLE_ARCHIN");
                        }
                        context.setContextType("W_EDH_TABLE_H", "id_String");
                        if(context.getStringValue("W_EDH_TABLE_H") == null) {
                            context.W_EDH_TABLE_H = null;
                        } else {
                            context.W_EDH_TABLE_H=(String) context.getProperty("W_EDH_TABLE_H");
                        }
                        context.setContextType("W_EDH_TABLE_IN", "id_String");
                        if(context.getStringValue("W_EDH_TABLE_IN") == null) {
                            context.W_EDH_TABLE_IN = null;
                        } else {
                            context.W_EDH_TABLE_IN=(String) context.getProperty("W_EDH_TABLE_IN");
                        }
                        context.setContextType("W_EXTRACTION_FIELDS", "id_String");
                        if(context.getStringValue("W_EXTRACTION_FIELDS") == null) {
                            context.W_EXTRACTION_FIELDS = null;
                        } else {
                            context.W_EXTRACTION_FIELDS=(String) context.getProperty("W_EXTRACTION_FIELDS");
                        }
                        context.setContextType("W_F_FILEMASK", "id_String");
                        if(context.getStringValue("W_F_FILEMASK") == null) {
                            context.W_F_FILEMASK = null;
                        } else {
                            context.W_F_FILEMASK=(String) context.getProperty("W_F_FILEMASK");
                        }
                        context.setContextType("W_FILENAME", "id_String");
                        if(context.getStringValue("W_FILENAME") == null) {
                            context.W_FILENAME = null;
                        } else {
                            context.W_FILENAME=(String) context.getProperty("W_FILENAME");
                        }
                        context.setContextType("W_FLAG_ABILITATA", "id_String");
                        if(context.getStringValue("W_FLAG_ABILITATA") == null) {
                            context.W_FLAG_ABILITATA = null;
                        } else {
                            context.W_FLAG_ABILITATA=(String) context.getProperty("W_FLAG_ABILITATA");
                        }
                        context.setContextType("W_FLAG_HEADER", "id_String");
                        if(context.getStringValue("W_FLAG_HEADER") == null) {
                            context.W_FLAG_HEADER = null;
                        } else {
                            context.W_FLAG_HEADER=(String) context.getProperty("W_FLAG_HEADER");
                        }
                        context.setContextType("W_HDFS_ETL_PATH", "id_String");
                        if(context.getStringValue("W_HDFS_ETL_PATH") == null) {
                            context.W_HDFS_ETL_PATH = null;
                        } else {
                            context.W_HDFS_ETL_PATH=(String) context.getProperty("W_HDFS_ETL_PATH");
                        }
                        context.setContextType("W_IMPORT_TYPE", "id_String");
                        if(context.getStringValue("W_IMPORT_TYPE") == null) {
                            context.W_IMPORT_TYPE = null;
                        } else {
                            context.W_IMPORT_TYPE=(String) context.getProperty("W_IMPORT_TYPE");
                        }
                        context.setContextType("W_LAST_PARTITION", "id_String");
                        if(context.getStringValue("W_LAST_PARTITION") == null) {
                            context.W_LAST_PARTITION = null;
                        } else {
                            context.W_LAST_PARTITION=(String) context.getProperty("W_LAST_PARTITION");
                        }
                        context.setContextType("W_MAPPER", "id_String");
                        if(context.getStringValue("W_MAPPER") == null) {
                            context.W_MAPPER = null;
                        } else {
                            context.W_MAPPER=(String) context.getProperty("W_MAPPER");
                        }
                        context.setContextType("W_PARTITION_FIELD", "id_String");
                        if(context.getStringValue("W_PARTITION_FIELD") == null) {
                            context.W_PARTITION_FIELD = null;
                        } else {
                            context.W_PARTITION_FIELD=(String) context.getProperty("W_PARTITION_FIELD");
                        }
                        context.setContextType("W_PATH_ARCHIN", "id_String");
                        if(context.getStringValue("W_PATH_ARCHIN") == null) {
                            context.W_PATH_ARCHIN = null;
                        } else {
                            context.W_PATH_ARCHIN=(String) context.getProperty("W_PATH_ARCHIN");
                        }
                        context.setContextType("W_PATH_IN", "id_String");
                        if(context.getStringValue("W_PATH_IN") == null) {
                            context.W_PATH_IN = null;
                        } else {
                            context.W_PATH_IN=(String) context.getProperty("W_PATH_IN");
                        }
                        context.setContextType("W_SOURCE_NAME", "id_String");
                        if(context.getStringValue("W_SOURCE_NAME") == null) {
                            context.W_SOURCE_NAME = null;
                        } else {
                            context.W_SOURCE_NAME=(String) context.getProperty("W_SOURCE_NAME");
                        }
                        context.setContextType("W_T_SOURCE_DATABASE", "id_String");
                        if(context.getStringValue("W_T_SOURCE_DATABASE") == null) {
                            context.W_T_SOURCE_DATABASE = null;
                        } else {
                            context.W_T_SOURCE_DATABASE=(String) context.getProperty("W_T_SOURCE_DATABASE");
                        }
                        context.setContextType("W_T_SOURCE_TABLE", "id_String");
                        if(context.getStringValue("W_T_SOURCE_TABLE") == null) {
                            context.W_T_SOURCE_TABLE = null;
                        } else {
                            context.W_T_SOURCE_TABLE=(String) context.getProperty("W_T_SOURCE_TABLE");
                        }
                        context.setContextType("DB_POSTGRES_CUSTOM_LOG_TABLE", "id_String");
                        if(context.getStringValue("DB_POSTGRES_CUSTOM_LOG_TABLE") == null) {
                            context.DB_POSTGRES_CUSTOM_LOG_TABLE = null;
                        } else {
                            context.DB_POSTGRES_CUSTOM_LOG_TABLE=(String) context.getProperty("DB_POSTGRES_CUSTOM_LOG_TABLE");
                        }
                        context.setContextType("DB_POSTGRES_Database", "id_String");
                        if(context.getStringValue("DB_POSTGRES_Database") == null) {
                            context.DB_POSTGRES_Database = null;
                        } else {
                            context.DB_POSTGRES_Database=(String) context.getProperty("DB_POSTGRES_Database");
                        }
                        context.setContextType("DB_POSTGRES_FILE_METADATA_TABLE", "id_String");
                        if(context.getStringValue("DB_POSTGRES_FILE_METADATA_TABLE") == null) {
                            context.DB_POSTGRES_FILE_METADATA_TABLE = null;
                        } else {
                            context.DB_POSTGRES_FILE_METADATA_TABLE=(String) context.getProperty("DB_POSTGRES_FILE_METADATA_TABLE");
                        }
                        context.setContextType("DB_POSTGRES_LOG_TABLE", "id_String");
                        if(context.getStringValue("DB_POSTGRES_LOG_TABLE") == null) {
                            context.DB_POSTGRES_LOG_TABLE = null;
                        } else {
                            context.DB_POSTGRES_LOG_TABLE=(String) context.getProperty("DB_POSTGRES_LOG_TABLE");
                        }
                        context.setContextType("DB_POSTGRES_Login", "id_String");
                        if(context.getStringValue("DB_POSTGRES_Login") == null) {
                            context.DB_POSTGRES_Login = null;
                        } else {
                            context.DB_POSTGRES_Login=(String) context.getProperty("DB_POSTGRES_Login");
                        }
                        context.setContextType("DB_POSTGRES_LOGTABLE", "id_String");
                        if(context.getStringValue("DB_POSTGRES_LOGTABLE") == null) {
                            context.DB_POSTGRES_LOGTABLE = null;
                        } else {
                            context.DB_POSTGRES_LOGTABLE=(String) context.getProperty("DB_POSTGRES_LOGTABLE");
                        }
                        context.setContextType("DB_POSTGRES_LOGVIEW", "id_String");
                        if(context.getStringValue("DB_POSTGRES_LOGVIEW") == null) {
                            context.DB_POSTGRES_LOGVIEW = null;
                        } else {
                            context.DB_POSTGRES_LOGVIEW=(String) context.getProperty("DB_POSTGRES_LOGVIEW");
                        }
                        context.setContextType("DB_POSTGRES_METADATA_FACT_RECUPERO_PARTIZIONI", "id_String");
                        if(context.getStringValue("DB_POSTGRES_METADATA_FACT_RECUPERO_PARTIZIONI") == null) {
                            context.DB_POSTGRES_METADATA_FACT_RECUPERO_PARTIZIONI = null;
                        } else {
                            context.DB_POSTGRES_METADATA_FACT_RECUPERO_PARTIZIONI=(String) context.getProperty("DB_POSTGRES_METADATA_FACT_RECUPERO_PARTIZIONI");
                        }
                        context.setContextType("DB_POSTGRES_METADATA_FACT_TABLE", "id_String");
                        if(context.getStringValue("DB_POSTGRES_METADATA_FACT_TABLE") == null) {
                            context.DB_POSTGRES_METADATA_FACT_TABLE = null;
                        } else {
                            context.DB_POSTGRES_METADATA_FACT_TABLE=(String) context.getProperty("DB_POSTGRES_METADATA_FACT_TABLE");
                        }
                        context.setContextType("DB_POSTGRES_Password", "id_Password");
                        if(context.getStringValue("DB_POSTGRES_Password") == null) {
                            context.DB_POSTGRES_Password = null;
                        } else {
                            String pwd_DB_POSTGRES_Password_value = context.getProperty("DB_POSTGRES_Password");
                            context.DB_POSTGRES_Password = null;
                            if(pwd_DB_POSTGRES_Password_value!=null) {
                                if(context_param.containsKey("DB_POSTGRES_Password")) {//no need to decrypt if it come from program argument or parent job runtime
                                    context.DB_POSTGRES_Password = pwd_DB_POSTGRES_Password_value;
                                } else if (!pwd_DB_POSTGRES_Password_value.isEmpty()) {
                                    try {
                                        context.DB_POSTGRES_Password = routines.system.PasswordEncryptUtil.decryptPassword(pwd_DB_POSTGRES_Password_value);
                                        context.put("DB_POSTGRES_Password",context.DB_POSTGRES_Password);
                                    } catch (java.lang.RuntimeException e) {
                                        //do nothing
                                    }
                                }
                            }
                        }
                        context.setContextType("DB_POSTGRES_Port", "id_String");
                        if(context.getStringValue("DB_POSTGRES_Port") == null) {
                            context.DB_POSTGRES_Port = null;
                        } else {
                            context.DB_POSTGRES_Port=(String) context.getProperty("DB_POSTGRES_Port");
                        }
                        context.setContextType("DB_POSTGRES_Schema", "id_String");
                        if(context.getStringValue("DB_POSTGRES_Schema") == null) {
                            context.DB_POSTGRES_Schema = null;
                        } else {
                            context.DB_POSTGRES_Schema=(String) context.getProperty("DB_POSTGRES_Schema");
                        }
                        context.setContextType("DB_POSTGRES_Server", "id_String");
                        if(context.getStringValue("DB_POSTGRES_Server") == null) {
                            context.DB_POSTGRES_Server = null;
                        } else {
                            context.DB_POSTGRES_Server=(String) context.getProperty("DB_POSTGRES_Server");
                        }
                        context.setContextType("DB_POSTGRES_SQOOP_METADATA_TABLE", "id_String");
                        if(context.getStringValue("DB_POSTGRES_SQOOP_METADATA_TABLE") == null) {
                            context.DB_POSTGRES_SQOOP_METADATA_TABLE = null;
                        } else {
                            context.DB_POSTGRES_SQOOP_METADATA_TABLE=(String) context.getProperty("DB_POSTGRES_SQOOP_METADATA_TABLE");
                        }
                        context.setContextType("DB_POSTGRES_STATTABLE", "id_String");
                        if(context.getStringValue("DB_POSTGRES_STATTABLE") == null) {
                            context.DB_POSTGRES_STATTABLE = null;
                        } else {
                            context.DB_POSTGRES_STATTABLE=(String) context.getProperty("DB_POSTGRES_STATTABLE");
                        }
                        context.setContextType("EDH_CLUSTER_dfs_client_failover_proxy_provider_edhcluster", "id_String");
                        if(context.getStringValue("EDH_CLUSTER_dfs_client_failover_proxy_provider_edhcluster") == null) {
                            context.EDH_CLUSTER_dfs_client_failover_proxy_provider_edhcluster = null;
                        } else {
                            context.EDH_CLUSTER_dfs_client_failover_proxy_provider_edhcluster=(String) context.getProperty("EDH_CLUSTER_dfs_client_failover_proxy_provider_edhcluster");
                        }
                        context.setContextType("EDH_CLUSTER_dfs_ha_namenodes_edhcluster", "id_String");
                        if(context.getStringValue("EDH_CLUSTER_dfs_ha_namenodes_edhcluster") == null) {
                            context.EDH_CLUSTER_dfs_ha_namenodes_edhcluster = null;
                        } else {
                            context.EDH_CLUSTER_dfs_ha_namenodes_edhcluster=(String) context.getProperty("EDH_CLUSTER_dfs_ha_namenodes_edhcluster");
                        }
                        context.setContextType("EDH_CLUSTER_dfs_namenode_rpc_address_edhcluster_namenode209", "id_String");
                        if(context.getStringValue("EDH_CLUSTER_dfs_namenode_rpc_address_edhcluster_namenode209") == null) {
                            context.EDH_CLUSTER_dfs_namenode_rpc_address_edhcluster_namenode209 = null;
                        } else {
                            context.EDH_CLUSTER_dfs_namenode_rpc_address_edhcluster_namenode209=(String) context.getProperty("EDH_CLUSTER_dfs_namenode_rpc_address_edhcluster_namenode209");
                        }
                        context.setContextType("EDH_CLUSTER_dfs_namenode_rpc_address_edhcluster_namenode264", "id_String");
                        if(context.getStringValue("EDH_CLUSTER_dfs_namenode_rpc_address_edhcluster_namenode264") == null) {
                            context.EDH_CLUSTER_dfs_namenode_rpc_address_edhcluster_namenode264 = null;
                        } else {
                            context.EDH_CLUSTER_dfs_namenode_rpc_address_edhcluster_namenode264=(String) context.getProperty("EDH_CLUSTER_dfs_namenode_rpc_address_edhcluster_namenode264");
                        }
                        context.setContextType("EDH_CLUSTER_dfs_nameservices", "id_String");
                        if(context.getStringValue("EDH_CLUSTER_dfs_nameservices") == null) {
                            context.EDH_CLUSTER_dfs_nameservices = null;
                        } else {
                            context.EDH_CLUSTER_dfs_nameservices=(String) context.getProperty("EDH_CLUSTER_dfs_nameservices");
                        }
                        context.setContextType("EDH_CLUSTER_ha_zookeeper_quorum", "id_String");
                        if(context.getStringValue("EDH_CLUSTER_ha_zookeeper_quorum") == null) {
                            context.EDH_CLUSTER_ha_zookeeper_quorum = null;
                        } else {
                            context.EDH_CLUSTER_ha_zookeeper_quorum=(String) context.getProperty("EDH_CLUSTER_ha_zookeeper_quorum");
                        }
                        context.setContextType("EDH_CLUSTER_JobHistory", "id_String");
                        if(context.getStringValue("EDH_CLUSTER_JobHistory") == null) {
                            context.EDH_CLUSTER_JobHistory = null;
                        } else {
                            context.EDH_CLUSTER_JobHistory=(String) context.getProperty("EDH_CLUSTER_JobHistory");
                        }
                        context.setContextType("EDH_CLUSTER_JobHistroyPrin", "id_String");
                        if(context.getStringValue("EDH_CLUSTER_JobHistroyPrin") == null) {
                            context.EDH_CLUSTER_JobHistroyPrin = null;
                        } else {
                            context.EDH_CLUSTER_JobHistroyPrin=(String) context.getProperty("EDH_CLUSTER_JobHistroyPrin");
                        }
                        context.setContextType("EDH_CLUSTER_JTOrRMPrin", "id_String");
                        if(context.getStringValue("EDH_CLUSTER_JTOrRMPrin") == null) {
                            context.EDH_CLUSTER_JTOrRMPrin = null;
                        } else {
                            context.EDH_CLUSTER_JTOrRMPrin=(String) context.getProperty("EDH_CLUSTER_JTOrRMPrin");
                        }
                        context.setContextType("EDH_CLUSTER_KeyTab", "id_String");
                        if(context.getStringValue("EDH_CLUSTER_KeyTab") == null) {
                            context.EDH_CLUSTER_KeyTab = null;
                        } else {
                            context.EDH_CLUSTER_KeyTab=(String) context.getProperty("EDH_CLUSTER_KeyTab");
                        }
                        context.setContextType("EDH_CLUSTER_NameNodePrin", "id_String");
                        if(context.getStringValue("EDH_CLUSTER_NameNodePrin") == null) {
                            context.EDH_CLUSTER_NameNodePrin = null;
                        } else {
                            context.EDH_CLUSTER_NameNodePrin=(String) context.getProperty("EDH_CLUSTER_NameNodePrin");
                        }
                        context.setContextType("EDH_CLUSTER_NameNodeUri", "id_String");
                        if(context.getStringValue("EDH_CLUSTER_NameNodeUri") == null) {
                            context.EDH_CLUSTER_NameNodeUri = null;
                        } else {
                            context.EDH_CLUSTER_NameNodeUri=(String) context.getProperty("EDH_CLUSTER_NameNodeUri");
                        }
                        context.setContextType("EDH_CLUSTER_Principal", "id_String");
                        if(context.getStringValue("EDH_CLUSTER_Principal") == null) {
                            context.EDH_CLUSTER_Principal = null;
                        } else {
                            context.EDH_CLUSTER_Principal=(String) context.getProperty("EDH_CLUSTER_Principal");
                        }
                        context.setContextType("EDH_CLUSTER_ResourceManager", "id_String");
                        if(context.getStringValue("EDH_CLUSTER_ResourceManager") == null) {
                            context.EDH_CLUSTER_ResourceManager = null;
                        } else {
                            context.EDH_CLUSTER_ResourceManager=(String) context.getProperty("EDH_CLUSTER_ResourceManager");
                        }
                        context.setContextType("EDH_CLUSTER_ResourceManagerScheduler", "id_String");
                        if(context.getStringValue("EDH_CLUSTER_ResourceManagerScheduler") == null) {
                            context.EDH_CLUSTER_ResourceManagerScheduler = null;
                        } else {
                            context.EDH_CLUSTER_ResourceManagerScheduler=(String) context.getProperty("EDH_CLUSTER_ResourceManagerScheduler");
                        }
                        context.setContextType("EDH_CLUSTER_StagingDirectory", "id_String");
                        if(context.getStringValue("EDH_CLUSTER_StagingDirectory") == null) {
                            context.EDH_CLUSTER_StagingDirectory = null;
                        } else {
                            context.EDH_CLUSTER_StagingDirectory=(String) context.getProperty("EDH_CLUSTER_StagingDirectory");
                        }
                        context.setContextType("EDH_CLUSTER_username", "id_String");
                        if(context.getStringValue("EDH_CLUSTER_username") == null) {
                            context.EDH_CLUSTER_username = null;
                        } else {
                            context.EDH_CLUSTER_username=(String) context.getProperty("EDH_CLUSTER_username");
                        }
                        context.setContextType("EDH_CLUSTER_yarn_resourcemanager_address_rm1", "id_String");
                        if(context.getStringValue("EDH_CLUSTER_yarn_resourcemanager_address_rm1") == null) {
                            context.EDH_CLUSTER_yarn_resourcemanager_address_rm1 = null;
                        } else {
                            context.EDH_CLUSTER_yarn_resourcemanager_address_rm1=(String) context.getProperty("EDH_CLUSTER_yarn_resourcemanager_address_rm1");
                        }
                        context.setContextType("EDH_CLUSTER_yarn_resourcemanager_address_rm2", "id_String");
                        if(context.getStringValue("EDH_CLUSTER_yarn_resourcemanager_address_rm2") == null) {
                            context.EDH_CLUSTER_yarn_resourcemanager_address_rm2 = null;
                        } else {
                            context.EDH_CLUSTER_yarn_resourcemanager_address_rm2=(String) context.getProperty("EDH_CLUSTER_yarn_resourcemanager_address_rm2");
                        }
                        context.setContextType("EDH_CLUSTER_yarn_resourcemanager_ha_enabled", "id_String");
                        if(context.getStringValue("EDH_CLUSTER_yarn_resourcemanager_ha_enabled") == null) {
                            context.EDH_CLUSTER_yarn_resourcemanager_ha_enabled = null;
                        } else {
                            context.EDH_CLUSTER_yarn_resourcemanager_ha_enabled=(String) context.getProperty("EDH_CLUSTER_yarn_resourcemanager_ha_enabled");
                        }
                        context.setContextType("EDH_CLUSTER_yarn_resourcemanager_ha_rm_ids", "id_String");
                        if(context.getStringValue("EDH_CLUSTER_yarn_resourcemanager_ha_rm_ids") == null) {
                            context.EDH_CLUSTER_yarn_resourcemanager_ha_rm_ids = null;
                        } else {
                            context.EDH_CLUSTER_yarn_resourcemanager_ha_rm_ids=(String) context.getProperty("EDH_CLUSTER_yarn_resourcemanager_ha_rm_ids");
                        }
                        context.setContextType("EDH_CLUSTER_yarn_resourcemanager_scheduler_address_rm1", "id_String");
                        if(context.getStringValue("EDH_CLUSTER_yarn_resourcemanager_scheduler_address_rm1") == null) {
                            context.EDH_CLUSTER_yarn_resourcemanager_scheduler_address_rm1 = null;
                        } else {
                            context.EDH_CLUSTER_yarn_resourcemanager_scheduler_address_rm1=(String) context.getProperty("EDH_CLUSTER_yarn_resourcemanager_scheduler_address_rm1");
                        }
                        context.setContextType("EDH_CLUSTER_yarn_resourcemanager_scheduler_address_rm2", "id_String");
                        if(context.getStringValue("EDH_CLUSTER_yarn_resourcemanager_scheduler_address_rm2") == null) {
                            context.EDH_CLUSTER_yarn_resourcemanager_scheduler_address_rm2 = null;
                        } else {
                            context.EDH_CLUSTER_yarn_resourcemanager_scheduler_address_rm2=(String) context.getProperty("EDH_CLUSTER_yarn_resourcemanager_scheduler_address_rm2");
                        }
                        context.setContextType("EDH_CLUSTER_HDFS_HdfsFileSeparator", "id_String");
                        if(context.getStringValue("EDH_CLUSTER_HDFS_HdfsFileSeparator") == null) {
                            context.EDH_CLUSTER_HDFS_HdfsFileSeparator = null;
                        } else {
                            context.EDH_CLUSTER_HDFS_HdfsFileSeparator=(String) context.getProperty("EDH_CLUSTER_HDFS_HdfsFileSeparator");
                        }
                        context.setContextType("EDH_CLUSTER_HDFS_HdfsRowSeparator", "id_String");
                        if(context.getStringValue("EDH_CLUSTER_HDFS_HdfsRowSeparator") == null) {
                            context.EDH_CLUSTER_HDFS_HdfsRowSeparator = null;
                        } else {
                            context.EDH_CLUSTER_HDFS_HdfsRowSeparator=(String) context.getProperty("EDH_CLUSTER_HDFS_HdfsRowSeparator");
                        }
                        context.setContextType("EDH_CLUSTER_HIVE_Database", "id_String");
                        if(context.getStringValue("EDH_CLUSTER_HIVE_Database") == null) {
                            context.EDH_CLUSTER_HIVE_Database = null;
                        } else {
                            context.EDH_CLUSTER_HIVE_Database=(String) context.getProperty("EDH_CLUSTER_HIVE_Database");
                        }
                        context.setContextType("EDH_CLUSTER_HIVE_dynamicPart", "id_String");
                        if(context.getStringValue("EDH_CLUSTER_HIVE_dynamicPart") == null) {
                            context.EDH_CLUSTER_HIVE_dynamicPart = null;
                        } else {
                            context.EDH_CLUSTER_HIVE_dynamicPart=(String) context.getProperty("EDH_CLUSTER_HIVE_dynamicPart");
                        }
                        context.setContextType("EDH_CLUSTER_HIVE_dynamicPartMax", "id_String");
                        if(context.getStringValue("EDH_CLUSTER_HIVE_dynamicPartMax") == null) {
                            context.EDH_CLUSTER_HIVE_dynamicPartMax = null;
                        } else {
                            context.EDH_CLUSTER_HIVE_dynamicPartMax=(String) context.getProperty("EDH_CLUSTER_HIVE_dynamicPartMax");
                        }
                        context.setContextType("EDH_CLUSTER_HIVE_dynamicPartMaxPerNode", "id_String");
                        if(context.getStringValue("EDH_CLUSTER_HIVE_dynamicPartMaxPerNode") == null) {
                            context.EDH_CLUSTER_HIVE_dynamicPartMaxPerNode = null;
                        } else {
                            context.EDH_CLUSTER_HIVE_dynamicPartMaxPerNode=(String) context.getProperty("EDH_CLUSTER_HIVE_dynamicPartMaxPerNode");
                        }
                        context.setContextType("EDH_CLUSTER_HIVE_executionEngine", "id_String");
                        if(context.getStringValue("EDH_CLUSTER_HIVE_executionEngine") == null) {
                            context.EDH_CLUSTER_HIVE_executionEngine = null;
                        } else {
                            context.EDH_CLUSTER_HIVE_executionEngine=(String) context.getProperty("EDH_CLUSTER_HIVE_executionEngine");
                        }
                        context.setContextType("EDH_CLUSTER_HIVE_hiveAdditionalJDBCParameters", "id_String");
                        if(context.getStringValue("EDH_CLUSTER_HIVE_hiveAdditionalJDBCParameters") == null) {
                            context.EDH_CLUSTER_HIVE_hiveAdditionalJDBCParameters = null;
                        } else {
                            context.EDH_CLUSTER_HIVE_hiveAdditionalJDBCParameters=(String) context.getProperty("EDH_CLUSTER_HIVE_hiveAdditionalJDBCParameters");
                        }
                        context.setContextType("EDH_CLUSTER_HIVE_HiveKeyTab", "id_String");
                        if(context.getStringValue("EDH_CLUSTER_HIVE_HiveKeyTab") == null) {
                            context.EDH_CLUSTER_HIVE_HiveKeyTab = null;
                        } else {
                            context.EDH_CLUSTER_HIVE_HiveKeyTab=(String) context.getProperty("EDH_CLUSTER_HIVE_HiveKeyTab");
                        }
                        context.setContextType("EDH_CLUSTER_HIVE_HiveKeyTabPrincipal", "id_String");
                        if(context.getStringValue("EDH_CLUSTER_HIVE_HiveKeyTabPrincipal") == null) {
                            context.EDH_CLUSTER_HIVE_HiveKeyTabPrincipal = null;
                        } else {
                            context.EDH_CLUSTER_HIVE_HiveKeyTabPrincipal=(String) context.getProperty("EDH_CLUSTER_HIVE_HiveKeyTabPrincipal");
                        }
                        context.setContextType("EDH_CLUSTER_HIVE_HivePrincipal", "id_String");
                        if(context.getStringValue("EDH_CLUSTER_HIVE_HivePrincipal") == null) {
                            context.EDH_CLUSTER_HIVE_HivePrincipal = null;
                        } else {
                            context.EDH_CLUSTER_HIVE_HivePrincipal=(String) context.getProperty("EDH_CLUSTER_HIVE_HivePrincipal");
                        }
                        context.setContextType("EDH_CLUSTER_HIVE_hiveSSLTrustStorePassword", "id_Password");
                        if(context.getStringValue("EDH_CLUSTER_HIVE_hiveSSLTrustStorePassword") == null) {
                            context.EDH_CLUSTER_HIVE_hiveSSLTrustStorePassword = null;
                        } else {
                            String pwd_EDH_CLUSTER_HIVE_hiveSSLTrustStorePassword_value = context.getProperty("EDH_CLUSTER_HIVE_hiveSSLTrustStorePassword");
                            context.EDH_CLUSTER_HIVE_hiveSSLTrustStorePassword = null;
                            if(pwd_EDH_CLUSTER_HIVE_hiveSSLTrustStorePassword_value!=null) {
                                if(context_param.containsKey("EDH_CLUSTER_HIVE_hiveSSLTrustStorePassword")) {//no need to decrypt if it come from program argument or parent job runtime
                                    context.EDH_CLUSTER_HIVE_hiveSSLTrustStorePassword = pwd_EDH_CLUSTER_HIVE_hiveSSLTrustStorePassword_value;
                                } else if (!pwd_EDH_CLUSTER_HIVE_hiveSSLTrustStorePassword_value.isEmpty()) {
                                    try {
                                        context.EDH_CLUSTER_HIVE_hiveSSLTrustStorePassword = routines.system.PasswordEncryptUtil.decryptPassword(pwd_EDH_CLUSTER_HIVE_hiveSSLTrustStorePassword_value);
                                        context.put("EDH_CLUSTER_HIVE_hiveSSLTrustStorePassword",context.EDH_CLUSTER_HIVE_hiveSSLTrustStorePassword);
                                    } catch (java.lang.RuntimeException e) {
                                        //do nothing
                                    }
                                }
                            }
                        }
                        context.setContextType("EDH_CLUSTER_HIVE_hiveSSLTrustStorePath", "id_String");
                        if(context.getStringValue("EDH_CLUSTER_HIVE_hiveSSLTrustStorePath") == null) {
                            context.EDH_CLUSTER_HIVE_hiveSSLTrustStorePath = null;
                        } else {
                            context.EDH_CLUSTER_HIVE_hiveSSLTrustStorePath=(String) context.getProperty("EDH_CLUSTER_HIVE_hiveSSLTrustStorePath");
                        }
                        context.setContextType("EDH_CLUSTER_HIVE_Login", "id_String");
                        if(context.getStringValue("EDH_CLUSTER_HIVE_Login") == null) {
                            context.EDH_CLUSTER_HIVE_Login = null;
                        } else {
                            context.EDH_CLUSTER_HIVE_Login=(String) context.getProperty("EDH_CLUSTER_HIVE_Login");
                        }
                        context.setContextType("EDH_CLUSTER_HIVE_Password", "id_Password");
                        if(context.getStringValue("EDH_CLUSTER_HIVE_Password") == null) {
                            context.EDH_CLUSTER_HIVE_Password = null;
                        } else {
                            String pwd_EDH_CLUSTER_HIVE_Password_value = context.getProperty("EDH_CLUSTER_HIVE_Password");
                            context.EDH_CLUSTER_HIVE_Password = null;
                            if(pwd_EDH_CLUSTER_HIVE_Password_value!=null) {
                                if(context_param.containsKey("EDH_CLUSTER_HIVE_Password")) {//no need to decrypt if it come from program argument or parent job runtime
                                    context.EDH_CLUSTER_HIVE_Password = pwd_EDH_CLUSTER_HIVE_Password_value;
                                } else if (!pwd_EDH_CLUSTER_HIVE_Password_value.isEmpty()) {
                                    try {
                                        context.EDH_CLUSTER_HIVE_Password = routines.system.PasswordEncryptUtil.decryptPassword(pwd_EDH_CLUSTER_HIVE_Password_value);
                                        context.put("EDH_CLUSTER_HIVE_Password",context.EDH_CLUSTER_HIVE_Password);
                                    } catch (java.lang.RuntimeException e) {
                                        //do nothing
                                    }
                                }
                            }
                        }
                        context.setContextType("EDH_CLUSTER_HIVE_Port", "id_String");
                        if(context.getStringValue("EDH_CLUSTER_HIVE_Port") == null) {
                            context.EDH_CLUSTER_HIVE_Port = null;
                        } else {
                            context.EDH_CLUSTER_HIVE_Port=(String) context.getProperty("EDH_CLUSTER_HIVE_Port");
                        }
                        context.setContextType("EDH_CLUSTER_HIVE_Server", "id_String");
                        if(context.getStringValue("EDH_CLUSTER_HIVE_Server") == null) {
                            context.EDH_CLUSTER_HIVE_Server = null;
                        } else {
                            context.EDH_CLUSTER_HIVE_Server=(String) context.getProperty("EDH_CLUSTER_HIVE_Server");
                        }
                        context.setContextType("EDH_CLUSTER_IMPALA_Database", "id_String");
                        if(context.getStringValue("EDH_CLUSTER_IMPALA_Database") == null) {
                            context.EDH_CLUSTER_IMPALA_Database = null;
                        } else {
                            context.EDH_CLUSTER_IMPALA_Database=(String) context.getProperty("EDH_CLUSTER_IMPALA_Database");
                        }
                        context.setContextType("EDH_CLUSTER_IMPALA_ImpalaPrincipal", "id_String");
                        if(context.getStringValue("EDH_CLUSTER_IMPALA_ImpalaPrincipal") == null) {
                            context.EDH_CLUSTER_IMPALA_ImpalaPrincipal = null;
                        } else {
                            context.EDH_CLUSTER_IMPALA_ImpalaPrincipal=(String) context.getProperty("EDH_CLUSTER_IMPALA_ImpalaPrincipal");
                        }
                        context.setContextType("EDH_CLUSTER_IMPALA_Login", "id_String");
                        if(context.getStringValue("EDH_CLUSTER_IMPALA_Login") == null) {
                            context.EDH_CLUSTER_IMPALA_Login = null;
                        } else {
                            context.EDH_CLUSTER_IMPALA_Login=(String) context.getProperty("EDH_CLUSTER_IMPALA_Login");
                        }
                        context.setContextType("EDH_CLUSTER_IMPALA_Port", "id_String");
                        if(context.getStringValue("EDH_CLUSTER_IMPALA_Port") == null) {
                            context.EDH_CLUSTER_IMPALA_Port = null;
                        } else {
                            context.EDH_CLUSTER_IMPALA_Port=(String) context.getProperty("EDH_CLUSTER_IMPALA_Port");
                        }
                        context.setContextType("EDH_CLUSTER_IMPALA_Server", "id_String");
                        if(context.getStringValue("EDH_CLUSTER_IMPALA_Server") == null) {
                            context.EDH_CLUSTER_IMPALA_Server = null;
                        } else {
                            context.EDH_CLUSTER_IMPALA_Server=(String) context.getProperty("EDH_CLUSTER_IMPALA_Server");
                        }
                        context.setContextType("DB_POSTGRES_configuration_metadata", "id_String");
                        if(context.getStringValue("DB_POSTGRES_configuration_metadata") == null) {
                            context.DB_POSTGRES_configuration_metadata = null;
                        } else {
                            context.DB_POSTGRES_configuration_metadata=(String) context.getProperty("DB_POSTGRES_configuration_metadata");
                        }
                } 
                public void processAllContext() {
                        processContext_0();
                }
            }

            new ContextProcessing().processAllContext();
        } catch (java.io.IOException ie) {
            System.err.println("Could not load context "+contextStr);
            ie.printStackTrace();
        }

        // get context value from parent directly
        if (parentContextMap != null && !parentContextMap.isEmpty()) {if (parentContextMap.containsKey("W_BUSINESS_GROUP")) {
                context.W_BUSINESS_GROUP = (String) parentContextMap.get("W_BUSINESS_GROUP");
            }if (parentContextMap.containsKey("W_BUSINESS_NAME")) {
                context.W_BUSINESS_NAME = (String) parentContextMap.get("W_BUSINESS_NAME");
            }if (parentContextMap.containsKey("W_DATA_TYPE")) {
                context.W_DATA_TYPE = (String) parentContextMap.get("W_DATA_TYPE");
            }if (parentContextMap.containsKey("W_DEFAULT_PARTITION")) {
                context.W_DEFAULT_PARTITION = (String) parentContextMap.get("W_DEFAULT_PARTITION");
            }if (parentContextMap.containsKey("W_EDH_DB_ARCHIN")) {
                context.W_EDH_DB_ARCHIN = (String) parentContextMap.get("W_EDH_DB_ARCHIN");
            }if (parentContextMap.containsKey("W_EDH_DB_H")) {
                context.W_EDH_DB_H = (String) parentContextMap.get("W_EDH_DB_H");
            }if (parentContextMap.containsKey("W_EDH_DB_IN")) {
                context.W_EDH_DB_IN = (String) parentContextMap.get("W_EDH_DB_IN");
            }if (parentContextMap.containsKey("W_EDH_TABLE_ARCHIN")) {
                context.W_EDH_TABLE_ARCHIN = (String) parentContextMap.get("W_EDH_TABLE_ARCHIN");
            }if (parentContextMap.containsKey("W_EDH_TABLE_H")) {
                context.W_EDH_TABLE_H = (String) parentContextMap.get("W_EDH_TABLE_H");
            }if (parentContextMap.containsKey("W_EDH_TABLE_IN")) {
                context.W_EDH_TABLE_IN = (String) parentContextMap.get("W_EDH_TABLE_IN");
            }if (parentContextMap.containsKey("W_EXTRACTION_FIELDS")) {
                context.W_EXTRACTION_FIELDS = (String) parentContextMap.get("W_EXTRACTION_FIELDS");
            }if (parentContextMap.containsKey("W_F_FILEMASK")) {
                context.W_F_FILEMASK = (String) parentContextMap.get("W_F_FILEMASK");
            }if (parentContextMap.containsKey("W_FILENAME")) {
                context.W_FILENAME = (String) parentContextMap.get("W_FILENAME");
            }if (parentContextMap.containsKey("W_FLAG_ABILITATA")) {
                context.W_FLAG_ABILITATA = (String) parentContextMap.get("W_FLAG_ABILITATA");
            }if (parentContextMap.containsKey("W_FLAG_HEADER")) {
                context.W_FLAG_HEADER = (String) parentContextMap.get("W_FLAG_HEADER");
            }if (parentContextMap.containsKey("W_HDFS_ETL_PATH")) {
                context.W_HDFS_ETL_PATH = (String) parentContextMap.get("W_HDFS_ETL_PATH");
            }if (parentContextMap.containsKey("W_IMPORT_TYPE")) {
                context.W_IMPORT_TYPE = (String) parentContextMap.get("W_IMPORT_TYPE");
            }if (parentContextMap.containsKey("W_LAST_PARTITION")) {
                context.W_LAST_PARTITION = (String) parentContextMap.get("W_LAST_PARTITION");
            }if (parentContextMap.containsKey("W_MAPPER")) {
                context.W_MAPPER = (String) parentContextMap.get("W_MAPPER");
            }if (parentContextMap.containsKey("W_PARTITION_FIELD")) {
                context.W_PARTITION_FIELD = (String) parentContextMap.get("W_PARTITION_FIELD");
            }if (parentContextMap.containsKey("W_PATH_ARCHIN")) {
                context.W_PATH_ARCHIN = (String) parentContextMap.get("W_PATH_ARCHIN");
            }if (parentContextMap.containsKey("W_PATH_IN")) {
                context.W_PATH_IN = (String) parentContextMap.get("W_PATH_IN");
            }if (parentContextMap.containsKey("W_SOURCE_NAME")) {
                context.W_SOURCE_NAME = (String) parentContextMap.get("W_SOURCE_NAME");
            }if (parentContextMap.containsKey("W_T_SOURCE_DATABASE")) {
                context.W_T_SOURCE_DATABASE = (String) parentContextMap.get("W_T_SOURCE_DATABASE");
            }if (parentContextMap.containsKey("W_T_SOURCE_TABLE")) {
                context.W_T_SOURCE_TABLE = (String) parentContextMap.get("W_T_SOURCE_TABLE");
            }if (parentContextMap.containsKey("DB_POSTGRES_CUSTOM_LOG_TABLE")) {
                context.DB_POSTGRES_CUSTOM_LOG_TABLE = (String) parentContextMap.get("DB_POSTGRES_CUSTOM_LOG_TABLE");
            }if (parentContextMap.containsKey("DB_POSTGRES_Database")) {
                context.DB_POSTGRES_Database = (String) parentContextMap.get("DB_POSTGRES_Database");
            }if (parentContextMap.containsKey("DB_POSTGRES_FILE_METADATA_TABLE")) {
                context.DB_POSTGRES_FILE_METADATA_TABLE = (String) parentContextMap.get("DB_POSTGRES_FILE_METADATA_TABLE");
            }if (parentContextMap.containsKey("DB_POSTGRES_LOG_TABLE")) {
                context.DB_POSTGRES_LOG_TABLE = (String) parentContextMap.get("DB_POSTGRES_LOG_TABLE");
            }if (parentContextMap.containsKey("DB_POSTGRES_Login")) {
                context.DB_POSTGRES_Login = (String) parentContextMap.get("DB_POSTGRES_Login");
            }if (parentContextMap.containsKey("DB_POSTGRES_LOGTABLE")) {
                context.DB_POSTGRES_LOGTABLE = (String) parentContextMap.get("DB_POSTGRES_LOGTABLE");
            }if (parentContextMap.containsKey("DB_POSTGRES_LOGVIEW")) {
                context.DB_POSTGRES_LOGVIEW = (String) parentContextMap.get("DB_POSTGRES_LOGVIEW");
            }if (parentContextMap.containsKey("DB_POSTGRES_METADATA_FACT_RECUPERO_PARTIZIONI")) {
                context.DB_POSTGRES_METADATA_FACT_RECUPERO_PARTIZIONI = (String) parentContextMap.get("DB_POSTGRES_METADATA_FACT_RECUPERO_PARTIZIONI");
            }if (parentContextMap.containsKey("DB_POSTGRES_METADATA_FACT_TABLE")) {
                context.DB_POSTGRES_METADATA_FACT_TABLE = (String) parentContextMap.get("DB_POSTGRES_METADATA_FACT_TABLE");
            }if (parentContextMap.containsKey("DB_POSTGRES_Password")) {
                context.DB_POSTGRES_Password = (java.lang.String) parentContextMap.get("DB_POSTGRES_Password");
            }if (parentContextMap.containsKey("DB_POSTGRES_Port")) {
                context.DB_POSTGRES_Port = (String) parentContextMap.get("DB_POSTGRES_Port");
            }if (parentContextMap.containsKey("DB_POSTGRES_Schema")) {
                context.DB_POSTGRES_Schema = (String) parentContextMap.get("DB_POSTGRES_Schema");
            }if (parentContextMap.containsKey("DB_POSTGRES_Server")) {
                context.DB_POSTGRES_Server = (String) parentContextMap.get("DB_POSTGRES_Server");
            }if (parentContextMap.containsKey("DB_POSTGRES_SQOOP_METADATA_TABLE")) {
                context.DB_POSTGRES_SQOOP_METADATA_TABLE = (String) parentContextMap.get("DB_POSTGRES_SQOOP_METADATA_TABLE");
            }if (parentContextMap.containsKey("DB_POSTGRES_STATTABLE")) {
                context.DB_POSTGRES_STATTABLE = (String) parentContextMap.get("DB_POSTGRES_STATTABLE");
            }if (parentContextMap.containsKey("EDH_CLUSTER_dfs_client_failover_proxy_provider_edhcluster")) {
                context.EDH_CLUSTER_dfs_client_failover_proxy_provider_edhcluster = (String) parentContextMap.get("EDH_CLUSTER_dfs_client_failover_proxy_provider_edhcluster");
            }if (parentContextMap.containsKey("EDH_CLUSTER_dfs_ha_namenodes_edhcluster")) {
                context.EDH_CLUSTER_dfs_ha_namenodes_edhcluster = (String) parentContextMap.get("EDH_CLUSTER_dfs_ha_namenodes_edhcluster");
            }if (parentContextMap.containsKey("EDH_CLUSTER_dfs_namenode_rpc_address_edhcluster_namenode209")) {
                context.EDH_CLUSTER_dfs_namenode_rpc_address_edhcluster_namenode209 = (String) parentContextMap.get("EDH_CLUSTER_dfs_namenode_rpc_address_edhcluster_namenode209");
            }if (parentContextMap.containsKey("EDH_CLUSTER_dfs_namenode_rpc_address_edhcluster_namenode264")) {
                context.EDH_CLUSTER_dfs_namenode_rpc_address_edhcluster_namenode264 = (String) parentContextMap.get("EDH_CLUSTER_dfs_namenode_rpc_address_edhcluster_namenode264");
            }if (parentContextMap.containsKey("EDH_CLUSTER_dfs_nameservices")) {
                context.EDH_CLUSTER_dfs_nameservices = (String) parentContextMap.get("EDH_CLUSTER_dfs_nameservices");
            }if (parentContextMap.containsKey("EDH_CLUSTER_ha_zookeeper_quorum")) {
                context.EDH_CLUSTER_ha_zookeeper_quorum = (String) parentContextMap.get("EDH_CLUSTER_ha_zookeeper_quorum");
            }if (parentContextMap.containsKey("EDH_CLUSTER_JobHistory")) {
                context.EDH_CLUSTER_JobHistory = (String) parentContextMap.get("EDH_CLUSTER_JobHistory");
            }if (parentContextMap.containsKey("EDH_CLUSTER_JobHistroyPrin")) {
                context.EDH_CLUSTER_JobHistroyPrin = (String) parentContextMap.get("EDH_CLUSTER_JobHistroyPrin");
            }if (parentContextMap.containsKey("EDH_CLUSTER_JTOrRMPrin")) {
                context.EDH_CLUSTER_JTOrRMPrin = (String) parentContextMap.get("EDH_CLUSTER_JTOrRMPrin");
            }if (parentContextMap.containsKey("EDH_CLUSTER_KeyTab")) {
                context.EDH_CLUSTER_KeyTab = (String) parentContextMap.get("EDH_CLUSTER_KeyTab");
            }if (parentContextMap.containsKey("EDH_CLUSTER_NameNodePrin")) {
                context.EDH_CLUSTER_NameNodePrin = (String) parentContextMap.get("EDH_CLUSTER_NameNodePrin");
            }if (parentContextMap.containsKey("EDH_CLUSTER_NameNodeUri")) {
                context.EDH_CLUSTER_NameNodeUri = (String) parentContextMap.get("EDH_CLUSTER_NameNodeUri");
            }if (parentContextMap.containsKey("EDH_CLUSTER_Principal")) {
                context.EDH_CLUSTER_Principal = (String) parentContextMap.get("EDH_CLUSTER_Principal");
            }if (parentContextMap.containsKey("EDH_CLUSTER_ResourceManager")) {
                context.EDH_CLUSTER_ResourceManager = (String) parentContextMap.get("EDH_CLUSTER_ResourceManager");
            }if (parentContextMap.containsKey("EDH_CLUSTER_ResourceManagerScheduler")) {
                context.EDH_CLUSTER_ResourceManagerScheduler = (String) parentContextMap.get("EDH_CLUSTER_ResourceManagerScheduler");
            }if (parentContextMap.containsKey("EDH_CLUSTER_StagingDirectory")) {
                context.EDH_CLUSTER_StagingDirectory = (String) parentContextMap.get("EDH_CLUSTER_StagingDirectory");
            }if (parentContextMap.containsKey("EDH_CLUSTER_username")) {
                context.EDH_CLUSTER_username = (String) parentContextMap.get("EDH_CLUSTER_username");
            }if (parentContextMap.containsKey("EDH_CLUSTER_yarn_resourcemanager_address_rm1")) {
                context.EDH_CLUSTER_yarn_resourcemanager_address_rm1 = (String) parentContextMap.get("EDH_CLUSTER_yarn_resourcemanager_address_rm1");
            }if (parentContextMap.containsKey("EDH_CLUSTER_yarn_resourcemanager_address_rm2")) {
                context.EDH_CLUSTER_yarn_resourcemanager_address_rm2 = (String) parentContextMap.get("EDH_CLUSTER_yarn_resourcemanager_address_rm2");
            }if (parentContextMap.containsKey("EDH_CLUSTER_yarn_resourcemanager_ha_enabled")) {
                context.EDH_CLUSTER_yarn_resourcemanager_ha_enabled = (String) parentContextMap.get("EDH_CLUSTER_yarn_resourcemanager_ha_enabled");
            }if (parentContextMap.containsKey("EDH_CLUSTER_yarn_resourcemanager_ha_rm_ids")) {
                context.EDH_CLUSTER_yarn_resourcemanager_ha_rm_ids = (String) parentContextMap.get("EDH_CLUSTER_yarn_resourcemanager_ha_rm_ids");
            }if (parentContextMap.containsKey("EDH_CLUSTER_yarn_resourcemanager_scheduler_address_rm1")) {
                context.EDH_CLUSTER_yarn_resourcemanager_scheduler_address_rm1 = (String) parentContextMap.get("EDH_CLUSTER_yarn_resourcemanager_scheduler_address_rm1");
            }if (parentContextMap.containsKey("EDH_CLUSTER_yarn_resourcemanager_scheduler_address_rm2")) {
                context.EDH_CLUSTER_yarn_resourcemanager_scheduler_address_rm2 = (String) parentContextMap.get("EDH_CLUSTER_yarn_resourcemanager_scheduler_address_rm2");
            }if (parentContextMap.containsKey("EDH_CLUSTER_HDFS_HdfsFileSeparator")) {
                context.EDH_CLUSTER_HDFS_HdfsFileSeparator = (String) parentContextMap.get("EDH_CLUSTER_HDFS_HdfsFileSeparator");
            }if (parentContextMap.containsKey("EDH_CLUSTER_HDFS_HdfsRowSeparator")) {
                context.EDH_CLUSTER_HDFS_HdfsRowSeparator = (String) parentContextMap.get("EDH_CLUSTER_HDFS_HdfsRowSeparator");
            }if (parentContextMap.containsKey("EDH_CLUSTER_HIVE_Database")) {
                context.EDH_CLUSTER_HIVE_Database = (String) parentContextMap.get("EDH_CLUSTER_HIVE_Database");
            }if (parentContextMap.containsKey("EDH_CLUSTER_HIVE_dynamicPart")) {
                context.EDH_CLUSTER_HIVE_dynamicPart = (String) parentContextMap.get("EDH_CLUSTER_HIVE_dynamicPart");
            }if (parentContextMap.containsKey("EDH_CLUSTER_HIVE_dynamicPartMax")) {
                context.EDH_CLUSTER_HIVE_dynamicPartMax = (String) parentContextMap.get("EDH_CLUSTER_HIVE_dynamicPartMax");
            }if (parentContextMap.containsKey("EDH_CLUSTER_HIVE_dynamicPartMaxPerNode")) {
                context.EDH_CLUSTER_HIVE_dynamicPartMaxPerNode = (String) parentContextMap.get("EDH_CLUSTER_HIVE_dynamicPartMaxPerNode");
            }if (parentContextMap.containsKey("EDH_CLUSTER_HIVE_executionEngine")) {
                context.EDH_CLUSTER_HIVE_executionEngine = (String) parentContextMap.get("EDH_CLUSTER_HIVE_executionEngine");
            }if (parentContextMap.containsKey("EDH_CLUSTER_HIVE_hiveAdditionalJDBCParameters")) {
                context.EDH_CLUSTER_HIVE_hiveAdditionalJDBCParameters = (String) parentContextMap.get("EDH_CLUSTER_HIVE_hiveAdditionalJDBCParameters");
            }if (parentContextMap.containsKey("EDH_CLUSTER_HIVE_HiveKeyTab")) {
                context.EDH_CLUSTER_HIVE_HiveKeyTab = (String) parentContextMap.get("EDH_CLUSTER_HIVE_HiveKeyTab");
            }if (parentContextMap.containsKey("EDH_CLUSTER_HIVE_HiveKeyTabPrincipal")) {
                context.EDH_CLUSTER_HIVE_HiveKeyTabPrincipal = (String) parentContextMap.get("EDH_CLUSTER_HIVE_HiveKeyTabPrincipal");
            }if (parentContextMap.containsKey("EDH_CLUSTER_HIVE_HivePrincipal")) {
                context.EDH_CLUSTER_HIVE_HivePrincipal = (String) parentContextMap.get("EDH_CLUSTER_HIVE_HivePrincipal");
            }if (parentContextMap.containsKey("EDH_CLUSTER_HIVE_hiveSSLTrustStorePassword")) {
                context.EDH_CLUSTER_HIVE_hiveSSLTrustStorePassword = (java.lang.String) parentContextMap.get("EDH_CLUSTER_HIVE_hiveSSLTrustStorePassword");
            }if (parentContextMap.containsKey("EDH_CLUSTER_HIVE_hiveSSLTrustStorePath")) {
                context.EDH_CLUSTER_HIVE_hiveSSLTrustStorePath = (String) parentContextMap.get("EDH_CLUSTER_HIVE_hiveSSLTrustStorePath");
            }if (parentContextMap.containsKey("EDH_CLUSTER_HIVE_Login")) {
                context.EDH_CLUSTER_HIVE_Login = (String) parentContextMap.get("EDH_CLUSTER_HIVE_Login");
            }if (parentContextMap.containsKey("EDH_CLUSTER_HIVE_Password")) {
                context.EDH_CLUSTER_HIVE_Password = (java.lang.String) parentContextMap.get("EDH_CLUSTER_HIVE_Password");
            }if (parentContextMap.containsKey("EDH_CLUSTER_HIVE_Port")) {
                context.EDH_CLUSTER_HIVE_Port = (String) parentContextMap.get("EDH_CLUSTER_HIVE_Port");
            }if (parentContextMap.containsKey("EDH_CLUSTER_HIVE_Server")) {
                context.EDH_CLUSTER_HIVE_Server = (String) parentContextMap.get("EDH_CLUSTER_HIVE_Server");
            }if (parentContextMap.containsKey("EDH_CLUSTER_IMPALA_Database")) {
                context.EDH_CLUSTER_IMPALA_Database = (String) parentContextMap.get("EDH_CLUSTER_IMPALA_Database");
            }if (parentContextMap.containsKey("EDH_CLUSTER_IMPALA_ImpalaPrincipal")) {
                context.EDH_CLUSTER_IMPALA_ImpalaPrincipal = (String) parentContextMap.get("EDH_CLUSTER_IMPALA_ImpalaPrincipal");
            }if (parentContextMap.containsKey("EDH_CLUSTER_IMPALA_Login")) {
                context.EDH_CLUSTER_IMPALA_Login = (String) parentContextMap.get("EDH_CLUSTER_IMPALA_Login");
            }if (parentContextMap.containsKey("EDH_CLUSTER_IMPALA_Port")) {
                context.EDH_CLUSTER_IMPALA_Port = (String) parentContextMap.get("EDH_CLUSTER_IMPALA_Port");
            }if (parentContextMap.containsKey("EDH_CLUSTER_IMPALA_Server")) {
                context.EDH_CLUSTER_IMPALA_Server = (String) parentContextMap.get("EDH_CLUSTER_IMPALA_Server");
            }if (parentContextMap.containsKey("DB_POSTGRES_configuration_metadata")) {
                context.DB_POSTGRES_configuration_metadata = (String) parentContextMap.get("DB_POSTGRES_configuration_metadata");
            }
        }

        //Resume: init the resumeUtil
        resumeEntryMethodName = ResumeUtil.getResumeEntryMethodName(resuming_checkpoint_path);
        resumeUtil = new ResumeUtil(resuming_logs_dir_path, isChildJob, rootPid);
        resumeUtil.initCommonInfo(pid, rootPid, fatherPid, projectName, jobName, contextStr, jobVersion);

		List<String> parametersToEncrypt = new java.util.ArrayList<String>();
			parametersToEncrypt.add("DB_POSTGRES_Password");
			parametersToEncrypt.add("EDH_CLUSTER_HIVE_hiveSSLTrustStorePassword");
			parametersToEncrypt.add("EDH_CLUSTER_HIVE_Password");
        //Resume: jobStart
        resumeUtil.addLog("JOB_STARTED", "JOB:" + jobName, parent_part_launcher, Thread.currentThread().getId() + "", "","","","",resumeUtil.convertToJsonText(context,ContextProperties.class,parametersToEncrypt));

if(execStat) {
    try {
        runStat.openSocket(!isChildJob);
        runStat.setAllPID(rootPid, fatherPid, pid, jobName);
        runStat.startThreadStat(clientHost, portStats);
        runStat.updateStatOnJob(RunStat.JOBSTART, fatherNode);
    } catch (java.io.IOException ioException) {
        ioException.printStackTrace();
    }
}



	
	    java.util.concurrent.ConcurrentHashMap<Object, Object> concurrentHashMap = new java.util.concurrent.ConcurrentHashMap<Object, Object>();
	    globalMap.put("concurrentHashMap", concurrentHashMap);
	

    long startUsedMemory = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
    long endUsedMemory = 0;
    long end = 0;

    startTime = System.currentTimeMillis();

try {
errorCode = null;Implicit_Context_RegexProcess(globalMap);
if(!"failure".equals(status)) { status = "end"; }
}catch (TalendException e_Implicit_Context_Regex) {
globalMap.put("Implicit_Context_Regex_SUBPROCESS_STATE", -1);

e_Implicit_Context_Regex.printStackTrace();

}

this.globalResumeTicket = true;//to run tPreJob




		if(enableLogStash) {
	        talendJobLog.addJobStartMessage();
	        try {
	            talendJobLogProcess(globalMap);
	        } catch (java.lang.Exception e) {
	            e.printStackTrace();
	        }
        }

this.globalResumeTicket = false;//to run others jobs

try {
errorCode = null;tImpalaConnection_1Process(globalMap);
if(!"failure".equals(status)) { status = "end"; }
}catch (TalendException e_tImpalaConnection_1) {
globalMap.put("tImpalaConnection_1_SUBPROCESS_STATE", -1);

e_tImpalaConnection_1.printStackTrace();

}

this.globalResumeTicket = true;//to run tPostJob




        end = System.currentTimeMillis();

        if (watch) {
            System.out.println((end-startTime)+" milliseconds");
        }

        endUsedMemory = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        if (false) {
            System.out.println((endUsedMemory - startUsedMemory) + " bytes memory increase when running : S_INGESTION_IMPORT_FILE_1");
        }
		if(enableLogStash) {
	        talendJobLog.addJobEndMessage(startTime, end, status);
	        try {
	            talendJobLogProcess(globalMap);
	        } catch (java.lang.Exception e) {
	            e.printStackTrace();
	        }
        }



if (execStat) {
    runStat.updateStatOnJob(RunStat.JOBEND, fatherNode);
    runStat.stopThreadStat();
}
    int returnCode = 0;


    if(errorCode == null) {
         returnCode = status != null && status.equals("failure") ? 1 : 0;
    } else {
         returnCode = errorCode.intValue();
    }
    resumeUtil.addLog("JOB_ENDED", "JOB:" + jobName, parent_part_launcher, Thread.currentThread().getId() + "", "","" + returnCode,"","","");
    resumeUtil.flush();
    return returnCode;

  }

    // only for OSGi env
    public void destroy() {
    closeSqlDbConnections();


    }



    private void closeSqlDbConnections() {
        try {
            Object obj_conn;
            obj_conn = globalMap.remove("conn_tHiveConnection_1");
            if (null != obj_conn) {
                ((java.sql.Connection) obj_conn).close();
            }
        } catch (java.lang.Exception e) {
        }
    }











    private java.util.Map<String, Object> getSharedConnections4REST() {
        java.util.Map<String, Object> connections = new java.util.HashMap<String, Object>();
            connections.put("conn_tHiveConnection_1", globalMap.get("conn_tHiveConnection_1"));






        return connections;
    }

    private void evalParam(String arg) {
        if (arg.startsWith("--resuming_logs_dir_path")) {
            resuming_logs_dir_path = arg.substring(25);
        } else if (arg.startsWith("--resuming_checkpoint_path")) {
            resuming_checkpoint_path = arg.substring(27);
        } else if (arg.startsWith("--parent_part_launcher")) {
            parent_part_launcher = arg.substring(23);
        } else if (arg.startsWith("--watch")) {
            watch = true;
        } else if (arg.startsWith("--stat_port=")) {
            String portStatsStr = arg.substring(12);
            if (portStatsStr != null && !portStatsStr.equals("null")) {
                portStats = Integer.parseInt(portStatsStr);
            }
        } else if (arg.startsWith("--trace_port=")) {
            portTraces = Integer.parseInt(arg.substring(13));
        } else if (arg.startsWith("--client_host=")) {
            clientHost = arg.substring(14);
        } else if (arg.startsWith("--context=")) {
            contextStr = arg.substring(10);
            isDefaultContext = false;
        } else if (arg.startsWith("--father_pid=")) {
            fatherPid = arg.substring(13);
        } else if (arg.startsWith("--root_pid=")) {
            rootPid = arg.substring(11);
        } else if (arg.startsWith("--father_node=")) {
            fatherNode = arg.substring(14);
        } else if (arg.startsWith("--pid=")) {
            pid = arg.substring(6);
        } else if (arg.startsWith("--context_type")) {
            String keyValue = arg.substring(15);
			int index = -1;
            if (keyValue != null && (index = keyValue.indexOf('=')) > -1) {
                if (fatherPid==null) {
                    context_param.setContextType(keyValue.substring(0, index), replaceEscapeChars(keyValue.substring(index + 1)));
                } else { // the subjob won't escape the especial chars
                    context_param.setContextType(keyValue.substring(0, index), keyValue.substring(index + 1) );
                }

            }

		} else if (arg.startsWith("--context_param")) {
            String keyValue = arg.substring(16);
            int index = -1;
            if (keyValue != null && (index = keyValue.indexOf('=')) > -1) {
                if (fatherPid==null) {
                    context_param.put(keyValue.substring(0, index), replaceEscapeChars(keyValue.substring(index + 1)));
                } else { // the subjob won't escape the especial chars
                    context_param.put(keyValue.substring(0, index), keyValue.substring(index + 1) );
                }
            }
        } else if (arg.startsWith("--log4jLevel=")) {
            log4jLevel = arg.substring(13);
		} else if (arg.startsWith("--audit.enabled") && arg.contains("=")) {//for trunjob call
		    final int equal = arg.indexOf('=');
			final String key = arg.substring("--".length(), equal);
			System.setProperty(key, arg.substring(equal + 1));
		}
    }
    
    private static final String NULL_VALUE_EXPRESSION_IN_COMMAND_STRING_FOR_CHILD_JOB_ONLY = "<TALEND_NULL>";

    private final String[][] escapeChars = {
        {"\\\\","\\"},{"\\n","\n"},{"\\'","\'"},{"\\r","\r"},
        {"\\f","\f"},{"\\b","\b"},{"\\t","\t"}
        };
    private String replaceEscapeChars (String keyValue) {

		if (keyValue == null || ("").equals(keyValue.trim())) {
			return keyValue;
		}

		StringBuilder result = new StringBuilder();
		int currIndex = 0;
		while (currIndex < keyValue.length()) {
			int index = -1;
			// judege if the left string includes escape chars
			for (String[] strArray : escapeChars) {
				index = keyValue.indexOf(strArray[0],currIndex);
				if (index>=0) {

					result.append(keyValue.substring(currIndex, index + strArray[0].length()).replace(strArray[0], strArray[1]));
					currIndex = index + strArray[0].length();
					break;
				}
			}
			// if the left string doesn't include escape chars, append the left into the result
			if (index < 0) {
				result.append(keyValue.substring(currIndex));
				currIndex = currIndex + keyValue.length();
			}
		}

		return result.toString();
    }

    public Integer getErrorCode() {
        return errorCode;
    }


    public String getStatus() {
        return status;
    }

    ResumeUtil resumeUtil = null;
}
/************************************************************************************************
 *     295370 characters generated by Talend Big Data Platform 
 *     on the 6 febbraio 2026 14.57.56 CET
 ************************************************************************************************/