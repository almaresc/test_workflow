
package datahub_aml.s_aml_profilatura_estrazione_main_parametrica_0_1;

import routines.DataOperation;
import routines.TalendDataGenerator;
import routines.DataQuality;
import routines.Relational;
import routines.routine_Encryption;
import routines.DataQualityDependencies;
import routines.Mathematical;
import routines.SQLike;
import routines.Numeric;
import routines.TalendStringUtil;
import routines.TalendString;
import routines.DQTechnical;
import routines.StringHandling;
import routines.DataMasking;
import routines.TalendDate;
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
 




	//the import part of tJava_1
	//import java.util.List;

	//the import part of tJava_2
	//import java.util.List;

	//the import part of tJava_3
	//import java.util.List;

	//the import part of tJava_4
	//import java.util.List;


@SuppressWarnings("unused")

/**
 * Job: S_AML_PROFILATURA_ESTRAZIONE_MAIN_PARAMETRICA Purpose: estrarre i dati per universo partecipazioni<br>
 * Description:  <br>
 * @author Pietrini, Luca
 * @version 7.3.1.20241003_1446-patch
 * @status 
 */
public class S_AML_PROFILATURA_ESTRAZIONE_MAIN_PARAMETRICA implements TalendJob {
	static {System.setProperty("TalendJob.log", "S_AML_PROFILATURA_ESTRAZIONE_MAIN_PARAMETRICA.log");}

	

	
	private static org.apache.logging.log4j.Logger log = org.apache.logging.log4j.LogManager.getLogger(S_AML_PROFILATURA_ESTRAZIONE_MAIN_PARAMETRICA.class);
	

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
			
			if(W_JOB_PRINCIPALE != null){
				
					this.setProperty("W_JOB_PRINCIPALE", W_JOB_PRINCIPALE.toString());
				
			}
			
			if(W_PROJECT_NAME != null){
				
					this.setProperty("W_PROJECT_NAME", W_PROJECT_NAME.toString());
				
			}
			
			if(CONTEXT_SALE_FILE_NAME != null){
				
					this.setProperty("CONTEXT_SALE_FILE_NAME", CONTEXT_SALE_FILE_NAME.toString());
				
			}
			
			if(W_INIZIO_DATE != null){
				
					this.setProperty("W_INIZIO_DATE", W_INIZIO_DATE.toString());
				
			}
			
			if(W_FINE_DATE != null){
				
					this.setProperty("W_FINE_DATE", W_FINE_DATE.toString());
				
			}
			
			if(data_riferimento != null){
				
					this.setProperty("data_riferimento", data_riferimento.toString());
				
			}
			
			if(W_FILE_NAME_ESITI_CONTROLLO != null){
				
					this.setProperty("W_FILE_NAME_ESITI_CONTROLLO", W_FILE_NAME_ESITI_CONTROLLO.toString());
				
			}
			
			if(W_DATA_RIFERIMENTO != null){
				
					this.setProperty("W_DATA_RIFERIMENTO", W_DATA_RIFERIMENTO.toString());
				
			}
			
			if(W_FLAG_REG_AUI != null){
				
					this.setProperty("W_FLAG_REG_AUI", W_FLAG_REG_AUI.toString());
				
			}
			
			if(W_CRAP != null){
				
					this.setProperty("W_CRAP", W_CRAP.toString());
				
			}
			
			if(W_NDG != null){
				
					this.setProperty("W_NDG", W_NDG.toString());
				
			}
			
			if(W_ESITI_PRECEDENTI != null){
				
					this.setProperty("W_ESITI_PRECEDENTI", W_ESITI_PRECEDENTI.toString());
				
			}
			
			if(DB_CONNECTION_Database != null){
				
					this.setProperty("DB_CONNECTION_Database", DB_CONNECTION_Database.toString());
				
			}
			
			if(DB_CONNECTION_Login != null){
				
					this.setProperty("DB_CONNECTION_Login", DB_CONNECTION_Login.toString());
				
			}
			
			if(DB_CONNECTION_Password != null){
				
					this.setProperty("DB_CONNECTION_Password", DB_CONNECTION_Password.toString());
				
			}
			
			if(DB_CONNECTION_Port != null){
				
					this.setProperty("DB_CONNECTION_Port", DB_CONNECTION_Port.toString());
				
			}
			
			if(DB_CONNECTION_Server != null){
				
					this.setProperty("DB_CONNECTION_Server", DB_CONNECTION_Server.toString());
				
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
			
			if(DB_POSTGRES_POC_Database != null){
				
					this.setProperty("DB_POSTGRES_POC_Database", DB_POSTGRES_POC_Database.toString());
				
			}
			
			if(DB_POSTGRES_POC_Login != null){
				
					this.setProperty("DB_POSTGRES_POC_Login", DB_POSTGRES_POC_Login.toString());
				
			}
			
			if(DB_POSTGRES_POC_Password != null){
				
					this.setProperty("DB_POSTGRES_POC_Password", DB_POSTGRES_POC_Password.toString());
				
			}
			
			if(DB_POSTGRES_POC_Port != null){
				
					this.setProperty("DB_POSTGRES_POC_Port", DB_POSTGRES_POC_Port.toString());
				
			}
			
			if(DB_POSTGRES_POC_Schema != null){
				
					this.setProperty("DB_POSTGRES_POC_Schema", DB_POSTGRES_POC_Schema.toString());
				
			}
			
			if(DB_POSTGRES_POC_Server != null){
				
					this.setProperty("DB_POSTGRES_POC_Server", DB_POSTGRES_POC_Server.toString());
				
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
			
			if(W_CONTEXT_TYPE != null){
				
					this.setProperty("W_CONTEXT_TYPE", W_CONTEXT_TYPE.toString());
				
			}
			
			if(W_DAVINCI_TABELLA_TARGET != null){
				
					this.setProperty("W_DAVINCI_TABELLA_TARGET", W_DAVINCI_TABELLA_TARGET.toString());
				
			}
			
			if(W_DB_ANDC_DH != null){
				
					this.setProperty("W_DB_ANDC_DH", W_DB_ANDC_DH.toString());
				
			}
			
			if(W_DB_ANDC_ETL != null){
				
					this.setProperty("W_DB_ANDC_ETL", W_DB_ANDC_ETL.toString());
				
			}
			
			if(W_DB_DAVINCI_POSTGRES_CUSTOM_LOG_TABLE != null){
				
					this.setProperty("W_DB_DAVINCI_POSTGRES_CUSTOM_LOG_TABLE", W_DB_DAVINCI_POSTGRES_CUSTOM_LOG_TABLE.toString());
				
			}
			
			if(W_DB_DAVINCI_POSTGRES_FILE_METADATA_TABLE != null){
				
					this.setProperty("W_DB_DAVINCI_POSTGRES_FILE_METADATA_TABLE", W_DB_DAVINCI_POSTGRES_FILE_METADATA_TABLE.toString());
				
			}
			
			if(W_DB_DAVINCI_POSTGRES_STATEMENT_TABLE != null){
				
					this.setProperty("W_DB_DAVINCI_POSTGRES_STATEMENT_TABLE", W_DB_DAVINCI_POSTGRES_STATEMENT_TABLE.toString());
				
			}
			
			if(W_FOLDER_CSV != null){
				
					this.setProperty("W_FOLDER_CSV", W_FOLDER_CSV.toString());
				
			}
			
			if(W_FOLDER_CSV_EXPORT != null){
				
					this.setProperty("W_FOLDER_CSV_EXPORT", W_FOLDER_CSV_EXPORT.toString());
				
			}
			
			if(W_FOLDER_IN_AUTOMAZIONI_CONTROLLI_II_LIV != null){
				
					this.setProperty("W_FOLDER_IN_AUTOMAZIONI_CONTROLLI_II_LIV", W_FOLDER_IN_AUTOMAZIONI_CONTROLLI_II_LIV.toString());
				
			}
			
			if(W_FOLDER_IN_AUTOMAZIONI_CONTROLLI_II_LIV_PROFILATURA != null){
				
					this.setProperty("W_FOLDER_IN_AUTOMAZIONI_CONTROLLI_II_LIV_PROFILATURA", W_FOLDER_IN_AUTOMAZIONI_CONTROLLI_II_LIV_PROFILATURA.toString());
				
			}
			
			if(W_HDFS_DAVINCI_ETL != null){
				
					this.setProperty("W_HDFS_DAVINCI_ETL", W_HDFS_DAVINCI_ETL.toString());
				
			}
			
			if(W_HDFS_DH != null){
				
					this.setProperty("W_HDFS_DH", W_HDFS_DH.toString());
				
			}
			
			if(W_HDFS_ETL != null){
				
					this.setProperty("W_HDFS_ETL", W_HDFS_ETL.toString());
				
			}
			
			if(W_HIVE_DAVINCI_SANDBOX_DB != null){
				
					this.setProperty("W_HIVE_DAVINCI_SANDBOX_DB", W_HIVE_DAVINCI_SANDBOX_DB.toString());
				
			}
			
			if(W_HIVE_DAVINCI_SANDBOX_TABLE != null){
				
					this.setProperty("W_HIVE_DAVINCI_SANDBOX_TABLE", W_HIVE_DAVINCI_SANDBOX_TABLE.toString());
				
			}
			
			if(W_NAME_TFILE_LIST != null){
				
					this.setProperty("W_NAME_TFILE_LIST", W_NAME_TFILE_LIST.toString());
				
			}
			
			if(W_MAIN_PARAMETRICA != null){
				
					this.setProperty("W_MAIN_PARAMETRICA", W_MAIN_PARAMETRICA.toString());
				
			}
			
			if(W_ESITI_PARAMETRICA != null){
				
					this.setProperty("W_ESITI_PARAMETRICA", W_ESITI_PARAMETRICA.toString());
				
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

public String W_JOB_PRINCIPALE;
public String getW_JOB_PRINCIPALE(){
	return this.W_JOB_PRINCIPALE;
}
public String W_PROJECT_NAME;
public String getW_PROJECT_NAME(){
	return this.W_PROJECT_NAME;
}
public String CONTEXT_SALE_FILE_NAME;
public String getCONTEXT_SALE_FILE_NAME(){
	return this.CONTEXT_SALE_FILE_NAME;
}
public String W_INIZIO_DATE;
public String getW_INIZIO_DATE(){
	return this.W_INIZIO_DATE;
}
public String W_FINE_DATE;
public String getW_FINE_DATE(){
	return this.W_FINE_DATE;
}
public String data_riferimento;
public String getData_riferimento(){
	return this.data_riferimento;
}
public String W_FILE_NAME_ESITI_CONTROLLO;
public String getW_FILE_NAME_ESITI_CONTROLLO(){
	return this.W_FILE_NAME_ESITI_CONTROLLO;
}
public String W_DATA_RIFERIMENTO;
public String getW_DATA_RIFERIMENTO(){
	return this.W_DATA_RIFERIMENTO;
}
public String W_FLAG_REG_AUI;
public String getW_FLAG_REG_AUI(){
	return this.W_FLAG_REG_AUI;
}
public String W_CRAP;
public String getW_CRAP(){
	return this.W_CRAP;
}
public String W_NDG;
public String getW_NDG(){
	return this.W_NDG;
}
public String W_ESITI_PRECEDENTI;
public String getW_ESITI_PRECEDENTI(){
	return this.W_ESITI_PRECEDENTI;
}
public String DB_CONNECTION_Database;
public String getDB_CONNECTION_Database(){
	return this.DB_CONNECTION_Database;
}
public String DB_CONNECTION_Login;
public String getDB_CONNECTION_Login(){
	return this.DB_CONNECTION_Login;
}
public String DB_CONNECTION_Password;
public String getDB_CONNECTION_Password(){
	return this.DB_CONNECTION_Password;
}
public String DB_CONNECTION_Port;
public String getDB_CONNECTION_Port(){
	return this.DB_CONNECTION_Port;
}
public String DB_CONNECTION_Server;
public String getDB_CONNECTION_Server(){
	return this.DB_CONNECTION_Server;
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
public String DB_POSTGRES_POC_Database;
public String getDB_POSTGRES_POC_Database(){
	return this.DB_POSTGRES_POC_Database;
}
public String DB_POSTGRES_POC_Login;
public String getDB_POSTGRES_POC_Login(){
	return this.DB_POSTGRES_POC_Login;
}
public java.lang.String DB_POSTGRES_POC_Password;
public java.lang.String getDB_POSTGRES_POC_Password(){
	return this.DB_POSTGRES_POC_Password;
}
public String DB_POSTGRES_POC_Port;
public String getDB_POSTGRES_POC_Port(){
	return this.DB_POSTGRES_POC_Port;
}
public String DB_POSTGRES_POC_Schema;
public String getDB_POSTGRES_POC_Schema(){
	return this.DB_POSTGRES_POC_Schema;
}
public String DB_POSTGRES_POC_Server;
public String getDB_POSTGRES_POC_Server(){
	return this.DB_POSTGRES_POC_Server;
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
public String W_CONTEXT_TYPE;
public String getW_CONTEXT_TYPE(){
	return this.W_CONTEXT_TYPE;
}
public String W_DAVINCI_TABELLA_TARGET;
public String getW_DAVINCI_TABELLA_TARGET(){
	return this.W_DAVINCI_TABELLA_TARGET;
}
public String W_DB_ANDC_DH;
public String getW_DB_ANDC_DH(){
	return this.W_DB_ANDC_DH;
}
public String W_DB_ANDC_ETL;
public String getW_DB_ANDC_ETL(){
	return this.W_DB_ANDC_ETL;
}
public String W_DB_DAVINCI_POSTGRES_CUSTOM_LOG_TABLE;
public String getW_DB_DAVINCI_POSTGRES_CUSTOM_LOG_TABLE(){
	return this.W_DB_DAVINCI_POSTGRES_CUSTOM_LOG_TABLE;
}
public String W_DB_DAVINCI_POSTGRES_FILE_METADATA_TABLE;
public String getW_DB_DAVINCI_POSTGRES_FILE_METADATA_TABLE(){
	return this.W_DB_DAVINCI_POSTGRES_FILE_METADATA_TABLE;
}
public String W_DB_DAVINCI_POSTGRES_STATEMENT_TABLE;
public String getW_DB_DAVINCI_POSTGRES_STATEMENT_TABLE(){
	return this.W_DB_DAVINCI_POSTGRES_STATEMENT_TABLE;
}
public String W_FOLDER_CSV;
public String getW_FOLDER_CSV(){
	return this.W_FOLDER_CSV;
}
public String W_FOLDER_CSV_EXPORT;
public String getW_FOLDER_CSV_EXPORT(){
	return this.W_FOLDER_CSV_EXPORT;
}
public String W_FOLDER_IN_AUTOMAZIONI_CONTROLLI_II_LIV;
public String getW_FOLDER_IN_AUTOMAZIONI_CONTROLLI_II_LIV(){
	return this.W_FOLDER_IN_AUTOMAZIONI_CONTROLLI_II_LIV;
}
public String W_FOLDER_IN_AUTOMAZIONI_CONTROLLI_II_LIV_PROFILATURA;
public String getW_FOLDER_IN_AUTOMAZIONI_CONTROLLI_II_LIV_PROFILATURA(){
	return this.W_FOLDER_IN_AUTOMAZIONI_CONTROLLI_II_LIV_PROFILATURA;
}
public String W_HDFS_DAVINCI_ETL;
public String getW_HDFS_DAVINCI_ETL(){
	return this.W_HDFS_DAVINCI_ETL;
}
public String W_HDFS_DH;
public String getW_HDFS_DH(){
	return this.W_HDFS_DH;
}
public String W_HDFS_ETL;
public String getW_HDFS_ETL(){
	return this.W_HDFS_ETL;
}
public String W_HIVE_DAVINCI_SANDBOX_DB;
public String getW_HIVE_DAVINCI_SANDBOX_DB(){
	return this.W_HIVE_DAVINCI_SANDBOX_DB;
}
public String W_HIVE_DAVINCI_SANDBOX_TABLE;
public String getW_HIVE_DAVINCI_SANDBOX_TABLE(){
	return this.W_HIVE_DAVINCI_SANDBOX_TABLE;
}
public String W_NAME_TFILE_LIST;
public String getW_NAME_TFILE_LIST(){
	return this.W_NAME_TFILE_LIST;
}
public String W_MAIN_PARAMETRICA;
public String getW_MAIN_PARAMETRICA(){
	return this.W_MAIN_PARAMETRICA;
}
public String W_ESITI_PARAMETRICA;
public String getW_ESITI_PARAMETRICA(){
	return this.W_ESITI_PARAMETRICA;
}
	}
	protected ContextProperties context = new ContextProperties(); // will be instanciated by MS.
	public ContextProperties getContext() {
		return this.context;
	}
	private final String jobVersion = "0.1";
	private final String jobName = "S_AML_PROFILATURA_ESTRAZIONE_MAIN_PARAMETRICA";
	private final String projectName = "DATAHUB_AML";
	public Integer errorCode = null;
	private String currentComponent = "";
	
		private final java.util.Map<String, Object> globalMap = new java.util.HashMap<String, Object>();
        private final static java.util.Map<String, Object> junitGlobalMap = new java.util.HashMap<String, Object>();
	
		private final java.util.Map<String, Long> start_Hash = new java.util.HashMap<String, Long>();
		private final java.util.Map<String, Long> end_Hash = new java.util.HashMap<String, Long>();
		private final java.util.Map<String, Boolean> ok_Hash = new java.util.HashMap<String, Boolean>();
		public  final java.util.List<String[]> globalBuffer = new java.util.ArrayList<String[]>();
	

private final JobStructureCatcherUtils talendJobLog = new JobStructureCatcherUtils(jobName, "_VaWCQG8kEe6R7sIPfGbQnw", "0.1");
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
				S_AML_PROFILATURA_ESTRAZIONE_MAIN_PARAMETRICA.this.exception = e;
			}
		}
		if (!(e instanceof TalendException)) {
		try {
			for (java.lang.reflect.Method m : this.getClass().getEnclosingClass().getMethods()) {
				if (m.getName().compareTo(currentComponent + "_error") == 0) {
					m.invoke(S_AML_PROFILATURA_ESTRAZIONE_MAIN_PARAMETRICA.this, new Object[] { e , currentComponent, globalMap});
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
			
			public void tHiveConnection_1_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tHiveConnection_1_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tJava_1_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tJava_1_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tImpalaConnection_1_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tImpalaConnection_1_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tJava_2_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tJava_2_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tFileList_1_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tFileList_1_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tJava_3_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tFileList_1_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tFileInputDelimited_1_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tFileInputDelimited_1_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tImpalaOutput_1_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tFileInputDelimited_1_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tImpalaRow_1_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tImpalaRow_1_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void tJava_4_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					tJava_4_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void talendJobLog_error(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {
				
				end_Hash.put(errorComponent, System.currentTimeMillis());
				
				status = "failure";
				
					talendJobLog_onSubJobError(exception, errorComponent, globalMap);
			}
			
			public void Implicit_Context_Regex_onSubJobError(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {

resumeUtil.addLog("SYSTEM_LOG", "NODE:"+ errorComponent, "", Thread.currentThread().getId()+ "", "FATAL", "", exception.getMessage(), ResumeUtil.getExceptionStackTrace(exception),"");

			}
			public void tHiveConnection_1_onSubJobError(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {

resumeUtil.addLog("SYSTEM_LOG", "NODE:"+ errorComponent, "", Thread.currentThread().getId()+ "", "FATAL", "", exception.getMessage(), ResumeUtil.getExceptionStackTrace(exception),"");

			}
			public void tJava_1_onSubJobError(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {

resumeUtil.addLog("SYSTEM_LOG", "NODE:"+ errorComponent, "", Thread.currentThread().getId()+ "", "FATAL", "", exception.getMessage(), ResumeUtil.getExceptionStackTrace(exception),"");

			}
			public void tImpalaConnection_1_onSubJobError(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {

resumeUtil.addLog("SYSTEM_LOG", "NODE:"+ errorComponent, "", Thread.currentThread().getId()+ "", "FATAL", "", exception.getMessage(), ResumeUtil.getExceptionStackTrace(exception),"");

			}
			public void tJava_2_onSubJobError(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {

resumeUtil.addLog("SYSTEM_LOG", "NODE:"+ errorComponent, "", Thread.currentThread().getId()+ "", "FATAL", "", exception.getMessage(), ResumeUtil.getExceptionStackTrace(exception),"");

			}
			public void tFileList_1_onSubJobError(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {

resumeUtil.addLog("SYSTEM_LOG", "NODE:"+ errorComponent, "", Thread.currentThread().getId()+ "", "FATAL", "", exception.getMessage(), ResumeUtil.getExceptionStackTrace(exception),"");

			}
			public void tFileInputDelimited_1_onSubJobError(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {

resumeUtil.addLog("SYSTEM_LOG", "NODE:"+ errorComponent, "", Thread.currentThread().getId()+ "", "FATAL", "", exception.getMessage(), ResumeUtil.getExceptionStackTrace(exception),"");

			}
			public void tImpalaRow_1_onSubJobError(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {

resumeUtil.addLog("SYSTEM_LOG", "NODE:"+ errorComponent, "", Thread.currentThread().getId()+ "", "FATAL", "", exception.getMessage(), ResumeUtil.getExceptionStackTrace(exception),"");

			}
			public void tJava_4_onSubJobError(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {

resumeUtil.addLog("SYSTEM_LOG", "NODE:"+ errorComponent, "", Thread.currentThread().getId()+ "", "FATAL", "", exception.getMessage(), ResumeUtil.getExceptionStackTrace(exception),"");

			}
			public void talendJobLog_onSubJobError(Exception exception, String errorComponent, final java.util.Map<String, Object> globalMap) throws TalendException {

resumeUtil.addLog("SYSTEM_LOG", "NODE:"+ errorComponent, "", Thread.currentThread().getId()+ "", "FATAL", "", exception.getMessage(), ResumeUtil.getExceptionStackTrace(exception),"");

			}
	






	

public static class row_Implicit_Context_RegexStruct implements routines.system.IPersistableRow<row_Implicit_Context_RegexStruct> {
    final static byte[] commonByteArrayLock_DATAHUB_AML_S_AML_PROFILATURA_ESTRAZIONE_MAIN_PARAMETRICA = new byte[0];
    static byte[] commonByteArray_DATAHUB_AML_S_AML_PROFILATURA_ESTRAZIONE_MAIN_PARAMETRICA = new byte[0];

	
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
			if(length > commonByteArray_DATAHUB_AML_S_AML_PROFILATURA_ESTRAZIONE_MAIN_PARAMETRICA.length) {
				if(length < 1024 && commonByteArray_DATAHUB_AML_S_AML_PROFILATURA_ESTRAZIONE_MAIN_PARAMETRICA.length == 0) {
   					commonByteArray_DATAHUB_AML_S_AML_PROFILATURA_ESTRAZIONE_MAIN_PARAMETRICA = new byte[1024];
				} else {
   					commonByteArray_DATAHUB_AML_S_AML_PROFILATURA_ESTRAZIONE_MAIN_PARAMETRICA = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_DATAHUB_AML_S_AML_PROFILATURA_ESTRAZIONE_MAIN_PARAMETRICA, 0, length);
			strReturn = new String(commonByteArray_DATAHUB_AML_S_AML_PROFILATURA_ESTRAZIONE_MAIN_PARAMETRICA, 0, length, utf8Charset);
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
			if(length > commonByteArray_DATAHUB_AML_S_AML_PROFILATURA_ESTRAZIONE_MAIN_PARAMETRICA.length) {
				if(length < 1024 && commonByteArray_DATAHUB_AML_S_AML_PROFILATURA_ESTRAZIONE_MAIN_PARAMETRICA.length == 0) {
   					commonByteArray_DATAHUB_AML_S_AML_PROFILATURA_ESTRAZIONE_MAIN_PARAMETRICA = new byte[1024];
				} else {
   					commonByteArray_DATAHUB_AML_S_AML_PROFILATURA_ESTRAZIONE_MAIN_PARAMETRICA = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_DATAHUB_AML_S_AML_PROFILATURA_ESTRAZIONE_MAIN_PARAMETRICA, 0, length);
			strReturn = new String(commonByteArray_DATAHUB_AML_S_AML_PROFILATURA_ESTRAZIONE_MAIN_PARAMETRICA, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_DATAHUB_AML_S_AML_PROFILATURA_ESTRAZIONE_MAIN_PARAMETRICA) {

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

		synchronized(commonByteArrayLock_DATAHUB_AML_S_AML_PROFILATURA_ESTRAZIONE_MAIN_PARAMETRICA) {

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
						if ((key_Implicit_Context_Context != null) && ("DB_POSTGRES_POC_Password".equals(key_Implicit_Context_Context)) && (currentValue_Implicit_Context_Context != null)) 
							currentValue_Implicit_Context_Context = currentValue_Implicit_Context_Context.replaceAll(".", "*");						
						if ((key_Implicit_Context_Context != null) && ("EDH_CLUSTER_HIVE_hiveSSLTrustStorePassword".equals(key_Implicit_Context_Context)) && (currentValue_Implicit_Context_Context != null)) 
							currentValue_Implicit_Context_Context = currentValue_Implicit_Context_Context.replaceAll(".", "*");						
						if ((key_Implicit_Context_Context != null) && ("EDH_CLUSTER_HIVE_Password".equals(key_Implicit_Context_Context)) && (currentValue_Implicit_Context_Context != null)) 
							currentValue_Implicit_Context_Context = currentValue_Implicit_Context_Context.replaceAll(".", "*");

  if (tmp_key_Implicit_Context_Context != null){
  try{
        if(key_Implicit_Context_Context!=null && "W_JOB_PRINCIPALE".equals(key_Implicit_Context_Context))
        {
           context.W_JOB_PRINCIPALE=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "W_PROJECT_NAME".equals(key_Implicit_Context_Context))
        {
           context.W_PROJECT_NAME=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "CONTEXT_SALE_FILE_NAME".equals(key_Implicit_Context_Context))
        {
           context.CONTEXT_SALE_FILE_NAME=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "W_INIZIO_DATE".equals(key_Implicit_Context_Context))
        {
           context.W_INIZIO_DATE=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "W_FINE_DATE".equals(key_Implicit_Context_Context))
        {
           context.W_FINE_DATE=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "data_riferimento".equals(key_Implicit_Context_Context))
        {
           context.data_riferimento=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "W_FILE_NAME_ESITI_CONTROLLO".equals(key_Implicit_Context_Context))
        {
           context.W_FILE_NAME_ESITI_CONTROLLO=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "W_DATA_RIFERIMENTO".equals(key_Implicit_Context_Context))
        {
           context.W_DATA_RIFERIMENTO=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "W_FLAG_REG_AUI".equals(key_Implicit_Context_Context))
        {
           context.W_FLAG_REG_AUI=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "W_CRAP".equals(key_Implicit_Context_Context))
        {
           context.W_CRAP=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "W_NDG".equals(key_Implicit_Context_Context))
        {
           context.W_NDG=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "W_ESITI_PRECEDENTI".equals(key_Implicit_Context_Context))
        {
           context.W_ESITI_PRECEDENTI=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "DB_CONNECTION_Database".equals(key_Implicit_Context_Context))
        {
           context.DB_CONNECTION_Database=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "DB_CONNECTION_Login".equals(key_Implicit_Context_Context))
        {
           context.DB_CONNECTION_Login=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "DB_CONNECTION_Password".equals(key_Implicit_Context_Context))
        {
           context.DB_CONNECTION_Password=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "DB_CONNECTION_Port".equals(key_Implicit_Context_Context))
        {
           context.DB_CONNECTION_Port=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "DB_CONNECTION_Server".equals(key_Implicit_Context_Context))
        {
           context.DB_CONNECTION_Server=value_Implicit_Context_Context;
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

        if(key_Implicit_Context_Context!=null && "DB_POSTGRES_POC_Database".equals(key_Implicit_Context_Context))
        {
           context.DB_POSTGRES_POC_Database=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "DB_POSTGRES_POC_Login".equals(key_Implicit_Context_Context))
        {
           context.DB_POSTGRES_POC_Login=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "DB_POSTGRES_POC_Password".equals(key_Implicit_Context_Context))
        {
           context.DB_POSTGRES_POC_Password=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "DB_POSTGRES_POC_Port".equals(key_Implicit_Context_Context))
        {
           context.DB_POSTGRES_POC_Port=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "DB_POSTGRES_POC_Schema".equals(key_Implicit_Context_Context))
        {
           context.DB_POSTGRES_POC_Schema=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "DB_POSTGRES_POC_Server".equals(key_Implicit_Context_Context))
        {
           context.DB_POSTGRES_POC_Server=value_Implicit_Context_Context;
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

        if(key_Implicit_Context_Context!=null && "W_CONTEXT_TYPE".equals(key_Implicit_Context_Context))
        {
           context.W_CONTEXT_TYPE=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "W_DAVINCI_TABELLA_TARGET".equals(key_Implicit_Context_Context))
        {
           context.W_DAVINCI_TABELLA_TARGET=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "W_DB_ANDC_DH".equals(key_Implicit_Context_Context))
        {
           context.W_DB_ANDC_DH=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "W_DB_ANDC_ETL".equals(key_Implicit_Context_Context))
        {
           context.W_DB_ANDC_ETL=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "W_DB_DAVINCI_POSTGRES_CUSTOM_LOG_TABLE".equals(key_Implicit_Context_Context))
        {
           context.W_DB_DAVINCI_POSTGRES_CUSTOM_LOG_TABLE=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "W_DB_DAVINCI_POSTGRES_FILE_METADATA_TABLE".equals(key_Implicit_Context_Context))
        {
           context.W_DB_DAVINCI_POSTGRES_FILE_METADATA_TABLE=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "W_DB_DAVINCI_POSTGRES_STATEMENT_TABLE".equals(key_Implicit_Context_Context))
        {
           context.W_DB_DAVINCI_POSTGRES_STATEMENT_TABLE=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "W_FOLDER_CSV".equals(key_Implicit_Context_Context))
        {
           context.W_FOLDER_CSV=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "W_FOLDER_CSV_EXPORT".equals(key_Implicit_Context_Context))
        {
           context.W_FOLDER_CSV_EXPORT=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "W_FOLDER_IN_AUTOMAZIONI_CONTROLLI_II_LIV".equals(key_Implicit_Context_Context))
        {
           context.W_FOLDER_IN_AUTOMAZIONI_CONTROLLI_II_LIV=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "W_FOLDER_IN_AUTOMAZIONI_CONTROLLI_II_LIV_PROFILATURA".equals(key_Implicit_Context_Context))
        {
           context.W_FOLDER_IN_AUTOMAZIONI_CONTROLLI_II_LIV_PROFILATURA=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "W_HDFS_DAVINCI_ETL".equals(key_Implicit_Context_Context))
        {
           context.W_HDFS_DAVINCI_ETL=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "W_HDFS_DH".equals(key_Implicit_Context_Context))
        {
           context.W_HDFS_DH=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "W_HDFS_ETL".equals(key_Implicit_Context_Context))
        {
           context.W_HDFS_ETL=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "W_HIVE_DAVINCI_SANDBOX_DB".equals(key_Implicit_Context_Context))
        {
           context.W_HIVE_DAVINCI_SANDBOX_DB=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "W_HIVE_DAVINCI_SANDBOX_TABLE".equals(key_Implicit_Context_Context))
        {
           context.W_HIVE_DAVINCI_SANDBOX_TABLE=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "W_NAME_TFILE_LIST".equals(key_Implicit_Context_Context))
        {
           context.W_NAME_TFILE_LIST=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "W_MAIN_PARAMETRICA".equals(key_Implicit_Context_Context))
        {
           context.W_MAIN_PARAMETRICA=value_Implicit_Context_Context;
        }

        if(key_Implicit_Context_Context!=null && "W_ESITI_PARAMETRICA".equals(key_Implicit_Context_Context))
        {
           context.W_ESITI_PARAMETRICA=value_Implicit_Context_Context;
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
		
		parametersToEncrypt_Implicit_Context_Context.add("DB_POSTGRES_POC_Password");
		
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
                            log4jParamters_tHiveConnection_1.append("NAMENODE_PRINCIPAL" + " = " + "context.EDH_CLUSTER_HIVE_NameNodePrin");
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
				talendJobLog.addCM("tHiveConnection_1", "tHiveConnection_1", "tHiveConnection");
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
			


System.out.println("***************************************");
System.out.println("***************START*******************");
context.W_JOB_PRINCIPALE = jobName;

System.out.println("ESECUZIONE JOB: " + context.W_JOB_PRINCIPALE);

System.out.println("context.W_MAIN_PARAMETRICA: " +context.W_MAIN_PARAMETRICA);






 



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
   	 				runStat.updateStatOnConnection("OnComponentOk6", 0, "ok");
				}
				tHiveConnection_1Process(globalMap);



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
			
							String url_tImpalaConnection_1 = "jdbc:hive2://" + context.EDH_CLUSTER_IMPALA_Server + ":" + context.EDH_CLUSTER_IMPALA_Port + "/" + context.EDH_CLUSTER_IMPALA_Database + ";principal=" + context.EDH_CLUSTER_IMPALA_ImpalaPrincipal + ";ssl=true";
				String additionalJdbcSettings_tImpalaConnection_1 = "";
				if (!"".equals(additionalJdbcSettings_tImpalaConnection_1.trim())) {
					if (!additionalJdbcSettings_tImpalaConnection_1.startsWith(";")) {
						additionalJdbcSettings_tImpalaConnection_1 = ";" + additionalJdbcSettings_tImpalaConnection_1;
					}
					url_tImpalaConnection_1 += additionalJdbcSettings_tImpalaConnection_1;
				}
	String dbUser_tImpalaConnection_1 = context.EDH_CLUSTER_IMPALA_Login;
	
	
		 
	final String decryptedPassword_tImpalaConnection_1 = routines.system.PasswordEncryptUtil.decryptPassword("enc:routine.encryption.key.v1:4VRVkydyfJimmpKA9tUawnaBja4kobIKq9rvZQ==");
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
				tJava_1Process(globalMap);



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
	

public void tJava_2Process(final java.util.Map<String, Object> globalMap) throws TalendException {
	globalMap.put("tJava_2_SUBPROCESS_STATE", 0);

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
	 * [tJava_2 begin ] start
	 */

	

	
		
		ok_Hash.put("tJava_2", false);
		start_Hash.put("tJava_2", System.currentTimeMillis());
		
	
	currentComponent="tJava_2";

	
		int tos_count_tJava_2 = 0;
		
			if(enableLogStash) {
				talendJobLog.addCM("tJava_2", "KerberosSetup", "tJava");
				talendJobLogProcess(globalMap);
			}
			


System.setProperty("javax.net.ssl.trustStore", context.EDH_CLUSTER_HIVE_hiveSSLTrustStorePath);
System.setProperty("javax.net.ssl.trustStorePassword", context.EDH_CLUSTER_HIVE_hiveSSLTrustStorePassword);
org.apache.hadoop.security.UserGroupInformation.loginUserFromKeytab(context.EDH_CLUSTER_Principal, context.EDH_CLUSTER_KeyTab);
 



/**
 * [tJava_2 begin ] stop
 */
	
	/**
	 * [tJava_2 main ] start
	 */

	

	
	
	currentComponent="tJava_2";

	

 


	tos_count_tJava_2++;

/**
 * [tJava_2 main ] stop
 */
	
	/**
	 * [tJava_2 process_data_begin ] start
	 */

	

	
	
	currentComponent="tJava_2";

	

 



/**
 * [tJava_2 process_data_begin ] stop
 */
	
	/**
	 * [tJava_2 process_data_end ] start
	 */

	

	
	
	currentComponent="tJava_2";

	

 



/**
 * [tJava_2 process_data_end ] stop
 */
	
	/**
	 * [tJava_2 end ] start
	 */

	

	
	
	currentComponent="tJava_2";

	

 

ok_Hash.put("tJava_2", true);
end_Hash.put("tJava_2", System.currentTimeMillis());

				if(execStat){   
   	 				runStat.updateStatOnConnection("OnComponentOk8", 0, "ok");
				}
				tImpalaConnection_1Process(globalMap);
				if(execStat){   
   	 				runStat.updateStatOnConnection("OnComponentOk9", 0, "ok");
				}
				tFileList_1Process(globalMap);



/**
 * [tJava_2 end ] stop
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
	 * [tJava_2 finally ] start
	 */

	

	
	
	currentComponent="tJava_2";

	

 



/**
 * [tJava_2 finally ] stop
 */
				}catch(java.lang.Exception e){	
					//ignore
				}catch(java.lang.Error error){
					//ignore
				}
				resourceMap = null;
			}
		

		globalMap.put("tJava_2_SUBPROCESS_STATE", 1);
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

				
			int NB_ITERATE_tJava_3 = 0; //for statistics
			

	
		
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
                            log4jParamters_tFileList_1.append("DIRECTORY" + " = " + "context.W_FOLDER_IN_AUTOMAZIONI_CONTROLLI_II_LIV_PROFILATURA");
                        log4jParamters_tFileList_1.append(" | ");
                            log4jParamters_tFileList_1.append("LIST_MODE" + " = " + "FILES");
                        log4jParamters_tFileList_1.append(" | ");
                            log4jParamters_tFileList_1.append("INCLUDSUBDIR" + " = " + "false");
                        log4jParamters_tFileList_1.append(" | ");
                            log4jParamters_tFileList_1.append("CASE_SENSITIVE" + " = " + "YES");
                        log4jParamters_tFileList_1.append(" | ");
                            log4jParamters_tFileList_1.append("ERROR" + " = " + "true");
                        log4jParamters_tFileList_1.append(" | ");
                            log4jParamters_tFileList_1.append("GLOBEXPRESSIONS" + " = " + "true");
                        log4jParamters_tFileList_1.append(" | ");
                            log4jParamters_tFileList_1.append("FILES" + " = " + "[{FILEMASK="+("\"*\"+context.W_MAIN_PARAMETRICA+\"*.csv\"")+"}]");
                        log4jParamters_tFileList_1.append(" | ");
                            log4jParamters_tFileList_1.append("ORDER_BY_NOTHING" + " = " + "true");
                        log4jParamters_tFileList_1.append(" | ");
                            log4jParamters_tFileList_1.append("ORDER_BY_FILENAME" + " = " + "false");
                        log4jParamters_tFileList_1.append(" | ");
                            log4jParamters_tFileList_1.append("ORDER_BY_FILESIZE" + " = " + "false");
                        log4jParamters_tFileList_1.append(" | ");
                            log4jParamters_tFileList_1.append("ORDER_BY_MODIFIEDDATE" + " = " + "false");
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
			   
    
  String directory_tFileList_1 = context.W_FOLDER_IN_AUTOMAZIONI_CONTROLLI_II_LIV_PROFILATURA;
  final java.util.List<String> maskList_tFileList_1 = new java.util.ArrayList<String>();
  final java.util.List<java.util.regex.Pattern> patternList_tFileList_1 = new java.util.ArrayList<java.util.regex.Pattern>(); 
    maskList_tFileList_1.add("*"+context.W_MAIN_PARAMETRICA+"*.csv");  
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
      java.util.Collections.sort(list_tFileList_1);
    
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
	NB_ITERATE_tJava_3++;
	
	
					if(execStat){				
	       				runStat.updateStatOnConnection("OnComponentOk13", 3, 0);
					}           			
				
					if(execStat){				
	       				runStat.updateStatOnConnection("OnComponentOk12", 3, 0);
					}           			
				
					if(execStat){				
	       				runStat.updateStatOnConnection("OnComponentOk14", 3, 0);
					}           			
				
					if(execStat){				
	       				runStat.updateStatOnConnection("row1", 3, 0);
					}           			
				
				if(execStat){
					runStat.updateStatOnConnection("iterate1", 1, "exec" + NB_ITERATE_tJava_3);
					//Thread.sleep(1000);
				}				
			

	
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
			


System.out.println("Inizio estrazione:"+((String)globalMap.get("tFileList_1_CURRENT_FILEPATH")) );




 



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

				if(execStat){   
   	 				runStat.updateStatOnConnection("OnComponentOk13", 0, "ok");
				}
				tFileInputDelimited_1Process(globalMap);



/**
 * [tJava_3 end ] stop
 */
						if(execStat){
							runStat.updateStatOnConnection("iterate1", 2, "exec" + NB_ITERATE_tJava_3);
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

    if (NB_FILEtFileList_1 == 0) throw new RuntimeException("No file found in directory " + directory_tFileList_1);
  
 

 
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
		

		globalMap.put("tFileList_1_SUBPROCESS_STATE", 1);
	}
	


public static class row1Struct implements routines.system.IPersistableRow<row1Struct> {
    final static byte[] commonByteArrayLock_DATAHUB_AML_S_AML_PROFILATURA_ESTRAZIONE_MAIN_PARAMETRICA = new byte[0];
    static byte[] commonByteArray_DATAHUB_AML_S_AML_PROFILATURA_ESTRAZIONE_MAIN_PARAMETRICA = new byte[0];

	
			    public String data_inizio_trim;

				public String getData_inizio_trim () {
					return this.data_inizio_trim;
				}

				public Boolean data_inizio_trimIsNullable(){
				    return true;
				}
				public Boolean data_inizio_trimIsKey(){
				    return false;
				}
				public Integer data_inizio_trimLength(){
				    return null;
				}
				public Integer data_inizio_trimPrecision(){
				    return null;
				}
				public String data_inizio_trimDefault(){
				
					return null;
				
				}
				public String data_inizio_trimComment(){
				
				    return "";
				
				}
				public String data_inizio_trimPattern(){
				
					return "";
				
				}
				public String data_inizio_trimOriginalDbColumnName(){
				
					return "data_inizio_trim";
				
				}

				
			    public String data_fine_trim;

				public String getData_fine_trim () {
					return this.data_fine_trim;
				}

				public Boolean data_fine_trimIsNullable(){
				    return true;
				}
				public Boolean data_fine_trimIsKey(){
				    return false;
				}
				public Integer data_fine_trimLength(){
				    return null;
				}
				public Integer data_fine_trimPrecision(){
				    return null;
				}
				public String data_fine_trimDefault(){
				
					return null;
				
				}
				public String data_fine_trimComment(){
				
				    return "";
				
				}
				public String data_fine_trimPattern(){
				
					return "dd-MM-yyyy";
				
				}
				public String data_fine_trimOriginalDbColumnName(){
				
					return "data_fine_trim";
				
				}

				
			    public String filtro_data;

				public String getFiltro_data () {
					return this.filtro_data;
				}

				public Boolean filtro_dataIsNullable(){
				    return true;
				}
				public Boolean filtro_dataIsKey(){
				    return false;
				}
				public Integer filtro_dataLength(){
				    return null;
				}
				public Integer filtro_dataPrecision(){
				    return null;
				}
				public String filtro_dataDefault(){
				
					return null;
				
				}
				public String filtro_dataComment(){
				
				    return "";
				
				}
				public String filtro_dataPattern(){
				
					return "";
				
				}
				public String filtro_dataOriginalDbColumnName(){
				
					return "filtro_data";
				
				}

				
			    public String an_fine_validita_filtro_a;

				public String getAn_fine_validita_filtro_a () {
					return this.an_fine_validita_filtro_a;
				}

				public Boolean an_fine_validita_filtro_aIsNullable(){
				    return true;
				}
				public Boolean an_fine_validita_filtro_aIsKey(){
				    return false;
				}
				public Integer an_fine_validita_filtro_aLength(){
				    return null;
				}
				public Integer an_fine_validita_filtro_aPrecision(){
				    return null;
				}
				public String an_fine_validita_filtro_aDefault(){
				
					return null;
				
				}
				public String an_fine_validita_filtro_aComment(){
				
				    return "";
				
				}
				public String an_fine_validita_filtro_aPattern(){
				
					return "";
				
				}
				public String an_fine_validita_filtro_aOriginalDbColumnName(){
				
					return "an_fine_validita_filtro_a";
				
				}

				
			    public String an_modulo_pervenuto_filtro_a;

				public String getAn_modulo_pervenuto_filtro_a () {
					return this.an_modulo_pervenuto_filtro_a;
				}

				public Boolean an_modulo_pervenuto_filtro_aIsNullable(){
				    return true;
				}
				public Boolean an_modulo_pervenuto_filtro_aIsKey(){
				    return false;
				}
				public Integer an_modulo_pervenuto_filtro_aLength(){
				    return null;
				}
				public Integer an_modulo_pervenuto_filtro_aPrecision(){
				    return null;
				}
				public String an_modulo_pervenuto_filtro_aDefault(){
				
					return null;
				
				}
				public String an_modulo_pervenuto_filtro_aComment(){
				
				    return "";
				
				}
				public String an_modulo_pervenuto_filtro_aPattern(){
				
					return "";
				
				}
				public String an_modulo_pervenuto_filtro_aOriginalDbColumnName(){
				
					return "an_modulo_pervenuto_filtro_a";
				
				}

				
			    public String data_modulo_av_filtro_a;

				public String getData_modulo_av_filtro_a () {
					return this.data_modulo_av_filtro_a;
				}

				public Boolean data_modulo_av_filtro_aIsNullable(){
				    return true;
				}
				public Boolean data_modulo_av_filtro_aIsKey(){
				    return false;
				}
				public Integer data_modulo_av_filtro_aLength(){
				    return null;
				}
				public Integer data_modulo_av_filtro_aPrecision(){
				    return null;
				}
				public String data_modulo_av_filtro_aDefault(){
				
					return null;
				
				}
				public String data_modulo_av_filtro_aComment(){
				
				    return "";
				
				}
				public String data_modulo_av_filtro_aPattern(){
				
					return "";
				
				}
				public String data_modulo_av_filtro_aOriginalDbColumnName(){
				
					return "data_modulo_av_filtro_a";
				
				}

				
			    public String an_sottogruppo_sae_filtro_a;

				public String getAn_sottogruppo_sae_filtro_a () {
					return this.an_sottogruppo_sae_filtro_a;
				}

				public Boolean an_sottogruppo_sae_filtro_aIsNullable(){
				    return true;
				}
				public Boolean an_sottogruppo_sae_filtro_aIsKey(){
				    return false;
				}
				public Integer an_sottogruppo_sae_filtro_aLength(){
				    return null;
				}
				public Integer an_sottogruppo_sae_filtro_aPrecision(){
				    return null;
				}
				public String an_sottogruppo_sae_filtro_aDefault(){
				
					return null;
				
				}
				public String an_sottogruppo_sae_filtro_aComment(){
				
				    return "";
				
				}
				public String an_sottogruppo_sae_filtro_aPattern(){
				
					return "";
				
				}
				public String an_sottogruppo_sae_filtro_aOriginalDbColumnName(){
				
					return "an_sottogruppo_sae_filtro_a";
				
				}

				



	private String readString(ObjectInputStream dis) throws IOException{
		String strReturn = null;
		int length = 0;
        length = dis.readInt();
		if (length == -1) {
			strReturn = null;
		} else {
			if(length > commonByteArray_DATAHUB_AML_S_AML_PROFILATURA_ESTRAZIONE_MAIN_PARAMETRICA.length) {
				if(length < 1024 && commonByteArray_DATAHUB_AML_S_AML_PROFILATURA_ESTRAZIONE_MAIN_PARAMETRICA.length == 0) {
   					commonByteArray_DATAHUB_AML_S_AML_PROFILATURA_ESTRAZIONE_MAIN_PARAMETRICA = new byte[1024];
				} else {
   					commonByteArray_DATAHUB_AML_S_AML_PROFILATURA_ESTRAZIONE_MAIN_PARAMETRICA = new byte[2 * length];
   				}
			}
			dis.readFully(commonByteArray_DATAHUB_AML_S_AML_PROFILATURA_ESTRAZIONE_MAIN_PARAMETRICA, 0, length);
			strReturn = new String(commonByteArray_DATAHUB_AML_S_AML_PROFILATURA_ESTRAZIONE_MAIN_PARAMETRICA, 0, length, utf8Charset);
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
			if(length > commonByteArray_DATAHUB_AML_S_AML_PROFILATURA_ESTRAZIONE_MAIN_PARAMETRICA.length) {
				if(length < 1024 && commonByteArray_DATAHUB_AML_S_AML_PROFILATURA_ESTRAZIONE_MAIN_PARAMETRICA.length == 0) {
   					commonByteArray_DATAHUB_AML_S_AML_PROFILATURA_ESTRAZIONE_MAIN_PARAMETRICA = new byte[1024];
				} else {
   					commonByteArray_DATAHUB_AML_S_AML_PROFILATURA_ESTRAZIONE_MAIN_PARAMETRICA = new byte[2 * length];
   				}
			}
			unmarshaller.readFully(commonByteArray_DATAHUB_AML_S_AML_PROFILATURA_ESTRAZIONE_MAIN_PARAMETRICA, 0, length);
			strReturn = new String(commonByteArray_DATAHUB_AML_S_AML_PROFILATURA_ESTRAZIONE_MAIN_PARAMETRICA, 0, length, utf8Charset);
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

		synchronized(commonByteArrayLock_DATAHUB_AML_S_AML_PROFILATURA_ESTRAZIONE_MAIN_PARAMETRICA) {

        	try {

        		int length = 0;
		
					this.data_inizio_trim = readString(dis);
					
					this.data_fine_trim = readString(dis);
					
					this.filtro_data = readString(dis);
					
					this.an_fine_validita_filtro_a = readString(dis);
					
					this.an_modulo_pervenuto_filtro_a = readString(dis);
					
					this.data_modulo_av_filtro_a = readString(dis);
					
					this.an_sottogruppo_sae_filtro_a = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }
    
    public void readData(org.jboss.marshalling.Unmarshaller dis) {

		synchronized(commonByteArrayLock_DATAHUB_AML_S_AML_PROFILATURA_ESTRAZIONE_MAIN_PARAMETRICA) {

        	try {

        		int length = 0;
		
					this.data_inizio_trim = readString(dis);
					
					this.data_fine_trim = readString(dis);
					
					this.filtro_data = readString(dis);
					
					this.an_fine_validita_filtro_a = readString(dis);
					
					this.an_modulo_pervenuto_filtro_a = readString(dis);
					
					this.data_modulo_av_filtro_a = readString(dis);
					
					this.an_sottogruppo_sae_filtro_a = readString(dis);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);

		

        }

		

      }


    }

    public void writeData(ObjectOutputStream dos) {
        try {

		
					// String
				
						writeString(this.data_inizio_trim,dos);
					
					// String
				
						writeString(this.data_fine_trim,dos);
					
					// String
				
						writeString(this.filtro_data,dos);
					
					// String
				
						writeString(this.an_fine_validita_filtro_a,dos);
					
					// String
				
						writeString(this.an_modulo_pervenuto_filtro_a,dos);
					
					// String
				
						writeString(this.data_modulo_av_filtro_a,dos);
					
					// String
				
						writeString(this.an_sottogruppo_sae_filtro_a,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }
    
    public void writeData(org.jboss.marshalling.Marshaller dos) {
        try {

		
					// String
				
						writeString(this.data_inizio_trim,dos);
					
					// String
				
						writeString(this.data_fine_trim,dos);
					
					// String
				
						writeString(this.filtro_data,dos);
					
					// String
				
						writeString(this.an_fine_validita_filtro_a,dos);
					
					// String
				
						writeString(this.an_modulo_pervenuto_filtro_a,dos);
					
					// String
				
						writeString(this.data_modulo_av_filtro_a,dos);
					
					// String
				
						writeString(this.an_sottogruppo_sae_filtro_a,dos);
					
        	} catch (IOException e) {
	            throw new RuntimeException(e);
        }


    }


    public String toString() {

		StringBuilder sb = new StringBuilder();
		sb.append(super.toString());
		sb.append("[");
		sb.append("data_inizio_trim="+data_inizio_trim);
		sb.append(",data_fine_trim="+data_fine_trim);
		sb.append(",filtro_data="+filtro_data);
		sb.append(",an_fine_validita_filtro_a="+an_fine_validita_filtro_a);
		sb.append(",an_modulo_pervenuto_filtro_a="+an_modulo_pervenuto_filtro_a);
		sb.append(",data_modulo_av_filtro_a="+data_modulo_av_filtro_a);
		sb.append(",an_sottogruppo_sae_filtro_a="+an_sottogruppo_sae_filtro_a);
	    sb.append("]");

	    return sb.toString();
    }
        public String toLogString(){
        	StringBuilder sb = new StringBuilder();
        	
        				if(data_inizio_trim == null){
        					sb.append("<null>");
        				}else{
            				sb.append(data_inizio_trim);
            			}
            		
        			sb.append("|");
        		
        				if(data_fine_trim == null){
        					sb.append("<null>");
        				}else{
            				sb.append(data_fine_trim);
            			}
            		
        			sb.append("|");
        		
        				if(filtro_data == null){
        					sb.append("<null>");
        				}else{
            				sb.append(filtro_data);
            			}
            		
        			sb.append("|");
        		
        				if(an_fine_validita_filtro_a == null){
        					sb.append("<null>");
        				}else{
            				sb.append(an_fine_validita_filtro_a);
            			}
            		
        			sb.append("|");
        		
        				if(an_modulo_pervenuto_filtro_a == null){
        					sb.append("<null>");
        				}else{
            				sb.append(an_modulo_pervenuto_filtro_a);
            			}
            		
        			sb.append("|");
        		
        				if(data_modulo_av_filtro_a == null){
        					sb.append("<null>");
        				}else{
            				sb.append(data_modulo_av_filtro_a);
            			}
            		
        			sb.append("|");
        		
        				if(an_sottogruppo_sae_filtro_a == null){
        					sb.append("<null>");
        				}else{
            				sb.append(an_sottogruppo_sae_filtro_a);
            			}
            		
        			sb.append("|");
        		
        	return sb.toString();
        }

    /**
     * Compare keys
     */
    public int compareTo(row1Struct other) {

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
public void tFileInputDelimited_1Process(final java.util.Map<String, Object> globalMap) throws TalendException {
	globalMap.put("tFileInputDelimited_1_SUBPROCESS_STATE", 0);

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



		row1Struct row1 = new row1Struct();




	
	/**
	 * [tImpalaOutput_1 begin ] start
	 */

	

	
		
		ok_Hash.put("tImpalaOutput_1", false);
		start_Hash.put("tImpalaOutput_1", System.currentTimeMillis());
		
	
	currentComponent="tImpalaOutput_1";

	
			runStat.updateStatAndLog(execStat,enableLogStash,resourceMap,iterateId,0,0,"row1");
			
		int tos_count_tImpalaOutput_1 = 0;
		
			if(enableLogStash) {
				talendJobLog.addCM("tImpalaOutput_1", "tImpalaOutput_1", "tImpalaOutput");
				talendJobLogProcess(globalMap);
			}
			
		java.sql.Connection conn_tImpalaOutput_1 = (java.sql.Connection)globalMap.get("conn_tImpalaConnection_1");
		String dbname_tImpalaOutput_1 = (String)globalMap.get("db_tImpalaConnection_1");
	if(dbname_tImpalaOutput_1!=null && !"".equals(dbname_tImpalaOutput_1.trim()) && !"default".equals(dbname_tImpalaOutput_1.trim())) {
		java.sql.Statement goToDatabase_tImpalaOutput_1 = conn_tImpalaOutput_1.createStatement();
		goToDatabase_tImpalaOutput_1.execute("use " + dbname_tImpalaOutput_1);
		goToDatabase_tImpalaOutput_1.close();
    }
	String tableName_tImpalaOutput_1 = "andc_etl.profilatura_main_tab_parametrica";
	int nb_line_tImpalaOutput_1 = 0;
    String insertPrefix_tImpalaOutput_1 = "INSERT OVERWRITE " + tableName_tImpalaOutput_1 + " (data_inizio_trim,data_fine_trim,filtro_data,an_fine_validita_filtro_a,an_modulo_pervenuto_filtro_a,data_modulo_av_filtro_a,an_sottogruppo_sae_filtro_a) VALUES ";		
	java.lang.StringBuilder sbValue_tImpalaOutput_1 = null;
    java.sql.Statement stmt_tImpalaOutput_1 = conn_tImpalaOutput_1.createStatement();
    int rowCounttImpalaOutput_1 = 0;
    boolean needInitialization_tImpalaOutput_1 = true;
	 
 



/**
 * [tImpalaOutput_1 begin ] stop
 */



	
	/**
	 * [tFileInputDelimited_1 begin ] start
	 */

	

	
		
		ok_Hash.put("tFileInputDelimited_1", false);
		start_Hash.put("tFileInputDelimited_1", System.currentTimeMillis());
		
	
	currentComponent="tFileInputDelimited_1";

	
		int tos_count_tFileInputDelimited_1 = 0;
		
                if(log.isDebugEnabled())
            log.debug("tFileInputDelimited_1 - "  + ("Start to work.") );
            if (log.isDebugEnabled()) {
                class BytesLimit65535_tFileInputDelimited_1{
                    public void limitLog4jByte() throws Exception{
                    StringBuilder log4jParamters_tFileInputDelimited_1 = new StringBuilder();
                    log4jParamters_tFileInputDelimited_1.append("Parameters:");
                            log4jParamters_tFileInputDelimited_1.append("FILENAME" + " = " + "((String)globalMap.get(\"tFileList_1_CURRENT_FILEPATH\"))");
                        log4jParamters_tFileInputDelimited_1.append(" | ");
                            log4jParamters_tFileInputDelimited_1.append("CSV_OPTION" + " = " + "false");
                        log4jParamters_tFileInputDelimited_1.append(" | ");
                            log4jParamters_tFileInputDelimited_1.append("ROWSEPARATOR" + " = " + "\"\\n\"");
                        log4jParamters_tFileInputDelimited_1.append(" | ");
                            log4jParamters_tFileInputDelimited_1.append("FIELDSEPARATOR" + " = " + "\";\"");
                        log4jParamters_tFileInputDelimited_1.append(" | ");
                            log4jParamters_tFileInputDelimited_1.append("HEADER" + " = " + "1");
                        log4jParamters_tFileInputDelimited_1.append(" | ");
                            log4jParamters_tFileInputDelimited_1.append("FOOTER" + " = " + "0");
                        log4jParamters_tFileInputDelimited_1.append(" | ");
                            log4jParamters_tFileInputDelimited_1.append("LIMIT" + " = " + "");
                        log4jParamters_tFileInputDelimited_1.append(" | ");
                            log4jParamters_tFileInputDelimited_1.append("REMOVE_EMPTY_ROW" + " = " + "true");
                        log4jParamters_tFileInputDelimited_1.append(" | ");
                            log4jParamters_tFileInputDelimited_1.append("UNCOMPRESS" + " = " + "false");
                        log4jParamters_tFileInputDelimited_1.append(" | ");
                            log4jParamters_tFileInputDelimited_1.append("DIE_ON_ERROR" + " = " + "false");
                        log4jParamters_tFileInputDelimited_1.append(" | ");
                            log4jParamters_tFileInputDelimited_1.append("ADVANCED_SEPARATOR" + " = " + "false");
                        log4jParamters_tFileInputDelimited_1.append(" | ");
                            log4jParamters_tFileInputDelimited_1.append("RANDOM" + " = " + "false");
                        log4jParamters_tFileInputDelimited_1.append(" | ");
                            log4jParamters_tFileInputDelimited_1.append("TRIMALL" + " = " + "false");
                        log4jParamters_tFileInputDelimited_1.append(" | ");
                            log4jParamters_tFileInputDelimited_1.append("TRIMSELECT" + " = " + "[{TRIM="+("false")+", SCHEMA_COLUMN="+("data_inizio_trim")+"}, {TRIM="+("false")+", SCHEMA_COLUMN="+("data_fine_trim")+"}, {TRIM="+("false")+", SCHEMA_COLUMN="+("filtro_data")+"}, {TRIM="+("false")+", SCHEMA_COLUMN="+("an_fine_validita_filtro_a")+"}, {TRIM="+("false")+", SCHEMA_COLUMN="+("an_modulo_pervenuto_filtro_a")+"}, {TRIM="+("false")+", SCHEMA_COLUMN="+("data_modulo_av_filtro_a")+"}, {TRIM="+("false")+", SCHEMA_COLUMN="+("an_sottogruppo_sae_filtro_a")+"}]");
                        log4jParamters_tFileInputDelimited_1.append(" | ");
                            log4jParamters_tFileInputDelimited_1.append("CHECK_FIELDS_NUM" + " = " + "false");
                        log4jParamters_tFileInputDelimited_1.append(" | ");
                            log4jParamters_tFileInputDelimited_1.append("CHECK_DATE" + " = " + "false");
                        log4jParamters_tFileInputDelimited_1.append(" | ");
                            log4jParamters_tFileInputDelimited_1.append("ENCODING" + " = " + "\"ISO-8859-15\"");
                        log4jParamters_tFileInputDelimited_1.append(" | ");
                            log4jParamters_tFileInputDelimited_1.append("SPLITRECORD" + " = " + "false");
                        log4jParamters_tFileInputDelimited_1.append(" | ");
                            log4jParamters_tFileInputDelimited_1.append("ENABLE_DECODE" + " = " + "false");
                        log4jParamters_tFileInputDelimited_1.append(" | ");
                if(log.isDebugEnabled())
            log.debug("tFileInputDelimited_1 - "  + (log4jParamters_tFileInputDelimited_1) );
                    } 
                } 
            new BytesLimit65535_tFileInputDelimited_1().limitLog4jByte();
            }
			if(enableLogStash) {
				talendJobLog.addCM("tFileInputDelimited_1", "tFileInputDelimited_1", "tFileInputDelimited");
				talendJobLogProcess(globalMap);
			}
			
	
	
	
 
	
	
	final routines.system.RowState rowstate_tFileInputDelimited_1 = new routines.system.RowState();
	
	
				int nb_line_tFileInputDelimited_1 = 0;
				org.talend.fileprocess.FileInputDelimited fid_tFileInputDelimited_1 = null;
				int limit_tFileInputDelimited_1 = -1;
				try{
					
						Object filename_tFileInputDelimited_1 = ((String)globalMap.get("tFileList_1_CURRENT_FILEPATH"));
						if(filename_tFileInputDelimited_1 instanceof java.io.InputStream){
							
			int footer_value_tFileInputDelimited_1 = 0, random_value_tFileInputDelimited_1 = -1;
			if(footer_value_tFileInputDelimited_1 >0 || random_value_tFileInputDelimited_1 > 0){
				throw new java.lang.Exception("When the input source is a stream,footer and random shouldn't be bigger than 0.");				
			}
		
						}
						try {
							fid_tFileInputDelimited_1 = new org.talend.fileprocess.FileInputDelimited(((String)globalMap.get("tFileList_1_CURRENT_FILEPATH")), "ISO-8859-15",";","\n",true,1,0,
									limit_tFileInputDelimited_1
								,-1, false);
						} catch(java.lang.Exception e) {
globalMap.put("tFileInputDelimited_1_ERROR_MESSAGE",e.getMessage());
							
								
									log.error("tFileInputDelimited_1 - " +e.getMessage());
								
								System.err.println(e.getMessage());
							
						}
					
				    
				    	log.info("tFileInputDelimited_1 - Retrieving records from the datasource.");
				    
					while (fid_tFileInputDelimited_1!=null && fid_tFileInputDelimited_1.nextRecord()) {
						rowstate_tFileInputDelimited_1.reset();
						
			    						row1 = null;			
												
									boolean whetherReject_tFileInputDelimited_1 = false;
									row1 = new row1Struct();
									try {
										
				int columnIndexWithD_tFileInputDelimited_1 = 0;
				
					columnIndexWithD_tFileInputDelimited_1 = 0;
					
							row1.data_inizio_trim = fid_tFileInputDelimited_1.get(columnIndexWithD_tFileInputDelimited_1);
						
				
					columnIndexWithD_tFileInputDelimited_1 = 1;
					
							row1.data_fine_trim = fid_tFileInputDelimited_1.get(columnIndexWithD_tFileInputDelimited_1);
						
				
					columnIndexWithD_tFileInputDelimited_1 = 2;
					
							row1.filtro_data = fid_tFileInputDelimited_1.get(columnIndexWithD_tFileInputDelimited_1);
						
				
					columnIndexWithD_tFileInputDelimited_1 = 3;
					
							row1.an_fine_validita_filtro_a = fid_tFileInputDelimited_1.get(columnIndexWithD_tFileInputDelimited_1);
						
				
					columnIndexWithD_tFileInputDelimited_1 = 4;
					
							row1.an_modulo_pervenuto_filtro_a = fid_tFileInputDelimited_1.get(columnIndexWithD_tFileInputDelimited_1);
						
				
					columnIndexWithD_tFileInputDelimited_1 = 5;
					
							row1.data_modulo_av_filtro_a = fid_tFileInputDelimited_1.get(columnIndexWithD_tFileInputDelimited_1);
						
				
					columnIndexWithD_tFileInputDelimited_1 = 6;
					
							row1.an_sottogruppo_sae_filtro_a = fid_tFileInputDelimited_1.get(columnIndexWithD_tFileInputDelimited_1);
						
				
				
										
										if(rowstate_tFileInputDelimited_1.getException()!=null) {
											throw rowstate_tFileInputDelimited_1.getException();
										}
										
										
							
			    					} catch (java.lang.Exception e) {
globalMap.put("tFileInputDelimited_1_ERROR_MESSAGE",e.getMessage());
			        					whetherReject_tFileInputDelimited_1 = true;
			        					
												log.error("tFileInputDelimited_1 - " +e.getMessage());
											
			                					System.err.println(e.getMessage());
			                					row1 = null;
			                				
										
			    					}
								
			log.debug("tFileInputDelimited_1 - Retrieving the record " + fid_tFileInputDelimited_1.getRowNumber() + ".");
		

 



/**
 * [tFileInputDelimited_1 begin ] stop
 */
	
	/**
	 * [tFileInputDelimited_1 main ] start
	 */

	

	
	
	currentComponent="tFileInputDelimited_1";

	

 


	tos_count_tFileInputDelimited_1++;

/**
 * [tFileInputDelimited_1 main ] stop
 */
	
	/**
	 * [tFileInputDelimited_1 process_data_begin ] start
	 */

	

	
	
	currentComponent="tFileInputDelimited_1";

	

 



/**
 * [tFileInputDelimited_1 process_data_begin ] stop
 */
// Start of branch "row1"
if(row1 != null) { 



	
	/**
	 * [tImpalaOutput_1 main ] start
	 */

	

	
	
	currentComponent="tImpalaOutput_1";

	
			if(runStat.update(execStat,enableLogStash,iterateId,1,1
				
					,"row1","tFileInputDelimited_1","tFileInputDelimited_1","tFileInputDelimited","tImpalaOutput_1","tImpalaOutput_1","tImpalaOutput"
				
			)) {
				talendJobLogProcess(globalMap);
			}
			
    			if(log.isTraceEnabled()){
    				log.trace("row1 - " + (row1==null? "": row1.toLogString()));
    			}
    		
	



        if(needInitialization_tImpalaOutput_1) {
                needInitialization_tImpalaOutput_1 = false;
            sbValue_tImpalaOutput_1 = new java.lang.StringBuilder(insertPrefix_tImpalaOutput_1);
        }
        sbValue_tImpalaOutput_1.append("(");
                    if(row1.data_inizio_trim==null) {
                        sbValue_tImpalaOutput_1.append("null");
                    } else {
                    sbValue_tImpalaOutput_1.append("'" + row1.data_inizio_trim.replaceAll("\'", "\\\\'") + "'");
                    }
                    if(row1.data_fine_trim==null) {
                        sbValue_tImpalaOutput_1.append(",null");
                    } else {
                    sbValue_tImpalaOutput_1.append(",'" + row1.data_fine_trim.replaceAll("\'", "\\\\'") + "'");
                    }
                    if(row1.filtro_data==null) {
                        sbValue_tImpalaOutput_1.append(",null");
                    } else {
                    sbValue_tImpalaOutput_1.append(",'" + row1.filtro_data.replaceAll("\'", "\\\\'") + "'");
                    }
                    if(row1.an_fine_validita_filtro_a==null) {
                        sbValue_tImpalaOutput_1.append(",null");
                    } else {
                    sbValue_tImpalaOutput_1.append(",'" + row1.an_fine_validita_filtro_a.replaceAll("\'", "\\\\'") + "'");
                    }
                    if(row1.an_modulo_pervenuto_filtro_a==null) {
                        sbValue_tImpalaOutput_1.append(",null");
                    } else {
                    sbValue_tImpalaOutput_1.append(",'" + row1.an_modulo_pervenuto_filtro_a.replaceAll("\'", "\\\\'") + "'");
                    }
                    if(row1.data_modulo_av_filtro_a==null) {
                        sbValue_tImpalaOutput_1.append(",null");
                    } else {
                    sbValue_tImpalaOutput_1.append(",'" + row1.data_modulo_av_filtro_a.replaceAll("\'", "\\\\'") + "'");
                    }
                    if(row1.an_sottogruppo_sae_filtro_a==null) {
                        sbValue_tImpalaOutput_1.append(",null");
                    } else {
                    sbValue_tImpalaOutput_1.append(",'" + row1.an_sottogruppo_sae_filtro_a.replaceAll("\'", "\\\\'") + "'");
                    }
        sbValue_tImpalaOutput_1.append(")");
        rowCounttImpalaOutput_1++;
		try {
			nb_line_tImpalaOutput_1++;
                if(rowCounttImpalaOutput_1 == 100) {
			stmt_tImpalaOutput_1.executeUpdate(sbValue_tImpalaOutput_1.toString());
			insertPrefix_tImpalaOutput_1 = insertPrefix_tImpalaOutput_1.replace("INSERT OVERWRITE", "INSERT INTO");
			
                    rowCounttImpalaOutput_1 = 0;
                    needInitialization_tImpalaOutput_1 = true;
                } else {
                    sbValue_tImpalaOutput_1.append(",");                    
                }
		} catch(java.lang.Exception e) {
				System.err.print("Exception in the component tImpalaOutput_1:" + e.getMessage());
		}
 


	tos_count_tImpalaOutput_1++;

/**
 * [tImpalaOutput_1 main ] stop
 */
	
	/**
	 * [tImpalaOutput_1 process_data_begin ] start
	 */

	

	
	
	currentComponent="tImpalaOutput_1";

	

 



/**
 * [tImpalaOutput_1 process_data_begin ] stop
 */
	
	/**
	 * [tImpalaOutput_1 process_data_end ] start
	 */

	

	
	
	currentComponent="tImpalaOutput_1";

	

 



/**
 * [tImpalaOutput_1 process_data_end ] stop
 */

} // End of branch "row1"




	
	/**
	 * [tFileInputDelimited_1 process_data_end ] start
	 */

	

	
	
	currentComponent="tFileInputDelimited_1";

	

 



/**
 * [tFileInputDelimited_1 process_data_end ] stop
 */
	
	/**
	 * [tFileInputDelimited_1 end ] start
	 */

	

	
	
	currentComponent="tFileInputDelimited_1";

	



            }
            }finally{
                if(!((Object)(((String)globalMap.get("tFileList_1_CURRENT_FILEPATH"))) instanceof java.io.InputStream)){
                	if(fid_tFileInputDelimited_1!=null){
                		fid_tFileInputDelimited_1.close();
                	}
                }
                if(fid_tFileInputDelimited_1!=null){
                	globalMap.put("tFileInputDelimited_1_NB_LINE", fid_tFileInputDelimited_1.getRowNumber());
					
						log.info("tFileInputDelimited_1 - Retrieved records count: "+ fid_tFileInputDelimited_1.getRowNumber() + ".");
					
                }
			}
			  

 
                if(log.isDebugEnabled())
            log.debug("tFileInputDelimited_1 - "  + ("Done.") );

ok_Hash.put("tFileInputDelimited_1", true);
end_Hash.put("tFileInputDelimited_1", System.currentTimeMillis());




/**
 * [tFileInputDelimited_1 end ] stop
 */

	
	/**
	 * [tImpalaOutput_1 end ] start
	 */

	

	
	
	currentComponent="tImpalaOutput_1";

	
	



        if(rowCounttImpalaOutput_1 != 0) {
            try {
                stmt_tImpalaOutput_1.executeUpdate(sbValue_tImpalaOutput_1.toString().substring(0, sbValue_tImpalaOutput_1.toString().length()-1));
            } catch(java.lang.Exception e) {
                    System.err.print("Exception in the component tImpalaOutput_1:" + e.getMessage());
            }
        }

	try {
		if (stmt_tImpalaOutput_1 != null) {
			stmt_tImpalaOutput_1.close();
		}
	}
	catch (java.lang.Exception e) {
			System.err.print("Exception in the component tImpalaOutput_1:" + e.getMessage());
}
	
			 		if(runStat.updateStatAndLog(execStat,enableLogStash,resourceMap,iterateId,"row1",2,0,
			 			"tFileInputDelimited_1","tFileInputDelimited_1","tFileInputDelimited","tImpalaOutput_1","tImpalaOutput_1","tImpalaOutput","output")) {
						talendJobLogProcess(globalMap);
					}
				
 

ok_Hash.put("tImpalaOutput_1", true);
end_Hash.put("tImpalaOutput_1", System.currentTimeMillis());

				if(execStat){   
   	 				runStat.updateStatOnConnection("OnComponentOk12", 0, "ok");
				}
				tImpalaRow_1Process(globalMap);



/**
 * [tImpalaOutput_1 end ] stop
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
	 * [tFileInputDelimited_1 finally ] start
	 */

	

	
	
	currentComponent="tFileInputDelimited_1";

	

 



/**
 * [tFileInputDelimited_1 finally ] stop
 */

	
	/**
	 * [tImpalaOutput_1 finally ] start
	 */

	

	
	
	currentComponent="tImpalaOutput_1";

	

 



/**
 * [tImpalaOutput_1 finally ] stop
 */



				}catch(java.lang.Exception e){	
					//ignore
				}catch(java.lang.Error error){
					//ignore
				}
				resourceMap = null;
			}
		

		globalMap.put("tFileInputDelimited_1_SUBPROCESS_STATE", 1);
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
				talendJobLog.addCM("tImpalaRow_1", "tImpalaRow_1", "tImpalaRow");
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

	

	query_tImpalaRow_1 = "INVALIDATE METADATA andc_etl.profilatura_main_tab_parametrica";
	whetherReject_tImpalaRow_1 = false;

	
	globalMap.put("tImpalaRow_1_QUERY",query_tImpalaRow_1);
	try {
	    		stmt_tImpalaRow_1.execute(query_tImpalaRow_1);
	} catch (java.lang.Exception e) {
		whetherReject_tImpalaRow_1 = true;
				log.error("tImpalaRow_1 - " + e.getMessage());
				System.err.println("Exception in the component tImpalaRow_1:" + e.getMessage());
    }
		if(!whetherReject_tImpalaRow_1) {
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

				if(execStat){   
   	 				runStat.updateStatOnConnection("OnComponentOk14", 0, "ok");
				}
				tJava_4Process(globalMap);



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
	

public void tJava_4Process(final java.util.Map<String, Object> globalMap) throws TalendException {
	globalMap.put("tJava_4_SUBPROCESS_STATE", 0);

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
	 * [tJava_4 begin ] start
	 */

	

	
		
		ok_Hash.put("tJava_4", false);
		start_Hash.put("tJava_4", System.currentTimeMillis());
		
	
	currentComponent="tJava_4";

	
		int tos_count_tJava_4 = 0;
		
			if(enableLogStash) {
				talendJobLog.addCM("tJava_4", "tJava_4", "tJava");
				talendJobLogProcess(globalMap);
			}
			


System.out.println(" END OF JOB: " +context.W_JOB_PRINCIPALE);
 



/**
 * [tJava_4 begin ] stop
 */
	
	/**
	 * [tJava_4 main ] start
	 */

	

	
	
	currentComponent="tJava_4";

	

 


	tos_count_tJava_4++;

/**
 * [tJava_4 main ] stop
 */
	
	/**
	 * [tJava_4 process_data_begin ] start
	 */

	

	
	
	currentComponent="tJava_4";

	

 



/**
 * [tJava_4 process_data_begin ] stop
 */
	
	/**
	 * [tJava_4 process_data_end ] start
	 */

	

	
	
	currentComponent="tJava_4";

	

 



/**
 * [tJava_4 process_data_end ] stop
 */
	
	/**
	 * [tJava_4 end ] start
	 */

	

	
	
	currentComponent="tJava_4";

	

 

ok_Hash.put("tJava_4", true);
end_Hash.put("tJava_4", System.currentTimeMillis());




/**
 * [tJava_4 end ] stop
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
	 * [tJava_4 finally ] start
	 */

	

	
	
	currentComponent="tJava_4";

	

 



/**
 * [tJava_4 finally ] stop
 */
				}catch(java.lang.Exception e){	
					//ignore
				}catch(java.lang.Error error){
					//ignore
				}
				resourceMap = null;
			}
		

		globalMap.put("tJava_4_SUBPROCESS_STATE", 1);
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
    public String contextStr = "Default";
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
        final S_AML_PROFILATURA_ESTRAZIONE_MAIN_PARAMETRICA S_AML_PROFILATURA_ESTRAZIONE_MAIN_PARAMETRICAClass = new S_AML_PROFILATURA_ESTRAZIONE_MAIN_PARAMETRICA();

        int exitCode = S_AML_PROFILATURA_ESTRAZIONE_MAIN_PARAMETRICAClass.runJobInTOS(args);
	        if(exitCode==0){
		        log.info("TalendJob: 'S_AML_PROFILATURA_ESTRAZIONE_MAIN_PARAMETRICA' - Done.");
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
			log.info("TalendJob: 'S_AML_PROFILATURA_ESTRAZIONE_MAIN_PARAMETRICA' - Start.");
		
		
		
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
            java.io.InputStream inContext = S_AML_PROFILATURA_ESTRAZIONE_MAIN_PARAMETRICA.class.getClassLoader().getResourceAsStream("datahub_aml/s_aml_profilatura_estrazione_main_parametrica_0_1/contexts/" + contextStr + ".properties");
            if (inContext == null) {
                inContext = S_AML_PROFILATURA_ESTRAZIONE_MAIN_PARAMETRICA.class.getClassLoader().getResourceAsStream("config/contexts/" + contextStr + ".properties");
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
                        context.setContextType("W_JOB_PRINCIPALE", "id_String");
                        if(context.getStringValue("W_JOB_PRINCIPALE") == null) {
                            context.W_JOB_PRINCIPALE = null;
                        } else {
                            context.W_JOB_PRINCIPALE=(String) context.getProperty("W_JOB_PRINCIPALE");
                        }
                        context.setContextType("W_PROJECT_NAME", "id_String");
                        if(context.getStringValue("W_PROJECT_NAME") == null) {
                            context.W_PROJECT_NAME = null;
                        } else {
                            context.W_PROJECT_NAME=(String) context.getProperty("W_PROJECT_NAME");
                        }
                        context.setContextType("CONTEXT_SALE_FILE_NAME", "id_String");
                        if(context.getStringValue("CONTEXT_SALE_FILE_NAME") == null) {
                            context.CONTEXT_SALE_FILE_NAME = null;
                        } else {
                            context.CONTEXT_SALE_FILE_NAME=(String) context.getProperty("CONTEXT_SALE_FILE_NAME");
                        }
                        context.setContextType("W_INIZIO_DATE", "id_String");
                        if(context.getStringValue("W_INIZIO_DATE") == null) {
                            context.W_INIZIO_DATE = null;
                        } else {
                            context.W_INIZIO_DATE=(String) context.getProperty("W_INIZIO_DATE");
                        }
                        context.setContextType("W_FINE_DATE", "id_String");
                        if(context.getStringValue("W_FINE_DATE") == null) {
                            context.W_FINE_DATE = null;
                        } else {
                            context.W_FINE_DATE=(String) context.getProperty("W_FINE_DATE");
                        }
                        context.setContextType("data_riferimento", "id_String");
                        if(context.getStringValue("data_riferimento") == null) {
                            context.data_riferimento = null;
                        } else {
                            context.data_riferimento=(String) context.getProperty("data_riferimento");
                        }
                        context.setContextType("W_FILE_NAME_ESITI_CONTROLLO", "id_String");
                        if(context.getStringValue("W_FILE_NAME_ESITI_CONTROLLO") == null) {
                            context.W_FILE_NAME_ESITI_CONTROLLO = null;
                        } else {
                            context.W_FILE_NAME_ESITI_CONTROLLO=(String) context.getProperty("W_FILE_NAME_ESITI_CONTROLLO");
                        }
                        context.setContextType("W_DATA_RIFERIMENTO", "id_String");
                        if(context.getStringValue("W_DATA_RIFERIMENTO") == null) {
                            context.W_DATA_RIFERIMENTO = null;
                        } else {
                            context.W_DATA_RIFERIMENTO=(String) context.getProperty("W_DATA_RIFERIMENTO");
                        }
                        context.setContextType("W_FLAG_REG_AUI", "id_String");
                        if(context.getStringValue("W_FLAG_REG_AUI") == null) {
                            context.W_FLAG_REG_AUI = null;
                        } else {
                            context.W_FLAG_REG_AUI=(String) context.getProperty("W_FLAG_REG_AUI");
                        }
                        context.setContextType("W_CRAP", "id_String");
                        if(context.getStringValue("W_CRAP") == null) {
                            context.W_CRAP = null;
                        } else {
                            context.W_CRAP=(String) context.getProperty("W_CRAP");
                        }
                        context.setContextType("W_NDG", "id_String");
                        if(context.getStringValue("W_NDG") == null) {
                            context.W_NDG = null;
                        } else {
                            context.W_NDG=(String) context.getProperty("W_NDG");
                        }
                        context.setContextType("W_ESITI_PRECEDENTI", "id_String");
                        if(context.getStringValue("W_ESITI_PRECEDENTI") == null) {
                            context.W_ESITI_PRECEDENTI = null;
                        } else {
                            context.W_ESITI_PRECEDENTI=(String) context.getProperty("W_ESITI_PRECEDENTI");
                        }
                        context.setContextType("DB_CONNECTION_Database", "id_String");
                        if(context.getStringValue("DB_CONNECTION_Database") == null) {
                            context.DB_CONNECTION_Database = null;
                        } else {
                            context.DB_CONNECTION_Database=(String) context.getProperty("DB_CONNECTION_Database");
                        }
                        context.setContextType("DB_CONNECTION_Login", "id_String");
                        if(context.getStringValue("DB_CONNECTION_Login") == null) {
                            context.DB_CONNECTION_Login = null;
                        } else {
                            context.DB_CONNECTION_Login=(String) context.getProperty("DB_CONNECTION_Login");
                        }
                        context.setContextType("DB_CONNECTION_Password", "id_String");
                        if(context.getStringValue("DB_CONNECTION_Password") == null) {
                            context.DB_CONNECTION_Password = null;
                        } else {
                            context.DB_CONNECTION_Password=(String) context.getProperty("DB_CONNECTION_Password");
                        }
                        context.setContextType("DB_CONNECTION_Port", "id_String");
                        if(context.getStringValue("DB_CONNECTION_Port") == null) {
                            context.DB_CONNECTION_Port = null;
                        } else {
                            context.DB_CONNECTION_Port=(String) context.getProperty("DB_CONNECTION_Port");
                        }
                        context.setContextType("DB_CONNECTION_Server", "id_String");
                        if(context.getStringValue("DB_CONNECTION_Server") == null) {
                            context.DB_CONNECTION_Server = null;
                        } else {
                            context.DB_CONNECTION_Server=(String) context.getProperty("DB_CONNECTION_Server");
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
                        context.setContextType("DB_POSTGRES_POC_Database", "id_String");
                        if(context.getStringValue("DB_POSTGRES_POC_Database") == null) {
                            context.DB_POSTGRES_POC_Database = null;
                        } else {
                            context.DB_POSTGRES_POC_Database=(String) context.getProperty("DB_POSTGRES_POC_Database");
                        }
                        context.setContextType("DB_POSTGRES_POC_Login", "id_String");
                        if(context.getStringValue("DB_POSTGRES_POC_Login") == null) {
                            context.DB_POSTGRES_POC_Login = null;
                        } else {
                            context.DB_POSTGRES_POC_Login=(String) context.getProperty("DB_POSTGRES_POC_Login");
                        }
                        context.setContextType("DB_POSTGRES_POC_Password", "id_Password");
                        if(context.getStringValue("DB_POSTGRES_POC_Password") == null) {
                            context.DB_POSTGRES_POC_Password = null;
                        } else {
                            String pwd_DB_POSTGRES_POC_Password_value = context.getProperty("DB_POSTGRES_POC_Password");
                            context.DB_POSTGRES_POC_Password = null;
                            if(pwd_DB_POSTGRES_POC_Password_value!=null) {
                                if(context_param.containsKey("DB_POSTGRES_POC_Password")) {//no need to decrypt if it come from program argument or parent job runtime
                                    context.DB_POSTGRES_POC_Password = pwd_DB_POSTGRES_POC_Password_value;
                                } else if (!pwd_DB_POSTGRES_POC_Password_value.isEmpty()) {
                                    try {
                                        context.DB_POSTGRES_POC_Password = routines.system.PasswordEncryptUtil.decryptPassword(pwd_DB_POSTGRES_POC_Password_value);
                                        context.put("DB_POSTGRES_POC_Password",context.DB_POSTGRES_POC_Password);
                                    } catch (java.lang.RuntimeException e) {
                                        //do nothing
                                    }
                                }
                            }
                        }
                        context.setContextType("DB_POSTGRES_POC_Port", "id_String");
                        if(context.getStringValue("DB_POSTGRES_POC_Port") == null) {
                            context.DB_POSTGRES_POC_Port = null;
                        } else {
                            context.DB_POSTGRES_POC_Port=(String) context.getProperty("DB_POSTGRES_POC_Port");
                        }
                        context.setContextType("DB_POSTGRES_POC_Schema", "id_String");
                        if(context.getStringValue("DB_POSTGRES_POC_Schema") == null) {
                            context.DB_POSTGRES_POC_Schema = null;
                        } else {
                            context.DB_POSTGRES_POC_Schema=(String) context.getProperty("DB_POSTGRES_POC_Schema");
                        }
                        context.setContextType("DB_POSTGRES_POC_Server", "id_String");
                        if(context.getStringValue("DB_POSTGRES_POC_Server") == null) {
                            context.DB_POSTGRES_POC_Server = null;
                        } else {
                            context.DB_POSTGRES_POC_Server=(String) context.getProperty("DB_POSTGRES_POC_Server");
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
                        context.setContextType("W_CONTEXT_TYPE", "id_String");
                        if(context.getStringValue("W_CONTEXT_TYPE") == null) {
                            context.W_CONTEXT_TYPE = null;
                        } else {
                            context.W_CONTEXT_TYPE=(String) context.getProperty("W_CONTEXT_TYPE");
                        }
                        context.setContextType("W_DAVINCI_TABELLA_TARGET", "id_String");
                        if(context.getStringValue("W_DAVINCI_TABELLA_TARGET") == null) {
                            context.W_DAVINCI_TABELLA_TARGET = null;
                        } else {
                            context.W_DAVINCI_TABELLA_TARGET=(String) context.getProperty("W_DAVINCI_TABELLA_TARGET");
                        }
                        context.setContextType("W_DB_ANDC_DH", "id_String");
                        if(context.getStringValue("W_DB_ANDC_DH") == null) {
                            context.W_DB_ANDC_DH = null;
                        } else {
                            context.W_DB_ANDC_DH=(String) context.getProperty("W_DB_ANDC_DH");
                        }
                        context.setContextType("W_DB_ANDC_ETL", "id_String");
                        if(context.getStringValue("W_DB_ANDC_ETL") == null) {
                            context.W_DB_ANDC_ETL = null;
                        } else {
                            context.W_DB_ANDC_ETL=(String) context.getProperty("W_DB_ANDC_ETL");
                        }
                        context.setContextType("W_DB_DAVINCI_POSTGRES_CUSTOM_LOG_TABLE", "id_String");
                        if(context.getStringValue("W_DB_DAVINCI_POSTGRES_CUSTOM_LOG_TABLE") == null) {
                            context.W_DB_DAVINCI_POSTGRES_CUSTOM_LOG_TABLE = null;
                        } else {
                            context.W_DB_DAVINCI_POSTGRES_CUSTOM_LOG_TABLE=(String) context.getProperty("W_DB_DAVINCI_POSTGRES_CUSTOM_LOG_TABLE");
                        }
                        context.setContextType("W_DB_DAVINCI_POSTGRES_FILE_METADATA_TABLE", "id_String");
                        if(context.getStringValue("W_DB_DAVINCI_POSTGRES_FILE_METADATA_TABLE") == null) {
                            context.W_DB_DAVINCI_POSTGRES_FILE_METADATA_TABLE = null;
                        } else {
                            context.W_DB_DAVINCI_POSTGRES_FILE_METADATA_TABLE=(String) context.getProperty("W_DB_DAVINCI_POSTGRES_FILE_METADATA_TABLE");
                        }
                        context.setContextType("W_DB_DAVINCI_POSTGRES_STATEMENT_TABLE", "id_String");
                        if(context.getStringValue("W_DB_DAVINCI_POSTGRES_STATEMENT_TABLE") == null) {
                            context.W_DB_DAVINCI_POSTGRES_STATEMENT_TABLE = null;
                        } else {
                            context.W_DB_DAVINCI_POSTGRES_STATEMENT_TABLE=(String) context.getProperty("W_DB_DAVINCI_POSTGRES_STATEMENT_TABLE");
                        }
                        context.setContextType("W_FOLDER_CSV", "id_String");
                        if(context.getStringValue("W_FOLDER_CSV") == null) {
                            context.W_FOLDER_CSV = null;
                        } else {
                            context.W_FOLDER_CSV=(String) context.getProperty("W_FOLDER_CSV");
                        }
                        context.setContextType("W_FOLDER_CSV_EXPORT", "id_String");
                        if(context.getStringValue("W_FOLDER_CSV_EXPORT") == null) {
                            context.W_FOLDER_CSV_EXPORT = null;
                        } else {
                            context.W_FOLDER_CSV_EXPORT=(String) context.getProperty("W_FOLDER_CSV_EXPORT");
                        }
                        context.setContextType("W_FOLDER_IN_AUTOMAZIONI_CONTROLLI_II_LIV", "id_String");
                        if(context.getStringValue("W_FOLDER_IN_AUTOMAZIONI_CONTROLLI_II_LIV") == null) {
                            context.W_FOLDER_IN_AUTOMAZIONI_CONTROLLI_II_LIV = null;
                        } else {
                            context.W_FOLDER_IN_AUTOMAZIONI_CONTROLLI_II_LIV=(String) context.getProperty("W_FOLDER_IN_AUTOMAZIONI_CONTROLLI_II_LIV");
                        }
                        context.setContextType("W_FOLDER_IN_AUTOMAZIONI_CONTROLLI_II_LIV_PROFILATURA", "id_String");
                        if(context.getStringValue("W_FOLDER_IN_AUTOMAZIONI_CONTROLLI_II_LIV_PROFILATURA") == null) {
                            context.W_FOLDER_IN_AUTOMAZIONI_CONTROLLI_II_LIV_PROFILATURA = null;
                        } else {
                            context.W_FOLDER_IN_AUTOMAZIONI_CONTROLLI_II_LIV_PROFILATURA=(String) context.getProperty("W_FOLDER_IN_AUTOMAZIONI_CONTROLLI_II_LIV_PROFILATURA");
                        }
                        context.setContextType("W_HDFS_DAVINCI_ETL", "id_String");
                        if(context.getStringValue("W_HDFS_DAVINCI_ETL") == null) {
                            context.W_HDFS_DAVINCI_ETL = null;
                        } else {
                            context.W_HDFS_DAVINCI_ETL=(String) context.getProperty("W_HDFS_DAVINCI_ETL");
                        }
                        context.setContextType("W_HDFS_DH", "id_String");
                        if(context.getStringValue("W_HDFS_DH") == null) {
                            context.W_HDFS_DH = null;
                        } else {
                            context.W_HDFS_DH=(String) context.getProperty("W_HDFS_DH");
                        }
                        context.setContextType("W_HDFS_ETL", "id_String");
                        if(context.getStringValue("W_HDFS_ETL") == null) {
                            context.W_HDFS_ETL = null;
                        } else {
                            context.W_HDFS_ETL=(String) context.getProperty("W_HDFS_ETL");
                        }
                        context.setContextType("W_HIVE_DAVINCI_SANDBOX_DB", "id_String");
                        if(context.getStringValue("W_HIVE_DAVINCI_SANDBOX_DB") == null) {
                            context.W_HIVE_DAVINCI_SANDBOX_DB = null;
                        } else {
                            context.W_HIVE_DAVINCI_SANDBOX_DB=(String) context.getProperty("W_HIVE_DAVINCI_SANDBOX_DB");
                        }
                        context.setContextType("W_HIVE_DAVINCI_SANDBOX_TABLE", "id_String");
                        if(context.getStringValue("W_HIVE_DAVINCI_SANDBOX_TABLE") == null) {
                            context.W_HIVE_DAVINCI_SANDBOX_TABLE = null;
                        } else {
                            context.W_HIVE_DAVINCI_SANDBOX_TABLE=(String) context.getProperty("W_HIVE_DAVINCI_SANDBOX_TABLE");
                        }
                        }

                private void processContext_1() {
                        context.setContextType("W_NAME_TFILE_LIST", "id_String");
                        if(context.getStringValue("W_NAME_TFILE_LIST") == null) {
                            context.W_NAME_TFILE_LIST = null;
                        } else {
                            context.W_NAME_TFILE_LIST=(String) context.getProperty("W_NAME_TFILE_LIST");
                        }
                        context.setContextType("W_MAIN_PARAMETRICA", "id_String");
                        if(context.getStringValue("W_MAIN_PARAMETRICA") == null) {
                            context.W_MAIN_PARAMETRICA = null;
                        } else {
                            context.W_MAIN_PARAMETRICA=(String) context.getProperty("W_MAIN_PARAMETRICA");
                        }
                        context.setContextType("W_ESITI_PARAMETRICA", "id_String");
                        if(context.getStringValue("W_ESITI_PARAMETRICA") == null) {
                            context.W_ESITI_PARAMETRICA = null;
                        } else {
                            context.W_ESITI_PARAMETRICA=(String) context.getProperty("W_ESITI_PARAMETRICA");
                        }
                } 
                public void processAllContext() {
                        processContext_0();
                        processContext_1();
                }
            }

            new ContextProcessing().processAllContext();
        } catch (java.io.IOException ie) {
            System.err.println("Could not load context "+contextStr);
            ie.printStackTrace();
        }

        // get context value from parent directly
        if (parentContextMap != null && !parentContextMap.isEmpty()) {if (parentContextMap.containsKey("W_JOB_PRINCIPALE")) {
                context.W_JOB_PRINCIPALE = (String) parentContextMap.get("W_JOB_PRINCIPALE");
            }if (parentContextMap.containsKey("W_PROJECT_NAME")) {
                context.W_PROJECT_NAME = (String) parentContextMap.get("W_PROJECT_NAME");
            }if (parentContextMap.containsKey("CONTEXT_SALE_FILE_NAME")) {
                context.CONTEXT_SALE_FILE_NAME = (String) parentContextMap.get("CONTEXT_SALE_FILE_NAME");
            }if (parentContextMap.containsKey("W_INIZIO_DATE")) {
                context.W_INIZIO_DATE = (String) parentContextMap.get("W_INIZIO_DATE");
            }if (parentContextMap.containsKey("W_FINE_DATE")) {
                context.W_FINE_DATE = (String) parentContextMap.get("W_FINE_DATE");
            }if (parentContextMap.containsKey("data_riferimento")) {
                context.data_riferimento = (String) parentContextMap.get("data_riferimento");
            }if (parentContextMap.containsKey("W_FILE_NAME_ESITI_CONTROLLO")) {
                context.W_FILE_NAME_ESITI_CONTROLLO = (String) parentContextMap.get("W_FILE_NAME_ESITI_CONTROLLO");
            }if (parentContextMap.containsKey("W_DATA_RIFERIMENTO")) {
                context.W_DATA_RIFERIMENTO = (String) parentContextMap.get("W_DATA_RIFERIMENTO");
            }if (parentContextMap.containsKey("W_FLAG_REG_AUI")) {
                context.W_FLAG_REG_AUI = (String) parentContextMap.get("W_FLAG_REG_AUI");
            }if (parentContextMap.containsKey("W_CRAP")) {
                context.W_CRAP = (String) parentContextMap.get("W_CRAP");
            }if (parentContextMap.containsKey("W_NDG")) {
                context.W_NDG = (String) parentContextMap.get("W_NDG");
            }if (parentContextMap.containsKey("W_ESITI_PRECEDENTI")) {
                context.W_ESITI_PRECEDENTI = (String) parentContextMap.get("W_ESITI_PRECEDENTI");
            }if (parentContextMap.containsKey("DB_CONNECTION_Database")) {
                context.DB_CONNECTION_Database = (String) parentContextMap.get("DB_CONNECTION_Database");
            }if (parentContextMap.containsKey("DB_CONNECTION_Login")) {
                context.DB_CONNECTION_Login = (String) parentContextMap.get("DB_CONNECTION_Login");
            }if (parentContextMap.containsKey("DB_CONNECTION_Password")) {
                context.DB_CONNECTION_Password = (String) parentContextMap.get("DB_CONNECTION_Password");
            }if (parentContextMap.containsKey("DB_CONNECTION_Port")) {
                context.DB_CONNECTION_Port = (String) parentContextMap.get("DB_CONNECTION_Port");
            }if (parentContextMap.containsKey("DB_CONNECTION_Server")) {
                context.DB_CONNECTION_Server = (String) parentContextMap.get("DB_CONNECTION_Server");
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
            }if (parentContextMap.containsKey("DB_POSTGRES_POC_Database")) {
                context.DB_POSTGRES_POC_Database = (String) parentContextMap.get("DB_POSTGRES_POC_Database");
            }if (parentContextMap.containsKey("DB_POSTGRES_POC_Login")) {
                context.DB_POSTGRES_POC_Login = (String) parentContextMap.get("DB_POSTGRES_POC_Login");
            }if (parentContextMap.containsKey("DB_POSTGRES_POC_Password")) {
                context.DB_POSTGRES_POC_Password = (java.lang.String) parentContextMap.get("DB_POSTGRES_POC_Password");
            }if (parentContextMap.containsKey("DB_POSTGRES_POC_Port")) {
                context.DB_POSTGRES_POC_Port = (String) parentContextMap.get("DB_POSTGRES_POC_Port");
            }if (parentContextMap.containsKey("DB_POSTGRES_POC_Schema")) {
                context.DB_POSTGRES_POC_Schema = (String) parentContextMap.get("DB_POSTGRES_POC_Schema");
            }if (parentContextMap.containsKey("DB_POSTGRES_POC_Server")) {
                context.DB_POSTGRES_POC_Server = (String) parentContextMap.get("DB_POSTGRES_POC_Server");
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
            }if (parentContextMap.containsKey("W_CONTEXT_TYPE")) {
                context.W_CONTEXT_TYPE = (String) parentContextMap.get("W_CONTEXT_TYPE");
            }if (parentContextMap.containsKey("W_DAVINCI_TABELLA_TARGET")) {
                context.W_DAVINCI_TABELLA_TARGET = (String) parentContextMap.get("W_DAVINCI_TABELLA_TARGET");
            }if (parentContextMap.containsKey("W_DB_ANDC_DH")) {
                context.W_DB_ANDC_DH = (String) parentContextMap.get("W_DB_ANDC_DH");
            }if (parentContextMap.containsKey("W_DB_ANDC_ETL")) {
                context.W_DB_ANDC_ETL = (String) parentContextMap.get("W_DB_ANDC_ETL");
            }if (parentContextMap.containsKey("W_DB_DAVINCI_POSTGRES_CUSTOM_LOG_TABLE")) {
                context.W_DB_DAVINCI_POSTGRES_CUSTOM_LOG_TABLE = (String) parentContextMap.get("W_DB_DAVINCI_POSTGRES_CUSTOM_LOG_TABLE");
            }if (parentContextMap.containsKey("W_DB_DAVINCI_POSTGRES_FILE_METADATA_TABLE")) {
                context.W_DB_DAVINCI_POSTGRES_FILE_METADATA_TABLE = (String) parentContextMap.get("W_DB_DAVINCI_POSTGRES_FILE_METADATA_TABLE");
            }if (parentContextMap.containsKey("W_DB_DAVINCI_POSTGRES_STATEMENT_TABLE")) {
                context.W_DB_DAVINCI_POSTGRES_STATEMENT_TABLE = (String) parentContextMap.get("W_DB_DAVINCI_POSTGRES_STATEMENT_TABLE");
            }if (parentContextMap.containsKey("W_FOLDER_CSV")) {
                context.W_FOLDER_CSV = (String) parentContextMap.get("W_FOLDER_CSV");
            }if (parentContextMap.containsKey("W_FOLDER_CSV_EXPORT")) {
                context.W_FOLDER_CSV_EXPORT = (String) parentContextMap.get("W_FOLDER_CSV_EXPORT");
            }if (parentContextMap.containsKey("W_FOLDER_IN_AUTOMAZIONI_CONTROLLI_II_LIV")) {
                context.W_FOLDER_IN_AUTOMAZIONI_CONTROLLI_II_LIV = (String) parentContextMap.get("W_FOLDER_IN_AUTOMAZIONI_CONTROLLI_II_LIV");
            }if (parentContextMap.containsKey("W_FOLDER_IN_AUTOMAZIONI_CONTROLLI_II_LIV_PROFILATURA")) {
                context.W_FOLDER_IN_AUTOMAZIONI_CONTROLLI_II_LIV_PROFILATURA = (String) parentContextMap.get("W_FOLDER_IN_AUTOMAZIONI_CONTROLLI_II_LIV_PROFILATURA");
            }if (parentContextMap.containsKey("W_HDFS_DAVINCI_ETL")) {
                context.W_HDFS_DAVINCI_ETL = (String) parentContextMap.get("W_HDFS_DAVINCI_ETL");
            }if (parentContextMap.containsKey("W_HDFS_DH")) {
                context.W_HDFS_DH = (String) parentContextMap.get("W_HDFS_DH");
            }if (parentContextMap.containsKey("W_HDFS_ETL")) {
                context.W_HDFS_ETL = (String) parentContextMap.get("W_HDFS_ETL");
            }if (parentContextMap.containsKey("W_HIVE_DAVINCI_SANDBOX_DB")) {
                context.W_HIVE_DAVINCI_SANDBOX_DB = (String) parentContextMap.get("W_HIVE_DAVINCI_SANDBOX_DB");
            }if (parentContextMap.containsKey("W_HIVE_DAVINCI_SANDBOX_TABLE")) {
                context.W_HIVE_DAVINCI_SANDBOX_TABLE = (String) parentContextMap.get("W_HIVE_DAVINCI_SANDBOX_TABLE");
            }if (parentContextMap.containsKey("W_NAME_TFILE_LIST")) {
                context.W_NAME_TFILE_LIST = (String) parentContextMap.get("W_NAME_TFILE_LIST");
            }if (parentContextMap.containsKey("W_MAIN_PARAMETRICA")) {
                context.W_MAIN_PARAMETRICA = (String) parentContextMap.get("W_MAIN_PARAMETRICA");
            }if (parentContextMap.containsKey("W_ESITI_PARAMETRICA")) {
                context.W_ESITI_PARAMETRICA = (String) parentContextMap.get("W_ESITI_PARAMETRICA");
            }
        }

        //Resume: init the resumeUtil
        resumeEntryMethodName = ResumeUtil.getResumeEntryMethodName(resuming_checkpoint_path);
        resumeUtil = new ResumeUtil(resuming_logs_dir_path, isChildJob, rootPid);
        resumeUtil.initCommonInfo(pid, rootPid, fatherPid, projectName, jobName, contextStr, jobVersion);

		List<String> parametersToEncrypt = new java.util.ArrayList<String>();
			parametersToEncrypt.add("DB_POSTGRES_Password");
			parametersToEncrypt.add("DB_POSTGRES_POC_Password");
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
errorCode = null;tJava_2Process(globalMap);
if(!"failure".equals(status)) { status = "end"; }
}catch (TalendException e_tJava_2) {
globalMap.put("tJava_2_SUBPROCESS_STATE", -1);

e_tJava_2.printStackTrace();

}

this.globalResumeTicket = true;//to run tPostJob




        end = System.currentTimeMillis();

        if (watch) {
            System.out.println((end-startTime)+" milliseconds");
        }

        endUsedMemory = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        if (false) {
            System.out.println((endUsedMemory - startUsedMemory) + " bytes memory increase when running : S_AML_PROFILATURA_ESTRAZIONE_MAIN_PARAMETRICA");
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
 *     284144 characters generated by Talend Big Data Platform 
 *     on the 6 febbraio 2026 14.56.07 CET
 ************************************************************************************************/