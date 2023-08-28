package org.apache.seatunnel.connectors.seatunnel.clickhouse.config;

/**
 * @author ah.he@aftership.com
 * @version 1.0
 * @date 2023/6/13 19:49
 */
public final class ClickHouseConstants {

    public static final String AUTH = "db-auth.ch.%s";
    public static final String DB_SSL_ROOT_CRT = "db-ssl.ch.%s-ca.crt";
    public static final String NAME_REFERENCE = "referenceName";
    public static final String NAME_BATCH_SIZE = "batchSize";
    public static final String ENVIRONMENT = "environment";
    public static final String CONFIG_CENTER_TOKEN = "config_center_token";
    public static final String CONFIG_CENTER_PROJECT = "config_center_project";
    public static final String CONFIG_CENTER_URL = "config_center_url";
    public static final String NAME_CA_PEM_VALUE = "caPemValue";
    public static final String NAME_CA_PEM_PATH = "caPemPath";
    public static final String NAME_HOST = "host";
    public static final String NAME_PORT = "port";
    public static final String NAME_USER = "user";
    public static final String NAME_PASSWORD = "password";

    public static final String NAME_DATABASE = "database";
    public static final String NAME_LOCAL_TABLE = "localTable";
    public static final String NAME_DISTRIBUTE_TABLE = "distributedTable";
    public static final String NAME_DDL_PROXY = "ddlProxy";
    public static final String NAME_SCHEMA = "schema";
    public static final String NAME_WRITE_MODE = "writeMode";
    public static final String NAME_JOB_MODE = "jobMode";
    public static final String NAME_IMPORT_MODE = "importMode";
    public static final String NAME_WRITE_SUSTAINING = "writeSustaining";

    // temporary
    public static final String NAME_TEMPORARY = "temporary";
    public static final String NAME_TEMPORARY_LOCAL_TABLE = "temporaryLocalTable";
    public static final String NAME_TEMPORARY_DISTRIBUTED_TABLE = "temporaryDistributedTable";
    public static final String NAME_PARTITION_FIELD = "partitionField";
    public static final String NAME_PARTITION_VALUE = "partitionValue";

    public static final String OVERWRITE = "overwrite";
    public static final String PARTITION = "partition";

    public static final int DEFAULT_CLICKHOUSE_BATCH_SIZE = 1000;
    public static final int CLICKHOUSE_BATCH_MAX = 10000000;
    public static final int CLICKHOUSE_SOCKET_TIMEOUT = 3600 * 1000;
    public static final int CLICKHOUSE_CONNECTION_TIMEOUT = 10 * 1000;

    /**
     * 删除数据分区，硬删除
     */
    public static final String ALTER_DROP_PARTITION = "ALTER TABLE %s.%s  DROP PARTITION '%s' ;";

    /**
     * 清空表数据
     * TRUNCATE TABLE [IF EXISTS] [db.]name [ON CLUSTER cluster]
     */
    public static final String TRUNCATE_TABLE = "TRUNCATE TABLE %s.%s on cluster default_cluster;";
}
