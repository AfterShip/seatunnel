package org.apache.seatunnel.connectors.seatunnel.spanner.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.connectors.seatunnel.common.config.ConfigCenterConfig;

/**
 * @author: gf.xu
 * @email: gf.xu@aftership.com
 * @date: 2023/8/25 13:52
 */
public class SpannerConfig extends ConfigCenterConfig {

    public static final Option<String> PROJECT_ID =
            Options.key("project_id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The Google Cloud Platform project ID that contains the source table.");

    public static final Option<String> INSTANCE_ID =
            Options.key("instance_id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "BigTable instance id. "
                                    + "Uniquely identifies BigTable instance within your Google Cloud Platform project.");

    public static final Option<String> DATABASE_ID =
            Options.key("database_id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The database to read from. A database contains a set of tables, schemas, stored procedures, and so on. "
                                    + "Each database is defined by a schema that describes the tables, indexes, and other information.");

    public static final Option<String> TABLE_ID =
            Options.key("table_id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The table to read from. A table contains individual records organized in rows. "
                                    + "Each record is composed of columns (also called fields). "
                                    + "Every table is defined by a schema that describes the column names, data types, and other information.");

    public static final Option<Integer> BATCH_SIZE =
            Options.key("batch_size")
                    .intType()
                    .defaultValue(100)
                    .withDescription(
                            "Size of the batched writes to the Spanner table. "
                                    + "When the number of buffered mutations is greater than this batchSize,"
                                    + "the mutations are written to Spanner table, Default value is 100");

    public static final Option<Long> MAX_PARTITIONS =
            Options.key("max_partitions")
                    .longType()
                    .noDefaultValue()
                    .withDescription(
                            "Maximum number of partitions. This is only a hint. The actual number of partitions may vary");

    public static final Option<Long> PARTITION_SIZE_MB =
            Options.key("partition_size_mb")
                    .longType()
                    .noDefaultValue()
                    .withDescription(
                            "Partition size in megabytes. This is only a hint. The actual partition size may vary");

    public static final Option<String> IMPORT_QUERY =
            Options.key("import_query")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The SELECT query to use to import data from the specified table.");
}
