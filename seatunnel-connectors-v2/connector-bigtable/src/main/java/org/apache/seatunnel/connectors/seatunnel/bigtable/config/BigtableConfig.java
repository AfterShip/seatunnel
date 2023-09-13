package org.apache.seatunnel.connectors.seatunnel.bigtable.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.connectors.seatunnel.common.config.ConfigCenterConfig;

/**
 * @author: gf.xu
 * @email: gf.xu@aftership.com
 * @date: 2023/8/17 14:26
 */
public class BigtableConfig extends ConfigCenterConfig {

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

    public static final Option<String> TABLE_ID =
            Options.key("table_id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The table to read from. A table contains individual records organized in rows. "
                                    + "Each record is composed of columns (also called fields). "
                                    + "Every table is defined by a schema that describes the column names, data types, and other information.");

    public static final Option<String> KEY_ALIAS =
            Options.key("key_alias")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Name of the field for row key.");

    public static final Option<String> COLUMN_MAPPINGS =
            Options.key("column_mappings")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Mappings from record field to Bigtable column name. "
                                    + "Column names must be formatted as <family>:<qualifier>.");

    public static final Option<EnCoding> ENCODING =
            Options.key("encoding")
                    .enumType(EnCoding.class)
                    .defaultValue(EnCoding.UTF8)
                    .withDescription("Bigtable record encoding");

    public static final Option<String> BIGTABLE_OPTIONS =
            Options.key("bigtable_options")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Additional connection properties for Bigtable");

    public static final Option<Integer> SINK_BUFFER_FLUSH_MAX_SIZE_IN_BYTES =
            Options.key("sink_buffer_flush_max_size_in_bytes")
                    .intType()
                    .defaultValue(2 * 1024 * 1024)
                    .withDescription(
                            "Writing option, maximum size in memory of buffered rows for each writing request. "
                                    + "This can improve performance for writing data to Bigtable database, "
                                    + "but may increase the latency.");

    public static final Option<Integer> SINK_BUFFER_FLUSH_MAX_MUTATIONS =
            Options.key("sink_buffer_flush_max_mutations")
                    .intType()
                    .defaultValue(1000)
                    .withDescription(
                            "Writing option, maximum number of rows to buffer for each writing request. "
                                    + "This can improve performance for writing data to Bigtable database, "
                                    + "but may increase the latency. ");

    public enum EnCoding {
        UTF8,
        GBK;
    }
}
