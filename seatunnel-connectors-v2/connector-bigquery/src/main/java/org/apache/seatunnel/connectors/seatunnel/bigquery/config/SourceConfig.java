package org.apache.seatunnel.connectors.seatunnel.bigquery.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.connectors.seatunnel.common.config.ConfigCenterConfig;

/**
 * @author ah.he@aftership.com
 * @version 1.0
 * @date 2023/8/1 19:42
 */
public class SourceConfig extends ConfigCenterConfig {

    public static final Option<String> PROJECT =
            Options.key("project")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("BigQuery dataset project.");

    public static final Option<String> DATASET =
            Options.key("dataset")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("BigQuery dataset.");

    public static final Option<String> TABLE =
            Options.key("table")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("BigQuery table.");

    public static final Option<String> FILTER =
            Options.key("filter")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Filters out rows that do not match the given condition. " +
                            "For example, if the filter is ‘age > 3 and name is null’.");

    public static final Option<String> PARTITION_FROM =
            Options.key("partition_from")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Inclusive partition start date, For example, ‘2019-01-01’.");

    public static final Option<String> PARTITION_TO =
            Options.key("partition_to")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Exclusive partition end date, For example, ‘2019-01-01’.");

    public static final Option<String> SQL =
            Options.key("sql")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("BigQuery sql used to query data.");

    public static final Option<String> TEMP_BUCKET =
            Options.key("temp_bucket")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The Google Cloud Storage bucket to store temporary data in.");
}
