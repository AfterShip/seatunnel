package org.apache.seatunnel.connectors.seatunnel.bigquery.source;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;

import com.google.auto.service.AutoService;

import static org.apache.seatunnel.connectors.seatunnel.bigquery.config.SourceConfig.DATASET;
import static org.apache.seatunnel.connectors.seatunnel.bigquery.config.SourceConfig.FILTER;
import static org.apache.seatunnel.connectors.seatunnel.bigquery.config.SourceConfig.PARTITION_FROM;
import static org.apache.seatunnel.connectors.seatunnel.bigquery.config.SourceConfig.PARTITION_TO;
import static org.apache.seatunnel.connectors.seatunnel.bigquery.config.SourceConfig.PROJECT;
import static org.apache.seatunnel.connectors.seatunnel.bigquery.config.SourceConfig.SQL;
import static org.apache.seatunnel.connectors.seatunnel.bigquery.config.SourceConfig.TABLE;
import static org.apache.seatunnel.connectors.seatunnel.bigquery.config.SourceConfig.TEMP_BUCKET;
import static org.apache.seatunnel.connectors.seatunnel.common.config.ConfigCenterConfig.CONFIG_CENTER_ENVIRONMENT;
import static org.apache.seatunnel.connectors.seatunnel.common.config.ConfigCenterConfig.CONFIG_CENTER_PROJECT;
import static org.apache.seatunnel.connectors.seatunnel.common.config.ConfigCenterConfig.CONFIG_CENTER_TOKEN;
import static org.apache.seatunnel.connectors.seatunnel.common.config.ConfigCenterConfig.CONFIG_CENTER_URL;

/**
 * @author ah.he@aftership.com
 * @version 1.0
 * @date 2023/8/2 13:57
 */
@AutoService(Factory.class)
public class BigQuerySourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return "BigQuery";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        CONFIG_CENTER_ENVIRONMENT,
                        CONFIG_CENTER_URL,
                        CONFIG_CENTER_TOKEN,
                        CONFIG_CENTER_PROJECT,
                        PROJECT,
                        DATASET,
                        TABLE,
                        TEMP_BUCKET)
                .optional(FILTER, PARTITION_FROM, PARTITION_TO, SQL)
                .build();
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return BigQuerySource.class;
    }
}
