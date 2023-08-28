package org.apache.seatunnel.connectors.seatunnel.bigquery.source;

import com.google.auto.service.AutoService;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;

import static org.apache.seatunnel.connectors.seatunnel.bigquery.config.SourceConfig.*;

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
        return OptionRule.builder().required(
                        CONFIG_CENTER_ENVIRONMENT,
                        CONFIG_CENTER_URL,
                        CONFIG_CENTER_TOKEN,
                        CONFIG_CENTER_PROJECT,
                        PROJECT,
                        DATASET,
                        TABLE,
                        TEMP_BUCKET)
                .optional(
                        FILTER,
                        PARTITION_FROM,
                        PARTITION_TO,
                        SQL)
                .build();
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return BigQuerySource.class;
    }
}
