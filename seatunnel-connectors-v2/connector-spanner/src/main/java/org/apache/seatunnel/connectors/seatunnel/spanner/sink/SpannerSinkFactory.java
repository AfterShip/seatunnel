package org.apache.seatunnel.connectors.seatunnel.spanner.sink;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;

import com.google.auto.service.AutoService;

import static org.apache.seatunnel.connectors.seatunnel.spanner.config.SpannerConfig.BATCH_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.spanner.config.SpannerConfig.CONFIG_CENTER_ENVIRONMENT;
import static org.apache.seatunnel.connectors.seatunnel.spanner.config.SpannerConfig.CONFIG_CENTER_PROJECT;
import static org.apache.seatunnel.connectors.seatunnel.spanner.config.SpannerConfig.CONFIG_CENTER_TOKEN;
import static org.apache.seatunnel.connectors.seatunnel.spanner.config.SpannerConfig.CONFIG_CENTER_URL;
import static org.apache.seatunnel.connectors.seatunnel.spanner.config.SpannerConfig.DATABASE_ID;
import static org.apache.seatunnel.connectors.seatunnel.spanner.config.SpannerConfig.INSTANCE_ID;
import static org.apache.seatunnel.connectors.seatunnel.spanner.config.SpannerConfig.PROJECT_ID;
import static org.apache.seatunnel.connectors.seatunnel.spanner.config.SpannerConfig.TABLE_ID;
import static org.apache.seatunnel.connectors.seatunnel.spanner.constants.SpannerConstants.IDENTIFIER;

/**
 * @author: gf.xu
 * @email: gf.xu@aftership.com
 * @date: 2023/8/25 14:29
 */
@AutoService(Factory.class)
public class SpannerSinkFactory implements TableSinkFactory {

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        PROJECT_ID,
                        INSTANCE_ID,
                        DATABASE_ID,
                        TABLE_ID,
                        CONFIG_CENTER_PROJECT,
                        CONFIG_CENTER_TOKEN,
                        CONFIG_CENTER_URL,
                        CONFIG_CENTER_ENVIRONMENT)
                .optional(BATCH_SIZE)
                .build();
    }
}
