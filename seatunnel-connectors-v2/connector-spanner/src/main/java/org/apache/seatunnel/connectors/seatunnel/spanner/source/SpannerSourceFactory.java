package org.apache.seatunnel.connectors.seatunnel.spanner.source;

import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.connectors.seatunnel.spanner.constants.SpannerConstants;

import static org.apache.seatunnel.connectors.seatunnel.common.config.ConfigCenterConfig.CONFIG_CENTER_ENVIRONMENT;
import static org.apache.seatunnel.connectors.seatunnel.common.config.ConfigCenterConfig.CONFIG_CENTER_PROJECT;
import static org.apache.seatunnel.connectors.seatunnel.common.config.ConfigCenterConfig.CONFIG_CENTER_TOKEN;
import static org.apache.seatunnel.connectors.seatunnel.common.config.ConfigCenterConfig.CONFIG_CENTER_URL;
import static org.apache.seatunnel.connectors.seatunnel.spanner.config.SpannerConfig.*;

/**
 * @author: gf.xu
 * @email: gf.xu@aftership.com
 * @date: 2023/9/6 09:57
 */
@Slf4j
@AutoService(Factory.class)
public class SpannerSourceFactory implements TableSourceFactory {

    @Override
    public String factoryIdentifier() {
        return SpannerConstants.IDENTIFIER;
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
                .optional(
                        IMPORT_QUERY,
                        PARTITION_SIZE_MB,
                        MAX_PARTITIONS)
                .build();
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return SpannerSource.class;
    }
}
