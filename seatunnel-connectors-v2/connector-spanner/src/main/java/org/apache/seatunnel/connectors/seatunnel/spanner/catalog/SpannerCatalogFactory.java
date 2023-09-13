package org.apache.seatunnel.connectors.seatunnel.spanner.catalog;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.factory.CatalogFactory;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.connectors.seatunnel.spanner.constants.SpannerConstants;

import com.google.auto.service.AutoService;

import static org.apache.seatunnel.connectors.seatunnel.common.config.ConfigCenterConfig.CONFIG_CENTER_ENVIRONMENT;
import static org.apache.seatunnel.connectors.seatunnel.common.config.ConfigCenterConfig.CONFIG_CENTER_PROJECT;
import static org.apache.seatunnel.connectors.seatunnel.common.config.ConfigCenterConfig.CONFIG_CENTER_TOKEN;
import static org.apache.seatunnel.connectors.seatunnel.common.config.ConfigCenterConfig.CONFIG_CENTER_URL;
import static org.apache.seatunnel.connectors.seatunnel.spanner.config.SpannerConfig.DATABASE_ID;
import static org.apache.seatunnel.connectors.seatunnel.spanner.config.SpannerConfig.INSTANCE_ID;
import static org.apache.seatunnel.connectors.seatunnel.spanner.config.SpannerConfig.PROJECT_ID;

/**
 * @author: gf.xu
 * @email: gf.xu@aftership.com
 * @date: 2023/9/7 10:56
 */
@AutoService(Factory.class)
public class SpannerCatalogFactory implements CatalogFactory {

    @Override
    public Catalog createCatalog(String catalogName, ReadonlyConfig options) {
        // todo create a SpannerCatalog
        return null;
    }

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
                        CONFIG_CENTER_PROJECT,
                        CONFIG_CENTER_TOKEN,
                        CONFIG_CENTER_URL,
                        CONFIG_CENTER_ENVIRONMENT)
                .build();
    }
}
