package org.apache.seatunnel.connectors.seatunnel.common.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

/**
 * @author ah.he@aftership.com
 * @version 1.0
 * @date 2023/8/21 16:15
 */
public abstract class ConfigCenterConfig {
    public static final Option<String> CONFIG_CENTER_ENVIRONMENT =
            Options.key("config_center_environment")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("eg: production/testing");
    public static final Option<String> CONFIG_CENTER_URL =
            Options.key("config_center_url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Config center url.");
    public static final Option<String> CONFIG_CENTER_TOKEN =
            Options.key("config_center_token")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Config center token.");
    public static final Option<String> CONFIG_CENTER_PROJECT =
            Options.key("config_center_project")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Config center project name.");
}
