package org.apache.seatunnel.connectors.seatunnel.spanner.config;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import lombok.Builder;
import lombok.Getter;

import java.io.Serializable;

import static org.apache.seatunnel.connectors.seatunnel.common.config.ConfigCenterConfig.CONFIG_CENTER_ENVIRONMENT;
import static org.apache.seatunnel.connectors.seatunnel.common.config.ConfigCenterConfig.CONFIG_CENTER_PROJECT;
import static org.apache.seatunnel.connectors.seatunnel.common.config.ConfigCenterConfig.CONFIG_CENTER_TOKEN;
import static org.apache.seatunnel.connectors.seatunnel.common.config.ConfigCenterConfig.CONFIG_CENTER_URL;
import static org.apache.seatunnel.connectors.seatunnel.spanner.config.SpannerConfig.BATCH_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.spanner.config.SpannerConfig.DATABASE_ID;
import static org.apache.seatunnel.connectors.seatunnel.spanner.config.SpannerConfig.IMPORT_QUERY;
import static org.apache.seatunnel.connectors.seatunnel.spanner.config.SpannerConfig.INSTANCE_ID;
import static org.apache.seatunnel.connectors.seatunnel.spanner.config.SpannerConfig.MAX_PARTITIONS;
import static org.apache.seatunnel.connectors.seatunnel.spanner.config.SpannerConfig.PARTITION_SIZE_MB;
import static org.apache.seatunnel.connectors.seatunnel.spanner.config.SpannerConfig.PROJECT_ID;
import static org.apache.seatunnel.connectors.seatunnel.spanner.config.SpannerConfig.TABLE_ID;

/**
 * @author: gf.xu
 * @email: gf.xu@aftership.com
 * @date: 2023/8/25 14:53
 */
@Getter
@Builder
public class SpannerParameters implements Serializable {

    private static final long serialVersionUID = 1L;

    private String projectId;

    private String instanceId;

    private String databaseId;

    private String tableId;

    private String configCenterToken;

    private String configCenterUrl;

    private String configCenterEnvironment;

    private String configCenterProject;

    private String importQuery;

    private Long partitionSizeMB;

    private Long maxPartitions;

    @Builder.Default private int batchSize = BATCH_SIZE.defaultValue();

    public static SpannerParameters buildWithSinkConfig(Config pluginConfig) {
        SpannerParametersBuilder builder = SpannerParameters.builder();
        // project_id
        builder.projectId(pluginConfig.getString(PROJECT_ID.key()));

        // instance_id
        builder.instanceId(pluginConfig.getString(INSTANCE_ID.key()));

        // database_id
        builder.databaseId(pluginConfig.getString(DATABASE_ID.key()));

        // table_id
        builder.tableId(pluginConfig.getString(TABLE_ID.key()));

        // config_center_token
        builder.configCenterToken(pluginConfig.getString(CONFIG_CENTER_TOKEN.key()));

        // config_center_url
        builder.configCenterUrl(pluginConfig.getString(CONFIG_CENTER_URL.key()));

        // config_center_environment
        builder.configCenterEnvironment(pluginConfig.getString(CONFIG_CENTER_ENVIRONMENT.key()));

        // config_center_project
        builder.configCenterProject(pluginConfig.getString(CONFIG_CENTER_PROJECT.key()));

        // batch_size
        if (pluginConfig.hasPath(BATCH_SIZE.key())) {
            builder.batchSize(pluginConfig.getInt(BATCH_SIZE.key()));
        }
        return builder.build();
    }

    public static SpannerParameters buildWithSourceConfig(Config pluginConfig) {
        SpannerParametersBuilder builder = SpannerParameters.builder();

        // project_id
        builder.projectId(pluginConfig.getString(PROJECT_ID.key()));

        // instance_id
        builder.instanceId(pluginConfig.getString(INSTANCE_ID.key()));

        // database_id
        builder.databaseId(pluginConfig.getString(DATABASE_ID.key()));

        // table_id
        builder.tableId(pluginConfig.getString(TABLE_ID.key()));

        // config_center_token
        builder.configCenterToken(pluginConfig.getString(CONFIG_CENTER_TOKEN.key()));

        // config_center_url
        builder.configCenterUrl(pluginConfig.getString(CONFIG_CENTER_URL.key()));

        // config_center_environment
        builder.configCenterEnvironment(pluginConfig.getString(CONFIG_CENTER_ENVIRONMENT.key()));

        // config_center_project
        builder.configCenterProject(pluginConfig.getString(CONFIG_CENTER_PROJECT.key()));

        // import_query
        if (pluginConfig.hasPath(IMPORT_QUERY.key())) {
            builder.importQuery(pluginConfig.getString(IMPORT_QUERY.key()));
        } else {
            builder.importQuery(
                    String.format("Select * from %s;", pluginConfig.getString(TABLE_ID.key())));
        }

        // max_partitions
        if (pluginConfig.hasPath(MAX_PARTITIONS.key())) {
            builder.maxPartitions(pluginConfig.getLong(MAX_PARTITIONS.key()));
        }

        // partition_size_mb
        if (pluginConfig.hasPath(PARTITION_SIZE_MB.key())) {
            builder.partitionSizeMB(pluginConfig.getLong(PARTITION_SIZE_MB.key()));
        }

        return builder.build();
    }
}
