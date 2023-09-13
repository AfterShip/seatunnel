package org.apache.seatunnel.connectors.seatunnel.bigtable.config;

import com.google.common.base.Strings;
import lombok.Builder;
import lombok.Getter;
import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.seatunnel.bigtable.common.HBaseColumn;
import org.apache.seatunnel.connectors.seatunnel.bigtable.exception.BigtableConnectorException;
import org.apache.seatunnel.connectors.seatunnel.bigtable.sink.BigtableTableSinkFactory;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.seatunnel.connectors.seatunnel.bigtable.config.BigtableConfig.*;

/**
 * @author: gf.xu
 * @email: gf.xu@aftership.com
 * @date: 2023/8/17 19:10
 */
@Getter
@Builder
public class BigtableParameters implements Serializable {

    private static final long serialVersionUID = 1L;

    private String projectId;

    private String instanceId;

    private String tableId;

    private String keyAlias;

    private Map<String, HBaseColumn> columnMappings;

    private String configCenterToken;

    private String configCenterUrl;

    private String configCenterEnvironment;

    private String configCenterProject;

    private Map<String, String> bigtableOptions;

    @Builder.Default
    private int bufferFlushMaxSizeInBytes = SINK_BUFFER_FLUSH_MAX_SIZE_IN_BYTES.defaultValue();

    @Builder.Default
    private int bufferFlushMaxMutations = SINK_BUFFER_FLUSH_MAX_MUTATIONS.defaultValue();

    @Builder.Default
    private EnCoding enCoding = ENCODING.defaultValue();

    public static BigtableParameters buildWithConfig(Config pluginConfig) {
        BigtableParametersBuilder builder = BigtableParameters.builder();
        // project_id
        builder.projectId(pluginConfig.getString(PROJECT_ID.key()));

        // instance_id
        builder.instanceId(pluginConfig.getString(INSTANCE_ID.key()));

        // table_id
        builder.tableId(pluginConfig.getString(TABLE_ID.key()));

        // key_alias
        builder.keyAlias(pluginConfig.getString(KEY_ALIAS.key()));

        // column_mappings
        String columnMappings = pluginConfig.getString(COLUMN_MAPPINGS.key());
        builder.columnMappings(getColumnMappings(columnMappings));

        // encoding
        if (pluginConfig.hasPath(ENCODING.key())) {
            String encoding = pluginConfig.getString(ENCODING.key());
            builder.enCoding(EnCoding.valueOf(encoding.toUpperCase()));
        }

        // config_center_token
        builder.configCenterToken(pluginConfig.getString(CONFIG_CENTER_TOKEN.key()));

        // config_center_url
        builder.configCenterUrl(pluginConfig.getString(CONFIG_CENTER_URL.key()));

        // config_center_environment
        builder.configCenterEnvironment(pluginConfig.getString(CONFIG_CENTER_ENVIRONMENT.key()));

        // config_center_project
        builder.configCenterProject(pluginConfig.getString(CONFIG_CENTER_PROJECT.key()));

        // bigtable_options
        if (pluginConfig.hasPath(BIGTABLE_OPTIONS.key())) {
            String bigtableOptions = pluginConfig.getString(BIGTABLE_OPTIONS.key());
            builder.bigtableOptions(parseKeyValueConfig(bigtableOptions, ",", "="));
        }

        // sink_buffer_flush_max_size_in_bytes
        if (pluginConfig.hasPath(SINK_BUFFER_FLUSH_MAX_SIZE_IN_BYTES.key())) {
            builder.bufferFlushMaxSizeInBytes(pluginConfig.getInt(SINK_BUFFER_FLUSH_MAX_SIZE_IN_BYTES.key()));
        }

        // sink_buffer_flush_max_mutations
        if (pluginConfig.hasPath(SINK_BUFFER_FLUSH_MAX_MUTATIONS.key())) {
            builder.bufferFlushMaxMutations(pluginConfig.getInt(SINK_BUFFER_FLUSH_MAX_MUTATIONS.key()));
        }
        return builder.build();
    }

    public static Map<String, HBaseColumn> getColumnMappings(String columnMappings) {
        Map<String, String> specifiedMappings = Strings.isNullOrEmpty(columnMappings) ?
                Collections.emptyMap() : parseKeyValueConfig(columnMappings, ",", "=");
        Map<String, HBaseColumn> mappings = new HashMap<>(specifiedMappings.size());

        for (Map.Entry<String, String> entry : specifiedMappings.entrySet()) {
            try {
                String field = entry.getKey();
                HBaseColumn column = HBaseColumn.fromFullName(entry.getValue());
                mappings.put(field, column);
            } catch (IllegalArgumentException e) {
                String errorMessage = String.format("Invalid column in mapping '%s'. Reason: %s",
                        entry.getKey(), e.getMessage());
                throw new BigtableConnectorException(
                        SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                        String.format(
                                "PluginName: %s, PluginType: %s, Message: %s", BigtableTableSinkFactory.IDENTIFIER
                                , PluginType.SINK, errorMessage));
            }
        }
        return mappings;
    }

    public static Map<String, String> parseKeyValueConfig(String configValue, String delimiter,
                                                          String keyValueDelimiter) {
        Map<String, String> map = new HashMap<>();
        for (String property : configValue.split(delimiter)) {
            String[] parts = property.split(keyValueDelimiter);
            String key = parts[0];
            String value = parts[1];
            map.put(key, value);
        }
        return map;
    }

    public static String getKVPair(String key, String value, String keyValueDelimiter) {
        return String.format("%s%s%s", key, keyValueDelimiter, value);
    }
}
