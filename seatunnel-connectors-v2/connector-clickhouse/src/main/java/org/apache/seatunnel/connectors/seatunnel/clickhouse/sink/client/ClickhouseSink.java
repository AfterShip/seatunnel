/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.clickhouse.sink.client;

import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickHouseConstants;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ProxyContext;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ReaderOption;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.exception.ClickhouseConnectorException;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.shard.Shard;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.shard.ShardMetadata;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.sink.file.ClickhouseTable;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.state.CKAggCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.state.CKCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.state.ClickhouseSinkState;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.util.ClickhouseUtil;
import org.apache.seatunnel.connectors.seatunnel.common.utils.ConfigCenterUtils;
import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URL;
import java.util.*;

import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickHouseConstants.*;
import static org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseConfig.*;

@AutoService(SeaTunnelSink.class)
public class ClickhouseSink
        implements SeaTunnelSink<SeaTunnelRow, ClickhouseSinkState, CKCommitInfo, CKAggCommitInfo> {

    private static final Logger logger = LoggerFactory.getLogger(ClickhouseSink.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    public static final String TMP_CA_PEM = "/tmp/ca-%s.pem";
    private String caPemPath;
    private String host;
    private String userName;
    private String password;
    private String dbSSLRootCRT;
    private ReaderOption option;

    @Override
    public String getPluginName() {
        return "Clickhouse";
    }

    @SuppressWarnings("checkstyle:MagicNumber")
    @Override
    public void prepare(Config config) throws PrepareFailException {
        CheckResult result =
                CheckConfigUtil.checkAllExists(config, DATABASE.key(), TABLE.key());

        boolean isCredential = config.hasPath(ClickHouseConstants.CONFIG_CENTER_TOKEN)
                && config.hasPath(ClickHouseConstants.CONFIG_CENTER_URL)
                && config.hasPath(ClickHouseConstants.ENVIRONMENT)
                && config.hasPath(ClickHouseConstants.CONFIG_CENTER_PROJECT);

        if (isCredential) {
            result = CheckConfigUtil.checkAllExists(config,
                    ClickHouseConstants.CONFIG_CENTER_TOKEN,
                    ClickHouseConstants.CONFIG_CENTER_URL,
                    ClickHouseConstants.ENVIRONMENT,
                    ClickHouseConstants.CONFIG_CENTER_PROJECT);
        }

        if (!result.isSuccess()) {
            throw new ClickhouseConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "PluginName: %s, PluginType: %s, Message: %s",
                            getPluginName(), PluginType.SINK, result.getMsg()));
        }
        Map<String, Object> defaultConfig =
                ImmutableMap.<String, Object>builder()
                        .put(BULK_SIZE.key(), BULK_SIZE.defaultValue())
                        .put(SPLIT_MODE.key(), SPLIT_MODE.defaultValue())
                        .build();

        config = config.withFallback(ConfigFactory.parseMap(defaultConfig));

        Map<String, String> entries = ConfigCenterUtils.getConfigCenterEntries(
                config.getString(ClickHouseConstants.CONFIG_CENTER_TOKEN),
                config.getString(ClickHouseConstants.CONFIG_CENTER_URL),
                config.getString(ClickHouseConstants.ENVIRONMENT),
                config.getString(ClickHouseConstants.CONFIG_CENTER_PROJECT));

        String clusterName = config.hasPath(CLUSTER.key()) ? config.getString(CLUSTER.key()) : "prod-data";
        ProxyContext proxyContext = getProxyContext(entries.get(String.format(ClickHouseConstants.AUTH, clusterName)));
        this.userName = proxyContext.getUser();
        this.password = proxyContext.getPassword();

        Properties clickhouseProperties = new Properties();
        if (CheckConfigUtil.isValidParam(config, CLICKHOUSE_CONFIG.key())) {
            config.getObject(CLICKHOUSE_CONFIG.key())
                    .forEach(
                            (key, value) ->
                                    clickhouseProperties.put(
                                            key, String.valueOf(value.unwrapped())));
        }

        if (isCredential) {
            clickhouseProperties.put("user", this.userName);
            clickhouseProperties.put("password", this.password);
        }

        // init caPem
        this.caPemPath = String.format(TMP_CA_PEM, UUID.randomUUID().toString().replace("-", "").toLowerCase());
        initCaPem(proxyContext, entries, clickhouseProperties, clusterName);

        Map<String, String> createNodesOptions = Maps.newHashMap();
        createNodesOptions.put(ClickHouseClientOption.SSL.getKey(), "true");
        createNodesOptions.put(ClickHouseClientOption.SSL_ROOT_CERTIFICATE.getKey(), this.caPemPath);

        List<ClickHouseNode> nodes;
        if (!isCredential) {
            nodes =
                    ClickhouseUtil.createNodes(
                            this.host,
                            config.getString(DATABASE.key()),
                            null,
                            null,
                            createNodesOptions);
        } else {
            nodes =
                    ClickhouseUtil.createNodes(
                            this.host,
                            config.getString(DATABASE.key()),
                            this.userName,
                            this.password,
                            createNodesOptions);
        }

        ClickhouseProxy proxy = new ClickhouseProxy(nodes.get(0));
        Map<String, String> tableSchema =
                proxy.getClickhouseTableSchema(config.getString(DATABASE.key()), config.getString(TABLE.key()));
        String shardKey = null;
        String shardKeyType = null;
        ClickhouseTable table =
                proxy.getClickhouseTable(
                        config.getString(DATABASE.key()), config.getString(TABLE.key()));
        if (config.getBoolean(SPLIT_MODE.key())) {
            if (!"Distributed".equals(table.getEngine())) {
                throw new ClickhouseConnectorException(
                        CommonErrorCode.ILLEGAL_ARGUMENT,
                        "split mode only support table which engine is "
                                + "'Distributed' engine at now");
            }
            if (config.hasPath(SHARDING_KEY.key())) {
                shardKey = config.getString(SHARDING_KEY.key());
                shardKeyType = tableSchema.get(shardKey);
            }
        }
        ShardMetadata metadata;
        String tableName = table.getDistributedEngine() != null ?
                table.getDistributedEngine().getTable() : config.getString(TABLE.key());
        String engine = table.getDistributedEngine() != null ?
                table.getDistributedEngine().getTableEngine() : table.getEngine();

        String writeMode = config.getString(WRITE_MODE.key());
        String importMode = config.getString(IMPORT_MODE.key());
        if (writeMode.equalsIgnoreCase(OVERWRITE)) {
            if (importMode.equalsIgnoreCase(PARTITION)) {
                // 删除数据分区，不可恢复
                String alterDropDetachPartition = String.format(ALTER_DROP_PARTITION, config.getString(DATABASE.key()),
                        tableName, config.getString(PARTITION_VALUE.key()));
                proxy.executeSql(alterDropDetachPartition);
            } else {
                // 导入整张表
                // 清空目标表数据，不可恢复
                String truncateTable = String.format(TRUNCATE_TABLE, config.getString(DATABASE.key()), tableName);
                proxy.executeSql(truncateTable);
            }
        }

        if (isCredential) {
            metadata =
                    new ShardMetadata(
                            shardKey,
                            shardKeyType,
                            table.getSortingKey(),
                            config.getString(DATABASE.key()),
                            tableName,
                            engine,
                            config.getBoolean(SPLIT_MODE.key()),
                            new Shard(1, 1, nodes.get(0)),
                            this.userName,
                            this.password);
        } else {
            metadata =
                    new ShardMetadata(
                            shardKey,
                            shardKeyType,
                            table.getSortingKey(),
                            config.getString(DATABASE.key()),
                            tableName,
                            engine,
                            config.getBoolean(SPLIT_MODE.key()),
                            new Shard(1, 1, nodes.get(0)));
        }

        proxy.close();

        String[] primaryKeys = null;
        if (config.hasPath(PRIMARY_KEY.key())) {
            String primaryKey = config.getString(PRIMARY_KEY.key());
            if (shardKey != null && !Objects.equals(primaryKey, shardKey)) {
                throw new ClickhouseConnectorException(
                        CommonErrorCode.ILLEGAL_ARGUMENT,
                        "sharding_key and primary_key must be consistent to ensure correct processing of cdc events");
            }
            primaryKeys = new String[]{primaryKey};
        }
        boolean supportUpsert = SUPPORT_UPSERT.defaultValue();
        if (config.hasPath(SUPPORT_UPSERT.key())) {
            supportUpsert = config.getBoolean(SUPPORT_UPSERT.key());
        }
        boolean allowExperimentalLightweightDelete =
                ALLOW_EXPERIMENTAL_LIGHTWEIGHT_DELETE.defaultValue();
        if (config.hasPath(ALLOW_EXPERIMENTAL_LIGHTWEIGHT_DELETE.key())) {
            allowExperimentalLightweightDelete =
                    config.getBoolean(ALLOW_EXPERIMENTAL_LIGHTWEIGHT_DELETE.key());
        }
        this.option =
                ReaderOption.builder()
                        .shardMetadata(metadata)
                        .properties(clickhouseProperties)
                        .tableEngine(engine)
                        .tableSchema(tableSchema)
                        .bulkSize(config.getInt(BULK_SIZE.key()))
                        .primaryKeys(primaryKeys)
                        .supportUpsert(supportUpsert)
                        .allowExperimentalLightweightDelete(allowExperimentalLightweightDelete)
                        .build();

        // delete caPem
        File file = new File(caPemPath);
        if (file.exists()) {
            if (file.delete()) {
                logger.info("Delete file in writer: {}", caPemPath);
            }
        }
    }

    @Override
    public SinkWriter<SeaTunnelRow, CKCommitInfo, ClickhouseSinkState> createWriter(
            SinkWriter.Context context) throws IOException {
        return new ClickhouseSinkWriter(option, context);
    }

    @Override
    public SinkWriter<SeaTunnelRow, CKCommitInfo, ClickhouseSinkState> restoreWriter(
            SinkWriter.Context context, List<ClickhouseSinkState> states) throws IOException {
        return SeaTunnelSink.super.restoreWriter(context, states);
    }

    @Override
    public Optional<Serializer<ClickhouseSinkState>> getWriterStateSerializer() {
        return Optional.of(new DefaultSerializer<>());
    }

    @Override
    public void setTypeInfo(SeaTunnelRowType seaTunnelRowType) {
        this.option.setSeaTunnelRowType(seaTunnelRowType);
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getConsumedType() {
        return this.option.getSeaTunnelRowType();
    }

    private ProxyContext getProxyContext(String proxyString) {
        if (StringUtils.isBlank(proxyString)) {
            logger.info("proxyString is null or empty.");
            return null;
        }
        try {
            return MAPPER.readValue(proxyString, ProxyContext.class);
        } catch (IOException e) {
            logger.error("Failed to parse proxy key: {}, error_msg: {}.", proxyString, e.getMessage(), e);
        }
        return null;
    }

    private void initCaPem(ProxyContext proxyContext, Map<String, String> entries, Properties properties, String clusterName) {
        try {
            URL url = null;
            if (!proxyContext.getHost().startsWith("http") && !proxyContext.getHost().startsWith("https")) {
                url = new URL("https://" + proxyContext.getHost());
            }
            this.host = proxyContext.getHost().replace("https://", "").replace("http://", "");
            logger.info("CH url: {}, caPem: {}", url, this.caPemPath);

            this.dbSSLRootCRT = entries.get(String.format(ClickHouseConstants.DB_SSL_ROOT_CRT, clusterName));
            File file = new File(this.caPemPath);

            //if file doesnt exists, then create it
            if (!file.exists()) {
                file.createNewFile();
                FileWriter fileWritter = new FileWriter(file.getAbsoluteFile(), true);
                BufferedWriter bufferWritter = new BufferedWriter(fileWritter);
                bufferWritter.write(this.dbSSLRootCRT);
                bufferWritter.close();
                logger.info("Write caPem file: {}", this.caPemPath);
            }
        } catch (IOException e) {
            throw new RuntimeException("Init capem failed." + e);
        }

        properties.setProperty(ClickHouseConstants.NAME_CA_PEM_VALUE, this.dbSSLRootCRT);
        properties.setProperty(ClickHouseClientOption.SSL.getKey(), "true");
        properties.setProperty(ClickHouseClientOption.SSL_ROOT_CERTIFICATE.getKey(), this.caPemPath);
    }
}
