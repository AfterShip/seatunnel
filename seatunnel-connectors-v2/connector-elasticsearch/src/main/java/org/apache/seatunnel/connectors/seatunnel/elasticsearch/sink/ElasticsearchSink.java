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

package org.apache.seatunnel.connectors.seatunnel.elasticsearch.sink;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.exception.ElasticsearchConnectorException;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.state.ElasticsearchAggregatedCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.state.ElasticsearchCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.state.ElasticsearchSinkState;

import com.google.auto.service.AutoService;

import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.SinkConfig.BATCH_NUMBER;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.SinkConfig.BATCH_SIZE_MB;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.SinkConfig.CLUSTER;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.SinkConfig.ID_FIELD;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.SinkConfig.INDEX;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.SinkConfig.MAX_RETRY_COUNT;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.constant.ElasticsearchConstants.CONFIG_CENTER_PROJECT;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.constant.ElasticsearchConstants.CONFIG_CENTER_TOKEN;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.constant.ElasticsearchConstants.CONFIG_CENTER_URL;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.constant.ElasticsearchConstants.ENVIRONMENT;

@AutoService(SeaTunnelSink.class)
public class ElasticsearchSink
        implements SeaTunnelSink<
                SeaTunnelRow,
                ElasticsearchSinkState,
                ElasticsearchCommitInfo,
                ElasticsearchAggregatedCommitInfo> {

    private Config pluginConfig;
    private SeaTunnelRowType seaTunnelRowType;

    private int maxBatchNumber = BATCH_NUMBER.defaultValue();
    private int maxBatchSizeMb = BATCH_SIZE_MB.defaultValue();
    private int maxRetryCount = MAX_RETRY_COUNT.defaultValue();

    @Override
    public String getPluginName() {
        return "Elasticsearch";
    }

    @Override
    public void prepare(Config config) throws PrepareFailException {
        CheckResult result =
                CheckConfigUtil.checkAllExists(config, INDEX.key(), ID_FIELD.key(), CLUSTER.key());

        boolean isCredential =
                config.hasPath(CONFIG_CENTER_TOKEN)
                        && config.hasPath(CONFIG_CENTER_URL)
                        && config.hasPath(ENVIRONMENT)
                        && config.hasPath(CONFIG_CENTER_PROJECT);

        if (isCredential) {
            result =
                    CheckConfigUtil.checkAllExists(
                            config,
                            CONFIG_CENTER_TOKEN,
                            CONFIG_CENTER_URL,
                            ENVIRONMENT,
                            CONFIG_CENTER_PROJECT);
        }

        if (!result.isSuccess()) {
            throw new ElasticsearchConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "PluginName: %s, PluginType: %s, Message: %s",
                            getPluginName(), PluginType.SINK, result.getMsg()));
        }

        this.pluginConfig = config;
        if (config.hasPath(BATCH_NUMBER.key())) {
            maxBatchNumber = config.getInt(BATCH_NUMBER.key());
        }
        if (config.hasPath(BATCH_SIZE_MB.key())) {
            maxBatchSizeMb = config.getInt(BATCH_SIZE_MB.key());
        }
        if (config.hasPath(MAX_RETRY_COUNT.key())) {
            maxRetryCount = config.getInt(MAX_RETRY_COUNT.key());
        }
    }

    @Override
    public void setTypeInfo(SeaTunnelRowType seaTunnelRowType) {
        this.seaTunnelRowType = seaTunnelRowType;
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getConsumedType() {
        return this.seaTunnelRowType;
    }

    @Override
    public SinkWriter<SeaTunnelRow, ElasticsearchCommitInfo, ElasticsearchSinkState> createWriter(
            SinkWriter.Context context) {
        return new ElasticsearchSinkWriter(
                context,
                seaTunnelRowType,
                pluginConfig,
                maxBatchNumber,
                maxBatchSizeMb,
                maxRetryCount);
    }
}
