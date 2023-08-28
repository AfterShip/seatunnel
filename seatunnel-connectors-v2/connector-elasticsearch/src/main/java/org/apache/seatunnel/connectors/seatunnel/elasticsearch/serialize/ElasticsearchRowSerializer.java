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

package org.apache.seatunnel.connectors.seatunnel.elasticsearch.serialize;

import org.apache.commons.lang3.StringUtils;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.exception.ElasticsearchConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.exception.ElasticsearchConnectorException;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import java.io.IOException;
import java.io.StringWriter;
import java.time.temporal.Temporal;
import java.util.*;

import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.SinkConfig.ID_FIELD_IGNORED;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.SinkConfig.*;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.constant.ElasticsearchConstants.*;

/** use in elasticsearch version >= 2.x and <= 8.x */
public class ElasticsearchRowSerializer implements SeaTunnelRowSerializer {
    private final SeaTunnelRowType seaTunnelRowType;
    private final ObjectMapper objectMapper = new ObjectMapper();

    private final Config pluginConfig;

    public ElasticsearchRowSerializer(
            Config pluginConfig,
            SeaTunnelRowType seaTunnelRowType) {
        this.pluginConfig = pluginConfig;
        this.seaTunnelRowType = seaTunnelRowType;
    }

    @Override
    public String serializeRow(SeaTunnelRow row) {
        switch (row.getRowKind()) {
            case INSERT:
            case UPDATE_AFTER:
                return serializeUpsert(row);
            case UPDATE_BEFORE:
            case DELETE:
                return serializeDelete(row);
            default:
                throw new ElasticsearchConnectorException(
                        CommonErrorCode.UNSUPPORTED_OPERATION,
                        "Unsupported write row kind: " + row.getRowKind());
        }
    }

    private String serializeUpsert(SeaTunnelRow row) {
        Map<String, Object> document = toDocumentMap(row);
        String index = null;
        String id = document.get(pluginConfig.getString(ID_FIELD.key())).toString();
        ArrayList<String> filterList = new ArrayList<>();
        if (pluginConfig.getBoolean(ID_FIELD_IGNORED.key())) {
            filterList.add(pluginConfig.getString(ID_FIELD.key()));
        }
        String indexType = pluginConfig.getString(INDEX_TYPE.key());
        if (INDEX_TYPE_CUSTOMIZE.equals(indexType)) {
            index = pluginConfig.getString(INDEX.key());
        } else if (INDEX_TYPE_FIELD.equals(indexType)) {
            index = (String) document.get(pluginConfig.getString(INDEX.key()));
            if (StringUtils.isBlank(index)) {
                throw new ElasticsearchConnectorException(
                        ElasticsearchConnectorErrorCode.BUSINESS_FAILED,
                        String.format("Index must not be null or empty, index field: %s",
                                pluginConfig.getString(INDEX.key())));
            }
            filterList.add(pluginConfig.getString(INDEX.key()));
        }
        HashMap<String, String> metadata = new HashMap<>();
        metadata.put("_index", index);
        metadata.put("_id", id);

        try {

            String actionType = pluginConfig.getString(ACTION_TYPE.key());

            if (actionType.equals(ACTION_TYPE_INDEX)) {
                /**
                 * format example: { "index" : {"_index" : "${your_index}", "_id" :
                 * "${your_document_id}"} }\n ${your_document_json}
                 */
                return new StringBuilder()
                        .append("{ \"index\" :")
                        .append(objectMapper.writeValueAsString(metadata))
                        .append("}")
                        .append("\n")
                        .append(transform(document, filterList))
                        .toString();
            } else {
                /**
                 * format example: { "update" : {"_index" : "${your_index}", "_id" :
                 * "${your_document_id}"} }\n { "doc" : ${your_document_json}, "doc_as_upsert" :
                 * true }
                 */
                return new StringBuilder()
                        .append("{ \"update\" :")
                        .append(objectMapper.writeValueAsString(metadata))
                        .append("}")
                        .append("\n")
                        .append("{ \"doc\" :")
                        .append(transform(document, filterList))
                        .append(", \"doc_as_upsert\" : true }")
                        .toString();
            }

        } catch (JsonProcessingException e) {
            throw new ElasticsearchConnectorException(
                    CommonErrorCode.JSON_OPERATION_FAILED,
                    "Object json deserialization exception.",
                    e);
        }
    }

    private String transform(Map<String, Object> document, List<String> filterList) {
        List<String> writeAsObjectFields = Arrays.asList((pluginConfig.hasPath(WRITE_AS_OBJECT_FIELDS.key())
                ? pluginConfig.getString(WRITE_AS_OBJECT_FIELDS.key())
                : "").split(","));
        StringWriter writer = new StringWriter();
        try (JsonGenerator generator = objectMapper.getFactory().createGenerator(writer)) {
            generator.writeStartObject();
            for (Map.Entry<String, Object> entry : document.entrySet()) {
                // From all the fields in input record, write only those fields that are present in output schema
                if (entry.getValue() == null || filterList.contains(entry.getKey())) {
                    continue;
                }
                boolean writeAsObject = writeAsObjectFields.contains(entry.getKey());
                RowToJson.write(generator, entry.getKey(), entry.getValue(),
                        seaTunnelRowType.getFieldType(seaTunnelRowType.indexOf(entry.getKey())),
                        writeAsObject);
            }
            generator.writeEndObject();
        } catch (IOException e) {
            throw new ElasticsearchConnectorException(
                    ElasticsearchConnectorErrorCode.BUSINESS_FAILED,
                    "transform failed");
        }
        return writer.toString();
    }

    private String serializeDelete(SeaTunnelRow row) {
        Map<String, Object> document = toDocumentMap(row);
        String index = null;
        String id = document.get(pluginConfig.getString(ID_FIELD.key())).toString();
        ArrayList<String> filterList = new ArrayList<>();
        if (pluginConfig.getBoolean(ID_FIELD_IGNORED.key())) {
            filterList.add(pluginConfig.getString(ID_FIELD.key()));
        }
        String indexType = pluginConfig.getString(INDEX_TYPE.key());
        if (INDEX_TYPE_CUSTOMIZE.equals(indexType)) {
            index = pluginConfig.getString(INDEX.key());
        } else if (INDEX_TYPE_FIELD.equals(indexType)) {
            index = (String) document.get(pluginConfig.getString(INDEX.key()));
            if (StringUtils.isBlank(index)) {
                throw new ElasticsearchConnectorException(
                        ElasticsearchConnectorErrorCode.BUSINESS_FAILED,
                        String.format("Index must not be null or empty, index field: %s",
                                pluginConfig.getString(INDEX.key())));
            }
            filterList.add(pluginConfig.getString(INDEX.key()));
        }
        HashMap<String, String> metadata = new HashMap<>();
        metadata.put("_index", index);
        metadata.put("_id", id);
        try {
            /**
             * format example: { "delete" : {"_index" : "${your_index}", "_id" :
             * "${your_document_id}"} }
             */
            return new StringBuilder()
                    .append("{ \"delete\" :")
                    .append(objectMapper.writeValueAsString(metadata))
                    .append("}")
                    .toString();
        } catch (JsonProcessingException e) {
            throw new ElasticsearchConnectorException(
                    CommonErrorCode.JSON_OPERATION_FAILED,
                    "Object json deserialization exception.",
                    e);
        }
    }

    private Map<String, Object> toDocumentMap(SeaTunnelRow row) {
        String[] fieldNames = seaTunnelRowType.getFieldNames();
        Map<String, Object> doc = new HashMap<>(fieldNames.length);
        Object[] fields = row.getFields();
        for (int i = 0; i < fieldNames.length; i++) {
            Object value = fields[i];
            if (value instanceof Temporal) {
                // jackson not support jdk8 new time api
                doc.put(fieldNames[i], value.toString());
            } else {
                doc.put(fieldNames[i], value);
            }
        }
        return doc;
    }

}
