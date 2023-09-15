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

package org.apache.seatunnel.connectors.seatunnel.elasticsearch.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.connectors.seatunnel.common.config.ConfigCenterConfig;

public class SinkConfig extends ConfigCenterConfig {

    public static final Option<String> INDEX =
            Options.key("index")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Elasticsearch index name.Index support contains variables of field name,such as seatunnel_${age},and the field must appear at seatunnel row. If not, we will treat it as a normal index");

    public static final Option<String> INDEX_TYPE =
            Options.key("index_type")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Identifies the value of index: output schema field or fill in manually");

    public static final Option<String> ACTION_TYPE =
            Options.key("action_type")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Identifies the type of request action: index or upsert");

    public static final Option<String> ID_FIELD =
            Options.key("id_field")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Primary key fields used to generate the document `_id`");

    public static final Option<String> KEY_DELIMITER =
            Options.key("key_delimiter")
                    .stringType()
                    .defaultValue("_")
                    .withDescription(
                            "Delimiter for composite keys (\"_\" by default), e.g., \"$\" would result in document `_id` \"KEY1$KEY2$KEY3\".");

    @SuppressWarnings("checkstyle:MagicNumber")
    public static final Option<Integer> BATCH_NUMBER =
            Options.key("batch_number")
                    .intType()
                    .defaultValue(1000)
                    .withDescription("Number of data records written per batch");

    @SuppressWarnings("checkstyle:MagicNumber")
    public static final Option<Integer> BATCH_SIZE_MB =
            Options.key("batch_size_mb")
                    .intType()
                    .defaultValue(5)
                    .withDescription("Maximum data records import per batch");

    @SuppressWarnings("checkstyle:MagicNumber")
    public static final Option<Integer> MAX_RETRY_COUNT =
            Options.key("max_retry_count")
                    .intType()
                    .defaultValue(3)
                    .withDescription("one bulk request max try count");

    public static final Option<String> CLUSTER =
            Options.key("cluster").stringType().noDefaultValue().withDescription("cluster of es");

    public static final Option<Boolean> ID_FIELD_IGNORED =
            Options.key("id_field_ignored")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("whether to ignore id field");

    public static final Option<String> WRITE_AS_OBJECT_FIELDS =
            Options.key("write_as_object_fields")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Write as object fields, split by comma.");
}
