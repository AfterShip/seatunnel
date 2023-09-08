package org.apache.seatunnel.connectors.seatunnel.spanner.sink;

import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableSet;
import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSimpleSink;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.spanner.config.SpannerParameters;
import org.apache.seatunnel.connectors.seatunnel.spanner.constants.SpannerConstants;
import org.apache.seatunnel.connectors.seatunnel.spanner.exception.SpannerConnectorException;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import java.io.IOException;
import java.util.Set;

import static org.apache.seatunnel.api.table.type.SqlType.*;
import static org.apache.seatunnel.connectors.seatunnel.common.config.ConfigCenterConfig.*;
import static org.apache.seatunnel.connectors.seatunnel.spanner.config.SpannerConfig.*;

/**
 * @author: gf.xu
 * @email: gf.xu@aftership.com
 * @date: 2023/8/25 14:28
 */
@AutoService(SeaTunnelSink.class)
public class SpannerSink extends AbstractSimpleSink<SeaTunnelRow, Void> {

    private static final Set<SqlType> SUPPORTED_FIELD_TYPES = ImmutableSet.of(
            BOOLEAN,
            STRING,
            TINYINT,
            SMALLINT,
            INT,
            BIGINT,
            FLOAT,
            DOUBLE,
            BYTES,
            DATE,
            TIMESTAMP,
            DECIMAL,
            ARRAY
    );

    private SeaTunnelRowType seaTunnelRowType;

    private SpannerParameters parameters;

    @Override
    public String getPluginName() {
        return SpannerConstants.IDENTIFIER;
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        // must have
        CheckResult result =
                CheckConfigUtil.checkAllExists(
                        pluginConfig,
                        PROJECT_ID.key(),
                        INSTANCE_ID.key(),
                        TABLE_ID.key(),
                        CONFIG_CENTER_TOKEN.key(),
                        CONFIG_CENTER_URL.key(),
                        CONFIG_CENTER_ENVIRONMENT.key(),
                        CONFIG_CENTER_PROJECT.key());
        if (!result.isSuccess()) {
            throw new SpannerConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "PluginName: %s, PluginType: %s, Message: %s",
                            getPluginName(), PluginType.SINK, result.getMsg()));
        }
        this.parameters = SpannerParameters.buildWithSinkConfig(pluginConfig);
    }

    @Override
    public void setTypeInfo(SeaTunnelRowType seaTunnelRowType) {
        this.seaTunnelRowType = seaTunnelRowType;
        for (int i = 0; i < seaTunnelRowType.getTotalFields(); i++) {
            String fieldName = seaTunnelRowType.getFieldName(i);
            SeaTunnelDataType<?> dataType = seaTunnelRowType.getFieldType(i);
            if (!SUPPORTED_FIELD_TYPES.contains(dataType.getSqlType())) {
                throw new SpannerConnectorException(
                        SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                        String.format("PluginName: %s, PluginType: %s, Message: %s",
                                getPluginName(), PluginType.SINK,
                                "Unsupported field type: " + dataType.getSqlType() + " for field: " + fieldName));
            }
        }
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getConsumedType() {
        return seaTunnelRowType;
    }

    @Override
    public AbstractSinkWriter<SeaTunnelRow, Void> createWriter(SinkWriter.Context context) throws IOException {
        return new SpannerWriter(context, parameters, seaTunnelRowType);
    }
}
