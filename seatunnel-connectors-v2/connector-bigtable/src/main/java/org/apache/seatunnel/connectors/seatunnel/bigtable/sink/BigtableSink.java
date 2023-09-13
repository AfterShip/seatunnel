package org.apache.seatunnel.connectors.seatunnel.bigtable.sink;

import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableSet;
import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.*;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.seatunnel.bigtable.config.BigtableParameters;
import org.apache.seatunnel.connectors.seatunnel.bigtable.exception.BigtableConnectorException;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSimpleSink;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import java.io.IOException;
import java.util.Set;

import static org.apache.seatunnel.api.table.type.SqlType.*;
import static org.apache.seatunnel.connectors.seatunnel.bigtable.config.BigtableConfig.*;

/**
 * @author: gf.xu
 * @email: gf.xu@aftership.com
 * @date: 2023/8/17 14:42
 */
@AutoService(SeaTunnelSink.class)
public class BigtableSink extends AbstractSimpleSink<SeaTunnelRow, Void> {

    private static final Set<SqlType> SUPPORTED_FIELD_TYPES = ImmutableSet.of(
            BOOLEAN,
            TINYINT,
            SMALLINT,
            INT,
            BIGINT,
            FLOAT,
            DOUBLE,
            STRING,
            BYTES
    );

    private SeaTunnelRowType seaTunnelRowType;

    private BigtableParameters bigtableParameters;

    private int rowkeyColumnIndex;

    @Override
    public String getPluginName() {
        return BigtableTableSinkFactory.IDENTIFIER;
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
                        KEY_ALIAS.key(),
                        COLUMN_MAPPINGS.key(),
                        CONFIG_CENTER_TOKEN.key(),
                        CONFIG_CENTER_URL.key(),
                        CONFIG_CENTER_ENVIRONMENT.key(),
                        CONFIG_CENTER_PROJECT.key());
        if (!result.isSuccess()) {
            throw new BigtableConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "PluginName: %s, PluginType: %s, Message: %s",
                            getPluginName(), PluginType.SINK, result.getMsg()));
        }
        this.bigtableParameters = BigtableParameters.buildWithConfig(pluginConfig);
    }

    @Override
    public void setTypeInfo(SeaTunnelRowType seaTunnelRowType) {
        this.seaTunnelRowType = seaTunnelRowType;
        this.rowkeyColumnIndex = seaTunnelRowType.indexOf(bigtableParameters.getKeyAlias());

        for (int i = 0; i < seaTunnelRowType.getTotalFields(); i++) {
            String fieldName = seaTunnelRowType.getFieldName(i);
            SeaTunnelDataType<?> dataType = seaTunnelRowType.getFieldType(i);
            if (!(dataType instanceof BasicType) || !SUPPORTED_FIELD_TYPES.contains(dataType.getSqlType())) {
                throw new BigtableConnectorException(
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
        return new BigtableSinkWriter(seaTunnelRowType, bigtableParameters, rowkeyColumnIndex);
    }
}
