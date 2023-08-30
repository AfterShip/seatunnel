package org.apache.seatunnel.connectors.seatunnel.gcs.sink;

import com.google.auto.service.AutoService;
import org.apache.commons.lang3.StringUtils;
import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSimpleSink;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.gcs.exception.GcsConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.gcs.exception.GcsConnectorException;
import org.apache.seatunnel.connectors.seatunnel.gcs.util.GCSPath;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import java.io.IOException;
import java.text.SimpleDateFormat;

import static org.apache.seatunnel.connectors.seatunnel.gcs.config.SinkConfig.*;
import static org.apache.seatunnel.connectors.seatunnel.gcs.constants.ContentTypeConstants.*;

@AutoService(SeaTunnelSink.class)
public class GcsSink extends AbstractSimpleSink<SeaTunnelRow, Void> {

    private SeaTunnelRowType seaTunnelRowType;

    public GcsSink(SeaTunnelRowType seaTunnelRowType) {
        this.seaTunnelRowType = seaTunnelRowType;
    }

    @Override
    public String getPluginName() {
        return "GCS";
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        // validate pluginConfig
        String path = pluginConfig.getString(PATH.key());
        try {
            GCSPath.from(path);
        } catch (IllegalArgumentException e) {
            throw new GcsConnectorException(GcsConnectorErrorCode.VALIDATE_FAILED,
                    "path is invalid, please check it." + e.getMessage());
        }
        if (pluginConfig.hasPath(SUFFIX.key())) {
            String suffix = pluginConfig.getString(SUFFIX.key());
            if (StringUtils.isNoneEmpty()) {
                try {
                    new SimpleDateFormat(suffix);
                } catch (IllegalArgumentException e) {
                    throw new GcsConnectorException(GcsConnectorErrorCode.VALIDATE_FAILED,
                            "suffix is invalid, please check it." + e.getMessage());
                }
            }
        }
        String format = pluginConfig.getString(FORMAT.key());
        String contentType = pluginConfig.getString(CONTENT_TYPE.key());
        validateContentType(format, contentType);

    }

    private void validateContentType(String format, String contentType) {
        switch (format) {
            case FORMAT_AVRO:
                if (!contentType.equals(CONTENT_TYPE_APPLICATION_AVRO)
                        && !contentType.equals(DEFAULT_CONTENT_TYPE)) {
                    throw new GcsConnectorException(GcsConnectorErrorCode.VALIDATE_FAILED,
                            "format is avro, but contentType is not supported.");
                }
                break;
            case FORMAT_CSV:
                if (!contentType.equals(CONTENT_TYPE_TEXT_CSV)
                        && !contentType.equals(CONTENT_TYPE_TEXT_PLAIN)
                        && !contentType.equals(CONTENT_TYPE_APPLICATION_CSV)
                        && !contentType.equals(DEFAULT_CONTENT_TYPE)) {
                    throw new GcsConnectorException(GcsConnectorErrorCode.VALIDATE_FAILED,
                            "format is csv, but contentType is not supported.");
                }
                break;
            case FORMAT_JSON:
                if (!contentType.equals(CONTENT_TYPE_APPLICATION_JSON)
                        && !contentType.equals(CONTENT_TYPE_TEXT_PLAIN)
                        && !contentType.equals(DEFAULT_CONTENT_TYPE)) {
                    throw new GcsConnectorException(GcsConnectorErrorCode.VALIDATE_FAILED,
                            "format is json, but contentType is not supported.");
                }
                break;
            case FORMAT_DELIMITED:
                if (!contentType.equals(CONTENT_TYPE_TEXT_PLAIN)
                        && !contentType.equals(CONTENT_TYPE_TEXT_CSV)
                        && !contentType.equals(CONTENT_TYPE_TEXT_TSV)
                        && !contentType.equals(CONTENT_TYPE_APPLICATION_CSV)
                        && !contentType.equals(DEFAULT_CONTENT_TYPE)) {
                    throw new GcsConnectorException(GcsConnectorErrorCode.VALIDATE_FAILED,
                            "format is delimited, but contentType is not supported.");
                }
                break;
            case FORMAT_PARQUET:
                if (!contentType.equals(DEFAULT_CONTENT_TYPE)) {
                    throw new GcsConnectorException(GcsConnectorErrorCode.VALIDATE_FAILED,
                            "format is parquet, but contentType is not supported.");
                }
                break;
            case FORMAT_ORC:
                if (!contentType.equals(DEFAULT_CONTENT_TYPE)) {
                    throw new GcsConnectorException(GcsConnectorErrorCode.VALIDATE_FAILED,
                            "format is orc, but contentType is not supported.");
                }
                break;
            case FORMAT_TSV:
                if (!contentType.equals(CONTENT_TYPE_TEXT_TSV)
                        && !contentType.equals(CONTENT_TYPE_TEXT_PLAIN)
                        && !contentType.equals(DEFAULT_CONTENT_TYPE)) {
                    throw new GcsConnectorException(GcsConnectorErrorCode.VALIDATE_FAILED,
                            "format is tsv, but contentType is not supported.");
                }
                break;
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
    public AbstractSinkWriter<SeaTunnelRow, Void> createWriter(SinkWriter.Context context) throws IOException {
        return null;
    }
}
