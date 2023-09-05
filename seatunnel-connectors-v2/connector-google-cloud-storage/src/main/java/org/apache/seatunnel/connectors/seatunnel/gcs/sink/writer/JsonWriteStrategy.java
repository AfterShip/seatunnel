package org.apache.seatunnel.connectors.seatunnel.gcs.sink.writer;


import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.gcs.config.FileSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.gcs.exception.GcsConnectorException;
import org.apache.seatunnel.connectors.seatunnel.gcs.sink.json.RowToJson;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.StringWriter;
import java.util.LinkedHashMap;

@Slf4j
public class JsonWriteStrategy extends AbstractWriteStrategy {

    private final ObjectMapper objectMapper;
    private final LinkedHashMap<String, BufferedWriter> beingWrittenOutputStream;

    public JsonWriteStrategy(FileSinkConfig fileSinkConfig) {
        super(fileSinkConfig);
        this.objectMapper = new ObjectMapper();
        this.beingWrittenOutputStream = new LinkedHashMap<>();
    }

    @Override
    public void setSeaTunnelRowTypeInfo(SeaTunnelRowType seaTunnelRowType) {
        super.setSeaTunnelRowTypeInfo(seaTunnelRowType);
    }

    @Override
    public void write(SeaTunnelRow seaTunnelRow) {
        super.write(seaTunnelRow);
        String filePath = getOrCreateFilePath();
        BufferedWriter writer = getOrCreateBufferedWriter(filePath);
        String record = convertRowToJson(seaTunnelRow);
        try {
            writer.write(record);
            writer.newLine();
        } catch (Exception e) {
            throw new GcsConnectorException(CommonErrorCode.FILE_OPERATION_FAILED,
                    String.format("Open file output stream [%s] failed", filePath),
                    e);
        }
    }

    @Override
    public void finishAndCloseFile() {
        beingWrittenOutputStream.forEach(
                (key, value) -> {
                    try {
                        value.flush();
                        fileSystemUtils.createFile(this.transactionDirectory + "_SUCCESS");
                    } catch (IOException e) {
                        throw new GcsConnectorException(
                                CommonErrorCode.FLUSH_DATA_FAILED,
                                String.format("Flush data to this file [%s] failed", key),
                                e);
                    } finally {
                        try {
                            value.close();
                        } catch (IOException e) {
                            log.error("error when close output stream {}", key, e);
                        }
                    }
                });
        beingWrittenOutputStream.clear();
    }

    private BufferedWriter getOrCreateBufferedWriter(@NonNull String filePath) {
        BufferedWriter writer = this.beingWrittenOutputStream.get(filePath);
        if (writer == null) {
            try {
                BufferedWriter newWriter =
                        new BufferedWriter(new OutputStreamWriter(fileSystemUtils.getOutputStream(filePath)));
                this.beingWrittenOutputStream.put(filePath, newWriter);
                return newWriter;
            } catch (IOException e) {
                String errorMsg = String.format("Get json writer for file [%s] error", filePath);
                throw new GcsConnectorException(
                        CommonErrorCode.WRITER_OPERATION_FAILED, errorMsg, e);
            }
        }
        return writer;
    }

    public String convertRowToJson(SeaTunnelRow element) {
        StringWriter writer = new StringWriter();
        try (JsonGenerator generator = objectMapper.getFactory().createGenerator(writer)) {
            generator.writeStartObject();
            String[] fieldNames = this.seaTunnelRowType.getFieldNames();
            for (int i = 0; i < fieldNames.length; i++) {
                String fieldName = this.seaTunnelRowType.getFieldName(i);
                Object value = element.getField(i);
                RowToJson.write(generator, fieldName, value,
                        seaTunnelRowType.getFieldType(seaTunnelRowType.indexOf(fieldName)));
            }
            generator.writeEndObject();
        } catch (IOException e) {
            throw new GcsConnectorException(
                    CommonErrorCode.WRITER_OPERATION_FAILED, "Convert row to json failed.", e);
        }
        return writer.toString();
    }

}
