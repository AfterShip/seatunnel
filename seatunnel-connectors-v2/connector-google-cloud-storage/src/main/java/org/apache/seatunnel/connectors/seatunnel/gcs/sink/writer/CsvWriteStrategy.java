package org.apache.seatunnel.connectors.seatunnel.gcs.sink.writer;

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.gcs.config.FileSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.gcs.exception.GcsConnectorException;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public class CsvWriteStrategy extends AbstractWriteStrategy {

    public static final String[] SEPARATOR =
            new String[] {",", "\u0002", "\u0003", "\u0004", "\u0005", "\u0006", "\u0007"};

    private final LinkedHashMap<String, BufferedWriter> beingWrittenOutputStream;

    public CsvWriteStrategy(FileSinkConfig fileSinkConfig) {
        super(fileSinkConfig);
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
        String record = convertRowToCsv(seaTunnelRow);
        try {
            writer.write(record);
            writer.newLine();
        } catch (IOException e) {
            String errorMsg =
                    String.format("Write record [%s] to file [%s] error", record, filePath);
            throw new GcsConnectorException(CommonErrorCode.WRITER_OPERATION_FAILED, errorMsg, e);
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
                        new BufferedWriter(
                                new OutputStreamWriter(fileSystemUtils.getOutputStream(filePath)));
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

    private String convertRowToCsv(SeaTunnelRow element) {
        Object[] fields = element.getFields();
        String[] strings = new String[fields.length];
        for (int i = 0; i < fields.length; i++) {
            strings[i] = convert(fields[i], seaTunnelRowType.getFieldType(i), 0);
        }
        return String.join(SEPARATOR[0], strings);
    }

    private String convert(Object field, SeaTunnelDataType<?> fieldType, int level) {
        if (field == null) {
            return "";
        }
        switch (fieldType.getSqlType()) {
            case DOUBLE:
            case FLOAT:
            case INT:
            case STRING:
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case BIGINT:
            case DECIMAL:
                return field.toString();
            case DATE:
                return String.valueOf(
                        ((LocalDate) field)
                                .atStartOfDay(ZoneOffset.UTC)
                                .toInstant()
                                .toEpochMilli());
                // Unsupported TIME:
            case TIMESTAMP:
                return String.valueOf(
                        ((LocalDateTime) field).toInstant(ZoneOffset.UTC).toEpochMilli());
            case NULL:
                return "";
            case BYTES:
                return new String((byte[]) field);
            case ARRAY:
                SeaTunnelDataType<?> elementType = ((ArrayType<?, ?>) fieldType).getElementType();
                return Arrays.stream((Object[]) field)
                        .map(f -> convert(f, elementType, level + 1))
                        .collect(Collectors.joining(SEPARATOR[level + 1]));
            case MAP:
                SeaTunnelDataType<?> keyType = ((MapType<?, ?>) fieldType).getKeyType();
                SeaTunnelDataType<?> valueType = ((MapType<?, ?>) fieldType).getValueType();
                return ((Map<Object, Object>) field)
                        .entrySet().stream()
                                .map(
                                        entry ->
                                                String.join(
                                                        SEPARATOR[level + 2],
                                                        convert(entry.getKey(), keyType, level + 1),
                                                        convert(
                                                                entry.getValue(),
                                                                valueType,
                                                                level + 1)))
                                .collect(Collectors.joining(SEPARATOR[level + 1]));
            case ROW:
                Object[] fields = ((SeaTunnelRow) field).getFields();
                String[] strings = new String[fields.length];
                for (int i = 0; i < fields.length; i++) {
                    strings[i] =
                            convert(
                                    fields[i],
                                    ((SeaTunnelRowType) fieldType).getFieldType(i),
                                    level + 1);
                }
                return String.join(SEPARATOR[level + 1], strings);
            default:
                throw new GcsConnectorException(
                        CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                        String.format(
                                "SeaTunnel format text not supported for parsing this type [%s]",
                                fieldType.getSqlType()));
        }
    }
}
