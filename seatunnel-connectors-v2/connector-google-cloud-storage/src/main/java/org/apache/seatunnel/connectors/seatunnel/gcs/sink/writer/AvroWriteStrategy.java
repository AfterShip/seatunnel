package org.apache.seatunnel.connectors.seatunnel.gcs.sink.writer;

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.gcs.config.FileSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.gcs.exception.AvroFormatErrorCode;
import org.apache.seatunnel.connectors.seatunnel.gcs.exception.GcsConnectorException;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

@Slf4j
public class AvroWriteStrategy extends AbstractWriteStrategy {

    private final LinkedHashMap<String, DataFileWriter<GenericRecord>> beingWrittenOutputStream;
    private Schema schema;

    public AvroWriteStrategy(FileSinkConfig fileSinkConfig) {
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
        DataFileWriter writer = getOrCreateDatumWriter(filePath);
        GenericRecord record = convertRowToGenericRecord(schema, seaTunnelRow);
        try {
            writer.append(record);
        } catch (Exception e) {
            throw new GcsConnectorException(
                    CommonErrorCode.FILE_OPERATION_FAILED,
                    String.format("Open file output stream [%s] failed", filePath),
                    e);
        }
    }

    private DataFileWriter getOrCreateDatumWriter(String filePath) {
        if (schema == null) {
            schema = buildAvroSchemaWithRowType(seaTunnelRowType);
        }
        DataFileWriter<GenericRecord> writer = this.beingWrittenOutputStream.get(filePath);
        if (writer == null) {
            try {
                GenericDatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
                DataFileWriter<GenericRecord> newWriter =
                        new DataFileWriter<>(datumWriter)
                                .create(schema, fileSystemUtils.getOutputStream(filePath));
                this.beingWrittenOutputStream.put(filePath, newWriter);
                return newWriter;
            } catch (IOException e) {
                String errorMsg = String.format("Get avro writer for file [%s] error", filePath);
                throw new GcsConnectorException(
                        CommonErrorCode.WRITER_OPERATION_FAILED, errorMsg, e);
            }
        }
        return writer;
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

    public GenericRecord convertRowToGenericRecord(Schema schema, SeaTunnelRow element) {
        GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        String[] fieldNames = this.seaTunnelRowType.getFieldNames();
        for (int i = 0; i < fieldNames.length; i++) {
            String fieldName = this.seaTunnelRowType.getFieldName(i);
            Object value = element.getField(i);
            builder.set(
                    fieldName.toLowerCase(),
                    resolveObject(value, this.seaTunnelRowType.getFieldType(i)));
        }
        return builder.build();
    }

    private Schema buildAvroSchemaWithRowType(SeaTunnelRowType seaTunnelRowType) {
        List<Schema.Field> fields = new ArrayList<>();
        SeaTunnelDataType<?>[] fieldTypes = seaTunnelRowType.getFieldTypes();
        String[] fieldNames = seaTunnelRowType.getFieldNames();
        for (int i = 0; i < fieldNames.length; i++) {
            fields.add(generateField(fieldNames[i], fieldTypes[i]));
        }
        return Schema.createRecord("etlSchemaBody", null, null, false, fields);
    }

    private Schema.Field generateField(String fieldName, SeaTunnelDataType<?> seaTunnelDataType) {
        return new Schema.Field(
                fieldName,
                seaTunnelDataType2AvroDataType(fieldName, seaTunnelDataType),
                null,
                null);
    }

    private Schema seaTunnelDataType2AvroDataType(
            String fieldName, SeaTunnelDataType<?> seaTunnelDataType) {
        switch (seaTunnelDataType.getSqlType()) {
            case STRING:
                return Schema.create(Schema.Type.STRING);
            case BYTES:
                return Schema.create(Schema.Type.BYTES);
            case TINYINT:
            case SMALLINT:
            case INT:
                return Schema.create(Schema.Type.INT);
            case BIGINT:
                return Schema.create(Schema.Type.LONG);
            case FLOAT:
                return Schema.create(Schema.Type.FLOAT);
            case DOUBLE:
                return Schema.create(Schema.Type.DOUBLE);
            case BOOLEAN:
                return Schema.create(Schema.Type.BOOLEAN);
            case MAP:
                SeaTunnelDataType<?> valueType = ((MapType<?, ?>) seaTunnelDataType).getValueType();
                return Schema.createMap(seaTunnelDataType2AvroDataType(fieldName, valueType));
            case ARRAY:
                SeaTunnelDataType<?> elementType =
                        ((ArrayType<?, ?>) seaTunnelDataType).getElementType();
                return Schema.createArray(seaTunnelDataType2AvroDataType(fieldName, elementType));
            case ROW:
                SeaTunnelDataType<?>[] fieldTypes =
                        ((SeaTunnelRowType) seaTunnelDataType).getFieldTypes();
                String[] fieldNames = ((SeaTunnelRowType) seaTunnelDataType).getFieldNames();
                List<Schema.Field> subField = new ArrayList<>();
                for (int i = 0; i < fieldNames.length; i++) {
                    subField.add(generateField(fieldNames[i], fieldTypes[i]));
                }
                return Schema.createRecord(fieldName, null, null, false, subField);
            case DECIMAL:
                int precision = ((DecimalType) seaTunnelDataType).getPrecision();
                int scale = ((DecimalType) seaTunnelDataType).getScale();
                LogicalTypes.Decimal decimal = LogicalTypes.decimal(precision, scale);
                return decimal.addToSchema(Schema.create(Schema.Type.BYTES));
            case TIMESTAMP:
                return LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
            case DATE:
                return LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
            case NULL:
                return Schema.create(Schema.Type.NULL);
            default:
                String errorMsg =
                        String.format(
                                "SeaTunnel avro format is not supported for this data type [%s]",
                                seaTunnelDataType.getSqlType());
                throw new GcsConnectorException(
                        AvroFormatErrorCode.UNSUPPORTED_DATA_TYPE, errorMsg);
        }
    }

    private Object resolveObject(Object data, SeaTunnelDataType<?> seaTunnelDataType) {
        if (data == null) {
            return null;
        }
        switch (seaTunnelDataType.getSqlType()) {
            case STRING:
            case SMALLINT:
            case INT:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case BOOLEAN:
            case MAP:
                return data;
            case TINYINT:
                Class<?> typeClass = seaTunnelDataType.getTypeClass();
                if (typeClass == Byte.class) {
                    if (data instanceof Byte) {
                        Byte aByte = (Byte) data;
                        return Byte.toUnsignedInt(aByte);
                    }
                }
                return data;
            case DECIMAL:
                BigDecimal decimal;
                decimal = (BigDecimal) data;
                return ByteBuffer.wrap(decimal.unscaledValue().toByteArray());
            case DATE:
                LocalDate localDate = (LocalDate) data;
                return localDate.toEpochDay();
            case BYTES:
                return ByteBuffer.wrap((byte[]) data);
            case ARRAY:
                SeaTunnelDataType<?> basicType =
                        ((ArrayType<?, ?>) seaTunnelDataType).getElementType();
                List<Object> records = new ArrayList<>(((Object[]) data).length);
                for (Object object : (Object[]) data) {
                    Object resolvedObject = resolveObject(object, basicType);
                    records.add(resolvedObject);
                }
                return records;
            case ROW:
                SeaTunnelRow seaTunnelRow = (SeaTunnelRow) data;
                SeaTunnelDataType<?>[] fieldTypes =
                        ((SeaTunnelRowType) seaTunnelDataType).getFieldTypes();
                String[] fieldNames = ((SeaTunnelRowType) seaTunnelDataType).getFieldNames();
                Schema recordSchema =
                        buildAvroSchemaWithRowType((SeaTunnelRowType) seaTunnelDataType);
                GenericRecordBuilder recordBuilder = new GenericRecordBuilder(recordSchema);
                for (int i = 0; i < fieldNames.length; i++) {
                    recordBuilder.set(
                            fieldNames[i].toLowerCase(),
                            resolveObject(seaTunnelRow.getField(i), fieldTypes[i]));
                }
                return recordBuilder.build();
            case TIMESTAMP:
                LocalDateTime dateTime = (LocalDateTime) data;
                return (dateTime).toInstant(ZoneOffset.UTC).toEpochMilli();
            default:
                String errorMsg =
                        String.format(
                                "SeaTunnel avro format is not supported for this data type [%s]",
                                seaTunnelDataType.getSqlType());
                throw new GcsConnectorException(
                        AvroFormatErrorCode.UNSUPPORTED_DATA_TYPE, errorMsg);
        }
    }
}
