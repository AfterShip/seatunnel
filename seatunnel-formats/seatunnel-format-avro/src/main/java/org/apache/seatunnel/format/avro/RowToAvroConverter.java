package org.apache.seatunnel.format.avro;

import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.DatumWriter;
import org.apache.seatunnel.api.table.type.*;
import org.apache.seatunnel.format.avro.exception.AvroFormatErrorCode;
import org.apache.seatunnel.format.avro.exception.SeaTunnelAvroFormatException;

import java.io.Serializable;
import java.math.BigDecimal;
import java.nio.ByteBuffer;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

public class RowToAvroConverter implements Serializable {

    private static final long serialVersionUID = -576124379280229724L;

    private final Schema schema;
    private final SeaTunnelRowType rowType;
    private final DatumWriter<GenericRecord> writer;

    public RowToAvroConverter(SeaTunnelRowType rowType) {
        this.schema = buildAvroSchemaWithRowType(rowType);
        this.rowType = rowType;
        this.writer = createWriter();
    }

    private DatumWriter<GenericRecord> createWriter() {
        GenericDatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        datumWriter.getData().addLogicalTypeConversion(new Conversions.DecimalConversion());
        datumWriter.getData().addLogicalTypeConversion(new TimeConversions.DateConversion());
        datumWriter
                .getData()
                .addLogicalTypeConversion(new TimeConversions.TimestampMillisConversion());
        return datumWriter;
    }

    public Schema getSchema() {
        return schema;
    }

    public DatumWriter<GenericRecord> getWriter() {
        return writer;
    }

    public GenericRecord convertRowToGenericRecord(SeaTunnelRow element) {
        GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        String[] fieldNames = rowType.getFieldNames();
        for (int i = 0; i < fieldNames.length; i++) {
            String fieldName = rowType.getFieldName(i);
            Object value = element.getField(i);
            builder.set(fieldName.toLowerCase(), resolveObject(value, rowType.getFieldType(i)));
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
        return Schema.createRecord("SeaTunnelRecord", null, null, false, fields);
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
                BasicType<?> elementType = (BasicType<?>) ((ArrayType<?, ?>) seaTunnelDataType).getElementType();
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
                throw new SeaTunnelAvroFormatException(
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
                //                BasicType<?> basicType = ((ArrayType<?, ?>)
                // seaTunnelDataType).getElementType();
                //                return Util.convertArray((Object[]) data, basicType);
                SeaTunnelDataType<?> basicType = ((ArrayType<?, ?>) seaTunnelDataType).getElementType();
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
                return (dateTime)
                        .toInstant(ZoneOffset.of(ZoneOffset.systemDefault().getId()))
                        .toEpochMilli();
            default:
                String errorMsg =
                        String.format(
                                "SeaTunnel avro format is not supported for this data type [%s]",
                                seaTunnelDataType.getSqlType());
                throw new SeaTunnelAvroFormatException(
                        AvroFormatErrorCode.UNSUPPORTED_DATA_TYPE, errorMsg);
        }
    }
}

