package org.apache.seatunnel.connectors.seatunnel.gcs.sink.json;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonGenerator;

import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.gcs.exception.GcsConnectorException;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;

public class RowToJson {

    /** Writes object and writes to json writer. */
    public static void write(
            JsonGenerator generator, String name, Object object, SeaTunnelDataType rowType)
            throws IOException {
        write(generator, name, false, object, rowType);
    }

    /** Writes object and writes to json writer. */
    private static void write(
            JsonGenerator generator,
            String name,
            boolean isArrayItem,
            Object object,
            SeaTunnelDataType rowType)
            throws IOException {
        SqlType sqlType = rowType.getSqlType();
        switch (sqlType) {
            case NULL:
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case BOOLEAN:
            case BYTES:
            case DATE:
            case DECIMAL:
                // case TIME: not support now
            case TIMESTAMP:
                writeSimpleTypes(generator, name, isArrayItem, object, rowType);
                break;
            case STRING:
                writeString(generator, name, isArrayItem, object, rowType);
                break;
            case ARRAY:
                writeArray(generator, name, object, rowType);
                break;
            case MAP:
                generator.writeFieldName(name);
                processMap(generator, object, rowType);
                break;
            case ROW:
                generator.writeFieldName(name);
                if (object == null) {
                    generator.writeNull();
                } else {
                    processRecord(generator, object, (SeaTunnelRowType) rowType);
                }
                break;
            default:
                throw new IllegalStateException(
                        String.format("Field '%s' is of unsupported type '%s'", name, sqlType));
        }
    }

    /** Writes simple types to json writer. */
    private static void writeSimpleTypes(
            JsonGenerator generator,
            String name,
            boolean isArrayItem,
            Object object,
            SeaTunnelDataType rowType)
            throws IOException {
        if (!isArrayItem) {
            generator.writeFieldName(name);
        }

        if (object == null) {
            generator.writeNull();
            return;
        }
        SqlType sqlType = rowType.getSqlType();
        switch (sqlType) {
            case NULL:
                generator.writeNull();
                break;
            case TINYINT:
                generator.writeNumber((Byte) object);
                break;
            case SMALLINT:
                generator.writeNumber((Short) object);
                break;
            case INT:
                generator.writeNumber((Integer) object);
                break;
            case BIGINT:
                generator.writeNumber((Long) object);
                break;
            case FLOAT:
                generator.writeNumber((Float) object);
                break;
            case DOUBLE:
                generator.writeNumber((Double) object);
                break;
            case DECIMAL:
                generator.writeNumber((BigDecimal) object);
                break;
            case BOOLEAN:
                generator.writeBoolean((Boolean) object);
                break;
            case STRING:
                generator.writeString(object.toString());
                break;
            case BYTES:
                if (object instanceof ByteBuffer) {
                    encodeBytes(generator, (ByteBuffer) object);
                } else if (object.getClass().isArray()
                        && object.getClass().getComponentType().equals(byte.class)) {
                    byte[] bytes = (byte[]) object;
                    writeBytes(bytes, 0, bytes.length, generator);
                } else {
                    throw new IOException(
                            "Expects either ByteBuffer or byte[]. Got " + object.getClass());
                }
                break;
            case DATE:
            case TIME:
            case TIMESTAMP:
                generator.writeString(object.toString());
                break;
            default:
                throw new IllegalStateException(
                        String.format("Field '%s' is of unsupported type '%s'", name, sqlType));
        }
    }

    /** 根据标志 writeAsObject，判断是否需要将 string 类型转为 object */
    private static void writeString(
            JsonGenerator generator,
            String name,
            boolean isArrayItem,
            Object object,
            SeaTunnelDataType rowType)
            throws IOException {
        writeSimpleTypes(generator, name, isArrayItem, object, rowType);
    }

    private static void encodeBytes(JsonGenerator generator, ByteBuffer buffer) throws IOException {
        if (buffer.hasArray()) {
            writeBytes(
                    buffer.array(),
                    buffer.arrayOffset() + buffer.position(),
                    buffer.remaining(),
                    generator);
        } else {
            byte[] buf = new byte[buffer.remaining()];
            buffer.mark();
            buffer.get(buf);
            buffer.reset();
            writeBytes(buf, 0, buf.length, generator);
        }
    }

    public static void writeBytes(byte[] bytes, int off, int len, JsonGenerator generator)
            throws IOException {
        generator.writeStartArray();
        for (int i = off; i < off + len; i++) {
            generator.writeNumber(bytes[i]);
        }
        generator.writeEndArray();
    }

    private static void writeArray(
            JsonGenerator generator, String name, Object value, SeaTunnelDataType rowType)
            throws IOException {
        if (value == null) {
            throw new RuntimeException(
                    String.format(
                            "Field '%s' is of value null, which is not a valid value for BigQuery type array.",
                            name));
        }

        Collection collection;
        if (value instanceof Collection) {
            collection = (Collection) value;
        } else if (value instanceof Object[]) {
            collection = Arrays.asList((Object[]) value);
        } else {
            throw new IllegalArgumentException(
                    String.format(
                            "A value for the field '%s' is of type '%s' when it is expected to be a Collection or array.",
                            name, value.getClass().getSimpleName()));
        }

        SeaTunnelDataType elementType = ((ArrayType) rowType).getElementType();
        SqlType elementSqlType = elementType.getSqlType();

        generator.writeFieldName(name);
        generator.writeStartArray();

        for (Object element : collection) {
            if (element == null) {
                throw new IllegalArgumentException(
                        String.format(
                                "Field '%s' contains null values in its array, "
                                        + "which is not allowed.",
                                name));
            }
            if (element instanceof SeaTunnelRow) {
                SeaTunnelRow record = (SeaTunnelRow) element;
                processRecord(generator, record, (SeaTunnelRowType) elementType);
            } else {
                write(generator, name, true, element, elementType);
            }
        }
        generator.writeEndArray();
    }

    private static void processRecord(
            JsonGenerator generator, Object object, SeaTunnelRowType rowType) throws IOException {
        generator.writeStartObject();
        for (int i = 0; i < rowType.getFieldNames().length; i++) {
            String subFieldName = rowType.getFieldName(i);
            SeaTunnelDataType subFieldType = rowType.getFieldType(i);
            write(
                    generator,
                    subFieldName,
                    false,
                    ((SeaTunnelRow) object).getField(i),
                    subFieldType);
        }
        generator.writeEndObject();
    }

    private static void processMap(
            JsonGenerator generator, Object object, SeaTunnelDataType rowType) throws IOException {
        if (!Objects.equals(SqlType.STRING, ((MapType) rowType).getKeyType().getSqlType())) {
            throw new GcsConnectorException(
                    CommonErrorCode.UNSUPPORTED_DATA_TYPE, "Map key type must be STRING");
        }
        generator.writeStartObject();
        Map<String, ?> mapData = (Map) object;
        SeaTunnelDataType valueType = ((MapType) rowType).getValueType();
        for (Map.Entry<String, ?> entry : mapData.entrySet()) {
            String fieldName = entry.getKey();
            write(generator, fieldName, false, entry.getValue(), valueType);
        }
        generator.writeEndObject();
    }
}
