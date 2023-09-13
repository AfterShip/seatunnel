package org.apache.seatunnel.connectors.seatunnel.elasticsearch.serialize;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.common.utils.JsonUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * @author wq.pan on 2023/8/8
 * @className RowToJson @Description @Version: 1.0
 */
public class RowToJson {

    private static final Logger LOG = LoggerFactory.getLogger(RowToJson.class);

    public static final Set<SqlType> UNSUPPORTED_ARRAY_TYPES =
            ImmutableSet.of(SqlType.ARRAY, SqlType.MAP);

    /** Writes object and writes to json writer. */
    public static void write(
            JsonGenerator generator,
            String name,
            Object object,
            SeaTunnelDataType rowType,
            boolean writeAsObject)
            throws IOException {
        write(generator, name, false, object, rowType, writeAsObject);
    }

    /** Writes object and writes to json writer. */
    private static void write(
            JsonGenerator generator,
            String name,
            boolean isArrayItem,
            Object object,
            SeaTunnelDataType rowType,
            boolean writeAsObject)
            throws IOException {
        SqlType sqlType = rowType.getSqlType();
        switch (sqlType) {
            case NULL:
            case INT:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case BOOLEAN:
            case BYTES:
            case DATE:
                // case TIME: not support now
            case TIMESTAMP:
                writeSimpleTypes(generator, name, isArrayItem, object, rowType);
                break;
            case STRING:
                writeString(generator, name, isArrayItem, object, rowType, writeAsObject);
                break;
            case ARRAY:
                writeArray(generator, name, object, rowType);
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
            SeaTunnelDataType rowType,
            boolean writeAsObject)
            throws IOException {
        if (writeAsObject) {
            if (!isArrayItem) {
                generator.writeFieldName(name);
            }

            if (object == null) {
                generator.writeNull();
            } else if (!isValid(object.toString())) {
                // 与用户协商，如果不是标准的 json 格式，当做脏数据处理，直接写 null;
                LOG.warn(
                        "Filed name: {}, value: {}, value is not a standard json format string, assigned as null. ",
                        name,
                        object);
                generator.writeNull();
            } else {
                writeJsonObject(object, generator);
            }
        } else {
            writeSimpleTypes(generator, name, isArrayItem, object, rowType);
        }
    }

    /**
     * 判断字符串是否是标准的 json 格式
     *
     * @param json
     * @return
     */
    private static boolean isValid(String json) {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            objectMapper.readTree(json);
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    private static void writeJsonObject(Object object, JsonGenerator generator) throws IOException {
        JsonNode jsonNode = JsonUtils.stringToJsonNode(object.toString());
        if (jsonNode.isObject()) {
            // 如果是一个对象，遍历属性并将其写入到 JsonGenerator 中
            generator.writeStartObject();
            jsonNode.fields()
                    .forEachRemaining(
                            entry -> {
                                try {
                                    String key = entry.getKey();
                                    JsonNode value = entry.getValue();
                                    if (value.isValueNode()) {
                                        // 简单类型（字符串、数字、布尔值）直接写入
                                        if (value.isTextual()) {
                                            generator.writeStringField(key, value.asText());
                                        } else if (value.isNumber()) {
                                            generator.writeNumberField(key, value.asDouble());
                                        } else if (value.isBoolean()) {
                                            generator.writeBooleanField(key, value.asBoolean());
                                        }
                                    } else if (value.isArray()) {
                                        // 数组类型，递归处理每个元素
                                        generator.writeArrayFieldStart(key);
                                        for (JsonNode element : value) {
                                            if (element.isValueNode()) {
                                                if (element.isTextual()) {
                                                    generator.writeString(element.asText());
                                                } else if (element.isNumber()) {
                                                    generator.writeNumber(element.asDouble());
                                                } else if (element.isBoolean()) {
                                                    generator.writeBoolean(element.asBoolean());
                                                }
                                            } else if (element.isObject() || element.isArray()) {
                                                writeJsonObject(element, generator);
                                            }
                                        }
                                        generator.writeEndArray();
                                    } else if (value.isObject()) {
                                        // 对象类型，递归处理每个属性
                                        generator.writeFieldName(key);
                                        writeJsonObject(value, generator);
                                    }
                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                }
                            });
            generator.writeEndObject();
        } else if (jsonNode.isArray()) {
            // 如果是一个数组，遍历其中的元素并将其写入到 generator 中
            generator.writeStartArray();
            jsonNode.elements()
                    .forEachRemaining(
                            element -> {
                                try {
                                    if (element.isTextual()) {
                                        generator.writeString(element.asText());
                                    } else if (element.isNumber()) {
                                        generator.writeNumber(element.asDouble());
                                    } else if (element.isBoolean()) {
                                        generator.writeBoolean(element.asBoolean());
                                    } else if (element.isArray() || element.isObject()) {
                                        writeJsonObject(element, generator);
                                    }
                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                }
                            });
            generator.writeEndArray();
        }
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
            JsonGenerator generator, String name, @Nullable Object value, SeaTunnelDataType rowType)
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

        if (UNSUPPORTED_ARRAY_TYPES.contains(elementSqlType)) {
            throw new IllegalArgumentException(
                    String.format(
                            "Field '%s' is an array of '%s', " + "which is not a valid type.",
                            name, elementSqlType));
        }

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
                write(generator, name, true, element, elementType, false);
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
                    subFieldType,
                    false);
        }
        generator.writeEndObject();
    }

    private static ZonedDateTime getZonedDateTime(long ts, TimeUnit unit) {
        long mod = unit.convert(1, TimeUnit.SECONDS);
        int fraction = (int) (ts % mod);
        long tsInSeconds = unit.toSeconds(ts);
        // create an Instant with time in seconds and fraction which will be stored as nano seconds.
        Instant instant = Instant.ofEpochSecond(tsInSeconds, unit.toNanos(fraction));
        return ZonedDateTime.ofInstant(instant, ZoneId.ofOffset("UTC", ZoneOffset.UTC));
    }
}
