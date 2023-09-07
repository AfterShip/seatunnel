package org.apache.seatunnel.connectors.seatunnel.bigquery.serialize;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.bigquery.enums.LogicalTypeEnum;
import org.apache.seatunnel.connectors.seatunnel.bigquery.exception.BigQueryConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.bigquery.exception.BigQueryConnectorException;

import org.apache.avro.Conversions;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.generic.GenericData;
import org.apache.commons.lang3.StringUtils;

import com.google.common.collect.Lists;

import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * @author ah.he@aftership.com
 * @version 1.0
 * @date 2023/8/8 19:03
 */
public class BigQueryDeserializer implements SeaTunnelRowDeserializer {

    public BigQueryDeserializer() {}

    @Override
    public SeaTunnelRow deserialize(BigQueryRecord record) {
        return new SeaTunnelRow(convert(record.getRecord()));
    }

    private Object[] convert(GenericData.Record record) {
        Schema recordSchema = record.getSchema();
        List<Schema.Field> fields = recordSchema.getFields();

        Object[] values = new Object[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            Schema.Field field = fields.get(i);
            String fieldName = field.name();
            Object fieldValue = record.get(fieldName);
            Schema fieldSchema = field.schema();

            values[i] = convertValue(field, fieldValue, fieldSchema);
        }
        return values;
    }

    /** Convert value */
    private Object convertValue(Schema.Field field, Object fieldValue, Schema fieldSchema) {
        if (fieldValue == null) {
            return null;
        }

        if (!isLogicalType(fieldSchema)) {
            Schema.Type fieldType = fieldSchema.getType();
            switch (fieldType) {
                case UNION:
                    return convertUnion(field, fieldValue, fieldSchema.getTypes());
                case RECORD:
                    GenericData.Record record = (GenericData.Record) fieldValue;
                    return new SeaTunnelRow(convert(record));
                case ARRAY:
                    return convertArray(field, fieldValue, fieldSchema.getElementType());
                case NULL:
                    return null;
                case STRING:
                    return fieldValue.toString();
                case BYTES:
                    return convertBytes(fieldValue);
                case INT:
                case LONG:
                case FLOAT:
                case DOUBLE:
                case BOOLEAN:
                    return fieldValue;
                default:
                    throw new BigQueryConnectorException(
                            CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                            "data type in array is not supported: " + fieldType);
            }
        } else {
            return convertLogicalValue(fieldValue, fieldSchema);
        }
    }

    /** Convert logical type value */
    private Object convertLogicalValue(Object fieldValue, Schema fieldSchema) {
        if (fieldValue == null) {
            return null;
        }

        try {
            LogicalType logicalType = fieldSchema.getLogicalType();
            if (isLogicalType(fieldSchema)) {
                // Avro does not support datetime(logical type)，we should re-build it.
                logicalType =
                        logicalType != null
                                ? logicalType
                                : new LogicalType(fieldSchema.getProp("logicalType"));
                LogicalTypeEnum logicalTypeEnum = LogicalTypeEnum.fromToken(logicalType.getName());
                switch (logicalTypeEnum) {
                    case DATE:
                        // date will be in yyyy-mm-dd format
                        return new TimeConversions.DateConversion()
                                .fromInt((Integer) fieldValue, fieldSchema, logicalType);
                    case TIME_MILLIS:
                        // time will be in hh:mm:ss format
                        return new TimeConversions.TimeMillisConversion()
                                .fromInt((Integer) fieldValue, fieldSchema, logicalType);
                    case TIME_MICROS:
                        // time will be in hh:mm:ss format
                        return new TimeConversions.TimeMicrosConversion()
                                .fromLong((Long) fieldValue, fieldSchema, logicalType);
                    case TIMESTAMP_MILLIS:
                        return LocalDateTime.ofInstant(
                                new TimeConversions.TimestampMillisConversion()
                                        .fromLong((Long) fieldValue, fieldSchema, logicalType),
                                ZoneId.of("UTC"));
                    case TIMESTAMP_MICROS:
                        return LocalDateTime.ofInstant(
                                new TimeConversions.TimestampMicrosConversion()
                                        .fromLong((Long) fieldValue, fieldSchema, logicalType),
                                ZoneId.of("UTC"));
                    case DATETIME:
                        return LocalDateTime.parse(fieldValue.toString());
                    case LOCAL_TIMESTAMP_MILLIS:
                        return new TimeConversions.LocalTimestampMillisConversion()
                                .fromLong((Long) fieldValue, fieldSchema, logicalType);
                    case LOCAL_TIMESTAMP_MICROS:
                        return new TimeConversions.LocalTimestampMicrosConversion()
                                .fromLong((Long) fieldValue, fieldSchema, logicalType);
                    case DECIMAL:
                        return new Conversions.DecimalConversion()
                                .fromBytes((ByteBuffer) fieldValue, fieldSchema, logicalType);
                    default:
                        throw new BigQueryConnectorException(
                                CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                                "data type is not supported: " + logicalType);
                }
            }
        } catch (ArithmeticException e) {
            throw new BigQueryConnectorException(
                    BigQueryConnectorErrorCode.UNSUPPORTED,
                    String.format(
                            "Field type %s has value that is too large.", fieldSchema.getType()));
        }
        return null;
    }

    /**
     * Check if the schema is logical type Avro does not support datetime(logical type), but we can
     * check the logical type from prop
     *
     * @param schema
     * @return
     */
    private static Boolean isLogicalType(Schema schema) {
        return schema.getLogicalType() != null
                || StringUtils.isNotBlank(schema.getProp("logicalType"));
    }

    private Object[] convertArray(Schema.Field field, Object values, Schema elementSchema) {
        List<Object> output;
        if (values instanceof Collection) {
            Collection<Object> valuesList = (Collection<Object>) values;
            output = new ArrayList<>(valuesList.size());
            for (Object value : valuesList) {
                output.add(convertValue(field, value, elementSchema));
            }
        } else {
            int length = Array.getLength(values);
            output = Lists.newArrayListWithCapacity(length);
            for (int i = 0; i < length; i++) {
                output.add(convertValue(field, Array.get(values, i), elementSchema));
            }
        }
        return output.toArray();
    }

    private Object convertUnion(Schema.Field field, Object value, List<Schema> schemas) {
        boolean isNullable = false;
        for (Schema possibleSchema : schemas) {
            if (possibleSchema.getType() == Schema.Type.NULL) {
                isNullable = true;
                if (value == null) {
                    return value;
                }
            } else {
                return convertValue(field, value, possibleSchema);
            }
        }
        if (isNullable) {
            return null;
        }
        throw new BigQueryConnectorException(
                BigQueryConnectorErrorCode.UNSUPPORTED,
                String.format(
                        "unable to determine union type. value: %s, schemas: %s", value, schemas));
    }

    private Object convertBytes(Object fieldValue) {
        ByteBuffer byteBuffer = (ByteBuffer) fieldValue;
        byte[] bytes = new byte[byteBuffer.remaining()];
        byteBuffer.get(bytes);
        return bytes;
    }
}
