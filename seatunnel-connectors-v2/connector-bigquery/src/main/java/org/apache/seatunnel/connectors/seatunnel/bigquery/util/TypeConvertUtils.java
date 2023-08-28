package org.apache.seatunnel.connectors.seatunnel.bigquery.util;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.StandardSQLTypeName;
import org.apache.seatunnel.api.table.type.*;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.bigquery.exception.BigQueryConnectorException;

/**
 * @author ah.he@aftership.com
 * @version 1.0
 * @date 2023/8/9 10:54
 */
public class TypeConvertUtils {

    public static SeaTunnelDataType<?> convert(Field field) {
        StandardSQLTypeName fieldType = field.getType().getStandardType();
        Field.Mode fieldMode = field.getMode();

        // array type
        if (fieldMode.equals(Field.Mode.REPEATED)) {
            switch (fieldType) {
                case BYTES:
                    return ArrayType.BYTE_ARRAY_TYPE;
                case STRING:
                case JSON:
                    return ArrayType.STRING_ARRAY_TYPE;
                case INT64:
                    return ArrayType.LONG_ARRAY_TYPE;
                case FLOAT64:
                    return ArrayType.DOUBLE_ARRAY_TYPE;
//                case NUMERIC:
//                case BIGNUMERIC:
//                    return ArrayType.STRING_ARRAY_TYPE;
                case BOOL:
                    return ArrayType.BOOLEAN_ARRAY_TYPE;
//                case DATE:
//                    return LocalTimeType.LOCAL_DATE_TYPE;
//                case TIME:
//                    return LocalTimeType.LOCAL_TIME_TYPE;
//                case TIMESTAMP:
//                case DATETIME:
//                    return LocalTimeType.LOCAL_DATE_TIME_TYPE;
//                case STRUCT:
//                    return ArrayType.STRING_ARRAY_TYPE;
                default:
                    throw new BigQueryConnectorException(
                            CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                            "data type in array is not supported: " + fieldType);
            }
        } else {
            switch (fieldType) {
                case BYTES:
                    return BasicType.BYTE_TYPE;
                case STRING:
                case JSON:
                    return BasicType.STRING_TYPE;
                case INT64:
                    return BasicType.LONG_TYPE;
                case FLOAT64:
                    return BasicType.DOUBLE_TYPE;
                case NUMERIC:
                case BIGNUMERIC:
                    return new DecimalType(BigQueryTypeSize.Numeric.PRECISION, BigQueryTypeSize.Numeric.SCALE);
                case BOOL:
                    return BasicType.BOOLEAN_TYPE;
                case DATE:
                    return LocalTimeType.LOCAL_DATE_TYPE;
                case TIMESTAMP:
                case DATETIME:
                    return LocalTimeType.LOCAL_DATE_TIME_TYPE;
                case STRUCT:
                    FieldList subFields = field.getSubFields();
                    int fieldsNum = subFields.size();
                    SeaTunnelDataType<?>[] seaTunnelDataTypes = new SeaTunnelDataType[fieldsNum];
                    String[] fieldNames = new String[fieldsNum];

                    for (int i = 0; i < fieldsNum; i++) {
                        Field subField = subFields.get(i);
                        fieldNames[i] = subField.getName();
                        seaTunnelDataTypes[i] = TypeConvertUtils.convert(subField);
                    }

                    return new SeaTunnelRowType(fieldNames, seaTunnelDataTypes);
                case TIME:
                    // spark sql not support time type
//                    return LocalTimeType.LOCAL_TIME_TYPE;
                default:
                    throw new BigQueryConnectorException(
                            CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                            "data type is not supported: " + fieldType);
            }
        }
    }
}
