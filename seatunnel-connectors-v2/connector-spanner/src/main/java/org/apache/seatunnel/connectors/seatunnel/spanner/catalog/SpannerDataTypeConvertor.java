package org.apache.seatunnel.connectors.seatunnel.spanner.catalog;

import com.google.auto.service.AutoService;
import com.google.cloud.spanner.Type;
import org.apache.seatunnel.api.table.catalog.DataTypeConvertException;
import org.apache.seatunnel.api.table.catalog.DataTypeConvertor;
import org.apache.seatunnel.api.table.type.*;
import org.apache.seatunnel.connectors.seatunnel.spanner.constants.SpannerConstants;

import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * todo implement SpannerDataTypeConvertor
 *
 * @author: gf.xu
 * @email: gf.xu@aftership.com
 * @date: 2023/9/7 10:57
 */
@AutoService(DataTypeConvertor.class)
public class SpannerDataTypeConvertor implements DataTypeConvertor<Type.Code> {

    @Override
    public SeaTunnelDataType<?> toSeaTunnelType(String connectorDataType) {
        checkNotNull(connectorDataType, "connectorDataType can not be null");
        Type.Code code = Type.Code.valueOf(connectorDataType);
//        switch (code) {
//            case BOOL:
//                return BasicType.BOOLEAN_TYPE;
//            case INT64:
//                return BasicType.LONG_TYPE;
//            case FLOAT64:
//                return BasicType.DOUBLE_TYPE;
//            case STRING:
//                return BasicType.STRING_TYPE;
//            case BYTES:
//                return PrimitiveByteArrayType.INSTANCE;
//            case DATE:
//                return LocalTimeType.LOCAL_DATE_TYPE;
//            case TIMESTAMP:
//                return LocalTimeType.LOCAL_DATE_TIME_TYPE;
//            case NUMERIC:
//                return new DecimalType(code.);
//            case ARRAY:
//                return ArrayType.ARRAY;
//            case STRUCT:
//                return SeaTunnelDataType.STRUCT;
//            default:
//                throw new DataTypeConvertException(
//                        SpannerConstants.IDENTIFIER,
//                        "Unsupported connector data type: " + connectorDataType);
//        }
        return null;
    }

    @Override
    public SeaTunnelDataType<?> toSeaTunnelType(
            Type.Code connectorDataType,
            Map<String, Object> dataTypeProperties) throws DataTypeConvertException {
        return null;
    }

    @Override
    public Type.Code toConnectorType(
            SeaTunnelDataType<?> seaTunnelDataType,
            Map<String, Object> dataTypeProperties) throws DataTypeConvertException {
        return null;
    }

    @Override
    public String getIdentity() {
        return SpannerConstants.IDENTIFIER;
    }
}
