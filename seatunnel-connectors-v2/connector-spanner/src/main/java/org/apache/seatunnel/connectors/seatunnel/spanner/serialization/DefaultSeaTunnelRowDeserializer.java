package org.apache.seatunnel.connectors.seatunnel.spanner.serialization;

import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Type;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author: gf.xu
 * @email: gf.xu@aftership.com
 * @date: 2023/9/7 10:49
 */
public class DefaultSeaTunnelRowDeserializer implements SeaTunnelRowDeserializer {

    private SeaTunnelRowType rowTypeInfo;

    public DefaultSeaTunnelRowDeserializer(SeaTunnelRowType rowTypeInfo) {
        this.rowTypeInfo = rowTypeInfo;
    }

    @Override
    public SeaTunnelRow deserialize(ResultSet resultSet) {
        List<Object> values = new ArrayList<>();
        for (int i = 0; i < rowTypeInfo.getTotalFields(); i++) {
            String fieldName = rowTypeInfo.getFieldNames()[i];
            Type columnType = resultSet.getColumnType(fieldName);
            Type.Code code = columnType.getCode();

            if (columnType == null || (resultSet.isNull(fieldName) && code != Type.Code.ARRAY)) {
                values.add(null);
                continue;
            }
            switch (code) {
                case BOOL:
                    values.add(resultSet.getBoolean(fieldName));
                    break;
                case INT64:
                    values.add(resultSet.getLong(fieldName));
                    break;
                case FLOAT64:
                    values.add(resultSet.getDouble(fieldName));
                    break;
                case STRING:
                    values.add(resultSet.getString(fieldName));
                    break;
                case BYTES:
                    ByteArray byteArray = resultSet.getBytes(fieldName);
                    values.add(byteArray.toByteArray());
                    break;
                case DATE:
                    // spanner DATE is a date without time zone. so create LocalDate from spanner
                    // DATE
                    Date spannerDate = resultSet.getDate(fieldName);
                    values.add(
                            LocalDate.of(
                                    spannerDate.getYear(),
                                    spannerDate.getMonth(),
                                    spannerDate.getDayOfMonth()));
                    break;
                case TIMESTAMP:
                    Timestamp spannerTs = resultSet.getTimestamp(fieldName);
                    // Spanner TIMESTAMP supports nano second level precision, however, cdap schema
                    // only supports
                    // microsecond level precision.
                    // TODO 时间精度是否能够满足
                    LocalDateTime localDateTime =
                            LocalDateTime.ofEpochSecond(
                                    spannerTs.getSeconds(), spannerTs.getNanos(), ZoneOffset.UTC);
                    values.add(localDateTime);
                    break;
                case JSON:
                    values.add(resultSet.getJson(fieldName));
                    break;
                case NUMERIC:
                    BigDecimal resultSetBigDecimal = resultSet.getBigDecimal(fieldName);
                    values.add(resultSetBigDecimal);
                    break;
                case ARRAY:
                    List<?> arrayValues =
                            transformArrayToList(
                                    resultSet, fieldName, columnType.getArrayElementType());
                    values.add(arrayValues.toArray());
                    break;
            }
        }

        SeaTunnelRow row = new SeaTunnelRow(values.toArray(values.toArray(new Object[0])));
        row.setRowKind(RowKind.INSERT);
        return row;
    }

    private List<?> transformArrayToList(
            ResultSet resultSet, String fieldName, Type arrayElementType) {
        if (resultSet.isNull(fieldName)) {
            return Collections.emptyList();
        }

        switch (arrayElementType.getCode()) {
            case BOOL:
                return resultSet.getBooleanList(fieldName);
            case INT64:
                return resultSet.getLongList(fieldName);
            case FLOAT64:
                return resultSet.getDoubleList(fieldName);
            case STRING:
                return resultSet.getStringList(fieldName);
            case BYTES:
                return resultSet.getBytesList(fieldName).stream()
                        .map(byteArray -> byteArray == null ? null : byteArray.toByteArray())
                        .collect(Collectors.toList());
            case DATE:
                // spanner DATE is a date without time zone. so create LocalDate from spanner DATE
                return resultSet.getDateList(fieldName).stream()
                        .map(this::convertSpannerDate)
                        .collect(Collectors.toList());
            case TIMESTAMP:
                // Spanner TIMESTAMP supports nano second level precision, however, cdap schema only
                // supports
                // microsecond level precision.
                // TODO 时间精度是否能够满足
                return resultSet.getTimestampList(fieldName).stream()
                        .map(this::convertSpannerTimestamp)
                        .collect(Collectors.toList());
            case JSON:
                return resultSet.getJsonList(fieldName);
            case NUMERIC:
                List<BigDecimal> resultSetBigDecimalList = resultSet.getBigDecimalList(fieldName);
                return new ArrayList<>(resultSetBigDecimalList);
            default:
                return Collections.emptyList();
        }
    }

    private LocalDate convertSpannerDate(Date spannerDate) {
        if (spannerDate == null) {
            return null;
        }

        return LocalDate.of(
                spannerDate.getYear(), spannerDate.getMonth(), spannerDate.getDayOfMonth());
    }

    private LocalDateTime convertSpannerTimestamp(Timestamp timestamp) {
        if (timestamp == null) {
            return null;
        }

        return LocalDateTime.ofEpochSecond(
                timestamp.getSeconds(), timestamp.getNanos(), ZoneOffset.UTC);
    }
}
