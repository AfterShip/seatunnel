package org.apache.seatunnel.connectors.seatunnel.spanner.sink;

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.spanner.common.BytesCounter;
import org.apache.seatunnel.connectors.seatunnel.spanner.config.SpannerParameters;
import org.apache.seatunnel.connectors.seatunnel.spanner.utils.SpannerUtil;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.Value;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.seatunnel.connectors.seatunnel.spanner.common.SourceSinkCounter.BYTES_WRITTEN;

/**
 * @author: gf.xu
 * @email: gf.xu@aftership.com
 * @date: 2023/8/25 14:29
 */
public class SpannerWriter extends AbstractSinkWriter<SeaTunnelRow, Void> {

    private final Spanner spanner;

    private final DatabaseClient databaseClient;

    private final SpannerParameters parameters;

    private final int batchSize;

    private final List<Mutation> mutations;

    private final SeaTunnelRowType seaTunnelRowType;

    private final BytesCounter counter;

    private SinkWriter.Context context;

    public SpannerWriter(
            SinkWriter.Context context,
            SpannerParameters parameters,
            SeaTunnelRowType seaTunnelRowType)
            throws IOException {
        String projectId = parameters.getProjectId();
        String instanceId = parameters.getInstanceId();
        String databaseId = parameters.getDatabaseId();
        String serviceAccount = SpannerUtil.getServiceAccount(parameters);
        this.context = context;
        this.counter = new BytesCounter();
        this.spanner =
                SpannerUtil.getSpannerServiceWithReadInterceptor(
                        serviceAccount, projectId, counter);
        this.databaseClient =
                spanner.getDatabaseClient(DatabaseId.of(projectId, instanceId, databaseId));
        this.batchSize = parameters.getBatchSize();
        this.parameters = parameters;
        this.seaTunnelRowType = seaTunnelRowType;
        this.mutations = new ArrayList<>();
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        Mutation.WriteBuilder builder = Mutation.newInsertOrUpdateBuilder(parameters.getTableId());
        for (int i = 0; i < element.getArity(); i++) {
            String fieldName = seaTunnelRowType.getFieldName(i);
            SeaTunnelDataType<?> type = seaTunnelRowType.getFieldType(i);
            builder.set(fieldName).to(convertToValue(fieldName, type, element.getField(i)));
        }
        Mutation mutation = builder.build();
        mutations.add(mutation);
        if (mutations.size() > batchSize) {
            databaseClient.write(mutations);
            mutations.clear();
        }
    }

    private Value convertToValue(String name, SeaTunnelDataType<?> type, Object value) {
        if (type instanceof LocalTimeType) {
            switch (type.getSqlType()) {
                case DATE:
                    LocalDate localDate = (LocalDate) value;
                    Date spannerDate =
                            Date.fromYearMonthDay(
                                    localDate.getYear(),
                                    localDate.getMonthValue(),
                                    localDate.getDayOfMonth());
                    return Value.date(spannerDate);
                    // NOT SUPPORT TIME
                case TIMESTAMP:
                    LocalDateTime localDateTime = (LocalDateTime) value;
                    Timestamp spannerTs =
                            Timestamp.ofTimeSecondsAndNanos(
                                    localDateTime.toEpochSecond(ZoneOffset.UTC),
                                    localDateTime.getNano());
                    return Value.timestamp(spannerTs);
                default:
                    throw new IllegalStateException(
                            String.format(
                                    "Field '%s' is of unsupported logical type '%s'",
                                    name, type.getSqlType()));
            }
        } else if (type instanceof DecimalType) {
            // decimal java.math.BigDecimal -> spanner NUMERIC
            return Value.numeric((BigDecimal) value);
        } else if (type instanceof ArrayType) {
            ArrayType arrayType = (ArrayType) type;
            return transformCollectionToValue(name, arrayType, value);
        } else {
            switch (type.getSqlType()) {
                case BOOLEAN:
                    return Value.bool((Boolean) value);
                case STRING:
                    return Value.string((String) value);
                case TINYINT:
                    Byte byteValue = (Byte) value;
                    if (byteValue == null) {
                        return Value.int64(null);
                    } else {
                        return Value.int64(byteValue.longValue());
                    }
                case SMALLINT:
                    Short shortValue = (Short) value;
                    if (shortValue == null) {
                        return Value.int64(null);
                    } else {
                        return Value.int64(shortValue.longValue());
                    }
                case INT:
                    Integer intValue = (Integer) value;
                    if (intValue == null) {
                        return Value.int64(null);
                    } else {
                        return Value.int64(intValue.longValue());
                    }
                case BIGINT:
                    return Value.int64((Long) value);
                case FLOAT:
                    Float floatValue = (Float) value;
                    if (floatValue == null) {
                        return Value.float64(null);
                    } else {
                        return Value.float64(floatValue.doubleValue());
                    }
                case DOUBLE:
                    return Value.float64((Double) value);
                case BYTES:
                    if (value == null) {
                        return Value.bytes(null);
                    }

                    if (value instanceof ByteBuffer) {
                        return Value.bytes(ByteArray.copyFrom((ByteBuffer) value));
                    } else {
                        return Value.bytes(ByteArray.copyFrom((byte[]) value));
                    }
                default:
                    throw new IllegalStateException(
                            String.format(
                                    "Field '%s' is of unsupported logical type '%s'",
                                    type, type.getSqlType()));
            }
        }
    }

    private Value transformCollectionToValue(String name, ArrayType arrayType, Object object) {
        if (!(object instanceof Object[])) {
            throw new IllegalStateException(
                    String.format(
                            "Field '%s' is an array of '%s', which is not supported.",
                            name, object.getClass()));
        }
        SeaTunnelDataType elementType = arrayType.getElementType();
        Object[] fields = (Object[]) object;
        List<Value> values =
                Arrays.stream(fields)
                        .map(value -> convertToValue(name, elementType, value))
                        .collect(Collectors.toList());

        switch (elementType.getSqlType()) {
            case BOOLEAN:
                return Value.boolArray(
                        values.stream()
                                .map(value -> value.isNull() ? null : value.getBool())
                                .collect(Collectors.toList()));
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
                return Value.int64Array(
                        values.stream()
                                .map(value -> value.isNull() ? null : value.getInt64())
                                .collect(Collectors.toList()));
            case FLOAT:
            case DOUBLE:
                return Value.float64Array(
                        values.stream()
                                .map(value -> value.isNull() ? null : value.getFloat64())
                                .collect(Collectors.toList()));
            case STRING:
                return Value.stringArray(
                        values.stream()
                                .map(value -> value.isNull() ? null : value.getString())
                                .collect(Collectors.toList()));

            case BYTES:
                return Value.bytesArray(
                        values.stream()
                                .map(value -> value.isNull() ? null : value.getBytes())
                                .collect(Collectors.toList()));
            case DATE:
                return Value.dateArray(
                        values.stream()
                                .map(value -> value.isNull() ? null : value.getDate())
                                .collect(Collectors.toList()));
            case TIMESTAMP:
                return Value.timestampArray(
                        values.stream()
                                .map(value -> value.isNull() ? null : value.getTimestamp())
                                .collect(Collectors.toList()));
            case DECIMAL:
                return Value.numericArray(
                        values.stream()
                                .map(value -> value.isNull() ? null : value.getNumeric())
                                .collect(Collectors.toList()));
            default:
                throw new IllegalStateException(
                        String.format(
                                "Field '%s' is an array of '%s', which is not supported.",
                                name, elementType.getSqlType()));
        }
    }

    @Override
    public void close() throws IOException {
        try {
            if (mutations.size() > 0) {
                databaseClient.write(mutations);
                context.getMetricsContext()
                        .counter(BYTES_WRITTEN.counterName())
                        .inc(counter.getValue());
                mutations.clear();
            }
        } finally {
            spanner.close();
        }
    }
}
