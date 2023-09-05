package org.apache.seatunnel.connectors.seatunnel.gcs.sink.writer;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Conversions;
import org.apache.avro.Schema;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.schema.*;
import org.apache.seatunnel.api.table.type.*;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.gcs.config.FileSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.gcs.exception.GcsConnectorException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Slf4j
public class ParquetWriteStrategy extends AbstractWriteStrategy {

    private final LinkedHashMap<String, ParquetWriter<GenericRecord>> beingWrittenWriter;
    private Schema schema;
    public static final int[] PRECISION_TO_BYTE_COUNT = new int[38];

    static {
        for (int prec = 1; prec <= 38; prec++) {
            // Estimated number of bytes needed.
            PRECISION_TO_BYTE_COUNT[prec - 1] =
                    (int) Math.ceil((Math.log(Math.pow(10, prec) - 1) / Math.log(2) + 1) / 8);
        }
    }

    public ParquetWriteStrategy(FileSinkConfig fileSinkConfig) {
        super(fileSinkConfig);
        this.beingWrittenWriter = new LinkedHashMap<>();
    }

    @Override
    public void write(SeaTunnelRow seaTunnelRow) {
        super.write(seaTunnelRow);
        String filePath = getOrCreateFilePath();
        ParquetWriter<GenericRecord> writer = getOrCreateWriter(filePath);
        GenericRecord record = convertRowToGenericRecord(this.schema, seaTunnelRow);
        try {
            writer.write(record);
        } catch (Exception e) {
            throw new GcsConnectorException(CommonErrorCode.FILE_OPERATION_FAILED,
                    String.format("Open file output stream [%s] failed", filePath),
                    e);
        }
    }

    @Override
    public void finishAndCloseFile() {
        beingWrittenWriter.forEach(
                (key, value) -> {
                    try {
                        value.close();
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
        beingWrittenWriter.clear();
    }

    public GenericRecord convertRowToGenericRecord(Schema schema, SeaTunnelRow element) {
        GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        String[] fieldNames = this.seaTunnelRowType.getFieldNames();
        for (int i = 0; i < fieldNames.length; i++) {
            String fieldName = this.seaTunnelRowType.getFieldName(i);
            Object value = element.getField(i);
            builder.set(fieldName.toLowerCase(), resolveObject(value, this.seaTunnelRowType.getFieldType(i)));
        }
        return builder.build();
    }

    private Object resolveObject(Object data, SeaTunnelDataType<?> seaTunnelDataType) {
        if (data == null) {
            return null;
        }
        switch (seaTunnelDataType.getSqlType()) {
            case ARRAY:
                SeaTunnelDataType<?> elementType =
                        ((ArrayType<?, ?>) seaTunnelDataType).getElementType();
                ArrayList<Object> records = new ArrayList<>(((Object[]) data).length);
                for (Object object : (Object[]) data) {
                    Object resolvedObject = resolveObject(object, elementType);
                    records.add(resolvedObject);
                }
                return records;
            case MAP:
            case STRING:
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case NULL:
            case DECIMAL:
            case DATE:
                return data;
            case TIMESTAMP:
                return ((LocalDateTime) data).toInstant(ZoneOffset.UTC).toEpochMilli();
            case BYTES:
                return ByteBuffer.wrap((byte[]) data);
            case ROW:
                SeaTunnelRow seaTunnelRow = (SeaTunnelRow) data;
                SeaTunnelDataType<?>[] fieldTypes =
                        ((SeaTunnelRowType) seaTunnelDataType).getFieldTypes();
                String[] fieldNames = ((SeaTunnelRowType) seaTunnelDataType).getFieldNames();
                List<Integer> sinkColumnsIndex =
                        IntStream.rangeClosed(0, fieldNames.length - 1)
                                .boxed()
                                .collect(Collectors.toList());
                Schema recordSchema =
                        buildAvroSchemaWithRowType(
                                (SeaTunnelRowType) seaTunnelDataType);
                GenericRecordBuilder recordBuilder = new GenericRecordBuilder(recordSchema);
                for (int i = 0; i < fieldNames.length; i++) {
                    recordBuilder.set(
                            fieldNames[i].toLowerCase(),
                            resolveObject(seaTunnelRow.getField(i), fieldTypes[i]));
                }
                return recordBuilder.build();
            default:
                String errorMsg =
                        String.format(
                                "SeaTunnel file connector is not supported for this data type [%s]",
                                seaTunnelDataType.getSqlType());
                throw new GcsConnectorException(CommonErrorCode.UNSUPPORTED_DATA_TYPE, errorMsg);
        }
    }

    private ParquetWriter<GenericRecord> getOrCreateWriter(@NonNull String filePath) {
        if (schema == null) {
            schema = buildAvroSchemaWithRowType(seaTunnelRowType);
        }
        ParquetWriter<GenericRecord> writer = this.beingWrittenWriter.get(filePath);
        GenericData dataModel = new GenericData();
        dataModel.addLogicalTypeConversion(new Conversions.DecimalConversion());
        dataModel.addLogicalTypeConversion(new TimeConversions.DateConversion());
        dataModel.addLogicalTypeConversion(new TimeConversions.LocalTimestampMillisConversion());
        if (writer == null) {
            Path path = new Path(filePath);
            try {
                HadoopOutputFile outputFile =
                        HadoopOutputFile.fromPath(path,
                                fileSystemUtils.getConfiguration(fileSinkConfig.getProjectId(), filePath));
                ParquetWriter<GenericRecord> newWriter =
                        AvroParquetWriter.<GenericRecord>builder(outputFile)
                                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                                .withDataModel(dataModel)
                                // use parquet v1 to improve compatibility
                                .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_1_0)
                                .withSchema(schema)
                                .build();
                this.beingWrittenWriter.put(filePath, newWriter);
                return newWriter;
            } catch (IOException e) {
                String errorMsg = String.format("Get parquet writer for file [%s] error", filePath);
                throw new GcsConnectorException(
                        CommonErrorCode.WRITER_OPERATION_FAILED, errorMsg, e);
            }
        }
        return writer;
    }

    private Schema buildAvroSchemaWithRowType(
            SeaTunnelRowType seaTunnelRowType) {
        ArrayList<Type> types = new ArrayList<>();
        SeaTunnelDataType<?>[] fieldTypes = seaTunnelRowType.getFieldTypes();
        String[] fieldNames = seaTunnelRowType.getFieldNames();
        for (int i = 0; i < fieldNames.length; i++) {
            Type type =
                    seaTunnelDataType2ParquetDataType(
                            fieldNames[i].toLowerCase(), fieldTypes[i]);
            types.add(type);
        }
        MessageType seaTunnelRow =
                Types.buildMessage().addFields(types.toArray(new Type[0])).named("etlSchemaBody");
        AvroSchemaConverter schemaConverter = new AvroSchemaConverter();
        return schemaConverter.convert(seaTunnelRow);
    }

    public static Type seaTunnelDataType2ParquetDataType(
            String fieldName, SeaTunnelDataType<?> seaTunnelDataType) {
        switch (seaTunnelDataType.getSqlType()) {
            case ARRAY:
                SeaTunnelDataType<?> elementType =
                        ((ArrayType<?, ?>) seaTunnelDataType).getElementType();
                return Types.optionalGroup()
                        .as(OriginalType.LIST)
                        .addField(
                                Types.repeatedGroup()
                                        .addField(
                                                seaTunnelDataType2ParquetDataType(
                                                        "array_element", elementType))
                                        .named("bag"))
                        .named(fieldName);
            case MAP:
                SeaTunnelDataType<?> keyType = ((MapType<?, ?>) seaTunnelDataType).getKeyType();
                SeaTunnelDataType<?> valueType = ((MapType<?, ?>) seaTunnelDataType).getValueType();
                return ConversionPatterns.mapType(
                        Type.Repetition.OPTIONAL,
                        fieldName,
                        seaTunnelDataType2ParquetDataType("key", keyType),
                        seaTunnelDataType2ParquetDataType("value", valueType));
            case STRING:
                return Types.primitive(
                                PrimitiveType.PrimitiveTypeName.BINARY, Type.Repetition.OPTIONAL)
                        .as(LogicalTypeAnnotation.stringType())
                        .named(fieldName);
            case BOOLEAN:
                return Types.primitive(
                                PrimitiveType.PrimitiveTypeName.BOOLEAN, Type.Repetition.OPTIONAL)
                        .named(fieldName);
            case TINYINT:
                return Types.primitive(
                                PrimitiveType.PrimitiveTypeName.INT32, Type.Repetition.OPTIONAL)
                        .as(LogicalTypeAnnotation.intType(8, true))
                        .as(OriginalType.INT_8)
                        .named(fieldName);
            case SMALLINT:
                return Types.primitive(
                                PrimitiveType.PrimitiveTypeName.INT32, Type.Repetition.OPTIONAL)
                        .as(LogicalTypeAnnotation.intType(16, true))
                        .as(OriginalType.INT_16)
                        .named(fieldName);
            case INT:
                return Types.primitive(
                                PrimitiveType.PrimitiveTypeName.INT32, Type.Repetition.OPTIONAL)
                        .named(fieldName);
            case DATE:
                return Types.primitive(
                                PrimitiveType.PrimitiveTypeName.INT32, Type.Repetition.OPTIONAL)
                        .as(LogicalTypeAnnotation.dateType())
                        .as(OriginalType.DATE)
                        .named(fieldName);
            case BIGINT:
                return Types.primitive(
                                PrimitiveType.PrimitiveTypeName.INT64, Type.Repetition.OPTIONAL)
                        .named(fieldName);
            case TIMESTAMP:
                return Types.primitive(
                                PrimitiveType.PrimitiveTypeName.INT64, Type.Repetition.OPTIONAL)
                        .as(OriginalType.TIMESTAMP_MILLIS)
                        .named(fieldName);
            case FLOAT:
                return Types.primitive(
                                PrimitiveType.PrimitiveTypeName.FLOAT, Type.Repetition.OPTIONAL)
                        .named(fieldName);
            case DOUBLE:
                return Types.primitive(
                                PrimitiveType.PrimitiveTypeName.DOUBLE, Type.Repetition.OPTIONAL)
                        .named(fieldName);
            case DECIMAL:
                int precision = ((DecimalType) seaTunnelDataType).getPrecision();
                int scale = ((DecimalType) seaTunnelDataType).getScale();
                return Types.optional(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
                        .length(PRECISION_TO_BYTE_COUNT[precision - 1])
                        .as(OriginalType.DECIMAL)
                        .precision(precision)
                        .scale(scale)
                        .named(fieldName);
            case BYTES:
                return Types.primitive(
                                PrimitiveType.PrimitiveTypeName.BINARY, Type.Repetition.OPTIONAL)
                        .named(fieldName);
            case ROW:
                SeaTunnelDataType<?>[] fieldTypes =
                        ((SeaTunnelRowType) seaTunnelDataType).getFieldTypes();
                String[] fieldNames = ((SeaTunnelRowType) seaTunnelDataType).getFieldNames();
                Type[] types = new Type[fieldTypes.length];
                for (int i = 0; i < fieldNames.length; i++) {
                    Type type = seaTunnelDataType2ParquetDataType(fieldNames[i], fieldTypes[i]);
                    types[i] = type;
                }
                return Types.optionalGroup().addFields(types).named(fieldName);
            case NULL:
            default:
                String errorMsg =
                        String.format(
                                "SeaTunnel file connector is not supported for this data type [%s]",
                                seaTunnelDataType.getSqlType());
                throw new GcsConnectorException(CommonErrorCode.UNSUPPORTED_DATA_TYPE, errorMsg);
        }
    }
}
