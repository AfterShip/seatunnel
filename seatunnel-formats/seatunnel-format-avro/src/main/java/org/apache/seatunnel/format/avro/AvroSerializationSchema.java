package org.apache.seatunnel.format.avro;


import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.seatunnel.api.serialization.SerializationSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.format.avro.exception.AvroFormatErrorCode;
import org.apache.seatunnel.format.avro.exception.SeaTunnelAvroFormatException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class AvroSerializationSchema implements SerializationSchema {


    private static final long serialVersionUID = 4438784443025715370L;

    private final ByteArrayOutputStream out;
    private final BinaryEncoder encoder;
    private final RowToAvroConverter converter;
    private final DatumWriter<GenericRecord> writer;

    public AvroSerializationSchema(SeaTunnelRowType rowType) {
        this.out = new ByteArrayOutputStream();
        this.encoder = EncoderFactory.get().binaryEncoder(out, null);
        this.converter = new RowToAvroConverter(rowType);
        this.writer = this.converter.getWriter();
    }

    @Override
    public byte[] serialize(SeaTunnelRow element) {
        GenericRecord record = converter.convertRowToGenericRecord(element);
        try {
            out.reset();
            writer.write(record, encoder);
            encoder.flush();
            return out.toByteArray();
        } catch (IOException e) {
            throw new SeaTunnelAvroFormatException(
                    AvroFormatErrorCode.SERIALIZATION_ERROR, e.toString());
        }
    }

}
