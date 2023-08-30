package org.apache.seatunnel.format.avro;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import java.io.IOException;

public class AvroDeserializationSchema implements DeserializationSchema<SeaTunnelRow> {

    private static final long serialVersionUID = -7907358485475741366L;

    private final SeaTunnelRowType rowType;
    private final AvroToRowConverter converter;

    public AvroDeserializationSchema(SeaTunnelRowType rowType) {
        this.rowType = rowType;
        this.converter = new AvroToRowConverter();
    }

    @Override
    public SeaTunnelRow deserialize(byte[] message) throws IOException {
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(message, null);
        GenericRecord record = this.converter.getReader().read(null, decoder);
        return converter.converter(record, rowType);
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getProducedType() {
        return this.rowType;
    }
}
