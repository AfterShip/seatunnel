package org.apache.seatunnel.connectors.seatunnel.gcs.sink;

import lombok.extern.slf4j.Slf4j;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.event.handler.DataTypeChangeEventHandler;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class GcsSinkWriter extends AbstractSinkWriter<SeaTunnelRow, Void> {

    private SeaTunnelRowType seaTunnelRowType;
    private final SinkWriter.Context context;

    public GcsSinkWriter(SeaTunnelRowType seaTunnelRowType, Context context) {
        this.seaTunnelRowType = seaTunnelRowType;
        this.context = context;
    }



    @Override
    public void write(SeaTunnelRow element) throws IOException {

    }

    @Override
    public void close() throws IOException {

    }

}
