package org.apache.seatunnel.connectors.seatunnel.gcs.sink;

import lombok.extern.slf4j.Slf4j;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.gcs.exception.GcsConnectorException;
import org.apache.seatunnel.connectors.seatunnel.gcs.sink.writer.WriteStrategy;
import org.apache.seatunnel.connectors.seatunnel.gcs.util.FileSystemUtils;

import java.io.IOException;
import java.util.Optional;

@Slf4j
public class GcsSinkWriter extends AbstractSinkWriter<SeaTunnelRow, Void> {

    private WriteStrategy writeStrategy;
    private FileSystemUtils fileSystemUtils;

    public GcsSinkWriter(
            WriteStrategy writeStrategy,
            SinkWriter.Context context) {
        this.writeStrategy = writeStrategy;
        this.fileSystemUtils = writeStrategy.getFileSystemUtils();
    }

    @Override
    public Optional<Void> prepareCommit() {
        writeStrategy.prepareCommit();
        return Optional.empty();
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        try {
            writeStrategy.write(element);
        } catch (GcsConnectorException e) {
            String errorMsg = String.format("Write this data [%s] to file failed", element);
            throw new GcsConnectorException(CommonErrorCode.FILE_OPERATION_FAILED, errorMsg, e);
        }
    }

    @Override
    public void close() throws IOException {

    }

}
