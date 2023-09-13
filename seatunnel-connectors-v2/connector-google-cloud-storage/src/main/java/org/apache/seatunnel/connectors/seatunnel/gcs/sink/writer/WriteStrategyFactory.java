package org.apache.seatunnel.connectors.seatunnel.gcs.sink.writer;

import lombok.extern.slf4j.Slf4j;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.gcs.config.FileFormat;
import org.apache.seatunnel.connectors.seatunnel.gcs.config.FileSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.gcs.exception.GcsConnectorException;

@Slf4j
public class WriteStrategyFactory {

    private WriteStrategyFactory() {}

    public static WriteStrategy of(String format, FileSinkConfig fileSinkConfig) {
        try {
            FileFormat fileFormat = FileFormat.valueOf(format.toUpperCase());
            return fileFormat.getWriteStrategy(fileSinkConfig);
        } catch (IllegalArgumentException e) {
            String errorMsg =
                    String.format(
                            "File sink connector not support this file type [%s], please check your config",
                            format);
            throw new GcsConnectorException(CommonErrorCode.ILLEGAL_ARGUMENT, errorMsg);
        }
    }

    public static WriteStrategy of(FileFormat fileFormat, FileSinkConfig fileSinkConfig) {
        return fileFormat.getWriteStrategy(fileSinkConfig);
    }

}
