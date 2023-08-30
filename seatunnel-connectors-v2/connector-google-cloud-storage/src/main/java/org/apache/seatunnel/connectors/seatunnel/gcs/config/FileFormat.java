package org.apache.seatunnel.connectors.seatunnel.gcs.config;

import org.apache.seatunnel.connectors.seatunnel.gcs.sink.writer.AvroWriteStrategy;
import org.apache.seatunnel.connectors.seatunnel.gcs.sink.writer.WriteStrategy;

import java.io.Serializable;

public enum FileFormat implements Serializable {
    AVRO("avro"){
        @Override
        public WriteStrategy getWriteStrategy(FileSinkConfig fileSinkConfig) {
            return new AvroWriteStrategy(fileSinkConfig);
        }
    };

    private final String suffix;

    FileFormat(String suffix) {
        this.suffix = suffix;
    }

    public String getSuffix() {
        return "." + suffix;
    }

    public WriteStrategy getWriteStrategy(FileSinkConfig fileSinkConfig) {
        return null;
    }
}
