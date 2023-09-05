package org.apache.seatunnel.connectors.seatunnel.gcs.config;

import org.apache.seatunnel.connectors.seatunnel.gcs.sink.writer.*;

import java.io.Serializable;

public enum FileFormat implements Serializable {

    AVRO("avro"){
        @Override
        public WriteStrategy getWriteStrategy(FileSinkConfig fileSinkConfig) {
            return new AvroWriteStrategy(fileSinkConfig);
        }
    },
    JSON("json"){
        @Override
        public WriteStrategy getWriteStrategy(FileSinkConfig fileSinkConfig) {
            return new JsonWriteStrategy(fileSinkConfig);
        }
    },
    CSV("csv") {
        @Override
        public WriteStrategy getWriteStrategy(FileSinkConfig fileSinkConfig) {
            return new CsvWriteStrategy(fileSinkConfig);
        }
    },
    PARQUET("parquet") {
        @Override
        public WriteStrategy getWriteStrategy(FileSinkConfig fileSinkConfig) {
            return new ParquetWriteStrategy(fileSinkConfig);
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
