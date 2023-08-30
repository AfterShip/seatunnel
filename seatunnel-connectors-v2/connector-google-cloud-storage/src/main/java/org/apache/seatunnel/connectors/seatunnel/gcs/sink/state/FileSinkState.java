package org.apache.seatunnel.connectors.seatunnel.gcs.sink.state;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;
import java.util.LinkedHashMap;

@Data
@AllArgsConstructor
public class FileSinkState implements Serializable {

    private final String transactionId;
    private final String uuidPrefix;
    private final Long checkpointId;
    private final LinkedHashMap<String, String> needMoveFiles;
    private final String transactionDir;

}
