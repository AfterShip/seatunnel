package org.apache.seatunnel.connectors.seatunnel.bigquery.source;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * @author ah.he@aftership.com
 * @version 1.0
 * @date 2023/8/2 13:55
 */
@AllArgsConstructor
@Getter
public class BigQuerySourceState implements Serializable {
    private boolean shouldEnumerate;
    private Map<Integer, List<BigQuerySourceSplit>> pendingSplit;
}
