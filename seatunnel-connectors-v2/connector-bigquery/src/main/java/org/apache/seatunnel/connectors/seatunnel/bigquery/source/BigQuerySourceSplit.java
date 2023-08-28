package org.apache.seatunnel.connectors.seatunnel.bigquery.source;

import com.google.cloud.hadoop.io.bigquery.UnshardedInputSplit;
import org.apache.seatunnel.api.source.SourceSplit;

/**
 * @author ah.he@aftership.com
 * @version 1.0
 * @date 2023/8/2 13:53
 */
public class BigQuerySourceSplit implements SourceSplit {

    private static final long serialVersionUID = -1L;

    private String splitId;

    private UnshardedInputSplit split;

    public BigQuerySourceSplit(String splitId, UnshardedInputSplit split) {
        this.splitId = splitId;
        this.split = split;
    }

    @Override
    public String splitId() {
        return splitId;
    }

    public UnshardedInputSplit getSplit() {
        return split;
    }
}
