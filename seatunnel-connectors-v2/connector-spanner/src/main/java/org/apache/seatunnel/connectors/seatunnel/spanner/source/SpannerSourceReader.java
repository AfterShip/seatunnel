package org.apache.seatunnel.connectors.seatunnel.spanner.source;

import org.apache.seatunnel.api.common.metrics.Counter;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.spanner.common.BytesCounter;
import org.apache.seatunnel.connectors.seatunnel.spanner.config.SpannerParameters;
import org.apache.seatunnel.connectors.seatunnel.spanner.serialization.DefaultSeaTunnelRowDeserializer;
import org.apache.seatunnel.connectors.seatunnel.spanner.serialization.SeaTunnelRowDeserializer;
import org.apache.seatunnel.connectors.seatunnel.spanner.utils.SpannerUtil;

import com.google.cloud.spanner.BatchClient;
import com.google.cloud.spanner.BatchReadOnlyTransaction;
import com.google.cloud.spanner.BatchTransactionId;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Partition;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;

import static org.apache.seatunnel.connectors.seatunnel.spanner.common.SourceSinkCounter.BYTES_READ;

/**
 * @author: gf.xu
 * @email: gf.xu@aftership.com
 * @date: 2023/9/6 14:34
 */
@Slf4j
public class SpannerSourceReader implements SourceReader<SeaTunnelRow, SpannerSourceSplit> {

    private SeaTunnelRowDeserializer rowDeserializer;

    private SeaTunnelRowType rowTypeInfo;

    private SpannerParameters spannerParameters;

    private SourceReader.Context context;

    private Counter bytesRead;

    private BytesCounter counter;

    private Spanner spanner;

    private boolean noMoreSplit;

    private final long pollNextWaitTime = 1000L;

    private Deque<SpannerSourceSplit> splits = new LinkedList<>();

    public SpannerSourceReader(
            SeaTunnelRowType rowTypeInfo, SpannerParameters spannerParameters, Context context) {
        this.rowTypeInfo = rowTypeInfo;
        this.spannerParameters = spannerParameters;
        this.context = context;
        this.counter = new BytesCounter();
        this.bytesRead = context.getMetricsContext().counter(BYTES_READ.counterName());
    }

    @Override
    public void open() throws Exception {
        rowDeserializer = new DefaultSeaTunnelRowDeserializer(rowTypeInfo);

        // create a spanner
        String serviceAccount = SpannerUtil.getServiceAccount(spannerParameters);
        spanner =
                SpannerUtil.getSpannerServiceWithReadInterceptor(
                        serviceAccount, spannerParameters.getProjectId(), counter);
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        synchronized (output.getCheckpointLock()) {
            SpannerSourceSplit split = splits.poll();
            if (split != null) {
                BatchTransactionId batchTransactionId = split.getTransactionId();
                Partition partition = split.getPartition();
                BatchClient batchClient =
                        spanner.getBatchClient(
                                DatabaseId.of(
                                        spannerParameters.getProjectId(),
                                        spannerParameters.getInstanceId(),
                                        spannerParameters.getDatabaseId()));
                BatchReadOnlyTransaction transaction =
                        batchClient.batchReadOnlyTransaction(batchTransactionId);
                ResultSet resultSet = transaction.execute(partition);
                outputFromResultSet(resultSet, output);
            } else if (noMoreSplit) {
                // signal to the source that we have reached the end of the data.
                log.info("Closed the bounded Spanner source");
                context.signalNoMoreElement();
            } else {
                Thread.sleep(pollNextWaitTime);
            }
        }
    }

    private void outputFromResultSet(ResultSet resultSet, Collector<SeaTunnelRow> output) {
        try {
            while (resultSet.next()) {
                SeaTunnelRow row = rowDeserializer.deserialize(resultSet);
                output.collect(row);
            }
        } finally {
            resultSet.close();
        }
    }

    @Override
    public List<SpannerSourceSplit> snapshotState(long checkpointId) throws Exception {
        return new ArrayList<>(splits);
    }

    @Override
    public void addSplits(List<SpannerSourceSplit> splits) {
        this.splits.addAll(splits);
    }

    @Override
    public void handleNoMoreSplits() {
        noMoreSplit = true;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        // nothing to do
    }

    @Override
    public void close() throws IOException {
        log.trace("Closing Record reader");
        bytesRead.inc(counter.getValue());
        if (spanner != null) {
            spanner.close();
        }
    }
}
