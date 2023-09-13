package org.apache.seatunnel.connectors.seatunnel.spanner.source;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.*;
import com.google.common.base.Strings;
import lombok.extern.slf4j.Slf4j;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.spanner.config.SpannerParameters;
import org.apache.seatunnel.connectors.seatunnel.spanner.exception.SpannerConnectorException;
import org.apache.seatunnel.connectors.seatunnel.spanner.utils.SpannerUtil;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.apache.seatunnel.connectors.seatunnel.spanner.exception.SpannerConnectorErrorCode.SPANNER_CONNECTOR_ERROR;

/**
 * For enumerating the splits of spanner source
 *
 * @author: gf.xu
 * @email: gf.xu@aftership.com
 * @date: 2023/9/6 14:49
 */
@Slf4j
public class SpannerSourceSplitEnumerator implements SourceSplitEnumerator<SpannerSourceSplit, SpannerSourceState> {

    private transient Spanner spanner;

    private final SpannerParameters parameters;

    private SourceSplitEnumerator.Context<SpannerSourceSplit> context;

    private final Map<Integer, List<SpannerSourceSplit>> pendingSplit;

    private volatile boolean shouldEnumerate;

    private final Object stateLock = new Object();

    public SpannerSourceSplitEnumerator(
            SpannerParameters spannerParameters,
            SourceSplitEnumerator.Context<SpannerSourceSplit> context) {
        this(spannerParameters, context, null);
    }

    public SpannerSourceSplitEnumerator(
            SpannerParameters spannerParameters,
            SourceSplitEnumerator.Context<SpannerSourceSplit> context,
            SpannerSourceState sourceState) {
        this.parameters = spannerParameters;
        this.context = context;
        this.pendingSplit = new HashMap<>();
        this.shouldEnumerate = sourceState == null;
        if (sourceState != null) {
            this.shouldEnumerate = sourceState.isShouldEnumerate();
            this.pendingSplit.putAll(sourceState.getPendingSplit());
        }
    }

    @Override
    public void open() {
        try {
            // create a spanner
            String serviceAccount = SpannerUtil.getServiceAccount(parameters);
            spanner = SpannerUtil.getSpannerService(serviceAccount, parameters.getProjectId());
        } catch (IOException e) {
            throw new SpannerConnectorException(SPANNER_CONNECTOR_ERROR, e.getMessage(), e);
        }
    }

    /**
     * enumerate splits
     * add splits to pendingSplit
     * set shouldEnumerate to false
     * get spanner splits
     */
    @Override
    public void run() throws Exception {
        Set<Integer> readers = context.registeredReaders();

        if (shouldEnumerate) {
            BatchClient batchClient =
                    spanner.getBatchClient(DatabaseId.of(parameters.getProjectId(), parameters.getInstanceId(),
                            parameters.getDatabaseId()));
            // Returns the logical start time of the batch. For batch pipelines, this is the time the pipeline was triggered.
            // For realtime pipelines, this is the time for the current microbatch being processed.
            // Returns:Logical time in milliseconds since epoch time (00:00:00 January 1, 1970 UTC).
            // TODO double check, we use current time as logical start time;
            Timestamp logicalStartTimeMicros =
                    Timestamp.ofTimeMicroseconds(TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis()));

            // create batch transaction id
            BatchReadOnlyTransaction batchReadOnlyTransaction =
                    batchClient.batchReadOnlyTransaction(TimestampBound.ofReadTimestamp(logicalStartTimeMicros));
            BatchTransactionId batchTransactionId = batchReadOnlyTransaction.getBatchTransactionId();

            // partitionQuery returns ImmutableList which doesn't implement java Serializable interface,
            // we add to array list, which implements java Serializable
            String importQuery = Strings.isNullOrEmpty(parameters.getImportQuery()) ?
                    String.format("Select * from %s;", parameters.getTableId()) : parameters.getImportQuery();
            List<Partition> partitions =
                    new ArrayList<>(
                            batchReadOnlyTransaction.partitionQuery(getPartitionOptions(),
                                    Statement.of(importQuery), Options.priority(Options.RpcPriority.LOW)));
            List<SpannerSourceSplit> splits = new ArrayList<>();
            for (Partition partition : partitions) {
                splits.add(new SpannerSourceSplit(batchTransactionId, partition));
            }

            synchronized (stateLock) {
                addPendingSplit(splits);
                shouldEnumerate = false;
            }

            // assign
            assignSplit(readers);
        }

        log.info(
                "No more splits to assign." + " Sending NoMoreSplitsEvent to reader {}.", readers);
        readers.forEach(context::signalNoMoreSplits);
    }

    private PartitionOptions getPartitionOptions() {
        PartitionOptions.Builder builder = PartitionOptions.newBuilder();
        if (parameters.getPartitionSizeMB() != null) {
            builder.setPartitionSizeBytes(parameters.getPartitionSizeMB() * 1024 * 1024);
        }
        if (parameters.getMaxPartitions() != null) {
            builder.setMaxPartitions(parameters.getMaxPartitions());
        }
        return builder.build();
    }

    private void addPendingSplit(Collection<SpannerSourceSplit> splits) {
        int readerCount = context.currentParallelism();
        for (SpannerSourceSplit split : splits) {
            int ownerReader = getSplitOwner(split.splitId(), readerCount);
            log.info("Assigning {} to {} reader.", split, ownerReader);
            pendingSplit.computeIfAbsent(ownerReader, r -> new ArrayList<>()).add(split);
        }
    }

    private static int getSplitOwner(String tp, int numReaders) {
        return (tp.hashCode() & Integer.MAX_VALUE) % numReaders;
    }

    private void assignSplit(Collection<Integer> readers) {
        log.debug("Assign pendingSplits to readers {}", readers);

        for (int reader : readers) {
            List<SpannerSourceSplit> assignmentForReader = pendingSplit.remove(reader);
            if (assignmentForReader != null && !assignmentForReader.isEmpty()) {
                log.info("Assign splits {} to reader {}", assignmentForReader, reader);
                try {
                    context.assignSplit(reader, assignmentForReader);
                } catch (Exception e) {
                    log.error(
                            "Failed to assign splits {} to reader {}",
                            assignmentForReader,
                            reader,
                            e);
                    pendingSplit.put(reader, assignmentForReader);
                }
            }
        }
    }

    @Override
    public void addSplitsBack(List<SpannerSourceSplit> splits, int subtaskId) {
        if (!splits.isEmpty()) {
            addPendingSplit(splits);
            assignSplit(Collections.singletonList(subtaskId));
        }
    }

    @Override
    public int currentUnassignedSplitSize() {
        return pendingSplit.size();
    }

    @Override
    public void handleSplitRequest(int subtaskId) {
        throw new SpannerConnectorException(
                CommonErrorCode.UNSUPPORTED_OPERATION, "Unsupported handleSplitRequest: " + subtaskId);
    }

    @Override
    public void registerReader(int subtaskId) {
        log.debug("Register reader {} to SpannerSourceSplitEnumerator.", subtaskId);
        if (!pendingSplit.isEmpty()) {
            assignSplit(Collections.singletonList(subtaskId));
        }
    }

    @Override
    public SpannerSourceState snapshotState(long checkpointId) throws Exception {
        // If the source is bounded, checkpoint is not triggered.
        synchronized (stateLock) {
            return new SpannerSourceState(shouldEnumerate, pendingSplit);
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        // nothing to do
    }

    @Override
    public void close() throws IOException {
        if (spanner != null) {
            spanner.close();
        }
    }
}
