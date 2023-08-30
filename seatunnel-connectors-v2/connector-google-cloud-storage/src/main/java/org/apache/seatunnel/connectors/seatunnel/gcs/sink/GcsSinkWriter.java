package org.apache.seatunnel.connectors.seatunnel.gcs.sink;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.gcs.exception.GcsConnectorException;
import org.apache.seatunnel.connectors.seatunnel.gcs.sink.commit.FileAggregatedCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.gcs.sink.commit.FileCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.gcs.sink.commit.FileSinkAggregatedCommitter;
import org.apache.seatunnel.connectors.seatunnel.gcs.sink.state.FileSinkState;
import org.apache.seatunnel.connectors.seatunnel.gcs.sink.writer.AbstractWriteStrategy;
import org.apache.seatunnel.connectors.seatunnel.gcs.sink.writer.WriteStrategy;
import org.apache.seatunnel.connectors.seatunnel.gcs.util.FileSystemUtils;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
public class GcsSinkWriter implements SinkWriter<SeaTunnelRow, FileCommitInfo, FileSinkState> {

    private WriteStrategy writeStrategy;
    private FileSystemUtils fileSystemUtils;

    public GcsSinkWriter(
            WriteStrategy writeStrategy,
            SinkWriter.Context context,
            String jobId) {
        this(writeStrategy, context, jobId, Collections.emptyList());
        writeStrategy.beginTransaction(1L);
    }

    public GcsSinkWriter(
            WriteStrategy writeStrategy,
            SinkWriter.Context context,
            String jobId,
            List<FileSinkState> fileSinkStates) {
        this.writeStrategy = writeStrategy;
        this.fileSystemUtils = writeStrategy.getFileSystemUtils();
        int subTaskIndex = context.getIndexOfSubtask();
        String uuidPrefix;
        if (!fileSinkStates.isEmpty()) {
            uuidPrefix = fileSinkStates.get(0).getUuidPrefix();
        } else {
            uuidPrefix = UUID.randomUUID().toString().replaceAll("-", "").substring(0, 10);
        }

        writeStrategy.init(jobId, uuidPrefix, subTaskIndex);
        if (!fileSinkStates.isEmpty()) {
            try {
                List<String> transactions = findTransactionList(jobId, uuidPrefix);
                FileSinkAggregatedCommitter fileSinkAggregatedCommitter =
                        new FileSinkAggregatedCommitter(fileSystemUtils);
                LinkedHashMap<String, FileSinkState> fileStatesMap = new LinkedHashMap<>();
                fileSinkStates.forEach(
                        fileSinkState ->
                                fileStatesMap.put(fileSinkState.getTransactionId(), fileSinkState));
                for (String transaction : transactions) {
                    if (fileStatesMap.containsKey(transaction)) {
                        // need commit
                        FileSinkState fileSinkState = fileStatesMap.get(transaction);
                        FileAggregatedCommitInfo fileCommitInfo =
                                fileSinkAggregatedCommitter.combine(
                                        Collections.singletonList(
                                                new FileCommitInfo(
                                                        fileSinkState.getNeedMoveFiles(),
                                                        fileSinkState.getTransactionDir())));
                        fileSinkAggregatedCommitter.commit(
                                Collections.singletonList(fileCommitInfo));
                    } else {
                        // need abort
                        writeStrategy.abortPrepare(transaction);
                    }
                }
            } catch (IOException e) {
                String errorMsg =
                        String.format("Try to process these fileStates %s failed", fileSinkStates);
                throw new GcsConnectorException(
                        CommonErrorCode.FILE_OPERATION_FAILED, errorMsg, e);
            }
            writeStrategy.beginTransaction(fileSinkStates.get(0).getCheckpointId() + 1);
        } else {
            writeStrategy.beginTransaction(1L);
        }
    }

    @Override
    public Optional<FileCommitInfo> prepareCommit() throws IOException {
        return writeStrategy.prepareCommit();
    }

    @Override
    public void abortPrepare() {
        writeStrategy.abortPrepare();
    }

    @Override
    public List<FileSinkState> snapshotState(long checkpointId) throws IOException {
        return writeStrategy.snapshotState(checkpointId);
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

    private List<String> findTransactionList(String jobId, String uuidPrefix) throws IOException {
        return fileSystemUtils
                .dirList(
                        AbstractWriteStrategy.getTransactionDirPrefix(
                                writeStrategy.getFileSinkConfig().getTmpPath(), jobId, uuidPrefix))
                .stream()
                .map(Path::getName)
                .collect(Collectors.toList());
    }

}
