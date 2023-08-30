package org.apache.seatunnel.connectors.seatunnel.gcs.sink.writer;

import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS;
import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem;
import com.google.common.collect.Lists;
import lombok.NonNull;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.Constants;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.common.utils.VariablesSubstitute;
import org.apache.seatunnel.connectors.seatunnel.gcs.config.FileFormat;
import org.apache.seatunnel.connectors.seatunnel.gcs.config.FileSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.gcs.exception.GcsConnectorException;
import org.apache.seatunnel.connectors.seatunnel.gcs.sink.commit.FileCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.gcs.sink.state.FileSinkState;
import org.apache.seatunnel.connectors.seatunnel.gcs.util.FileSystemUtils;
import org.apache.seatunnel.connectors.seatunnel.gcs.util.GCSPath;

import java.io.File;
import java.io.IOException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.regex.Matcher;

public abstract class AbstractWriteStrategy implements WriteStrategy {

    protected final FileSinkConfig fileSinkConfig;
    protected final List<Integer> sinkColumnsIndexInRow;
    protected String jobId;
    protected int subTaskIndex;
    protected FileSystemUtils fileSystemUtils;
    protected String transactionId;
    protected String uuidPrefix;
    protected String transactionDirectory;
    protected LinkedHashMap<String, String> needMoveFiles;
    protected LinkedHashMap<String, String> beingWrittenFile = new LinkedHashMap<>();
    protected SeaTunnelRowType seaTunnelRowType;
    // Checkpoint id from engine is start with 1
    protected Long checkpointId = 0L;
    protected int batchSize;
    protected int currentBatchSize = 0;
    public static final String NON_PARTITION = "NON_PARTITION";

    public AbstractWriteStrategy(FileSinkConfig fileSinkConfig) {
        this.fileSinkConfig = fileSinkConfig;
        this.sinkColumnsIndexInRow = fileSinkConfig.getSinkColumnsIndexInRow();
        this.batchSize = 1000000;
    }

    @Override
    public void init(String jobId, String uuidPrefix, int subTaskIndex) {
        this.jobId = jobId;
        this.uuidPrefix = uuidPrefix;
        this.subTaskIndex = subTaskIndex;
    }

    @Override
    public void write(SeaTunnelRow seaTunnelRow) {
        if (currentBatchSize >= batchSize) {
            currentBatchSize = 0;
        }
        currentBatchSize++;
    }

    @Override
    public Configuration getConfiguration() {
        Configuration configuration = new Configuration();
        configuration.set(
                String.format("fs.gs.impl", "hdfs"), GoogleHadoopFileSystem.class.getName());
        configuration.set("fs.AbstractFileSystem.gs.impl", GoogleHadoopFS.class.getName());
        configuration.set("fs.gs.project.id", fileSinkConfig.getProjectId());
        configuration.set("fs.gs.system.bucket", GCSPath.from(fileSinkConfig.getPath()).getBucket());
        configuration.set("fs.gs.path.encoding", "uri-path");
        configuration.set("fs.gs.working.dir", GCSPath.ROOT_DIR);
        configuration.set("fs.gs.impl.disable.cache", "true");
        return configuration;
    }

    @Override
    public String generateFileName(String transactionId) {
        String fileNameExpression = "${transactionId}";
        FileFormat fileFormat = fileSinkConfig.getFileFormat();
        String suffix = fileFormat.getSuffix();
        if (StringUtils.isBlank(fileNameExpression)) {
            return transactionId + suffix;
        }
        DateTimeFormatter df = DateTimeFormatter.ofPattern("yyyy.MM.dd");
        String formattedDate = df.format(ZonedDateTime.now());
        Map<String, String> valuesMap = new HashMap<>();
        valuesMap.put(Constants.UUID, UUID.randomUUID().toString());
        valuesMap.put(Constants.NOW, formattedDate);
        valuesMap.put("yyyy.MM.dd", formattedDate);
        valuesMap.put("transactionId", transactionId);
        String substitute =
                VariablesSubstitute.substitute(fileNameExpression, valuesMap);
        return substitute + suffix;
    }

    @Override
    public Optional<FileCommitInfo> prepareCommit() {
        this.finishAndCloseFile();
        LinkedHashMap<String, String> commitMap = new LinkedHashMap<>(this.needMoveFiles);
        return Optional.of(new FileCommitInfo(commitMap, transactionDirectory));
    }

    @Override
    public void abortPrepare() {
        abortPrepare(transactionId);
    }

    @Override
    public void setFileSystemUtils(FileSystemUtils fileSystemUtils) {
        this.fileSystemUtils = fileSystemUtils;
    }

    @Override
    public void setSeaTunnelRowTypeInfo(SeaTunnelRowType seaTunnelRowType) {
        this.seaTunnelRowType = seaTunnelRowType;
    }

    @Override
    public FileSystemUtils getFileSystemUtils() {
        return this.fileSystemUtils;
    }

    @Override
    public long getCheckpointId() {
        return this.checkpointId;
    }

    @Override
    public FileSinkConfig getFileSinkConfig() {
        return fileSinkConfig;
    }

    @Override
    public List<FileSinkState> snapshotState(long checkpointId) {
        ArrayList<FileSinkState> fileState =
                Lists.newArrayList(
                        new FileSinkState(
                                this.transactionId,
                                this.uuidPrefix,
                                this.checkpointId,
                                new LinkedHashMap<>(this.needMoveFiles),
                                this.getTransactionDir(transactionId)));
        this.beingWrittenFile.clear();
        this.beginTransaction(checkpointId + 1);
        return fileState;
    }


    public void abortPrepare(String transactionId) {
        try {
            fileSystemUtils.deleteFile(getTransactionDir(transactionId));
        } catch (IOException e) {
            throw new GcsConnectorException(
                    CommonErrorCode.FILE_OPERATION_FAILED,
                    "Abort transaction "
                            + transactionId
                            + " error, delete transaction directory failed",
                    e);
        }
    }

    @Override
    public void beginTransaction(Long checkpointId) {
        this.checkpointId = checkpointId;
        this.transactionId = getTransactionId(checkpointId);
        this.transactionDirectory = getTransactionDir(this.transactionId);
        this.needMoveFiles = new LinkedHashMap<>();
    }

    private String getTransactionDir(@NonNull String transactionId) {
        String transactionDirectoryPrefix =
                getTransactionDirPrefix(fileSinkConfig.getTmpPath(), jobId, uuidPrefix);
        return String.join(
                File.separator, new String[] {transactionDirectoryPrefix, transactionId});
    }

    public static String getTransactionDirPrefix(String tmpPath, String jobId, String uuidPrefix) {
        String[] strings = new String[] {tmpPath, "seatunnel", jobId, uuidPrefix};
        return String.join(File.separator, strings);
    }

    private String getTransactionId(Long checkpointId) {
        return "T"
                + "_"
                + jobId
                + "_"
                + uuidPrefix
                + "_"
                + subTaskIndex
                + "_"
                + checkpointId;
    }

    public String getOrCreateFilePath(@NonNull SeaTunnelRow seaTunnelRow) {
        String beingWrittenFilePath = beingWrittenFile.get(NON_PARTITION);
        if (beingWrittenFilePath != null) {
            return beingWrittenFilePath;
        } else {
            String[] pathSegments =
                    new String[]{
                            transactionDirectory, generateFileName(transactionId)
                    };
            String newBeingWrittenFilePath = String.join(File.separator, pathSegments);
            beingWrittenFile.put(NON_PARTITION, newBeingWrittenFilePath);
            return newBeingWrittenFilePath;
        }
    }

    public String getTargetLocation(@NonNull String seaTunnelFilePath) {
        String tmpPath = seaTunnelFilePath.replaceAll(
                Matcher.quoteReplacement(transactionDirectory),
                Matcher.quoteReplacement(fileSinkConfig.getPath()));
        return tmpPath.replaceAll(
                NON_PARTITION + Matcher.quoteReplacement(File.separator), "");
    }


}
