package org.apache.seatunnel.connectors.seatunnel.gcs.sink.writer;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.gcs.config.FileFormat;
import org.apache.seatunnel.connectors.seatunnel.gcs.config.FileSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.gcs.util.FileSystemUtils;
import org.apache.seatunnel.connectors.seatunnel.gcs.util.GCSPath;

import org.apache.commons.lang3.StringUtils;

import java.text.SimpleDateFormat;
import java.util.LinkedHashMap;
import java.util.Optional;

public abstract class AbstractWriteStrategy implements WriteStrategy {

    protected final FileSinkConfig fileSinkConfig;
    protected FileSystemUtils fileSystemUtils;
    protected String transactionDirectory;
    protected LinkedHashMap<String, String> beingWrittenFile = new LinkedHashMap<>();
    protected SeaTunnelRowType seaTunnelRowType;
    protected int batchSize;
    protected int currentBatchSize = 0;
    public static final String NON_PARTITION = "NON_PARTITION";

    public AbstractWriteStrategy(FileSinkConfig fileSinkConfig) {
        this.fileSinkConfig = fileSinkConfig;
        this.batchSize = 1000000;
        this.transactionDirectory = getOrCreateDirectory();
    }

    @Override
    public void write(SeaTunnelRow seaTunnelRow) {
        if (currentBatchSize >= batchSize) {
            currentBatchSize = 0;
        }
        currentBatchSize++;
    }

    @Override
    public String generateFileName() {
        FileFormat fileFormat = fileSinkConfig.getFileFormat();
        String suffix = fileFormat.getSuffix();
        return "data" + suffix;
    }

    @Override
    public Optional<Void> prepareCommit() {
        this.finishAndCloseFile();
        return Optional.empty();
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
    public FileSinkConfig getFileSinkConfig() {
        return fileSinkConfig;
    }

    public String getOrCreateDirectory() {
        long logicalStartTime = System.currentTimeMillis();
        String suffix = fileSinkConfig.getSuffix();
        String timeSuffix =
                StringUtils.isEmpty(suffix)
                        ? ""
                        : new SimpleDateFormat(suffix).format(logicalStartTime);
        return GCSPath.SCHEME
                + fileSinkConfig.getPath()
                + GCSPath.ROOT_DIR
                + timeSuffix
                + GCSPath.ROOT_DIR;
    }

    public String getOrCreateFilePath() {
        String beingWrittenFilePath = beingWrittenFile.get(NON_PARTITION);
        if (beingWrittenFilePath != null) {
            return beingWrittenFilePath;
        } else {
            String newBeingWrittenFilePath = this.transactionDirectory + generateFileName();
            beingWrittenFile.put(NON_PARTITION, newBeingWrittenFilePath);
            return newBeingWrittenFilePath;
        }
    }
}
