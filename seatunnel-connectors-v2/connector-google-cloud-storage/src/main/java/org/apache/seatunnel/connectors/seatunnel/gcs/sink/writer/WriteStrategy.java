package org.apache.seatunnel.connectors.seatunnel.gcs.sink.writer;

import org.apache.hadoop.conf.Configuration;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.gcs.config.FileSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.gcs.sink.commit.FileCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.gcs.sink.state.FileSinkState;
import org.apache.seatunnel.connectors.seatunnel.gcs.util.FileSystemUtils;

import java.io.Serializable;
import java.util.List;
import java.util.Optional;

public interface WriteStrategy extends Serializable {

    /**
     * init
     */
    void init(String jobId, String uuidPrefix, int subTaskIndex);

    /**
     * use hadoop conf generate hadoop configuration
     *
     * @return Configuration
     */
    Configuration getConfiguration();

    /**
     * write seaTunnelRow to target datasource
     *
     * @param seaTunnelRow seaTunnelRow
     */
    void write(SeaTunnelRow seaTunnelRow);

    /**
     * set seaTunnelRowTypeInfo in writer
     *
     * @param seaTunnelRowType seaTunnelRowType
     */
    void setSeaTunnelRowTypeInfo(SeaTunnelRowType seaTunnelRowType);

    /**
     * get sink configuration
     *
     * @return sink configuration
     */
    FileSinkConfig getFileSinkConfig();

    /**
     * get file system utils
     *
     * @return file system utils
     */
    FileSystemUtils getFileSystemUtils();

    /**
     * set file system utils
     *
     * @param fileSystemUtils fileSystemUtils
     */
    void setFileSystemUtils(FileSystemUtils fileSystemUtils);

    /**
     * use transaction id generate file name
     *
     * @param transactionId transaction id
     * @return file name
     */
    String generateFileName(String transactionId);

    /** when a transaction is triggered, release resources */
    void finishAndCloseFile();

    /**
     * get current checkpoint id
     *
     * @return checkpoint id
     */
    long getCheckpointId();

    /**
     * prepare commit operation
     *
     * @return the file commit information
     */
    Optional<FileCommitInfo> prepareCommit();

    /** abort prepare commit operation */
    void abortPrepare();

    /**
     * abort prepare commit operation using transaction id
     *
     * @param transactionId transaction id
     */
    void abortPrepare(String transactionId);

    /**
     * when a checkpoint was triggered, snapshot the state of connector
     *
     * @param checkpointId checkpointId
     * @return the list of states
     */
    List<FileSinkState> snapshotState(long checkpointId);

    /**
     * when a checkpoint triggered, file sink should begin a new transaction
     *
     * @param checkpointId checkpoint id
     */
    void beginTransaction(Long checkpointId);

}
