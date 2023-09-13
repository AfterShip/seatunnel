package org.apache.seatunnel.connectors.seatunnel.gcs.sink.writer;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.gcs.config.FileSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.gcs.util.FileSystemUtils;

import java.io.Serializable;
import java.util.Optional;

public interface WriteStrategy extends Serializable {

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
     * @return file name
     */
    String generateFileName();

    /** when a transaction is triggered, release resources */
    void finishAndCloseFile();

    /**
     * prepare commit operation
     *
     * @return the file commit information
     */
    Optional<Void> prepareCommit();
}
