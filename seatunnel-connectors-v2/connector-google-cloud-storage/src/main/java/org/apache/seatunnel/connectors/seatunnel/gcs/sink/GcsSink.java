package org.apache.seatunnel.connectors.seatunnel.gcs.sink;

import com.google.auth.Credentials;
import com.google.auto.service.AutoService;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import org.apache.commons.lang3.StringUtils;
import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.api.sink.SinkCommitter;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.common.utils.ConfigCenterUtils;
import org.apache.seatunnel.connectors.seatunnel.common.utils.GCPUtils;
import org.apache.seatunnel.connectors.seatunnel.gcs.config.FileSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.gcs.exception.GcsConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.gcs.exception.GcsConnectorException;
import org.apache.seatunnel.connectors.seatunnel.gcs.sink.commit.FileAggregatedCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.gcs.sink.commit.FileCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.gcs.sink.commit.FileSinkAggregatedCommitter;
import org.apache.seatunnel.connectors.seatunnel.gcs.sink.state.FileSinkState;
import org.apache.seatunnel.connectors.seatunnel.gcs.sink.writer.WriteStrategy;
import org.apache.seatunnel.connectors.seatunnel.gcs.sink.writer.WriteStrategyFactory;
import org.apache.seatunnel.connectors.seatunnel.gcs.util.FileSystemUtils;
import org.apache.seatunnel.connectors.seatunnel.gcs.util.GCSPath;
import org.apache.seatunnel.connectors.seatunnel.gcs.util.GcsUtils;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Optional;

import static org.apache.seatunnel.connectors.seatunnel.common.config.ConfigCenterConfig.*;
import static org.apache.seatunnel.connectors.seatunnel.gcs.config.GcsSinkConfig.*;
import static org.apache.seatunnel.connectors.seatunnel.gcs.constants.ContentTypeConstants.*;

@AutoService(SeaTunnelSink.class)
public class GcsSink implements SeaTunnelSink<SeaTunnelRow, FileSinkState, FileCommitInfo, FileAggregatedCommitInfo> {

    private SeaTunnelRowType seaTunnelRowType;
    private Config pluginConfig;
    private FileSinkConfig fileSinkConfig;
    private FileSystemUtils fileSystemUtils;
    private WriteStrategy writeStrategy;
    private JobContext jobContext;
    private String jobId;

    @Override
    public String getPluginName() {
        return "GCS";
    }

    @Override
    public void setJobContext(JobContext jobContext) {
        this.jobContext = jobContext;
        this.jobId = jobContext.getJobId();
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        this.pluginConfig = pluginConfig;
        // validate pluginConfig
        String path = pluginConfig.getString(PATH.key());
        String bucket;
        try {
            bucket = GCSPath.from(path).getBucket();
        } catch (IllegalArgumentException e) {
            throw new GcsConnectorException(GcsConnectorErrorCode.VALIDATE_FAILED,
                    "path is invalid, please check it." + e.getMessage());
        }
        if (pluginConfig.hasPath(SUFFIX.key())) {
            String suffix = pluginConfig.getString(SUFFIX.key());
            if (StringUtils.isNoneEmpty()) {
                try {
                    new SimpleDateFormat(suffix);
                } catch (IllegalArgumentException e) {
                    throw new GcsConnectorException(GcsConnectorErrorCode.VALIDATE_FAILED,
                            "suffix is invalid, please check it." + e.getMessage());
                }
            }
        }
        String format = pluginConfig.getString(FORMAT.key());
        String contentType = pluginConfig.getString(CONTENT_TYPE.key());
        validateContentType(format, contentType);
        String serviceAccountJson = ConfigCenterUtils.getServiceAccountFromConfigCenter(
                pluginConfig.getString(CONFIG_CENTER_TOKEN.key()),
                pluginConfig.getString(CONFIG_CENTER_URL.key()),
                pluginConfig.getString(CONFIG_CENTER_ENVIRONMENT.key()),
                pluginConfig.getString(CONFIG_CENTER_PROJECT.key()));
        // check bucket exists
        Credentials credentials = GCPUtils.getCredentials(serviceAccountJson);
        Storage storage = GcsUtils.getStorage(pluginConfig.getString(PROJECT_ID.key()), credentials);
        try {
            storage.get(bucket);
        } catch (StorageException e) {
            throw new GcsConnectorException(GcsConnectorErrorCode.VALIDATE_FAILED,
                    String.format("Unable to access or create bucket %s. ", bucket)
                            + "Ensure you entered the correct bucket path and have permissions for it." + e.getMessage());
        }
    }

    @Override
    public void setTypeInfo(SeaTunnelRowType seaTunnelRowType) {
        this.seaTunnelRowType = seaTunnelRowType;
        this.fileSinkConfig = new FileSinkConfig(pluginConfig, seaTunnelRowType);
        this.writeStrategy =
                WriteStrategyFactory.of(fileSinkConfig.getFileFormat(), fileSinkConfig);
        this.fileSystemUtils = new FileSystemUtils(fileSinkConfig.getProjectId());
        this.writeStrategy.setSeaTunnelRowTypeInfo(seaTunnelRowType);
        this.writeStrategy.setFileSystemUtils(fileSystemUtils);
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getConsumedType() {
        return this.seaTunnelRowType;
    }

    @Override
    public SinkWriter<SeaTunnelRow, FileCommitInfo, FileSinkState> restoreWriter(SinkWriter.Context context, List<FileSinkState> states) throws IOException {
        return new GcsSinkWriter(writeStrategy, context, jobId, states);
    }

    @Override
    public Optional<SinkAggregatedCommitter<FileCommitInfo, FileAggregatedCommitInfo>> createAggregatedCommitter() throws IOException {
        return Optional.of(new FileSinkAggregatedCommitter(fileSystemUtils));
    }

    @Override
    public SinkWriter<SeaTunnelRow, FileCommitInfo, FileSinkState> createWriter(SinkWriter.Context context) throws IOException {
        return new GcsSinkWriter(writeStrategy, context, jobId);
    }

    @Override
    public Optional<Serializer<FileCommitInfo>> getCommitInfoSerializer() {
        return Optional.of(new DefaultSerializer<>());
    }

    @Override
    public Optional<Serializer<FileAggregatedCommitInfo>> getAggregatedCommitInfoSerializer() {
        return Optional.of(new DefaultSerializer<>());
    }

    @Override
    public Optional<Serializer<FileSinkState>> getWriterStateSerializer() {
        return Optional.of(new DefaultSerializer<>());
    }

    private void validateContentType(String format, String contentType) {
        switch (format) {
            case FORMAT_AVRO:
                if (!contentType.equals(CONTENT_TYPE_APPLICATION_AVRO)) {
                    throw new GcsConnectorException(GcsConnectorErrorCode.VALIDATE_FAILED,
                            "format is avro, but contentType is not supported.");
                }
                break;
//            case FORMAT_CSV:
//                if (!contentType.equals(CONTENT_TYPE_TEXT_CSV)
//                        && !contentType.equals(CONTENT_TYPE_TEXT_PLAIN)
//                        && !contentType.equals(CONTENT_TYPE_APPLICATION_CSV)
//                        && !contentType.equals(DEFAULT_CONTENT_TYPE)) {
//                    throw new GcsConnectorException(GcsConnectorErrorCode.VALIDATE_FAILED,
//                            "format is csv, but contentType is not supported.");
//                }
//                break;
//            case FORMAT_JSON:
//                if (!contentType.equals(CONTENT_TYPE_APPLICATION_JSON)
//                        && !contentType.equals(CONTENT_TYPE_TEXT_PLAIN)
//                        && !contentType.equals(DEFAULT_CONTENT_TYPE)) {
//                    throw new GcsConnectorException(GcsConnectorErrorCode.VALIDATE_FAILED,
//                            "format is json, but contentType is not supported.");
//                }
//                break;
//            case FORMAT_DELIMITED:
//                if (!contentType.equals(CONTENT_TYPE_TEXT_PLAIN)
//                        && !contentType.equals(CONTENT_TYPE_TEXT_CSV)
//                        && !contentType.equals(CONTENT_TYPE_TEXT_TSV)
//                        && !contentType.equals(CONTENT_TYPE_APPLICATION_CSV)
//                        && !contentType.equals(DEFAULT_CONTENT_TYPE)) {
//                    throw new GcsConnectorException(GcsConnectorErrorCode.VALIDATE_FAILED,
//                            "format is delimited, but contentType is not supported.");
//                }
//                break;
//            case FORMAT_PARQUET:
//                if (!contentType.equals(DEFAULT_CONTENT_TYPE)) {
//                    throw new GcsConnectorException(GcsConnectorErrorCode.VALIDATE_FAILED,
//                            "format is parquet, but contentType is not supported.");
//                }
//                break;
//            case FORMAT_ORC:
//                if (!contentType.equals(DEFAULT_CONTENT_TYPE)) {
//                    throw new GcsConnectorException(GcsConnectorErrorCode.VALIDATE_FAILED,
//                            "format is orc, but contentType is not supported.");
//                }
//                break;
//            case FORMAT_TSV:
//                if (!contentType.equals(CONTENT_TYPE_TEXT_TSV)
//                        && !contentType.equals(CONTENT_TYPE_TEXT_PLAIN)
//                        && !contentType.equals(DEFAULT_CONTENT_TYPE)) {
//                    throw new GcsConnectorException(GcsConnectorErrorCode.VALIDATE_FAILED,
//                            "format is tsv, but contentType is not supported.");
//                }
//                break;
            default:
                throw new GcsConnectorException(GcsConnectorErrorCode.VALIDATE_FAILED,
                        "unsupported format." + format);
        }
    }

}
