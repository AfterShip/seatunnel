package org.apache.seatunnel.connectors.seatunnel.gcs.sink;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSimpleSink;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.common.utils.ConfigCenterUtils;
import org.apache.seatunnel.connectors.seatunnel.common.utils.GCPUtils;
import org.apache.seatunnel.connectors.seatunnel.gcs.config.FileSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.gcs.exception.GcsConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.gcs.exception.GcsConnectorException;
import org.apache.seatunnel.connectors.seatunnel.gcs.sink.writer.WriteStrategy;
import org.apache.seatunnel.connectors.seatunnel.gcs.sink.writer.WriteStrategyFactory;
import org.apache.seatunnel.connectors.seatunnel.gcs.util.FileSystemUtils;
import org.apache.seatunnel.connectors.seatunnel.gcs.util.GCSPath;
import org.apache.seatunnel.connectors.seatunnel.gcs.util.GcsUtils;

import org.apache.commons.lang3.StringUtils;

import com.google.auth.Credentials;
import com.google.auto.service.AutoService;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;

import java.io.IOException;
import java.text.SimpleDateFormat;

import static org.apache.seatunnel.connectors.seatunnel.common.config.ConfigCenterConfig.CONFIG_CENTER_ENVIRONMENT;
import static org.apache.seatunnel.connectors.seatunnel.common.config.ConfigCenterConfig.CONFIG_CENTER_PROJECT;
import static org.apache.seatunnel.connectors.seatunnel.common.config.ConfigCenterConfig.CONFIG_CENTER_TOKEN;
import static org.apache.seatunnel.connectors.seatunnel.common.config.ConfigCenterConfig.CONFIG_CENTER_URL;
import static org.apache.seatunnel.connectors.seatunnel.gcs.config.GcsSinkConfig.PATH;
import static org.apache.seatunnel.connectors.seatunnel.gcs.config.GcsSinkConfig.PROJECT_ID;
import static org.apache.seatunnel.connectors.seatunnel.gcs.config.GcsSinkConfig.SUFFIX;

@AutoService(SeaTunnelSink.class)
public class GcsSink extends AbstractSimpleSink<SeaTunnelRow, Void> {

    protected SeaTunnelRowType seaTunnelRowType;
    protected Config pluginConfig;
    protected FileSinkConfig fileSinkConfig;
    protected FileSystemUtils fileSystemUtils;
    protected WriteStrategy writeStrategy;
    protected JobContext jobContext;
    private String serviceAccountJson;

    @Override
    public String getPluginName() {
        return "GCS";
    }

    @Override
    public void setJobContext(JobContext jobContext) {
        this.jobContext = jobContext;
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
            throw new GcsConnectorException(
                    GcsConnectorErrorCode.VALIDATE_FAILED,
                    "path is invalid, please check it." + e.getMessage());
        }
        if (pluginConfig.hasPath(SUFFIX.key())) {
            String suffix = pluginConfig.getString(SUFFIX.key());
            if (StringUtils.isNoneEmpty()) {
                try {
                    new SimpleDateFormat(suffix);
                } catch (IllegalArgumentException e) {
                    throw new GcsConnectorException(
                            GcsConnectorErrorCode.VALIDATE_FAILED,
                            "suffix is invalid, please check it." + e.getMessage());
                }
            }
        }
        serviceAccountJson =
                ConfigCenterUtils.getServiceAccountFromConfigCenter(
                        pluginConfig.getString(CONFIG_CENTER_TOKEN.key()),
                        pluginConfig.getString(CONFIG_CENTER_URL.key()),
                        pluginConfig.getString(CONFIG_CENTER_ENVIRONMENT.key()),
                        pluginConfig.getString(CONFIG_CENTER_PROJECT.key()));
        // check bucket exists
        Credentials credentials = GCPUtils.getCredentials(serviceAccountJson);
        Storage storage =
                GcsUtils.getStorage(pluginConfig.getString(PROJECT_ID.key()), credentials);
        try {
            storage.get(bucket);
        } catch (StorageException e) {
            throw new GcsConnectorException(
                    GcsConnectorErrorCode.VALIDATE_FAILED,
                    String.format("Unable to access or create bucket %s. ", bucket)
                            + "Ensure you entered the correct bucket path and have permissions for it."
                            + e.getMessage());
        }
    }

    @Override
    public void setTypeInfo(SeaTunnelRowType seaTunnelRowType) {
        this.seaTunnelRowType = seaTunnelRowType;
        this.fileSinkConfig = new FileSinkConfig(pluginConfig, seaTunnelRowType);
        this.writeStrategy =
                WriteStrategyFactory.of(fileSinkConfig.getFileFormat(), fileSinkConfig);
        this.fileSystemUtils =
                new FileSystemUtils(fileSinkConfig.getProjectId(), serviceAccountJson);
        this.writeStrategy.setSeaTunnelRowTypeInfo(seaTunnelRowType);
        this.writeStrategy.setFileSystemUtils(fileSystemUtils);
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getConsumedType() {
        return this.seaTunnelRowType;
    }

    @Override
    public AbstractSinkWriter<SeaTunnelRow, Void> createWriter(SinkWriter.Context context)
            throws IOException {
        return new GcsSinkWriter(writeStrategy, context);
    }
}
