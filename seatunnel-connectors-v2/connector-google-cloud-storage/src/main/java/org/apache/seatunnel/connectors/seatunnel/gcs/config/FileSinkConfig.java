package org.apache.seatunnel.connectors.seatunnel.gcs.config;

import lombok.Data;
import lombok.NonNull;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import static org.apache.seatunnel.connectors.seatunnel.gcs.config.GcsSinkConfig.*;

@Data
public class FileSinkConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private String path;

    private FileFormat fileFormat;

    private String projectId;

    private String suffix;

    public FileSinkConfig(@NonNull Config pluginConfig, @NonNull SeaTunnelRowType seaTunnelRowType) {
        this.path = pluginConfig.getString(PATH.key());
        this.fileFormat = FileFormat.valueOf(pluginConfig.getString(FORMAT.key()).toUpperCase());
        this.projectId = pluginConfig.getString(PROJECT_ID.key());
        this.suffix = pluginConfig.hasPath(SUFFIX.key()) ? pluginConfig.getString(SUFFIX.key()) : null;
    }
}
