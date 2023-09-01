package org.apache.seatunnel.connectors.seatunnel.gcs.config;

import lombok.Data;
import lombok.NonNull;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.seatunnel.connectors.seatunnel.gcs.config.GcsSinkConfig.*;

@Data
public class FileSinkConfig {

    private List<String> sinkColumnList;

    private String tmpPath = "/tmp/seatunnel";

    private List<Integer> sinkColumnsIndexInRow;

    private String path;

    private FileFormat fileFormat;

    private String fileFormatContentType;

    private String projectId;


    public FileSinkConfig(@NonNull Config pluginConfig, @NonNull SeaTunnelRowType seaTunnelRowType) {
        this.path = pluginConfig.getString(PATH.key());
        this.fileFormat = FileFormat.valueOf(pluginConfig.getString(FORMAT.key()).toUpperCase());
        this.fileFormatContentType = pluginConfig.getString(CONTENT_TYPE.key());
        this.sinkColumnList = Arrays.asList(seaTunnelRowType.getFieldNames());
        Map<String, Integer> columnsMap =
                new HashMap<>(seaTunnelRowType.getFieldNames().length);
        String[] fieldNames = seaTunnelRowType.getFieldNames();
        for (int i = 0; i < fieldNames.length; i++) {
            columnsMap.put(fieldNames[i].toLowerCase(), i);
        }
        this.sinkColumnsIndexInRow =
                this.sinkColumnList.stream()
                        .map(column -> columnsMap.get(column.toLowerCase()))
                        .collect(Collectors.toList());
        this.projectId = pluginConfig.getString(PROJECT_ID.key());
    }
}
