package org.apache.seatunnel.connectors.seatunnel.gcs.sink.writer;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.seatunnel.api.serialization.SerializationSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.gcs.config.FileSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.gcs.exception.GcsConnectorException;
import org.apache.seatunnel.connectors.seatunnel.gcs.util.FileSystemUtils;
import org.apache.seatunnel.format.avro.AvroSerializationSchema;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;

@Slf4j
public class AvroWriteStrategy extends AbstractWriteStrategy {

    private FileSinkConfig fileSinkConfig;
    private SerializationSchema serializationSchema;
    private final Map<String, Boolean> isFirstWrite;
    private final LinkedHashMap<String, FSDataOutputStream> beingWrittenOutputStream;
    private String tmpDirectory;
    private List<Integer> sinkColumnsIndexInRow;

    public AvroWriteStrategy(FileSinkConfig fileSinkConfig) {
        super(fileSinkConfig);
        this.fileSinkConfig = fileSinkConfig;
        this.tmpDirectory = fileSinkConfig.getTmpPath();
        this.sinkColumnsIndexInRow = fileSinkConfig.getSinkColumnsIndexInRow();
        this.isFirstWrite = new HashMap<>();
        this.beingWrittenOutputStream = new LinkedHashMap<>();
    }

    @Override
    public void setSeaTunnelRowTypeInfo(SeaTunnelRowType seaTunnelRowType) {
        super.setSeaTunnelRowTypeInfo(seaTunnelRowType);
        this.serializationSchema = new AvroSerializationSchema(seaTunnelRowType);
    }

    @Override
    public void write(SeaTunnelRow seaTunnelRow) {
        super.write(seaTunnelRow);
        String filePath = getOrCreateFilePath(seaTunnelRow);
        FSDataOutputStream outputStream = getOrCreateOutputStream(filePath);
        try {
            if (isFirstWrite.get(filePath) == null) {
                isFirstWrite.put(filePath, false);
            } else {
                outputStream.writeBytes("\n");
            }
            outputStream.write(serializationSchema.serialize(seaTunnelRow.copy(
                    sinkColumnsIndexInRow.stream()
                            .mapToInt(Integer::intValue)
                            .toArray())));
        } catch (Exception e) {
            throw new GcsConnectorException(CommonErrorCode.FILE_OPERATION_FAILED,
                    String.format("Open file output stream [%s] failed", filePath),
                    e);
        }
    }

    @Override
    public void finishAndCloseFile() {
        //todo _SUCCESS
        beingWrittenOutputStream.forEach(
                (key, value) -> {
                    try {
                        value.flush();
                    } catch (IOException e) {
                        throw new GcsConnectorException(
                                CommonErrorCode.FLUSH_DATA_FAILED,
                                String.format("Flush data to this file [%s] failed", key),
                                e);
                    } finally {
                        try {
                            value.close();
                        } catch (IOException e) {
                            log.error("error when close output stream {}", key, e);
                        }
                    }
                    needMoveFiles.put(key, getTargetLocation(key));
                });
        beingWrittenOutputStream.clear();
        isFirstWrite.clear();
    }

    private FSDataOutputStream getOrCreateOutputStream(@NonNull String filePath) {
        FSDataOutputStream fsDataOutputStream = beingWrittenOutputStream.get(filePath);
        if (fsDataOutputStream == null) {
            try {
                fsDataOutputStream = fileSystemUtils.getOutputStream(filePath);
                beingWrittenOutputStream.put(filePath, fsDataOutputStream);
                isFirstWrite.put(filePath, true);
            } catch (IOException e) {
                throw new GcsConnectorException(
                        CommonErrorCode.FILE_OPERATION_FAILED,
                        String.format("Open file output stream [%s] failed", filePath),
                        e);
            }
        }
        return fsDataOutputStream;
    }


}
