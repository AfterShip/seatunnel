package org.apache.seatunnel.connectors.seatunnel.gcs.util;

import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS;
import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.gcs.exception.GcsConnectorException;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import static org.apache.parquet.avro.AvroReadSupport.READ_INT96_AS_FIXED;
import static org.apache.parquet.avro.AvroSchemaConverter.ADD_LIST_ELEMENT_RECORDS;
import static org.apache.parquet.avro.AvroWriteSupport.WRITE_FIXED_AS_INT96;
import static org.apache.parquet.avro.AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE;

@Slf4j
public class FileSystemUtils implements Serializable {
    private static final int WRITE_BUFFER_SIZE = 2048;
    private transient Configuration configuration;
    private String projectId;

    public FileSystemUtils(String projectId) {
        this.projectId = projectId;
    }

    public Configuration getConfiguration(String projectId, String path) {
        Configuration configuration = new Configuration();
        configuration.set(
                String.format("fs.gs.impl", "hdfs"), GoogleHadoopFileSystem.class.getName());
        configuration.set("fs.AbstractFileSystem.gs.impl", GoogleHadoopFS.class.getName());
        configuration.set("fs.gs.project.id", projectId);
        configuration.set("fs.gs.system.bucket", GCSPath.from(path).getBucket());
        configuration.set("fs.gs.path.encoding", "uri-path");
        configuration.set("fs.gs.working.dir", GCSPath.ROOT_DIR);
        configuration.set("fs.gs.impl.disable.cache", "true");
        configuration.setBoolean(READ_INT96_AS_FIXED, true);
        configuration.setBoolean(WRITE_FIXED_AS_INT96, true);
        configuration.setBoolean(ADD_LIST_ELEMENT_RECORDS, false);
        configuration.setBoolean(WRITE_OLD_LIST_STRUCTURE, false);
        return configuration;
    }

    public FileSystem getFileSystem(@NonNull String path) throws IOException {
        if (configuration == null) {
            configuration = getConfiguration(projectId, path);
        }
        FileSystem fileSystem =
                FileSystem.get(URI.create(path.replaceAll("\\\\", "/")), configuration);
        fileSystem.setWriteChecksum(false);
        return fileSystem;
    }

    public FSDataOutputStream getOutputStream(@NonNull String outFilePath) throws IOException {
        FileSystem fileSystem = getFileSystem(outFilePath);
        Path path = new Path(outFilePath);
        return fileSystem.create(path, true, WRITE_BUFFER_SIZE);
    }

    public void createFile(@NonNull String filePath) throws IOException {
        FileSystem fileSystem = getFileSystem(filePath);
        Path path = new Path(filePath);
        if (!fileSystem.createNewFile(path)) {
            throw new GcsConnectorException(
                    CommonErrorCode.FILE_OPERATION_FAILED, "create file " + filePath + " error");
        }
    }

    public void deleteFile(@NonNull String file) throws IOException {
        FileSystem fileSystem = getFileSystem(file);
        Path path = new Path(file);
        if (fileSystem.exists(path)) {
            if (!fileSystem.delete(path, true)) {
                throw new GcsConnectorException(
                        CommonErrorCode.FILE_OPERATION_FAILED, "delete file " + file + " error");
            }
        }
    }

    /**
     * rename file
     *
     * @param oldName old file name
     * @param newName target file name
     * @param rmWhenExist if this is true, we will delete the target file when it already exists
     * @throws IOException throw IOException
     */
    public void renameFile(@NonNull String oldName, @NonNull String newName, boolean rmWhenExist)
            throws IOException {
        FileSystem fileSystem = getFileSystem(newName);
        log.info("begin rename file oldName :[" + oldName + "] to newName :[" + newName + "]");

        Path oldPath = new Path(oldName);
        Path newPath = new Path(newName);

        if (!fileExist(oldPath.toString())) {
            log.warn(
                    "rename file :["
                            + oldPath
                            + "] to ["
                            + newPath
                            + "] already finished in the last commit, skip");
            return;
        }

        if (rmWhenExist) {
            if (fileExist(newName) && fileExist(oldName)) {
                fileSystem.delete(newPath, true);
                log.info("Delete already file: {}", newPath);
            }
        }
        if (!fileExist(newPath.getParent().toString())) {
            createDir(newPath.getParent().toString());
        }

        if (fileSystem.rename(oldPath, newPath)) {
            log.info("rename file :[" + oldPath + "] to [" + newPath + "] finish");
        } else {
            throw new GcsConnectorException(
                    CommonErrorCode.FILE_OPERATION_FAILED,
                    "rename file :[" + oldPath + "] to [" + newPath + "] error");
        }
    }

    public void createDir(@NonNull String filePath) throws IOException {
        FileSystem fileSystem = getFileSystem(filePath);
        Path dfs = new Path(filePath);
        if (!fileSystem.mkdirs(dfs)) {
            throw new GcsConnectorException(
                    CommonErrorCode.FILE_OPERATION_FAILED, "create dir " + filePath + " error");
        }
    }

    public boolean fileExist(@NonNull String filePath) throws IOException {
        FileSystem fileSystem = getFileSystem(filePath);
        Path fileName = new Path(filePath);
        return fileSystem.exists(fileName);
    }

    /** get the dir in filePath */
    public List<Path> dirList(@NonNull String filePath) throws IOException {
        FileSystem fileSystem = getFileSystem(filePath);
        List<Path> pathList = new ArrayList<>();
        if (!fileExist(filePath)) {
            return pathList;
        }
        Path fileName = new Path(filePath);
        FileStatus[] status = fileSystem.listStatus(fileName);
        if (status != null) {
            for (FileStatus fileStatus : status) {
                if (fileStatus.isDirectory()) {
                    pathList.add(fileStatus.getPath());
                }
            }
        }
        return pathList;
    }
}

