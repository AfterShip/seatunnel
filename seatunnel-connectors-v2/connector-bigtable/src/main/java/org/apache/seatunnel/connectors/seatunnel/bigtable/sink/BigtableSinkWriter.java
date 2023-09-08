package org.apache.seatunnel.connectors.seatunnel.bigtable.sink;

import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.ByteArray;
import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import com.google.cloud.bigtable.hbase.BigtableOptionsFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.bigtable.common.HBaseColumn;
import org.apache.seatunnel.connectors.seatunnel.bigtable.config.BigtableParameters;
import org.apache.seatunnel.connectors.seatunnel.bigtable.exception.BigtableConnectorException;
import org.apache.seatunnel.connectors.seatunnel.bigtable.utils.HBaseConfigurationUtil;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.common.utils.ConfigCenterUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author: gf.xu
 * @email: gf.xu@aftership.com
 * @date: 2023/8/17 15:02
 */
public class BigtableSinkWriter extends AbstractSinkWriter<SeaTunnelRow, Void> {

    private Integer rowkeyColumnIndex;
    private Connection connection;
    private BufferedMutator mutator;
    private SeaTunnelRowType seaTunnelRowType;
    private BigtableParameters bigtableParameters;
    private transient AtomicLong numPendingRequests;

    public BigtableSinkWriter(SeaTunnelRowType seaTunnelRowType,
                              BigtableParameters bigtableParameters,
                              int rowkeyColumnIndex) throws IOException {
        this.seaTunnelRowType = seaTunnelRowType;
        this.rowkeyColumnIndex = rowkeyColumnIndex;
        this.bigtableParameters = bigtableParameters;
        String projectId = bigtableParameters.getProjectId();
        String instanceId = bigtableParameters.getInstanceId();
        String tableId = bigtableParameters.getTableId();
        Credentials credentials = getCredentials(bigtableParameters);
        Configuration oldConfig = HBaseConfigurationUtil.getHBaseConfiguration();
        Configuration newConfig = BigtableConfiguration.withCredentials(oldConfig, credentials);
        BigtableConfiguration.configure(newConfig, projectId, instanceId);
        newConfig.set(BigtableOptionsFactory.BIGTABLE_USE_BATCH, "true");
        bigtableParameters.getBigtableOptions().forEach(newConfig::set);

        this.connection = BigtableConfiguration.connect(newConfig);

        // create a parameter instance, set the table name and custom buffer size
        BufferedMutatorParams params = new BufferedMutatorParams(TableName.valueOf(tableId));
        if (bigtableParameters.getBufferFlushMaxSizeInBytes() > 0) {
            params.writeBufferSize(bigtableParameters.getBufferFlushMaxSizeInBytes());
        }
        this.mutator = connection.getBufferedMutator(params);
        this.numPendingRequests = new AtomicLong(0);
    }

    private Credentials getCredentials(BigtableParameters bigtableParameters) throws IOException {
        String credentials = ConfigCenterUtils.getServiceAccountFromConfigCenter(
                bigtableParameters.getConfigCenterUrl(),
                bigtableParameters.getConfigCenterProject(),
                bigtableParameters.getConfigCenterToken(),
                bigtableParameters.getConfigCenterEnvironment()
        );
        return GoogleCredentials.fromStream(
                new ByteArrayInputStream(credentials.getBytes(StandardCharsets.UTF_8)));
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        byte[] rowKeyBytes = convertColumnToBytes(element, rowkeyColumnIndex);
        Put put = new Put(rowKeyBytes);
        for (int i = 0; i < element.getArity(); i++) {
            //todo to be confirmed
            if (i == rowkeyColumnIndex) {
                continue;
            }
            byte[] columnBytes = convertColumnToBytes(element, i);
            if (columnBytes == null) {
                continue;
            }
            String columnName = seaTunnelRowType.getFieldName(i);
            HBaseColumn hbaseColumn = bigtableParameters.getColumnMappings().get(columnName);
            put.addColumn(
                    Bytes.toBytes(hbaseColumn.getFamily()),
                    Bytes.toBytes(hbaseColumn.getQualifier()),
                    columnBytes);
        }
        mutator.mutate(put);
        if (numPendingRequests.incrementAndGet() >= bigtableParameters.getBufferFlushMaxMutations()) {
            mutator.flush();
            numPendingRequests.set(0);
        }
    }

    private byte[] convertColumnToBytes(SeaTunnelRow row, int index) {
        Object value = row.getField(index);
        if (value == null) {
            return null;
        }
        SeaTunnelDataType<?> fieldType = seaTunnelRowType.getFieldType(index);
        switch (fieldType.getSqlType()) {
            case TINYINT:
                return Bytes.toBytes((Byte) value);
            case SMALLINT:
                return Bytes.toBytes((Short) value);
            case INT:
                return Bytes.toBytes((Integer) value);
            case BIGINT:
                return Bytes.toBytes((Long) value);
            case FLOAT:
                return Bytes.toBytes((Float) value);
            case DOUBLE:
                return Bytes.toBytes((Double) value);
            case BOOLEAN:
                return Bytes.toBytes((Boolean) value);
            case STRING:
                return value.toString()
                        .getBytes(Charset.forName(bigtableParameters.getEnCoding().toString()));
            case BYTES:
                if (value instanceof ByteBuffer) {
                    return ByteArray.copyFrom((ByteBuffer) value).toByteArray();
                } else {
                    return ByteArray.copyFrom((byte[]) value).toByteArray();
                }
            default:
                String errorMsg =
                        String.format(
                                "Bigtable connector does not support this column type [%s]",
                                fieldType.getSqlType());
                throw new BigtableConnectorException(CommonErrorCode.UNSUPPORTED_DATA_TYPE, errorMsg);
        }
    }

    @Override
    public void close() throws IOException {
        try {
            if (this.mutator != null) {
                this.mutator.close();
            }
        } finally {
            if (this.connection != null) {
                this.connection.close();
            }

        }
    }
}
