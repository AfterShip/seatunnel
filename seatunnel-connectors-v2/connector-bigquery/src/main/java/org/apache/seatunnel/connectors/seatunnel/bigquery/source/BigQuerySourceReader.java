package org.apache.seatunnel.connectors.seatunnel.bigquery.source;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.bigquery.config.SourceConfig;
import org.apache.seatunnel.connectors.seatunnel.bigquery.serialize.BigQueryDeserializer;
import org.apache.seatunnel.connectors.seatunnel.bigquery.serialize.BigQueryRecord;
import org.apache.seatunnel.connectors.seatunnel.bigquery.serialize.SeaTunnelRowDeserializer;
import org.apache.seatunnel.connectors.seatunnel.bigquery.util.BigQueryUtils;

import org.apache.hadoop.conf.Configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.hadoop.io.bigquery.UnshardedInputSplit;

import java.io.IOException;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;

/**
 * @author ah.he@aftership.com
 * @version 1.0
 * @date 2023/8/7 17:14
 */
public class BigQuerySourceReader implements SourceReader<SeaTunnelRow, BigQuerySourceSplit> {
    private static final Logger log = LoggerFactory.getLogger(BigQuerySourceReader.class);
    Context context;

    private final SeaTunnelRowDeserializer deserializer;

    private String projectId;

    private String datasetId;

    private String temporaryTableName;

    private String serviceAccount;

    Deque<BigQuerySourceSplit> splits = new LinkedList<>();

    boolean noMoreSplit;

    private final long pollNextWaitTime = 1000L;

    public BigQuerySourceReader(
            Context context,
            Config pluginConfig,
            String serviceAccount,
            String temporaryTableName) {
        this.context = context;
        this.projectId = pluginConfig.getString(SourceConfig.PROJECT.key());
        this.datasetId = pluginConfig.getString(SourceConfig.DATASET.key());
        this.temporaryTableName = temporaryTableName;
        this.serviceAccount = serviceAccount;
        this.deserializer = new BigQueryDeserializer();
    }

    @Override
    public void open() throws Exception {}

    @Override
    public void close() throws IOException {}

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        synchronized (output.getCheckpointLock()) {
            BigQuerySourceSplit bigQuerySourceSplit = splits.poll();
            if (bigQuerySourceSplit != null) {
                UnshardedInputSplit inputSplit = bigQuerySourceSplit.getSplit();

                Configuration configuration =
                        BigQueryUtils.getBigQueryConfig(
                                serviceAccount, projectId, datasetId, temporaryTableName);
                AvroRecordReaderDecorator avroRecordReader = new AvroRecordReaderDecorator();
                avroRecordReader.initialize(inputSplit, configuration);
                while (avroRecordReader.nextKeyValue()) {
                    SeaTunnelRow seaTunnelRow =
                            deserializer.deserialize(
                                    new BigQueryRecord(avroRecordReader.getCurrentValue()));
                    output.collect(seaTunnelRow);
                }
            } else if (noMoreSplit) {
                // signal to the source that we have reached the end of the data.
                log.info("Closed the bounded BigQuery source");
                context.signalNoMoreElement();
            } else {
                Thread.sleep(pollNextWaitTime);
            }
        }
    }

    @Override
    public List<BigQuerySourceSplit> snapshotState(long checkpointId) throws Exception {
        return Collections.emptyList();
    }

    @Override
    public void addSplits(List<BigQuerySourceSplit> splits) {
        this.splits.addAll(splits);
    }

    @Override
    public void handleNoMoreSplits() {
        noMoreSplit = true;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {}
}
