package org.apache.seatunnel.connectors.seatunnel.bigquery.source;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.bigquery.config.SourceConfig;
import org.apache.seatunnel.connectors.seatunnel.bigquery.exception.BigQueryConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.bigquery.exception.BigQueryConnectorException;
import org.apache.seatunnel.connectors.seatunnel.bigquery.util.BigQueryUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.util.Progressable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.JobConfiguration;
import com.google.api.services.bigquery.model.JobConfigurationExtract;
import com.google.api.services.bigquery.model.JobConfigurationQuery;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableReference;
import com.google.cloud.hadoop.io.bigquery.BigQueryConfiguration;
import com.google.cloud.hadoop.io.bigquery.BigQueryHelper;
import com.google.cloud.hadoop.io.bigquery.Export;
import com.google.cloud.hadoop.io.bigquery.ExportFileFormat;
import com.google.cloud.hadoop.io.bigquery.NoopFederatedExportToCloudStorage;
import com.google.cloud.hadoop.io.bigquery.UnshardedInputSplit;
import com.google.cloud.hadoop.util.ConfigurationUtil;
import com.google.cloud.hadoop.util.HadoopToStringUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.google.cloud.hadoop.io.bigquery.AbstractBigQueryInputFormat.EXTERNAL_TABLE_TYPE;

/**
 * BigQuerySourceSplitEnumerator.
 *
 * @author ah.he@aftership.com
 * @version 1.0
 * @date 2023/8/7 17:46
 */
public class BigQuerySourceSplitEnumerator
        implements SourceSplitEnumerator<BigQuerySourceSplit, BigQuerySourceState> {

    private static final Logger log = LoggerFactory.getLogger(BigQuerySourceSplitEnumerator.class);

    public static final String PROJECT_ID_KEY = "mapred.bq.project.id";

    public static final String INPUT_PROJECT_ID_KEY = "mapred.bq.input.project.id";

    public static final String INPUT_DATASET_ID_KEY = "mapred.bq.input.dataset.id";

    public static final String INPUT_TABLE_ID_KEY = "mapred.bq.input.table.id";

    private Context<BigQuerySourceSplit> context;

    private Config pluginConfig;

    private String serviceAccount;

    private String projectId;

    private String datasetId;

    private String tableName;

    private String temporaryTableName;

    private String sql;

    private BigQueryHelper bigQueryHelper;

    private Export export;

    private Map<Integer, List<BigQuerySourceSplit>> pendingSplit;

    // Used by UnshardedExportToCloudStorage
    private InputFormat<LongWritable, Text> delegateInputFormat;

    private volatile boolean shouldEnumerate;

    private final Object stateLock = new Object();

    public BigQuerySourceSplitEnumerator(
            SourceSplitEnumerator.Context<BigQuerySourceSplit> context,
            Config pluginConfig,
            String serviceAccount,
            String sql,
            String temporaryTableName) {
        this(context, pluginConfig, null, serviceAccount, sql, temporaryTableName);
    }

    public BigQuerySourceSplitEnumerator(
            Context<BigQuerySourceSplit> context,
            Config pluginConfig,
            BigQuerySourceState sourceState,
            String serviceAccount,
            String sql,
            String temporaryTableName) {
        this.context = context;
        this.pluginConfig = pluginConfig;
        this.serviceAccount = serviceAccount;
        this.sql = sql;
        this.temporaryTableName = temporaryTableName;
        this.pendingSplit = new HashMap<>();
        this.delegateInputFormat = new TextInputFormat();
        this.shouldEnumerate = sourceState == null;
        if (sourceState != null) {
            this.shouldEnumerate = sourceState.isShouldEnumerate();
            this.pendingSplit.putAll(sourceState.getPendingSplit());
        }

        init();
    }

    @Override
    public void open() {}

    @Override
    public void run() {
        Set<Integer> readers = context.registeredReaders();

        if (shouldEnumerate) {
            try {
                // Run the query and export the result to a temporary table.
                runJob();
                // Get the splits from the temporary table.
                List<BigQuerySourceSplit> newSplits = getBigQuerySplit();

                synchronized (stateLock) {
                    addPendingSplit(newSplits);
                    shouldEnumerate = false;
                }
            } catch (Exception e) {
                throw new BigQueryConnectorException(
                        BigQueryConnectorErrorCode.GET_BIGQUERY_SPLIT,
                        "Get BigQuery split failed. " + e,
                        e);
            }
            assignSplit(readers);
        }

        log.debug(
                "No more splits to assign." + " Sending NoMoreSplitsEvent to reader {}.", readers);
        readers.forEach(context::signalNoMoreSplits);
    }

    @Override
    public void close() throws IOException {
        cleanupJob();
    }

    @Override
    public void addSplitsBack(List<BigQuerySourceSplit> splits, int subtaskId) {
        log.debug("Add back splits {} to BigQuerySourceSplitEnumerator.", splits);
        if (!splits.isEmpty()) {
            addPendingSplit(splits);
            assignSplit(Collections.singletonList(subtaskId));
        }
    }

    @Override
    public int currentUnassignedSplitSize() {
        return pendingSplit.size();
    }

    @Override
    public void handleSplitRequest(int subtaskId) {
        throw new BigQueryConnectorException(
                CommonErrorCode.UNSUPPORTED_OPERATION,
                String.format("Unsupported handleSplitRequest: %d", subtaskId));
    }

    @Override
    public void registerReader(int subtaskId) {
        log.debug("Register reader {} to BigQuerySourceSplitEnumerator.", subtaskId);
        if (!pendingSplit.isEmpty()) {
            assignSplit(Collections.singletonList(subtaskId));
        }
    }

    @Override
    public BigQuerySourceState snapshotState(long checkpointId) throws Exception {
        synchronized (stateLock) {
            return new BigQuerySourceState(shouldEnumerate, pendingSplit);
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {}

    /** Initialize the BigQuerySourceSplitEnumerator. */
    private void init() {
        this.projectId = pluginConfig.getString(SourceConfig.PROJECT.key());
        this.datasetId = pluginConfig.getString(SourceConfig.DATASET.key());
        this.tableName = pluginConfig.getString(SourceConfig.TABLE.key());

        Bigquery bigQueryService = BigQueryUtils.getBigQueryService(serviceAccount);
        this.bigQueryHelper = new BigQueryHelper(bigQueryService);
    }

    /**
     * Run the query and export the result to a temporary table.
     *
     * @throws IOException if there's an error running the query or exporting the result.
     * @throws InterruptedException if the job is interrupted while waiting for the query to
     *     complete.
     */
    private void runJob() throws IOException, InterruptedException {
        log.info("Run query: {}", sql);
        TableReference sourceTable =
                new TableReference()
                        .setDatasetId(datasetId)
                        .setProjectId(projectId)
                        .setTableId(tableName);
        String location = bigQueryHelper.getTable(sourceTable).getLocation();
        TableReference exportTableReference =
                createExportTableReference(projectId, datasetId, temporaryTableName);

        runQuery(bigQueryHelper, projectId, exportTableReference, sql, location);
    }

    /**
     * Create a TableReference for the export table.
     *
     * @param datasetProjectId the project id of the dataset.
     * @param datasetId the dataset id.
     * @param tableId the table id.
     * @return a TableReference for the export table.
     */
    private TableReference createExportTableReference(
            String datasetProjectId, String datasetId, String tableId) {
        return new TableReference()
                .setProjectId(datasetProjectId)
                .setDatasetId(datasetId)
                .setTableId(tableId);
    }

    /**
     * Run the query and export the result to a temporary table.
     *
     * @param bigQueryHelper the BigQueryHelper to use.
     * @param projectId the project id.
     * @param tableRef the table to export.
     * @param query the query to run.
     * @param location the location of the table to export.
     * @throws IOException if there's an error running the query or exporting the result.
     * @throws InterruptedException if the job is interrupted while waiting for the query to
     *     complete.
     */
    private static void runQuery(
            BigQueryHelper bigQueryHelper,
            String projectId,
            TableReference tableRef,
            String query,
            String location)
            throws IOException, InterruptedException {

        // Create a query statement and query request object.
        JobConfigurationQuery queryConfig = new JobConfigurationQuery();
        queryConfig.setAllowLargeResults(true);
        queryConfig.setQuery(query);
        queryConfig.setUseLegacySql(false);

        // Set the table to put results into.
        queryConfig.setDestinationTable(tableRef);

        queryConfig.setCreateDisposition("CREATE_IF_NEEDED");

        // Require table to be empty.
        queryConfig.setWriteDisposition("WRITE_EMPTY");

        JobConfiguration config = new JobConfiguration();
        config.setQuery(queryConfig);

        JobReference jobReference = getJobReference(bigQueryHelper, projectId, location);

        com.google.api.services.bigquery.model.Job job =
                new com.google.api.services.bigquery.model.Job();
        job.setConfiguration(config);
        job.setJobReference(jobReference);

        // Run the job.
        log.info("Save query result to table: {}.", tableRef.getTableId());
        com.google.api.services.bigquery.model.Job response =
                bigQueryHelper.insertJobOrFetchDuplicate(projectId, job);
        // Create anonymous Progressable object
        Progressable progressable =
                new Progressable() {
                    @Override
                    public void progress() {
                        // TODO(user): ensure task doesn't time out
                    }
                };

        // Poll until job is complete.
        com.google.cloud.hadoop.io.bigquery.BigQueryUtils.waitForJobCompletion(
                bigQueryHelper.getRawBigquery(), projectId, jobReference, progressable);
        if (bigQueryHelper.tableExists(tableRef)) {
            log.info("Set table: {} expiration time to 7 days.", tableRef.getTableId());
            long expirationMillis = System.currentTimeMillis() + TimeUnit.DAYS.toMillis(7);
            Table table = bigQueryHelper.getTable(tableRef).setExpirationTime(expirationMillis);
            bigQueryHelper
                    .getRawBigquery()
                    .tables()
                    .update(
                            tableRef.getProjectId(),
                            tableRef.getDatasetId(),
                            tableRef.getTableId(),
                            table)
                    .execute();
        }
    }

    /** Gets the Job Reference for the BQ job to execute. */
    private static JobReference getJobReference(
            BigQueryHelper bigQueryHelper, String projectId, String location) {
        return bigQueryHelper.createJobReference(projectId, "query_based_export", location);
    }

    /**
     * Get the splits from the temporary table.
     *
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    private List<BigQuerySourceSplit> getBigQuerySplit() throws IOException, InterruptedException {
        List<BigQuerySourceSplit> splits = new ArrayList<>();

        String tempBucket = pluginConfig.getString(SourceConfig.TEMP_BUCKET.key());
        String exportPath = BigQueryUtils.getTemporaryGcsPath(tempBucket);

        Configuration configuration =
                BigQueryUtils.getBigQueryConfig(
                        serviceAccount, projectId, datasetId, temporaryTableName);
        configuration.set(BigQueryConfiguration.TEMP_GCS_PATH.getKey(), exportPath);

        this.export =
                constructExport(
                        configuration,
                        getExportFileFormat(),
                        exportPath,
                        bigQueryHelper,
                        delegateInputFormat);

        // Invoke the export, maybe wait for it to complete.
        this.export.prepare();
        this.export.beginExport();
        this.export.waitForUsableMapReduceInput();

        JobContext context = new JobContextImpl(configuration, new JobID());
        List<InputSplit> inputSplits = this.export.getSplits(context);
        log.info("getSplits -> {}.", HadoopToStringUtil.toString(inputSplits));

        inputSplits.forEach(
                x ->
                        splits.add(
                                new BigQuerySourceSplit(
                                        String.valueOf(x.hashCode()), (UnshardedInputSplit) x)));
        return splits;
    }

    /**
     * Construct an Export object based on the configuration.
     *
     * @param configuration
     * @param format
     * @param exportPath
     * @param bigQueryHelper
     * @param delegateInputFormat
     * @return
     * @throws IOException
     */
    private static Export constructExport(
            Configuration configuration,
            ExportFileFormat format,
            String exportPath,
            BigQueryHelper bigQueryHelper,
            InputFormat<LongWritable, Text> delegateInputFormat)
            throws IOException {
        log.info("Temporary export path: {}.", exportPath);

        // Extract relevant configuration settings.
        Map<String, String> mandatoryConfig =
                ConfigurationUtil.getMandatoryConfig(
                        configuration, BigQueryConfiguration.MANDATORY_CONFIG_PROPERTIES_INPUT);
        String jobProjectId = mandatoryConfig.get(PROJECT_ID_KEY);
        String inputProjectId = mandatoryConfig.get(INPUT_PROJECT_ID_KEY);
        String datasetId = mandatoryConfig.get(INPUT_DATASET_ID_KEY);
        String tableName = mandatoryConfig.get(INPUT_TABLE_ID_KEY);

        TableReference exportTableReference =
                new TableReference()
                        .setDatasetId(datasetId)
                        .setProjectId(inputProjectId)
                        .setTableId(tableName);
        Table table = bigQueryHelper.getTable(exportTableReference);

        if (EXTERNAL_TABLE_TYPE.equals(table.getType())) {
            log.info("Table is already external, so skipping export");
            return new NoopFederatedExportToCloudStorage(
                    configuration,
                    format,
                    bigQueryHelper,
                    jobProjectId,
                    table,
                    delegateInputFormat);
        }
        JobConfigurationExtract extractConfig = new JobConfigurationExtract();
        extractConfig.setUseAvroLogicalTypes(true);

        return new UnshardedExportToCloudStorageDecorator(
                configuration,
                exportPath,
                format,
                bigQueryHelper,
                jobProjectId,
                table,
                delegateInputFormat,
                extractConfig);
    }

    private void addPendingSplit(Collection<BigQuerySourceSplit> splits) {
        int readerCount = context.currentParallelism();
        for (BigQuerySourceSplit split : splits) {
            int ownerReader = getSplitOwner(split.splitId(), readerCount);
            log.info("Assigning {} to {} reader.", split, ownerReader);
            pendingSplit.computeIfAbsent(ownerReader, r -> new ArrayList<>()).add(split);
        }
    }

    private static int getSplitOwner(String tp, int numReaders) {
        return (tp.hashCode() & Integer.MAX_VALUE) % numReaders;
    }

    private void assignSplit(Collection<Integer> readers) {
        log.debug("Assign pendingSplits to readers {}", readers);

        for (int reader : readers) {
            List<BigQuerySourceSplit> assignmentForReader = pendingSplit.remove(reader);
            if (assignmentForReader != null && !assignmentForReader.isEmpty()) {
                log.info("Assign splits {} to reader {}", assignmentForReader, reader);
                try {
                    context.assignSplit(reader, assignmentForReader);
                } catch (Exception e) {
                    log.error(
                            "Failed to assign splits {} to reader {}",
                            assignmentForReader,
                            reader,
                            e);
                    pendingSplit.put(reader, assignmentForReader);
                }
            }
        }
    }

    private static ExportFileFormat getExportFileFormat() {
        return ExportFileFormat.AVRO;
    }

    public void cleanupJob() {
        log.info("Clean up job.");
        try {
            this.export.cleanupExport();
        } catch (IOException ioe) {
            log.error("Could not delete intermediate data from BigQuery export");
        }
        log.info("Delete temporary table: {}.", temporaryTableName);
        BigQueryUtils.deleteBigQueryTable(
                BigQueryUtils.getBigQuery(projectId, serviceAccount),
                projectId,
                datasetId,
                temporaryTableName);
    }
}
