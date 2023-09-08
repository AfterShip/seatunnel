package org.apache.seatunnel.connectors.seatunnel.bigquery.source;

import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.auto.service.AutoService;
import com.google.cloud.bigquery.*;
import com.google.common.base.Strings;
import org.apache.commons.lang3.StringUtils;
import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.source.*;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.seatunnel.bigquery.config.SourceConfig;
import org.apache.seatunnel.connectors.seatunnel.bigquery.constant.BigQueryConstants;
import org.apache.seatunnel.connectors.seatunnel.bigquery.exception.BigQueryConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.bigquery.exception.BigQueryConnectorException;
import org.apache.seatunnel.connectors.seatunnel.bigquery.util.BigQueryUtils;
import org.apache.seatunnel.connectors.seatunnel.bigquery.util.TypeConvertUtils;
import org.apache.seatunnel.connectors.seatunnel.common.utils.ConfigCenterUtils;
import org.apache.seatunnel.connectors.seatunnel.common.utils.GCPUtils;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import java.time.DateTimeException;
import java.time.LocalDate;
import java.util.Objects;

/**
 * @author ah.he@aftership.com
 * @version 1.0
 * @date 2023/8/2 13:50
 */
@AutoService(SeaTunnelSource.class)
public class BigQuerySource implements SeaTunnelSource<
        SeaTunnelRow, BigQuerySourceSplit, BigQuerySourceState>,
        SupportParallelism,
        SupportColumnProjection {

    private Config pluginConfig;

    private SeaTunnelRowType rowTypeInfo;

    private String serviceAccount;

    private String sql;

    private String temporaryTableName;

    @Override
    public String getPluginName() {
        return "BigQuery";
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        this.pluginConfig = pluginConfig;
        CheckResult checkResult =
                CheckConfigUtil.checkAllExists(
                        pluginConfig,
                        SourceConfig.CONFIG_CENTER_ENVIRONMENT.key(),
                        SourceConfig.CONFIG_CENTER_URL.key(),
                        SourceConfig.CONFIG_CENTER_TOKEN.key(),
                        SourceConfig.CONFIG_CENTER_PROJECT.key(),
                        SourceConfig.TEMP_BUCKET.key(),
                        SourceConfig.PROJECT.key(),
                        SourceConfig.DATASET.key(),
                        SourceConfig.TABLE.key());

        if (!checkResult.isSuccess()) {
            throw new BigQueryConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "PluginName: %s, PluginType: %s, Message: %s",
                            getPluginName(), PluginType.SOURCE, checkResult.getMsg()));
        }
        serviceAccount = ConfigCenterUtils.getServiceAccountFromConfigCenter(
                pluginConfig.getString(SourceConfig.CONFIG_CENTER_TOKEN.key()),
                pluginConfig.getString(SourceConfig.CONFIG_CENTER_URL.key()),
                pluginConfig.getString(SourceConfig.CONFIG_CENTER_ENVIRONMENT.key()),
                pluginConfig.getString(SourceConfig.CONFIG_CENTER_PROJECT.key())
        );
        ServiceAccountCredentials credentials = GCPUtils.getGoogleCredentials(serviceAccount);

        BigQuery bigQuery = BigQueryUtils.getBigQuery(pluginConfig.getString(SourceConfig.PROJECT.key()), credentials);
        sql = buildQuerySQL(bigQuery);

        // dry run bigQuery sql
        QueryJobConfiguration queryConfig =
                QueryJobConfiguration.newBuilder(sql)
                        .setDryRun(true)
                        .setUseQueryCache(false)
                        .build();

        JobStatistics.QueryStatistics statistics;
        try {
            Job job = bigQuery.create(JobInfo.of(queryConfig));
            statistics = job.getStatistics();
        } catch (BigQueryException bigQueryException) {
            throw new BigQueryConnectorException(
                    BigQueryConnectorErrorCode.EXECUTE_SQL_FAILED,
                    String.format(
                            "PluginName: %s, PluginType: %s, Message: %s, Sql: %s",
                            getPluginName(),
                            PluginType.SOURCE,
                            BigQueryConnectorErrorCode.EXECUTE_SQL_FAILED.getDescription(),
                            sql));
        }

        if (null != statistics) {
            Schema schema = statistics.getSchema();
            int fieldsNum = schema.getFields().size();
            SeaTunnelDataType<?>[] seaTunnelDataTypes = new SeaTunnelDataType[fieldsNum];

            String[] fieldNames = new String[fieldsNum];
            for (int i = 0; i < fieldsNum; i++) {
                Field field = schema.getFields().get(i);
                fieldNames[i] = field.getName();
                seaTunnelDataTypes[i] = TypeConvertUtils.convert(field);
            }

            this.rowTypeInfo = new SeaTunnelRowType(fieldNames, seaTunnelDataTypes);
            String tableName = pluginConfig.getString(SourceConfig.TABLE.key());
            temporaryTableName = BigQueryUtils.buildTemporaryTableName(tableName);
        }
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getProducedType() {
        return this.rowTypeInfo;
    }

    @Override
    public SourceReader<SeaTunnelRow, BigQuerySourceSplit> createReader(SourceReader.Context readerContext) throws Exception {
        return new BigQuerySourceReader(readerContext, pluginConfig, serviceAccount, temporaryTableName);
    }

    @Override
    public SourceSplitEnumerator<BigQuerySourceSplit, BigQuerySourceState> createEnumerator(
            SourceSplitEnumerator.Context<BigQuerySourceSplit> enumeratorContext) throws Exception {
        return new BigQuerySourceSplitEnumerator(enumeratorContext, pluginConfig, serviceAccount, sql, temporaryTableName);
    }

    @Override
    public SourceSplitEnumerator<BigQuerySourceSplit, BigQuerySourceState> restoreEnumerator(
            SourceSplitEnumerator.Context<BigQuerySourceSplit> enumeratorContext, BigQuerySourceState checkpointState) throws Exception {
        return new BigQuerySourceSplitEnumerator(enumeratorContext, pluginConfig, checkpointState, serviceAccount, sql, temporaryTableName);
    }

    private String buildQuerySQL(BigQuery bigQuery) {
        String query;
        // if sql is not empty, use sql directly
        if (pluginConfig.hasPath(SourceConfig.SQL.key())
                && StringUtils.isNotBlank(pluginConfig.getString(SourceConfig.SQL.key()))) {
            query = pluginConfig.getString(SourceConfig.SQL.key());
        } else {
            String project = pluginConfig.getString(SourceConfig.PROJECT.key());
            String dataset = pluginConfig.getString(SourceConfig.DATASET.key());
            String tableName = pluginConfig.getString(SourceConfig.TABLE.key());
            String filter = getFilter();
            String partitionFromDate = pluginConfig.hasPath(SourceConfig.PARTITION_FROM.key()) ? pluginConfig.getString(SourceConfig.PARTITION_FROM.key()) : null;
            String partitionToDate = pluginConfig.hasPath(SourceConfig.PARTITION_TO.key()) ? pluginConfig.getString(SourceConfig.PARTITION_TO.key()) : null;

            Table sourceTable = BigQueryUtils.getTable(bigQuery, project, dataset, tableName);
            TableDefinition.Type type = sourceTable.getDefinition().getType();

            if (type == TableDefinition.Type.VIEW || type == TableDefinition.Type.MATERIALIZED_VIEW) {
                query = generateQueryForMaterializingView(project, dataset, tableName, filter);
            } else {
                validatePartitionProperties(bigQuery, project, dataset, tableName, partitionFromDate, partitionToDate);
                query = generateQueryFromTable(bigQuery, partitionFromDate, partitionToDate, filter, project, dataset, tableName);
            }
        }
        return query;
    }

    private void validatePartitionProperties(BigQuery bigQuery, String project, String dataset, String tableName,
                                             String partitionFromDate, String partitionToDate) {
        Table sourceTable = BigQueryUtils.getTable(bigQuery, project, dataset, tableName);

        if (sourceTable.getDefinition() instanceof StandardTableDefinition) {
            TimePartitioning timePartitioning =
                    ((StandardTableDefinition) sourceTable.getDefinition()).getTimePartitioning();
            if (timePartitioning == null) {
                return;
            }
        }

        if (partitionFromDate == null && partitionToDate == null) {
            return;
        }
        LocalDate fromDate = null;
        if (partitionFromDate != null) {
            try {
                fromDate = LocalDate.parse(partitionFromDate);
            } catch (DateTimeException ex) {
                throw new BigQueryConnectorException(
                        SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                        String.format("Invalid partition from date format: %s, " +
                                "Ensure partition from date is of format 'yyyy-MM-dd'.", partitionFromDate));
            }
        }
        LocalDate toDate = null;
        if (partitionToDate != null) {
            try {
                toDate = LocalDate.parse(partitionToDate);
            } catch (DateTimeException ex) {
                throw new BigQueryConnectorException(
                        SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                        String.format("Invalid partition to date format: %s, " +
                                "Ensure partition to date is of format 'yyyy-MM-dd'.", partitionToDate));
            }
        }

        if (fromDate != null && toDate != null && fromDate.isAfter(toDate) && !fromDate.isEqual(toDate)) {
            throw new BigQueryConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format("'Partition From Date' must be before or equal 'Partition To Date'. " +
                            "partitionFromDate: %s, partitionToDate: %s.", partitionFromDate, partitionToDate));
        }
    }

    private String generateQueryFromTable(BigQuery bigQuery, String partitionFromDate, String partitionToDate, String filter,
                                          String project, String dataset, String table) {
        if (partitionFromDate == null && partitionToDate == null && filter == null) {
            return null;
        }
        String queryTemplate = "select * from `%s` where %s";
        Table sourceTable = BigQueryUtils.getTable(bigQuery, project, dataset, table);
        StandardTableDefinition tableDefinition = Objects.requireNonNull(sourceTable).getDefinition();
        TimePartitioning timePartitioning = tableDefinition.getTimePartitioning();
        if (timePartitioning == null && filter == null) {
            return null;
        }
        StringBuilder condition = new StringBuilder();

        if (timePartitioning != null) {
            String timePartitionCondition = generateTimePartitionCondition(tableDefinition, timePartitioning,
                    partitionFromDate, partitionToDate);
            condition.append(timePartitionCondition);
        }

        if (filter != null) {
            if (condition.length() == 0) {
                condition.append(filter);
            } else {
                condition.append(" and (").append(filter).append(")");
            }
        }

        String tableName = project + "." + dataset + "." + table;
        return String.format(queryTemplate, tableName, condition.toString());
    }

    private String generateTimePartitionCondition(StandardTableDefinition tableDefinition,
                                                  TimePartitioning timePartitioning, String partitionFromDate,
                                                  String partitionToDate) {
        StringBuilder timePartitionCondition = new StringBuilder();
        String columnName = timePartitioning.getField() !=
                null ? timePartitioning.getField() : BigQueryConstants.DEFAULT_COLUMN_NAME;

        LegacySQLTypeName columnType = null;
        if (!BigQueryConstants.DEFAULT_COLUMN_NAME.equals(columnName)) {
            columnType = tableDefinition.getSchema().getFields().get(columnName).getType();
        }

        if (partitionFromDate != null) {
            if (LegacySQLTypeName.DATE.equals(columnType)) {
                columnName = "TIMESTAMP(\"" + columnName + "\")";
            }
            timePartitionCondition.append(columnName).append(" >= ").append("TIMESTAMP(\"")
                    .append(partitionFromDate).append("\")");
        }
        if (partitionFromDate != null && partitionToDate != null) {
            timePartitionCondition.append(" and ");
        }
        if (partitionToDate != null) {
            if (LegacySQLTypeName.DATE.equals(columnType)) {
                columnName = "TIMESTAMP(\"" + columnName + "\")";
            }
            timePartitionCondition.append(columnName).append(" < ").append("TIMESTAMP(\"")
                    .append(partitionToDate).append("\")");
        }
        return timePartitionCondition.toString();
    }

    String generateQueryForMaterializingView(String datasetProject, String dataset, String table, String filter) {
        String queryTemplate = "select * from `%s`%s";
        StringBuilder condition = new StringBuilder();

        if (!Strings.isNullOrEmpty(filter)) {
            condition.append(String.format(" where %s", filter));
        }

        String tableName = datasetProject + "." + dataset + "." + table;
        return String.format(queryTemplate, tableName, condition.toString());
    }

    public String getFilter() {
        if (pluginConfig.hasPath(SourceConfig.FILTER.key())
                && StringUtils.isNotBlank(pluginConfig.getString(SourceConfig.FILTER.key()))) {
            String filter = pluginConfig.getString(SourceConfig.FILTER.key());
            // remove the WHERE keyword from the filter if the user adds it at the begging of the expression
            if (filter.toUpperCase().startsWith("WHERE")) {
                filter = filter.substring("WHERE".length());
            }
            return filter;
        } else {
            return "1=1";
        }
    }
}
