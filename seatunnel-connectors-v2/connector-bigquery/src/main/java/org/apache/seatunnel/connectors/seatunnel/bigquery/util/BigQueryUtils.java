package org.apache.seatunnel.connectors.seatunnel.bigquery.util;

import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.connectors.seatunnel.bigquery.exception.BigQueryConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.bigquery.exception.BigQueryConnectorException;
import org.apache.seatunnel.connectors.seatunnel.common.utils.GCPUtils;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.security.UserGroupInformation;

import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.bigquery.Bigquery;
import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.hadoop.io.bigquery.BigQueryConfiguration;
import com.google.cloud.hadoop.util.RetryHttpInitializer;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;

/**
 * @author ah.he@aftership.com
 * @version 1.0
 * @date 2023/8/2 20:22
 */
public class BigQueryUtils {

    private static final String TEMPORARY_BUCKET_FORMAT =
            "gs://%s/integration_bigquery_source/hadoop/input/%s";

    // Objects for handling HTTP transport and JSON formatting of API calls
    private static final HttpTransport HTTP_TRANSPORT = new NetHttpTransport();

    private static final JsonFactory JSON_FACTORY = new JacksonFactory();

    private static final String APP_NAME = "BigQuery Source Connector";

    public static final String MAP_REDUCE_JSON_KEY_PREFIX = "mapred.bq";

    public static final String ROOT_DIR = "/";

    public static com.google.api.services.bigquery.Bigquery getBigQueryService(
            String serviceAccount) {
        if (StringUtils.isNotBlank(serviceAccount)) {
            return new Bigquery.Builder(
                            HTTP_TRANSPORT,
                            JSON_FACTORY,
                            new RetryHttpInitializer(
                                    GCPUtils.getCredential(serviceAccount), APP_NAME))
                    .setApplicationName(APP_NAME)
                    .build();
        }
        return new Bigquery.Builder(HTTP_TRANSPORT, JSON_FACTORY, null)
                .setApplicationName(APP_NAME)
                .build();
    }

    public static BigQuery getBigQuery(String project, Credentials credentials) {
        BigQueryOptions.Builder bigqueryBuilder =
                BigQueryOptions.newBuilder().setProjectId(project);
        if (credentials != null) {
            bigqueryBuilder.setCredentials(credentials);
        }
        BigQuery bigQuery = bigqueryBuilder.build().getService();
        if (null == bigQuery) {
            throw new BigQueryConnectorException(
                    BigQueryConnectorErrorCode.GET_SERVICE_FAILED,
                    String.format("Get BigQuery service failed. project: %s'", project));
        }
        return bigQuery;
    }

    public static BigQuery getBigQuery(String project, String serviceAccount) {
        GoogleCredentials credentials = GCPUtils.getGoogleCredentials(serviceAccount);
        return getBigQuery(project, credentials);
    }

    public static Table getTable(
            BigQuery bigQuery, String project, String dataset, String tableName) {
        Table table = bigQuery.getTable(TableId.of(dataset, tableName));
        if (null == table) {
            throw new BigQueryConnectorException(
                    SeaTunnelAPIErrorCode.TABLE_NOT_EXISTED,
                    String.format(
                            "BigQuery table '%s:%s.%s' does not exist.",
                            project, dataset, tableName));
        }
        return table;
    }

    public static Schema getTableSchema(
            BigQuery bigQuery, String project, String dataset, String tableName) {
        Table table = getTable(bigQuery, project, dataset, tableName);
        Schema schema = table.getDefinition().getSchema();
        if (schema == null) {
            throw new BigQueryConnectorException(
                    BigQueryConnectorErrorCode.SCHEMA_NOT_EXISTED,
                    String.format(
                            "Cannot read from table '%s:%s.%s' because it has no schema.",
                            project, dataset, tableName));
        }
        return schema;
    }

    /** Delete a table. */
    public static void deleteBigQueryTable(
            BigQuery bigQuery, String project, String dataset, String tableName) {
        TableId tableId = TableId.of(dataset, tableName);
        if (bigQuery.getTable(tableId) == null) {
            return;
        }
        boolean deleted = bigQuery.delete(tableId);
        if (!deleted) {
            throw new BigQueryConnectorException(
                    BigQueryConnectorErrorCode.DELETE_TABLE_FAILED,
                    String.format("Delete table '%s:%s.%s' failed.", project, dataset, tableName));
        }
    }

    /** Build GCS path for a supplied bucket. */
    public static String getTemporaryGcsPath(String bucket) {
        return String.format(TEMPORARY_BUCKET_FORMAT, bucket, UUID.randomUUID());
    }

    /**
     * Build a temporary table name.
     *
     * @param tableName the name of the table to export.
     * @return a temporary table name.
     */
    public static String buildTemporaryTableName(String tableName) {
        return tableName + "_" + UUID.randomUUID().toString().replace("-", "");
    }

    public static Configuration getBigQueryConfig(
            String serviceAccount, String projectId, String dataset, String tableName) {
        Job job = null;
        try {
            job = Job.getInstance();
            // some input formats require the credentials to be present in the job.
            if (UserGroupInformation.isSecurityEnabled()) {
                org.apache.hadoop.security.Credentials credentials =
                        UserGroupInformation.getCurrentUser().getCredentials();
                job.getCredentials().addAll(credentials);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        Configuration configuration = job.getConfiguration();
        configuration.clear();
        if (serviceAccount != null) {
            final Map<String, String> authProperties =
                    GCPUtils.generateAuthProperties(
                            serviceAccount,
                            MAP_REDUCE_JSON_KEY_PREFIX,
                            GCPUtils.CLOUD_JSON_KEYFILE_PREFIX);
            authProperties.forEach(configuration::set);
        }
        configuration.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem");
        configuration.set(
                "fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS");
        configuration.set("fs.gs.project.id", projectId);
        configuration.set("fs.gs.working.dir", ROOT_DIR);
        configuration.set(BigQueryConfiguration.PROJECT_ID.getKey(), projectId);
        configuration.set(BigQueryConfiguration.INPUT_PROJECT_ID.getKey(), projectId);
        configuration.set(BigQueryConfiguration.INPUT_DATASET_ID.getKey(), dataset);
        configuration.set(BigQueryConfiguration.INPUT_TABLE_ID.getKey(), tableName);
        return configuration;
    }
}
