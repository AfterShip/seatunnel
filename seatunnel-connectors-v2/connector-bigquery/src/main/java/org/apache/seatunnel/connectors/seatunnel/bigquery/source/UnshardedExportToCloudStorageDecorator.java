package org.apache.seatunnel.connectors.seatunnel.bigquery.source;

import com.google.api.services.bigquery.model.*;
import com.google.cloud.hadoop.io.bigquery.BigQueryHelper;
import com.google.cloud.hadoop.io.bigquery.BigQueryStrings;
import com.google.cloud.hadoop.io.bigquery.ExportFileFormat;
import com.google.cloud.hadoop.io.bigquery.UnshardedExportToCloudStorage;
import com.google.common.flogger.GoogleLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;

import javax.annotation.Nullable;
import java.io.IOException;

/**
 * @author ah.he@aftership.com
 * @version 1.0
 * @date 2023/8/23 11:18
 */
public class UnshardedExportToCloudStorageDecorator extends UnshardedExportToCloudStorage {

    private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

    private JobConfigurationExtract extractConfig;

    public UnshardedExportToCloudStorageDecorator(
            Configuration configuration,
            String gcsPath,
            ExportFileFormat fileFormat,
            BigQueryHelper bigQueryHelper,
            String projectId,
            Table tableToExport,
            @Nullable InputFormat<LongWritable, Text> delegateInputFormat,
            JobConfigurationExtract extractConfig) {
        super(
                configuration,
                gcsPath,
                fileFormat,
                bigQueryHelper,
                projectId,
                tableToExport,
                delegateInputFormat);
        this.extractConfig = extractConfig;
    }


    @Override
    public void beginExport() throws IOException {
        // Set source.
        extractConfig.setSourceTable(tableToExport.getTableReference());

        // Set destination.
        extractConfig.setDestinationUris(getExportPaths());
        extractConfig.set(DESTINATION_FORMAT_KEY, fileFormat.getFormatIdentifier());

        JobConfiguration config = new JobConfiguration();
        config.setExtract(extractConfig);

        JobReference jobReference =
                bigQueryHelper.createJobReference(
                        projectId, "exporttocloudstorage", tableToExport.getLocation());

        Job job = new Job();
        job.setConfiguration(config);
        job.setJobReference(jobReference);

        // Insert and run job.
        try {
            Job response = bigQueryHelper.insertJobOrFetchDuplicate(projectId, job);
            logger.atFine().log("Got response '%s'", response);
            exportJobReference = response.getJobReference();
        } catch (IOException e) {
            String error = String.format(
                    "Error while exporting table %s",
                    BigQueryStrings.toString(tableToExport.getTableReference()));
            throw new IOException(error, e);
        }
    }
}
