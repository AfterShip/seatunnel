package org.apache.seatunnel.connectors.seatunnel.gcs.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

public class GcsSinkConfig {

    public static final Option<String> PROJECT_ID =
            Options.key("project_id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Google Cloud Project ID, which uniquely identifies a project.It can be found on the Dashboard in the Google Cloud Platform Console.");

    public static final Option<String> PATH =
            Options.key("path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The path to write to. For example, gs://<bucket>/path/to/directory");

    public static final Option<String> FORMAT =
            Options.key("format")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The format to write in. The format must be one of 'json', 'avro', 'parquet', 'csv', 'tsv', or 'delimited'.");

    public static final Option<String> SUFFIX =
            Options.key("suffix")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The time format for the output directory that will be appended to the path. " +
                            " For example, the format 'yyyy-MM-dd-HH-mm' will result in a directory of the form '2015-01-01-20-42'. " +
                            " If not specified, nothing will be appended to the path.");

    public static final Option<String> LOCATION =
            Options.key("location")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The location where the gcs bucket will get created. This value is ignored if the bucket already exists");

    public static final Option<String> CONTENT_TYPE =
            Options.key("content_type")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The Content Type property is used to indicate the media type of the resource. Defaults to 'application/octet-stream'.");
}
