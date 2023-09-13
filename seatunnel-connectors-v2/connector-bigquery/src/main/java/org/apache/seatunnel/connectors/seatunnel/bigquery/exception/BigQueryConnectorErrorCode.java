package org.apache.seatunnel.connectors.seatunnel.bigquery.exception;

import org.apache.seatunnel.common.exception.SeaTunnelErrorCode;

/**
 * @author ah.he@aftership.com
 * @version 1.0
 * @date 2023/8/4 16:39
 */
public enum BigQueryConnectorErrorCode implements SeaTunnelErrorCode {
    GET_SERVICE_FAILED("BIGQUERY-01", "Get BigQuery service failed."),
    SCHEMA_NOT_EXISTED("BIGQUERY-02", "Schema not existed."),
    EXECUTE_SQL_FAILED("BIGQUERY-03", "Execute sql failed."),
    UNSUPPORTED("BIGQUERY-04", "Unsupported."),
    BIGQUERY_VIEW_DESTINATION_TABLE_CREATION_FAILED(
            "BIGQUERY-05", "Bigquery view destination table creation failed."),
    GET_BIGQUERY_SPLIT("BIGQUERY-06", "Get splits failed."),
    DELETE_TABLE_FAILED("BIGQUERY-07", "Delete Table failed.");

    private final String code;
    private final String description;

    BigQueryConnectorErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return code;
    }

    @Override
    public String getDescription() {
        return description;
    }
}
