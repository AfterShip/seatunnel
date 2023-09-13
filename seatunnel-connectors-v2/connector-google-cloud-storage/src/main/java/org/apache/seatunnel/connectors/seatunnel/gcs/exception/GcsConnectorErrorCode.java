package org.apache.seatunnel.connectors.seatunnel.gcs.exception;

import org.apache.seatunnel.common.exception.SeaTunnelErrorCode;

public enum GcsConnectorErrorCode implements SeaTunnelErrorCode {
    VALIDATE_FAILED("Gcs-001", "config validate failed.");

    private final String code;
    private final String description;

    GcsConnectorErrorCode(String code, String description) {
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
