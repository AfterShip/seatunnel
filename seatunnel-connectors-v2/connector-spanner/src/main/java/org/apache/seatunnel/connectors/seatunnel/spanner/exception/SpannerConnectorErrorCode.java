package org.apache.seatunnel.connectors.seatunnel.spanner.exception;

import org.apache.seatunnel.common.exception.SeaTunnelErrorCode;

/**
 * @author: gf.xu
 * @email: gf.xu@aftership.com
 * @date: 2023/9/6 16:44
 */
public enum SpannerConnectorErrorCode implements SeaTunnelErrorCode {
    SPANNER_CONNECTOR_ERROR("SPANNER-01", "Failed to create spanner service.");

    private final String code;
    private final String description;

    SpannerConnectorErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return null;
    }

    @Override
    public String getDescription() {
        return null;
    }
}
