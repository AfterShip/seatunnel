package org.apache.seatunnel.format.avro.exception;

import org.apache.seatunnel.common.exception.SeaTunnelErrorCode;

public enum AvroFormatErrorCode implements SeaTunnelErrorCode {
    UNSUPPORTED_DATA_TYPE("AVRO-01", "Unsupported data type."),
    SERIALIZATION_ERROR("AVRO-02", "serialize error."),
    FILED_NOT_EXIST("AVRO-03", "Field not exist.");

    private final String code;
    private final String description;

    AvroFormatErrorCode(String code, String description) {
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