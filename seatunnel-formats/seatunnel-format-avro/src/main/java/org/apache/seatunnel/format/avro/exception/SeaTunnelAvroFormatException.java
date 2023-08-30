package org.apache.seatunnel.format.avro.exception;


import org.apache.seatunnel.common.exception.SeaTunnelErrorCode;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;

public class SeaTunnelAvroFormatException extends SeaTunnelRuntimeException {

    public SeaTunnelAvroFormatException(
            SeaTunnelErrorCode seaTunnelErrorCode, String errorMessage) {
        super(seaTunnelErrorCode, errorMessage);
    }
}
