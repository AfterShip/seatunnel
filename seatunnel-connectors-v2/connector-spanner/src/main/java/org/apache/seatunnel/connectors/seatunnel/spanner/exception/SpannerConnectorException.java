package org.apache.seatunnel.connectors.seatunnel.spanner.exception;

import org.apache.seatunnel.common.exception.SeaTunnelErrorCode;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;

/**
 * @author: gf.xu
 * @email: gf.xu@aftership.com
 * @date: 2023/8/25 14:55
 */
public class SpannerConnectorException extends SeaTunnelRuntimeException {

    public SpannerConnectorException(SeaTunnelErrorCode seaTunnelErrorCode, String errorMessage) {
        super(seaTunnelErrorCode, errorMessage);
    }

    public SpannerConnectorException(
            SeaTunnelErrorCode seaTunnelErrorCode, String errorMessage, Throwable cause) {
        super(seaTunnelErrorCode, errorMessage, cause);
    }

    public SpannerConnectorException(SeaTunnelErrorCode seaTunnelErrorCode, Throwable cause) {
        super(seaTunnelErrorCode, cause);
    }
}
