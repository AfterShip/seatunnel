package org.apache.seatunnel.connectors.seatunnel.bigquery.exception;

import org.apache.seatunnel.common.exception.SeaTunnelErrorCode;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;

/**
 * @author ah.he@aftership.com
 * @version 1.0
 * @date 2023/8/2 19:36
 */
public class BigQueryConnectorException extends SeaTunnelRuntimeException {

    public BigQueryConnectorException(
            SeaTunnelErrorCode seaTunnelErrorCode, String errorMessage) {
        super(seaTunnelErrorCode, errorMessage);
    }

    public BigQueryConnectorException(
            SeaTunnelErrorCode seaTunnelErrorCode, String errorMessage, Throwable cause) {
        super(seaTunnelErrorCode, errorMessage, cause);
    }

    public BigQueryConnectorException(SeaTunnelErrorCode seaTunnelErrorCode, Throwable cause) {
        super(seaTunnelErrorCode, cause);
    }
}
