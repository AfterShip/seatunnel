package org.apache.seatunnel.connectors.seatunnel.gcs.exception;

import org.apache.seatunnel.common.exception.SeaTunnelErrorCode;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;

/**
 * @author wq.pan on 2023/8/30
 * @className GcsConnectorException @Description @Version: 1.0
 */
public class GcsConnectorException extends SeaTunnelRuntimeException {

    public GcsConnectorException(SeaTunnelErrorCode seaTunnelErrorCode, String errorMessage) {
        super(seaTunnelErrorCode, errorMessage);
    }

    public GcsConnectorException(
            SeaTunnelErrorCode seaTunnelErrorCode, String errorMessage, Throwable cause) {
        super(seaTunnelErrorCode, errorMessage, cause);
    }
}
