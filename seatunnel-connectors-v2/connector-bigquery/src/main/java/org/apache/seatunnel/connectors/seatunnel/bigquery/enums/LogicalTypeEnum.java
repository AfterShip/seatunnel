package org.apache.seatunnel.connectors.seatunnel.bigquery.enums;

import org.apache.avro.Schema;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * @author ah.he@aftership.com
 * @version 1.0
 * @date 2023/8/21 19:05
 */
public enum LogicalTypeEnum {
    DATE(Schema.Type.INT, "date"),
    TIMESTAMP_MILLIS(Schema.Type.LONG, "timestamp-millis"),
    TIMESTAMP_MICROS(Schema.Type.LONG, "timestamp-micros"),
    TIME_MILLIS(Schema.Type.INT, "time-millis"),
    TIME_MICROS(Schema.Type.LONG, "time-micros"),
    DECIMAL(Schema.Type.BYTES, "decimal"),
    LOCAL_TIMESTAMP_MILLIS(Schema.Type.STRING, "local-timestamp-millis"),
    LOCAL_TIMESTAMP_MICROS(Schema.Type.STRING, "local-timestamp-micros"),
    DATETIME(Schema.Type.STRING, "datetime");

    private final Schema.Type type;
    private final String token;

    private static final Map<String, LogicalTypeEnum> LOOKUP_BY_TOKEN;

    static {
        Map<String, LogicalTypeEnum> map = new HashMap<>();
        for (LogicalTypeEnum logicalType : values()) {
            map.put(logicalType.token, logicalType);
        }
        LOOKUP_BY_TOKEN = Collections.unmodifiableMap(map);
    }

    LogicalTypeEnum(Schema.Type type, String token) {
        this.type = type;
        this.token = token;
    }

    /** @return returns the token string associated with logical type. */
    public String getToken() {
        return token;
    }

    public static LogicalTypeEnum fromToken(String token) {
        LogicalTypeEnum logicalType = LOOKUP_BY_TOKEN.get(token);
        if (logicalType != null) {
            return logicalType;
        }
        throw new IllegalArgumentException("Unknown logical type for token: " + token);
    }
}
