package org.apache.seatunnel.connectors.seatunnel.elasticsearch.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

/**
 * @author wq.pan on 2023/8/7
 * @className ProxyContext
 * @Description
 * @Version: 1.0
 */
@Data
public class ProxyContext {
    public ProxyContext() {
    }

    public ProxyContext(String host, String user, String password) {
        this.host = host;
        this.user = user;
        this.password = password;
    }

    @JsonProperty("host")
    private String host;

    @JsonProperty("user")
    private String user;

    @JsonProperty("password")
    private String password;
}