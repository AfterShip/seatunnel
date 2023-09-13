package org.apache.seatunnel.connectors.seatunnel.clickhouse.config;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public class ProxyContext {
    public ProxyContext() {}

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

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ProxyContext that = (ProxyContext) o;
        return Objects.equals(host, that.host)
                && Objects.equals(user, that.user)
                && Objects.equals(password, that.password);
    }

    @Override
    public int hashCode() {
        return Objects.hash(host, user, password);
    }
}
