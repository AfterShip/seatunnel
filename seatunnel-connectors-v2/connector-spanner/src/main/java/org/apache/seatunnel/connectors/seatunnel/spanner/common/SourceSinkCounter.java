package org.apache.seatunnel.connectors.seatunnel.spanner.common;

/**
 * @author: gf.xu
 * @email: gf.xu@aftership.com
 * @date: 2023/9/6 18:39
 */
public enum SourceSinkCounter {
    BYTES_READ("bytes-read"),
    BYTES_WRITTEN("bytes-written");

    String name;

    SourceSinkCounter(String name) {
        this.name = name;
    }

    public String counterName() {
        return name;
    }
}
