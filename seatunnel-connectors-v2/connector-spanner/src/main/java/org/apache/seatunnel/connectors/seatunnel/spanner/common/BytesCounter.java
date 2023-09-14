package org.apache.seatunnel.connectors.seatunnel.spanner.common;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Counts number of bytes read/written to Spanner
 *
 * @author: gf.xu
 * @email: gf.xu@aftership.com
 * @date: 2023/9/6 17:46
 */
public class BytesCounter {

    private final AtomicLong counter;

    public BytesCounter() {
        counter = new AtomicLong(0);
    }

    public void increment(long value) {
        counter.addAndGet(value);
    }

    public long getValue() {
        return counter.get();
    }
}
