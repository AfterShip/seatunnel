package org.apache.seatunnel.connectors.seatunnel.spanner;

import org.apache.seatunnel.connectors.seatunnel.spanner.sink.SpannerSinkFactory;
import org.apache.seatunnel.connectors.seatunnel.spanner.source.SpannerSourceFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * @author: gf.xu
 * @email: gf.xu@aftership.com
 * @date: 2023/9/6 19:48
 */
public class SpannerFactoryTest {

    @Test
    void optionRule() {
        Assertions.assertNotNull((new SpannerSourceFactory()).optionRule());
        Assertions.assertNotNull((new SpannerSinkFactory()).optionRule());
    }
}
