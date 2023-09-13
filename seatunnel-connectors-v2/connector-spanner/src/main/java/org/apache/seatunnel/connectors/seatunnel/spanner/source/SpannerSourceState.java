package org.apache.seatunnel.connectors.seatunnel.spanner.source;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * @author: gf.xu
 * @email: gf.xu@aftership.com
 * @date: 2023/9/6 11:04
 */
@Getter
@AllArgsConstructor
public class SpannerSourceState implements Serializable {

    private boolean shouldEnumerate;

    private Map<Integer, List<SpannerSourceSplit>> pendingSplit;
}
