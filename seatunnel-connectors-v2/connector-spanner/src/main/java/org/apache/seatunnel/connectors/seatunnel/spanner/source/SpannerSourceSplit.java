package org.apache.seatunnel.connectors.seatunnel.spanner.source;

import com.google.cloud.spanner.BatchTransactionId;
import com.google.cloud.spanner.Partition;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;
import org.apache.seatunnel.api.source.SourceSplit;

/**
 * @author: gf.xu
 * @email: gf.xu@aftership.com
 * @date: 2023/9/6 10:54
 */
@Getter
@AllArgsConstructor
@ToString
public class SpannerSourceSplit implements SourceSplit {

    private static final long serialVersionUID = -1L;


   private BatchTransactionId transactionId;

    private Partition partition;

    @Override
    public String splitId() {
        return partition.toString();
    }
}
