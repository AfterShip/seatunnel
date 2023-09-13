package org.apache.seatunnel.connectors.seatunnel.bigquery.serialize;

import org.apache.avro.generic.GenericData;

import lombok.Getter;
import lombok.ToString;

/**
 * @author ah.he@aftership.com
 * @version 1.0
 * @date 2023/8/8 19:11
 */
@Getter
@ToString
public class BigQueryRecord {

    private GenericData.Record record;

    public BigQueryRecord(GenericData.Record record) {
        this.record = record;
    }
}
