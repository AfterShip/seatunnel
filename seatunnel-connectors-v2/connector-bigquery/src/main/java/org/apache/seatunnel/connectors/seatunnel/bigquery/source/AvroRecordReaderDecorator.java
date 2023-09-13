package org.apache.seatunnel.connectors.seatunnel.bigquery.source;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;

import com.google.cloud.hadoop.io.bigquery.AvroRecordReader;

import java.io.IOException;

/**
 * @author ah.he@aftership.com
 * @version 1.0
 * @date 2023/8/18 12:33
 */
public class AvroRecordReaderDecorator extends AvroRecordReader {
    public void initialize(InputSplit inputSplit, Configuration conf) throws IOException {
        initializeInternal(inputSplit, conf);
    }
}
