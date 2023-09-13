package org.apache.seatunnel.connectors.seatunnel.bigquery.util;

/**
 * Stores the Precision and Scale for the different Numeric (Decimal) types provided by BigQuery
 * https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#numeric_types
 */
public final class BigQueryTypeSize {

    /** Precision and scale of BigQuery Numeric class */
    public static final class Numeric {
        public static final int PRECISION = 38;
        public static final int SCALE = 9;
    }

    /** Precision and Scale of BigQuery BigNumeric class */
    public static final class BigNumeric {
        public static final int PRECISION = 77;
        public static final int SCALE = 38;
    }

    private BigQueryTypeSize() {
        // Avoid the class to ever being called
        throw new AssertionError();
    }
}
