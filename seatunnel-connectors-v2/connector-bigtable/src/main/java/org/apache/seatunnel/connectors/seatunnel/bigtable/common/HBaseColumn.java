package org.apache.seatunnel.connectors.seatunnel.bigtable.common;

import java.io.Serializable;

/**
 * This class represents a reference to HBase column
 *
 * @author: gf.xu
 * @email: gf.xu@aftership.com
 * @date: 2023/8/17 19:35
 */
public class HBaseColumn implements Serializable {

    private static final String FAMILY_QUALIFIER_DELIMITER = ":";
    private final String family;
    private final String qualifier;

    private HBaseColumn(String family, String qualifier) {
        this.family = family;
        this.qualifier = qualifier;
    }

    public static HBaseColumn fromFamilyAndQualifier(String family, String qualifier) {
        return new HBaseColumn(family, qualifier);
    }

    /**
     * Parses the provided full name and returns hbase column with family and qualifier.
     *
     * @param fullName full name
     * @return hbase column with family and qualifier
     * @throws IllegalArgumentException if provided full name does not comply with
     *     'family:qualifier' format
     */
    public static HBaseColumn fromFullName(String fullName) {
        if (!fullName.contains(FAMILY_QUALIFIER_DELIMITER)) {
            throw new IllegalArgumentException(
                    "Wrong name format. Expected format is 'family:qualifier'");
        }
        int delimiterIndex = fullName.indexOf(FAMILY_QUALIFIER_DELIMITER);
        String family = fullName.substring(0, delimiterIndex);
        String qualifier = fullName.substring(delimiterIndex + FAMILY_QUALIFIER_DELIMITER.length());
        return new HBaseColumn(family, qualifier);
    }

    public String getFamily() {
        return family;
    }

    public String getQualifier() {
        return qualifier;
    }

    public String getQualifiedName() {
        return String.format("%s%s%s", family, FAMILY_QUALIFIER_DELIMITER, qualifier);
    }
}
