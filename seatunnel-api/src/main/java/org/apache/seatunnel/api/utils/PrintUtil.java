package org.apache.seatunnel.api.utils;

import org.apache.commons.lang3.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class PrintUtil {
    private static final Logger LOG = LoggerFactory.getLogger(PrintUtil.class);

    public static void printResult(Map<String, Object> result) {
        List<String> names = new ArrayList<>();
        List<String> values = new ArrayList<>();
        result.forEach(
                (name, val) -> {
                    names.add(name);
                    values.add(String.valueOf(val));
                });

        int maxLength = 0;
        for (String name : names) {
            maxLength = Math.max(maxLength, name.length());
        }
        final int length = 5;
        maxLength = maxLength + length;
        StringBuilder builder = new StringBuilder();
        builder.append("\n**********************Reports***********************\n");
        for (int i = 0; i < names.size(); i++) {
            String name = names.get(i);
            builder.append(name).append(StringUtils.repeat(" ", maxLength - name.length()));
            builder.append("|  ").append(values.get(i));

            if (i + 1 < names.size()) {
                builder.append("\n");
            }
        }
        builder.append("\n*********************Reports************************\n");
        LOG.info(builder.toString());
    }
}
