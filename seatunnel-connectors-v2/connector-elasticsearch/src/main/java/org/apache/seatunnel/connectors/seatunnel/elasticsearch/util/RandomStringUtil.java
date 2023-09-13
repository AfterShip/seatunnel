package org.apache.seatunnel.connectors.seatunnel.elasticsearch.util;

import java.util.concurrent.ThreadLocalRandom;

/**
 * @author wq.pan on 2023/8/7
 * @className RandomStringUtil @Description @Version: 1.0
 */
public class RandomStringUtil {

    private static final String BASE_STR =
            "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

    private static final int DEFAULT_STR_SIZE = 5;

    private RandomStringUtil() {}

    public static String randomString() {
        return randomString(DEFAULT_STR_SIZE);
    }

    public static String randomString(int length) {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < length; i++) {
            int number = random.nextInt(62);
            builder.append(BASE_STR.charAt(number));
        }
        return builder.toString();
    }
}
