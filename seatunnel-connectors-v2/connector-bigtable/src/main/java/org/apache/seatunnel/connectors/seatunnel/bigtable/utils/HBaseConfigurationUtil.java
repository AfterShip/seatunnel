package org.apache.seatunnel.connectors.seatunnel.bigtable.utils;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

/**
 * @author: gf.xu
 * @email: gf.xu@aftership.com
 * @date: 2023/8/18 14:26
 */
public class HBaseConfigurationUtil {
    private static final Logger LOG = LoggerFactory.getLogger(HBaseConfigurationUtil.class);

    public static final String ENV_HBASE_CONF_DIR = "HBASE_CONF_DIR";

    public static Configuration getHBaseConfiguration() {
        // Instantiate an HBaseConfiguration to load the hbase-default.xml and hbase-site.xml from
        // the classpath.
        Configuration configuration = HBaseConfiguration.create();


        // we using bigtable, so will not load from hbase-default.xml and hbase-site.xml from
        //  the classpath.
        return configuration;
    }


    /**
     * Deserialize a Hadoop {@link Configuration} from byte[]. Deserialize configs to {@code
     * targetConfig} if it is set.
     */
    public static Configuration deserializeConfiguration(
            byte[] serializedConfig, Configuration targetConfig) {
        if (null == targetConfig) {
            targetConfig = new Configuration();
        }
        try {
            deserializeWritable(targetConfig, serializedConfig);
        } catch (IOException e) {
            throw new RuntimeException(
                    "Encounter an IOException when deserialize the Configuration.", e);
        }
        return targetConfig;
    }


    /** Serialize a Hadoop {@link Configuration} into byte[]. */
    public static byte[] serializeConfiguration(Configuration conf) {
        try {
            return serializeWritable(conf);
        } catch (IOException e) {
            throw new RuntimeException(
                    "Encounter an IOException when serialize the Configuration.", e);
        }
    }

    /**
     * Serialize writable byte[].
     *
     * @param <T> the type parameter
     * @param writable the writable
     * @return the byte [ ]
     * @throws IOException the io exception
     */
    private static <T extends Writable> byte[] serializeWritable(T writable) throws IOException {
        Preconditions.checkArgument(writable != null);

        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream);
        writable.write(outputStream);
        return byteArrayOutputStream.toByteArray();
    }

    /**
     * Deserialize writable.
     *
     * @param <T> the type parameter
     * @param writable the writable
     * @param bytes the bytes
     * @throws IOException the io exception
     */
    private static <T extends Writable> void deserializeWritable(T writable, byte[] bytes)
            throws IOException {
        Preconditions.checkArgument(writable != null);
        Preconditions.checkArgument(bytes != null);

        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
        DataInputStream dataInputStream = new DataInputStream(byteArrayInputStream);
        writable.readFields(dataInputStream);
    }

    public static Configuration createHBaseConf() {
        Configuration hbaseClientConf = HBaseConfiguration.create();

        String hbaseConfDir = System.getenv(ENV_HBASE_CONF_DIR);

        if (hbaseConfDir != null) {
            if (new File(hbaseConfDir).exists()) {
                String coreSite = hbaseConfDir + "/core-site.xml";
                String hdfsSite = hbaseConfDir + "/hdfs-site.xml";
                String hbaseSite = hbaseConfDir + "/hbase-site.xml";
                if (new File(coreSite).exists()) {
                    hbaseClientConf.addResource(new org.apache.hadoop.fs.Path(coreSite));
                    LOG.info("Adding " + coreSite + " to hbase configuration");
                }
                if (new File(hdfsSite).exists()) {
                    hbaseClientConf.addResource(new org.apache.hadoop.fs.Path(hdfsSite));
                    LOG.info("Adding " + hdfsSite + " to hbase configuration");
                }
                if (new File(hbaseSite).exists()) {
                    hbaseClientConf.addResource(new org.apache.hadoop.fs.Path(hbaseSite));
                    LOG.info("Adding " + hbaseSite + " to hbase configuration");
                }
            } else {
                LOG.warn(
                        "HBase config directory '{}' not found, cannot load HBase configuration.",
                        hbaseConfDir);
            }
        } else {
            LOG.warn(
                    "{} env variable not found, cannot load HBase configuration.",
                    ENV_HBASE_CONF_DIR);
        }
        return hbaseClientConf;
    }
}
