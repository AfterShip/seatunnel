package org.apache.seatunnel.connectors.seatunnel.common.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aftership.data.config.center.ConfigCenterClient;
import com.aftership.data.config.center.models.ClientConfig;

import java.io.IOException;
import java.util.Base64;
import java.util.Map;

/**
 * @author ah.he@aftership.com
 * @version 1.0
 * @date 2022/9/29 19:49
 */
public class ConfigCenterUtils {

    private static final Logger LOG = LoggerFactory.getLogger(ConfigCenterUtils.class);
    private static final String GCP_AUTH_SERVICE_ACCOUNT = "gcp-auth.service-account";

    public static Map<String, String> getConfigCenterEntries(
            String configCenterToken,
            String configCenterEndpoint,
            String configCenterEnv,
            String configCenterProjectName) {
        ClientConfig clientConfig =
                ClientConfig.newBuilder()
                        .withAppKey(new String(Base64.getDecoder().decode(configCenterToken)))
                        .withConfigCenterEnv(configCenterEnv)
                        .withEndpoint(configCenterEndpoint)
                        .withProjectName(configCenterProjectName)
                        .withNodeEnv(configCenterEnv)
                        .build();
        ConfigCenterClient configCenterClient = ConfigCenterClient.loadFrom(clientConfig);
        Map<String, String> configMap = null;
        try {
            configMap = configCenterClient.getConfig();
        } catch (IOException e) {
            LOG.error("Failed to get config map from config-center. " + e.getMessage());
            throw new RuntimeException(e);
        }
        return configMap;
    }

    public static String getServiceAccountFromConfigCenter(Map<String, String> configMap) {
        return getServiceAccountFromConfigCenter(configMap, GCP_AUTH_SERVICE_ACCOUNT);
    }

    public static String getServiceAccountFromConfigCenter(
            Map<String, String> configMap, String serviceAccountKey) {
        return configMap.get(serviceAccountKey);
    }

    public static String getServiceAccountFromConfigCenter(
            String configCenterToken,
            String configCenterEndpoint,
            String configCenterEnv,
            String configCenterProjectName) {
        return getServiceAccountFromConfigCenter(
                configCenterToken,
                configCenterEndpoint,
                configCenterEnv,
                configCenterProjectName,
                GCP_AUTH_SERVICE_ACCOUNT);
    }

    public static String getServiceAccountFromConfigCenter(
            String configCenterToken,
            String configCenterEndpoint,
            String configCenterEnv,
            String configCenterProjectName,
            String serviceAccountKey) {
        Map<String, String> configMap =
                getConfigCenterEntries(
                        configCenterToken,
                        configCenterEndpoint,
                        configCenterEnv,
                        configCenterProjectName);
        return configMap.get(serviceAccountKey);
    }
}
