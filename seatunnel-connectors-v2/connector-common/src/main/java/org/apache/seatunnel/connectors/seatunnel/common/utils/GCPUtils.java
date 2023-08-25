package org.apache.seatunnel.connectors.seatunnel.common.utils;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.services.storage.StorageScopes;
import com.google.auth.oauth2.ServiceAccountCredentials;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

/**
 * @author ah.he@aftership.com
 * @version 1.0
 * @date 2023/8/15 13:53
 */
public class GCPUtils {

    public static final String CLOUD_JSON_KEYFILE_PREFIX = "google.cloud";
    public static final String CLOUD_ACCOUNT_EMAIL_SUFFIX = "auth.service.account.email";
    public static final String CLOUD_ACCOUNT_PRIVATE_KEY_ID_SUFFIX =
            "auth.service.account.private.key.id";
    public static final String CLOUD_ACCOUNT_KEY_SUFFIX = "auth.service.account.private.key";
    public static final String CLOUD_ACCOUNT_JSON_SUFFIX = "auth.service.account.json";
    public static final String PRIVATE_KEY_WRAP =
            "-----BEGIN PRIVATE KEY-----\\n%s\\n-----END PRIVATE KEY-----\\n";
    public static final String SERVICE_ACCOUNT_TYPE = "cdap.gcs.auth.service.account.type";
    public static final String SERVICE_ACCOUNT_TYPE_JSON = "JSON";

    public static ServiceAccountCredentials getGoogleCredentials(String content) {
        try {
            return (ServiceAccountCredentials)
                    ServiceAccountCredentials.fromStream(
                                    new ByteArrayInputStream(content.getBytes()))
                            .createScoped(StorageScopes.all());
        } catch (IOException e) {
            throw new RuntimeException("Get google credentials failed." + e);
        }
    }

    public static Credential getCredential(String content) {
        try {
            return GoogleCredential.fromStream(new ByteArrayInputStream(content.getBytes()))
                    .createScoped(StorageScopes.all());
        } catch (IOException e) {
            throw new RuntimeException("Get credential failed." + e);
        }
    }

    public static Map<String, String> generateAuthProperties(
            String serviceAccount, String... keyPrefix) {
        String privateKeyData = null;
        Map<String, String> properties = new HashMap<>();
        properties.put(SERVICE_ACCOUNT_TYPE, SERVICE_ACCOUNT_TYPE_JSON);

        for (String prefix : keyPrefix) {
            ServiceAccountCredentials credentials = getGoogleCredentials(serviceAccount);

            properties.put(
                    String.format("%s.%s", prefix, CLOUD_ACCOUNT_EMAIL_SUFFIX),
                    credentials.getClientEmail());
            properties.put(
                    String.format("%s.%s", prefix, CLOUD_ACCOUNT_PRIVATE_KEY_ID_SUFFIX),
                    credentials.getPrivateKeyId());
            if (privateKeyData == null) {
                privateKeyData =
                        String.format(
                                PRIVATE_KEY_WRAP,
                                Base64.getEncoder()
                                        .encodeToString(credentials.getPrivateKey().getEncoded()));
            }
            properties.put(
                    String.format("%s.%s", prefix, CLOUD_ACCOUNT_KEY_SUFFIX), privateKeyData);
            properties.put(
                    String.format("%s.%s", prefix, CLOUD_ACCOUNT_JSON_SUFFIX), serviceAccount);
        }
        return properties;
    }
}
