package org.apache.seatunnel.connectors.seatunnel.elasticsearch.constant;

/**
 * @author wq.pan on 2023/8/7
 * @className ElasticsearchConstants @Description @Version: 1.0
 */
public class ElasticsearchConstants {

    public static final String PROXY_JSON = "db-auth.es.%s";

    public static final String CA_CLIENT_KEY = "db-ssl.es.%s-client.key";
    public static final String CA_CLIENT_CRT = "db-ssl.es.%s-client.crt";
    public static final String CA_CLIENT_ROOT_CRT = "db-ssl.es.%s-client-ca.crt";

    public static final String NEW_CA_CLIENT_KEY = "db-ssl.es.%s-new-client.key";
    public static final String NEW_CA_CLIENT_CRT = "db-ssl.es.%s-new-client.crt";
    public static final String NEW_CA_CLIENT_ROOT_CRT = "db-ssl.es.%s-new-client-ca.crt";

    public static final String ENVIRONMENT = "environment";
    public static final String CONFIG_CENTER_TOKEN = "config_center_token";
    public static final String CONFIG_CENTER_PROJECT = "config_center_project";
    public static final String CONFIG_CENTER_URL = "config_center_url";
    public static final String KEY_STORE_PASSWORD = "keyStorePassword";
    public static final String ES_CLUSTER = "cluster";

    /** rest client connect config 5 * */
    public static final String MAX_CONN_TOTAL = "maxConnTotal";

    public static final String MAX_CONN_PER_ROUTE = "maxConnPerRoute";
    public static final String SOCKET_TIMEOUT_MILLIS = "socketTimeoutMillis";
    public static final String CONNECT_TIMEOUT_MILLIS = "connectTimeoutMillis";
    public static final String CONNECT_REQUEST_TIMEOUT = "connectRequestTimeout";

    /** index type * */
    public static final String INDEX_TYPE_CUSTOMIZE = "customize";

    public static final String INDEX_TYPE_FIELD = "field";

    /** action type * */
    public static final String ACTION_TYPE_INDEX = "index";

    public static final String ACTION_TYPE_UPSERT = "upsert";
}
