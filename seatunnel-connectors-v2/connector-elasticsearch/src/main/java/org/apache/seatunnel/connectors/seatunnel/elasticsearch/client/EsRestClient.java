/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.elasticsearch.client;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.HttpStatus;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.TrustAllStrategy;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.ssl.SSLContexts;
import org.apache.http.util.Asserts;
import org.apache.http.util.EntityUtils;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.connectors.seatunnel.common.utils.ConfigCenterUtils;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.dto.BulkResponse;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.dto.ElasticsearchClusterInfo;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.dto.ProxyContext;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.dto.source.IndexDocsCount;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.dto.source.ScrollResult;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.exception.ElasticsearchConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.exception.ElasticsearchConnectorException;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.util.RandomStringUtil;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.util.SSLUtils;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.TextNode;
import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.KeyFactory;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.constant.ElasticsearchConstants.*;

@Slf4j
public class EsRestClient {


    private static final int CONNECTION_REQUEST_TIMEOUT = 10 * 1000;

    private static final int SOCKET_TIMEOUT = 5 * 60 * 1000;

    private final RestClient restClient;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public EsRestClient(RestClient restClient) {
        this.restClient = restClient;
    }

    public static EsRestClient createInstance(Config pluginConfig) {
        Map<String, String> entriesMap = ConfigCenterUtils.getConfigCenterEntries(
                pluginConfig.getString(CONFIG_CENTER_TOKEN),
                pluginConfig.getString(CONFIG_CENTER_URL),
                pluginConfig.getString(ENVIRONMENT),
                pluginConfig.getString(CONFIG_CENTER_PROJECT));

        String cluster = pluginConfig.getString(ES_CLUSTER);
        ProxyContext proxyContext = getProxyContext(entriesMap.get(String.format(PROXY_JSON, cluster)));
        List<HttpHost> hosts = new ArrayList<>();
        RestClientBuilder builder;
        for (String address : StringUtils.split(proxyContext.getHost(), ",")) {
            URL url = null;
            try {
                url = new URL(address);
            } catch (MalformedURLException e) {
                throw new RuntimeException(e);
            }
            hosts.add(new HttpHost(url.getHost(), url.getPort(), url.getProtocol()));
        }
        builder = RestClient.builder(hosts.toArray(new HttpHost[0]));
        // httpClientConfigCallback
        builder.setHttpClientConfigCallback(httpClientBuilder -> {
            // credentials provider
            CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY,
                    new UsernamePasswordCredentials(proxyContext.getUser(),
                            proxyContext.getPassword()));
            httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);

            String clientRootCrt = entriesMap.get(String.format(CA_CLIENT_ROOT_CRT, cluster)) == null
                    ? entriesMap.get(String.format(NEW_CA_CLIENT_ROOT_CRT, cluster))
                    : entriesMap.get(String.format(CA_CLIENT_ROOT_CRT, cluster));

            String clientCrt = entriesMap.get(String.format(CA_CLIENT_CRT, cluster)) == null
                    ? entriesMap.get(String.format(NEW_CA_CLIENT_CRT, cluster))
                    : entriesMap.get(String.format(CA_CLIENT_CRT, cluster));

            String clientKey = entriesMap.get(String.format(CA_CLIENT_KEY, cluster)) == null
                    ? entriesMap.get(String.format(NEW_CA_CLIENT_KEY, cluster))
                    : entriesMap.get(String.format(CA_CLIENT_KEY, cluster));

            // ssl context
            String keystorePassword = pluginConfig.hasPath(KEY_STORE_PASSWORD)
                    ? pluginConfig.getString(KEY_STORE_PASSWORD)
                    : RandomStringUtil.randomString();
            SSLContext sslContext = sslContext(
                    clientRootCrt,
                    clientCrt,
                    clientKey,
                    keystorePassword);
            httpClientBuilder.setSSLContext(sslContext);

            int maxConnTotal = pluginConfig.hasPath(MAX_CONN_TOTAL) ? pluginConfig.getInt(MAX_CONN_TOTAL) : 30;
            int maxConnPerRoute = pluginConfig.hasPath(MAX_CONN_PER_ROUTE) ? pluginConfig.getInt(MAX_CONN_PER_ROUTE) : 30;

            httpClientBuilder.setMaxConnTotal(maxConnTotal);
            httpClientBuilder.setMaxConnPerRoute(maxConnPerRoute);
            return httpClientBuilder;
        });

        // requestConfigCallback
        builder.setRequestConfigCallback(requestConfigBuilder -> {
            int socketTimeout = pluginConfig.hasPath(SOCKET_TIMEOUT_MILLIS) ? pluginConfig.getInt(SOCKET_TIMEOUT_MILLIS) : 30000;
            int connectTimeout = pluginConfig.hasPath(CONNECT_TIMEOUT_MILLIS) ? pluginConfig.getInt(CONNECT_TIMEOUT_MILLIS) : 3000;
            int connectRequestTimeout = pluginConfig.hasPath(CONNECT_REQUEST_TIMEOUT) ? pluginConfig.getInt(CONNECT_REQUEST_TIMEOUT) : 2000;
            requestConfigBuilder.setSocketTimeout(socketTimeout);
            requestConfigBuilder.setConnectTimeout(connectTimeout);
            requestConfigBuilder.setConnectionRequestTimeout(connectRequestTimeout);
            return requestConfigBuilder;
        });

        return new EsRestClient(builder.build());
    }

    public static EsRestClient createInstance(
            List<String> hosts,
            Optional<String> username,
            Optional<String> password,
            boolean tlsVerifyCertificate,
            boolean tlsVerifyHostnames,
            Optional<String> keystorePath,
            Optional<String> keystorePassword,
            Optional<String> truststorePath,
            Optional<String> truststorePassword) {
        RestClientBuilder restClientBuilder =
                getRestClientBuilder(
                        hosts,
                        username,
                        password,
                        tlsVerifyCertificate,
                        tlsVerifyHostnames,
                        keystorePath,
                        keystorePassword,
                        truststorePath,
                        truststorePassword);
        return new EsRestClient(restClientBuilder.build());
    }

    private static ProxyContext getProxyContext(String proxyString) {
        if (StringUtils.isBlank(proxyString)) {
            log.info("proxyString is null or empty.");
            return null;
        }
        try {
            return MAPPER.readValue(proxyString, ProxyContext.class);
        } catch (IOException e) {
            log.error("Failed to parse proxy key: {}, error_msg: {}.", proxyString, e.getMessage(), e);
        }
        return null;
    }

    private static SSLContext sslContext(String caRootCrt,
                                         String caClientCrt,
                                         String primaryKey,
                                         String keystorePassword) {
        try {
            // 允许使用非 pkcs8 格式秘钥
            java.security.Security.addProvider(
                    new org.bouncycastle.jce.provider.BouncyCastleProvider()
            );
            // 添加服务器 CA 证书
            CertificateFactory cAf = CertificateFactory.getInstance("X.509");
            InputStream inputStream = new ByteArrayInputStream(caRootCrt.getBytes());
            X509Certificate ca = (X509Certificate) cAf.generateCertificate(inputStream);
            inputStream.close();

            KeyStore caKs = KeyStore.getInstance(KeyStore.getDefaultType());
            caKs.load(null, null);
            caKs.setCertificateEntry("ca-certificate", ca);
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(caKs);

            // 添加客户端 CA 证书
            CertificateFactory cf = CertificateFactory.getInstance("X.509");
            inputStream = new ByteArrayInputStream(caClientCrt.getBytes());
            X509Certificate caCert = (X509Certificate) cf.generateCertificate(inputStream);
            inputStream.close();

            // 设置秘钥 KeyStore
            KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
            ks.load(null, null);
            ks.setCertificateEntry("certificate", caCert);

            inputStream = new ByteArrayInputStream(primaryKey.getBytes());
            PrivateKey privateKey = getPrivateKey(inputStream);
            char[] pwdChar = keystorePassword.toCharArray();
            ks.setKeyEntry("private-key", privateKey, pwdChar,
                    new java.security.cert.Certificate[]{caCert});
            KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            kmf.init(ks, pwdChar);

            // create SSL socket
            SSLContext context = SSLContext.getInstance("TLSv1.2");
            context.init(kmf.getKeyManagers(), tmf.getTrustManagers(), new SecureRandom());
            return context;
        } catch (Exception e) {
            log.error("Failed to create SSLContext!", e);
            throw new RuntimeException("Failed to create SSLContext!", e);
        }
    }

    private static PrivateKey getPrivateKey(InputStream inputStream) throws Exception {
        Base64.Decoder decoder = Base64.getMimeDecoder();
        byte[] buffer = decoder.decode(getPem(inputStream));

        PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(buffer);
        KeyFactory keyFactory = KeyFactory.getInstance("RSA");
        return keyFactory.generatePrivate(keySpec);
    }

    private static String getPem(InputStream inputStream) throws Exception {
        BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
        String readLine = null;
        StringBuilder sb = new StringBuilder();
        while ((readLine = br.readLine()) != null) {
            if (readLine.charAt(0) == '-') {
                continue;
            } else {
                sb.append(readLine);
                sb.append('\r');
            }
        }
        br.close();
        inputStream.close();
        return sb.toString();
    }

    private static RestClientBuilder getRestClientBuilder(
            List<String> hosts,
            Optional<String> username,
            Optional<String> password,
            boolean tlsVerifyCertificate,
            boolean tlsVerifyHostnames,
            Optional<String> keystorePath,
            Optional<String> keystorePassword,
            Optional<String> truststorePath,
            Optional<String> truststorePassword) {
        HttpHost[] httpHosts = new HttpHost[hosts.size()];
        for (int i = 0; i < hosts.size(); i++) {
            httpHosts[i] = HttpHost.create(hosts.get(i));
        }

        RestClientBuilder restClientBuilder =
                RestClient.builder(httpHosts)
                        .setRequestConfigCallback(
                                requestConfigBuilder ->
                                        requestConfigBuilder
                                                .setConnectionRequestTimeout(
                                                        CONNECTION_REQUEST_TIMEOUT)
                                                .setSocketTimeout(SOCKET_TIMEOUT));

        restClientBuilder.setHttpClientConfigCallback(
                httpClientBuilder -> {
                    if (username.isPresent()) {
                        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                        credentialsProvider.setCredentials(
                                AuthScope.ANY,
                                new UsernamePasswordCredentials(username.get(), password.get()));
                        httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }

                    try {
                        if (tlsVerifyCertificate) {
                            Optional<SSLContext> sslContext =
                                    SSLUtils.buildSSLContext(
                                            keystorePath,
                                            keystorePassword,
                                            truststorePath,
                                            truststorePassword);
                            sslContext.ifPresent(e -> httpClientBuilder.setSSLContext(e));
                        } else {
                            SSLContext sslContext =
                                    SSLContexts.custom()
                                            .loadTrustMaterial(new TrustAllStrategy())
                                            .build();
                            httpClientBuilder.setSSLContext(sslContext);
                        }
                        if (!tlsVerifyHostnames) {
                            httpClientBuilder.setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE);
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    return httpClientBuilder;
                });
        return restClientBuilder;
    }

    public BulkResponse bulk(String requestBody) {
        Request request = new Request("POST", "/_bulk");
        request.setJsonEntity(requestBody);
        try {
            Response response = restClient.performRequest(request);
            if (response == null) {
                throw new ElasticsearchConnectorException(
                        ElasticsearchConnectorErrorCode.BULK_RESPONSE_ERROR,
                        "bulk es Response is null");
            }
            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                ObjectMapper objectMapper = new ObjectMapper();
                String entity = EntityUtils.toString(response.getEntity());
                JsonNode json = objectMapper.readTree(entity);
                int took = json.get("took").asInt();
                boolean errors = json.get("errors").asBoolean();
                return new BulkResponse(errors, took, entity);
            } else {
                throw new ElasticsearchConnectorException(
                        ElasticsearchConnectorErrorCode.BULK_RESPONSE_ERROR,
                        String.format(
                                "bulk es response status code=%d,request boy=%s",
                                response.getStatusLine().getStatusCode(), requestBody));
            }
        } catch (IOException e) {
            throw new ElasticsearchConnectorException(
                    ElasticsearchConnectorErrorCode.BULK_RESPONSE_ERROR,
                    String.format("bulk es error,request boy=%s", requestBody),
                    e);
        }
    }

    public ElasticsearchClusterInfo getClusterInfo() {
        Request request = new Request("GET", "/");
        try {
            Response response = restClient.performRequest(request);
            String result = EntityUtils.toString(response.getEntity());
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode jsonNode = objectMapper.readTree(result);
            JsonNode versionNode = jsonNode.get("version");
            return ElasticsearchClusterInfo.builder()
                    .clusterVersion(versionNode.get("number").asText())
                    .distribution(
                            Optional.ofNullable(versionNode.get("distribution"))
                                    .map(e -> e.asText())
                                    .orElse(null))
                    .build();
        } catch (IOException e) {
            throw new ElasticsearchConnectorException(
                    ElasticsearchConnectorErrorCode.GET_ES_VERSION_FAILED,
                    "fail to get elasticsearch version.",
                    e);
        }
    }

    public void close() {
        try {
            restClient.close();
        } catch (IOException e) {
            log.warn("close elasticsearch connection error", e);
        }
    }

    /**
     * first time to request search documents by scroll call /${index}/_search?scroll=${scroll}
     *
     * @param index index name
     * @param source select fields
     * @param scrollTime such as:1m
     * @param scrollSize fetch documents count in one request
     */
    public ScrollResult searchByScroll(
            String index,
            List<String> source,
            Map<String, Object> query,
            String scrollTime,
            int scrollSize) {
        Map<String, Object> param = new HashMap<>();
        param.put("query", query);
        param.put("_source", source);
        param.put("sort", new String[] {"_doc"});
        param.put("size", scrollSize);
        String endpoint = "/" + index + "/_search?scroll=" + scrollTime;
        ScrollResult scrollResult =
                getDocsFromScrollRequest(endpoint, JsonUtils.toJsonString(param));
        return scrollResult;
    }

    /**
     * scroll to get result call _search/scroll
     *
     * @param scrollId the scroll id of the last request
     * @param scrollTime such as:1m
     */
    public ScrollResult searchWithScrollId(String scrollId, String scrollTime) {
        Map<String, String> param = new HashMap<>();
        param.put("scroll_id", scrollId);
        param.put("scroll", scrollTime);
        ScrollResult scrollResult =
                getDocsFromScrollRequest("/_search/scroll", JsonUtils.toJsonString(param));
        return scrollResult;
    }

    private ScrollResult getDocsFromScrollRequest(String endpoint, String requestBody) {
        Request request = new Request("POST", endpoint);
        request.setJsonEntity(requestBody);
        try {
            Response response = restClient.performRequest(request);
            if (response == null) {
                throw new ElasticsearchConnectorException(
                        ElasticsearchConnectorErrorCode.SCROLL_REQUEST_ERROR,
                        "POST " + endpoint + " response null");
            }
            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                String entity = EntityUtils.toString(response.getEntity());
                ObjectNode responseJson = JsonUtils.parseObject(entity);

                JsonNode shards = responseJson.get("_shards");
                int totalShards = shards.get("total").intValue();
                int successful = shards.get("successful").intValue();
                Asserts.check(
                        totalShards == successful,
                        String.format(
                                "POST %s,total shards(%d)!= successful shards(%d)",
                                endpoint, totalShards, successful));

                ScrollResult scrollResult = getDocsFromScrollResponse(responseJson);
                return scrollResult;
            } else {
                throw new ElasticsearchConnectorException(
                        ElasticsearchConnectorErrorCode.SCROLL_REQUEST_ERROR,
                        String.format(
                                "POST %s response status code=%d,request boy=%s",
                                endpoint, response.getStatusLine().getStatusCode(), requestBody));
            }
        } catch (IOException e) {
            throw new ElasticsearchConnectorException(
                    ElasticsearchConnectorErrorCode.SCROLL_REQUEST_ERROR,
                    String.format("POST %s error,request boy=%s", endpoint, requestBody),
                    e);
        }
    }

    private ScrollResult getDocsFromScrollResponse(ObjectNode responseJson) {
        ScrollResult scrollResult = new ScrollResult();
        String scrollId = responseJson.get("_scroll_id").asText();
        scrollResult.setScrollId(scrollId);

        JsonNode hitsNode = responseJson.get("hits").get("hits");
        List<Map<String, Object>> docs = new ArrayList<>(hitsNode.size());
        scrollResult.setDocs(docs);

        Iterator<JsonNode> iter = hitsNode.iterator();
        while (iter.hasNext()) {
            Map<String, Object> doc = new HashMap<>();
            JsonNode hitNode = iter.next();
            doc.put("_index", hitNode.get("_index").textValue());
            doc.put("_id", hitNode.get("_id").textValue());
            JsonNode source = hitNode.get("_source");
            for (Iterator<Map.Entry<String, JsonNode>> iterator = source.fields();
                    iterator.hasNext(); ) {
                Map.Entry<String, JsonNode> entry = iterator.next();
                String fieldName = entry.getKey();
                if (entry.getValue() instanceof TextNode) {
                    doc.put(fieldName, entry.getValue().textValue());
                } else {
                    doc.put(fieldName, entry.getValue());
                }
            }
            docs.add(doc);
        }
        return scrollResult;
    }

    public List<IndexDocsCount> getIndexDocsCount(String index) {
        String endpoint = String.format("/_cat/indices/%s?h=index,docsCount&format=json", index);
        Request request = new Request("GET", endpoint);
        try {
            Response response = restClient.performRequest(request);
            if (response == null) {
                throw new ElasticsearchConnectorException(
                        ElasticsearchConnectorErrorCode.GET_INDEX_DOCS_COUNT_FAILED,
                        "GET " + endpoint + " response null");
            }
            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                String entity = EntityUtils.toString(response.getEntity());
                List<IndexDocsCount> indexDocsCounts =
                        JsonUtils.toList(entity, IndexDocsCount.class);
                return indexDocsCounts;
            } else {
                throw new ElasticsearchConnectorException(
                        ElasticsearchConnectorErrorCode.GET_INDEX_DOCS_COUNT_FAILED,
                        String.format(
                                "GET %s response status code=%d",
                                endpoint, response.getStatusLine().getStatusCode()));
            }
        } catch (IOException ex) {
            throw new ElasticsearchConnectorException(
                    ElasticsearchConnectorErrorCode.GET_INDEX_DOCS_COUNT_FAILED, ex);
        }
    }

    public List<String> listIndex() {
        String endpoint = "/_cat/indices?format=json";
        Request request = new Request("GET", endpoint);
        try {
            Response response = restClient.performRequest(request);
            if (response == null) {
                throw new ElasticsearchConnectorException(
                        ElasticsearchConnectorErrorCode.LIST_INDEX_FAILED,
                        "GET " + endpoint + " response null");
            }
            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                String entity = EntityUtils.toString(response.getEntity());
                return JsonUtils.toList(entity, Map.class).stream()
                        .map(map -> map.get("index").toString())
                        .collect(Collectors.toList());
            } else {
                throw new ElasticsearchConnectorException(
                        ElasticsearchConnectorErrorCode.LIST_INDEX_FAILED,
                        String.format(
                                "GET %s response status code=%d",
                                endpoint, response.getStatusLine().getStatusCode()));
            }
        } catch (IOException ex) {
            throw new ElasticsearchConnectorException(
                    ElasticsearchConnectorErrorCode.LIST_INDEX_FAILED, ex);
        }
    }

    // todo: We don't support set the index mapping now.
    public void createIndex(String indexName) {
        String endpoint = String.format("/%s", indexName);
        Request request = new Request("PUT", endpoint);
        try {
            Response response = restClient.performRequest(request);
            if (response == null) {
                throw new ElasticsearchConnectorException(
                        ElasticsearchConnectorErrorCode.CREATE_INDEX_FAILED,
                        "PUT " + endpoint + " response null");
            }
            if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
                throw new ElasticsearchConnectorException(
                        ElasticsearchConnectorErrorCode.CREATE_INDEX_FAILED,
                        String.format(
                                "PUT %s response status code=%d",
                                endpoint, response.getStatusLine().getStatusCode()));
            }
        } catch (IOException ex) {
            throw new ElasticsearchConnectorException(
                    ElasticsearchConnectorErrorCode.CREATE_INDEX_FAILED, ex);
        }
    }

    public void dropIndex(String tableName) {
        String endpoint = String.format("/%s", tableName);
        Request request = new Request("DELETE", endpoint);
        try {
            Response response = restClient.performRequest(request);
            if (response == null) {
                throw new ElasticsearchConnectorException(
                        ElasticsearchConnectorErrorCode.DROP_INDEX_FAILED,
                        "DELETE " + endpoint + " response null");
            }
            // todo: if the index doesn't exist, the response status code is 200?
            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                return;
            } else {
                throw new ElasticsearchConnectorException(
                        ElasticsearchConnectorErrorCode.DROP_INDEX_FAILED,
                        String.format(
                                "DELETE %s response status code=%d",
                                endpoint, response.getStatusLine().getStatusCode()));
            }
        } catch (IOException ex) {
            throw new ElasticsearchConnectorException(
                    ElasticsearchConnectorErrorCode.DROP_INDEX_FAILED, ex);
        }
    }

    /**
     * get es field name and type mapping realtion
     *
     * @param index index name
     * @return {key-> field name,value->es type}
     */
    public Map<String, String> getFieldTypeMapping(String index, List<String> source) {
        String endpoint = String.format("/%s/_mappings", index);
        Request request = new Request("GET", endpoint);
        Map<String, String> mapping = new HashMap<>();
        try {
            Response response = restClient.performRequest(request);
            if (response == null) {
                throw new ElasticsearchConnectorException(
                        ElasticsearchConnectorErrorCode.GET_INDEX_DOCS_COUNT_FAILED,
                        "GET " + endpoint + " response null");
            }
            if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
                throw new ElasticsearchConnectorException(
                        ElasticsearchConnectorErrorCode.GET_INDEX_DOCS_COUNT_FAILED,
                        String.format(
                                "GET %s response status code=%d",
                                endpoint, response.getStatusLine().getStatusCode()));
            }
            String entity = EntityUtils.toString(response.getEntity());
            log.info(String.format("GET %s respnse=%s", endpoint, entity));
            ObjectNode responseJson = JsonUtils.parseObject(entity);
            for (Iterator<JsonNode> it = responseJson.elements(); it.hasNext(); ) {
                JsonNode indexProperty = it.next();
                JsonNode mappingsProperty = indexProperty.get("mappings");
                if (mappingsProperty.has("mappingsProperty")) {
                    JsonNode properties = mappingsProperty.get("properties");
                    mapping = getFieldTypeMappingFromProperties(properties, source);
                } else {
                    for (JsonNode typeNode : mappingsProperty) {
                        JsonNode properties;
                        if (typeNode.has("properties")) {
                            properties = typeNode.get("properties");
                        } else {
                            properties = typeNode;
                        }
                        mapping.putAll(getFieldTypeMappingFromProperties(properties, source));
                    }
                }
            }
        } catch (IOException ex) {
            throw new ElasticsearchConnectorException(
                    ElasticsearchConnectorErrorCode.GET_INDEX_DOCS_COUNT_FAILED, ex);
        }
        return mapping;
    }

    private static Map<String, String> getFieldTypeMappingFromProperties(
            JsonNode properties, List<String> source) {
        Map<String, String> allElasticSearchFieldTypeInfoMap = new HashMap<>();
        properties
                .fields()
                .forEachRemaining(
                        entry -> {
                            String fieldName = entry.getKey();
                            JsonNode fieldProperty = entry.getValue();
                            if (fieldProperty.has("type")) {
                                allElasticSearchFieldTypeInfoMap.put(
                                        fieldName, fieldProperty.get("type").asText());
                            }
                        });
        if (CollectionUtils.isEmpty(source)) {
            return allElasticSearchFieldTypeInfoMap;
        }

        return source.stream()
                .collect(
                        Collectors.toMap(
                                Function.identity(),
                                fieldName -> {
                                    String fieldType =
                                            allElasticSearchFieldTypeInfoMap.get(fieldName);
                                    if (fieldType == null) {
                                        log.warn(
                                                "fail to get elasticsearch field {} mapping type,so give a default type text",
                                                fieldName);
                                        return "text";
                                    }
                                    return fieldType;
                                }));
    }
}
