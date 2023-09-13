package org.apache.seatunnel.connectors.seatunnel.spanner.utils;

import org.apache.seatunnel.connectors.seatunnel.common.utils.ConfigCenterUtils;
import org.apache.seatunnel.connectors.seatunnel.common.utils.GCPUtils;
import org.apache.seatunnel.connectors.seatunnel.spanner.common.BytesCounter;
import org.apache.seatunnel.connectors.seatunnel.spanner.config.SpannerParameters;

import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.spi.v1.SpannerInterceptorProvider;
import com.google.spanner.v1.PartialResultSet;
import com.google.spanner.v1.ResultSet;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.ForwardingClientCallListener;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;

import java.io.IOException;

/**
 * @author: gf.xu
 * @email: gf.xu@aftership.com
 * @date: 2023/8/25 15:29
 */
public class SpannerUtil {

    /** Construct and return the {@link Spanner} service */
    public static Spanner getSpannerService(String serviceAccount, String projectId)
            throws IOException {
        SpannerOptions.Builder optionsBuilder = buildSpannerOptions(serviceAccount, projectId);
        return optionsBuilder.build().getService();
    }

    /**
     * Construct and return the {@link Spanner} service with an interceptor that increments the
     * provided counter by the number of bytes read from Spanner.
     */
    public static Spanner getSpannerServiceWithReadInterceptor(
            String serviceAccount, String projectId, BytesCounter counter) throws IOException {
        class InterceptedClientCall<ReqT, RespT>
                extends ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT> {
            InterceptedClientCall(ClientCall<ReqT, RespT> call) {
                super(call);
            }

            @Override
            public void start(Listener<RespT> responseListener, Metadata headers) {
                super.start(
                        new ForwardingClientCallListener.SimpleForwardingClientCallListener<RespT>(
                                responseListener) {
                            @Override
                            public void onMessage(RespT message) {
                                if (message instanceof PartialResultSet) {
                                    PartialResultSet partialResultSet = (PartialResultSet) message;
                                    counter.increment(partialResultSet.getSerializedSize());
                                } else if (message instanceof ResultSet) {
                                    ResultSet resultSet = (ResultSet) message;
                                    counter.increment(resultSet.getSerializedSize());
                                }
                                super.onMessage(message);
                            }
                        },
                        headers);
            }
        }

        SpannerOptions.Builder optionsBuilder = buildSpannerOptions(serviceAccount, projectId);
        optionsBuilder.setInterceptorProvider(
                SpannerInterceptorProvider.createDefault()
                        .with(
                                new ClientInterceptor() {
                                    @Override
                                    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
                                            MethodDescriptor<ReqT, RespT> method,
                                            CallOptions callOptions,
                                            Channel next) {
                                        ClientCall<ReqT, RespT> call =
                                                next.newCall(method, callOptions);
                                        return new InterceptedClientCall<>(call);
                                    }
                                }));

        return optionsBuilder.build().getService();
    }

    /**
     * Construct and return a {@link SpannerOptions.Builder} with the provided credentials and
     * projectId
     */
    private static SpannerOptions.Builder buildSpannerOptions(
            String serviceAccount, String projectId) throws IOException {
        SpannerOptions.Builder optionsBuilder = SpannerOptions.newBuilder();
        if (serviceAccount != null) {
            optionsBuilder.setCredentials(GCPUtils.getGoogleCredentials(serviceAccount));
        }
        optionsBuilder.setProjectId(projectId);
        return optionsBuilder;
    }

    public static String getServiceAccount(SpannerParameters spannerParameters) {
        return ConfigCenterUtils.getServiceAccountFromConfigCenter(
                spannerParameters.getConfigCenterToken(),
                spannerParameters.getConfigCenterUrl(),
                spannerParameters.getConfigCenterEnvironment(),
                spannerParameters.getConfigCenterProject());
    }
}
