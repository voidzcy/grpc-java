/*
 * Copyright 2019 The gRPC Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.grpc.xds;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.Any;
import io.envoyproxy.envoy.api.v2.DiscoveryRequest;
import io.envoyproxy.envoy.api.v2.DiscoveryResponse;
import io.envoyproxy.envoy.api.v2.Listener;
import io.envoyproxy.envoy.api.v2.RouteConfiguration;
import io.envoyproxy.envoy.api.v2.core.Address;
import io.envoyproxy.envoy.api.v2.core.ConfigSource;
import io.envoyproxy.envoy.api.v2.core.Node;
import io.envoyproxy.envoy.api.v2.listener.FilterChain;
import io.envoyproxy.envoy.api.v2.route.RedirectAction;
import io.envoyproxy.envoy.api.v2.route.Route;
import io.envoyproxy.envoy.api.v2.route.RouteAction;
import io.envoyproxy.envoy.api.v2.route.VirtualHost;
import io.envoyproxy.envoy.config.filter.network.http_connection_manager.v2.HttpConnectionManager;
import io.envoyproxy.envoy.config.filter.network.http_connection_manager.v2.Rds;
import io.envoyproxy.envoy.config.listener.v2.ApiListener;
import io.envoyproxy.envoy.service.discovery.v2.AggregatedDiscoveryServiceGrpc.AggregatedDiscoveryServiceImplBase;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.SynchronizationContext;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.internal.BackoffPolicy;
import io.grpc.internal.FakeClock;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import io.grpc.xds.XdsClient.ConfigUpdate;
import io.grpc.xds.XdsClient.ConfigWatcher;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * Tests for {@link XdsClientImpl}.
 */
public class XdsClientImplTest {

  private static final String HOSTNAME = "foo.googleapis.com";
  private static final int PORT = 8080;

  private static final Node NODE = Node.getDefaultInstance();

  @Rule
  public final GrpcCleanupRule cleanupRule = new GrpcCleanupRule();

  private final SynchronizationContext syncContext = new SynchronizationContext(
      new Thread.UncaughtExceptionHandler() {
        @Override
        public void uncaughtException(Thread t, Throwable e) {
          throw new AssertionError(e);
        }
      });
  private final FakeClock fakeClock = new FakeClock();

  private final Queue<StreamObserver<DiscoveryResponse>> responseObservers = new ArrayDeque<>();
  private final Queue<StreamObserver<DiscoveryRequest>> requestObservers = new ArrayDeque<>();

  @Mock
  private BackoffPolicy.Provider backoffPolicyProvider;
  @Mock
  private BackoffPolicy backoffPolicy;
  @Mock
  private ConfigWatcher configWatcher;

  private ManagedChannel channel;
  private XdsClientImpl xdsClient;

  @Before
  public void setUp() throws IOException {
    MockitoAnnotations.initMocks(this);
    when(backoffPolicyProvider.get()).thenReturn(backoffPolicy);

    String serverName = InProcessServerBuilder.generateName();
    AggregatedDiscoveryServiceImplBase serviceImpl = new AggregatedDiscoveryServiceImplBase() {
      @Override
      public StreamObserver<DiscoveryRequest> streamAggregatedResources(
          final StreamObserver<DiscoveryResponse> responseObserver) {
        responseObservers.offer(responseObserver);
        @SuppressWarnings("unchecked")
        StreamObserver<DiscoveryRequest> requestObserver = mock(StreamObserver.class);
        requestObservers.offer(requestObserver);
        Answer<Void> closeRpc = new Answer<Void>() {
          @Override
          public Void answer(InvocationOnMock invocation) {
            responseObserver.onCompleted();
            return null;
          }
        };
        doAnswer(closeRpc).when(requestObserver).onCompleted();

        return requestObserver;
      }
    };

    cleanupRule.register(
        InProcessServerBuilder
            .forName(serverName)
            .addService(serviceImpl)
            .directExecutor()
            .build()
            .start());
    channel =
        cleanupRule.register(InProcessChannelBuilder.forName(serverName).directExecutor().build());
    xdsClient =
        new XdsClientImpl(serverName, NODE, null, syncContext,
            fakeClock.getScheduledExecutorService(), backoffPolicyProvider,
            fakeClock.getStopwatchSupplier().get(), HOSTNAME, PORT, configWatcher);
    xdsClient.startDiscoveryRpc(channel);
    assertThat(responseObservers).hasSize(1);
    assertThat(requestObservers).hasSize(1);
  }

  @After
  public void tearDown() {
    xdsClient.shutdownDiscoveryRpc();
    channel.shutdown();
  }

  // Always test from the entire workflow: start with LDS, then RDS (if necessary), then CDS,
  // then EDS. Even if the test case covers only a specific resource type response handling.

  // Discovery responses should follow management server spec and xDS protocol. See
  // https://www.envoyproxy.io/docs/envoy/latest/api-docs/xds_protocol.

  /**
   * Client sends back a NACK LDS request when receiving an LDS response that does not contain a
   * listener for the requested resource.
   *
   * <p>This is the case when an LDS response does not contain the info for the requested resource.
   * Client should silently wait for future updates.
   */
  @Test
  public void nackLdsResponseWithoutMatchingResource() {
    StreamObserver<DiscoveryResponse> responseObserver = responseObservers.poll();
    StreamObserver<DiscoveryRequest> requestObserver = requestObservers.poll();

    // Client sends an LDS request for the host name (with port) to management server.
    verify(requestObserver)
        .onNext(eq(buildDiscoveryRequest("", "foo.googleapis.com:8080",
            XdsClientImpl.ADS_TYPE_URL_LDS, "")));

    List<Any> listeners = ImmutableList.of(
        Any.pack(buildListener("bar.googleapis.com",
            Any.pack(HttpConnectionManager.getDefaultInstance()))),
        Any.pack(buildListener("baz.googleapis.com",
            Any.pack(HttpConnectionManager.getDefaultInstance()))),
        Any.pack(buildListener("foo.googleapis.com:443",
            Any.pack(HttpConnectionManager.getDefaultInstance())))
    );
    DiscoveryResponse response =
        buildDiscoveryResponse("0", listeners, XdsClientImpl.ADS_TYPE_URL_LDS, "0000");
    responseObserver.onNext(response);

    // Client sends an NACK LDS request.
    verify(requestObserver)
        .onNext(eq(buildDiscoveryRequest("", "foo.googleapis.com:8080",
            XdsClientImpl.ADS_TYPE_URL_LDS, "0000")));

    verifyNoMoreInteractions(requestObserver);
    verifyZeroInteractions(configWatcher);
  }

  /**
   * An LDS response contains the requested listener and an in-lined RouteConfiguration message for
   * that listener. But VirtualHost information for the cluster cannot be resolved.
   * An error is returned to the watching party.
   */
  @Test
  public void failToFindVirtualHostInLdsResponseInLineRouteConfig() {
    StreamObserver<DiscoveryResponse> responseObserver = responseObservers.poll();
    StreamObserver<DiscoveryRequest> requestObserver = requestObservers.poll();

    // Client sends an LDS request for the host name (with port) to management server.
    verify(requestObserver)
        .onNext(eq(buildDiscoveryRequest("", "foo.googleapis.com:8080",
            XdsClientImpl.ADS_TYPE_URL_LDS, "")));

    RouteConfiguration routeConfig =
        buildRouteConfiguration(
            "do not care",  // don't care route config name when in-lined
            ImmutableList.of(
                buildVirtualHost(ImmutableList.of("something does not match"),
                    "some cluster"),
                buildVirtualHost(ImmutableList.of("something else does not match"),
                    "some other cluster")));

    List<Any> listeners = ImmutableList.of(
        Any.pack(buildListener("bar.googleapis.com",
            Any.pack(HttpConnectionManager.getDefaultInstance()))),
        Any.pack(buildListener("baz.googleapis.com",
            Any.pack(HttpConnectionManager.getDefaultInstance()))),
        Any.pack(buildListener("foo.googleapis.com:8080", /* matching resource */
            Any.pack(HttpConnectionManager.newBuilder().setRouteConfig(routeConfig).build())))
    );
    DiscoveryResponse response =
        buildDiscoveryResponse("0", listeners, XdsClientImpl.ADS_TYPE_URL_LDS, "0000");
    responseObserver.onNext(response);

    // Client sends an NACK LDS request.
    verify(requestObserver)
        .onNext(eq(buildDiscoveryRequest("", "foo.googleapis.com:8080",
            XdsClientImpl.ADS_TYPE_URL_LDS, "0000")));

    ArgumentCaptor<Status> errorStatusCaptor = ArgumentCaptor.forClass(null);
    verify(configWatcher).onError(errorStatusCaptor.capture());
    Status error = errorStatusCaptor.getValue();
    assertThat(error.getCode()).isEqualTo(Code.NOT_FOUND);
    assertThat(error.getDescription())
        .isEqualTo("Virtual host for target foo.googleapis.com:8080 not found");

    verifyNoMoreInteractions(requestObserver);
  }

  /**
   * Client resolves the virtual host config from an LDS response that contains a
   * RouteConfiguration message directly in-line for the requested resource. No RDS is needed.
   * Config is returned to the watching party.
   */
  @Test
  public void resolveVirtualHostInLdsResponse() {
    StreamObserver<DiscoveryResponse> responseObserver = responseObservers.poll();
    StreamObserver<DiscoveryRequest> requestObserver = requestObservers.poll();

    // Client sends an LDS request for the host name (with port) to management server.
    verify(requestObserver)
        .onNext(eq(buildDiscoveryRequest("", "foo.googleapis.com:8080",
            XdsClientImpl.ADS_TYPE_URL_LDS, "")));

    RouteConfiguration routeConfig =
        buildRouteConfiguration(
            "do not care",  // don't care route config name when in-lined
            ImmutableList.of(
                buildVirtualHost(ImmutableList.of("foo.googleapis.com", "bar.googleapis.com"),
                    "cluster.googleapis.com"),
                buildVirtualHost(ImmutableList.of("something does not match"),
                    "some cluster")));

    List<Any> listeners = ImmutableList.of(
        Any.pack(buildListener("bar.googleapis.com",
            Any.pack(HttpConnectionManager.getDefaultInstance()))),
        Any.pack(buildListener("baz.googleapis.com",
            Any.pack(HttpConnectionManager.getDefaultInstance()))),
        Any.pack(buildListener("foo.googleapis.com:8080", /* matching resource */
            Any.pack(HttpConnectionManager.newBuilder().setRouteConfig(routeConfig).build())))
    );
    DiscoveryResponse response =
        buildDiscoveryResponse("0", listeners, XdsClientImpl.ADS_TYPE_URL_LDS, "0000");
    responseObserver.onNext(response);

    // Client sends an ACK request.
    verify(requestObserver)
        .onNext(eq(buildDiscoveryRequest("0", "foo.googleapis.com:8080",
            XdsClientImpl.ADS_TYPE_URL_LDS, "0000")));

    ArgumentCaptor<ConfigUpdate> configUpdateCaptor = ArgumentCaptor.forClass(null);
    verify(configWatcher).onConfigChanged(configUpdateCaptor.capture());
    assertThat(configUpdateCaptor.getValue().getClusterName()).isEqualTo("cluster.googleapis.com");

    verifyNoMoreInteractions(requestObserver);
  }

  /**
   * Client sends back a NACK RDS request when receiving an RDS response that does not contain a
   * route for the requested resource.
   *
   * <p>This is the case when an RDS response does not contain the info for the requested resource.
   * Client should silently wait for future updates. For the case of handling an RDS response
   * containing the info for the requested resource but no matching VirtualHost can be found,
   * see {@link #failToFindVirtualHostInRdsResponse}.
   *
   */
  @Test
  public void nackRdsResponseWithoutMatchingResource() {
    StreamObserver<DiscoveryResponse> responseObserver = responseObservers.poll();
    StreamObserver<DiscoveryRequest> requestObserver = requestObservers.poll();

    // Client sends an LDS request for the host name (with port) to management server.
    verify(requestObserver)
        .onNext(eq(buildDiscoveryRequest("", "foo.googleapis.com:8080",
            XdsClientImpl.ADS_TYPE_URL_LDS, "")));

    Rds rdsConfig =
        Rds.newBuilder()
            .setConfigSource(ConfigSource.getDefaultInstance())
            .setRouteConfigName("route-foo.googleapis.com")
            .build();
    List<Any> listeners = ImmutableList.of(
        Any.pack(buildListener("bar.googleapis.com",
            Any.pack(HttpConnectionManager.getDefaultInstance()))),
        Any.pack(buildListener("baz.googleapis.com",
            Any.pack(HttpConnectionManager.getDefaultInstance()))),
        Any.pack(buildListener("foo.googleapis.com:8080", /* matching resource */
            Any.pack(HttpConnectionManager.newBuilder().setRds(rdsConfig).build())))
    );
    DiscoveryResponse response =
        buildDiscoveryResponse("0", listeners, XdsClientImpl.ADS_TYPE_URL_LDS, "0000");
    responseObserver.onNext(response);

    // Client sends an ACK LDS request.
    verify(requestObserver)
        .onNext(eq(buildDiscoveryRequest("0", "foo.googleapis.com:8080",
            XdsClientImpl.ADS_TYPE_URL_LDS, "0000")));

    // Client sends an (first) RDS request.
    verify(requestObserver)
        .onNext(eq(buildDiscoveryRequest("", "route-foo.googleapis.com",
            XdsClientImpl.ADS_TYPE_URL_RDS, "")));

    List<Any> routeConfigs = ImmutableList.of(
        Any.pack(
            buildRouteConfiguration(
                "some resource name does not match route-foo.googleapis.com",
                ImmutableList.of(
                    buildVirtualHost(ImmutableList.of("whatever"),
                        "whatever cluster")))),
        Any.pack(
            buildRouteConfiguration(
                "some other resource name does not match route-foo.googleapis.com",
                ImmutableList.of(
                    buildVirtualHost(ImmutableList.of("also whatever"),
                        "some more whatever cluster")))));
    response = buildDiscoveryResponse("0", routeConfigs, XdsClientImpl.ADS_TYPE_URL_RDS, "0000");
    responseObserver.onNext(response);

    // Client sends an NACK RDS request.
    verify(requestObserver)
        .onNext(eq(buildDiscoveryRequest("", "route-foo.googleapis.com",
            XdsClientImpl.ADS_TYPE_URL_RDS, "0000")));

    verifyNoMoreInteractions(requestObserver);
    verifyZeroInteractions(configWatcher);
  }

  /**
   * Client resolves the virtual host config from an RDS response for the requested resource.
   * Config is returned to the watching party.
   */
  @Test
  public void resolveVirtualHostInRdsResponse() {
    StreamObserver<DiscoveryResponse> responseObserver = responseObservers.poll();
    StreamObserver<DiscoveryRequest> requestObserver = requestObservers.poll();

    Rds rdsConfig =
        Rds.newBuilder()
            .setConfigSource(ConfigSource.getDefaultInstance())
            .setRouteConfigName("route-foo.googleapis.com")
            .build();

    List<Any> listeners = ImmutableList.of(
        Any.pack(buildListener("bar.googleapis.com",
            Any.pack(HttpConnectionManager.getDefaultInstance()))),
        Any.pack(buildListener("baz.googleapis.com",
            Any.pack(HttpConnectionManager.getDefaultInstance()))),
        Any.pack(buildListener("foo.googleapis.com:8080", /* matching resource */
            Any.pack(HttpConnectionManager.newBuilder().setRds(rdsConfig).build())))
    );
    DiscoveryResponse response =
        buildDiscoveryResponse("0", listeners, XdsClientImpl.ADS_TYPE_URL_LDS, "0000");
    responseObserver.onNext(response);

    // Client sends an ACK LDS request and an RDS request for "route-foo.googleapis.com". (Omitted)

    List<Any> routeConfigs = ImmutableList.of(
        Any.pack(
            buildRouteConfiguration(
                "route-foo.googleapis.com",
                ImmutableList.of(
                    buildVirtualHost(ImmutableList.of("something does not match"),
                        "some cluster"),
                    buildVirtualHost(ImmutableList.of("foo.googleapis.com", "bar.googleapis.com"),
                        "cluster.googleapis.com")))),  // matching virtual host
        Any.pack(
            buildRouteConfiguration(
                "some resource name does not match route-foo.googleapis.com",
                ImmutableList.of(
                    buildVirtualHost(ImmutableList.of("something also does not match"),
                        "some more cluster")))));
    response = buildDiscoveryResponse("0", routeConfigs, XdsClientImpl.ADS_TYPE_URL_RDS, "0000");
    responseObserver.onNext(response);

    // Client sent an ACK RDS request.
    verify(requestObserver).onNext(
        buildDiscoveryRequest("0", "route-foo.googleapis.com",
            XdsClientImpl.ADS_TYPE_URL_RDS, "0000"));

    ArgumentCaptor<ConfigUpdate> configUpdateCaptor = ArgumentCaptor.forClass(null);
    verify(configWatcher).onConfigChanged(configUpdateCaptor.capture());
    assertThat(configUpdateCaptor.getValue().getClusterName()).isEqualTo("cluster.googleapis.com");
  }

  /**
   * Client cannot find the virtual host config in the RDS response for the requested resource.
   * An error is returned to the watching party.
   */
  @Test
  public void failToFindVirtualHostInRdsResponse() {
    StreamObserver<DiscoveryResponse> responseObserver = responseObservers.poll();
    StreamObserver<DiscoveryRequest> requestObserver = requestObservers.poll();

    Rds rdsConfig =
        Rds.newBuilder()
            .setConfigSource(ConfigSource.getDefaultInstance())
            .setRouteConfigName("route-foo.googleapis.com")
            .build();

    List<Any> listeners = ImmutableList.of(
        Any.pack(buildListener("bar.googleapis.com",
            Any.pack(HttpConnectionManager.getDefaultInstance()))),
        Any.pack(buildListener("baz.googleapis.com",
            Any.pack(HttpConnectionManager.getDefaultInstance()))),
        Any.pack(buildListener("foo.googleapis.com:8080", /* matching resource */
            Any.pack(HttpConnectionManager.newBuilder().setRds(rdsConfig).build())))
    );
    DiscoveryResponse response =
        buildDiscoveryResponse("0", listeners, XdsClientImpl.ADS_TYPE_URL_LDS, "0000");
    responseObserver.onNext(response);

    // Client sends an ACK LDS request and an RDS request for "route-foo.googleapis.com". (Omitted)

    List<Any> routeConfigs = ImmutableList.of(
        Any.pack(
            buildRouteConfiguration(
                "route-foo.googleapis.com",
                ImmutableList.of(
                    buildVirtualHost(ImmutableList.of("something does not match"),
                        "some cluster"),
                    buildVirtualHost(
                        ImmutableList.of("something else does not match", "also does not match"),
                        "cluster.googleapis.com")))),
        Any.pack(
            buildRouteConfiguration(
                "some resource name does not match route-foo.googleapis.com",
                ImmutableList.of(
                    buildVirtualHost(ImmutableList.of("one more does not match"),
                        "some more cluster")))));
    response = buildDiscoveryResponse("0", routeConfigs, XdsClientImpl.ADS_TYPE_URL_RDS, "0000");
    responseObserver.onNext(response);

    // Client sent an NACK RDS request.
    verify(requestObserver).onNext(
        buildDiscoveryRequest("", "route-foo.googleapis.com",
            XdsClientImpl.ADS_TYPE_URL_RDS, "0000"));

    ArgumentCaptor<Status> errorStatusCaptor = ArgumentCaptor.forClass(null);
    verify(configWatcher).onError(errorStatusCaptor.capture());
    Status error = errorStatusCaptor.getValue();
    assertThat(error.getCode()).isEqualTo(Status.Code.NOT_FOUND);
    assertThat(error.getDescription())
        .isEqualTo("Virtual host for target foo.googleapis.com:8080 not found");
  }

  /**
   * The VirtualHost message in RDS response contains a VirtualHost with domain matching the
   * requested host name, but cannot be resolved to find a cluster name in the RouteAction message.
   */
  @Test
  public void matchingVirtualHostDoesNotContainRouteAction() {
    StreamObserver<DiscoveryResponse> responseObserver = responseObservers.poll();
    StreamObserver<DiscoveryRequest> requestObserver = requestObservers.poll();

    Rds rdsConfig =
        Rds.newBuilder()
            .setConfigSource(ConfigSource.getDefaultInstance())
            .setRouteConfigName("route-foo.googleapis.com")
            .build();

    List<Any> listeners = ImmutableList.of(
        Any.pack(buildListener("bar.googleapis.com",
            Any.pack(HttpConnectionManager.getDefaultInstance()))),
        Any.pack(buildListener("baz.googleapis.com",
            Any.pack(HttpConnectionManager.getDefaultInstance()))),
        Any.pack(buildListener("foo.googleapis.com:8080", /* matching resource */
            Any.pack(HttpConnectionManager.newBuilder().setRds(rdsConfig).build())))
    );
    DiscoveryResponse response =
        buildDiscoveryResponse("0", listeners, XdsClientImpl.ADS_TYPE_URL_LDS, "0000");
    responseObserver.onNext(response);

    // Client sends an ACK LDS request and an RDS request for "route-foo.googleapis.com". (Omitted)

    // A VirtualHost with a Route that contains only redirect configuration.
    VirtualHost virtualHost =
        VirtualHost.newBuilder()
            .setName("virtualhost00.googleapis.com")  // don't care
            .addDomains("foo.googleapis.com")
            .addRoutes(
                Route.newBuilder()
                    .setRedirect(
                        RedirectAction.newBuilder()
                            .setHostRedirect("bar.googleapis.com")
                            .setPortRedirect(443)))
            .build();

    List<Any> routeConfigs = ImmutableList.of(
        Any.pack(
            buildRouteConfiguration("route-foo.googleapis.com",
                ImmutableList.of(virtualHost))));
    response = buildDiscoveryResponse("0", routeConfigs, XdsClientImpl.ADS_TYPE_URL_RDS, "0000");
    responseObserver.onNext(response);

    // Client sent an NACK RDS request.
    verify(requestObserver).onNext(
        buildDiscoveryRequest("", "route-foo.googleapis.com",
            XdsClientImpl.ADS_TYPE_URL_RDS, "0000"));

    ArgumentCaptor<Status> errorStatusCaptor = ArgumentCaptor.forClass(null);
    verify(configWatcher).onError(errorStatusCaptor.capture());
    Status error = errorStatusCaptor.getValue();
    assertThat(error.getCode()).isEqualTo(Status.Code.NOT_FOUND);
    assertThat(error.getDescription())
        .isEqualTo("Virtual host for target foo.googleapis.com:8080 not found");
  }
  
  // TODO(chengyuanzhang): retry tests.

  @Test
  public void matchHostName_exactlyMatch() {
    String pattern = "foo.googleapis.com";
    assertThat(XdsClientImpl.matchHostName("bar.googleapis.com", pattern)).isFalse();
    assertThat(XdsClientImpl.matchHostName("fo.googleapis.com", pattern)).isFalse();
    assertThat(XdsClientImpl.matchHostName("oo.googleapis.com", pattern)).isFalse();
    assertThat(XdsClientImpl.matchHostName("googleapis.com", pattern)).isFalse();
    assertThat(XdsClientImpl.matchHostName("foo.googleapis", pattern)).isFalse();
    assertThat(XdsClientImpl.matchHostName("foo.googleapis.com", pattern)).isTrue();
  }

  @Test
  public void matchHostName_prefixWildcard() {
    String pattern = "*.foo.googleapis.com";
    assertThat(XdsClientImpl.matchHostName("foo.googleapis.com", pattern)).isFalse();
    assertThat(XdsClientImpl.matchHostName("bar-baz.foo.googleapis", pattern)).isFalse();
    assertThat(XdsClientImpl.matchHostName("bar.foo.googleapis.com", pattern)).isTrue();
    pattern = "*-bar.foo.googleapis.com";
    assertThat(XdsClientImpl.matchHostName("bar.foo.googleapis.com", pattern)).isFalse();
    assertThat(XdsClientImpl.matchHostName("baz-bar.foo.googleapis", pattern)).isFalse();
    assertThat(XdsClientImpl.matchHostName("-bar.foo.googleapis.com", pattern)).isFalse();
    assertThat(XdsClientImpl.matchHostName("baz-bar.foo.googleapis.com", pattern))
        .isTrue();
  }

  @Test
  public void matchHostName_postfixMatch() {
    String pattern = "foo.*";
    assertThat(XdsClientImpl.matchHostName("bar.googleapis.com", pattern)).isFalse();
    assertThat(XdsClientImpl.matchHostName("bar.foo.googleapis.com", pattern)).isFalse();
    assertThat(XdsClientImpl.matchHostName("foo.googleapis.com", pattern)).isTrue();
    assertThat(XdsClientImpl.matchHostName("foo.com", pattern)).isTrue();
    pattern = "foo-*";
    assertThat(XdsClientImpl.matchHostName("bar-.googleapis.com", pattern)).isFalse();
    assertThat(XdsClientImpl.matchHostName("foo.googleapis.com", pattern)).isFalse();
    assertThat(XdsClientImpl.matchHostName("foo.googleapis.com", pattern)).isFalse();
    assertThat(XdsClientImpl.matchHostName("foo-", pattern)).isFalse();
    assertThat(XdsClientImpl.matchHostName("foo-bar.com", pattern)).isTrue();
    assertThat(XdsClientImpl.matchHostName("foo-.com", pattern)).isTrue();
    assertThat(XdsClientImpl.matchHostName("foo-bar", pattern)).isTrue();
  }

  // https://www.envoyproxy.io/docs/envoy/latest/api-v2/api/v2/discovery.proto#discoveryresponse
  private static DiscoveryResponse buildDiscoveryResponse(String versionInfo,
      List<com.google.protobuf.Any> resources, String typeUrl, String nonce) {
    return
        DiscoveryResponse.newBuilder()
            .setVersionInfo(versionInfo)
            .setTypeUrl(typeUrl)
            .addAllResources(resources)
            .setNonce(nonce)
            .build();
  }

  // https://www.envoyproxy.io/docs/envoy/latest/api-v2/api/v2/discovery.proto#discoveryrequest
  private static DiscoveryRequest buildDiscoveryRequest(String versionInfo,
      String resourceName, String typeUrl, String nonce) {
    return
        DiscoveryRequest.newBuilder()
            .setVersionInfo(versionInfo)
            .setNode(NODE)
            .setTypeUrl(typeUrl)
            .addResourceNames(resourceName)
            .setResponseNonce(nonce)
            .build();
  }

  // https://www.envoyproxy.io/docs/envoy/v1.5.0/api-v2/lds.proto
  private static Listener buildListener(String name, com.google.protobuf.Any apiListener) {
    return
        Listener.newBuilder()
            .setName(name)
            .setAddress(Address.getDefaultInstance())
            .addFilterChains(FilterChain.getDefaultInstance())
            .setApiListener(ApiListener.newBuilder().setApiListener(apiListener))
            .build();
  }

  // https://www.envoyproxy.io/docs/envoy/latest/api-v2/api/v2/rds.proto#routeconfiguration
  private static RouteConfiguration buildRouteConfiguration(String name,
      List<VirtualHost> virtualHosts) {
    return
        RouteConfiguration.newBuilder()
            .setName(name)
            .addAllVirtualHosts(virtualHosts)
            .build();
  }

  // https://www.envoyproxy.io/docs/envoy/v1.5.0/api-v1/route_config/vhost#virtual-host
  private static VirtualHost buildVirtualHost(List<String> domains, String clusterName) {
    return
        VirtualHost.newBuilder()
            .setName("virtualhost00.googleapis.com")  // don't care
            .addAllDomains(domains)
            .addRoutes(Route.newBuilder()
                .setRoute(RouteAction.newBuilder().setCluster("whatever cluster")))
            .addRoutes(
                // Only the last (default) route matters.
                Route.newBuilder()
                    .setRoute(RouteAction.newBuilder().setCluster(clusterName)))
            .build();
  }
}
