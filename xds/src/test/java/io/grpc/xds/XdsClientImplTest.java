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
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.Any;
import com.google.protobuf.UInt32Value;
import io.envoyproxy.envoy.api.v2.ClusterLoadAssignment;
import io.envoyproxy.envoy.api.v2.ClusterLoadAssignment.Policy;
import io.envoyproxy.envoy.api.v2.DiscoveryRequest;
import io.envoyproxy.envoy.api.v2.DiscoveryResponse;
import io.envoyproxy.envoy.api.v2.Listener;
import io.envoyproxy.envoy.api.v2.RouteConfiguration;
import io.envoyproxy.envoy.api.v2.core.Address;
import io.envoyproxy.envoy.api.v2.core.AggregatedConfigSource;
import io.envoyproxy.envoy.api.v2.core.ConfigSource;
import io.envoyproxy.envoy.api.v2.core.HealthStatus;
import io.envoyproxy.envoy.api.v2.core.Node;
import io.envoyproxy.envoy.api.v2.core.SocketAddress;
import io.envoyproxy.envoy.api.v2.listener.FilterChain;
import io.envoyproxy.envoy.api.v2.route.RedirectAction;
import io.envoyproxy.envoy.api.v2.route.Route;
import io.envoyproxy.envoy.api.v2.route.RouteAction;
import io.envoyproxy.envoy.api.v2.route.VirtualHost;
import io.envoyproxy.envoy.config.filter.network.http_connection_manager.v2.HttpConnectionManager;
import io.envoyproxy.envoy.config.filter.network.http_connection_manager.v2.Rds;
import io.envoyproxy.envoy.config.listener.v2.ApiListener;
import io.envoyproxy.envoy.service.discovery.v2.AggregatedDiscoveryServiceGrpc.AggregatedDiscoveryServiceImplBase;
import io.envoyproxy.envoy.type.FractionalPercent;
import io.envoyproxy.envoy.type.FractionalPercent.DenominatorType;
import io.grpc.Context;
import io.grpc.Context.CancellationListener;
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
import io.grpc.xds.EnvoyProtoData.DropOverload;
import io.grpc.xds.EnvoyProtoData.LbEndpoint;
import io.grpc.xds.EnvoyProtoData.Locality;
import io.grpc.xds.EnvoyProtoData.LocalityLbEndpoints;
import io.grpc.xds.XdsClient.ConfigUpdate;
import io.grpc.xds.XdsClient.ConfigWatcher;
import io.grpc.xds.XdsClient.EndpointUpdate;
import io.grpc.xds.XdsClient.EndpointWatcher;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatcher;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

/**
 * Tests for {@link XdsClientImpl}.
 */
public class XdsClientImplTest {

  private static final String HOSTNAME = "foo.googleapis.com";
  private static final int PORT = 8080;

  private static final Node NODE = Node.getDefaultInstance();
  private static final FakeClock.TaskFilter RPC_RETRY_TASK_FILTER =
      new FakeClock.TaskFilter() {
        @Override
        public boolean shouldAccept(Runnable command) {
          return command.toString().contains(XdsClientImpl.RpcRetryTask.class.getSimpleName());
        }
      };

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
  private final AtomicBoolean callEnded = new AtomicBoolean(true);

  @Mock
  private AggregatedDiscoveryServiceImplBase mockedDiscoveryService;
  @Mock
  private BackoffPolicy.Provider backoffPolicyProvider;
  @Mock
  private BackoffPolicy backoffPolicy1;
  @Mock
  private BackoffPolicy backoffPolicy2;
  @Mock
  private ConfigWatcher configWatcher;
  @Mock
  private EndpointWatcher endpointWatcher;

  private ManagedChannel channel;
  private XdsClientImpl xdsClient;

  @Before
  public void setUp() throws IOException {
    MockitoAnnotations.initMocks(this);
    when(backoffPolicyProvider.get()).thenReturn(backoffPolicy1, backoffPolicy2);
    when(backoffPolicy1.nextBackoffNanos()).thenReturn(10L, 100L);
    when(backoffPolicy2.nextBackoffNanos()).thenReturn(20L, 200L);

    String serverName = InProcessServerBuilder.generateName();
    AggregatedDiscoveryServiceImplBase serviceImpl = new AggregatedDiscoveryServiceImplBase() {
      @Override
      public StreamObserver<DiscoveryRequest> streamAggregatedResources(
          final StreamObserver<DiscoveryResponse> responseObserver) {
        assertThat(callEnded.get()).isTrue();  // ensure previous call was ended
        callEnded.set(false);
        Context.current().addListener(
            new CancellationListener() {
              @Override
              public void cancelled(Context context) {
                callEnded.set(true);
              }
            }, MoreExecutors.directExecutor());
        responseObservers.offer(responseObserver);
        @SuppressWarnings("unchecked")
        StreamObserver<DiscoveryRequest> requestObserver = mock(StreamObserver.class);
        requestObservers.offer(requestObserver);
        return requestObserver;
      }
    };
    mockedDiscoveryService =
        mock(AggregatedDiscoveryServiceImplBase.class, delegatesTo(serviceImpl));

    cleanupRule.register(
        InProcessServerBuilder
            .forName(serverName)
            .addService(mockedDiscoveryService)
            .directExecutor()
            .build()
            .start());
    channel =
        cleanupRule.register(InProcessChannelBuilder.forName(serverName).directExecutor().build());
    xdsClient =
        new XdsClientImpl(serverName, channel, NODE, syncContext,
            fakeClock.getScheduledExecutorService(), backoffPolicyProvider,
            fakeClock.getStopwatchSupplier().get());
    // Only the connection to management server is established, no RPC request is sent until at
    // least one watcher is registered.
    assertThat(responseObservers).isEmpty();
    assertThat(requestObservers).isEmpty();
  }

  @After
  public void tearDown() {
    xdsClient.shutdown();
    assertThat(callEnded.get()).isTrue();
    assertThat(channel.isShutdown()).isTrue();
    assertThat(fakeClock.getPendingTasks()).isEmpty();
  }

  // Always test the real workflow and integrity of XdsClient: RDS protocol should always followed
  // after at least one LDS request-response, from which the RDS resource name comes. CDS and EDS
  // can be tested separately as they are used in a standalone way.

  // Discovery responses should follow management server spec and xDS protocol. See
  // https://www.envoyproxy.io/docs/envoy/latest/api-docs/xds_protocol.

  /**
   * Client receives an LDS response that does not contain a Listener for the requested resource.
   * The LDS response is ACKed.
   * The config watcher is notified with an error.
   */
  @Test
  public void ldsResponseWithoutMatchingResource() {
    xdsClient.watchConfigData(HOSTNAME, PORT, configWatcher);
    StreamObserver<DiscoveryResponse> responseObserver = responseObservers.poll();
    StreamObserver<DiscoveryRequest> requestObserver = requestObservers.poll();

    // Client sends an LDS request for the host name (with port) to management server.
    verify(requestObserver)
        .onNext(eq(buildDiscoveryRequest("", "foo.googleapis.com:8080",
            XdsClientImpl.ADS_TYPE_URL_LDS, "")));

    List<Any> listeners = ImmutableList.of(
        Any.pack(buildListener("bar.googleapis.com",
            Any.pack(HttpConnectionManager.newBuilder()
                .setRouteConfig(
                    buildRouteConfiguration("route-bar.googleapis.com",
                        ImmutableList.of(
                            buildVirtualHost(
                                ImmutableList.of("bar.googleapis.com"),
                                "cluster-bar.googleapis.com"))))
                .build()))),
        Any.pack(buildListener("baz.googleapis.com",
            Any.pack(HttpConnectionManager.newBuilder()
                .setRouteConfig(
                    buildRouteConfiguration("route-baz.googleapis.com",
                        ImmutableList.of(
                            buildVirtualHost(
                                ImmutableList.of("baz.googleapis.com"),
                                "cluster-baz.googleapis.com"))))
                .build()))));
    DiscoveryResponse response =
        buildDiscoveryResponse("0", listeners, XdsClientImpl.ADS_TYPE_URL_LDS, "0000");
    responseObserver.onNext(response);

    // Client sends an ACK LDS request.
    verify(requestObserver)
        .onNext(eq(buildDiscoveryRequest("0", "foo.googleapis.com:8080",
            XdsClientImpl.ADS_TYPE_URL_LDS, "0000")));

    ArgumentCaptor<Status> errorStatusCaptor = ArgumentCaptor.forClass(null);
    verify(configWatcher).onError(errorStatusCaptor.capture());
    Status error = errorStatusCaptor.getValue();
    assertThat(error.getCode()).isEqualTo(Code.NOT_FOUND);
    assertThat(error.getDescription())
        .isEqualTo("Listener for requested resource [foo.googleapis.com:8080] does not exist");

    verifyNoMoreInteractions(requestObserver);
  }

  /**
   * An LDS response contains the requested listener and an in-lined RouteConfiguration message for
   * that listener. But the RouteConfiguration message is invalid as it does not contain any
   * VirtualHost with domains matching the requested hostname.
   * The LDS response is NACKed, as if the XdsClient has not received this response.
   * The config watcher is NOT notified with an error.
   */
  @Test
  public void failToFindVirtualHostInLdsResponseInLineRouteConfig() {
    xdsClient.watchConfigData(HOSTNAME, PORT, configWatcher);
    StreamObserver<DiscoveryResponse> responseObserver = responseObservers.poll();
    StreamObserver<DiscoveryRequest> requestObserver = requestObservers.poll();

    // Client sends an LDS request for the host name (with port) to management server.
    verify(requestObserver)
        .onNext(eq(buildDiscoveryRequest("", "foo.googleapis.com:8080",
            XdsClientImpl.ADS_TYPE_URL_LDS, "")));

    RouteConfiguration routeConfig =
        buildRouteConfiguration(
            "route.googleapis.com",
            ImmutableList.of(
                buildVirtualHost(ImmutableList.of("something does not match"),
                    "some cluster"),
                buildVirtualHost(ImmutableList.of("something else does not match"),
                    "some other cluster")));

    List<Any> listeners = ImmutableList.of(
        Any.pack(buildListener("foo.googleapis.com:8080", /* matching resource */
            Any.pack(HttpConnectionManager.newBuilder().setRouteConfig(routeConfig).build()))));
    DiscoveryResponse response =
        buildDiscoveryResponse("0", listeners, XdsClientImpl.ADS_TYPE_URL_LDS, "0000");
    responseObserver.onNext(response);

    // Client sends an NACK LDS request.
    verify(requestObserver)
        .onNext(
            argThat(new DiscoveryRequestMatcher("", "foo.googleapis.com:8080",
                XdsClientImpl.ADS_TYPE_URL_LDS, "0000")));

    verify(configWatcher, never()).onConfigChanged(any(ConfigUpdate.class));
    verify(configWatcher, never()).onError(any(Status.class));
    verifyNoMoreInteractions(requestObserver);
  }

  /**
   * Client resolves the virtual host config from an LDS response that contains a
   * RouteConfiguration message directly in-line for the requested resource. No RDS is needed.
   * The LDS response is ACKed.
   * The config watcher is notified with an update.
   */
  @Test
  public void resolveVirtualHostInLdsResponse() {
    xdsClient.watchConfigData(HOSTNAME, PORT, configWatcher);
    StreamObserver<DiscoveryResponse> responseObserver = responseObservers.poll();
    StreamObserver<DiscoveryRequest> requestObserver = requestObservers.poll();

    // Client sends an LDS request for the host name (with port) to management server.
    verify(requestObserver)
        .onNext(eq(buildDiscoveryRequest("", "foo.googleapis.com:8080",
            XdsClientImpl.ADS_TYPE_URL_LDS, "")));

    List<Any> listeners = ImmutableList.of(
        Any.pack(buildListener("bar.googleapis.com",
            Any.pack(HttpConnectionManager.newBuilder()
                .setRouteConfig(
                    buildRouteConfiguration("route-bar.googleapis.com",
                        ImmutableList.of(
                            buildVirtualHost(
                                ImmutableList.of("bar.googleapis.com"),
                                "cluster-bar.googleapis.com"))))
                .build()))),
        Any.pack(buildListener("baz.googleapis.com",
            Any.pack(HttpConnectionManager.newBuilder()
                .setRouteConfig(
                    buildRouteConfiguration("route-baz.googleapis.com",
                        ImmutableList.of(
                            buildVirtualHost(
                                ImmutableList.of("baz.googleapis.com"),
                                "cluster-baz.googleapis.com"))))
                .build()))),
        Any.pack(buildListener("foo.googleapis.com:8080", /* matching resource */
            Any.pack(
                HttpConnectionManager.newBuilder()
                    .setRouteConfig(
                        buildRouteConfiguration("route-foo.googleapis.com",
                            ImmutableList.of(
                                buildVirtualHost(
                                    ImmutableList.of("foo.googleapis.com", "bar.googleapis.com"),
                                    "cluster.googleapis.com"),
                                buildVirtualHost(
                                    ImmutableList.of("something does not match"),
                                    "some cluster"))))
                    .build()))));
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
   * Client receives an RDS response (after a previous LDS request-response) that does not contain a
   * RouteConfiguration for the requested resource while each received RouteConfiguration is valid.
   * The RDS response is ACKed.
   * The config watcher is NOT notified with an error (RDS protocol is incremental, responses
   * not containing requested resources does not indicate absence).
   */
  @Test
  public void rdsResponseWithoutMatchingResource() {
    xdsClient.watchConfigData(HOSTNAME, PORT, configWatcher);
    StreamObserver<DiscoveryResponse> responseObserver = responseObservers.poll();
    StreamObserver<DiscoveryRequest> requestObserver = requestObservers.poll();

    // Client sends an LDS request for the host name (with port) to management server.
    verify(requestObserver)
        .onNext(eq(buildDiscoveryRequest("", "foo.googleapis.com:8080",
            XdsClientImpl.ADS_TYPE_URL_LDS, "")));

    Rds rdsConfig =
        Rds.newBuilder()
            // Must set to use ADS.
            .setConfigSource(
                ConfigSource.newBuilder().setAds(AggregatedConfigSource.getDefaultInstance()))
            .setRouteConfigName("route-foo.googleapis.com")
            .build();
    List<Any> listeners = ImmutableList.of(
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

    // Management server should only sends RouteConfiguration messages with at least one
    // VirtualHost with domains matching requested hostname. Otherwise, it is invalid data.
    List<Any> routeConfigs = ImmutableList.of(
        Any.pack(
            buildRouteConfiguration(
                "some resource name does not match route-foo.googleapis.com",
                ImmutableList.of(
                    buildVirtualHost(ImmutableList.of("foo.googleapis.com"),
                        "whatever cluster")))),
        Any.pack(
            buildRouteConfiguration(
                "some other resource name does not match route-foo.googleapis.com",
                ImmutableList.of(
                    buildVirtualHost(ImmutableList.of("foo.googleapis.com"),
                        "some more whatever cluster")))));
    response = buildDiscoveryResponse("0", routeConfigs, XdsClientImpl.ADS_TYPE_URL_RDS, "0000");
    responseObserver.onNext(response);

    // Client sends an ACK RDS request.
    verify(requestObserver)
        .onNext(eq(buildDiscoveryRequest("0", "route-foo.googleapis.com",
            XdsClientImpl.ADS_TYPE_URL_RDS, "0000")));

    verify(configWatcher, never()).onConfigChanged(any(ConfigUpdate.class));
    verify(configWatcher, never()).onError(any(Status.class));
  }

  /**
   * Client resolves the virtual host config from an RDS response for the requested resource. The
   * RDS response is ACKed.
   * The config watcher is notified with an update.
   */
  @Test
  public void resolveVirtualHostInRdsResponse() {
    xdsClient.watchConfigData(HOSTNAME, PORT, configWatcher);
    StreamObserver<DiscoveryResponse> responseObserver = responseObservers.poll();
    StreamObserver<DiscoveryRequest> requestObserver = requestObservers.poll();

    Rds rdsConfig =
        Rds.newBuilder()
            // Must set to use ADS.
            .setConfigSource(
                ConfigSource.newBuilder().setAds(AggregatedConfigSource.getDefaultInstance()))
            .setRouteConfigName("route-foo.googleapis.com")
            .build();

    List<Any> listeners = ImmutableList.of(
        Any.pack(buildListener("foo.googleapis.com:8080", /* matching resource */
            Any.pack(HttpConnectionManager.newBuilder().setRds(rdsConfig).build())))
    );
    DiscoveryResponse response =
        buildDiscoveryResponse("0", listeners, XdsClientImpl.ADS_TYPE_URL_LDS, "0000");
    responseObserver.onNext(response);

    // Client sends an ACK LDS request and an RDS request for "route-foo.googleapis.com". (Omitted)

    // Management server should only sends RouteConfiguration messages with at least one
    // VirtualHost with domains matching requested hostname. Otherwise, it is invalid data.
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
                    buildVirtualHost(ImmutableList.of("foo.googleapis.com"),
                        "some more cluster")))));
    response = buildDiscoveryResponse("0", routeConfigs, XdsClientImpl.ADS_TYPE_URL_RDS, "0000");
    responseObserver.onNext(response);

    // Client sent an ACK RDS request.
    verify(requestObserver)
        .onNext(eq(buildDiscoveryRequest("0", "route-foo.googleapis.com",
            XdsClientImpl.ADS_TYPE_URL_RDS, "0000")));

    ArgumentCaptor<ConfigUpdate> configUpdateCaptor = ArgumentCaptor.forClass(null);
    verify(configWatcher).onConfigChanged(configUpdateCaptor.capture());
    assertThat(configUpdateCaptor.getValue().getClusterName()).isEqualTo("cluster.googleapis.com");
  }

  /**
   * Client receives an RDS response (after a previous LDS request-response) containing a
   * RouteConfiguration message for the requested resource. But the RouteConfiguration message
   * is invalid as it does not contain any VirtualHost with domains matching the requested
   * hostname.
   * The LDS response is NACKed, as if the XdsClient has not received this response.
   * The config watcher is NOT notified with an error.
   */
  @Test
  public void failToFindVirtualHostInRdsResponse() {
    xdsClient.watchConfigData(HOSTNAME, PORT, configWatcher);
    StreamObserver<DiscoveryResponse> responseObserver = responseObservers.poll();
    StreamObserver<DiscoveryRequest> requestObserver = requestObservers.poll();

    Rds rdsConfig =
        Rds.newBuilder()
            // Must set to use ADS.
            .setConfigSource(
                ConfigSource.newBuilder().setAds(AggregatedConfigSource.getDefaultInstance()))
            .setRouteConfigName("route-foo.googleapis.com")
            .build();

    List<Any> listeners = ImmutableList.of(
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
    verify(requestObserver)
        .onNext(
            argThat(new DiscoveryRequestMatcher("", "route-foo.googleapis.com",
                XdsClientImpl.ADS_TYPE_URL_RDS, "0000")));

    verify(configWatcher, never()).onConfigChanged(any(ConfigUpdate.class));
    verify(configWatcher, never()).onError(any(Status.class));
  }

  /**
   * Client receives an RDS response (after a previous LDS request-response) containing a
   * RouteConfiguration message for the requested resource. But the RouteConfiguration message
   * is invalid as the VirtualHost with domains matching the requested hostname contains invalid
   * data, its RouteAction message is absent.
   * The LDS response is NACKed, as if the XdsClient has not received this response.
   * The config watcher is NOT notified with an error.
   */
  @Test
  public void matchingVirtualHostDoesNotContainRouteAction() {
    xdsClient.watchConfigData(HOSTNAME, PORT, configWatcher);
    StreamObserver<DiscoveryResponse> responseObserver = responseObservers.poll();
    StreamObserver<DiscoveryRequest> requestObserver = requestObservers.poll();

    Rds rdsConfig =
        Rds.newBuilder()
            // Must set to use ADS.
            .setConfigSource(
                ConfigSource.newBuilder().setAds(AggregatedConfigSource.getDefaultInstance()))
            .setRouteConfigName("route-foo.googleapis.com")
            .build();

    List<Any> listeners = ImmutableList.of(
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
    verify(requestObserver)
        .onNext(
            argThat(new DiscoveryRequestMatcher("", "route-foo.googleapis.com",
                XdsClientImpl.ADS_TYPE_URL_RDS, "0000")));

    verify(configWatcher, never()).onConfigChanged(any(ConfigUpdate.class));
    verify(configWatcher, never()).onError(any(Status.class));
  }

  /**
   * Client receives LDS/RDS responses for updating resources previously received.
   *
   * <p>Tests for streaming behavior.
   */
  @Test
  public void notifyUpdatedResources() {
    xdsClient.watchConfigData(HOSTNAME, PORT, configWatcher);
    StreamObserver<DiscoveryResponse> responseObserver = responseObservers.poll();
    StreamObserver<DiscoveryRequest> requestObserver = requestObservers.poll();

    // Client sends an LDS request for the host name (with port) to management server.
    verify(requestObserver)
        .onNext(eq(buildDiscoveryRequest("", "foo.googleapis.com:8080",
            XdsClientImpl.ADS_TYPE_URL_LDS, "")));

    // Management server sends back an LDS response containing a RouteConfiguration for the
    // requested Listener directly in-line.
    RouteConfiguration routeConfig =
        buildRouteConfiguration(
            "route-foo.googleapis.com",
            ImmutableList.of(
                buildVirtualHost(ImmutableList.of("foo.googleapis.com", "bar.googleapis.com"),
                    "cluster.googleapis.com"),
                buildVirtualHost(ImmutableList.of("something does not match"),
                    "some cluster")));

    List<Any> listeners = ImmutableList.of(
        Any.pack(buildListener("foo.googleapis.com:8080", /* matching resource */
            Any.pack(HttpConnectionManager.newBuilder().setRouteConfig(routeConfig).build())))
    );
    DiscoveryResponse response =
        buildDiscoveryResponse("0", listeners, XdsClientImpl.ADS_TYPE_URL_LDS, "0000");
    responseObserver.onNext(response);

    // Client sends an ACK LDS request.
    verify(requestObserver)
        .onNext(eq(buildDiscoveryRequest("0", "foo.googleapis.com:8080",
            XdsClientImpl.ADS_TYPE_URL_LDS, "0000")));

    // Cluster name is resolved and notified to config watcher.
    ArgumentCaptor<ConfigUpdate> configUpdateCaptor = ArgumentCaptor.forClass(null);
    verify(configWatcher).onConfigChanged(configUpdateCaptor.capture());
    assertThat(configUpdateCaptor.getValue().getClusterName()).isEqualTo("cluster.googleapis.com");

    // Management sends back another LDS response containing updates for the requested Listener.
    routeConfig =
        buildRouteConfiguration(
            "another-route-foo.googleapis.com",
            ImmutableList.of(
                buildVirtualHost(ImmutableList.of("foo.googleapis.com", "bar.googleapis.com"),
                    "another-cluster.googleapis.com"),
                buildVirtualHost(ImmutableList.of("something does not match"),
                    "some cluster")));

    listeners = ImmutableList.of(
        Any.pack(buildListener("foo.googleapis.com:8080", /* matching resource */
            Any.pack(HttpConnectionManager.newBuilder().setRouteConfig(routeConfig).build())))
    );
    response =
        buildDiscoveryResponse("1", listeners, XdsClientImpl.ADS_TYPE_URL_LDS, "0001");
    responseObserver.onNext(response);

    // Client sends an ACK LDS request.
    verify(requestObserver)
        .onNext(eq(buildDiscoveryRequest("1", "foo.googleapis.com:8080",
            XdsClientImpl.ADS_TYPE_URL_LDS, "0001")));

    // Updated cluster name is notified to config watcher.
    configUpdateCaptor = ArgumentCaptor.forClass(null);
    verify(configWatcher, times(2)).onConfigChanged(configUpdateCaptor.capture());
    assertThat(configUpdateCaptor.getValue().getClusterName())
        .isEqualTo("another-cluster.googleapis.com");

    // Management server sends back another LDS response containing updates for the requested
    // Listener and telling client to do RDS.
    Rds rdsConfig =
        Rds.newBuilder()
            // Must set to use ADS.
            .setConfigSource(
                ConfigSource.newBuilder().setAds(AggregatedConfigSource.getDefaultInstance()))
            .setRouteConfigName("some-route-to-foo.googleapis.com")
            .build();

    listeners = ImmutableList.of(
        Any.pack(buildListener("foo.googleapis.com:8080", /* matching resource */
            Any.pack(HttpConnectionManager.newBuilder().setRds(rdsConfig).build())))
    );
    response =
        buildDiscoveryResponse("2", listeners, XdsClientImpl.ADS_TYPE_URL_LDS, "0002");
    responseObserver.onNext(response);

    // Client sends an ACK LDS request.
    verify(requestObserver)
        .onNext(eq(buildDiscoveryRequest("2", "foo.googleapis.com:8080",
            XdsClientImpl.ADS_TYPE_URL_LDS, "0002")));

    // Client sends an (first) RDS request.
    verify(requestObserver)
        .onNext(eq(buildDiscoveryRequest("", "some-route-to-foo.googleapis.com",
            XdsClientImpl.ADS_TYPE_URL_RDS, "")));

    // Management server sends back an RDS response containing the RouteConfiguration
    // for the requested resource.
    List<Any> routeConfigs = ImmutableList.of(
        Any.pack(
            buildRouteConfiguration(
                "some-route-to-foo.googleapis.com",
                ImmutableList.of(
                    buildVirtualHost(ImmutableList.of("something does not match"),
                        "some cluster"),
                    buildVirtualHost(ImmutableList.of("foo.googleapis.com", "bar.googleapis.com"),
                        "some-other-cluster.googleapis.com")))));
    response = buildDiscoveryResponse("0", routeConfigs, XdsClientImpl.ADS_TYPE_URL_RDS, "0000");
    responseObserver.onNext(response);

    // Client sent an ACK RDS request.
    verify(requestObserver)
        .onNext(eq(buildDiscoveryRequest("0", "some-route-to-foo.googleapis.com",
            XdsClientImpl.ADS_TYPE_URL_RDS, "0000")));

    // Updated cluster name is notified to config watcher again.
    configUpdateCaptor = ArgumentCaptor.forClass(null);
    verify(configWatcher, times(3)).onConfigChanged(configUpdateCaptor.capture());
    assertThat(configUpdateCaptor.getValue().getClusterName())
        .isEqualTo("some-other-cluster.googleapis.com");

    // Management server sends back another RDS response containing updated information for the
    // RouteConfiguration currently in-use by client.
    routeConfigs = ImmutableList.of(
        Any.pack(
            buildRouteConfiguration(
                "some-route-to-foo.googleapis.com",
                ImmutableList.of(
                    buildVirtualHost(ImmutableList.of("foo.googleapis.com", "bar.googleapis.com"),
                        "an-updated-cluster.googleapis.com")))));
    response = buildDiscoveryResponse("1", routeConfigs, XdsClientImpl.ADS_TYPE_URL_RDS, "0001");
    responseObserver.onNext(response);

    // Client sent an ACK RDS request.
    verify(requestObserver)
        .onNext(eq(buildDiscoveryRequest("1", "some-route-to-foo.googleapis.com",
            XdsClientImpl.ADS_TYPE_URL_RDS, "0001")));

    // Updated cluster name is notified to config watcher again.
    configUpdateCaptor = ArgumentCaptor.forClass(null);
    verify(configWatcher, times(4)).onConfigChanged(configUpdateCaptor.capture());
    assertThat(configUpdateCaptor.getValue().getClusterName())
        .isEqualTo("an-updated-cluster.googleapis.com");
  }

  // TODO(chengyuanzhang): tests for timeout waiting for responses for incremental
  //  protocols (RDS/EDS).

  /**
   * Client receives multiple RDS responses without RouteConfiguration for the requested
   * resource. It should continue waiting until such an RDS response arrives, as RDS
   * protocol is incremental.
   *
   * <p>Tests for RDS incremental protocol behavior.
   */
  @Test
  public void waitRdsResponsesForRequestedResource() {
    xdsClient.watchConfigData(HOSTNAME, PORT, configWatcher);
    StreamObserver<DiscoveryResponse> responseObserver = responseObservers.poll();
    StreamObserver<DiscoveryRequest> requestObserver = requestObservers.poll();

    // Client sends an LDS request for the host name (with port) to management server.
    verify(requestObserver)
        .onNext(eq(buildDiscoveryRequest("", "foo.googleapis.com:8080",
            XdsClientImpl.ADS_TYPE_URL_LDS, "")));

    // Management sends back an LDS response telling client to do RDS.
    Rds rdsConfig =
        Rds.newBuilder()
            // Must set to use ADS.
            .setConfigSource(
                ConfigSource.newBuilder().setAds(AggregatedConfigSource.getDefaultInstance()))
            .setRouteConfigName("route-foo.googleapis.com")
            .build();

    List<Any> listeners = ImmutableList.of(
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

    // Management server sends back an RDS response that does not contain RouteConfiguration
    // for the requested resource.
    List<Any> routeConfigs = ImmutableList.of(
        Any.pack(
            buildRouteConfiguration(
                "some resource name does not match route-foo.googleapis.com",
                ImmutableList.of(
                    buildVirtualHost(ImmutableList.of("foo.googleapis.com"),
                        "some more cluster")))));
    response = buildDiscoveryResponse("0", routeConfigs, XdsClientImpl.ADS_TYPE_URL_RDS, "0000");
    responseObserver.onNext(response);

    // Client sent an ACK RDS request.
    verify(requestObserver)
        .onNext(eq(buildDiscoveryRequest("0", "route-foo.googleapis.com",
            XdsClientImpl.ADS_TYPE_URL_RDS, "0000")));

    // Client waits for future RDS responses silently.
    verifyNoMoreInteractions(configWatcher);

    // Management server sends back another RDS response containing the RouteConfiguration
    // for the requested resource.
    routeConfigs = ImmutableList.of(
        Any.pack(
            buildRouteConfiguration(
                "route-foo.googleapis.com",
                ImmutableList.of(
                    buildVirtualHost(ImmutableList.of("something does not match"),
                        "some cluster"),
                    buildVirtualHost(ImmutableList.of("foo.googleapis.com", "bar.googleapis.com"),
                        "another-cluster.googleapis.com")))));
    response = buildDiscoveryResponse("1", routeConfigs, XdsClientImpl.ADS_TYPE_URL_RDS, "0001");
    responseObserver.onNext(response);

    // Client sent an ACK RDS request.
    verify(requestObserver)
        .onNext(eq(buildDiscoveryRequest("1", "route-foo.googleapis.com",
            XdsClientImpl.ADS_TYPE_URL_RDS, "0001")));

    // Updated cluster name is notified to config watcher.
    ArgumentCaptor<ConfigUpdate> configUpdateCaptor = ArgumentCaptor.forClass(null);
    verify(configWatcher).onConfigChanged(configUpdateCaptor.capture());
    assertThat(configUpdateCaptor.getValue().getClusterName())
        .isEqualTo("another-cluster.googleapis.com");
  }

  /**
   * Client receives RDS responses containing RouteConfigurations for resources that were
   * not requested (management server sends them proactively). Later client receives an LDS
   * response with the requested Listener containing Rds config pointing to do RDS for one of
   * the previously received RouteConfigurations. No RDS request needs to be sent for that
   * RouteConfiguration as it can be found in local cache (management server will not send
   * RDS responses for that RouteConfiguration again). A future RDS response update for
   * that RouteConfiguration should be notified to config watcher.
   *
   * <p>Tests for caching RDS response data behavior.
   */
  @Test
  public void receiveRdsResponsesForRouteConfigurationsToBeUsedLater() {
    xdsClient.watchConfigData(HOSTNAME, PORT, configWatcher);
    StreamObserver<DiscoveryResponse> responseObserver = responseObservers.poll();
    StreamObserver<DiscoveryRequest> requestObserver = requestObservers.poll();

    // Client sends an LDS request for the host name (with port) to management server.
    verify(requestObserver)
        .onNext(eq(buildDiscoveryRequest("", "foo.googleapis.com:8080",
            XdsClientImpl.ADS_TYPE_URL_LDS, "")));

    // Management sends back an LDS response telling client to do RDS.
    Rds rdsConfig =
        Rds.newBuilder()
            // Must set to use ADS.
            .setConfigSource(
                ConfigSource.newBuilder().setAds(AggregatedConfigSource.getDefaultInstance()))
            .setRouteConfigName("route-foo1.googleapis.com")
            .build();

    List<Any> listeners = ImmutableList.of(
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
        .onNext(eq(buildDiscoveryRequest("", "route-foo1.googleapis.com",
            XdsClientImpl.ADS_TYPE_URL_RDS, "")));

    // Management server sends back an RDS response containing RouteConfigurations
    // more than requested.
    List<Any> routeConfigs = ImmutableList.of(
        // Currently wanted resource.
        Any.pack(
            buildRouteConfiguration(
                "route-foo1.googleapis.com",
                ImmutableList.of(
                    buildVirtualHost(ImmutableList.of("foo.googleapis.com"),
                        "cluster1.googleapis.com")))),
        // Resources currently not wanted.
        Any.pack(
            buildRouteConfiguration(
                "route-foo2.googleapis.com",
                ImmutableList.of(
                    buildVirtualHost(ImmutableList.of("foo.googleapis.com"),
                        "cluster2.googleapis.com")))),
        Any.pack(
            buildRouteConfiguration(
                "route-foo3.googleapis.com",
                ImmutableList.of(
                    buildVirtualHost(ImmutableList.of("foo.googleapis.com"),
                        "cluster3.googleapis.com")))));
    response = buildDiscoveryResponse("0", routeConfigs, XdsClientImpl.ADS_TYPE_URL_RDS, "0000");
    responseObserver.onNext(response);

    // Client sent an ACK RDS request.
    verify(requestObserver)
        .onNext(eq(buildDiscoveryRequest("0", "route-foo1.googleapis.com",
            XdsClientImpl.ADS_TYPE_URL_RDS, "0000")));

    // Resolved cluster name is notified to config watcher.
    ArgumentCaptor<ConfigUpdate> configUpdateCaptor = ArgumentCaptor.forClass(null);
    verify(configWatcher).onConfigChanged(configUpdateCaptor.capture());
    assertThat(configUpdateCaptor.getValue().getClusterName()).isEqualTo("cluster1.googleapis.com");

    // Management server sends back another LDS response containing updates for the requested
    // Listener and telling client to do RDS for a RouteConfiguration which had previously
    // sent to client.
    rdsConfig =
        Rds.newBuilder()
            // Must set to use ADS.
            .setConfigSource(
                ConfigSource.newBuilder().setAds(AggregatedConfigSource.getDefaultInstance()))
            .setRouteConfigName("route-foo2.googleapis.com")
            .build();

    listeners = ImmutableList.of(
        Any.pack(buildListener("foo.googleapis.com:8080", /* matching resource */
            Any.pack(HttpConnectionManager.newBuilder().setRds(rdsConfig).build())))
    );
    response = buildDiscoveryResponse("1", listeners, XdsClientImpl.ADS_TYPE_URL_LDS, "0001");
    responseObserver.onNext(response);

    // Client sent an ACK LDS request.
    verify(requestObserver)
        .onNext(eq(buildDiscoveryRequest("1", "foo.googleapis.com:8080",
            XdsClientImpl.ADS_TYPE_URL_LDS, "0001")));

    // Updated cluster name is notified to config watcher.
    configUpdateCaptor = ArgumentCaptor.forClass(null);
    verify(configWatcher, times(2)).onConfigChanged(configUpdateCaptor.capture());
    assertThat(configUpdateCaptor.getValue().getClusterName()).isEqualTo("cluster2.googleapis.com");

    // At this time, no RDS request is sent as the result can be found in local cache (even if
    // a request is sent for it, management server does not necessarily reply).
    verify(requestObserver, times(0))
        .onNext(eq(buildDiscoveryRequest("0", "route-foo.googleapis.com",
            XdsClientImpl.ADS_TYPE_URL_RDS, "0000")));

    verifyNoMoreInteractions(requestObserver);

    // Management server sends back another RDS response containing updates for the
    // RouteConfiguration that the client was pointed to most recently (i.e.,
    // "route-foo2.googleapis.com").
    routeConfigs = ImmutableList.of(
        Any.pack(
            buildRouteConfiguration(
                "route-foo2.googleapis.com",
                ImmutableList.of(
                    buildVirtualHost(ImmutableList.of("foo.googleapis.com"),
                        "a-new-cluster.googleapis.com")))));
    response = buildDiscoveryResponse("1", routeConfigs, XdsClientImpl.ADS_TYPE_URL_RDS, "0001");
    responseObserver.onNext(response);

    // Client sent an ACK RDS request.
    verify(requestObserver)
        .onNext(eq(buildDiscoveryRequest("1", "route-foo2.googleapis.com",
            XdsClientImpl.ADS_TYPE_URL_RDS, "0001")));

    // Updated cluster name is notified to config watcher.
    configUpdateCaptor = ArgumentCaptor.forClass(null);
    verify(configWatcher, times(3)).onConfigChanged(configUpdateCaptor.capture());
    assertThat(configUpdateCaptor.getValue().getClusterName())
        .isEqualTo("a-new-cluster.googleapis.com");
  }

  /**
   * An RouteConfiguration is removed by server by sending client an LDS response removing the
   * corresponding Listener.
   */
  @Test
  public void routeConfigurationRemovedNotifiedToWatcher() {
    xdsClient.watchConfigData(HOSTNAME, PORT, configWatcher);
    StreamObserver<DiscoveryResponse> responseObserver = responseObservers.poll();
    StreamObserver<DiscoveryRequest> requestObserver = requestObservers.poll();

    // Client sends an LDS request for the host name (with port) to management server.
    verify(requestObserver)
        .onNext(eq(buildDiscoveryRequest("", "foo.googleapis.com:8080",
            XdsClientImpl.ADS_TYPE_URL_LDS, "")));

    // Management sends back an LDS response telling client to do RDS.
    Rds rdsConfig =
        Rds.newBuilder()
            // Must set to use ADS.
            .setConfigSource(
                ConfigSource.newBuilder().setAds(AggregatedConfigSource.getDefaultInstance()))
            .setRouteConfigName("route-foo.googleapis.com")
            .build();

    List<Any> listeners = ImmutableList.of(
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

    // Management server sends back an RDS response containing RouteConfiguration requested.
    List<Any> routeConfigs = ImmutableList.of(
        Any.pack(
            buildRouteConfiguration(
                "route-foo.googleapis.com",
                ImmutableList.of(
                    buildVirtualHost(ImmutableList.of("foo.googleapis.com"),
                        "cluster.googleapis.com")))));
    response = buildDiscoveryResponse("0", routeConfigs, XdsClientImpl.ADS_TYPE_URL_RDS, "0000");
    responseObserver.onNext(response);

    // Client sent an ACK RDS request.
    verify(requestObserver)
        .onNext(eq(buildDiscoveryRequest("0", "route-foo.googleapis.com",
            XdsClientImpl.ADS_TYPE_URL_RDS, "0000")));

    // Resolved cluster name is notified to config watcher.
    ArgumentCaptor<ConfigUpdate> configUpdateCaptor = ArgumentCaptor.forClass(null);
    verify(configWatcher).onConfigChanged(configUpdateCaptor.capture());
    assertThat(configUpdateCaptor.getValue().getClusterName()).isEqualTo("cluster.googleapis.com");

    // Management server sends back another LDS response with the previous Listener (currently
    // in-use by client) removed as the RouteConfiguration it references to is absent.
    response =
        buildDiscoveryResponse("1", ImmutableList.<com.google.protobuf.Any>of(), // empty
            XdsClientImpl.ADS_TYPE_URL_LDS, "0001");
    responseObserver.onNext(response);

    // Client sent an ACK LDS request.
    verify(requestObserver)
        .onNext(eq(buildDiscoveryRequest("1", "foo.googleapis.com:8080",
            XdsClientImpl.ADS_TYPE_URL_LDS, "0001")));

    // Notify config watcher with an error.
    ArgumentCaptor<Status> errorStatusCaptor = ArgumentCaptor.forClass(null);
    verify(configWatcher).onError(errorStatusCaptor.capture());
    Status error = errorStatusCaptor.getValue();
    assertThat(error.getCode()).isEqualTo(Code.NOT_FOUND);
    assertThat(error.getDescription())
        .isEqualTo("Listener for requested resource [foo.googleapis.com:8080] does not exist");
  }

  /**
   * Client receives an EDS response that does not contain a ClusterLoadAssignment for the
   * requested resource while each received ClusterLoadAssignment is valid.
   * The EDS response is ACKed.
<<<<<<< HEAD
   * Endpoint watchers are NOT notified with an error (EDS protocol is incremental, responses
=======
   * The config watcher is NOT notified with an error (EDS protocol is incremental, responses
>>>>>>> Add tests for EDS protocol and endpoint watchers.
   * not containing requested resources does not indicate absence).
   */
  @Test
  public void edsResponseWithoutMatchingResource() {
    xdsClient.watchEndpointData("cluster-foo.googleapis.com", endpointWatcher);
    StreamObserver<DiscoveryResponse> responseObserver = responseObservers.poll();
    StreamObserver<DiscoveryRequest> requestObserver = requestObservers.poll();

    // Client sends an EDS request for the only cluster being watched to management server.
    verify(requestObserver)
        .onNext(eq(buildDiscoveryRequest("", "cluster-foo.googleapis.com",
            XdsClientImpl.ADS_TYPE_URL_EDS, "")));

    // Management server sends back an EDS response without ClusterLoadAssignment for the requested
    // cluster.
    List<Any> clusterLoadAssignments = ImmutableList.of(
        Any.pack(buildClusterLoadAssignment("cluster-bar.googleapis.com",
            ImmutableList.of(
                buildLocalityLbEndpoints("region1", "zone1", "subzone1",
                    ImmutableList.of(
                        buildLbEndpoint("192.168.0.1", 8080, HealthStatus.HEALTHY, 2)),
                    1, 0)),
            ImmutableList.<ClusterLoadAssignment.Policy.DropOverload>of())),
        Any.pack(buildClusterLoadAssignment("cluster-baz.googleapis.com",
            ImmutableList.of(
                buildLocalityLbEndpoints("region2", "zone2", "subzone2",
                    ImmutableList.of(
                        buildLbEndpoint("192.168.234.52", 8888, HealthStatus.UNKNOWN, 5)),
                    6, 1)),
            ImmutableList.<ClusterLoadAssignment.Policy.DropOverload>of())));

    DiscoveryResponse response =
        buildDiscoveryResponse("0", clusterLoadAssignments,
            XdsClientImpl.ADS_TYPE_URL_EDS, "0000");
    responseObserver.onNext(response);

    // Client sent an ACK EDS request.
    verify(requestObserver)
        .onNext(eq(buildDiscoveryRequest("0", "cluster-foo.googleapis.com",
            XdsClientImpl.ADS_TYPE_URL_EDS, "0000")));

    verifyZeroInteractions(endpointWatcher);
  }

  /**
   * Normal workflow of receiving an EDS response containing ClusterLoadAssignment message for
   * a requested cluster.
   */
  @Test
  public void edsResponseWithMatchingResource() {
    xdsClient.watchEndpointData("cluster-foo.googleapis.com", endpointWatcher);
    StreamObserver<DiscoveryResponse> responseObserver = responseObservers.poll();
    StreamObserver<DiscoveryRequest> requestObserver = requestObservers.poll();

    // Client sends an EDS request for the only cluster being watched to management server.
    verify(requestObserver)
        .onNext(eq(buildDiscoveryRequest("", "cluster-foo.googleapis.com",
            XdsClientImpl.ADS_TYPE_URL_EDS, "")));

    // Management server sends back an EDS response with ClusterLoadAssignment for the requested
    // cluster.
    List<Any> clusterLoadAssignments = ImmutableList.of(
        Any.pack(buildClusterLoadAssignment("cluster-foo.googleapis.com",
            ImmutableList.of(
                buildLocalityLbEndpoints("region1", "zone1", "subzone1",
                    ImmutableList.of(
                        buildLbEndpoint("192.168.0.1", 8080, HealthStatus.HEALTHY, 2)),
                    1, 0),
                buildLocalityLbEndpoints("region3", "zone3", "subzone3",
                    ImmutableList.of(
                        buildLbEndpoint("192.168.142.5", 80, HealthStatus.UNKNOWN, 5)),
                    2, 1)),
            ImmutableList.of(
                buildDropOverload("lb", 200),
                buildDropOverload("throttle", 1000)))),
        Any.pack(buildClusterLoadAssignment("cluster-baz.googleapis.com",
            ImmutableList.of(
                buildLocalityLbEndpoints("region2", "zone2", "subzone2",
                    ImmutableList.of(
                        buildLbEndpoint("192.168.234.52", 8888, HealthStatus.UNKNOWN, 5)),
                    6, 1)),
            ImmutableList.<ClusterLoadAssignment.Policy.DropOverload>of())));

    DiscoveryResponse response =
        buildDiscoveryResponse("0", clusterLoadAssignments,
            XdsClientImpl.ADS_TYPE_URL_EDS, "0000");
    responseObserver.onNext(response);

    // Client sent an ACK EDS request.
    verify(requestObserver)
        .onNext(eq(buildDiscoveryRequest("0", "cluster-foo.googleapis.com",
            XdsClientImpl.ADS_TYPE_URL_EDS, "0000")));

    ArgumentCaptor<EndpointUpdate> endpointUpdateCaptor = ArgumentCaptor.forClass(null);
    verify(endpointWatcher).onEndpointChanged(endpointUpdateCaptor.capture());
    EndpointUpdate endpointUpdate = endpointUpdateCaptor.getValue();
    assertThat(endpointUpdate.getClusterName()).isEqualTo("cluster-foo.googleapis.com");
    assertThat(endpointUpdate.getDropPolicies())
        .containsExactly(
            new DropOverload("lb", 200),
            new DropOverload("throttle", 1000));
    assertThat(endpointUpdate.getLocalityLbEndpointsMap())
        .containsExactly(
            new Locality("region1", "zone1", "subzone1"),
            new LocalityLbEndpoints(
                ImmutableList.of(
                    new LbEndpoint("192.168.0.1", 8080,
                        2, true)), 1, 0),
            new Locality("region3", "zone3", "subzone3"),
            new LocalityLbEndpoints(
                ImmutableList.of(
                    new LbEndpoint("192.168.142.5", 80,
                        5, true)), 2, 1));
  }

  @Test
  public void multipleEndpointWatchers() {
    EndpointWatcher watcher1 = mock(EndpointWatcher.class);
    EndpointWatcher watcher2 = mock(EndpointWatcher.class);
    EndpointWatcher watcher3 = mock(EndpointWatcher.class);
    xdsClient.watchEndpointData("cluster-foo.googleapis.com", watcher1);
    xdsClient.watchEndpointData("cluster-foo.googleapis.com", watcher2);
    xdsClient.watchEndpointData("cluster-bar.googleapis.com", watcher3);

    StreamObserver<DiscoveryResponse> responseObserver = responseObservers.poll();
    StreamObserver<DiscoveryRequest> requestObserver = requestObservers.poll();

    // Client sends an EDS request containing all clusters being watched to management server.
    verify(requestObserver)
        .onNext(
            argThat(
                new DiscoveryRequestMatcher("",
                    ImmutableList.of("cluster-foo.googleapis.com", "cluster-bar.googleapis.com"),
                    XdsClientImpl.ADS_TYPE_URL_EDS, "")));

    // Management server sends back an EDS response contains ClusterLoadAssignment for only one of
    // requested cluster.
    List<Any> clusterLoadAssignments = ImmutableList.of(
        Any.pack(buildClusterLoadAssignment("cluster-foo.googleapis.com",
            ImmutableList.of(
                buildLocalityLbEndpoints("region1", "zone1", "subzone1",
                    ImmutableList.of(
                        buildLbEndpoint("192.168.0.1", 8080, HealthStatus.HEALTHY, 2)),
                    1, 0)),
            ImmutableList.<Policy.DropOverload>of())));

    DiscoveryResponse response =
        buildDiscoveryResponse("0", clusterLoadAssignments,
            XdsClientImpl.ADS_TYPE_URL_EDS, "0000");
    responseObserver.onNext(response);

    // Client sent an ACK EDS request.
    verify(requestObserver)
        .onNext(
            argThat(
                new DiscoveryRequestMatcher("0",
                    ImmutableList.of("cluster-foo.googleapis.com", "cluster-bar.googleapis.com"),
                    XdsClientImpl.ADS_TYPE_URL_EDS, "0000")));

    // Two watchers get notification of endpoint update for the cluster they are interested in.
    ArgumentCaptor<EndpointUpdate> endpointUpdateCaptor1 = ArgumentCaptor.forClass(null);
    verify(watcher1).onEndpointChanged(endpointUpdateCaptor1.capture());
    EndpointUpdate endpointUpdate1 = endpointUpdateCaptor1.getValue();
    assertThat(endpointUpdate1.getClusterName()).isEqualTo("cluster-foo.googleapis.com");
    assertThat(endpointUpdate1.getLocalityLbEndpointsMap())
        .containsExactly(
            new Locality("region1", "zone1", "subzone1"),
            new LocalityLbEndpoints(
                ImmutableList.of(
                    new LbEndpoint("192.168.0.1", 8080,
                        2, true)), 1, 0));

    ArgumentCaptor<EndpointUpdate> endpointUpdateCaptor2 = ArgumentCaptor.forClass(null);
    verify(watcher1).onEndpointChanged(endpointUpdateCaptor2.capture());
    EndpointUpdate endpointUpdate2 = endpointUpdateCaptor2.getValue();
    assertThat(endpointUpdate2.getClusterName()).isEqualTo("cluster-foo.googleapis.com");
    assertThat(endpointUpdate2.getLocalityLbEndpointsMap())
        .containsExactly(
            new Locality("region1", "zone1", "subzone1"),
            new LocalityLbEndpoints(
                ImmutableList.of(
                    new LbEndpoint("192.168.0.1", 8080,
                        2, true)), 1, 0));

    verifyZeroInteractions(watcher3);

    // Management server sends back another EDS response contains ClusterLoadAssignment for the
    // other requested cluster.
    clusterLoadAssignments = ImmutableList.of(
        Any.pack(buildClusterLoadAssignment("cluster-bar.googleapis.com",
            ImmutableList.of(
                buildLocalityLbEndpoints("region2", "zone2", "subzone2",
                    ImmutableList.of(
                        buildLbEndpoint("192.168.234.52", 8888, HealthStatus.UNKNOWN, 5)),
                    6, 1)),
            ImmutableList.<ClusterLoadAssignment.Policy.DropOverload>of())));

    response = buildDiscoveryResponse("1", clusterLoadAssignments,
        XdsClientImpl.ADS_TYPE_URL_EDS, "0001");
    responseObserver.onNext(response);

    // Client sent an ACK EDS request.
    verify(requestObserver)
        .onNext(
            argThat(
                new DiscoveryRequestMatcher("1",
                    ImmutableList.of("cluster-foo.googleapis.com", "cluster-bar.googleapis.com"),
                    XdsClientImpl.ADS_TYPE_URL_EDS, "0001")));

    // The corresponding watcher gets notified.
    ArgumentCaptor<EndpointUpdate> endpointUpdateCaptor3 = ArgumentCaptor.forClass(null);
    verify(watcher3).onEndpointChanged(endpointUpdateCaptor3.capture());
    EndpointUpdate endpointUpdate3 = endpointUpdateCaptor3.getValue();
    assertThat(endpointUpdate3.getClusterName()).isEqualTo("cluster-bar.googleapis.com");
    assertThat(endpointUpdate3.getLocalityLbEndpointsMap())
        .containsExactly(
            new Locality("region2", "zone2", "subzone2"),
            new LocalityLbEndpoints(
                ImmutableList.of(
                    new LbEndpoint("192.168.234.52", 8888,
                        5, true)), 6, 1));
  }

  /**
   * (EDS response caching behavior) Adding endpoint watchers interested in some cluster that
   * some other endpoint watcher had already been watching on will result in endpoint update
   * notified to the newly added watcher immediately, without sending new EDS requests.
   */
  @Test
  public void receivedEndpointUpdateNotifiedToWatcherImmediately() {
    EndpointWatcher watcher1 = mock(EndpointWatcher.class);
    xdsClient.watchEndpointData("cluster-foo.googleapis.com", watcher1);

    // Streaming RPC starts after a first watcher is added.
    StreamObserver<DiscoveryResponse> responseObserver = responseObservers.poll();
    StreamObserver<DiscoveryRequest> requestObserver = requestObservers.poll();

    // Client sends an EDS request to management server.
    verify(requestObserver)
        .onNext(eq(buildDiscoveryRequest("", "cluster-foo.googleapis.com",
            XdsClientImpl.ADS_TYPE_URL_EDS, "")));

    // Management server sends back an EDS response with ClusterLoadAssignment for the requested
    // cluster.
    List<Any> clusterLoadAssignments = ImmutableList.of(
        Any.pack(buildClusterLoadAssignment("cluster-foo.googleapis.com",
            ImmutableList.of(
                buildLocalityLbEndpoints("region1", "zone1", "subzone1",
                    ImmutableList.of(
                        buildLbEndpoint("192.168.0.1", 8080, HealthStatus.HEALTHY, 2),
                        buildLbEndpoint("192.132.53.5", 80, HealthStatus.UNHEALTHY, 5)),
                    1, 0)),
            ImmutableList.<Policy.DropOverload>of())));

    DiscoveryResponse response =
        buildDiscoveryResponse("0", clusterLoadAssignments,
            XdsClientImpl.ADS_TYPE_URL_EDS, "0000");
    responseObserver.onNext(response);

    // Client sent an ACK EDS request.
    verify(requestObserver)
        .onNext(eq(buildDiscoveryRequest("0", "cluster-foo.googleapis.com",
            XdsClientImpl.ADS_TYPE_URL_EDS, "0000")));

    ArgumentCaptor<EndpointUpdate> endpointUpdateCaptor1 = ArgumentCaptor.forClass(null);
    verify(watcher1).onEndpointChanged(endpointUpdateCaptor1.capture());
    EndpointUpdate endpointUpdate1 = endpointUpdateCaptor1.getValue();
    assertThat(endpointUpdate1.getClusterName()).isEqualTo("cluster-foo.googleapis.com");
    assertThat(endpointUpdate1.getLocalityLbEndpointsMap())
        .containsExactly(
            new Locality("region1", "zone1", "subzone1"),
            new LocalityLbEndpoints(
                ImmutableList.of(
                    new LbEndpoint("192.168.0.1", 8080, 2, true),
                    new LbEndpoint("192.132.53.5", 80,5, false)),
                1, 0));

    // Another endpoint watcher interested in the same cluster is added.
    EndpointWatcher watcher2 = mock(EndpointWatcher.class);
    xdsClient.watchEndpointData("cluster-foo.googleapis.com", watcher2);

    // Since the client has received endpoint update for this cluster before, cached result is
    // notified to the newly added watcher immediately.
    ArgumentCaptor<EndpointUpdate> endpointUpdateCaptor2 = ArgumentCaptor.forClass(null);
    verify(watcher2).onEndpointChanged(endpointUpdateCaptor2.capture());
<<<<<<< HEAD
    EndpointUpdate endpointUpdate2 = endpointUpdateCaptor2.getValue();
=======
    EndpointUpdate endpointUpdate2 = endpointUpdateCaptor1.getValue();
>>>>>>> Add tests for EDS protocol and endpoint watchers.
    assertThat(endpointUpdate2.getClusterName()).isEqualTo("cluster-foo.googleapis.com");
    assertThat(endpointUpdate2.getLocalityLbEndpointsMap())
        .containsExactly(
            new Locality("region1", "zone1", "subzone1"),
            new LocalityLbEndpoints(
                ImmutableList.of(
                    new LbEndpoint("192.168.0.1", 8080, 2, true),
                    new LbEndpoint("192.132.53.5", 80,5, false)),
                1, 0));

    verifyNoMoreInteractions(requestObserver);
  }

<<<<<<< HEAD
  /**
   * An endpoint watcher is registered for a cluster that already has some other endpoint watchers
   * watching on. Endpoint information received previously is in local cache and notified to
   * the new watcher immediately.
   */
  @Test
  public void watchEndpointsForClusterAlreadyBeingWatched() {
    EndpointWatcher watcher1 = mock(EndpointWatcher.class);
    xdsClient.watchEndpointData("cluster-foo.googleapis.com", watcher1);
    StreamObserver<DiscoveryResponse> responseObserver = responseObservers.poll();
    StreamObserver<DiscoveryRequest> requestObserver = requestObservers.poll();

    // Client sends first EDS request.
    verify(requestObserver)
        .onNext(eq(buildDiscoveryRequest("", "cluster-foo.googleapis.com",
            XdsClientImpl.ADS_TYPE_URL_EDS, "")));

    // Management server sends back an EDS response containing ClusterLoadAssignments for
    // some cluster not requested.
    List<Any> clusterLoadAssignments = ImmutableList.of(
        Any.pack(buildClusterLoadAssignment("cluster-foo.googleapis.com",
            ImmutableList.of(
                buildLocalityLbEndpoints("region1", "zone1", "subzone1",
                    ImmutableList.of(
                        buildLbEndpoint("192.168.0.1", 8080, HealthStatus.HEALTHY, 2)),
                    1, 0)),
            ImmutableList.<Policy.DropOverload>of())));

    DiscoveryResponse response =
        buildDiscoveryResponse("0", clusterLoadAssignments,
            XdsClientImpl.ADS_TYPE_URL_EDS, "0000");
    responseObserver.onNext(response);

    // Client sent an ACK EDS request.
    verify(requestObserver)
        .onNext(eq(buildDiscoveryRequest("0", "cluster-foo.googleapis.com",
            XdsClientImpl.ADS_TYPE_URL_EDS, "0000")));

    ArgumentCaptor<EndpointUpdate> endpointUpdateCaptor1 = ArgumentCaptor.forClass(null);
    verify(watcher1).onEndpointChanged(endpointUpdateCaptor1.capture());
    EndpointUpdate endpointUpdate1 = endpointUpdateCaptor1.getValue();
    assertThat(endpointUpdate1.getClusterName()).isEqualTo("cluster-foo.googleapis.com");
    assertThat(endpointUpdate1.getDropPolicies()).isEmpty();
    assertThat(endpointUpdate1.getLocalityLbEndpointsMap())
        .containsExactly(
            new Locality("region1", "zone1", "subzone1"),
            new LocalityLbEndpoints(
                ImmutableList.of(
                    new LbEndpoint("192.168.0.1", 8080,
                        2, true)), 1, 0));

    // A second endpoint watcher is registered for endpoints in the same cluster.
    EndpointWatcher watcher2 = mock(EndpointWatcher.class);
    xdsClient.watchEndpointData("cluster-foo.googleapis.com", watcher2);

    // Cached endpoint information is notified to the new watcher immediately, without sending
    // another EDS request.
    ArgumentCaptor<EndpointUpdate> endpointUpdateCaptor2 = ArgumentCaptor.forClass(null);
    verify(watcher2).onEndpointChanged(endpointUpdateCaptor2.capture());
    EndpointUpdate endpointUpdate2 = endpointUpdateCaptor2.getValue();
    assertThat(endpointUpdate2.getClusterName()).isEqualTo("cluster-foo.googleapis.com");
    assertThat(endpointUpdate2.getDropPolicies()).isEmpty();
    assertThat(endpointUpdate2.getLocalityLbEndpointsMap())
        .containsExactly(
            new Locality("region1", "zone1", "subzone1"),
            new LocalityLbEndpoints(
                ImmutableList.of(
                    new LbEndpoint("192.168.0.1", 8080,
                        2, true)), 1, 0));

    verifyNoMoreInteractions(requestObserver);
  }

=======
>>>>>>> Add tests for EDS protocol and endpoint watchers.
  @Test
  public void addRemoveEndpointWatchersFreely() {
    EndpointWatcher watcher1 = mock(EndpointWatcher.class);
    xdsClient.watchEndpointData("cluster-foo.googleapis.com", watcher1);

    // Streaming RPC starts after a first watcher is added.
    StreamObserver<DiscoveryResponse> responseObserver = responseObservers.poll();
    StreamObserver<DiscoveryRequest> requestObserver = requestObservers.poll();

    // Client sends an EDS request to management server.
    verify(requestObserver)
        .onNext(eq(buildDiscoveryRequest("", "cluster-foo.googleapis.com",
            XdsClientImpl.ADS_TYPE_URL_EDS, "")));

    // Management server sends back an EDS response with ClusterLoadAssignment for the requested
    // cluster.
    List<Any> clusterLoadAssignments = ImmutableList.of(
        Any.pack(buildClusterLoadAssignment("cluster-foo.googleapis.com",
            ImmutableList.of(
                buildLocalityLbEndpoints("region1", "zone1", "subzone1",
                    ImmutableList.of(
                        buildLbEndpoint("192.168.0.1", 8080, HealthStatus.HEALTHY, 2),
                        buildLbEndpoint("192.132.53.5", 80, HealthStatus.UNHEALTHY, 5)),
                    1, 0)),
            ImmutableList.<Policy.DropOverload>of())));

    DiscoveryResponse response =
        buildDiscoveryResponse("0", clusterLoadAssignments,
            XdsClientImpl.ADS_TYPE_URL_EDS, "0000");
    responseObserver.onNext(response);

    // Client sent an ACK EDS request.
    verify(requestObserver)
        .onNext(eq(buildDiscoveryRequest("0", "cluster-foo.googleapis.com",
            XdsClientImpl.ADS_TYPE_URL_EDS, "0000")));

    ArgumentCaptor<EndpointUpdate> endpointUpdateCaptor1 = ArgumentCaptor.forClass(null);
    verify(watcher1).onEndpointChanged(endpointUpdateCaptor1.capture());
    EndpointUpdate endpointUpdate1 = endpointUpdateCaptor1.getValue();
    assertThat(endpointUpdate1.getClusterName()).isEqualTo("cluster-foo.googleapis.com");
    assertThat(endpointUpdate1.getLocalityLbEndpointsMap())
        .containsExactly(
            new Locality("region1", "zone1", "subzone1"),
            new LocalityLbEndpoints(
                ImmutableList.of(
                    new LbEndpoint("192.168.0.1", 8080, 2, true),
                    new LbEndpoint("192.132.53.5", 80,5, false)),
                1, 0));

    // Add another endpoint watcher for a different cluster.
    EndpointWatcher watcher2 = mock(EndpointWatcher.class);
    xdsClient.watchEndpointData("cluster-bar.googleapis.com", watcher2);

    // Client sent a new EDS request for all interested resources.
    verify(requestObserver)
        .onNext(
            argThat(
                new DiscoveryRequestMatcher("0",
                    ImmutableList.of("cluster-foo.googleapis.com", "cluster-bar.googleapis.com"),
                    XdsClientImpl.ADS_TYPE_URL_EDS, "0000")));

    // Management server sends back an EDS response with ClusterLoadAssignment for one of requested
    // cluster.
    clusterLoadAssignments = ImmutableList.of(
        Any.pack(buildClusterLoadAssignment("cluster-bar.googleapis.com",
            ImmutableList.of(
                buildLocalityLbEndpoints("region2", "zone2", "subzone2",
                    ImmutableList.of(
                        buildLbEndpoint("192.168.312.6", 443, HealthStatus.HEALTHY, 1)),
                    6, 1)),
            ImmutableList.<Policy.DropOverload>of())));

    response = buildDiscoveryResponse("1", clusterLoadAssignments,
        XdsClientImpl.ADS_TYPE_URL_EDS, "0001");
    responseObserver.onNext(response);

    // Client sent an ACK EDS request for all interested resources.
    verify(requestObserver)
        .onNext(
            argThat(
                new DiscoveryRequestMatcher("1",
                    ImmutableList.of("cluster-foo.googleapis.com", "cluster-bar.googleapis.com"),
                    XdsClientImpl.ADS_TYPE_URL_EDS, "0001")));

    ArgumentCaptor<EndpointUpdate> endpointUpdateCaptor2 = ArgumentCaptor.forClass(null);
    verify(watcher2).onEndpointChanged(endpointUpdateCaptor2.capture());
    EndpointUpdate endpointUpdate2 = endpointUpdateCaptor2.getValue();
    assertThat(endpointUpdate2.getClusterName()).isEqualTo("cluster-bar.googleapis.com");
    assertThat(endpointUpdate2.getLocalityLbEndpointsMap())
        .containsExactly(
            new Locality("region2", "zone2", "subzone2"),
            new LocalityLbEndpoints(
                ImmutableList.of(
                    new LbEndpoint("192.168.312.6", 443, 1, true)),
                6, 1));

    // Cancel one of the watcher.
    xdsClient.cancelEndpointDataWatch("cluster-foo.googleapis.com", watcher1);

    // Since the cancelled watcher was the last watcher interested in that cluster, client
    // sent an new EDS request to unsubscribe from that cluster.
    verify(requestObserver)
        .onNext(eq(buildDiscoveryRequest("1", "cluster-bar.googleapis.com",
            XdsClientImpl.ADS_TYPE_URL_EDS, "0001")));

    // Management server should not respond as it had previously sent the requested resource.

    // Cancel the other watcher.
    xdsClient.cancelEndpointDataWatch("cluster-bar.googleapis.com", watcher2);

    // Since the cancelled watcher was the last watcher interested in that cluster, client
    // sent an new EDS request to unsubscribe from that cluster.
    verify(requestObserver)
        .onNext(
            argThat(
                new DiscoveryRequestMatcher("1",
                    ImmutableList.<String>of(),  // empty resources
                    XdsClientImpl.ADS_TYPE_URL_EDS, "0001")));

    // All endpoint watchers have been cancelled.

    // Management server sends back an EDS response for updating previously sent resources.
    clusterLoadAssignments = ImmutableList.of(
        Any.pack(buildClusterLoadAssignment("cluster-foo.googleapis.com",
            ImmutableList.of(
                buildLocalityLbEndpoints("region3", "zone3", "subzone3",
                    ImmutableList.of(
                        buildLbEndpoint("192.168.432.6", 80, HealthStatus.HEALTHY, 2)),
                    3, 0)),
            ImmutableList.<Policy.DropOverload>of())),
        Any.pack(buildClusterLoadAssignment("cluster-bar.googleapis.com",
            ImmutableList.of(
                buildLocalityLbEndpoints("region4", "zone4", "subzone4",
                    ImmutableList.of(
                        buildLbEndpoint("192.168.75.6", 8888, HealthStatus.HEALTHY, 2)),
                    3, 0)),
            ImmutableList.<Policy.DropOverload>of())));

    response = buildDiscoveryResponse("2", clusterLoadAssignments,
        XdsClientImpl.ADS_TYPE_URL_EDS, "0002");
    responseObserver.onNext(response);

    // Client sent an ACK EDS request.
    verify(requestObserver)
        .onNext(
            argThat(
                new DiscoveryRequestMatcher("2",
                    ImmutableList.<String>of(),  // empty resources
                    XdsClientImpl.ADS_TYPE_URL_EDS, "0002")));

    // Cancelled watchers do not receive notification.
    verifyNoMoreInteractions(watcher1, watcher2);

    // A new endpoint watcher is added to watch an old but was no longer interested in cluster.
    EndpointWatcher watcher3 = mock(EndpointWatcher.class);
    xdsClient.watchEndpointData("cluster-bar.googleapis.com", watcher3);

    // Nothing should be notified to the new watcher as we are still waiting management server's
    // latest response.
    // Cached endpoint data should have been purged.
    verify(watcher3, never()).onEndpointChanged(any(EndpointUpdate.class));

    // An EDS request is sent to re-subscribe the cluster again.
    verify(requestObserver)
        .onNext(eq(buildDiscoveryRequest("2", "cluster-bar.googleapis.com",
            XdsClientImpl.ADS_TYPE_URL_EDS, "0002")));

    // Management server sends back an EDS response for re-subscribed resource.
    clusterLoadAssignments = ImmutableList.of(
        Any.pack(buildClusterLoadAssignment("cluster-bar.googleapis.com",
            ImmutableList.of(
                buildLocalityLbEndpoints("region4", "zone4", "subzone4",
                    ImmutableList.of(
                        buildLbEndpoint("192.168.75.6", 8888, HealthStatus.HEALTHY, 2)),
                    3, 0)),
            ImmutableList.<Policy.DropOverload>of())));

    response = buildDiscoveryResponse("3", clusterLoadAssignments,
        XdsClientImpl.ADS_TYPE_URL_EDS, "0003");
    responseObserver.onNext(response);

    ArgumentCaptor<EndpointUpdate> endpointUpdateCaptor3 = ArgumentCaptor.forClass(null);
    verify(watcher3).onEndpointChanged(endpointUpdateCaptor3.capture());
    EndpointUpdate endpointUpdate3 = endpointUpdateCaptor3.getValue();
    assertThat(endpointUpdate3.getClusterName()).isEqualTo("cluster-bar.googleapis.com");
    assertThat(endpointUpdate3.getLocalityLbEndpointsMap())
        .containsExactly(
            new Locality("region4", "zone4", "subzone4"),
            new LocalityLbEndpoints(
                ImmutableList.of(
                    new LbEndpoint("192.168.75.6", 8888, 2, true)),
                3, 0));

    // Client sent an ACK EDS request.
    verify(requestObserver)
        .onNext(
            argThat(
                new DiscoveryRequestMatcher("3",
                    ImmutableList.of("cluster-bar.googleapis.com"),
                    XdsClientImpl.ADS_TYPE_URL_EDS, "0003")));
  }

<<<<<<< HEAD
=======
  // TODO(chengyuanzhang): tests for caching EDS responses proactively sent by management server.

  // TODO(chengyuanzhang): tests for LDS/RDS/CDS/EDS sharing the same RPC stream.

>>>>>>> Add tests for EDS protocol and endpoint watchers.
  // TODO(chengyuanzhang): incorporate interactions with cluster watchers and end endpoint watchers
  //  during retry.
  
  /**
   * RPC stream closed and retry during the period of first tiem resolving service config
   * (LDS/RDS only).
   */
  @Test
  public void streamClosedAndRetryWhenResolvingConfig() {
    InOrder inOrder =
        Mockito.inOrder(mockedDiscoveryService, backoffPolicyProvider, backoffPolicy1,
            backoffPolicy2);
    xdsClient.watchConfigData(HOSTNAME, PORT, configWatcher);

    ArgumentCaptor<StreamObserver<DiscoveryResponse>> responseObserverCaptor =
        ArgumentCaptor.forClass(null);
    inOrder.verify(mockedDiscoveryService)
        .streamAggregatedResources(responseObserverCaptor.capture());
    StreamObserver<DiscoveryResponse> responseObserver =
        responseObserverCaptor.getValue();  // same as responseObservers.poll()
    StreamObserver<DiscoveryRequest> requestObserver = requestObservers.poll();

    // Client sends an LDS request for the host name (with port) to management server.
    verify(requestObserver)
        .onNext(eq(buildDiscoveryRequest("", "foo.googleapis.com:8080",
            XdsClientImpl.ADS_TYPE_URL_LDS, "")));

    // Management server closes the RPC stream immediately.
    responseObserver.onCompleted();
    inOrder.verify(backoffPolicyProvider).get();
    inOrder.verify(backoffPolicy1).nextBackoffNanos();
    assertThat(fakeClock.getPendingTasks(RPC_RETRY_TASK_FILTER)).hasSize(1);

    // Retry after backoff.
    fakeClock.forwardNanos(9L);
    assertThat(requestObservers).isEmpty();
    fakeClock.forwardNanos(1L);
    inOrder.verify(mockedDiscoveryService)
        .streamAggregatedResources(responseObserverCaptor.capture());
    responseObserver = responseObserverCaptor.getValue();
    requestObserver = requestObservers.poll();

    // Client retried by sending an LDS request.
    verify(requestObserver)
        .onNext(eq(buildDiscoveryRequest("", "foo.googleapis.com:8080",
            XdsClientImpl.ADS_TYPE_URL_LDS, "")));

    // Management server closes the RPC stream with an error.
    responseObserver.onError(Status.UNAVAILABLE.asException());
    verifyNoMoreInteractions(backoffPolicyProvider);
    inOrder.verify(backoffPolicy1).nextBackoffNanos();
    assertThat(fakeClock.getPendingTasks(RPC_RETRY_TASK_FILTER)).hasSize(1);

    // Retry after backoff.
    fakeClock.forwardNanos(99L);
    assertThat(requestObservers).isEmpty();
    fakeClock.forwardNanos(1L);
    inOrder.verify(mockedDiscoveryService)
        .streamAggregatedResources(responseObserverCaptor.capture());
    responseObserver = responseObserverCaptor.getValue();
    requestObserver = requestObservers.poll();

    // Client retried again by sending an LDS.
    verify(requestObserver)
        .onNext(eq(buildDiscoveryRequest("", "foo.googleapis.com:8080",
            XdsClientImpl.ADS_TYPE_URL_LDS, "")));

    // Management server responses with a listener for the requested resource.
    Rds rdsConfig =
        Rds.newBuilder()
            .setConfigSource(
                ConfigSource.newBuilder().setAds(AggregatedConfigSource.getDefaultInstance()))
            .setRouteConfigName("route-foo.googleapis.com")
            .build();

    List<Any> listeners = ImmutableList.of(
        Any.pack(buildListener("foo.googleapis.com:8080", /* matching resource */
            Any.pack(HttpConnectionManager.newBuilder().setRds(rdsConfig).build())))
    );
    DiscoveryResponse ldsResponse =
        buildDiscoveryResponse("0", listeners, XdsClientImpl.ADS_TYPE_URL_LDS, "0000");
    responseObserver.onNext(ldsResponse);

    // Client sent back an ACK LDS request.
    verify(requestObserver)
        .onNext(eq(buildDiscoveryRequest("0", "foo.googleapis.com:8080",
            XdsClientImpl.ADS_TYPE_URL_LDS, "0000")));

    // Client sent an RDS request based on the received listener.
    verify(requestObserver)
        .onNext(eq(buildDiscoveryRequest("", "route-foo.googleapis.com",
            XdsClientImpl.ADS_TYPE_URL_RDS, "")));

    // Management server encounters an error and closes the stream.
    responseObserver.onError(Status.UNKNOWN.asException());

    // Reset backoff and retry immediately.
    inOrder.verify(backoffPolicyProvider).get();
    fakeClock.runDueTasks();
    inOrder.verify(mockedDiscoveryService)
        .streamAggregatedResources(responseObserverCaptor.capture());
    responseObserver = responseObserverCaptor.getValue();
    requestObserver = requestObservers.poll();
    verify(requestObserver)
        .onNext(eq(buildDiscoveryRequest("", "foo.googleapis.com:8080",
            XdsClientImpl.ADS_TYPE_URL_LDS, "")));

    // RPC stream closed immediately
    responseObserver.onError(Status.UNKNOWN.asException());
    inOrder.verify(backoffPolicy2).nextBackoffNanos();
    assertThat(fakeClock.getPendingTasks(RPC_RETRY_TASK_FILTER)).hasSize(1);

    // Retry after backoff.
    fakeClock.forwardNanos(19L);
    assertThat(requestObservers).isEmpty();
    fakeClock.forwardNanos(1L);
    inOrder.verify(mockedDiscoveryService)
        .streamAggregatedResources(responseObserverCaptor.capture());
    responseObserver = responseObserverCaptor.getValue();
    requestObserver = requestObservers.poll();
    verify(requestObserver)
        .onNext(eq(buildDiscoveryRequest("", "foo.googleapis.com:8080",
            XdsClientImpl.ADS_TYPE_URL_LDS, "")));

    // Management server sends an LDS response.
    responseObserver.onNext(ldsResponse);

    // Client sends an ACK LDS request and an RDS request for "route-foo.googleapis.com". (Omitted)

    List<Any> routeConfigs = ImmutableList.of(
        Any.pack(
            buildRouteConfiguration(
                "route-foo.googleapis.com",
                ImmutableList.of(
                    buildVirtualHost(ImmutableList.of("foo.googleapis.com"),
                        "cluster.googleapis.com")))));
    DiscoveryResponse rdsResponse =
        buildDiscoveryResponse("0", routeConfigs, XdsClientImpl.ADS_TYPE_URL_RDS, "0000");
    // Management server sends an RDS response.
    responseObserver.onNext(rdsResponse);

    // Client has resolved the cluster based on the RDS response.
    configWatcher
        .onConfigChanged(
            eq(ConfigUpdate.newBuilder().setClusterName("cluster.googleapis.com").build()));

    // RPC stream closed with an error again.
    responseObserver.onError(Status.UNKNOWN.asException());

    // Reset backoff and retry immediately.
    inOrder.verify(backoffPolicyProvider).get();
    fakeClock.runDueTasks();
    requestObserver = requestObservers.poll();
    verify(requestObserver)
        .onNext(eq(buildDiscoveryRequest("", "foo.googleapis.com:8080",
            XdsClientImpl.ADS_TYPE_URL_LDS, "")));

    verifyNoMoreInteractions(backoffPolicyProvider, backoffPolicy1, backoffPolicy2);
  }

  // TODO(chengyuanzhang): test for race between stream closed and watcher changes. Should only
  //  for ClusterWatchers and EndpointWatchers.

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
  public void matchHostName_postfixWildCard() {
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

  private static DiscoveryRequest buildDiscoveryRequest(String versionInfo,
      String resourceName, String typeUrl, String nonce) {
    return buildDiscoveryRequest(versionInfo, ImmutableList.of(resourceName), typeUrl, nonce);
  }

  private static DiscoveryRequest buildDiscoveryRequest(String versionInfo,
      List<String> resourceNames, String typeUrl, String nonce) {
    return
        DiscoveryRequest.newBuilder()
            .setVersionInfo(versionInfo)
            .setNode(NODE)
            .setTypeUrl(typeUrl)
            .addAllResourceNames(resourceNames)
            .setResponseNonce(nonce)
            .build();
  }

  private static Listener buildListener(String name, com.google.protobuf.Any apiListener) {
    return
        Listener.newBuilder()
            .setName(name)
            .setAddress(Address.getDefaultInstance())
            .addFilterChains(FilterChain.getDefaultInstance())
            .setApiListener(ApiListener.newBuilder().setApiListener(apiListener))
            .build();
  }

  private static RouteConfiguration buildRouteConfiguration(String name,
      List<VirtualHost> virtualHosts) {
    return
        RouteConfiguration.newBuilder()
            .setName(name)
            .addAllVirtualHosts(virtualHosts)
            .build();
  }

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

  private static ClusterLoadAssignment buildClusterLoadAssignment(String clusterName,
      List<io.envoyproxy.envoy.api.v2.endpoint.LocalityLbEndpoints> localityLbEndpoints,
      List<Policy.DropOverload> dropOverloads) {
    return
        ClusterLoadAssignment.newBuilder()
            .setClusterName(clusterName)
            .addAllEndpoints(localityLbEndpoints)
            .setPolicy(
                Policy.newBuilder()
                    .setDisableOverprovisioning(true)
                    .addAllDropOverloads(dropOverloads))
            .build();
  }

  private static Policy.DropOverload buildDropOverload(String category, int dropPerMillion) {
    return
        Policy.DropOverload.newBuilder()
            .setCategory(category)
            .setDropPercentage(
                FractionalPercent.newBuilder()
                    .setNumerator(dropPerMillion)
                    .setDenominator(DenominatorType.MILLION))
            .build();
  }

  private static io.envoyproxy.envoy.api.v2.endpoint.LocalityLbEndpoints buildLocalityLbEndpoints(
      String region, String zone, String subzone,
      List<io.envoyproxy.envoy.api.v2.endpoint.LbEndpoint> lbEndpoints,
      int loadBalancingWeight, int priority) {
    return
        io.envoyproxy.envoy.api.v2.endpoint.LocalityLbEndpoints.newBuilder()
            .setLocality(
                io.envoyproxy.envoy.api.v2.core.Locality.newBuilder()
                    .setRegion(region)
                    .setZone(zone)
                    .setSubZone(subzone))
            .addAllLbEndpoints(lbEndpoints)
            .setLoadBalancingWeight(UInt32Value.newBuilder().setValue(loadBalancingWeight))
            .setPriority(priority)
            .build();
  }

  private static io.envoyproxy.envoy.api.v2.endpoint.LbEndpoint buildLbEndpoint(String address,
      int port, HealthStatus healthStatus, int loadbalancingWeight) {
    return
        io.envoyproxy.envoy.api.v2.endpoint.LbEndpoint.newBuilder()
            .setEndpoint(
                io.envoyproxy.envoy.api.v2.endpoint.Endpoint.newBuilder().setAddress(
                    Address.newBuilder().setSocketAddress(
                        SocketAddress.newBuilder().setAddress(address).setPortValue(port))))
            .setHealthStatus(healthStatus).setLoadBalancingWeight(
                UInt32Value.newBuilder().setValue(loadbalancingWeight))
            .build();
  }

  /**
<<<<<<< HEAD
   * Matcher for DiscoveryRequest without the comparison of error_details field, which is used for
   * management server debugging purposes.
   *
   * <p>In general, if you are sure error_details field should not be set in a DiscoveryRequest,
   * compare with message equality. Otherwise, this matcher is handy for comparing other fields
   * only.
=======
   * Matcher for DiscoveryRequest used to verify NACK requests. Eliminates the comparison of
   * error_details for DiscoveryRequests if they are expected to be an NACK request.
>>>>>>> Add tests for EDS protocol and endpoint watchers.
   */
  private static class DiscoveryRequestMatcher implements ArgumentMatcher<DiscoveryRequest> {
    private final String versionInfo;
    private final String typeUrl;
    private final Set<String> resourceNames;
    private final String responseNonce;

    private DiscoveryRequestMatcher(String versionInfo, String resourceName, String typeUrl,
        String responseNonce) {
      this(versionInfo, ImmutableList.of(resourceName), typeUrl, responseNonce);
    }

    private DiscoveryRequestMatcher(String versionInfo, List<String> resourceNames, String typeUrl,
        String responseNonce) {
      this.versionInfo = versionInfo;
      this.resourceNames = new HashSet<>(resourceNames);
      this.typeUrl = typeUrl;
      this.responseNonce = responseNonce;
    }

    @Override
    public boolean matches(DiscoveryRequest argument) {
      if (!typeUrl.equals(argument.getTypeUrl())) {
        return false;
      }
      if (!versionInfo.equals(argument.getVersionInfo())) {
        return false;
      }
      if (!responseNonce.equals(argument.getResponseNonce())) {
        return false;
      }
      if (!resourceNames.equals(new HashSet<>(argument.getResourceNamesList()))) {
        return false;
      }
      return NODE.equals(argument.getNode());
    }
  }
}
