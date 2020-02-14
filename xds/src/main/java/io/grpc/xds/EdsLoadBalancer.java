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

import static com.google.common.base.Preconditions.checkNotNull;
import static io.grpc.ConnectivityState.TRANSIENT_FAILURE;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.envoyproxy.envoy.api.v2.core.Node;
import io.grpc.Attributes;
import io.grpc.InternalLogId;
import io.grpc.LoadBalancer;
import io.grpc.LoadBalancerRegistry;
import io.grpc.Status;
import io.grpc.internal.ExponentialBackoffPolicy;
import io.grpc.internal.GrpcUtil;
import io.grpc.internal.ObjectPool;
import io.grpc.util.GracefulSwitchLoadBalancer;
import io.grpc.xds.Bootstrapper.BootstrapInfo;
import io.grpc.xds.Bootstrapper.ServerInfo;
import io.grpc.xds.EnvoyProtoData.DropOverload;
import io.grpc.xds.EnvoyProtoData.Locality;
import io.grpc.xds.EnvoyProtoData.LocalityLbEndpoints;
import io.grpc.xds.LocalityStore.LocalityStoreFactory;
import io.grpc.xds.XdsClient.EndpointUpdate;
import io.grpc.xds.XdsClient.EndpointWatcher;
import io.grpc.xds.XdsClient.RefCountedXdsClientObjectPool;
import io.grpc.xds.XdsClient.XdsChannelFactory;
import io.grpc.xds.XdsClient.XdsClientFactory;
import io.grpc.xds.XdsLoadBalancerProvider.XdsConfig;
import io.grpc.xds.XdsLogger.XdsLogLevel;
import io.grpc.xds.XdsSubchannelPickers.ErrorPicker;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;

/** Load balancer for the EDS LB policy. */
final class EdsLoadBalancer extends LoadBalancer {
  private final XdsLogger logger;
  private final ResourceUpdateCallback resourceUpdateCallback;
  private final GracefulSwitchLoadBalancer switchingLoadBalancer;
  private final LoadBalancerRegistry lbRegistry;
  private final LocalityStoreFactory localityStoreFactory;
  private final Bootstrapper bootstrapper;
  private final XdsChannelFactory channelFactory;
  private final Helper edsLbHelper;
  // Cache for load stats stores for each service in cluster keyed by cluster service names.
  private final Map<String, LoadStatsStore> loadStatsStoreMap = new HashMap<>();

  // Most recent XdsConfig.
  @Nullable
  private XdsConfig xdsConfig;
  // Most recent EndpointWatcher.
  @Nullable
  private EndpointWatcher endpointWatcher;
  @Nullable
  private ObjectPool<XdsClient> xdsClientPool;
  @Nullable
  private XdsClient xdsClient;
  @Nullable
  private LoadReportClient loadReportClient;
  @Nullable
  private String clusterName;

  EdsLoadBalancer(Helper edsLbHelper, ResourceUpdateCallback resourceUpdateCallback) {
    this(
        checkNotNull(edsLbHelper, "edsLbHelper"),
        checkNotNull(resourceUpdateCallback, "resourceUpdateCallback"),
        LoadBalancerRegistry.getDefaultRegistry(),
        LocalityStoreFactory.getInstance(),
        Bootstrapper.getInstance(),
        XdsChannelFactory.getInstance());
  }

  @VisibleForTesting
  EdsLoadBalancer(
      Helper edsLbHelper,
      ResourceUpdateCallback resourceUpdateCallback,
      LoadBalancerRegistry lbRegistry,
      LocalityStoreFactory localityStoreFactory,
      Bootstrapper bootstrapper,
      XdsChannelFactory channelFactory) {
    logger = XdsLogger.withLogId(InternalLogId.allocate("eds-lb", null));
    this.edsLbHelper = edsLbHelper;
    this.resourceUpdateCallback = resourceUpdateCallback;
    this.lbRegistry = lbRegistry;
    this.localityStoreFactory = localityStoreFactory;
    this.switchingLoadBalancer = new GracefulSwitchLoadBalancer(edsLbHelper);
    this.bootstrapper = bootstrapper;
    this.channelFactory = channelFactory;
    logger.log(XdsLogLevel.INFO, "Created");
  }

  @Override
  public void handleResolvedAddresses(ResolvedAddresses resolvedAddresses) {
    logger.log(XdsLogLevel.DEBUG, "Received ResolvedAddresses {0}", resolvedAddresses);

    Object lbConfig = resolvedAddresses.getLoadBalancingPolicyConfig();
    if (!(lbConfig instanceof XdsConfig)) {
      edsLbHelper.updateBalancingState(
          TRANSIENT_FAILURE,
          new ErrorPicker(Status.UNAVAILABLE.withDescription(
              "Load balancing config '" + lbConfig + "' is not an XdsConfig")));
      return;
    }
    XdsConfig newXdsConfig = (XdsConfig) lbConfig;
    logger.log(XdsLogLevel.INFO,
        "Received policy update, load balancing config: {0}",  newXdsConfig);

    if (xdsClientPool == null) {
      // Init xdsClientPool and xdsClient.
      // There are two usecases:
      // 1. EDS-only:
      //    The name resolver resolves a ResolvedAddresses with an XdsConfig. Use the bootstrap
      //    information to create a channel.
      // 2. Non EDS-only:
      //    XDS_CLIENT_POOL attribute is available from ResolvedAddresses either from
      //    XdsNameResolver or CDS policy.
      //
      // We assume XdsConfig switching happens only within one usecase, and there is no switching
      // between different usecases.

      boolean useXdsClientFromChannel = false;
      Attributes attributes = resolvedAddresses.getAttributes();
      xdsClientPool = attributes.get(XdsAttributes.XDS_CLIENT_POOL);
      if (xdsClientPool == null) { // This is the EDS-only usecase.
        final BootstrapInfo bootstrapInfo;
        try {
          bootstrapInfo = bootstrapper.readBootstrap();
        } catch (Exception e) {
          edsLbHelper.updateBalancingState(
              TRANSIENT_FAILURE,
              new ErrorPicker(
                  Status.UNAVAILABLE.withDescription("Failed to bootstrap").withCause(e)));
          return;
        }

        final List<ServerInfo> serverList = bootstrapInfo.getServers();
        final Node node = bootstrapInfo.getNode();
        if (serverList.isEmpty()) {
          edsLbHelper.updateBalancingState(
              TRANSIENT_FAILURE,
              new ErrorPicker(
                  Status.UNAVAILABLE
                      .withDescription("No traffic director provided by bootstrap")));
          return;
        }
        XdsClientFactory xdsClientFactory = new XdsClientFactory() {
          @Override
          XdsClient createXdsClient() {
            return
                new XdsClientImpl(
                    serverList,
                    channelFactory,
                    node,
                    edsLbHelper.getSynchronizationContext(),
                    edsLbHelper.getScheduledExecutorService(),
                    new ExponentialBackoffPolicy.Provider(),
                    GrpcUtil.STOPWATCH_SUPPLIER);
          }
        };
        xdsClientPool = new RefCountedXdsClientObjectPool(xdsClientFactory);
      } else {
        useXdsClientFromChannel = true;
      }
      xdsClient = xdsClientPool.getObject();
      if (useXdsClientFromChannel) {
        logger.log(XdsLogLevel.INFO, "Using xDS client {0} from channel", xdsClient);
      }
    }

    // The edsServiceName field is null in legacy gRPC client with EDS: use target authority for
    // querying endpoints, but in the future we expect this to be explicitly given by EDS config.
    // We assume if edsServiceName is null, it will always be null in later resolver updates;
    // and if edsServiceName is not null, it will always be not null.
    String clusterServiceName = newXdsConfig.edsServiceName;
    if (clusterServiceName == null) {
      clusterServiceName = edsLbHelper.getAuthority();
    }
    if (clusterName == null) {
      // TODO(zdapeng): Use the correct cluster name. Currently load reporting will be broken if
      //     edsServiceName is changed because we are using edsServiceName for the cluster name.
      clusterName = clusterServiceName;
    }

    boolean shouldReportStats = newXdsConfig.lrsServerName != null;
    if (shouldReportStats && !isReportingStats()) {
      // Start load reporting. This may be a restarting after previously stopping the load
      // reporting, so need to re-add all the pre-existing loadStatsStores to the new
      // loadReportClient.
      loadReportClient = xdsClient.reportClientStats(clusterName, newXdsConfig.lrsServerName);
      for (Map.Entry<String, LoadStatsStore> entry : loadStatsStoreMap.entrySet()) {
        loadReportClient.addLoadStatsStore(entry.getKey(), entry.getValue());
      }
    }
    if (!shouldReportStats && isReportingStats()) {
      cancelClientStatsReport();
    }

    // Note: childPolicy change will be handled in LocalityStore, to be implemented.
    // If edsServiceName in XdsConfig is changed, do a graceful switch.
    if (xdsConfig == null
        || !Objects.equals(newXdsConfig.edsServiceName, xdsConfig.edsServiceName)) {
      LoadBalancer.Factory clusterEndpointsLoadBalancerFactory =
          new ClusterEndpointsBalancerFactory(clusterServiceName);
      switchingLoadBalancer.switchTo(clusterEndpointsLoadBalancerFactory);
    }
    resolvedAddresses = resolvedAddresses.toBuilder()
        .setLoadBalancingPolicyConfig(newXdsConfig)
        .build();
    switchingLoadBalancer.handleResolvedAddresses(resolvedAddresses);

    this.xdsConfig = newXdsConfig;
  }

  @Override
  public void handleNameResolutionError(Status error) {
    logger.log(XdsLogLevel.WARNING, "Received policy update error: {0}", error);
    // This will go into TRANSIENT_FAILURE if we have not yet received any endpoint update and
    // otherwise keep running with the data we had previously.
    switchingLoadBalancer.handleNameResolutionError(error);
  }

  @Override
  public boolean canHandleEmptyAddressListFromNameResolution() {
    return true;
  }

  @Override
  public void shutdown() {
    logger.log(XdsLogLevel.INFO, "Shutdown");
    switchingLoadBalancer.shutdown();
    if (isReportingStats()) {
      cancelClientStatsReport();
    }
    if (xdsClient != null) {
      xdsClient = xdsClientPool.returnObject(xdsClient);
    }
  }

  /** Whether the client stats for the cluster is currently reported to the traffic director. */
  private boolean isReportingStats() {
    return loadReportClient != null;
  }

  /** Stops to report client stats for the cluster. */
  private void cancelClientStatsReport() {
    xdsClient.cancelClientStatsReport(clusterName);
    loadReportClient = null;
  }

  /**
   * A load balancer factory that provides a load balancer for a given cluster service.
   */
  private final class ClusterEndpointsBalancerFactory extends LoadBalancer.Factory {
    final String clusterServiceName;
    @Nullable
    final String oldClusterServiceName;

    ClusterEndpointsBalancerFactory(String clusterServiceName) {
      this.clusterServiceName = clusterServiceName;
      if (xdsConfig != null) {
        oldClusterServiceName = xdsConfig.edsServiceName;
      } else {
        oldClusterServiceName = null;
      }
    }

    @Override
    public LoadBalancer newLoadBalancer(Helper helper) {
      return new ClusterEndpointsBalancer(helper);
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof ClusterEndpointsBalancerFactory)) {
        return false;
      }
      ClusterEndpointsBalancerFactory that = (ClusterEndpointsBalancerFactory) o;
      return clusterServiceName.equals(that.clusterServiceName);
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), clusterServiceName);
    }

    /**
     * Load-balances endpoints for a given cluster.
     */
    final class ClusterEndpointsBalancer extends LoadBalancer {
      final Helper helper;
      final EndpointWatcherImpl endpointWatcher;
      final LocalityStore localityStore;

      ClusterEndpointsBalancer(Helper helper) {
        this.helper = helper;

        LoadStatsStore loadStatsStore = new LoadStatsStoreImpl();
        loadStatsStoreMap.put(clusterServiceName, loadStatsStore);
        if (isReportingStats()) {
          loadReportClient.addLoadStatsStore(clusterServiceName, loadStatsStore);
        }
        localityStore = localityStoreFactory.newLocalityStore(helper, lbRegistry, loadStatsStore);

        endpointWatcher = new EndpointWatcherImpl(localityStore);
        logger.log(
            XdsLogLevel.INFO,
            "Start endpoints watcher on {0} with xDS client {1}", clusterServiceName, xdsClient);
        xdsClient.watchEndpointData(clusterServiceName, endpointWatcher);
        if (EdsLoadBalancer.this.endpointWatcher != null) {
          xdsClient.cancelEndpointDataWatch(
              oldClusterServiceName, EdsLoadBalancer.this.endpointWatcher);
          logger.log(
              XdsLogLevel.INFO,
              "Cancelled endpoints watcher on {0} with xDS client {1}",
              clusterServiceName, xdsClient);
        }
        EdsLoadBalancer.this.endpointWatcher = endpointWatcher;
      }

      // TODO(zddapeng): In handleResolvedAddresses() handle child policy change if any.

      @Override
      public void handleNameResolutionError(Status error) {
        // Go into TRANSIENT_FAILURE if we have not yet received any endpoint update. Otherwise,
        // we keep running with the data we had previously.
        if (!endpointWatcher.firstEndpointUpdateReceived) {
          helper.updateBalancingState(TRANSIENT_FAILURE, new ErrorPicker(error));
        }
      }

      @Override
      public boolean canHandleEmptyAddressListFromNameResolution() {
        return true;
      }

      @Override
      public void shutdown() {
        loadStatsStoreMap.remove(clusterServiceName);
        if (isReportingStats()) {
          loadReportClient.removeLoadStatsStore(clusterServiceName);
        }
        localityStore.reset();
        xdsClient.cancelEndpointDataWatch(clusterServiceName, endpointWatcher);
        logger.log(
            XdsLogLevel.INFO,
            "Cancelled endpoints watcher on {0} with xDS client {1}",
            clusterServiceName, xdsClient);
      }
    }
  }

  /**
   * Callbacks for the EDS-only-with-fallback usecase. Being deprecated.
   */
  interface ResourceUpdateCallback {

    void onWorking();

    void onError();

    void onAllDrop();
  }

  private final class EndpointWatcherImpl implements EndpointWatcher {

    final LocalityStore localityStore;
    boolean firstEndpointUpdateReceived;

    EndpointWatcherImpl(LocalityStore localityStore) {
      this.localityStore = localityStore;
    }

    @Override
    public void onEndpointChanged(EndpointUpdate endpointUpdate) {
      logger.log(
          XdsLogLevel.INFO,
          "Received endpoint update from xDS client {0}: {1} localities, {2} drop categories",
          xdsClient,
          endpointUpdate.getLocalityLbEndpointsMap().size(),
          endpointUpdate.getDropPolicies().size());
      logger.log(
          XdsLogLevel.DEBUG,
          "Received endpoint update from xDS client {0}: {1}", xdsClient, endpointUpdate);

      if (!firstEndpointUpdateReceived) {
        firstEndpointUpdateReceived = true;
        resourceUpdateCallback.onWorking();
      }

      List<DropOverload> dropOverloads = endpointUpdate.getDropPolicies();
      ImmutableList.Builder<DropOverload> dropOverloadsBuilder = ImmutableList.builder();
      for (DropOverload dropOverload : dropOverloads) {
        dropOverloadsBuilder.add(dropOverload);
        if (dropOverload.getDropsPerMillion() == 1_000_000) {
          resourceUpdateCallback.onAllDrop();
          break;
        }
      }
      localityStore.updateDropPercentage(dropOverloadsBuilder.build());

      ImmutableMap.Builder<Locality, LocalityLbEndpoints> localityEndpointsMapping =
          new ImmutableMap.Builder<>();
      for (Map.Entry<Locality, LocalityLbEndpoints> entry
          : endpointUpdate.getLocalityLbEndpointsMap().entrySet()) {
        int localityWeight = entry.getValue().getLocalityWeight();

        if (localityWeight != 0) {
          localityEndpointsMapping.put(entry.getKey(), entry.getValue());
        }
      }

      localityStore.updateLocalityStore(localityEndpointsMapping.build());
    }

    @Override
    public void onError(Status error) {
      logger.log(
          XdsLogLevel.WARNING, "Received error from xDS client {0}: {1}", xdsClient, error);
      resourceUpdateCallback.onError();
      // If we get an error before getting any valid result, we should put the channel in
      // TRANSIENT_FAILURE; if they get an error after getting a valid result, we keep using the
      // previous channel state.
      if (!firstEndpointUpdateReceived) {
        edsLbHelper.updateBalancingState(TRANSIENT_FAILURE, new ErrorPicker(error));
      }
    }
  }
}
