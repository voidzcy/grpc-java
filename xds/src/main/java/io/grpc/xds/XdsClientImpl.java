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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.rpc.Code;
import io.envoyproxy.envoy.api.v2.Cluster;
import io.envoyproxy.envoy.api.v2.Cluster.DiscoveryType;
import io.envoyproxy.envoy.api.v2.Cluster.EdsClusterConfig;
import io.envoyproxy.envoy.api.v2.Cluster.LbPolicy;
import io.envoyproxy.envoy.api.v2.ClusterLoadAssignment;
import io.envoyproxy.envoy.api.v2.DiscoveryRequest;
import io.envoyproxy.envoy.api.v2.DiscoveryResponse;
import io.envoyproxy.envoy.api.v2.Listener;
import io.envoyproxy.envoy.api.v2.RouteConfiguration;
import io.envoyproxy.envoy.api.v2.core.Node;
import io.envoyproxy.envoy.api.v2.route.Route;
import io.envoyproxy.envoy.api.v2.route.VirtualHost;
import io.envoyproxy.envoy.config.filter.network.http_connection_manager.v2.HttpConnectionManager;
import io.envoyproxy.envoy.config.filter.network.http_connection_manager.v2.Rds;
import io.envoyproxy.envoy.service.discovery.v2.AggregatedDiscoveryServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.SynchronizationContext;
import io.grpc.SynchronizationContext.ScheduledHandle;
import io.grpc.alts.GoogleDefaultChannelBuilder;
import io.grpc.internal.BackoffPolicy;
import io.grpc.stub.StreamObserver;
import io.grpc.xds.Bootstrapper.ChannelCreds;
import io.grpc.xds.EnvoyProtoData.DropOverload;
import io.grpc.xds.EnvoyProtoData.Locality;
import io.grpc.xds.EnvoyProtoData.LocalityLbEndpoints;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;

final class XdsClientImpl extends XdsClient {
  private static final Logger logger = Logger.getLogger(XdsClientImpl.class.getName());

  @VisibleForTesting
  static final String ADS_TYPE_URL_LDS = "type.googleapis.com/envoy.api.v2.Listener";
  @VisibleForTesting
  static final String ADS_TYPE_URL_RDS =
      "type.googleapis.com/envoy.api.v2.RouteConfiguration";
  @VisibleForTesting
  static final String ADS_TYPE_URL_CDS = "type.googleapis.com/envoy.api.v2.Cluster";
  @VisibleForTesting
  static final String ADS_TYPE_URL_EDS =
      "type.googleapis.com/envoy.api.v2.ClusterLoadAssignment";

  private final String serverUri;  // may need it to configure LRS server.
  private final ManagedChannel channel;
  private final SynchronizationContext syncContext;
  private final ScheduledExecutorService timeService;
  private final BackoffPolicy.Provider backoffPolicyProvider;
  private final Stopwatch stopwatch;
  // The node identifier to be included in xDS requests. Management server only requires the
  // first request to carry the node identifier on a stream. It should be identical if present
  // more than once.
  private final Node node;

  // Cached data for RDS responses, keyed by RouteConfiguration names.
  // LDS responses indicate absence of RouteConfigurations and RDS responses indicate presence
  // of RouteConfigurations.
  // Optimization: only cache clusterName field in the RouteConfiguration messages of RDS
  // responses.
  private final Map<String, String> routeConfigNamesToClusterNames = new HashMap<>();

  // Cached data for CDS responses, keyed by cluster names.
  // Optimization: cache ClusterUpdate, which contains only information needed by gRPC, instead
  // of whole Cluster messages to reduce memory usage.
  private final Map<String, ClusterUpdate> clusterNamesToClusterUpdates = new HashMap<>();

  // Cached data for EDS responses, keyed by cluster names.
  // CDS responses indicate absence of clusters and RDS responses indicate presence of clusters.
  // Optimization: cache EndpointUpdate, which contains only information needed by gRPC, instead
  // of whole ClusterLoadAssignment messages to reduce memory usage.
  private final Map<String, EndpointUpdate> clusterNamesToEndpointUpdates = new HashMap<>();

  // Cluster watchers waiting for cluster information updates. Multiple cluster watchers
  // can watch on information for the same cluster.
  private final Map<String, Set<ClusterWatcher>> clusterWatchers = new HashMap<>();

  // Endpoint watchers waiting for endpoint updates for each cluster. Multiple endpoint
  // watchers can watch endpoints in the same cluster.
  private final Map<String, Set<EndpointWatcher>> endpointWatchers = new HashMap<>();

  @Nullable
  private AdsStream adsStream;
  @Nullable
  private BackoffPolicy retryBackoffPolicy;
  @Nullable
  private ScheduledHandle rpcRetryTimer;

  // Following fields are set only after the ConfigWatcher registered. Once set, they should
  // never change.
  @Nullable
  private ConfigWatcher configWatcher;
  // The host name portion of "xds:" URI that the gRPC client targets for.
  @Nullable
  private String hostName;
  // The "xds:" URI (including port suffix if present) that the gRPC client targets for.
  @Nullable
  private String ldsResourceName;

  XdsClientImpl(
      // URI of the management server to be connected to.
      String serverUri,
      Node node,
      // List of channel credential configurations for the channel to management server.
      // Should pick the first supported one.
      List<ChannelCreds> channelCredsList,
      SynchronizationContext syncContext,
      ScheduledExecutorService timeService,
      BackoffPolicy.Provider backoffPolicyProvider,
      Stopwatch stopwatch) {
    this(
        serverUri,
        buildChannel(checkNotNull(serverUri, "serverUri"),
            checkNotNull(channelCredsList, "channelCredsList")),
        node,
        syncContext,
        timeService,
        backoffPolicyProvider,
        stopwatch);
  }

  @VisibleForTesting
  XdsClientImpl(
      String serverUri,
      ManagedChannel channel,
      Node node,
      SynchronizationContext syncContext,
      ScheduledExecutorService timeService,
      BackoffPolicy.Provider backoffPolicyProvider,
      Stopwatch stopwatch) {
    this.serverUri = checkNotNull(serverUri, "serverUri");
    this.channel = checkNotNull(channel, "channel");
    this.node = checkNotNull(node, "node");
    this.syncContext = checkNotNull(syncContext, "syncContext");
    this.timeService = checkNotNull(timeService, "timeService");
    this.backoffPolicyProvider = checkNotNull(backoffPolicyProvider, "backoffPolicyProvider");
    this.stopwatch = checkNotNull(stopwatch, "stopwatch");
  }

  @Override
  void shutdown() {
    channel.shutdown();
    if (adsStream != null) {
      adsStream.close(Status.CANCELLED.withDescription("shutdown").asException());
    }
    if (rpcRetryTimer != null) {
      rpcRetryTimer.cancel();
    }
  }

  @Override
  void watchConfigData(String hostName, int port, ConfigWatcher watcher) {
    checkState(configWatcher == null, "ConfigWatcher is already registered");
    configWatcher = checkNotNull(watcher, "watcher");
    this.hostName = checkNotNull(hostName, "hostName");
    if (port == -1) {
      ldsResourceName = hostName;
    } else {
      ldsResourceName = hostName + ":" + port;
    }
    if (rpcRetryTimer != null) {
      // Currently in retry backoff.
      return;
    }
    if (adsStream == null) {
      startRpcStream();
    }
    adsStream.sendXdsRequest(ADS_TYPE_URL_LDS, ImmutableList.of(ldsResourceName));
  }

  @Override
  void watchClusterData(String clusterName, ClusterWatcher watcher) {
    boolean needRequest = false;
    if (!clusterWatchers.containsKey(clusterName)) {
      needRequest = true;
      clusterWatchers.put(clusterName, new HashSet<ClusterWatcher>());
    }
    Set<ClusterWatcher> watchers = clusterWatchers.get(clusterName);
    if (watchers.contains(watcher)) {
      logger.log(Level.WARNING, "Watcher {0} already registered", watcher);
    }
    watchers.add(watcher);
    // If local cache contains cluster information to be watched, notify the watcher immediately.
    if (clusterNamesToClusterUpdates.containsKey(clusterName)) {
      watcher.onClusterChanged(clusterNamesToClusterUpdates.get(clusterName));
    }
    if (rpcRetryTimer != null) {
      // Currently in retry backoff.
      return;
    }
    if (needRequest) {
      if (adsStream == null) {
        startRpcStream();
      }
      adsStream.sendXdsRequest(ADS_TYPE_URL_CDS, clusterWatchers.keySet());
    }
  }

  @Override
  void cancelClusterDataWatch(String clusterName, ClusterWatcher watcher) {
    Set<ClusterWatcher> watchers = clusterWatchers.get(clusterName);
    if (watchers == null) {
      logger.log(Level.WARNING, "Watcher {0} was not registered", watcher);
      return;
    }
    watchers.remove(watcher);
    if (watchers.isEmpty()) {
      clusterWatchers.remove(clusterName);
      // If unsubscribe the last resource, do NOT send a CDS request for an empty resource list.
      // This is a workaround for CDS protocol resource unsubscribe.
      if (clusterWatchers.isEmpty()) {
        return;
      }
      // No longer interested in this cluster, send an updated CDS request to unsubscribe
      // this resource.
      if (rpcRetryTimer != null) {
        // Currently in retry backoff.
        return;
      }
      checkState(adsStream != null,
          "Severe bug: ADS stream was not created while an endpoint watcher was registered");
      adsStream.sendXdsRequest(ADS_TYPE_URL_EDS, endpointWatchers.keySet());
    }
  }

  @Override
  void watchEndpointData(String clusterName, EndpointWatcher watcher) {
    boolean needRequest = false;
    if (!endpointWatchers.containsKey(clusterName)) {
      logger.log(Level.FINE, "Start watching endpoints in cluster {0}", clusterName);
      needRequest = true;
      endpointWatchers.put(clusterName, new HashSet<EndpointWatcher>());
    }
    Set<EndpointWatcher> watchers = endpointWatchers.get(clusterName);
    if (watchers.contains(watcher)) {
      logger.log(Level.WARNING, "Watcher {0} already registered", watcher);
    }
    watchers.add(watcher);
    // If local cache contains endpoint information for the cluster to be watched, notify
    // the watcher immediately.
    if (clusterNamesToEndpointUpdates.containsKey(clusterName)) {
      watcher.onEndpointChanged(clusterNamesToEndpointUpdates.get(clusterName));
    }
    if (rpcRetryTimer != null) {
      // Currently in retry backoff.
      return;
    }
    if (needRequest) {
      if (adsStream == null) {
        startRpcStream();
      }
      adsStream.sendXdsRequest(ADS_TYPE_URL_EDS, endpointWatchers.keySet());
    }
  }

  @Override
  void cancelEndpointDataWatch(String clusterName, EndpointWatcher watcher) {
    Set<EndpointWatcher> watchers = endpointWatchers.get(clusterName);
    if (watchers == null) {
      logger.log(Level.WARNING, "Watcher {0} was not registered", watcher);
      return;
    }
    watchers.remove(watcher);
    if (watchers.isEmpty()) {
      logger.log(Level.FINE, "Stop watching endpoints in cluster {0}", clusterName);
      endpointWatchers.remove(clusterName);
      // Remove the corresponding EDS cache entry.
      clusterNamesToEndpointUpdates.remove(clusterName);
      // No longer interested in this cluster, send an updated EDS request to unsubscribe
      // this resource.
      if (rpcRetryTimer != null) {
        // Currently in retry backoff.
        return;
      }
      adsStream.sendXdsRequest(ADS_TYPE_URL_EDS, endpointWatchers.keySet());
    }
  }

  /**
   * Builds a channel to the given server URI with the first supported channel creds config.
   */
  private static ManagedChannel buildChannel(String serverUri,List<ChannelCreds> channelCredsList) {
    ManagedChannel ch = null;
    // Use the first supported channel credentials configuration.
    // Currently, only "google_default" is supported.
    for (ChannelCreds creds : channelCredsList) {
      if (creds.getType().equals("google_default")) {
        ch = GoogleDefaultChannelBuilder.forTarget(serverUri).build();
        break;
      }
    }
    if (ch == null) {
      ch = ManagedChannelBuilder.forTarget(serverUri).build();
    }
    return ch;
  }

  /**
   * Establishes the RPC connection by creating a new RPC stream on the given channel for
   * xDS protocol communication.
   */
  private void startRpcStream() {
    checkState(adsStream == null, "Previous adsStream has not been cleared yet");
    AggregatedDiscoveryServiceGrpc.AggregatedDiscoveryServiceStub stub =
        AggregatedDiscoveryServiceGrpc.newStub(channel);
    adsStream = new AdsStream(stub);
    adsStream.start();
    stopwatch.reset().start();
  }

  /**
   * Handles LDS response to find the HttpConnectionManager message for the requested resource name.
   * Proceed with the resolved RouteConfiguration in HttpConnectionManager message of the requested
   * listener, if exists, to find the VirtualHost configuration for the "xds:" URI
   * (with the port, if any, stripped off). Or sends an RDS request if configured for dynamic
   * resolution. The response is NACKed if contains invalid data for gRPC's usage. Otherwise, an
   * ACK request is sent to management server.
   */
  private void handleLdsResponse(DiscoveryResponse ldsResponse) {
    logger.log(Level.FINE, "Received an LDS response: {0}", ldsResponse);
    checkState(ldsResourceName != null && configWatcher != null,
        "No LDS request was ever sent. Management server is doing something wrong");
    adsStream.ldsRespNonce = ldsResponse.getNonce();

    // Unpack Listener messages.
    List<Listener> listeners = new ArrayList<>(ldsResponse.getResourcesCount());
    try {
      for (com.google.protobuf.Any res : ldsResponse.getResourcesList()) {
        listeners.add(res.unpack(Listener.class));
      }
    } catch (InvalidProtocolBufferException e) {
      adsStream.sendNackRequest(ADS_TYPE_URL_LDS, ImmutableList.of(ldsResourceName),
          ldsResponse.getNonce(), "Broken LDS response.");
      return;
    }

    // Unpack HttpConnectionManager messages
    HttpConnectionManager requestedHttpConnManager = null;
    List<HttpConnectionManager> httpConnectionManagers = new ArrayList<>();
    try {
      for (Listener listener : listeners) {
        HttpConnectionManager hm =
            listener.getApiListener().getApiListener().unpack(HttpConnectionManager.class);
        httpConnectionManagers.add(hm);
        if (listener.getName().equals(ldsResourceName)) {
          requestedHttpConnManager = hm;
        }
      }
    } catch (InvalidProtocolBufferException e) {
      adsStream.sendNackRequest(ADS_TYPE_URL_LDS, ImmutableList.of(ldsResourceName),
          ldsResponse.getNonce(), "Broken LDS response.");
      return;
    }

    String errorMessage = null;
    // All RouteConfigurations referenced by this LDS response, either in in-lined
    // RouteConfiguration message or in RDS config.
    Set<String> routeConfigs = new HashSet<>();
    for (HttpConnectionManager hm : httpConnectionManagers) {
      // The HttpConnectionManager message must either provide the RouteConfiguration directly
      // in-line or tell the client to use RDS to obtain it.
      if (hm.hasRouteConfig()) {
        routeConfigs.add(hm.getRouteConfig().getName());
      } else if (hm.hasRds()) {
        Rds rds = hm.getRds();
        if (!rds.getConfigSource().hasAds()) {
          errorMessage = "For using RDS, it must be set to use ADS.";
          break;
        }
        routeConfigs.add(rds.getRouteConfigName());
      } else {
        errorMessage = "HttpConnectionManager message must either provide the "
            + "RouteConfiguration directly in-line or tell the client to use RDS to obtain it.";
        break;
      }
    }

    // Field clusterName found in the in-lined RouteConfiguration, if exists.
    String clusterName = null;
    // RouteConfiguration name to be used as the resource name for RDS request.
    String rdsRouteConfigName = null;
    if (errorMessage == null && requestedHttpConnManager != null) {
      if (requestedHttpConnManager.hasRouteConfig()) {
        RouteConfiguration rc = requestedHttpConnManager.getRouteConfig();
        clusterName = processRouteConfig(rc);
        if (clusterName == null) {
          errorMessage = "Cannot find a valid cluster name in VirtualHost inside "
              + "RouteConfiguration with domains matching: " + hostName + ".";
        }
      } else if (requestedHttpConnManager.hasRds()) {
        Rds rds = requestedHttpConnManager.getRds();
        rdsRouteConfigName = rds.getRouteConfigName();
      }
      // Else impossible as we have already validated all HttpConnectionManager messages.
    }

    if (errorMessage != null) {
      adsStream.sendNackRequest(ADS_TYPE_URL_LDS, ImmutableList.of(ldsResourceName),
          ldsResponse.getNonce(), errorMessage);
      return;
    }
    adsStream.sendAckRequest(ADS_TYPE_URL_LDS, ImmutableList.of(ldsResourceName),
        ldsResponse.getVersionInfo(), ldsResponse.getNonce());

    // Remove RDS cache entries for RouteConfigurations not referenced by this LDS response.
    // LDS responses represents the state of the world, RouteConfigurations not referenced
    // by this LDS response are those no longer exist.
    routeConfigNamesToClusterNames.keySet().retainAll(routeConfigs);

    // Process the requested Listener if exists, either extract cluster information from in-lined
    // RouteConfiguration message or send an RDS request for dynamic resolution.
    if (clusterName != null) {
      // Found clusterName in the in-lined RouteConfiguration.
      ConfigUpdate configUpdate = ConfigUpdate.newBuilder().setClusterName(clusterName).build();
      configWatcher.onConfigChanged(configUpdate);
    } else if (rdsRouteConfigName != null) {
      // Update the RDS resource we wish to request. An RDS request may not be necessary, but
      // we need to keep what we request updated in case of notifying watcher upon receiving
      // an RDS response for updating the requested resource.
      adsStream.rdsResourceName = rdsRouteConfigName;
      // First look up the RDS cache to see if we had received an RDS response containing the
      // desired RouteConfiguration previously. Otherwise, send an RDS request for dynamic
      // resolution.
      if (routeConfigNamesToClusterNames.containsKey(rdsRouteConfigName)) {
        ConfigUpdate configUpdate =
            ConfigUpdate.newBuilder()
                .setClusterName(routeConfigNamesToClusterNames.get(rdsRouteConfigName))
                .build();
        configWatcher.onConfigChanged(configUpdate);
      } else {
        adsStream.sendXdsRequest(ADS_TYPE_URL_RDS, ImmutableList.of(rdsRouteConfigName));
      }
    } else {
      // The requested Listener does not exist.
      configWatcher.onError(
          Status.NOT_FOUND.withDescription(
              "Listener for requested resource [" + ldsResourceName + "] does not exist"));
    }
  }

  /**
   * Handles RDS response to find the RouteConfiguration message for the requested resource name.
   * Proceed with the resolved RouteConfiguration if exists to find the VirtualHost configuration
   * for the "xds:" URI (with the port, if any, stripped off). The response is NACKed if contains
   * invalid data for gRPC's usage. Otherwise, an ACK request is sent to management server.
   */
  private void handleRdsResponse(DiscoveryResponse rdsResponse) {
    logger.log(Level.FINE, "Received an RDS response: {0}", rdsResponse);
    checkState(adsStream.rdsResourceName != null,
        "Never requested for RDS resources, management server is doing something wrong");
    adsStream.rdsRespNonce = rdsResponse.getNonce();

    // Unpack RouteConfiguration messages.
    List<RouteConfiguration> routeConfigs = new ArrayList<>(rdsResponse.getResourcesCount());
    try {
      for (com.google.protobuf.Any res : rdsResponse.getResourcesList()) {
        routeConfigs.add(res.unpack(RouteConfiguration.class));
      }
    } catch (InvalidProtocolBufferException e) {
      adsStream.sendNackRequest(ADS_TYPE_URL_RDS, ImmutableList.of(adsStream.rdsResourceName),
          rdsResponse.getNonce(), "Broken RDS response.");
      return;
    }

    // Validate and cache information from each RouteConfiguration message.
    Map<String, String> clusterNames = new HashMap<>();
    for (RouteConfiguration routeConfig : routeConfigs) {
      String clusterName = processRouteConfig(routeConfig);
      if (clusterName == null) {
        adsStream.sendNackRequest(ADS_TYPE_URL_RDS, ImmutableList.of(adsStream.rdsResourceName),
            rdsResponse.getNonce(),
            "Cannot find a valid cluster name in VirtualHost inside "
                + "RouteConfiguration with domains matching: " + hostName + ".");
        return;
      }
      clusterNames.put(routeConfig.getName(), clusterName);
    }
    routeConfigNamesToClusterNames.putAll(clusterNames);

    adsStream.sendAckRequest(ADS_TYPE_URL_RDS, ImmutableList.of(adsStream.rdsResourceName),
        rdsResponse.getVersionInfo(), rdsResponse.getNonce());

    // Notify the ConfigWatcher if this RDS response contains the most recently requested
    // RDS resource.
    if (clusterNames.containsKey(adsStream.rdsResourceName)) {
      ConfigUpdate configUpdate =
          ConfigUpdate.newBuilder()
              .setClusterName(clusterNames.get(adsStream.rdsResourceName))
              .build();
      configWatcher.onConfigChanged(configUpdate);
    }
    // Do not notify an error to the ConfigWatcher. RDS protocol is incremental, not receiving
    // requested RouteConfiguration in this response does not imply absence.
  }

  /**
   * Handles CDS response, which contains a list of Cluster messages. Each
   * Cluster message contains information for a logical cluster. The response is NACKed if
   * contains invalid information for gRPC's usage. Otherwise, an ACK request is sent to
   * management server. Cluster messages are saved in local cache, in case of corresponding
   * cluster watchers are registered later. Each CDS response represents the state of the world,
   * a newer response always supersede previous ones. Cluster watchers interested in cluster
   * information contained in this response are notified.
   */
  private void handleCdsResponse(DiscoveryResponse cdsResponse) {
    logger.log(Level.FINE, "Received an CDS response: {0}", cdsResponse);
    checkState(adsStream.cdsResourceNames != null,
        "Never requested for CDS resources, management server is doing something wrong");
    adsStream.cdsRespNonce = cdsResponse.getNonce();

    // Unpack Cluster messages.
    List<Cluster> clusters = new ArrayList<>(cdsResponse.getResourcesCount());
    try {
      for (com.google.protobuf.Any res : cdsResponse.getResourcesList()) {
        clusters.add(res.unpack(Cluster.class));
      }
    } catch (InvalidProtocolBufferException e) {
      adsStream.sendNackRequest(ADS_TYPE_URL_CDS, adsStream.cdsResourceNames,
          cdsResponse.getNonce(), "Broken CDS response");
      for (Set<ClusterWatcher> watchers : clusterWatchers.values()) {
        for (ClusterWatcher watcher : watchers) {
          watcher.onError(Status.fromThrowable(e).augmentDescription("Broken CDS response"));
        }
      }
      return;
    }

    boolean invalidData = false;
    // Full cluster information update received in this CDS response to be cached.
    Map<String, ClusterUpdate> clusterUpdates = new HashMap<>();
    // ClusterUpdate data to be pushed to each watcher.
    Map<String, ClusterUpdate> desiredClusterUpdates = new HashMap<>();
    for (Cluster cluster : clusters) {
      String clusterName = cluster.getName();
      ClusterUpdate.Builder updateBuilder = ClusterUpdate.newBuilder();
      updateBuilder.setClusterName(clusterName);
      // The type field must be set to EDS.
      if (!cluster.getType().equals(DiscoveryType.EDS)) {
        invalidData = true;
        break;
      }
      // In the eds_cluster_config field, the eds_config field must be set to indicate to
      // use EDS (must be set to use ADS).
      EdsClusterConfig edsClusterConfig = cluster.getEdsClusterConfig();
      if (!edsClusterConfig.hasEdsConfig() || !edsClusterConfig.getEdsConfig().hasAds()) {
        invalidData = true;
        break;
      }
      // If the service_name field is set, that value will be used for the EDS request
      // instead of the cluster name (default).
      if (!edsClusterConfig.getServiceName().isEmpty()) {
        updateBuilder.setEdsServiceName(edsClusterConfig.getServiceName());
      }
      // The lb_policy field must be set to ROUND_ROBIN.
      if (!cluster.getLbPolicy().equals(LbPolicy.ROUND_ROBIN)) {
        invalidData = true;
        break;
      }
      updateBuilder.setLbPolicy("round_robin");
      // If the lrs_server field is set, it must have its self field set, in which case the
      // client should use LRS for load reporting. Otherwise (the lrs_server field is not set),
      // LRS load reporting will be disabled.
      if (cluster.hasLrsServer()) {
        if (!cluster.getLrsServer().hasSelf()) {
          invalidData = true;
          break;
        }
        updateBuilder.setEnableLrs(true);
        updateBuilder.setLrsServerName(serverUri);
      } else {
        updateBuilder.setEnableLrs(false);
      }
      ClusterUpdate update = updateBuilder.build();
      clusterUpdates.put(clusterName, update);
      if (endpointWatchers.containsKey(clusterName)) {
        desiredClusterUpdates.put(clusterName, update);
      }
    }
    if (invalidData) {
      adsStream.sendNackRequest(ADS_TYPE_URL_CDS, adsStream.cdsResourceNames,
          cdsResponse.getNonce(),
          "Cluster message contains invalid information for gRPC's usage");
      return;
    }
    adsStream.sendAckRequest(ADS_TYPE_URL_CDS, adsStream.cdsResourceNames,
        cdsResponse.getVersionInfo(), cdsResponse.getNonce());

    // Update local CDS cache with data in this response.
    clusterNamesToClusterUpdates.clear();
    clusterNamesToClusterUpdates.putAll(clusterUpdates);

    // Notify watchers waiting for updates of endpoint information received in this EDS response.
    for (Map.Entry<String, ClusterUpdate> entry : desiredClusterUpdates.entrySet()) {
      for (ClusterWatcher watcher : clusterWatchers.get(entry.getKey())) {
        watcher.onClusterChanged(entry.getValue());
      }
    }
  }

  /**
   * Processes RouteConfiguration message (from an resource information in an LDS or RDS
   * response), which may contain a VirtualHost with domains matching the "xds:"
   * URI hostname directly in-line. Returns the clusterName found in that VirtualHost
   * message. Returns {@code null} if such a clusterName cannot be resolved.
   *
   * <p>Note we only validate VirtualHosts with domains matching the "xds:" URI hostname.
   */
  @Nullable
  private String processRouteConfig(RouteConfiguration config) {
    List<VirtualHost> virtualHosts = config.getVirtualHostsList();
    int matchingLen = -1;  // longest length of wildcard pattern that matches host name
    VirtualHost targetVirtualHost = null;  // target VirtualHost with longest matched domain
    for (VirtualHost vHost : virtualHosts) {
      for (String domain : vHost.getDomainsList()) {
        if (matchHostName(hostName, domain) && domain.length() > matchingLen) {
          matchingLen = domain.length();
          targetVirtualHost = vHost;
        }
      }
    }

    // Proceed with the virtual host that has longest wildcard matched domain name with the
    // hostname in original "xds:" URI.
    if (targetVirtualHost != null) {
      // The client will look only at the last route in the list (the default route),
      // whose match field must be empty and whose route field must be set.
      List<Route> routes = targetVirtualHost.getRoutesList();
      if (!routes.isEmpty()) {
        Route route = routes.get(routes.size() - 1);
        // TODO(chengyuanzhang): check the match field must be empty.
        if (route.hasRoute()) {
          return route.getRoute().getCluster();
        }
      }
    }
    return null;
  }

  /**
   * Handles EDS response, which contains a list of ClusterLoadAssignment messages. Each
   * ClusterLoadAssignment message contains information for endpoints in a cluster, grouped
   * by localities. The response is NACKed if contains invalid information for gRPC's usage.
   * Otherwise, an ACK request is sent to management server. Information in ClusterLoadAssignment
   * messages are saved in local cache, in case of corresponding endpoint watchers are registered
   * later. Endpoint watchers interested in endpoint information contained in this response are
   * notified.
   */
  private void handleEdsResponse(DiscoveryResponse edsResponse) {
    logger.log(Level.FINE, "Received an EDS response: {0}", edsResponse);
    checkState(adsStream.edsResourceNames != null,
        "Never requested for EDS resources, management server is doing something wrong");
    adsStream.edsRespNonce = edsResponse.getNonce();

    // Unpack ClusterLoadAssignment messages.
    List<ClusterLoadAssignment> clusterLoadAssignments =
        new ArrayList<>(edsResponse.getResourcesCount());
    try {
      for (com.google.protobuf.Any res : edsResponse.getResourcesList()) {
        clusterLoadAssignments.add(res.unpack(ClusterLoadAssignment.class));
      }
    } catch (InvalidProtocolBufferException e) {
      adsStream.sendNackRequest(ADS_TYPE_URL_EDS, adsStream.edsResourceNames,
          edsResponse.getNonce(), "Broken EDS response");
      return;
    }

    String errorMessage = null;
    // Endpoint information updates for requested clusters received in this EDS response.
    Map<String, EndpointUpdate> endpointUpdates = new HashMap<>();
    // Walk through each ClusterLoadAssignment message. If any of them for requested clusters
    // contain invalid information for gRPC's load balancing usage, the whole response is rejected.
    validateData:
    for (ClusterLoadAssignment assignment : clusterLoadAssignments) {
      String clusterName = assignment.getClusterName();
      // Skip information for clusters not requested.
      // Management server is required to always send newly requested resources, even if they
      // may have been sent previously (proactively). Thus, client does not need to cache
      // unrequested resources.
      if (!endpointWatchers.containsKey(clusterName)) {
        continue;
      }
      EndpointUpdate.Builder updateBuilder = EndpointUpdate.newBuilder();
      updateBuilder.setClusterName(clusterName);
      if (assignment.getEndpointsCount() == 0) {
        errorMessage = "Cluster without any locality endpoint.";
        break;
      }
      // The policy.disable_overprovisioning field must be set to true.
      if (!assignment.getPolicy().getDisableOverprovisioning()) {
        errorMessage = "Cluster requires overprovisioning.";
        break;
      }
      for (io.envoyproxy.envoy.api.v2.endpoint.LocalityLbEndpoints localityLbEndpoints
          : assignment.getEndpointsList()) {
        // The lb_endpoints field for LbEndpoint must contain at least one entry.
        if (localityLbEndpoints.getLbEndpointsCount() == 0) {
          errorMessage = "Locality with no endpoint.";
          break validateData;
        }
        // The endpoint field of each lb_endpoints must be set.
        // Inside of it: the address field must be set.
        for (io.envoyproxy.envoy.api.v2.endpoint.LbEndpoint lbEndpoint
            : localityLbEndpoints.getLbEndpointsList()) {
          if (!lbEndpoint.hasEndpoint() || !lbEndpoint.getEndpoint().hasAddress()) {
            errorMessage = "Invalid endpoint address information.";
            break validateData;
          }
        }
        // Note endpoints with health status other than UNHEALTHY and UNKNOWN are still
        // handed over to watching parties. It is watching parties' responsibility to
        // filter out unhealthy endpoints. See EnvoyProtoData.LbEndpoint#isHealthy().
        updateBuilder.addLocalityLbEndpoints(
            Locality.fromEnvoyProtoLocality(localityLbEndpoints.getLocality()),
            LocalityLbEndpoints.fromEnvoyProtoLocalityLbEndpoints(localityLbEndpoints));
      }
      for (io.envoyproxy.envoy.api.v2.ClusterLoadAssignment.Policy.DropOverload dropOverload
          : assignment.getPolicy().getDropOverloadsList()) {
        updateBuilder.addDropPolicy(DropOverload.fromEnvoyProtoDropOverload(dropOverload));
      }
      EndpointUpdate update = updateBuilder.build();
      endpointUpdates.put(clusterName, update);
    }
    if (errorMessage != null) {
      adsStream.sendNackRequest(ADS_TYPE_URL_EDS, adsStream.edsResourceNames,
          edsResponse.getNonce(),
          "ClusterLoadAssignment message contains invalid information for gRPC's usage: "
              + errorMessage);
      return;
    }
    adsStream.sendAckRequest(ADS_TYPE_URL_EDS, adsStream.edsResourceNames,
        edsResponse.getVersionInfo(), edsResponse.getNonce());

    // Update local EDS cache by inserting updated endpoint information.
    clusterNamesToEndpointUpdates.putAll(endpointUpdates);

    // Notify watchers waiting for updates of endpoint information received in this EDS response.
    for (Map.Entry<String, EndpointUpdate> entry : endpointUpdates.entrySet()) {
      for (EndpointWatcher watcher : endpointWatchers.get(entry.getKey())) {
        watcher.onEndpointChanged(entry.getValue());
      }
    }
  }

  @VisibleForTesting
  final class RpcRetryTask implements Runnable {
    @Override
    public void run() {
      startRpcStream();
      if (configWatcher != null) {
        adsStream.sendXdsRequest(ADS_TYPE_URL_LDS, ImmutableList.of(ldsResourceName));
      }
      if (!clusterWatchers.isEmpty()) {
        adsStream.sendXdsRequest(ADS_TYPE_URL_CDS, clusterWatchers.keySet());
      }
      if (!endpointWatchers.isEmpty()) {
        adsStream.sendXdsRequest(ADS_TYPE_URL_EDS, endpointWatchers.keySet());
      }
    }
  }

  private final class AdsStream implements StreamObserver<DiscoveryResponse> {
    private final AggregatedDiscoveryServiceGrpc.AggregatedDiscoveryServiceStub stub;

    private StreamObserver<DiscoveryRequest> requestWriter;
    private boolean responseReceived;
    private boolean closed;

    // Last successfully applied version_info for each resource type. Starts with empty string.
    // A version_info is used to update management server with client's most recent knowledge of
    // resources.
    private String ldsVersion = "";
    private String rdsVersion = "";
    private String cdsVersion = "";
    private String edsVersion = "";

    // Response nonce for the most recently received discovery responses of each resource type.
    // Client initiated requests start response nonce with empty string.
    // A nonce is used to indicate the specific DiscoveryResponse each DiscoveryRequest
    // corresponds to.
    // A nonce becomes stale following a newer nonce being presented to the client in a
    // DiscoveryResponse.
    private String ldsRespNonce = "";
    private String rdsRespNonce = "";
    private String cdsRespNonce = "";
    private String edsRespNonce = "";

    // Most recently requested resource name(s) for each resource type. Note the resource_name in
    // LDS requests will always be "xds:" URI (including port suffix if present).
    @Nullable
    private String rdsResourceName;
    @Nullable
    private Collection<String> cdsResourceNames;
    @Nullable
    private Collection<String> edsResourceNames;

    private AdsStream(AggregatedDiscoveryServiceGrpc.AggregatedDiscoveryServiceStub stub) {
      this.stub = checkNotNull(stub, "stub");
    }

    private void start() {
      requestWriter = stub.withWaitForReady().streamAggregatedResources(this);
    }

    @Override
    public void onNext(final DiscoveryResponse response) {
      syncContext.execute(new Runnable() {
        @Override
        public void run() {
          responseReceived = true;
          String typeUrl = response.getTypeUrl();
          if (typeUrl.equals(ADS_TYPE_URL_LDS)) {
            handleLdsResponse(response);
          } else if (typeUrl.equals(ADS_TYPE_URL_RDS)) {
            handleRdsResponse(response);
          } else if (typeUrl.equals(ADS_TYPE_URL_CDS)) {
            handleCdsResponse(response);
          } else if (typeUrl.equals(ADS_TYPE_URL_EDS)) {
            handleEdsResponse(response);
          } else {
            logger.log(Level.FINE, "Received an unknown type of DiscoveryResponse {0}",
                response);
          }
        }
      });
    }

    @Override
    public void onError(final Throwable t) {
      syncContext.execute(new Runnable() {
        @Override
        public void run() {
          handleStreamClosed(
              Status.fromThrowable(t).augmentDescription("ADS stream [" + this + "] had an error"));
        }
      });
    }

    @Override
    public void onCompleted() {
      syncContext.execute(new Runnable() {
        @Override
        public void run() {
          handleStreamClosed(
              Status.UNAVAILABLE.withDescription("ADS stream [" + this + "] was closed by server"));
        }
      });
    }

    private void handleStreamClosed(Status error) {
      logger.log(Level.INFO, error.getDescription(), error.getCause());
      checkArgument(!error.isOk(), "unexpected OK status");
      if (closed) {
        return;
      }
      closed = true;
      cleanUp();
      if (responseReceived || retryBackoffPolicy == null) {
        // Reset the backoff sequence if had received a response, or backoff sequence
        // has never been initialized.
        retryBackoffPolicy = backoffPolicyProvider.get();
      }
      long delayNanos = 0;
      if (!responseReceived) {
        delayNanos =
            Math.max(
                0,
                retryBackoffPolicy.nextBackoffNanos() - stopwatch.elapsed(TimeUnit.NANOSECONDS));
      }
      logger.log(Level.FINE, "{0} stream closed, retry in {1} ns", new Object[]{this, delayNanos});
      rpcRetryTimer =
          syncContext.schedule(
              new RpcRetryTask(), delayNanos, TimeUnit.NANOSECONDS, timeService);
    }

    private void close(Exception error) {
      if (closed) {
        return;
      }
      closed = true;
      cleanUp();
      requestWriter.onError(error);
    }

    private void cleanUp() {
      if (adsStream == this) {
        adsStream = null;
      }
    }

    /**
     * Sends a DiscoveryRequest for the given resource name to management server. Memories the
     * requested resource name (except for LDS as we always request for the singleton Listener)
     * as we need it to find resources in responses.
     */
    private void sendXdsRequest(String typeUrl, Collection<String> resourceNames) {
      checkState(requestWriter != null, "ADS stream has not been started");
      String version = "";
      String nonce = "";
      if (typeUrl.equals(ADS_TYPE_URL_LDS)) {
        version = ldsVersion;
        nonce = ldsRespNonce;
      } else if (typeUrl.equals(ADS_TYPE_URL_RDS)) {
        checkArgument(resourceNames.size() == 1,
            "RDS request requesting for more than one resource");
        version = rdsVersion;
        nonce = rdsRespNonce;
        rdsResourceName = resourceNames.iterator().next();
      } else if (typeUrl.equals(ADS_TYPE_URL_CDS)) {
        version = cdsVersion;
        nonce = cdsRespNonce;
        cdsResourceNames = resourceNames;
      } else if (typeUrl.equals(ADS_TYPE_URL_EDS)) {
        version = edsVersion;
        nonce = edsRespNonce;
        edsResourceNames = resourceNames;
      }
      DiscoveryRequest request =
          DiscoveryRequest
              .newBuilder()
              .setVersionInfo(version)
              .setNode(node)
              .addAllResourceNames(resourceNames)
              .setTypeUrl(typeUrl)
              .setResponseNonce(nonce)
              .build();
      requestWriter.onNext(request);
    }

    /**
     * Sends a DiscoveryRequest with the given information as an ACK. Updates the latest accepted
     * version for the corresponding resource type.
     */
    private void sendAckRequest(String typeUrl, Collection<String> resourceNames,
        String versionInfo, String nonce) {
      checkState(requestWriter != null, "ADS stream has not been started");
      if (typeUrl.equals(ADS_TYPE_URL_LDS)) {
        ldsVersion = versionInfo;
        ldsRespNonce = nonce;
      } else if (typeUrl.equals(ADS_TYPE_URL_RDS)) {
        rdsVersion = versionInfo;
        rdsRespNonce = nonce;
      } else if (typeUrl.equals(ADS_TYPE_URL_CDS)) {
        cdsVersion = versionInfo;
        cdsRespNonce = nonce;
      } else if (typeUrl.equals(ADS_TYPE_URL_EDS)) {
        edsVersion = versionInfo;
        edsRespNonce = nonce;
      }
      DiscoveryRequest request =
          DiscoveryRequest
              .newBuilder()
              .setVersionInfo(versionInfo)
              .setNode(node)
              .addAllResourceNames(resourceNames)
              .setTypeUrl(typeUrl)
              .setResponseNonce(nonce)
              .build();
      requestWriter.onNext(request);
    }

    /**
     * Sends a DiscoveryRequest with the given information as an NACK. NACK takes the previous
     * accepted version.
     */
    private void sendNackRequest(String typeUrl, Collection<String> resourceNames, String nonce,
        String message) {
      checkState(requestWriter != null, "ADS stream has not been started");
      String versionInfo = "";
      if (typeUrl.equals(ADS_TYPE_URL_LDS)) {
        versionInfo = ldsVersion;
      } else if (typeUrl.equals(ADS_TYPE_URL_RDS)) {
        versionInfo = rdsVersion;
      } else if (typeUrl.equals(ADS_TYPE_URL_CDS)) {
        versionInfo = cdsVersion;
      } else if (typeUrl.equals(ADS_TYPE_URL_EDS)) {
        versionInfo = edsVersion;
      }
      DiscoveryRequest request =
          DiscoveryRequest
              .newBuilder()
              .setVersionInfo(versionInfo)
              .setNode(node)
              .addAllResourceNames(resourceNames)
              .setTypeUrl(typeUrl)
              .setResponseNonce(nonce)
              .setErrorDetail(
                  com.google.rpc.Status.newBuilder()
                      .setCode(Code.INVALID_ARGUMENT_VALUE)
                      .setMessage(message))
              .build();
      requestWriter.onNext(request);
    }
  }

  /**
   * Returns {@code true} iff {@code hostName} matches the domain name {@code pattern} with
   * case-insensitive.
   *
   * <p>Wildcard pattern rules:
   * <ol>
   * <li>A single asterisk (*) matches any domain.</li>
   * <li>Asterisk (*) is only permitted in the left-most or the right-most part of the pattern,
   *     but not both.</li>
   * </ol>
   */
  @VisibleForTesting
  static boolean matchHostName(String hostName, String pattern) {
    checkArgument(hostName.length() != 0 && !hostName.startsWith(".") && !hostName.endsWith("."),
        "Invalid host name");
    checkArgument(pattern.length() != 0 && !pattern.startsWith(".") && !pattern.endsWith("."),
        "Invalid pattern/domain name");

    hostName = hostName.toLowerCase(Locale.US);
    pattern = pattern.toLowerCase(Locale.US);
    // hostName and pattern are now in lower case -- domain names are case-insensitive.

    if (!pattern.contains("*")) {
      // Not a wildcard pattern -- hostName and pattern must match exactly.
      return hostName.equals(pattern);
    }
    // Wildcard pattern

    if (pattern.length() == 1) {
      return true;
    }

    int index = pattern.indexOf('*');

    // At most one asterisk (*) is allowed.
    if (pattern.indexOf('*', index + 1) != -1) {
      return false;
    }

    // Asterisk can only match prefix or suffix.
    if (index != 0 && index != pattern.length() - 1) {
      return false;
    }

    // HostName must be at least as long as the pattern because asterisk has to
    // match one or more characters.
    if (hostName.length() < pattern.length()) {
      return false;
    }

    if (index == 0 && hostName.endsWith(pattern.substring(1))) {
      // Prefix matching fails.
      return true;
    }

    // Pattern matches hostname if suffix matching succeeds.
    return index == pattern.length() - 1
        && hostName.startsWith(pattern.substring(0, pattern.length() - 1));
  }
}
