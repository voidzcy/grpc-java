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
import static io.grpc.ConnectivityState.CONNECTING;
import static io.grpc.ConnectivityState.IDLE;
import static io.grpc.ConnectivityState.READY;
import static io.grpc.ConnectivityState.TRANSIENT_FAILURE;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.grpc.ConnectivityState;
import io.grpc.EquivalentAddressGroup;
import io.grpc.InternalLogId;
import io.grpc.LoadBalancer;
import io.grpc.LoadBalancer.Helper;
import io.grpc.LoadBalancer.PickResult;
import io.grpc.LoadBalancer.PickSubchannelArgs;
import io.grpc.LoadBalancer.ResolvedAddresses;
import io.grpc.LoadBalancer.SubchannelPicker;
import io.grpc.LoadBalancerProvider;
import io.grpc.LoadBalancerRegistry;
import io.grpc.Status;
import io.grpc.SynchronizationContext.ScheduledHandle;
import io.grpc.util.ForwardingLoadBalancerHelper;
import io.grpc.xds.ClientLoadCounter.LoadRecordingSubchannelPicker;
import io.grpc.xds.EnvoyProtoData.DropOverload;
import io.grpc.xds.EnvoyProtoData.LbEndpoint;
import io.grpc.xds.EnvoyProtoData.Locality;
import io.grpc.xds.EnvoyProtoData.LocalityLbEndpoints;
import io.grpc.xds.LoadStatsManager.LoadStatsStore;
import io.grpc.xds.WeightedRandomPicker.WeightedChildPicker;
import io.grpc.xds.XdsLogger.XdsLogLevel;
import io.grpc.xds.XdsSubchannelPickers.ErrorPicker;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Manages EAG and locality info for a collection of subchannels, not including subchannels
 * created by the fallback balancer.
 */
// Must be accessed/run in SynchronizedContext.
interface LocalityStore {

  void reset();

  void updateLocalityStore(Map<Locality, LocalityLbEndpoints> localityInfoMap);

  void updateDropPercentage(List<DropOverload> dropOverloads);

  void setLoadStatsStore(@Nullable LoadStatsStore loadStatsStore);

  @VisibleForTesting
  abstract class LocalityStoreFactory {
    private static final LocalityStoreFactory DEFAULT_INSTANCE =
        new LocalityStoreFactory() {
          @Override
          LocalityStore newLocalityStore(
              InternalLogId logId,
              Helper helper,
              LoadBalancerRegistry lbRegistry) {
            return new LocalityStoreImpl(logId, helper, lbRegistry);
          }
        };

    static LocalityStoreFactory getInstance() {
      return DEFAULT_INSTANCE;
    }

    abstract LocalityStore newLocalityStore(
        InternalLogId logId,
        Helper helper,
        LoadBalancerRegistry lbRegistry);
  }

  final class LocalityStoreImpl implements LocalityStore {
    private static final String ROUND_ROBIN = "round_robin";
    private static final long DELAYED_DELETION_TIMEOUT_MINUTES = 15L;

    private final XdsLogger logger;
    private final Helper helper;
    private final LoadBalancerProvider loadBalancerProvider;
    private final ThreadSafeRandom random;
    private final PriorityManager priorityManager = new PriorityManager();
    private final Map<Locality, LocalityLbInfo> localityMap = new HashMap<>();
    private List<DropOverload> dropOverloads = ImmutableList.of();
    @Nullable
    private LoadStatsStore loadStatsStore;

    LocalityStoreImpl(InternalLogId logId, Helper helper, LoadBalancerRegistry lbRegistry) {
      this(logId, helper, lbRegistry, ThreadSafeRandom.ThreadSafeRandomImpl.instance);
    }

    @VisibleForTesting
    LocalityStoreImpl(InternalLogId logId, Helper helper, LoadBalancerRegistry lbRegistry,
        ThreadSafeRandom random) {
      this.helper = checkNotNull(helper, "helper");
      loadBalancerProvider = checkNotNull(
          lbRegistry.getProvider(ROUND_ROBIN),
          "Unable to find '%s' LoadBalancer", ROUND_ROBIN);
      this.random = checkNotNull(random, "random");
      logger = XdsLogger.withLogId(checkNotNull(logId, "logId"));
    }

    private final class DroppablePicker extends SubchannelPicker {

      final List<DropOverload> dropOverloads;
      final SubchannelPicker delegate;
      final ThreadSafeRandom random;

      DroppablePicker(
          List<DropOverload> dropOverloads, SubchannelPicker delegate,
          ThreadSafeRandom random) {
        this.dropOverloads = dropOverloads;
        this.delegate = delegate;
        this.random = random;
      }

      @Override
      public PickResult pickSubchannel(PickSubchannelArgs args) {
        for (DropOverload dropOverload : dropOverloads) {
          int rand = random.nextInt(1000_000);
          if (rand < dropOverload.getDropsPerMillion()) {
            logger.log(
                XdsLogLevel.INFO, "Drop request with category: {0}", dropOverload.getCategory());
            if (loadStatsStore != null) {
              loadStatsStore.recordDroppedRequest(dropOverload.getCategory());
            }
            return PickResult.withDrop(Status.UNAVAILABLE.withDescription(
                "dropped by loadbalancer: " + dropOverload.toString()));
          }
        }
        return delegate.pickSubchannel(args);
      }

      @Override
      public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("dropOverloads", dropOverloads)
            .add("delegate", delegate)
            .toString();
      }
    }

    @Override
    public void reset() {
      for (Locality locality : localityMap.keySet()) {
        localityMap.get(locality).shutdown();
      }
      localityMap.clear();
      priorityManager.reset();
    }

    @Override
    public void updateLocalityStore(final Map<Locality, LocalityLbEndpoints> localityInfoMap) {
      Set<Locality> newLocalities = localityInfoMap.keySet();
      // TODO: put endPointWeights into attributes for WRR.
      for (Locality locality : newLocalities) {
        if (localityMap.containsKey(locality)) {
          LocalityLbInfo localityLbInfo = localityMap.get(locality);
          localityLbInfo.refreshEndpoints(localityInfoMap.get(locality));
        }
      }
      priorityManager.updateLocalities(localityInfoMap);
      for (Locality oldLocality : localityMap.keySet()) {
        if (!newLocalities.contains(oldLocality)) {
          deactivate(oldLocality);
        }
      }
    }

    @Override
    public void updateDropPercentage(List<DropOverload> dropOverloads) {
      this.dropOverloads = checkNotNull(dropOverloads, "dropOverloads");
    }

    @Override
    public void setLoadStatsStore(@Nullable LoadStatsStore loadStatsStore) {
      this.loadStatsStore = loadStatsStore;
    }

    private void deactivate(final Locality locality) {
      if (!localityMap.containsKey(locality) || localityMap.get(locality).isDeactivated()) {
        return;
      }

      final LocalityLbInfo localityLbInfo = localityMap.get(locality);
      class DeletionTask implements Runnable {

        @Override
        public void run() {
          localityLbInfo.shutdown();
          localityMap.remove(locality);
        }

        @Override
        public String toString() {
          return "DeletionTask: locality=" + locality;
        }
      }

      localityLbInfo.delayedDeletionTimer = helper.getSynchronizationContext().schedule(
          new DeletionTask(), DELAYED_DELETION_TIMEOUT_MINUTES,
          TimeUnit.MINUTES, helper.getScheduledExecutorService());
    }

    @Nullable
    private static ConnectivityState aggregateState(
        @Nullable ConnectivityState overallState, ConnectivityState childState) {
      if (overallState == null) {
        return childState;
      }
      if (overallState == READY || childState == READY) {
        return READY;
      }
      if (overallState == CONNECTING || childState == CONNECTING) {
        return CONNECTING;
      }
      if (overallState == IDLE || childState == IDLE) {
        return IDLE;
      }
      return overallState;
    }

    private void updatePicker(
        @Nullable ConnectivityState state,  List<WeightedChildPicker> childPickers) {
      SubchannelPicker picker;
      if (childPickers.isEmpty()) {
        if (state == TRANSIENT_FAILURE) {
          picker = new ErrorPicker(Status.UNAVAILABLE); // TODO: more details in status
        } else {
          picker = XdsSubchannelPickers.BUFFER_PICKER;
        }
      } else {
        picker = new WeightedRandomPicker(childPickers);
      }

      if (!dropOverloads.isEmpty()) {
        picker = new DroppablePicker(dropOverloads, picker, random);
      }

      if (state != null) {
        helper.updateBalancingState(state, picker);
      }
    }

    /**
     * State of a single Locality.
     */
    // TODO(zdapeng): rename it to LocalityLbState
    private final class LocalityLbInfo {

      final Locality locality;
      final LoadBalancer childBalancer;
      final ChildHelper childHelper;
      @Nullable
      private ScheduledHandle delayedDeletionTimer;

      LocalityLbInfo(Locality locality) {
        this.locality = checkNotNull(locality, "locality");
        if (loadStatsStore != null) {
          loadStatsStore.addLocality(locality);
        }
        childHelper = new ChildHelper();
        childBalancer = loadBalancerProvider.newLoadBalancer(childHelper);
      }

      void refreshEndpoints(LocalityLbEndpoints localityLbEndpoints) {
        final List<EquivalentAddressGroup> eags = new ArrayList<>();
        for (LbEndpoint endpoint : localityLbEndpoints.getEndpoints()) {
          if (endpoint.isHealthy()) {
            eags.add(endpoint.getAddress());
          }
        }
        // In extreme case handleResolvedAddresses() may trigger updateBalancingState()
        // immediately, so execute handleResolvedAddresses() after all the setup in the caller is
        // complete.
        childHelper.getSynchronizationContext().execute(new Runnable() {
          @Override
          public void run() {
            if (eags.isEmpty() && !childBalancer.canHandleEmptyAddressListFromNameResolution()) {
              childBalancer.handleNameResolutionError(
                  Status.UNAVAILABLE.withDescription(
                      "Locality " + locality + " has no healthy endpoint"));
            } else {
              childBalancer.handleResolvedAddresses(
                  ResolvedAddresses.newBuilder().setAddresses(eags).build());
            }
          }
        });
      }

      void shutdown() {
        if (delayedDeletionTimer != null) {
          delayedDeletionTimer.cancel();
          delayedDeletionTimer = null;
        }
        childBalancer.shutdown();
        if (loadStatsStore != null) {
          loadStatsStore.removeLocality(locality);
        }
        logger.log(XdsLogLevel.INFO, "Shut down child balancer for locality {0}", locality);
      }

      void reactivate() {
        if (delayedDeletionTimer != null) {
          delayedDeletionTimer.cancel();
          delayedDeletionTimer = null;
        }
      }

      boolean isDeactivated() {
        return delayedDeletionTimer != null;
      }

      class ChildHelper extends ForwardingLoadBalancerHelper {
        private SubchannelPicker currentChildPicker = XdsSubchannelPickers.BUFFER_PICKER;
        private ConnectivityState currentChildState = CONNECTING;

        @Override
        protected Helper delegate() {
          return helper;
        }

        @Override
        public void updateBalancingState(ConnectivityState newState, SubchannelPicker newPicker) {
          logger.log(
              XdsLogLevel.INFO,
              "Update load balancing state for locality {0} to {1}", locality, newState);
          currentChildState = newState;
          if (loadStatsStore != null) {
            ClientLoadCounter counter = loadStatsStore.getLocalityCounter(locality);
            currentChildPicker = new LoadRecordingSubchannelPicker(counter, newPicker);
          } else {
            currentChildPicker = newPicker;
          }
          priorityManager.updatePriorityState(priorityManager.getPriority(locality));
        }

        @Override
        public String getAuthority() {
          //FIXME: This should be a new proposed field of Locality, locality_name
          return locality.getSubZone();
        }
      }
    }

    private final class PriorityManager {

      private final List<List<Locality>> priorityTable = new ArrayList<>();
      private Map<Locality, LocalityLbEndpoints> localityInfoMap = ImmutableMap.of();
      private int currentPriority = -1;
      private ScheduledHandle failOverTimer;

      /**
       * Updates the priority ordering of localities with the given collection of localities.
       * Recomputes the current ready localities to be used.
       */
      void updateLocalities(Map<Locality, LocalityLbEndpoints> localityInfoMap) {
        this.localityInfoMap = localityInfoMap;
        priorityTable.clear();
        for (Locality newLocality : localityInfoMap.keySet()) {
          int priority = localityInfoMap.get(newLocality).getPriority();
          while (priorityTable.size() <= priority) {
            priorityTable.add(new ArrayList<Locality>());
          }
          priorityTable.get(priority).add(newLocality);
        }
        if (logger.isLoggable(XdsLogLevel.INFO)) {
          for (int i = 0; i < priorityTable.size(); i++) {
            logger.log(
                XdsLogLevel.INFO,
                "Priority {0} contains localities: {1}", i, priorityTable.get(i));
          }
        }
        if (priorityTable.isEmpty()) {
          helper.updateBalancingState(
              TRANSIENT_FAILURE,
              new ErrorPicker(Status.UNAVAILABLE.withDescription("Received 0 locality")));
          return;
        }
        currentPriority = -1;
        failOver();
      }

      /**
       * Refreshes the group of localities with the given priority. Recomputes the current ready
       * localities to be used.
       */
      void updatePriorityState(int priority) {
        if (priority == -1 || priority > currentPriority) {
          return;
        }
        List<WeightedChildPicker> childPickers = new ArrayList<>();

        ConnectivityState overallState = null;
        for (Locality l : priorityTable.get(priority)) {
          if (!localityMap.containsKey(l)) {
            initLocality(l);
          }
          LocalityLbInfo localityLbInfo = localityMap.get(l);
          localityLbInfo.reactivate();
          ConnectivityState childState = localityLbInfo.childHelper.currentChildState;
          SubchannelPicker childPicker = localityLbInfo.childHelper.currentChildPicker;

          overallState = aggregateState(overallState, childState);

          if (READY == childState) {
            childPickers.add(
                new WeightedChildPicker(localityInfoMap.get(l).getLocalityWeight(), childPicker));
          }
        }
        logger.log(XdsLogLevel.INFO, "Update priority {0} state to {1}", priority, overallState);
        if (priority == currentPriority) {
          updatePicker(overallState, childPickers);
          if (overallState == READY) {
            cancelFailOverTimer();
          } else if (overallState == TRANSIENT_FAILURE) {
            cancelFailOverTimer();
            failOver();
          } else if (failOverTimer == null) {
            failOver();
          } // else, still connecting and failOverTimer not expired yet, noop
        } else if (overallState == READY) {
          updatePicker(overallState, childPickers);
          cancelFailOverTimer();
          currentPriority = priority;
        }

        if (overallState == READY) {
          for (int p = priority + 1; p < priorityTable.size(); p++) {
            for (Locality xdsLocality : priorityTable.get(p)) {
              deactivate(xdsLocality);
            }
          }
        }
      }

      int getPriority(Locality locality) {
        if (localityInfoMap.containsKey(locality)) {
          return localityInfoMap.get(locality).getPriority();
        }
        return -1;
      }

      void reset() {
        cancelFailOverTimer();
        priorityTable.clear();
        localityInfoMap = ImmutableMap.of();
        currentPriority = -1;
      }

      private void cancelFailOverTimer() {
        if (failOverTimer != null) {
          failOverTimer.cancel();
          failOverTimer = null;
        }
      }

      private void failOver() {
        if (currentPriority == priorityTable.size() - 1) {
          return;
        }

        currentPriority++;

        List<Locality> localities = priorityTable.get(currentPriority);
        boolean initializedBefore = false;
        for (Locality locality : localities) {
          if (localityMap.containsKey(locality)) {
            initializedBefore = true;
            localityMap.get(locality).reactivate();
          } else {
            initLocality(locality);
          }
        }

        if (!initializedBefore) {
          class FailOverTask implements Runnable {
            @Override
            public void run() {
              logger.log(XdsLogLevel.INFO, "Failing over to priority {0}", currentPriority + 1);
              failOverTimer = null;
              failOver();
            }
          }

          failOverTimer = helper.getSynchronizationContext().schedule(
              new FailOverTask(), 10, TimeUnit.SECONDS, helper.getScheduledExecutorService());
        }

        updatePriorityState(currentPriority);
      }

      private void initLocality(Locality locality) {
        logger.log(XdsLogLevel.INFO, "Create child balancer for locality {0}", locality);
        LocalityLbInfo localityLbInfo = new LocalityLbInfo(locality);
        localityMap.put(locality, localityLbInfo);
        localityLbInfo.refreshEndpoints(localityInfoMap.get(locality));
      }
    }
  }
}
