/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.broker.broker.helix;

import com.google.common.collect.ImmutableList;
import com.yammer.metrics.core.MetricsRegistry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixConstants.ChangeType;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.SystemPropertyKeys;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.Message;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.broker.broker.AccessControlFactory;
import org.apache.pinot.broker.broker.BrokerServerBuilder;
import org.apache.pinot.broker.queryquota.TableQueryQuotaManager;
import org.apache.pinot.broker.routing.HelixExternalViewBasedRouting;
import org.apache.pinot.common.config.TagNameUtils;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.common.utils.NetUtil;
import org.apache.pinot.common.utils.ServiceStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class HelixBrokerStarter {
  private static final Logger LOGGER = LoggerFactory.getLogger(HelixBrokerStarter.class);
  private static final String ROUTING_TABLE_PARAMS_SUBSET_KEY = "pinot.broker.routing.table";

  private final Configuration _properties;
  private final String _clusterName;
  private final String _zkServers;
  private final String _brokerId;

  // Spectator Helix manager handles the custom change listeners, properties read/write
  private HelixManager _spectatorHelixManager;
  // Participant Helix manager handles Helix functionality such as state transitions and messages
  private HelixManager _participantHelixManager;

  private HelixAdmin _helixAdmin;
  private ZkHelixPropertyStore<ZNRecord> _propertyStore;
  private HelixDataAccessor _helixDataAccessor;
  private HelixExternalViewBasedRouting _helixExternalViewBasedRouting;
  private BrokerServerBuilder _brokerServerBuilder;
  private AccessControlFactory _accessControlFactory;
  private MetricsRegistry _metricsRegistry;
  private ClusterChangeMediator _clusterChangeMediator;
  private TimeboundaryRefreshMessageHandlerFactory _tbiMessageHandler;

  public HelixBrokerStarter(Configuration properties, String clusterName, String zkServer)
      throws Exception {
    this(properties, clusterName, zkServer, null);
  }

  public HelixBrokerStarter(Configuration properties, String clusterName, String zkServer, @Nullable String brokerHost)
      throws Exception {
    _properties = properties;
    setupHelixSystemProperties();

    _clusterName = clusterName;

    // Remove all white-spaces from the list of zkServers (if any).
    _zkServers = zkServer.replaceAll("\\s+", "");

    if (brokerHost == null) {
      brokerHost = NetUtil.getHostAddress();
    }
    _brokerId = _properties.getString(CommonConstants.Helix.Instance.INSTANCE_ID_KEY,
        CommonConstants.Helix.PREFIX_OF_BROKER_INSTANCE + brokerHost + "_" + _properties
            .getInt(CommonConstants.Helix.KEY_OF_BROKER_QUERY_PORT, CommonConstants.Helix.DEFAULT_BROKER_QUERY_PORT));
    _properties.addProperty(CommonConstants.Broker.CONFIG_OF_BROKER_ID, _brokerId);
  }

  private void setupHelixSystemProperties() {
    System.setProperty(SystemPropertyKeys.FLAPPING_TIME_WINDOW, _properties
        .getString(CommonConstants.Broker.CONFIG_OF_HELIX_FLAPPING_TIME_WINDOW_MS,
            CommonConstants.Broker.DEFAULT_HELIX_FLAPPING_TIME_WINDOW_MS));
  }

  public void start()
      throws Exception {
    LOGGER.info("Starting Pinot broker");

    // Connect the spectator Helix manager
    LOGGER.info("Connecting spectator Helix manager");
    _spectatorHelixManager =
        HelixManagerFactory.getZKHelixManager(_clusterName, _brokerId, InstanceType.SPECTATOR, _zkServers);
    _spectatorHelixManager.connect();
    _helixAdmin = _spectatorHelixManager.getClusterManagmentTool();
    _propertyStore = _spectatorHelixManager.getHelixPropertyStore();
    _helixDataAccessor = _spectatorHelixManager.getHelixDataAccessor();
    _helixExternalViewBasedRouting = new HelixExternalViewBasedRouting(_propertyStore, _spectatorHelixManager,
        _properties.subset(ROUTING_TABLE_PARAMS_SUBSET_KEY));
    TableQueryQuotaManager tableQueryQuotaManager = new TableQueryQuotaManager(_spectatorHelixManager);
    LiveInstanceChangeHandler liveInstanceChangeHandler = new LiveInstanceChangeHandler(_spectatorHelixManager);

    // Set up the broker server builder
    LOGGER.info("Setting up broker server builder");
    _brokerServerBuilder = new BrokerServerBuilder(_properties, _helixExternalViewBasedRouting,
        _helixExternalViewBasedRouting.getTimeBoundaryService(), liveInstanceChangeHandler, tableQueryQuotaManager);
    _accessControlFactory = _brokerServerBuilder.getAccessControlFactory();
    _metricsRegistry = _brokerServerBuilder.getMetricsRegistry();
    BrokerMetrics brokerMetrics = _brokerServerBuilder.getBrokerMetrics();
    _helixExternalViewBasedRouting.setBrokerMetrics(brokerMetrics);
    tableQueryQuotaManager.setBrokerMetrics(brokerMetrics);
    _brokerServerBuilder.start();

    // Initialize the cluster change mediator
    LOGGER.info("Initializing cluster change mediator");
    Map<ChangeType, List<ClusterChangeHandler>> changeHandlersMap = new HashMap<>();
    List<ClusterChangeHandler> externalViewChangeHandlers = new ArrayList<>();
    externalViewChangeHandlers.add(_helixExternalViewBasedRouting);
    externalViewChangeHandlers.add(tableQueryQuotaManager);
    externalViewChangeHandlers.addAll(getCustomExternalViewChangeHandlers(_spectatorHelixManager));
    changeHandlersMap.put(ChangeType.EXTERNAL_VIEW, externalViewChangeHandlers);
    List<ClusterChangeHandler> instanceConfigChangeHandlers = new ArrayList<>();
    instanceConfigChangeHandlers.add(_helixExternalViewBasedRouting);
    instanceConfigChangeHandlers.addAll(getCustomInstanceConfigChangeHandlers(_spectatorHelixManager));
    changeHandlersMap.put(ChangeType.INSTANCE_CONFIG, instanceConfigChangeHandlers);
    List<ClusterChangeHandler> liveInstanceChangeHandlers = new ArrayList<>();
    liveInstanceChangeHandlers.add(liveInstanceChangeHandler);
    liveInstanceChangeHandlers.addAll(getCustomLiveInstanceChangeHandlers(_spectatorHelixManager));
    changeHandlersMap.put(ChangeType.LIVE_INSTANCE, liveInstanceChangeHandlers);
    _clusterChangeMediator = new ClusterChangeMediator(changeHandlersMap, brokerMetrics);
    _clusterChangeMediator.start();
    _spectatorHelixManager.addExternalViewChangeListener(_clusterChangeMediator);
    _spectatorHelixManager.addInstanceConfigChangeListener(_clusterChangeMediator);
    _spectatorHelixManager.addLiveInstanceChangeListener(_clusterChangeMediator);

    // Connect the participant Helix manager
    LOGGER.info("Connecting participant Helix manager");
    _participantHelixManager =
        HelixManagerFactory.getZKHelixManager(_clusterName, _brokerId, InstanceType.PARTICIPANT, _zkServers);
    StateMachineEngine stateMachineEngine = _participantHelixManager.getStateMachineEngine();
    StateModelFactory<?> stateModelFactory =
        new BrokerResourceOnlineOfflineStateModelFactory(_propertyStore, _helixDataAccessor,
            _helixExternalViewBasedRouting, tableQueryQuotaManager);
    stateMachineEngine
        .registerStateModelFactory(BrokerResourceOnlineOfflineStateModelFactory.getStateModelDef(), stateModelFactory);
    _participantHelixManager.connect();
    _tbiMessageHandler = new TimeboundaryRefreshMessageHandlerFactory(_helixExternalViewBasedRouting, _properties
        .getLong(CommonConstants.Broker.CONFIG_OF_BROKER_REFRESH_TIMEBOUNDARY_INFO_SLEEP_INTERVAL,
            CommonConstants.Broker.DEFAULT_BROKER_REFRESH_TIMEBOUNDARY_INFO_SLEEP_INTERVAL_MS));
    _participantHelixManager.getMessagingService()
        .registerMessageHandlerFactory(Message.MessageType.USER_DEFINE_MSG.toString(), _tbiMessageHandler);
    addInstanceTagIfNeeded();
    brokerMetrics.addCallbackGauge("helix.connected", () -> _participantHelixManager.isConnected() ? 1L : 0L);
    _participantHelixManager
        .addPreConnectCallback(() -> brokerMetrics.addMeteredGlobalValue(BrokerMeter.HELIX_ZOOKEEPER_RECONNECTS, 1L));

    // Register the service status handler
    LOGGER.info("Registering service status handler");
    double minResourcePercentForStartup = _properties
        .getDouble(CommonConstants.Broker.CONFIG_OF_BROKER_MIN_RESOURCE_PERCENT_FOR_START,
            CommonConstants.Broker.DEFAULT_BROKER_MIN_RESOURCE_PERCENT_FOR_START);
    ServiceStatus.setServiceStatusCallback(new ServiceStatus.MultipleCallbackServiceStatusCallback(ImmutableList
        .of(new ServiceStatus.IdealStateAndCurrentStateMatchServiceStatusCallback(_participantHelixManager,
                _clusterName, _brokerId, minResourcePercentForStartup),
            new ServiceStatus.IdealStateAndExternalViewMatchServiceStatusCallback(_participantHelixManager,
                _clusterName, _brokerId, minResourcePercentForStartup))));

    LOGGER.info("Finish starting Pinot broker");
  }

  private void addInstanceTagIfNeeded() {
    InstanceConfig instanceConfig =
        _helixDataAccessor.getProperty(_helixDataAccessor.keyBuilder().instanceConfig(_brokerId));
    List<String> instanceTags = instanceConfig.getTags();
    if (instanceTags == null || instanceTags.isEmpty()) {
      if (ZKMetadataProvider.getClusterTenantIsolationEnabled(_propertyStore)) {
        _helixAdmin.addInstanceTag(_clusterName, _brokerId,
            TagNameUtils.getBrokerTagForTenant(TagNameUtils.DEFAULT_TENANT_NAME));
      } else {
        _helixAdmin.addInstanceTag(_clusterName, _brokerId, CommonConstants.Helix.UNTAGGED_BROKER_INSTANCE);
      }
    }
  }

  public void shutdown() {
    LOGGER.info("Shutting down Pinot broker");

    if (_tbiMessageHandler != null) {
      LOGGER.info("Shutting down time boundary info refresh message handler");
      _tbiMessageHandler.shutdown();
    }

    if (_participantHelixManager != null) {
      LOGGER.info("Disconnecting participant Helix manager");
      _participantHelixManager.disconnect();
    }

    if (_clusterChangeMediator != null) {
      LOGGER.info("Stopping cluster change mediator");
      _clusterChangeMediator.stop();
    }

    if (_brokerServerBuilder != null) {
      LOGGER.info("Stopping broker server builder");
      _brokerServerBuilder.stop();
    }

    if (_spectatorHelixManager != null) {
      LOGGER.info("Disconnecting spectator Helix manager");
      _spectatorHelixManager.disconnect();
    }

    LOGGER.info("Finish shutting down Pinot broker");
  }

  /**
   * To be overridden to plug in custom external view change handlers.
   * <p>NOTE: all change handlers will be run in a single thread, so any slow change handler can block other change
   * handlers from running. For slow change handler, make it asynchronous.
   *
   * @param spectatorHelixManager Spectator Helix manager
   * @return List of custom external view change handlers to plug in
   */
  @SuppressWarnings("unused")
  protected List<ClusterChangeHandler> getCustomExternalViewChangeHandlers(HelixManager spectatorHelixManager) {
    return Collections.emptyList();
  }

  /**
   * To be overridden to plug in custom instance config change handlers.
   * <p>NOTE: all change handlers will be run in a single thread, so any slow change handler can block other change
   * handlers from running. For slow change handler, make it asynchronous.
   *
   * @param spectatorHelixManager Spectator Helix manager
   * @return List of custom instance config change handlers to plug in
   */
  @SuppressWarnings("unused")
  protected List<ClusterChangeHandler> getCustomInstanceConfigChangeHandlers(HelixManager spectatorHelixManager) {
    return Collections.emptyList();
  }

  /**
   * To be overridden to plug in custom live instance change handlers.
   * <p>NOTE: all change handlers will be run in a single thread, so any slow change handler can block other change
   * handlers from running. For slow change handler, make it asynchronous.
   *
   * @param spectatorHelixManager Spectator Helix manager
   * @return List of custom live instance change handlers to plug in
   */
  @SuppressWarnings("unused")
  protected List<ClusterChangeHandler> getCustomLiveInstanceChangeHandlers(HelixManager spectatorHelixManager) {
    return Collections.emptyList();
  }

  public AccessControlFactory getAccessControlFactory() {
    return _accessControlFactory;
  }

  public HelixManager getSpectatorHelixManager() {
    return _spectatorHelixManager;
  }

  public HelixExternalViewBasedRouting getHelixExternalViewBasedRouting() {
    return _helixExternalViewBasedRouting;
  }

  public BrokerServerBuilder getBrokerServerBuilder() {
    return _brokerServerBuilder;
  }

  public MetricsRegistry getMetricsRegistry() {
    return _metricsRegistry;
  }

  public static HelixBrokerStarter getDefault()
      throws Exception {
    Configuration properties = new PropertiesConfiguration();
    int port = 5001;
    properties.addProperty(CommonConstants.Helix.KEY_OF_BROKER_QUERY_PORT, port);
    properties.addProperty(CommonConstants.Broker.CONFIG_OF_BROKER_TIMEOUT_MS, 30 * 1000L);
    return new HelixBrokerStarter(properties, "quickstart", "localhost:2122");
  }

  public static void main(String[] args)
      throws Exception {
    getDefault().start();
  }
}
