package org.opentcs.customadapter;


import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import com.google.inject.assistedinject.Assisted;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.logging.Logger;
import org.opentcs.components.kernel.services.PeripheralService;
import org.opentcs.components.kernel.services.PlantModelService;
import org.opentcs.components.kernel.services.VehicleService;
import org.opentcs.customizations.kernel.KernelExecutor;
import org.opentcs.data.model.Vehicle;

@Singleton
public class CommunicationStrategy
    implements
      CustomAdapterComponentsFactory {
  private static final Logger LOG = Logger.getLogger(CommunicationStrategy.class.getName());

  private final Map<String, Provider<StrategyCreator>> strategyProviders;
  private final VehicleConfigurationProvider configProvider;
  private final ScheduledExecutorService executor;
  private final PlantModelService plantModelService;
  private final VehicleService vehicleService;

  @Inject
  CommunicationStrategy(
      Map<String, Provider<StrategyCreator>> strategyProviders,
      @KernelExecutor
      ScheduledExecutorService executor,
      VehicleConfigurationProvider configProvider,
      PlantModelService plantModelService,
      VehicleService vehicleService,
      PeripheralService peripheralService
  ) {
    this.strategyProviders = strategyProviders;
    this.executor = executor;
    this.configProvider = new VehicleConfigurationProvider();
    this.plantModelService = plantModelService;
    this.vehicleService = vehicleService;
  }

//  @SuppressWarnings("checkstyle:TodoComment")
//  private void initializeStrategies() {
//    strategyProviders.put("ModbusTCP", (Provider<StrategyCreator>) new ModbusTCPStrategy());
//    // TODO: Add other strategies here
//  }

  @Override
  public CustomVehicleCommAdapter createCustomCommAdapter(@Assisted
  Vehicle vehicle, PeripheralService peripheralService) {
    VehicleConfiguration config = configProvider.getConfiguration(vehicle.getName());
    if (config == null) {
      config = new VehicleConfiguration("ModbusTCP", "192.168.0.72", 502, "", 0);
      configProvider.setConfiguration(vehicle.getName(), config);
    }

    String strategyKey = config.currentStrategy();
    Provider<StrategyCreator> creatorProvider = strategyProviders.get(strategyKey);
    if (creatorProvider == null) {
      LOG.warning("Unknown strategy: " + strategyKey + ". Using default ModbusTCP strategy.");
      creatorProvider = strategyProviders.get("ModbusTCP");
    }

    StrategyCreator creator = creatorProvider.get();
    return creator.createAdapter(vehicle, config, executor, plantModelService, peripheralService);
  }
}
