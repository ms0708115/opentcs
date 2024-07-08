package org.opentcs.customadapter;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.swing.BoxLayout;
import javax.swing.JComboBox;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JTextField;
import org.opentcs.customizations.kernel.KernelExecutor;
import org.opentcs.data.model.Vehicle;
import org.opentcs.drivers.vehicle.VehicleCommAdapter;
import org.opentcs.drivers.vehicle.VehicleCommAdapterDescription;
import org.opentcs.drivers.vehicle.VehicleCommAdapterFactory;

@Singleton
public class CustomCommunicationAdapterFactory
    implements
      VehicleCommAdapterFactory {

  private static final Logger LOG = Logger.getLogger(
      CustomCommunicationAdapterFactory.class.getName()
  );

  private final CustomAdapterComponentsFactory adapterFactory;
  private final VehicleConfigurationProvider configProvider;
  private boolean initialized;


  /**
   * CustomCommunicationAdapterFactory constructor.
   *
   * @param adapterFactory - an instance of CustomAdapterComponentsFactory to initialize the
   * adapterFactory field.
   * @param configProvider - an instance of VehicleConfigurationProvider to initialize the
   * configProvider field.
   */
  @Inject
  public CustomCommunicationAdapterFactory(
      CustomAdapterComponentsFactory adapterFactory, VehicleConfigurationProvider configProvider
  ) {
    this.adapterFactory = adapterFactory;
    this.configProvider = configProvider;
  }

  @Override
  public void initialize() {
    if (isInitialized()) {
      return;
    }
    LOG.info("Initializing CustomCommunicationAdapterFactory");
    configProvider.loadConfigurations();
    initialized = true;
  }

  @Override
  public boolean isInitialized() {
    return initialized;
  }

  @Override
  public void terminate() {
    if (!isInitialized()) {
      return;
    }
    LOG.info("Terminating CustomCommunicationAdapterFactory");
    configProvider.saveConfigurations();
    initialized = false;
  }

  @Override
  public VehicleCommAdapterDescription getDescription() {
    return new CustomVehicleCommAdapterDescription();
  }

  @Override
  public boolean providesAdapterFor(@Nonnull
  Vehicle vehicle) {
    LOG.fine("Checking if adapter is provided for vehicle: " + vehicle.getName());
    return true;
  }

  @Nullable
  @Override
  public VehicleCommAdapter getAdapterFor(@Nonnull
  Vehicle vehicle) {
    LOG.info("Creating adapter for vehicle: " + vehicle.getName());
    return adapterFactory.createCustomCommAdapter(vehicle);
  }

  private static class CustomVehicleCommAdapterDescription
      extends
        VehicleCommAdapterDescription {
    CustomVehicleCommAdapterDescription() {
    }

    @Override
    public String getDescription() {
      return "Custom Vehicle Communication Adapter";
    }

    @Override
    public boolean isSimVehicleCommAdapter() {
      return false;
    }
  }
}

@Singleton
class CommunicationStrategy
    implements
      CustomAdapterComponentsFactory {
  private static final Logger LOG = Logger.getLogger(CommunicationStrategy.class.getName());

  private final Map<String, StrategyCreator> strategies = new HashMap<>();
  private final VehicleConfigurationProvider configProvider;
  private final ScheduledExecutorService executor;

  @Inject
  CommunicationStrategy(
      @KernelExecutor
      ScheduledExecutorService executor,
      VehicleConfigurationProvider configProvider
  ) {
    this.executor = executor;
    this.configProvider = configProvider;
    initializeStrategies();
  }

  @SuppressWarnings("checkstyle:TodoComment")
  private void initializeStrategies() {
    strategies.put("ModbusTCP", new ModbusTCPStrategy());
    // TODO: Add other strategies here
  }

  @Override
  public CustomVehicleCommAdapter createCustomCommAdapter(Vehicle vehicle) {
    VehicleConfiguration config = configProvider.getConfiguration(vehicle.getName());
    if (config == null) {
      config = createConfigWithUserInput(vehicle);
      configProvider.setConfiguration(vehicle.getName(), config);
    }

    String strategyKey = config.getCurrentStrategy();
    StrategyCreator creator = strategies.get(strategyKey);
    if (creator == null) {
      LOG.warning("Unknown strategy: " + strategyKey + ". Using default ModbusTCP strategy.");
      creator = strategies.get("ModbusTCP");
    }

    return creator.createAdapter(vehicle, config, executor);
  }

  private VehicleConfiguration createConfigWithUserInput(Vehicle vehicle) {
    String defaultHost = "localhost";
    int defaultPort = 502;
    String defaultStrategy = "ModbusTCP";

    JTextField hostField = new JTextField(defaultHost, 10);
    JTextField portField = new JTextField(String.valueOf(defaultPort), 10);
    JComboBox<String> strategyComboBox = new JComboBox<>(
        strategies.keySet().toArray(new String[0])
    );
    strategyComboBox.setSelectedItem(defaultStrategy);

    JPanel panel = new JPanel();
    panel.setLayout(new BoxLayout(panel, BoxLayout.Y_AXIS));
    panel.add(new JLabel("Host:"));
    panel.add(hostField);
    panel.add(new JLabel("Port:"));
    panel.add(portField);
    panel.add(new JLabel("Communication Strategy:"));
    panel.add(strategyComboBox);

    int result = JOptionPane.showConfirmDialog(
        null, panel,
        "Enter Configuration for " + vehicle.getName(), JOptionPane.OK_CANCEL_OPTION
    );

    String host;
    int port;
    String strategy;

    if (result == JOptionPane.OK_OPTION) {
      host = hostField.getText().isEmpty() ? defaultHost : hostField.getText();
      try {
        port = Integer.parseInt(portField.getText());
        if (port < 0 || port > 65535) {
          LOG.warning("Invalid port number. Using default port " + defaultPort);
          port = defaultPort;
        }
      }
      catch (NumberFormatException e) {
        LOG.warning("Invalid port number. Using default port " + defaultPort);
        port = defaultPort;
      }
      strategy = (String) strategyComboBox.getSelectedItem();
      if (strategy == null || strategy.isEmpty()) {
        strategy = defaultStrategy;
      }
    }
    else {
      // Use default values if user cancels
      host = defaultHost;
      port = defaultPort;
      strategy = defaultStrategy;
    }

    return new VehicleConfiguration(strategy, host, port);
  }
}

interface StrategyCreator {
  CustomVehicleCommAdapter createAdapter(
      Vehicle vehicle, VehicleConfiguration config, ScheduledExecutorService executor
  );
}

class ModbusTCPStrategy
    implements
      StrategyCreator {
  ModbusTCPStrategy() {
  }

  @Override
  public CustomVehicleCommAdapter createAdapter(
      Vehicle vehicle, VehicleConfiguration config, ScheduledExecutorService executor
  ) {
    return new ModbusTCPVehicleCommAdapter(
        new CustomProcessModel(vehicle),
        "RECHARGE",
        1000,
        executor,
        config.getHost(),
        config.getPort()
    );
  }
}

@Singleton
class VehicleConfigurationProvider {
  private static final Logger LOG = Logger.getLogger(VehicleConfigurationProvider.class.getName());
  private static final String CONFIG_FILE = "vehicle_config.json";

  private final Map<String, VehicleConfiguration> configurations = new HashMap<>();

  VehicleConfigurationProvider() {
    loadConfigurations();
  }

  public VehicleConfiguration getConfiguration(String vehicleName) {
    return configurations.getOrDefault(vehicleName, null);
  }

  public void setConfiguration(String vehicleName, VehicleConfiguration config) {
    configurations.put(vehicleName, config);
    saveConfigurations();
  }

  public void loadConfigurations() {
    try (FileReader reader = new FileReader(CONFIG_FILE)) {
      Type type = new TypeToken<HashMap<String, VehicleConfiguration>>() {}.getType();
      Map<String, VehicleConfiguration> loadedConfigs = new Gson().fromJson(reader, type);
      if (loadedConfigs != null) {
        configurations.clear();
        configurations.putAll(loadedConfigs);
      }
      LOG.info("Configurations loaded successfully.");
    }
    catch (IOException e) {
      LOG.log(Level.WARNING, "Failed to load configurations. Using empty configuration.", e);
    }
  }

  public void saveConfigurations() {
    try (FileWriter writer = new FileWriter(CONFIG_FILE)) {
      new Gson().toJson(configurations, writer);
      LOG.info("Configurations saved successfully.");
    }
    catch (IOException e) {
      LOG.log(Level.SEVERE, "Failed to save configurations", e);
    }
  }
}

class VehicleConfiguration {
  private String currentStrategy;
  private String host;
  private int port;

  VehicleConfiguration() {
  }

  VehicleConfiguration(String currentStrategy, String host, int port) {
    this.currentStrategy = currentStrategy;
    this.host = host;
    this.port = port;
  }

  // Getters and setters
  public String getCurrentStrategy() {
    return currentStrategy;
  }

  public void setCurrentStrategy(String currentStrategy) {
    this.currentStrategy = currentStrategy;
  }

  public String getHost() {
    return host;
  }

  public void setHost(String host) {
    this.host = host;
  }

  public int getPort() {
    return port;
  }

  public void setPort(int port) {
    if (port < 0 || port > 65535) {
      throw new IllegalArgumentException("Invalid port number, port number range is 0 - 65535");
    }
    this.port = port;
  }
}
