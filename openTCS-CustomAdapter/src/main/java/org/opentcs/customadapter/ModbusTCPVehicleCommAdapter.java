package org.opentcs.customadapter;

import static java.util.Objects.requireNonNull;

import com.digitalpetri.modbus.master.ModbusTcpMaster;
import com.digitalpetri.modbus.master.ModbusTcpMasterConfig;
import com.digitalpetri.modbus.requests.ReadHoldingRegistersRequest;
import com.digitalpetri.modbus.requests.WriteMultipleRegistersRequest;
import com.digitalpetri.modbus.responses.ModbusResponse;
import com.digitalpetri.modbus.responses.ReadHoldingRegistersResponse;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.beans.PropertyChangeEvent;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiFunction;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.jgrapht.alg.util.Pair;
import org.opentcs.access.KernelRuntimeException;
import org.opentcs.components.kernel.services.PlantModelService;
import org.opentcs.customizations.kernel.KernelExecutor;
import org.opentcs.data.model.Path;
import org.opentcs.data.model.PlantModel;
import org.opentcs.data.model.Point;
import org.opentcs.data.model.Triple;
import org.opentcs.data.model.Vehicle;
import org.opentcs.data.order.TransportOrder;
import org.opentcs.data.peripherals.PeripheralOperation;
import org.opentcs.drivers.vehicle.LoadHandlingDevice;
import org.opentcs.drivers.vehicle.MovementCommand;
import org.opentcs.drivers.vehicle.VehicleProcessModel;
import org.opentcs.drivers.vehicle.management.VehicleProcessModelTO;
import org.opentcs.util.ExplainedBoolean;

public class ModbusTCPVehicleCommAdapter
    extends
      CustomVehicleCommAdapter {
  /**
   * The name of the load handling device set by this adapter.
   */
  public static final String LHD_NAME = "default";
  /**
   * This class's Logger.
   */
  private static final Logger LOG = Logger.getLogger(ModbusTCPVehicleCommAdapter.class.getName());
  /**
   * An error code indicating that there's a conflict between a load operation and the vehicle's
   * current load state.
   */
  private static final String LOAD_OPERATION_CONFLICT = "cannotLoadWhenLoaded";
  /**
   * An error code indicating that there's a conflict between an unload operation and the vehicle's
   * current load state.
   */
  private static final String UNLOAD_OPERATION_CONFLICT = "cannotUnloadWhenNotLoaded";
  /**
   * MAP2: station name -> <CMD1, CMD2>.
   */
  private final Map<Long, Pair<CMD1, CMD2>> stationCommandsMap = new ConcurrentHashMap<>();
  private final Map<Long, String> positionMap;
  private final List<MovementCommand> allMovementCommands = new ArrayList<>();
  private final List<ModbusCommand> positionModbusCommand = new ArrayList<>();
  private final List<ModbusCommand> cmdModbusCommand = new ArrayList<>();
  private TransportOrder currentTransportOrder;
  /**
   * Represents a vehicle associated with a ModbusTCPVehicleCommAdapter.
   */
  private final Vehicle vehicle;
  /**
   * The vehicle's load state.
   */
  private LoadState loadState = LoadState.EMPTY;
  /**
   * The host address for the TCP connection.
   */
  private final String host;
  /**
   * The port number for the TCP connection.
   */
  private final int port;
  /**
   * Indicates whether the vehicle is currently connected.
   */
  private boolean isConnected;
  /**
   * Represents a Modbus TCP master used for communication with Modbus TCP devices.
   */
  private ModbusTcpMaster heartbeatMaster;
  private ModbusTcpMaster statusMaster;
  private ModbusTcpMaster taskMaster;
  /**
   * Represents the state of a variable indicating whether it has been initialized.
   */
  private boolean initialized;
  private final AtomicBoolean heartBeatToggle = new AtomicBoolean(false);
  private ScheduledFuture<?> heartBeatFuture;
  private PositionUpdater positionUpdater;
  private final PlantModelService plantModelService;
  private final VehicleConfigurationProvider configProvider;
  private final PriorityBlockingQueue<ModbusRequest> requestQueue = new PriorityBlockingQueue<>();
  private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
  private final ByteBufAllocator allocator = PooledByteBufAllocator.DEFAULT;

  /**
   * A communication adapter for ModbusTCP-based vehicle communication.
   * <p>
   * Allows communication between the vehicle and the control system using the ModbusTCP protocol.
   *
   * @param executor The executor for handling background tasks.
   * @param vehicle The vehicle associated with this communication adapter.
   * @param host The host IP address for the ModbusTCP connection.
   * @param port The port number for the ModbusTCP connection.
   * @param plantModelService The plant model service for accessing plant model information.
   */
  @SuppressWarnings("checkstyle:TodoComment")

  @Inject
  public ModbusTCPVehicleCommAdapter(
      @KernelExecutor
      ScheduledExecutorService executor,
      @Assisted
      Vehicle vehicle,
      String host,
      int port,
      PlantModelService plantModelService
  ) {
    super(new CustomProcessModel(vehicle), "RECHARGE", 1000, executor);
    LOG.warning(String.format("HIIIIIIIIIIIIIIIIIIII"));
    this.vehicle = requireNonNull(vehicle, "vehicle");
    this.configProvider = new VehicleConfigurationProvider();
    this.host = configProvider.getConfiguration(vehicle.getName()).host();
    this.port = configProvider.getConfiguration(vehicle.getName()).port();
    LOG.warning(String.format("DEVICE HOST:%s, PORT: %d", this.host, this.port));
    this.plantModelService = requireNonNull(plantModelService, "plantModelService");
    this.isConnected = false;
    this.currentTransportOrder = null;
    this.positionMap = new HashMap<>();
  }

  @Override
  public void initialize() {
    if (isInitialized()) {
      LOG.warning("Device has been initialized");
      return;
    }
    super.initialize();
    getProcessModel().setState(Vehicle.State.IDLE);
    LOG.warning("Device has been set to IDLE state");
    ((ExecutorService) getExecutor()).submit(() -> getProcessModel().setPosition("Point-0026"));
    LOG.warning("Device has been set to Point-0026");
    getProcessModel().setLoadHandlingDevices(
        List.of(new LoadHandlingDevice(LHD_NAME, false))
    );
    LOG.warning("Device has set load handling device");
    initializePositionMap();
    this.positionUpdater = new PositionUpdater(getProcessModel(), getExecutor());
    getExecutor().submit(this::processRequests);
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
    super.terminate();
    positionUpdater.stopPositionUpdates();
    stopHeartBeat();
    initialized = false;// Stop the heartbeat mechanism
  }

  private void startHeartbeat() {
    heartBeatFuture = getExecutor().scheduleAtFixedRate(() -> {
      boolean currentValue = heartBeatToggle.getAndSet(!heartBeatToggle.get());
      ModbusRequest writeRequest = new ModbusRequest(
          new WriteMultipleRegistersRequest(
              100, 1, Unpooled.buffer(2).writeShort(currentValue ? 1 : 0)
          ),
          100,
          1
      );
      requestQueue.offer(writeRequest);

      ModbusRequest readRequest = new ModbusRequest(
          new ReadHoldingRegistersRequest(100, 1),
          100,
          2
      );
      requestQueue.offer(readRequest);
    }, 0, 500, TimeUnit.MILLISECONDS);
  }

  private void stopHeartBeat() {
    if (heartBeatFuture != null && !heartBeatFuture.isCancelled()) {
      heartBeatFuture.cancel(true);
    }
  }

  private void initializePositionMap() {
    try {
      PlantModel plantModel = plantModelService.getPlantModel();
      for (Point point : plantModel.getPoints()) {
        String positionName = point.getName();
        Long precisePosition = point.getPose().getPosition().getX();
        positionMap.put(precisePosition, positionName);
      }

      LOG.info("Position map initialized with " + positionMap.size() + " entries.");
    }
    catch (KernelRuntimeException e) {
      LOG.severe("Failed to initialize position map: " + e.getMessage());
    }
  }

  /**
   * Retrieves the position associated with the given precise position.
   *
   * @param precisePosition The precise position for which to retrieve the position.
   * @return The position associated with the given precise position. Returns the last known
   * position
   * if no position is found in the map.
   */
  public String getPositionFromMap(Long precisePosition) {
    String position = positionMap.get(precisePosition);
    if (position != null) {
      positionUpdater.lastKnownPosition = position;
      getProcessModel().setPosition(position);
      return position;
    }
    else {
      return positionUpdater.lastKnownPosition;
    }
  }

  /**
   * Processes updates of the {@link CustomProcessModel}.
   *
   * <p><em>Overriding methods should also call this.</em></p>
   *
   * @param evt The property change event published by the model.
   */
  @SuppressWarnings("checkstyle:TodoComment")
  @Override
  public void propertyChange(PropertyChangeEvent evt) {
    super.propertyChange(evt);

    if (!((evt.getSource()) instanceof CustomProcessModel)) {
      return;
    }
    if (Objects.equals(
        evt.getPropertyName(),
        VehicleProcessModel.Attribute.LOAD_HANDLING_DEVICES.name()
    )) {
      if (!getProcessModel().getLoadHandlingDevices().isEmpty()
          && getProcessModel().getLoadHandlingDevices().getFirst().isFull()) {
        loadState = LoadState.FULL;
        // TODO: need change vehicle model size in future.
//        getProcessModel().setLength(configuration.vehicleLengthLoaded());
      }
      else {
        loadState = LoadState.EMPTY;
//        getProcessModel().setLength(configuration.vehicleLengthUnloaded());
      }
    }
  }

  /**
   * Enables the communication adapter.
   */
  @Override
  public synchronized void enable() {
    LOG.warning("Attempting to enable ModbusTCPVehicleCommAdapter for " + getName());
    if (isEnabled()) {
      LOG.warning("ModbusTCPVehicleCommAdapter for " + getName() + " is already enabled");
      return;
    }
    try {
      super.enable();
      LOG.warning("ModbusTCPVehicleCommAdapter for " + getName() + " enabled successfully");
    }
    catch (Exception e) {
      LOG.severe(
          "Error enabling ModbusTCPVehicleCommAdapter for " + getName() + ": " + e.getMessage()
      );
      e.printStackTrace();
    }
  }

  /**
   * Disables the communication adapter.
   *
   * <p>If the adapter is already disabled, the method returns without doing anything.
   * When disabling the adapter, the vehicle's connection to the adapter is terminated,
   * the adapter is marked as disabled, and the vehicle's state is set to UNKNOWN.</p>
   *
   * <p>This method is synchronized to ensure that only one thread can disable the adapter
   * at a time.</p>
   *
   * @see CustomVehicleCommAdapter#disable()
   */
  @Override
  public synchronized void disable() {
    if (!isEnabled()) {
      return;
    }

    getExecutor().shutdownNow();
    try {
      getExecutor().awaitTermination(5, TimeUnit.SECONDS);
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    stationCommandsMap.clear();
    positionMap.clear();
    allMovementCommands.clear();
    positionModbusCommand.clear();
    cmdModbusCommand.clear();
    currentTransportOrder = null;

    super.disable();
  }

  /**
   * Retrieves the custom process model associated with the vehicle.
   *
   * @return The custom process model.
   */
  @Override
  @Nonnull
  public CustomProcessModel getProcessModel() {
    return (CustomProcessModel) super.getProcessModel();
  }

  @Override
  protected void sendSpecificCommand(MovementCommand cmd) {
    if (!isVehicleConnected()) {
      LOG.warning("Not connected to Modbus TCP server. Cannot send command.");
      return;
    }

    if (!allMovementCommands.contains(cmd)) {
      LOG.warning(String.format("%s: Command is NOT in MovementCommands pool.", getName()));
      // open this comment back after testing.
      getProcessModel().commandFailed(cmd);
    }
  }

  @Override
  public synchronized boolean enqueueCommand(MovementCommand newCommand) {
    requireNonNull(newCommand, "newCommand cannot be empty");

    if (!canAcceptNextCommand()) {
      return false;
    }

    checkTransportOrderAndLog(newCommand);
    queueNewCommand(newCommand);

    if (newCommand.isFinalMovement()) {
      processAllMovementCommands();
      allMovementCommands.clear();
    }
    return true;
  }

  private void checkTransportOrderAndLog(MovementCommand newCommand) {
    if (currentTransportOrder == null || !currentTransportOrder.equals(
        newCommand.getTransportOrder()
    )) {
      LOG.info(
          String.format(
              "New Transport order (%s) has received.", newCommand.getTransportOrder().getName()
          )
      );
      currentTransportOrder = newCommand.getTransportOrder();
      allMovementCommands.clear();
    }
  }

  private void queueNewCommand(MovementCommand newCommand) {
    allMovementCommands.add(newCommand);

    LOG.info(
        String.format(
            "%s: Custom Adding command: , Current Movement Pool Size: %d", getName(),
            allMovementCommands.size()
        )
    );

    getUnsentCommands().add(newCommand);
    getProcessModel().commandEnqueued(newCommand);
  }

  private void processAllMovementCommands() {
    LOG.info(
        String.format(
            "Processing all movement commands for TransportOrder: %s", currentTransportOrder
                .getName()
        )
    );
    convertMovementCommandsToModbusCommands(allMovementCommands);
    writeAllModbusCommands();
    allMovementCommands.clear();
    currentTransportOrder = null;
  }

  /**
   * Converts a list of MovementCommand objects to a list of ModbusCommand objects.
   *
   * @param commands The list of MovementCommand objects to be converted.
   */
  private void convertMovementCommandsToModbusCommands(
      List<MovementCommand> commands
  ) {
    stationCommandsMap.clear();
    positionModbusCommand.clear();
    cmdModbusCommand.clear();
    processMovementCommands(commands);
    int positionBaseAddress = 1000;
    int cmdBaseAddress = 1200;

    // Convert stationCommandsMap to ModbusCommand list
    for (Map.Entry<Long, Pair<CMD1, CMD2>> entry : stationCommandsMap.entrySet()) {
      long stationPosition = entry.getKey();
      Pair<CMD1, CMD2> cmds = entry.getValue();
      positionModbusCommand.add(
          new ModbusCommand(
              "POSITION", (int) stationPosition, positionBaseAddress,
              ModbusCommand.DataFormat.DECIMAL
          )
      );
      cmdModbusCommand.add(
          new ModbusCommand(
              "CMD1", cmds.getFirst().toShort(), cmdBaseAddress,
              ModbusCommand.DataFormat.HEXADECIMAL
          )
      );
      cmdModbusCommand.add(
          new ModbusCommand(
              "CMD2", cmds.getSecond().toShort(), cmdBaseAddress + 1,
              ModbusCommand.DataFormat.HEXADECIMAL
          )
      );

      positionBaseAddress += 2;
      cmdBaseAddress += 2;
    }
  }

  /**
   * Processes the given list of movement commands.
   *
   * @param commands The list of movement commands to be processed.
   */
  private void processMovementCommands(List<MovementCommand> commands) {
    long startPosition;
    for (MovementCommand cmd : commands) {
      // If step doesn't have source point,
      // that means vehicle is start from the destination, no need to move.
      if (cmd.getStep().getSourcePoint() == null) {
        LOG.warning(String.format("Vehicle %s don't need to move actually.", vehicle.getName()));
        continue;
      }
      LOG.info(
          String.format(
              "Vehicle %s got movements at Source point %s.",
              vehicle.getName(),
              cmd.getStep().getSourcePoint().getName()
          )
      );
      startPosition = cmd.getStep().getSourcePoint().getPose().getPosition().getX();

      Pair<CMD1, CMD2> pairCommands = new Pair<>(createCMD1(cmd), createCMD2(cmd));
      stationCommandsMap.put(startPosition, pairCommands);
    }
  }

  @SuppressWarnings("checkstyle:TodoComment")
  private CMD1 createCMD1(MovementCommand cmd) {
    int liftCmd = 0;
    int speedLevel = 0;
    int obstacleSensor = 5;
    double maxSpeed = 0;
    String command = cmd.getFinalOperation();
    liftCmd = getLiftCommand(command);
    if (cmd.getStep().getPath() != null) {
      maxSpeed = getMaxAllowedSpeed(cmd.getStep().getPath());
    }
    int speedCase = (int) (maxSpeed * 5);
    speedLevel = switch (speedCase) {
      case 0 -> 1;
      case 3 -> 2;
      case 6 -> 3;
      case 9 -> 4;
      case 12 -> 5;
      default -> 1;
    };
    return new CMD1(
        liftCmd, speedLevel, obstacleSensor, 0
    );
    // cmd.getStep().getVehicleOrientation()
    //            == Vehicle.Orientation.FORWARD ? 0 : 1
  }

  private CMD2 createCMD2(MovementCommand cmd) {
    String switchOperation = "";
    // TODO: change liftHeight according to the device.
    int liftHeight = 0;
    int motionCommand = 0;
    List<PeripheralOperation> peripheralOperations = null;

    if (cmd.getStep().getPath() != null) {
      peripheralOperations = cmd.getStep().getPath().getPeripheralOperations();
    }
    if (peripheralOperations != null && !peripheralOperations.isEmpty()) {
      switchOperation = peripheralOperations.getFirst().getOperation();
    }
    motionCommand = switch (switchOperation) {
      case "switchLeft" -> 1;
      case "switchRight" -> 2;
      default -> 0;
    };
    return new CMD2(liftHeight, motionCommand);
  }

  private double getMaxAllowedSpeed(Path path) {
    double processModelMaxFwdVelocity = getProcessModel().getMaxFwdVelocity();
    double maxPathVelocity = path.getMaxVelocity();
    return Math.min(processModelMaxFwdVelocity, maxPathVelocity);
  }

  private static int getLiftCommand(String command) {
    return switch (command) {
      case "Load" -> 2;
      case "Unload" -> 1;
      default -> 1;
    };
  }

  private CompletableFuture<Void> writeAllModbusCommands() {
    return writeSingleRegister(108, 0)
        .thenCompose(v -> {
          if (!positionModbusCommand.isEmpty()) {
            try {
              return writeModbusCommands(positionModbusCommand, "Position");
            }
            catch (ExecutionException | InterruptedException e) {
              throw new RuntimeException(e);
            }
          }
          else {
            return CompletableFuture.completedFuture(null);
          }
        })
        .thenCompose(v -> {
          if (!cmdModbusCommand.isEmpty()) {
            try {
              return writeModbusCommands(cmdModbusCommand, "CMD");
            }
            catch (ExecutionException | InterruptedException e) {
              throw new RuntimeException(e);
            }
          }
          else {
            return CompletableFuture.completedFuture(null);
          }
        })
        .thenCompose(v -> writeSingleRegister(108, 1))
        .thenCompose(v -> readAndVerifyCommands())
        .exceptionally(ex -> {
          LOG.severe("Error in writeAllModbusCommands: " + ex.getMessage());
          return null;
        });
  }

  private CompletableFuture<Void> writeSingleRegister(int address, int value) {
    ByteBuf buffer = Unpooled.buffer(2);
    buffer.writeShort(value);
    WriteMultipleRegistersRequest request = new WriteMultipleRegistersRequest(address, 1, buffer);

    return sendModbusRequest(request, address)
        .thenAccept(response -> {
          LOG.info("Successfully wrote register at address " + address + " with value " + value);
        })
        .exceptionally(ex -> {
          LOG.severe("Failed to write register at address " + address + ": " + ex.getMessage());
          return null;
        })
        .whenComplete((v, ex) -> buffer.release());
  }

  private CompletableFuture<Integer> readSingleRegister(int address) {
    ReadHoldingRegistersRequest request = new ReadHoldingRegistersRequest(address, 1);
    return sendModbusRequest(request, address)
        .thenApply(response -> {
          if (response instanceof ReadHoldingRegistersResponse) {
            ReadHoldingRegistersResponse readResponse = (ReadHoldingRegistersResponse) response;
            ByteBuf responseBuffer = readResponse.getRegisters();
            try {
              int result = responseBuffer.readUnsignedShort();
              LOG.info(String.format("READ ADDRESS %d GOT %d", address, result));
              return result;
            }
            finally {
              responseBuffer.release();
            }
          }
          throw new RuntimeException("Invalid response type");
        });
  }

  private CompletableFuture<Long> readDWordRegister(int startAddress) {
    return sendModbusRequest(new ReadHoldingRegistersRequest(startAddress, 2), startAddress)
        .thenApply(response -> {
          if (response instanceof ReadHoldingRegistersResponse readResponse) {
            ByteBuf registers = readResponse.getRegisters();
            try {
              // Read two 16-bit registers and combine into a 32-bit integer
              int lowWord = registers.readUnsignedShort();
              int highWord = registers.readUnsignedShort();
              // Use little endian combination
              return (long) (highWord << 16 | lowWord);
            }
            finally {
              registers.release();
            }
          }
          else {
            throw new IllegalArgumentException("Unexpected response type");
          }
        });
  }

  private CompletableFuture<Void> readAndVerifyCommands() {
    List<CompletableFuture<Boolean>> readFutures = cmdModbusCommand.stream()
        .map(this::readAndCompareCommand)
        .toList();
    CompletableFuture<?>[] futuresArray = readFutures.toArray(new CompletableFuture<?>[0]);

    return CompletableFuture.allOf(futuresArray)
        .thenRun(() -> {
          boolean allVerified = readFutures.stream().allMatch(future -> {
            try {
              return future.get();
            }
            catch (InterruptedException | ExecutionException e) {
              LOG.severe("Error while verifying command: " + e.getMessage());
              return false;
            }
          });
          if (!allVerified) {
            throw new CompletionException(new RuntimeException("Verification failed, retrying..."));
          }
        })
        .exceptionally(ex -> {
          LOG.warning("Verification failed, retrying: " + ex.getMessage());
          return writeAllModbusCommands().thenRun(() -> {}).join();
        });
  }

  private CompletableFuture<Boolean> readAndCompareCommand(ModbusCommand command) {
    ReadHoldingRegistersRequest request = new ReadHoldingRegistersRequest(command.address(), 1);
    return sendModbusRequest(request, command.address())
        .thenApply(response -> {
          if (response instanceof ReadHoldingRegistersResponse readResponse) {
            ByteBuf registers = readResponse.getRegisters();
            try {
              if (registers.readableBytes() >= 2) {
                int value = registers.readUnsignedShort();
                boolean matches = value == command.value();
                LOG.info(
                    String.format(
                        "Read and verified command at address %d: expected %d, got %d",
                        command.address(), command.value(), value
                    )
                );
                return matches;
              }
              else {
                LOG.warning("Insufficient data returned for address " + command.address());
                return false;
              }
            }
            finally {
              registers.release();
            }
          }
          else {
            LOG.warning("Unexpected response type for address " + command.address());
            return false;
          }
        })
        .exceptionally(ex -> {
          LOG.severe(
              "Failed to read and compare command at address " + command.address() + ": " + ex
                  .getMessage()
          );
          return false;
        });
  }

  /**
   * Writes Modbus commands in batches to the specified command type.
   *
   * @param commands The list of ModbusCommand objects to be written.
   * @param commandType The type of the command being written.
   * @return A CompletableFuture representing the completion of the write operation.
   * @throws ExecutionException If an execution error occurs during the write operation.
   * @throws InterruptedException If the write operation is interrupted.
   */
  private CompletableFuture<Void> writeModbusCommands(
      List<ModbusCommand> commands,
      String commandType
  )
      throws ExecutionException,
        InterruptedException {
    commands.sort(Comparator.comparingInt(ModbusCommand::address));
    CompletableFuture<Void> futureChain = CompletableFuture.completedFuture(null);
    List<ModbusCommand> batch = new ArrayList<>();
    int batchWordSize = 0;
    int startAddress = commands.getFirst().address();
    int maxBatchSize = 120;

    for (ModbusCommand command : commands) {
      // Get the word size of the command
      int commandWordSize = getCommandWordSize(command);
      // If the word size of the current batch plus the new command exceeds the limit,
      // write the current batch and start a new batch
      if (batchWordSize + commandWordSize >= maxBatchSize) {
        // Write the current batch into the chain call
        int finalStartAddress = startAddress;
        List<ModbusCommand> finalBatch = new ArrayList<>(batch);
        futureChain = futureChain.thenCompose(
            v -> writeBatch(
                finalBatch, finalStartAddress, commandType
            )
        );
        batch.clear();
        startAddress = command.address() + commandWordSize;
        batchWordSize = 0;
      }
      batch.add(command);
      batchWordSize += commandWordSize;
    }

    // Write the last batch if it's not empty
    if (!batch.isEmpty()) {
      int finalStartAddress = startAddress;
      List<ModbusCommand> finalBatch = new ArrayList<>(batch);
      futureChain = futureChain.thenCompose(
          v -> writeBatch(finalBatch, finalStartAddress, commandType)
      );
    }

    return futureChain;
  }

  private int getCommandWordSize(ModbusCommand command) {
    return command.format() == ModbusCommand.DataFormat.DECIMAL ? 2 : 1;
  }

  private CompletableFuture<Void> writeBatch(
      List<ModbusCommand> batch, int startAddress, String commandType
  ) {
    ByteBuf values = Unpooled.buffer();

    try {
      for (ModbusCommand command : batch) {
        if (command.format() == ModbusCommand.DataFormat.DECIMAL) {
          values.writeIntLE(command.value());
        }
        else {
          values.writeShortLE(command.value());
        }
        LOG.info("Writing " + commandType + " command: " + command.toLogString());
      }

      WriteMultipleRegistersRequest request = new WriteMultipleRegistersRequest(
          startAddress,
          values.readableBytes() / 2,  // Convert bytes to word count
          values
      );

      return sendModbusRequest(request, startAddress)
          .thenAccept(
              response -> LOG.info(
                  "Successfully wrote " + batch.size() + " " + commandType
                      + " registers starting at address " + startAddress
              )
          )
          .exceptionally(ex -> {
            LOG.severe("Failed to write " + commandType + " registers: " + ex.getMessage());
            return null;
          });
    }
    finally {
      values.release();
    }
  }

  @Override
  protected boolean performConnection() {
    LOG.info("Connecting to Modbus TCP server at " + host + ":" + port);

    CompletableFuture<ModbusTcpMaster> heartbeatFuture = createMaster("Heartbeat");
    CompletableFuture<ModbusTcpMaster> statusFuture = createMaster("Status");
    CompletableFuture<ModbusTcpMaster> taskFuture = createMaster("Task");

    try {
      CompletableFuture.allOf(heartbeatFuture, statusFuture, taskFuture).get(30, TimeUnit.SECONDS);

      this.isConnected = true;
      LOG.info("Successfully connected all Modbus TCP masters");
      getProcessModel().setCommAdapterConnected(true);
      startHeartbeat();
      LOG.warning("Starting sending heart bit.");
      positionUpdater.startPositionUpdates();
      LOG.warning("Starting updating position");

      return true;
    }
    catch (Exception ex) {
      LOG.log(Level.SEVERE, "Failed to connect to Modbus TCP server", ex);
      this.isConnected = false;
      return false;
    }
  }

  private CompletableFuture<ModbusTcpMaster> createMaster(String type) {
    ModbusTcpMasterConfig config = new ModbusTcpMasterConfig.Builder(host)
        .setPort(port)
        .build();

    return CompletableFuture.supplyAsync(() -> {
      LOG.info("Creating new ModbusTcpMaster instance for " + type);
      return new ModbusTcpMaster(config);
    }).thenCompose(newMaster -> {
      switch (type) {
        case "Heartbeat":
          heartbeatMaster = newMaster;
          break;
        case "Status":
          statusMaster = newMaster;
          break;
        case "Task":
          taskMaster = newMaster;
          break;
        default:
          throw new IllegalArgumentException("Invalid type: " + type);
      }
      return newMaster.connect();
    }).exceptionally(ex -> {
      LOG.severe("Failed to create or connect " + type + " master: " + ex.getMessage());
      throw new CompletionException(ex);
    });
  }

  private ModbusTcpMaster selectAppropriatesMaster(int address) {
    return switch (address) {
      case 100 -> heartbeatMaster;
      case 110 -> statusMaster;
      default -> taskMaster;
    };
  }

//  @Override
//  protected boolean performDisconnection() {
//    LOG.info("Disconnecting from Modbus TCP server");
//    if (master != null) {
//      return master.disconnect()
//          .thenRun(() -> {
//            LOG.info("Successfully disconnected from Modbus TCP server");
//            this.isConnected = false;
//            getProcessModel().setCommAdapterConnected(false);
//            this.master = null;
//          })
//          .exceptionally(ex -> {
//            LOG.log(Level.SEVERE, "Failed to disconnect from Modbus TCP server", ex);
//            return null;
//          })
//          .isDone();
//    }
//    return true;
//  }

  @Override
  protected boolean performDisconnection() {
    LOG.info("Disconnecting from Modbus TCP servers");

    // Stop the heartbeat mechanism
    if (heartBeatFuture != null) {
      heartBeatFuture.cancel(true);
      heartBeatFuture = null;
    }

    // Stop position updates
    if (positionUpdater != null) {
      positionUpdater.stopPositionUpdates();
    }

    // Clear any pending requests
    requestQueue.clear();

    // Function to handle disconnect for each master
    BiFunction<ModbusTcpMaster, String, CompletableFuture<Void>> disconnectMaster = (
        master, masterName
    ) -> {
      if (master != null) {
        return master.disconnect()
            .thenRun(() -> {
              LOG.info("Successfully disconnected from " + masterName);
              getProcessModel().setCommAdapterConnected(false);
            })
            .exceptionally(ex -> {
              LOG.log(Level.SEVERE, "Failed to disconnect from " + masterName, ex);
              return null;
            });
      }
      return CompletableFuture.completedFuture(null);
    };

    // Disconnect all masters
    CompletableFuture<Void> disconnectHeartbeatMaster = disconnectMaster.apply(
        heartbeatMaster, "Heartbeat Master"
    );
    CompletableFuture<Void> disconnectStatusMaster = disconnectMaster.apply(
        statusMaster, "Status Master"
    );
    CompletableFuture<Void> disconnectTaskMaster = disconnectMaster.apply(
        taskMaster, "Task Master"
    );

    // Combine all futures and wait for all to complete
    CompletableFuture<Void> allDisconnections = CompletableFuture.allOf(
        disconnectHeartbeatMaster,
        disconnectStatusMaster,
        disconnectTaskMaster
    );

    try {
      allDisconnections.get(30, TimeUnit.SECONDS); // Wait for all disconnections to complete
      this.isConnected = false;
      this.heartbeatMaster = null;
      this.statusMaster = null;
      this.taskMaster = null;
      LOG.info("All masters disconnected successfully");
      return true;
    }
    catch (InterruptedException | ExecutionException | TimeoutException e) {
      LOG.log(Level.SEVERE, "Failed to disconnect one or more masters", e);
      return false;
    }
  }


  @Override
  protected boolean isVehicleConnected() {
    return isConnected && heartbeatMaster != null && statusMaster != null && taskMaster != null;
  }

  private void processRequests() {
    while (!Thread.currentThread().isInterrupted()) {
      try {
        ModbusRequest request = requestQueue.take();
        processRequest(request);
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  private void processRequest(ModbusRequest request) {
    ByteBuf buffer = allocator.buffer();
    try {
      if (request.isReadOperation()) {
        rwLock.readLock().lock();
        try {
          sendModbusRequest(request.getRequest(), request.getAddress());
        }
        finally {
          rwLock.readLock().unlock();
        }
      }
      else {
        rwLock.writeLock().lock();
        try {
          sendModbusRequest(request.getRequest(), request.getAddress());
        }
        finally {
          rwLock.writeLock().unlock();
        }
      }
    }
    finally {
      buffer.release();
    }
  }

  private CompletableFuture<ModbusResponse> sendModbusRequest(
      com.digitalpetri.modbus.requests.ModbusRequest request,
      int address
  ) {
    ModbusTcpMaster master = selectAppropriatesMaster(address);
    if (master == null) {
      return CompletableFuture.failedFuture(
          new IllegalStateException("Modbus master is not initialized")
      );
    }

    return CompletableFuture.supplyAsync(() -> {
      try {
        return master.sendRequest(request, 0).get(5, TimeUnit.SECONDS);
      }
      catch (InterruptedException | ExecutionException | TimeoutException e) {
        Thread.currentThread().interrupt();
        throw new CompletionException("Request interrupted", e);
      }
    }, getExecutor()).thenApply(response -> {
      if (response instanceof ReadHoldingRegistersResponse) {
        try {
          return response;
        }
        finally {
          ((ReadHoldingRegistersResponse) response).getRegisters().release();
        }
      }
      return response;
    }).exceptionally(ex -> {
      LOG.severe("Failed to send Modbus request: " + ex.getMessage());
      return retrySendModbusRequest(request, 3, 1000, address).join();
    });
  }

  private CompletableFuture<ModbusResponse> retrySendModbusRequest(
      com.digitalpetri.modbus.requests.ModbusRequest request,
      int retries,
      long delay,
      int address
  ) {
    if (retries <= 0) {
      return CompletableFuture.failedFuture(new RuntimeException("Max retries reached"));
    }

    return CompletableFuture.supplyAsync(() -> {
      try {
        Thread.sleep(delay);
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new CompletionException(e);
      }
      return sendModbusRequest(request, address);
    }, getExecutor()).thenCompose(future -> future.handle((response, ex) -> {
      if (ex != null) {
        LOG.warning("Retry failed: " + ex.toString() + ". Retries left: " + (retries - 1));
        return retrySendModbusRequest(request, retries - 1, delay * 2, address);
      }
      return CompletableFuture.completedFuture(response);
    })).thenCompose(innerFuture -> innerFuture);
  }


  @Override
  @Nonnull
  public synchronized ExplainedBoolean canProcess(
      @Nonnull
      TransportOrder order
  ) {
    requireNonNull(order, "order");

    return canProcess(
        order.getFutureDriveOrders().stream()
            .map(driveOrder -> driveOrder.getDestination().getOperation())
            .collect(Collectors.toList())
    );
  }

  private ExplainedBoolean canProcess(List<String> operations) {
    requireNonNull(operations, "operations");

    LOG.info(String.format("%s: Checking process ability of %s...", getName(), operations));
    boolean canProcess = true;
    String reason = "";

    // Do NOT require the vehicle to be IDLE or CHARGING here!
    // That would mean a vehicle moving to a parking position or recharging location would always
    // have to finish that order first, which would render a transport order's dispensable flag
    // useless.
    boolean loaded = loadState == LoadState.FULL;
    Iterator<String> opIter = operations.iterator();
    while (canProcess && opIter.hasNext()) {
      final String nextOp = opIter.next();
      // If we're loaded, we cannot load another piece, but could unload.
      if (loaded) {
        if (nextOp.startsWith(getProcessModel().getLoadOperation())) {
          canProcess = false;
          reason = LOAD_OPERATION_CONFLICT;
        }
        else if (nextOp.startsWith(getProcessModel().getUnloadOperation())) {
          loaded = false;
        }
      } // If we're not loaded, we could load, but not unload.
      else if (nextOp.startsWith(getProcessModel().getLoadOperation())) {
        loaded = true;
      }
      else if (nextOp.startsWith(getProcessModel().getUnloadOperation())) {
        canProcess = false;
        reason = UNLOAD_OPERATION_CONFLICT;
      }
    }
    if (!canProcess) {
      LOG.info(String.format("%s: Cannot process %s, reason: '%s'", getName(), operations, reason));
    }
    return new ExplainedBoolean(canProcess, reason);
  }

  @Override
  public void processMessage(@Nullable
  Object message) {
    LOG.info("Received message: " + message);
    // Implement specific message processing logic
  }

  /**
   * null.
   */
  @Override
  protected void updateVehiclePosition(String position) {
    getProcessModel().setPosition(position);
  }

  /**
   * null.
   */
  @Override
  protected void updateVehicleState() {

  }

  @Override
  protected VehicleProcessModelTO createCustomTransferableProcessModel() {
    return new CustomProcessModelTO()
        .setLoadOperation(getProcessModel().getLoadOperation())
        .setUnloadOperation(getProcessModel().getUnloadOperation())
        .setMaxAcceleration(getProcessModel().getMaxAcceleration())
        .setMaxDeceleration(getProcessModel().getMaxDecceleration())
        .setMaxFwdVelocity(getProcessModel().getMaxFwdVelocity())
        .setMaxRevVelocity(getProcessModel().getMaxRevVelocity());
  }

  private record ModbusCommand(String name, int value, int address, DataFormat format) {
    public enum DataFormat {
      DECIMAL,
      HEXADECIMAL
    }

    public ModbusCommand {
      if (value < 0) {
        throw new IllegalArgumentException("Value cannot be negative");
      }
    }

    /**
     * Returns the string representation of the value based on its format.
     *
     * @return A string representation of the value.
     */
    public String getFormattedValue() {
      return switch (format) {
        case DECIMAL -> String.valueOf(value);
        case HEXADECIMAL -> String.format("%04X", value);
      };
    }

    public String toLogString() {
      return String.format(
          "ModbusCommand{name='%s', value=%s, address=%d, format=%s}",
          name, getFormattedValue(), address, format
      );
    }
  }

  /**
   * The vehicle's possible load states.
   */
  private enum LoadState {
    EMPTY,
    FULL;
  }

  public class PositionUpdater {
    private static final int UPDATE_INTERVAL = 80;
    private static final int MAX_INTERPOLATION_TIME = 500;
    private static final int POSITION_REGISTER_ADDRESS = 110;

    private final VehicleProcessModel processModel;
    private final ScheduledExecutorService executor;
    private ScheduledFuture<?> positionFuture;

    private String lastKnownPosition;
    private long lastUpdateTime;
    private long lastPosition;
    private Triple lastPrecisePosition;
    private double lastOrientation;
    private double estimatedSpeed; // mm/s

    /**
     * The PositionUpdater class is responsible for updating the position of a vehicle.
     * It schedules regular updates of the vehicle position using a fixed rate.
     * The position updates are executed by invoking the updatePosition() method.
     * <p>
     * public PositionUpdater(VehicleProcessModel processModel, ScheduledExecutorService executor)
     * <p>
     * Constructor for the PositionUpdater class. Initializes the PositionUpdater object
     * with the provided processModel and executor objects.
     *
     * @param processModel The VehicleProcessModel object associated with the vehicle.
     * @param executor The ScheduledExecutorService used to schedule position updates.
     */
    public PositionUpdater(VehicleProcessModel processModel, ScheduledExecutorService executor) {
      this.processModel = processModel;
      this.executor = executor;
      this.lastKnownPosition = null;
    }

    /**
     * Starts position updates for the vehicle.
     * This method schedules regular updates of the vehicle position using a fixed rate.
     * The position updates are executed by invoking the updatePosition() method.
     * The initial delay is 0, and the update interval is configured by the UPDATE_INTERVAL field.
     */
    public void startPositionUpdates() {
      positionFuture = executor.scheduleAtFixedRate(
          this::updatePosition,
          0,
          UPDATE_INTERVAL,
          TimeUnit.MILLISECONDS
      );
    }

    private void stopPositionUpdates() {
      if (positionFuture != null && !positionFuture.isCancelled()) {
        positionFuture.cancel(true);
      }
    }

    private void updatePosition() {
      readDWordRegister(POSITION_REGISTER_ADDRESS)
          .thenAccept(this::processPositionUpdate)
          .exceptionally(ex -> {
            LOG.warning("Failed to update position: " + ex.getMessage());
            interpolatePosition();
            return null;
          });
    }

    private void processPositionUpdate(long newPosition) {
      long currentTime = System.currentTimeMillis();

      if (lastUpdateTime > 0) {
        long timeDiff = currentTime - lastUpdateTime;
        long posDiff = newPosition - lastPosition;
        estimatedSpeed = (double) posDiff / timeDiff * 1000;
      }

      lastPosition = newPosition;
      lastUpdateTime = currentTime;

      String openTcsPosition = convertToOpenTcsPosition(newPosition);
      Triple precisePosition = convertToPrecisePosition(newPosition);

      processModel.setPosition(openTcsPosition);
      processModel.setPrecisePosition(precisePosition);

      lastPrecisePosition = precisePosition;
    }

    private void interpolatePosition() {
      long currentTime = System.currentTimeMillis();
      long timeSinceLastUpdate = currentTime - lastUpdateTime;

      if (timeSinceLastUpdate > MAX_INTERPOLATION_TIME) {
        // If the maximum interpolation time is exceeded, no interpolation will be performed
        return;
      }

      // Calculate the interpolated position
      double interpolationFactor = (double) timeSinceLastUpdate / UPDATE_INTERVAL;
      long interpolatedPosition = lastPosition + (long) (estimatedSpeed * interpolationFactor);

      String openTcsPosition = convertToOpenTcsPosition(interpolatedPosition);
      Triple interpolatedPrecisePosition = interpolatePrecisePosition(interpolationFactor);

      processModel.setPosition(openTcsPosition);
      processModel.setPrecisePosition(interpolatedPrecisePosition);
    }

    private Triple interpolatePrecisePosition(double factor) {
      if (lastPrecisePosition == null) {
        return null;
      }
      long dx = (long) (estimatedSpeed * factor * Math.cos(Math.toRadians(lastOrientation)));
      long dy = (long) (estimatedSpeed * factor * Math.sin(Math.toRadians(lastOrientation)));
      return new Triple(
          lastPrecisePosition.getX() + dx,
          lastPrecisePosition.getY() + dy,
          lastPrecisePosition.getZ()
      );
    }

    private String convertToOpenTcsPosition(long position) {
      return getPositionFromMap(position);
    }

    private Triple convertToPrecisePosition(long position) {
      // Implement conversion from original position value to Triple
      return new Triple(position, 0, 0);
    }
  }

  private static class ModbusRequest
      implements
        Comparable<ModbusRequest> {
    private final com.digitalpetri.modbus.requests.ModbusRequest request;
    private final int address;
    private final int priority;

    ModbusRequest(
        com.digitalpetri.modbus.requests.ModbusRequest request,
        int address, int priority
    ) {
      this.request = request;
      this.address = address;
      this.priority = priority;
    }

    public com.digitalpetri.modbus.requests.ModbusRequest getRequest() {
      return request;
    }

    public int getAddress() {
      return address;
    }

    public boolean isReadOperation() {
      return request instanceof ReadHoldingRegistersRequest;
    }

    @Override
    public int compareTo(ModbusRequest other) {
      return Integer.compare(other.priority, this.priority);
    }
  }
}
