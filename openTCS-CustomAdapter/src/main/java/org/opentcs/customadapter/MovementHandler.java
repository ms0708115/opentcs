package org.opentcs.customadapter;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;
import org.opentcs.data.model.Vehicle;
import org.opentcs.drivers.vehicle.LoadHandlingDevice;
import org.opentcs.drivers.vehicle.MovementCommand;

public class MovementHandler {
  private static final Logger LOG = Logger.getLogger(MovementHandler.class.getName());

  private final ScheduledExecutorService executor;
  private final ModbusTCPVehicleCommAdapter adapter;
  private ScheduledFuture<?> monitoringFuture;
  private List<MovementCommand> pendingCommands;
  private int currentCommandIndex;
  private final AtomicBoolean running = new AtomicBoolean(true);
  private final CountDownLatch shutdownLatch = new CountDownLatch(1);
  private boolean isRunnung;
  private boolean setStop;
  private final ScheduledExecutorService movementScheduledExecutor;
  private int vehicleStatus = 0;
  private int liftStatus = 0;
  private int loadStatus = 0;

  /**
   * Handles the movement of a vehicle by executing a list of commands.
   *
   * @param executor The executor service used to schedule the execution of commands.
   * @param adapter The ModbusTCPVehicleCommAdapter used for communication with the vehicle.
   */
  public MovementHandler(ScheduledExecutorService executor, ModbusTCPVehicleCommAdapter adapter) {
    this.executor = executor;
    this.adapter = adapter;
    this.pendingCommands = new ArrayList<>();
    this.currentCommandIndex = 0;
    this.isRunnung = false;
    this.setStop = true;
    this.movementScheduledExecutor = new ScheduledThreadPoolExecutor(2);

  }

  /**
   * Starts monitoring and executing a list of movement commands.
   *
   * @param commands The list of movement commands to monitor and execute.
   */
  public void startMonitoring(List<MovementCommand> commands) {
    running.set(true);

    if (monitoringFuture != null && !monitoringFuture.isDone()) {
      LOG.info(
          adapter.getProcessModel().getName() + ": " +
              String.format(
                  "%s: Cancelling the old monitoring task", adapter.getProcessModel().getName()
              )
      );
      monitoringFuture.cancel(true);
    }

    pendingCommands = new ArrayList<>(commands);
    currentCommandIndex = 0;
    monitoringFuture = movementScheduledExecutor.scheduleWithFixedDelay(() -> {
      if (!running.get() || Thread.currentThread().isInterrupted()) {
        shutdownLatch.countDown();
        return;
      }
      try {
        checkVehicleStatus();
      }
      catch (Exception e) {
        LOG.severe(
            adapter.getProcessModel().getName() + ": "
                + "Error in checkVehicleStatus: " + e.getMessage()
        );
      }
    }, 0, 1000, TimeUnit.MILLISECONDS);
  }

  private void checkVehicleStatus() {
    this.vehicleStatus = adapter.getPositionUpdater().getVehicleStatus();
    try {
      updateVehicleStatus(
          this.vehicleStatus, this.liftStatus, this.loadStatus, adapter.getProcessModel()
              .getPosition()
      );
    }
    catch (Exception ex) {
      LOG.severe(
          adapter.getProcessModel().getName() + ": Failed to read vehicle status: " + ex
              .getMessage()
      );
    }
  }

  private void updateVehicleStatus(
      int vehicleStatus, int liftStatus, int loadStatus, String currentPosition
  ) {
    LOG.info(
        adapter.getProcessModel().getName() + ": " +
            "Updating vehicle status: vehicleStatus=" + vehicleStatus + ", liftStatus=" + liftStatus
            + ", loadStatus=" + loadStatus
            + ", currentPosition=" + currentPosition
    );

    updateVehicleState(vehicleStatus, loadStatus);

    // Check if current movement command is completed
    if (currentCommandIndex < pendingCommands.size()) {
      MovementCommand currentCommand = pendingCommands.get(currentCommandIndex);

      if (hasReachedDestination(vehicleStatus, currentCommand, currentPosition) &&
          isOperationCompleted(currentCommand, liftStatus, loadStatus)) {
        LOG.info(
            adapter.getProcessModel().getName() + ": " +
                String.format(
                    "CURRENT LOCATION MATCH THE DESTINATION AND OPERATION COMPLETED: %s",
                    currentPosition
                )
        );
        currentCommandIndex++;
        if (currentCommandIndex >= pendingCommands.size()) {
          LOG.info(
              adapter.getProcessModel().getName() + ": " +
                  "All commands completed"
          );
          resetMonitorParameter();
        }
        this.setStop = true;
        adapter.getProcessModel().commandExecuted(currentCommand);
      }
      else {
        LOG.info(
            adapter.getProcessModel().getName() + ": " +
                String.format(
                    "VEHICLE HAS NOT COMPLETED THE COMMAND, "
                        + "EXPECT: %s, CURRENTLY AT: %s, OPERATION: %s",
                    currentCommand.getStep().getDestinationPoint().getName(),
                    currentPosition,
                    currentCommand.getOperation()
                )
        );
      }
    }

  }

  private boolean isOperationCompleted(MovementCommand command, int liftStatus, int loadStatus) {
    String operation = command.getOperation();
    if (operation.isEmpty() || operation.equals("NOP")) {
      return true;
    }

    if (adapter.getProcessModel().getState() != Vehicle.State.IDLE) {
      return false;
    }

    if (operation.equalsIgnoreCase("Load")) {
      this.loadStatus = 1;
      this.liftStatus = 2;
      return (liftStatus == 2 && loadStatus == 1);
    }
    else if (operation.equalsIgnoreCase("Unload")) {
      this.loadStatus = 2;
      this.liftStatus = 2;
      return (liftStatus == 2 && loadStatus == 2);
    }
    else {
      return true;
    }
  }

  private void updateVehicleState(int vehicleStatus, int loadStatus) {
    Vehicle.State vehicleState;
    switch (vehicleStatus) {
      case 0, 2 -> {
        vehicleState = Vehicle.State.IDLE;
        this.isRunnung = false;
      }
      case 1 -> {
        vehicleState = Vehicle.State.EXECUTING;
        this.isRunnung = true;
      }
      default -> {
        vehicleState = Vehicle.State.UNKNOWN;
        this.isRunnung = false;
      }
    }
    if ((this.isRunnung && this.setStop) ||
        (pendingCommands.get(currentCommandIndex).getStep().getSourcePoint() == null
            && this.setStop)) {
      try {
        if (pendingCommands.get(currentCommandIndex).getStep().getSourcePoint() == null) {
          Thread.sleep(300);
        }
      }
      catch (Exception ex) {
        LOG.warning("Thread Sleep Fail");
      }

      LOG.warning("SET 105 TO STOP (0)");
      adapter.updateWriteModbusInfo(adapter.getVehicleCommandWriteModbusMapKey(), 0);
      setStop = false;
    }

    boolean liftState = (loadStatus == 1);
    adapter.getProcessModel().setState(vehicleState);
    // Update load handling devices based on lift status
    List<LoadHandlingDevice> devices = new ArrayList<>();
    devices.add(new LoadHandlingDevice("default", liftState));
    adapter.getProcessModel().setLoadHandlingDevices(devices);
  }

  private boolean hasReachedDestination(
      int vehicleStatus, MovementCommand command, String currentPosition
  ) {
    LOG.info(
        adapter.getProcessModel().getName() + ": " +
            String.format(
                "CHECKING BETWEEN: %s & %s",
                command.getStep().getDestinationPoint().getName(),
                currentPosition
            )
    );
    if (command.isFinalMovement()) {
      return (command.getStep().getDestinationPoint().getName().equals(currentPosition)
          && vehicleStatus == 2);
    }
    return (command.getStep().getDestinationPoint().getName().equals(currentPosition));
  }


  public CompletableFuture<Void> stopMonitoring() {
    return CompletableFuture.runAsync(() -> {
      resetMonitorParameter();

      if (monitoringFuture != null) {
        monitoringFuture.cancel(true);
      }
      try {
        if (!shutdownLatch.await(5, TimeUnit.SECONDS)) {
          LOG.warning("Timeout waiting for position updates to stop");
        }
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOG.warning("Interrupted while waiting for position updates to stop");
      }

      LOG.warning("MOVEMENT HANDLER HAS BEEN STOPPED");
    }, executor);
  }

  private void resetMonitorParameter() {
    LOG.info(adapter.getProcessModel().getName() + "Monitor has been RESET.");
    running.set(false);
    pendingCommands.clear();
    currentCommandIndex = 0;
    this.setStop = true;
  }
}
