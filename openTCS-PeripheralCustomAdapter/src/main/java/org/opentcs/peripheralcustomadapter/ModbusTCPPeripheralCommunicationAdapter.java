package org.opentcs.peripheralcustomadapter;

import static java.util.Objects.requireNonNull;
import static org.opentcs.util.Assertions.checkState;

import com.digitalpetri.modbus.master.ModbusTcpMaster;
import com.digitalpetri.modbus.master.ModbusTcpMasterConfig;
import com.digitalpetri.modbus.requests.ModbusRequest;
import com.digitalpetri.modbus.requests.ReadInputRegistersRequest;
import com.digitalpetri.modbus.requests.WriteMultipleRegistersRequest;
import com.digitalpetri.modbus.responses.ModbusResponse;
import com.digitalpetri.modbus.responses.ReadHoldingRegistersResponse;
import com.digitalpetri.modbus.responses.ReadInputRegistersResponse;
import com.digitalpetri.modbus.responses.WriteMultipleRegistersResponse;
import com.digitalpetri.modbus.responses.WriteSingleRegisterResponse;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.timeout.TimeoutException;
import jakarta.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.opentcs.components.kernel.services.PeripheralService;
import org.opentcs.customizations.ApplicationEventBus;
import org.opentcs.data.model.Location;
import org.opentcs.data.model.PeripheralInformation;
import org.opentcs.data.model.TCSResourceReference;
import org.opentcs.data.peripherals.PeripheralJob;
import org.opentcs.drivers.peripherals.PeripheralJobCallback;
import org.opentcs.drivers.peripherals.PeripheralProcessModel;
import org.opentcs.util.ExplainedBoolean;
import org.opentcs.util.event.EventHandler;

public class ModbusTCPPeripheralCommunicationAdapter
    extends
      PeripheralCommunicationAdapter {

  private static final Logger LOG = Logger.getLogger(
      ModbusTCPPeripheralCommunicationAdapter.class.getName()
  );

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
  private boolean initialized;
  private final ScheduledExecutorService executor;
  private ModbusTcpMaster master;
  private final AtomicBoolean readHeartBeatToggle = new AtomicBoolean(false);
  private final AtomicBoolean writeHeartBeatToggle = new AtomicBoolean(false);
  private final AtomicInteger heartBeatCount = new AtomicInteger(0);
  private final AtomicBoolean heartBeatFail = new AtomicBoolean(false);
  private final AtomicInteger loadingEFEMStatus = new AtomicInteger(0);
  private final AtomicInteger eFEMQuantity = new AtomicInteger(0);
  private final AtomicInteger eFEMStatus = new AtomicInteger(0);
  private final AtomicInteger loadingZIP1Status = new AtomicInteger(0);
  private final AtomicInteger loadingZIP2Status = new AtomicInteger(0);
  private final AtomicInteger loadingOHBStatus = new AtomicInteger(0);
  private final AtomicInteger loadingSideFork1Status = new AtomicInteger(0);
  private final AtomicInteger loadingSideFork2Status = new AtomicInteger(0);
  private final AtomicBoolean getOHBFail = new AtomicBoolean(false);
  private final AtomicBoolean getSideForkFail = new AtomicBoolean(false);
  private final AtomicBoolean getSTKFail = new AtomicBoolean(false);
  private final AtomicBoolean getEFEMFail = new AtomicBoolean(false);

  private ScheduledFuture<?> readHeartBeatFuture;
  private ScheduledFuture<?> writeHeartBeatFuture;
  private ScheduledFuture<?> pollingStatusFuture;
  private final PeripheralDeviceConfigurationProvider configProvider;
  private TCSResourceReference<Location> location;
  private final PeripheralService peripheralService;

  private final ConcurrentHashMap<String, AtomicInteger> statusMap = new ConcurrentHashMap<>();
  private ScheduledFuture<?> catchReadSingleRegisterFuture;

  private final String statusMapHeartbeat = "heartbeat";
  private final String statusMapEFEMStatus = "loadingEFEMStatus";
  private final String statusMapEFEMQuantity = "eFEMQuantity";
  private final String statusMapEFEMState = "eFEMState";
  private final String statusMapEFEMRespond = "eFEMRespond";
  private final String statusMapEFEMRespondContent = "eFEMRespondContent";
  private final String statusMapZIPStatus1 = "loadingZIP1Status";
  private final String statusMapZIPStatus2 = "loadingZIP2Status";
  private final String statusMapOHBStatus = "loadingOHBStatus";
  private final String statusMapSideforkStatus1 = "loadingSideFork1Status";
  private final String statusMapSideforkStatus2 = "loadingSideFork2Status";

  /**
   * Creates a new instance.
   *
   * @param location The reference to the location this adapter is attached to.
   * @param eventHandler The handler used to send events to.
   * @param peripheralService Peripheral Service.
   */
  @Inject
  public ModbusTCPPeripheralCommunicationAdapter(
      @Assisted
      TCSResourceReference<Location> location,
      @ApplicationEventBus
      EventHandler eventHandler,
      PeripheralService peripheralService
  ) {
    super(location, eventHandler, peripheralService);
    this.configProvider = new PeripheralDeviceConfigurationProvider();
    this.host = configProvider.getConfiguration(location.getName()).host();
    this.port = configProvider.getConfiguration(location.getName()).port();
    this.executor = Executors.newScheduledThreadPool(3);
    this.location = location;
    this.isConnected = false;
    this.peripheralService = requireNonNull(peripheralService, "peripheralService");
    initializeStatusMap();
  }

  private void initializeStatusMap() {
    statusMap.put(statusMapHeartbeat, new AtomicInteger(0));
    statusMap.put(statusMapEFEMStatus, new AtomicInteger(0));
    statusMap.put(statusMapEFEMQuantity, new AtomicInteger(0));
    statusMap.put(statusMapEFEMState, new AtomicInteger(0));
    statusMap.put(statusMapEFEMRespond, new AtomicInteger(0));
    statusMap.put(statusMapEFEMRespondContent, new AtomicInteger(0));
    statusMap.put(statusMapZIPStatus1, new AtomicInteger(0));
    statusMap.put(statusMapZIPStatus2, new AtomicInteger(0));
    statusMap.put(statusMapOHBStatus, new AtomicInteger(0));
    statusMap.put(statusMapSideforkStatus1, new AtomicInteger(0));
    statusMap.put(statusMapSideforkStatus2, new AtomicInteger(0));
  }

  @Override
  public void initialize() {
    if (isInitialized()) {
      LOG.warning("Peripheral Device has been initialized");
      return;
    }
    super.initialize();
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
    stopReadHeartBeat();
    if (location.getName().equals("Magazine_loadport")) {
      stopWriteHeartBeat();
    }
    stopPollingSensor();
    shutdownExecutor();
    initialized = false;// Stop the heartbeat mechanism
  }

  private void shutdownExecutor() {
    LOG.info("Shutting down executor service");
    executor.shutdown();
    try {
      if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
        executor.shutdownNow();
        if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
          LOG.severe("Executor did not terminate");
        }
      }
    }
    catch (InterruptedException ie) {
      executor.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }

  @Override
  protected boolean performConnection() {
    LOG.info("Connecting to Modbus TCP server at " + host + ":" + port);
    ModbusTcpMasterConfig config = new ModbusTcpMasterConfig.Builder(host)
        .setPort(port)
        .build();

    try {
      return CompletableFuture.supplyAsync(() -> {
        LOG.info("Creating new ModbusTcpMaster instance");
        return new ModbusTcpMaster(config);
      })
          .thenCompose(newMaster -> {
            this.master = newMaster;
            LOG.info("Initiating connection to Modbus TCP server");
            return newMaster.connect();
          })
          .thenRun(() -> {
            this.isConnected = true;
            LOG.info("Successfully connected to Modbus TCP server");
            getProcessModel().withCommAdapterConnected(true);
            startCatchReadSingleRegister();
            startReadHeartbeat();
            if (location.getName().equals("Magazine_loadport")) {
              startWriteHeartBeat();
            }
            pollingSensorStatus();

            setProcessModel(getProcessModel().withState(PeripheralInformation.State.IDLE));
            sendProcessModelChangedEvent(PeripheralProcessModel.Attribute.STATE);
          })
          .exceptionally(ex -> {
            LOG.log(Level.SEVERE, "Failed to connect to Modbus TCP server", ex);
            this.isConnected = false;
            return null;
          })
          .isDone();
    }
    catch (Exception e) {
      LOG.log(Level.SEVERE, "Unexpected error during connection attempt", e);
      return false;
    }
  }

  @Override
  protected boolean performDisconnection() {
    LOG.info("Disconnecting from Modbus TCP server");
    if (master != null) {
      return master.disconnect()
          .thenRun(() -> {
            LOG.info("Successfully disconnected from Modbus TCP server");
            this.isConnected = false;
            getProcessModel().withCommAdapterConnected(false);
            this.master = null;
            stopReadHeartBeat();
            if (location.getName().equals("Magazine_loadport")) {
              stopWriteHeartBeat();
            }
            stopPollingSensor();
            stopCatchReadSingleRegister();

            setProcessModel(getProcessModel().withState(PeripheralInformation.State.UNKNOWN));
            sendProcessModelChangedEvent(PeripheralProcessModel.Attribute.STATE);

          })
          .exceptionally(ex -> {
            LOG.log(Level.SEVERE, "Failed to disconnect from Modbus TCP server", ex);
            return null;
          })
          .isDone();
    }
    return true;
  }

  private void getEFEMInfo() {
    try {
      int newLoadingResult = getStatus(statusMapEFEMStatus);
      int oldLoadingResult = loadingEFEMStatus.getAndSet(newLoadingResult);
      int newQuantityResult = getStatus(statusMapEFEMQuantity);
      int oldQuantityResult = eFEMQuantity.getAndSet(newQuantityResult);
      int newStatusResult = getStatus(statusMapEFEMState);
      int oldStatusResult = eFEMStatus.getAndSet(newStatusResult);

      getEFEMFail.set(false);

      if (oldLoadingResult != newLoadingResult) {
        if (oldLoadingResult == 2) {
          peripheralService.updateObjectProperty(location, "LoadingStatus", "Load");
          LOG.info("Peripheral : " + location.getName() + ", Current Status :Load");
        }
        else if (oldLoadingResult == 1) {
          peripheralService.updateObjectProperty(location, "LoadingStatus", "Unload");
          LOG.info("Peripheral : " + location.getName() + ", Current Status :Unload");
        }
        else {
          peripheralService.updateObjectProperty(location, "LoadingStatus", "UNKNOWN");
          LOG.info("Peripheral : " + location.getName() + ", Current Status :Unknown");
        }
        if (newQuantityResult == oldQuantityResult) {
          peripheralService.updateObjectProperty(
              location, "Magazine_Quantity", String.valueOf(newQuantityResult)
          );
        }

        if (newStatusResult != oldStatusResult) {
          if (newStatusResult == 1) {
            setProcessModel(getProcessModel().withState(PeripheralInformation.State.EXECUTING));
          }
          else if (newStatusResult == 2) {
            setProcessModel(getProcessModel().withState(PeripheralInformation.State.UNAVAILABLE));
          }
          else if (newStatusResult == 4) {
            setProcessModel(getProcessModel().withState(PeripheralInformation.State.IDLE));
          }
          else if (newStatusResult == 8) {
            setProcessModel(getProcessModel().withState(PeripheralInformation.State.ERROR));
          }
          else if (newStatusResult == 16) {
            setProcessModel(getProcessModel().withState(PeripheralInformation.State.ERROR));
          }
          else {
            setProcessModel(getProcessModel().withState(PeripheralInformation.State.IDLE));
            //If the state is Simulate, the peripheral state can not set Unknown.
            //setProcessModel(getProcessModel().withState(PeripheralInformation.State.UNKNOWN));
          }
          sendProcessModelChangedEvent(PeripheralProcessModel.Attribute.STATE);
        }
      }
    }
    catch (NullPointerException ex) {
      getEFEMFail.set(true);
      LOG.info("Peripheral: " + location.getName() + ", Get EFEM exception: " + ex.getMessage());
      setProcessModel(getProcessModel().withState(PeripheralInformation.State.ERROR));
      sendProcessModelChangedEvent(PeripheralProcessModel.Attribute.STATE);
    }
  }

  private void getOHBInfo() {
    try {
      int newResult = getStatus(statusMapOHBStatus);
      int oldResult = loadingOHBStatus.getAndSet(newResult);
      if (getOHBFail.get()) {
        setProcessModel(
            getProcessModel().withState(PeripheralInformation.State.EXECUTING)
        );
        sendProcessModelChangedEvent(PeripheralProcessModel.Attribute.STATE);
        getOHBFail.set(false);
      }
      if (newResult != oldResult) {
        if (newResult == 2) {
          peripheralService.updateObjectProperty(location, "LoadingStatus", "Load");
          LOG.info("Peripheral : " + location.getName() + ", Current Status :Load");
        }
        else if (newResult == 1) {
          peripheralService.updateObjectProperty(location, "LoadingStatus", "Unload");
          LOG.info("Peripheral : " + location.getName() + ", Current Status :Unload");
        }
        else {
          peripheralService.updateObjectProperty(location, "LoadingStatus", "UNKNOWN");
          LOG.info("Peripheral : " + location.getName() + ", Current Status :Unknown");
        }
      }
    }
    catch (NullPointerException ex) {
      getOHBFail.set(true);
      LOG.info("Peripheral: " + location.getName() + ", Get OHB exception: " + ex.getMessage());
      setProcessModel(getProcessModel().withState(PeripheralInformation.State.ERROR));
      sendProcessModelChangedEvent(PeripheralProcessModel.Attribute.STATE);
    }
  }

  private void getSideForkInfo() {
    try {
      int newSideFork1Result = getStatus(statusMapSideforkStatus1);
      int oldSideFork1Result = loadingSideFork1Status.getAndSet(newSideFork1Result);
      int newSideFork2Result = getStatus(statusMapSideforkStatus2);
      int oldSideFork2Result = loadingSideFork2Status.getAndSet(newSideFork2Result);

      if (getSideForkFail.get()) {
        setProcessModel(
            getProcessModel().withState(PeripheralInformation.State.EXECUTING)
        );
        sendProcessModelChangedEvent(PeripheralProcessModel.Attribute.STATE);
        getSideForkFail.set(false);
      }

      if (newSideFork1Result != oldSideFork1Result) {
        if (newSideFork1Result == 2) {
          //peripheralService.updateObjectProperty(location, "LoadingStatus", "Load");
          LOG.info("Peripheral :" + location.getName() + "#1, Current Status :Load");
        }
        else if (newSideFork1Result == 1) {
          // peripheralService.updateObjectProperty(location, "LoadingStatus", "Unload");
          LOG.info("Peripheral :" + location.getName() + "#1, Current Status :Unload");
        }
        else {
          // peripheralService.updateObjectProperty(location, "LoadingStatus", "UNKNOWN");
          LOG.info("Peripheral :" + location.getName() + "#1, Current Status :Unknown");
        }
      }
      if (newSideFork2Result != oldSideFork2Result) {
        if (newSideFork2Result == 2) {
          peripheralService.updateObjectProperty(location, "LoadingStatus", "Load");
          LOG.info("Peripheral :" + location.getName() + "#2, Current Status :Load");
        }
        else if (newSideFork2Result == 1) {
          peripheralService.updateObjectProperty(location, "LoadingStatus", "Unload");
          LOG.info("Peripheral :" + location.getName() + "#2, Current Status :Unload");
        }
        else {
          peripheralService.updateObjectProperty(location, "LoadingStatus", "UNKNOWN");
          LOG.info("Peripheral :" + location.getName() + "#2, Current Status :Unknown");
        }
      }
    }
    catch (NullPointerException ex) {
      getSideForkFail.set(true);
      LOG.info(
          "Peripheral: " + location.getName() + ", Get SideFork exception: " + ex.getMessage()
      );
      setProcessModel(getProcessModel().withState(PeripheralInformation.State.ERROR));
      sendProcessModelChangedEvent(PeripheralProcessModel.Attribute.STATE);
    }
  }

  private void getZIPInfo() {
    try {
      int newZIP1Result = getStatus(statusMapZIPStatus1);
      int oldZIP1Result = loadingZIP1Status.getAndSet(newZIP1Result);
      int newZIP2Result = getStatus(statusMapZIPStatus2);
      int oldZIP2Result = loadingZIP2Status.getAndSet(newZIP2Result);

      if (getSTKFail.get()) {
        setProcessModel(
            getProcessModel().withState(PeripheralInformation.State.EXECUTING)
        );
        sendProcessModelChangedEvent(PeripheralProcessModel.Attribute.STATE);
        getSTKFail.set(false);
      }

      if (newZIP1Result != oldZIP1Result) {
        if (newZIP1Result == 2) {
          //peripheralService.updateObjectProperty(location, "LoadingStatus", "Load");
          LOG.info("Peripheral :" + location.getName() + "#1, Current Status :Load");
        }
        else if (newZIP1Result == 1) {
          // peripheralService.updateObjectProperty(location, "LoadingStatus", "Unload");
          LOG.info("Peripheral :" + location.getName() + "#1, Current Status :Unload");
        }
        else {
          // peripheralService.updateObjectProperty(location, "LoadingStatus", "UNKNOWN");
          LOG.info("Peripheral :" + location.getName() + "#1, Current Status :Unknown");
        }
      }
      if (newZIP2Result != oldZIP2Result) {
        if (newZIP2Result == 2) {
          peripheralService.updateObjectProperty(location, "LoadingStatus", "Load");
          LOG.info("Peripheral :" + location.getName() + "#2, Current Status :Load");
        }
        else if (newZIP2Result == 1) {
          peripheralService.updateObjectProperty(location, "LoadingStatus", "Unload");
          LOG.info("Peripheral :" + location.getName() + "#2, Current Status :Unload");
        }
        else {
          peripheralService.updateObjectProperty(location, "LoadingStatus", "UNKNOWN");
          LOG.info("Peripheral :" + location.getName() + "#2, Current Status :Unknown");

        }
      }
    }
    catch (NullPointerException ex) {
      getSTKFail.set(true);
      LOG.info("Peripheral: " + location.getName() + ", Get STK exception: " + ex.getMessage());
      setProcessModel(getProcessModel().withState(PeripheralInformation.State.ERROR));
      sendProcessModelChangedEvent(PeripheralProcessModel.Attribute.STATE);
    }
  }

  private void pollingSensorStatus() {
    pollingStatusFuture = executor.scheduleWithFixedDelay(() -> {
      if (!heartBeatFail.get()) {
        try {
          if (String.CASE_INSENSITIVE_ORDER.compare(location.getName(), "Magazine_loadport")
              == 0) {
            getEFEMInfo();
          }
          else if (String.CASE_INSENSITIVE_ORDER.compare(
              location.getName(), "STK_2"
          ) == 0) {
            getZIPInfo();
          }
          else if (String.CASE_INSENSITIVE_ORDER.compare(
              location.getName(), "OHB"
          ) == 0) {
            getOHBInfo();
          }
          else if (String.CASE_INSENSITIVE_ORDER.compare(
              location.getName(), "Sidefork"
          ) == 0) {
            getSideForkInfo();
          }
        }
        catch (Exception e) {
          LOG.severe("Error in sensor polling: " + e.getMessage());
        }
      }
    }, 0, 500, TimeUnit.MILLISECONDS);
  }

  private void stopPollingSensor() {
    if (pollingStatusFuture != null && !pollingStatusFuture.isCancelled()) {
      LOG.info("Stop Polling Sensor.");
      pollingStatusFuture.cancel(true);
    }
  }

  private void stopCatchReadSingleRegister() {
    if (catchReadSingleRegisterFuture != null && !catchReadSingleRegisterFuture.isCancelled()) {
      LOG.info("Stop Catch ReadSingleRegister.");
      catchReadSingleRegisterFuture.cancel(true);
    }
  }

  private void startCatchReadSingleRegister() {
    LOG.info("Starting reading single register, Peripheral Name : " + location.getName() + ".");

    catchReadSingleRegisterFuture = executor.scheduleAtFixedRate(() -> {
      try {
        readSingleRegister(300, 12).thenAccept(value -> {
          updateStatus(statusMapHeartbeat, value.get(300));
          if (location.getName().equals("STK2")) {
            updateStatus(statusMapZIPStatus1, value.get(301));
            updateStatus(statusMapZIPStatus2, value.get(302));
          }
          else if (location.getName().equals("OHB")) {
            updateStatus(statusMapOHBStatus, value.get(303));
          }
          else if (location.getName().equals("Sidefork")) {
            updateStatus(statusMapSideforkStatus1, value.get(305));
            updateStatus(statusMapSideforkStatus2, value.get(306));
          }
          else if (location.getName().equals("Magazine_loadport")) {
            updateStatus(statusMapEFEMStatus, value.get(301));
            updateStatus(statusMapEFEMQuantity, value.get(302));
            updateStatus(statusMapEFEMState, value.get(303));
            updateStatus(statusMapEFEMRespond, value.get(310));
            updateStatus(statusMapEFEMRespondContent, value.get(311));
          }
        });
      }
      catch (Exception e) {
        LOG.severe("Error in heartbeat: " + e.getMessage());
      }
    }, 0, 250, TimeUnit.MILLISECONDS);
  }

  public void updateStatus(String key, int value) {
    AtomicInteger atomicValue = statusMap.get(key);
    if (atomicValue != null) {
      atomicValue.set(value);
    }
    else {
      LOG.warning("Attempted to update unknown status key: " + key);
    }
  }

  public int getStatus(String key) {
    AtomicInteger value = statusMap.get(key);
    if (value == null) {
      throw new NullPointerException("Get status map value is null");
    }

    return value.get();
  }

  private void startReadHeartbeat() {
    LOG.info("Starting reading heart bit, Peripheral Name : " + location.getName() + ".");

    readHeartBeatFuture = executor.scheduleWithFixedDelay(() -> {
      if (!getOHBFail.get() && !getSTKFail.get() && !getSideForkFail.get() && !getEFEMFail.get()) {
        try {
          boolean newHeartBit = getStatus(statusMapHeartbeat) == 1;
          boolean oldHeartBit = readHeartBeatToggle.getAndSet(newHeartBit);
          if (oldHeartBit == newHeartBit) {
            if (heartBeatCount.addAndGet(1) >= 4) {
              LOG.info("Heart bit unchanged, Peripheral: " + location.getName());
              heartBeatFail.set(true);
              setProcessModel(getProcessModel().withState(PeripheralInformation.State.ERROR));
              sendProcessModelChangedEvent(PeripheralProcessModel.Attribute.STATE);
            }
          }
          else {
            heartBeatCount.set(0);
            heartBeatFail.set(false);
          }
        }
        catch (NullPointerException ex) {
          LOG.info(
              "Peripheral: " + location.getName() + ", Heart bit exception: " + ex.getMessage()
          );
          heartBeatFail.set(true);
          setProcessModel(getProcessModel().withState(PeripheralInformation.State.ERROR));
          sendProcessModelChangedEvent(PeripheralProcessModel.Attribute.STATE);
        }
      }
    }, 0, 500, TimeUnit.MILLISECONDS);
  }

  private void startWriteHeartBeat() {
    LOG.info("Starting sending heart bit, Peripheral Name : " + location.getName() + ".");

    writeHeartBeatFuture = executor.scheduleWithFixedDelay(() -> {
      try {
        boolean value = writeHeartBeatToggle.getAndSet(!writeHeartBeatToggle.get());
        CompletableFuture<Void> writeFuture = writeSingleRegister(300, value ? 1 : 0);

        writeFuture.thenRun(() -> {
          LOG.fine("Heart bit sent successfully: " + (value ? 1 : 0));
        }).exceptionally(ex -> {
          LOG.warning("Failed to send heart bit: " + ex.getMessage());
          return null;
        });
      }
      catch (Exception e) {
        LOG.severe("Error in write heartbeat: " + e.getMessage());
      }
    }, 0, 500, TimeUnit.MILLISECONDS);
  }

  private void stopReadHeartBeat() {
    if (readHeartBeatFuture != null && !readHeartBeatFuture.isCancelled()) {
      LOG.info("Stop reading heart bit.");
      readHeartBeatFuture.cancel(true);
    }
  }

  private void stopWriteHeartBeat() {
    if (writeHeartBeatFuture != null && !writeHeartBeatFuture.isCancelled()) {
      LOG.info("Stop sending heart bit.");
      writeHeartBeatFuture.cancel(true);
    }
  }

  private CompletableFuture<Void> writeSingleRegister(int address, int value) {
    ByteBuf buffer = Unpooled.buffer(2);
    buffer.writeShort(value);
    WriteMultipleRegistersRequest request = new WriteMultipleRegistersRequest(address, 1, buffer);

    return sendModbusRequest(request)
        .thenAccept(response -> {
        })
        .exceptionally(ex -> {
          LOG.severe("Failed to write register at address " + address + ": " + ex.getMessage());
          return null;
        });
  }

  private CompletableFuture<Map<Integer, Integer>> readSingleRegister(int address, int quantity) {
    ReadInputRegistersRequest request = new ReadInputRegistersRequest(address, quantity);
    return sendModbusRequest(request)
        .thenApply(response -> {
          Map<Integer, Integer> result = new HashMap<>();
          if (response instanceof ReadInputRegistersResponse readResponse) {
            ByteBuf responseBuffer = readResponse.getRegisters();
            for (int i = 0; i < quantity; i++) {
              int value = responseBuffer.readUnsignedShort();
              result.put(address + i, value);
              //LOG.info(String.format("READ ADDRESS %d GOT %d", address + i, value));
            }
            if (responseBuffer != null && responseBuffer.refCnt() > 0) {
              responseBuffer.release();
            }
            return result;
          }
          throw new RuntimeException("Invalid response type");
        });
  }

  private CompletableFuture<ModbusResponse> sendModbusRequest(
      ModbusRequest request
  ) {
    return sendModbusRequestWithRetry(request, 3)
        .whenComplete((response, ex) -> {})
        .exceptionally(ex -> {
          LOG.severe("All retries failed for Modbus request: " + ex.getMessage());
          throw new CompletionException("Failed to send Modbus request after retries", ex);
        });
  }

  private CompletableFuture<ModbusResponse> sendModbusRequestWithRetry(
      ModbusRequest request,
      int retriesLeft
  ) {
    if (master == null) {
      return CompletableFuture.failedFuture(
          new IllegalStateException("Modbus master is not initialized")
      );
    }

    return CompletableFuture.supplyAsync(() -> sendRequest(request), executor)
        .thenApply(this::processResponse)
        .exceptionally(ex -> {
          LOG.severe("Failed to send Modbus request: " + ex.getMessage());
          return null;
        }).thenCompose(response -> {
          boolean shouldRetry = response == null && retriesLeft > 0;
          if (shouldRetry) {
            return CompletableFuture.runAsync(() -> {
              try {
                Thread.sleep(1000);
              }
              catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
            }, executor)
                .thenCompose(v -> sendModbusRequestWithRetry(request, retriesLeft - 1));
          }
          return CompletableFuture.completedFuture(response);


        });
  }

  private ModbusResponse sendRequest(ModbusRequest request) {
    try {
      return master.sendRequest(request, 0).get(500, TimeUnit.MILLISECONDS);
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new CompletionException("Request interrupted", e);
    }
    catch (ExecutionException e) {
      throw new CompletionException("Request failed", e.getCause());
    }
    catch (TimeoutException | java.util.concurrent.TimeoutException e) {
      throw new CompletionException("Request timed out", e);
    }
  }

  private ModbusResponse processResponse(ModbusResponse response) {
    if (response instanceof ReadHoldingRegistersResponse readResponse) {
      return handleReadHoldingRegistersResponse(readResponse);
    }
    else if (response instanceof WriteMultipleRegistersResponse writeResponse) {
      return handleWriteMultipleRegistersResponse(writeResponse);
    }
    return response;
  }

  private ReadHoldingRegistersResponse handleReadHoldingRegistersResponse(
      ReadHoldingRegistersResponse readResponse
  ) {
    ByteBuf registers = readResponse.getRegisters().copy();
    return new ReadHoldingRegistersResponse(registers) {
    };
  }

  private WriteMultipleRegistersResponse handleWriteMultipleRegistersResponse(
      WriteMultipleRegistersResponse writeResponse
  ) {
    return writeResponse;
  }

  private ReadInputRegistersResponse handleReadInputRegistersResponse(
      ReadInputRegistersResponse readInputResponse
  ) {
    return readInputResponse;
  }

  private WriteSingleRegisterResponse handleWriteSingleRegisterResponse(
      WriteSingleRegisterResponse writeSingleResponse
  ) {
    return writeSingleResponse;
  }

  @Override
  public synchronized void process(
      @Nonnull
      PeripheralJob job,
      @Nonnull
      PeripheralJobCallback callback
  ) {
    ExplainedBoolean canProcess = canProcess(job);
    checkState(
        canProcess.getValue(),
        "%s: Can't process job: %s",
        getProcessModel().getLocation().getName(),
        canProcess.getReason()
    );
    CompletableFuture<Void> processFuture = new CompletableFuture<>();

    sendHandshakeToEFEM(job)
        .thenCompose(__ -> {
          ScheduledFuture<?> scheduledFuture = executor.scheduleAtFixedRate(() -> {
            readSingleRegister(310, 2)
                .thenAccept(value -> {
                  LOG.info(
                      "Read data 310: " + value.get(310) + " Read data 311: " + value.get(311)
                          + ", Current Time: " + System.currentTimeMillis()
                  );

                  if (value.get(310) == 1 && value.get(311) == 1) {
                    processFuture.complete(null);
                  }
                })
                .exceptionally(ex -> {
                  LOG.severe("Error reading registers: " + ex.getMessage());
                  return null;
                });
          }, 0, 500, TimeUnit.MILLISECONDS);

          return processFuture.whenComplete((result, ex) -> scheduledFuture.cancel(false));
        })
        .orTimeout(5, TimeUnit.SECONDS)
        .thenRun(() -> {
          callback.peripheralJobFinished(job.getReference());
          LOG.info("Peripheral job finished successfully");
        })
        .exceptionally(ex -> {
          if (ex instanceof TimeoutException) {
            LOG.info("Request update EFEM response handshake timeout");
          }
          else {
            LOG.info("Error processing peripheral job: " + ex.getMessage());
          }
          callback.peripheralJobFailed(job.getReference());
          return null;
        })
        .whenComplete((__, ex) -> sendHandshakeReturnZeroToEFEM());
  }

  private CompletableFuture<Void> sendHandshakeToEFEM(PeripheralJob job) {
    int value = convertJobOperation(job.getPeripheralOperation().getOperation());
    return CompletableFuture.allOf(
        writeSingleRegister(311, value),
        writeSingleRegister(310, 1)
    ).thenRun(() -> {
      LOG.info("Successfully wrote registers for handshake");
    }).exceptionally(ex -> {
      LOG.severe("Failed to write registers for handshake: " + ex.getMessage());
      throw new CompletionException(ex);
    });
  }

  private CompletableFuture<Void> sendHandshakeReturnZeroToEFEM() {
    return CompletableFuture.allOf(
        writeSingleRegister(311, 0),
        writeSingleRegister(310, 0)
    ).thenRun(() -> {
      LOG.info("Successfully wrote registers at address 311 and 310 value return to zero");
    }).exceptionally(ex -> {
      LOG.severe(
          "Failed to write registers at address 311 and 310 value return to zero: " + ex
              .getMessage()
      );
      return null;
    });
  }

  private int convertJobOperation(String operation) {
    switch (operation) {
      case "Load" -> {
        return 1;
      }
      case "Unload" -> {
        return 2;
      }
      default -> {
        return 0;
      }
    }
  }
}
