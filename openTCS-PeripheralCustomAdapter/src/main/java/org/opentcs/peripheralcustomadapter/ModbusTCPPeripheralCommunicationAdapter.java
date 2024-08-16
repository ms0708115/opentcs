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
import io.netty.util.ReferenceCountUtil;
import jakarta.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.IntStream;
import org.opentcs.components.kernel.services.PeripheralService;
import org.opentcs.customizations.ApplicationEventBus;
import org.opentcs.customizations.kernel.KernelExecutor;
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
  private ScheduledFuture<?> readHeartBeatFuture;
  private ScheduledFuture<?> writeHeartBeatFuture;
  private ScheduledFuture<?> pollingStatusFuture;
  private final PeripheralDeviceConfigurationProvider configProvider;
  private TCSResourceReference<Location> location;
  private final PeripheralService peripheralService;

  /**
   * Creates a new instance.
   *
   * @param location The reference to the location this adapter is attached to.
   * @param eventHandler The handler used to send events to.
   * @param kernelExecutor The kernel's executor.
   * @param peripheralService Peripheral Service.
   */
  @Inject
  public ModbusTCPPeripheralCommunicationAdapter(
      @Assisted
      TCSResourceReference<Location> location,
      @ApplicationEventBus
      EventHandler eventHandler,
      @KernelExecutor
      ScheduledExecutorService kernelExecutor,
      PeripheralService peripheralService
  ) {
    super(location, eventHandler, kernelExecutor, peripheralService);
    this.configProvider = new PeripheralDeviceConfigurationProvider();
    this.host = configProvider.getConfiguration(location.getName()).host();
    this.port = configProvider.getConfiguration(location.getName()).port();
    this.executor = kernelExecutor;
    this.location = location;
    this.isConnected = false;
    this.peripheralService = requireNonNull(peripheralService, "peripheralService");
  }

  @Override
  public void initialize() {
    if (isInitialized()) {
      LOG.warning("Peripheral Device has been initialized");
      return;
    }
    super.initialize();
    setProcessModel(getProcessModel().withState(PeripheralInformation.State.IDLE));
    sendProcessModelChangedEvent(PeripheralProcessModel.Attribute.STATE);
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
    initialized = false;// Stop the heartbeat mechanism
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
            startReadHeartbeat();
            if (location.getName().equals("Magazine_loadport")) {
              startWriteHeartBeat();
            }
            pollingSensorStatus();

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
          })
          .exceptionally(ex -> {
            LOG.log(Level.SEVERE, "Failed to disconnect from Modbus TCP server", ex);
            return null;
          })
          .isDone();
    }
    return true;
  }

  private void getEFEMInfo(Map<Integer, Integer> value, int index) {
    switch (index) {
      case 0 -> {
        int newResult = value.get(301);
        int oldResult = loadingEFEMStatus.getAndSet(newResult);
        if (newResult != oldResult) {
          if (newResult == 2) {
            peripheralService.updateObjectProperty(location, "LoadingStatus", "Load");
            LOG.info("Peripheral :" + location.getName() + ", Current Status :Load");
          }
          else if (newResult == 1) {
            peripheralService.updateObjectProperty(location, "LoadingStatus", "Unload");
            LOG.info("Peripheral :" + location.getName() + ", Current Status :Unload");
          }
          else {
            peripheralService.updateObjectProperty(location, "LoadingStatus", "Unknown");
            LOG.info("Peripheral :" + location.getName() + ", Current Status :Unknown");
          }
        }
      }
      case 1 -> {
        eFEMQuantity.set(value.get(301 + index));
        peripheralService.updateObjectProperty(
            location, "Magazine_Quantity ", String.valueOf(eFEMQuantity.get())
        );
        //LOG.info("Peripheral :" + location.getName() + ", Quantity :" + eFEMQuantity.get());
      }
      case 2 -> {

        eFEMStatus.set(value.get(301 + index));
        if (eFEMStatus.get() == 1) {
          setProcessModel(getProcessModel().withState(PeripheralInformation.State.EXECUTING));
        }
        else if (eFEMStatus.get() == 2) {
          setProcessModel(getProcessModel().withState(PeripheralInformation.State.UNAVAILABLE));
        }
        else if (eFEMStatus.get() == 4) {
          setProcessModel(getProcessModel().withState(PeripheralInformation.State.IDLE));
        }
        else if (eFEMStatus.get() == 8) {
          setProcessModel(getProcessModel().withState(PeripheralInformation.State.ERROR));
        }
        else if (eFEMStatus.get() == 16) {
          setProcessModel(getProcessModel().withState(PeripheralInformation.State.ERROR));
        }
        else {
          setProcessModel(getProcessModel().withState(PeripheralInformation.State.UNKNOWN));
        }
        sendProcessModelChangedEvent(PeripheralProcessModel.Attribute.STATE);
      }
      default -> throw new IllegalStateException("Unexpected value: " + index);
    }
  }

  private void getOHBInfo(Map<Integer, Integer> value) {
    int newResult = value.get(303);
    int oldResult = loadingOHBStatus.getAndSet(newResult);

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

  private void getSideForkInfo(Map<Integer, Integer> value, int index) {
    switch (index) {
      case 0 -> {
        int newResult = value.get(305);
        int oldResult = loadingSideFork1Status.getAndSet(newResult);
        if (newResult != oldResult) {
          if (newResult == 2) {
            //peripheralService.updateObjectProperty(location, "LoadingStatus", "Load");
            LOG.info("Peripheral :" + location.getName() + "#1, Current Status :Load");
          }
          else if (newResult == 1) {
            // peripheralService.updateObjectProperty(location, "LoadingStatus", "Unload");
            LOG.info("Peripheral :" + location.getName() + "#1, Current Status :Unload");
          }
          else {
            // peripheralService.updateObjectProperty(location, "LoadingStatus", "UNKNOWN");
            LOG.info("Peripheral :" + location.getName() + "#1, Current Status :Unknown");
          }
        }
      }
      case 1 -> {
        int newResult = value.get(305 + index);
        int oldResult = loadingSideFork2Status.getAndSet(newResult);
        if (newResult != oldResult) {
          if (newResult == 2) {
            peripheralService.updateObjectProperty(location, "LoadingStatus", "Load");
            LOG.info("Peripheral :" + location.getName() + "#2, Current Status :Load");
          }
          else if (newResult == 1) {
            peripheralService.updateObjectProperty(location, "LoadingStatus", "Unload");
            LOG.info("Peripheral :" + location.getName() + "#2, Current Status :Unload");
          }
          else {
            peripheralService.updateObjectProperty(location, "LoadingStatus", "UNKNOWN");
            LOG.info("Peripheral :" + location.getName() + "#2, Current Status :Unknown");
          }
        }
      }
      default -> throw new IllegalStateException("Unexpected value: " + index);
    }
  }

  private void getZIPInfo(Map<Integer, Integer> value, int index) {
    switch (index) {
      case 0 -> {
        int newResult = value.get(301);
        int oldResult = loadingZIP1Status.getAndSet(newResult);
        if (newResult != oldResult) {
          if (newResult == 2) {
            //peripheralService.updateObjectProperty(location, "LoadingStatus", "Load");
            LOG.info("Peripheral :" + location.getName() + "#1, Current Status :Load");
          }
          else if (newResult == 1) {
            // peripheralService.updateObjectProperty(location, "LoadingStatus", "Unload");
            LOG.info("Peripheral :" + location.getName() + "#1, Current Status :Unload");
          }
          else {
            // peripheralService.updateObjectProperty(location, "LoadingStatus", "UNKNOWN");
            LOG.info("Peripheral :" + location.getName() + "#1, Current Status :Unknown");
          }
        }
      }
      case 1 -> {
        int newResult = value.get(301 + index);
        int oldResult = loadingZIP2Status.getAndSet(newResult);
        if (newResult != oldResult) {
          if (newResult == 2) {
            peripheralService.updateObjectProperty(location, "LoadingStatus", "Load");
            LOG.info("Peripheral :" + location.getName() + "#2, Current Status :Load");
          }
          else if (newResult == 1) {
            peripheralService.updateObjectProperty(location, "LoadingStatus", "Unload");
            LOG.info("Peripheral :" + location.getName() + "#2, Current Status :Unload");
          }
          else {
            peripheralService.updateObjectProperty(location, "LoadingStatus", "UNKNOWN");
            LOG.info("Peripheral :" + location.getName() + "#2, Current Status :Unknown");
          }
        }
      }
      default -> throw new IllegalStateException("Unexpected value: " + index);
    }
  }

  private void pollingSensorStatus() {
    pollingStatusFuture = executor.scheduleWithFixedDelay(() -> {
      if (!heartBeatFail.get()) {
        try {
          if (String.CASE_INSENSITIVE_ORDER.compare(location.getName(), "Magazine_loadport")
              == 0) {
            readSingleRegister(301, 3).thenAccept(
                value -> {
                  IntStream.range(0, 3).forEachOrdered(i -> {
                    getEFEMInfo(value, i);
                  });
                }
            );
          }
          else if (String.CASE_INSENSITIVE_ORDER.compare(
              location.getName(), "STK_2"
          )
              == 0) {
                readSingleRegister(301, 2).thenAccept(
                    value -> {
                      IntStream.range(0, 2).forEachOrdered(i -> {
                        getZIPInfo(value, i);
                        setProcessModel(
                            getProcessModel().withState(PeripheralInformation.State.EXECUTING)
                        );
                        sendProcessModelChangedEvent(PeripheralProcessModel.Attribute.STATE);
                      });
                    }
                );
              }
          else if (String.CASE_INSENSITIVE_ORDER.compare(
              location.getName(), "OHB"
          )
              == 0) {
                readSingleRegister(303, 1).thenAccept(value -> {
                  getOHBInfo(value);
                  setProcessModel(
                      getProcessModel().withState(PeripheralInformation.State.EXECUTING)
                  );
                  sendProcessModelChangedEvent(PeripheralProcessModel.Attribute.STATE);
                }
                );
              }
          else if (String.CASE_INSENSITIVE_ORDER.compare(
              location.getName(), "Sidefork"
          ) == 0) {
            readSingleRegister(305, 2).thenAccept(
                value -> {
                  IntStream.range(0, 2).forEachOrdered(i -> {
                    getSideForkInfo(value, i);
                    setProcessModel(
                        getProcessModel().withState(PeripheralInformation.State.EXECUTING)
                    );
                    sendProcessModelChangedEvent(PeripheralProcessModel.Attribute.STATE);
                  });
                }
            );
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

  private void startReadHeartbeat() {
    LOG.info("Starting reading heart bit, Peripheral Name : " + location.getName() + ".");

    readHeartBeatFuture = executor.scheduleWithFixedDelay(() -> {
      try {
        readSingleRegister(300, 1).thenAccept(value -> {
          boolean newHeartBit = value.get(300) == 1;
          boolean oldHeartBit = readHeartBeatToggle.getAndSet(newHeartBit);
          if (oldHeartBit == newHeartBit) {
            if (heartBeatCount.incrementAndGet() >= 2) {
              LOG.info("Heart bit unchanged, Peripheral: " + location.getName());
              heartBeatFail.set(true);
              setProcessModel(getProcessModel().withState(PeripheralInformation.State.ERROR));
            }
          }
          else {
            heartBeatCount.set(0);
            heartBeatFail.set(false);
          }
        });
      }
      catch (Exception e) {
        LOG.severe("Error in heartbeat: " + e.getMessage());
      }
    }, 0, 300, TimeUnit.MILLISECONDS);
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
          // LOG.info("Successfully wrote register at address " + address + " with value " + value);
        })
        .exceptionally(ex -> {
          LOG.severe("Failed to write register at address " + address + ": " + ex.getMessage());
          return null;
        })
        .whenComplete((v, ex) -> {
          if (buffer.refCnt() > 0) {
            buffer.release();
          }
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
              LOG.info(String.format("READ ADDRESS %d GOT %d", address + i, value));
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
        .whenComplete((response, ex) -> {
          if (response != null) {
            ReferenceCountUtil.release(response);
          }
        })
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
    else if (response instanceof ReadInputRegistersResponse readInputResponse) {
      return handleReadInputRegistersResponse(readInputResponse);
    }
    else if (response instanceof WriteSingleRegisterResponse writeSingleResponse) {
      return handleWriteSingleRegisterResponse(writeSingleResponse);
    }
    return response;
  }

  private ReadHoldingRegistersResponse handleReadHoldingRegistersResponse(
      ReadHoldingRegistersResponse readResponse
  ) {
    ByteBuf registers = readResponse.getRegisters();
    registers.retain();
    return new ReadHoldingRegistersResponse(registers) {
      @Override
      public boolean release() {
        boolean released = super.release();
        if (released && registers.refCnt() > 0) {
          return registers.release();
        }
        return released;
      }

      @Override
      public boolean release(int decrement) {
        boolean released = super.release(decrement);
        if (released && registers.refCnt() > 0) {
          return registers.release(decrement);
        }
        return released;
      }
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
    ByteBuf registers = readInputResponse.getRegisters();
    registers.retain();
    return new ReadInputRegistersResponse(registers) {
      @Override
      public boolean release() {
        boolean released = super.release();
        if (released && registers.refCnt() > 0) {
          return registers.release();
        }
        return released;
      }

      @Override
      public boolean release(int decrement) {
        boolean released = super.release(decrement);
        if (released && registers.refCnt() > 0) {
          return registers.release(decrement);
        }
        return released;
      }
    };
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
