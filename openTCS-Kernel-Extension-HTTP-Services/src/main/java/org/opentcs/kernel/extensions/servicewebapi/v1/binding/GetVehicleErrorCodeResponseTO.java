package org.opentcs.kernel.extensions.servicewebapi.v1.binding;

import jakarta.annotation.Nonnull;
import org.opentcs.data.model.Vehicle;

public class GetVehicleErrorCodeResponseTO {
  @Nonnull
  private String name;

  @Nonnull
  private int errorCode;

  public GetVehicleErrorCodeResponseTO() {
  }

  public String getName() {
    return name;
  }

  public GetVehicleErrorCodeResponseTO setName(String name) {
    this.name = name;
    return this;
  }

  public int getErrorCode() {
    return errorCode;
  }

  public GetVehicleErrorCodeResponseTO setErrorCode(int errorCode) {
    this.errorCode = errorCode;
    return this;
  }

  public static GetVehicleErrorCodeResponseTO fromVehicle(Vehicle vehicle) {
    GetVehicleErrorCodeResponseTO state = new GetVehicleErrorCodeResponseTO();
    state.name = vehicle.getName();
    state.errorCode = Integer.valueOf(vehicle.getProperties().get("ErrorCode"));
    return state;
  }
}
