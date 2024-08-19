package org.opentcs.kernel.extensions.servicewebapi.v1.binding;

import jakarta.annotation.Nonnull;

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
}
