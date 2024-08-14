package org.opentcs.kernel.extensions.servicewebapi.v1.binding;

import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.stream.Collectors;
import org.opentcs.data.model.Location;
import org.opentcs.data.model.PeripheralInformation;
import org.opentcs.kernel.extensions.servicewebapi.v1.binding.shared.Property;

public class GetPeripheralResponseTo {

  @Nonnull
  private String name;

  @Nonnull
  private PeripheralInformation.State state;

  @Nonnull
  private List<Property> properties;

  public GetPeripheralResponseTo() {
  }

  public String getName() {
    return name;
  }

  public GetPeripheralResponseTo setName(String name) {
    this.name = name;
    return this;
  }

  public PeripheralInformation.State getState() {
    return state;
  }

  public GetPeripheralResponseTo setState(PeripheralInformation.State state) {
    this.state = state;
    return this;
  }

  public List<Property> getProperties() {
    return properties;
  }

  public GetPeripheralResponseTo setProperties(List<Property> properties) {
    this.properties = properties;
    return this;
  }

  public static GetPeripheralResponseTo fromPeripheral(Location location) {
    GetPeripheralResponseTo state = new GetPeripheralResponseTo();
    state.name = location.getName();
    state.state = location.getPeripheralInformation().getState();
    state.properties = location.getProperties().entrySet().stream()
        .map(entry -> new Property(entry.getKey(), entry.getValue()))
        .collect(Collectors.toList());
    return state;
  }
}
