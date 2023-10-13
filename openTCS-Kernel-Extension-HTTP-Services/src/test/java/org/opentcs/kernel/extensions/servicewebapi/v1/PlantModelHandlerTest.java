/**
 * Copyright (c) The openTCS Authors.
 *
 * This program is free software and subject to the MIT license. (For details,
 * see the licensing information (LICENSE.txt) you should have received with
 * this copy of the software.)
 */
package org.opentcs.kernel.extensions.servicewebapi.v1;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import static org.assertj.core.api.Assertions.assertThat;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import org.opentcs.components.kernel.services.PlantModelService;
import org.opentcs.kernel.extensions.servicewebapi.KernelExecutorWrapper;
import static org.mockito.Mockito.mock;
import org.opentcs.access.to.model.PlantModelCreationTO;
import org.opentcs.data.model.Block;
import org.opentcs.data.model.Location;
import org.opentcs.data.model.LocationType;
import org.opentcs.data.model.Path;
import org.opentcs.data.model.PlantModel;
import org.opentcs.data.model.Point;
import org.opentcs.data.model.Vehicle;
import org.opentcs.data.model.visualization.VisualLayout;
import org.opentcs.kernel.extensions.servicewebapi.v1.binding.PlantModelTO;
import org.opentcs.kernel.extensions.servicewebapi.v1.binding.plantmodel.BlockTO;
import org.opentcs.kernel.extensions.servicewebapi.v1.binding.plantmodel.LocationTO;
import org.opentcs.kernel.extensions.servicewebapi.v1.binding.plantmodel.LocationTypeTO;
import org.opentcs.kernel.extensions.servicewebapi.v1.binding.plantmodel.PathTO;
import org.opentcs.kernel.extensions.servicewebapi.v1.binding.plantmodel.PointTO;
import org.opentcs.kernel.extensions.servicewebapi.v1.binding.plantmodel.VehicleTO;
import org.opentcs.kernel.extensions.servicewebapi.v1.binding.plantmodel.VisualLayoutTO;
import org.opentcs.kernel.extensions.servicewebapi.v1.binding.shared.PropertyTO;
import org.opentcs.kernel.extensions.servicewebapi.v1.binding.shared.TripleTO;

/**
 * Unit tests for {@link PlantModelHandler}.
 */
public class PlantModelHandlerTest {

  private PlantModelService orderService;
  private KernelExecutorWrapper executorWrapper;
  private PlantModelHandler handler;

  @BeforeEach
  public void setUp() {
    orderService = mock();
    executorWrapper = new KernelExecutorWrapper(Executors.newSingleThreadExecutor());

    handler = new PlantModelHandler(orderService, executorWrapper);
  }

  @Test
  public void putPlantModel() {
    // Act
    handler.putPlantModel(
        new PlantModelTO("name")
            .setPoints(
                List.of(
                    new PointTO("some-point")
                )
            )
            .setPaths(
                List.of(
                    new PathTO("some-path", "src-point", "dest-point")
                )
            )
            .setLocationTypes(
                List.of(
                    new LocationTypeTO("some-loc-type")
                )
            )
            .setLocations(
                List.of(
                    new LocationTO(
                        "some-location",
                        "some-loc-type",
                        new TripleTO(1, 2, 3)
                    )
                )
            )
            .setBlocks(
                List.of(
                    new BlockTO("some-block")
                )
            )
            .setVehicles(
                List.of(
                    new VehicleTO("some-vehicle")
                )
            )
            .setVisualLayout(new VisualLayoutTO("some-layout"))
            .setProperties(
                List.of(
                    new PropertyTO("some-key", "some-value")
                )
            )
    );

    // Assert
    ArgumentCaptor<PlantModelCreationTO> captor
        = ArgumentCaptor.forClass(PlantModelCreationTO.class);
    then(orderService).should().createPlantModel(captor.capture());
    assertThat(captor.getValue().getPoints()).hasSize(1);
    assertThat(captor.getValue().getPaths()).hasSize(1);
    assertThat(captor.getValue().getLocationTypes()).hasSize(1);
    assertThat(captor.getValue().getLocations()).hasSize(1);
    assertThat(captor.getValue().getBlocks()).hasSize(1);
    assertThat(captor.getValue().getVehicles()).hasSize(1);
    assertThat(captor.getValue().getVisualLayout()).isNotNull();
    assertThat(captor.getValue().getProperties()).hasSize(1);
  }

  @Test
  public void getPlantModel() {
    // Arrange
    PlantModel plantModel
        = new PlantModel("some-plant-model")
            .withPoints(
                Set.of(
                    new Point("some-point")
                )
            )
            .withPaths(
                Set.of(
                    new Path(
                        "some-path",
                        new Point("src-point").getReference(),
                        new Point("dest-point").getReference()
                    )
                )
            )
            .withLocationTypes(
                Set.of(
                    new LocationType("some-loc-type")
                )
            )
            .withLocations(
                Set.of(
                    new Location(
                        "some-location",
                        new LocationType("loc-type").getReference()
                    )
                )
            )
            .withBlocks(
                Set.of(
                    new Block("some-block")
                )
            )
            .withVehicles(
                Set.of(
                    new Vehicle("some-vehicle")
                )
            )
            .withVisuaLayouts(
                Set.of(
                    new VisualLayout("some-layout")
                )
            )
            .withProperties(
                Map.of("some-key", "some-value")
            );
    given(orderService.getPlantModel())
        .willReturn(plantModel);

    // Act
    PlantModelTO to = handler.getPlantModel();

    // Assert
    then(orderService).should().getPlantModel();
    assertThat(to)
        .returns("some-plant-model", PlantModelTO::getName);
    assertThat(to.getPoints()).hasSize(1);
    assertThat(to.getPaths()).hasSize(1);
    assertThat(to.getLocationTypes()).hasSize(1);
    assertThat(to.getLocations()).hasSize(1);
    assertThat(to.getBlocks()).hasSize(1);
    assertThat(to.getVehicles()).hasSize(1);
    assertThat(to.getVisualLayout()).isNotNull();
    assertThat(to.getProperties()).hasSize(1);
  }

}