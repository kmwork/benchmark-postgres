package ru.datana.cassandra.model;

import lombok.Builder;
import lombok.Getter;
import lombok.Singular;

import java.util.List;

@Builder
@Getter
public class MultiSensorDataModel {
    private TechnicalData technicalData;
    @Singular("addSensorData")
    private List<SensorData> sensorData;
}
