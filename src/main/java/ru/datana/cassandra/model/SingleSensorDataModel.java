package ru.datana.cassandra.model;

import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
public class SingleSensorDataModel {
    private TechnicalData technicalData;
    private SensorData sensorData;
}
