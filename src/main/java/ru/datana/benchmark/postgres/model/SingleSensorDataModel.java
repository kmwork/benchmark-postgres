package ru.datana.benchmark.postgres.model;

import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
public class SingleSensorDataModel {
    private TechnicalData technicalData;
    private SensorData sensorData;
}
