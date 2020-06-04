package ru.datana.benchmark.postgres.model;

import lombok.Data;

import java.util.List;

@Data
public class MultiSensorDataModel {
    private TechnicalData technicalData;
    private List<SensorData> sensorData;
}
