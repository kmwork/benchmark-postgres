package ru.datana.benchmark.postgres.model;

import lombok.Builder;
import lombok.Getter;
import lombok.Singular;

import java.sql.Timestamp;
import java.util.Set;

@Builder
@Getter
public class SensorData {
    private long sensorId;
    private double data;
    private Timestamp controllerDatetime;
    private byte status;
    @Singular("addError")
    private Set<String> errors;
}
