package ru.datana.benchmark.postgres.model;

import lombok.Builder;
import lombok.Getter;
import lombok.Singular;
import lombok.ToString;

import java.sql.Timestamp;
import java.util.Set;
import java.util.UUID;

@Builder
@Getter
@ToString
public class SensorData {
    private UUID sensorId;
    private double data;
    private Timestamp controllerDatetime;
    private byte status;
    @Singular("addError")
    private Set<String> errors;
}
