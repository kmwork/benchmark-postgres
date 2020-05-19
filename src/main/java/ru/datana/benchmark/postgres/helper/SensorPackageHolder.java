package ru.datana.benchmark.postgres.helper;

import ru.datana.benchmark.postgres.model.TechnicalData;

import java.time.LocalDateTime;
import java.util.UUID;
import java.util.stream.IntStream;

public class SensorPackageHolder {
    private static final String SENSOR_UUID_TEMPLATE = "00000000-0000-4000-9000-%012d";

    private int count;
    private int number;
    private UUID[] uuidPool;
    private LocalDateTime packageTime;
    private TechnicalData technicalData;

    public SensorPackageHolder(int count) {
        this.count = count;
        this.number = -1;
        this.uuidPool = new UUID[count];
        IntStream.range(0, count).forEach(i -> uuidPool[i] = UUID.fromString(String.format(SENSOR_UUID_TEMPLATE, i)));
        packageTime = LocalDateTime.now();
        technicalData = GenerateHelper.generateTechnicalData(packageTime);
    }

    public SensorPackageHolder next() {
        if (++number == count) {
            number = 0;
            packageTime = packageTime.plusSeconds(1);
            technicalData = GenerateHelper.generateTechnicalData(packageTime);
        }
        return this;
    }

    public UUID getSensorId() {
        return uuidPool[number];
    }

    public TechnicalData getTechnicalData() {
        return technicalData;
    }
}
