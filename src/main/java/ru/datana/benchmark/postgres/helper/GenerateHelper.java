package ru.datana.benchmark.postgres.helper;

import ru.datana.benchmark.postgres.model.SensorData;
import ru.datana.benchmark.postgres.model.TechnicalData;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class GenerateHelper {
    private static Random generator = new Random(System.nanoTime());
    private static final String SENSOR_UUID_TEMPLATE = "00000000-0000-4000-9000-%012d";
    private static final int GENERATOR_SIX_MONTH_BOUND = 60 * 60 * 24 * 30 * 6;
    private static long nextId = System.nanoTime();
    private static LocalDateTime packageTime = LocalDateTime.now();

    public static TechnicalData generateTechnicalData(boolean nextOneSecond) {
        packageTime = nextOneSecond ? packageTime.plusSeconds(1) :
                LocalDateTime.now().minusSeconds(generator.nextInt(GENERATOR_SIX_MONTH_BOUND));
        return generateTechnicalData(packageTime);
    }

    public static long getId() {
        return nextId++;
    }

    public static TechnicalData generateTechnicalData(LocalDateTime responseTime) {
        return TechnicalData.builder()
                .requestId(UUID.randomUUID())
                .controllerId(UUID.randomUUID())
                .taskId(UUID.randomUUID())
                .requestDatetime(Timestamp.valueOf(LocalDateTime.now().minusSeconds(generator.nextInt(GENERATOR_SIX_MONTH_BOUND))))
                .requestDatetimeProxy(Timestamp.valueOf(LocalDateTime.now().minusSeconds(generator.nextInt(GENERATOR_SIX_MONTH_BOUND))))
                .responseDatetime(Timestamp.valueOf(responseTime))
                .build();
    }


    public static SensorData generateSensorData(int indexSensor) {
        byte status = (byte) generator.nextInt(2);
        return SensorData.builder()
                .sensorId(UUID.fromString(String.format(SENSOR_UUID_TEMPLATE, indexSensor)))
                .data(generator.nextDouble())
                .controllerDatetime(Timestamp.valueOf(LocalDateTime.now().minusSeconds(generator.nextInt(GENERATOR_SIX_MONTH_BOUND))))
                .status(status)
                .errors(status == 1
                        ? Collections.emptyList()
                        : IntStream.rangeClosed(1, generator.nextInt(3) + 1).mapToObj(i -> "error-" + i).collect(Collectors.toList()))
                .build();
    }
}
