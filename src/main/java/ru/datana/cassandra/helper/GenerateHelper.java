package ru.datana.cassandra.helper;

import ru.datana.cassandra.model.SensorData;
import ru.datana.cassandra.model.TechnicalData;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class GenerateHelper {
    private static Random generator = new Random(System.nanoTime());
    private static final int GENERATOR_SIX_MONTH_BOUND = 60 * 60 * 24 * 30 * 6;
    private static final String SENSOR_UUID_TEMPLATE = "00000000-0000-4000-9000-%012d";

    public static TechnicalData generateTechnicalData() {
        return generateTechnicalData(LocalDateTime.now().minusSeconds(generator.nextInt(GENERATOR_SIX_MONTH_BOUND)));
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

    @SuppressWarnings("unused")
    public static SensorData generateSensorData(long sensorId) {
        return generateSensorData(UUID.fromString(String.format(SENSOR_UUID_TEMPLATE, sensorId)));
    }

    public static SensorData generateSensorData(UUID sensorId) {
        byte status = (byte) generator.nextInt(2);
        return SensorData.builder()
                .sensorId(sensorId)
                .data(generator.nextDouble())
                .controllerDatetime(Timestamp.valueOf(LocalDateTime.now().minusSeconds(generator.nextInt(GENERATOR_SIX_MONTH_BOUND))))
                .status(status)
                .errors(status == 1
                        ? Collections.emptyList()
                        : IntStream.rangeClosed(1, generator.nextInt(3) + 1).mapToObj(i -> "error-" + i).collect(Collectors.toList()))
                .build();
    }
}
