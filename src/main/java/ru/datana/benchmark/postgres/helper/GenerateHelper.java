package ru.datana.benchmark.postgres.helper;

import ru.datana.benchmark.postgres.model.SensorData;
import ru.datana.benchmark.postgres.model.TechnicalData;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class GenerateHelper {
    private static Random generator = new Random(System.nanoTime());
    private static final int GENERATOR_SIX_MONTH_BOUND = 60 * 60 * 24 * 30 * 6;
    private static long nextId = System.nanoTime();

    public static TechnicalData generateTechnicalData() {
        return generateTechnicalData(LocalDateTime.now().minusSeconds(generator.nextInt(GENERATOR_SIX_MONTH_BOUND)));
    }

    public static long getId() {
        return nextId++;
    }

    public static TechnicalData generateTechnicalData(LocalDateTime responseTime) {
        return TechnicalData.builder()
                .requestId(getId())
                .controllerId(getId())
                .taskId(getId())
                .requestDatetime(Timestamp.valueOf(LocalDateTime.now().minusSeconds(generator.nextInt(GENERATOR_SIX_MONTH_BOUND))))
                .requestDatetimeProxy(Timestamp.valueOf(LocalDateTime.now().minusSeconds(generator.nextInt(GENERATOR_SIX_MONTH_BOUND))))
                .responseDatetime(Timestamp.valueOf(responseTime))
                .build();
    }


    public static SensorData generateSensorData(long sensorId) {
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
