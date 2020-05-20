package ru.datana.benchmark.postgres.helper;

import ru.datana.benchmark.postgres.model.TechnicalData;

import java.time.LocalDateTime;
import java.util.stream.IntStream;

public class SensorPackageHolder {

    private int count;
    private int number;
    private long[] longPool;
    private LocalDateTime packageTime;
    private TechnicalData technicalData;

    public SensorPackageHolder(int count) {
        this.count = count;
        this.number = -1;
        this.longPool = new long[count];
        IntStream.range(0, count).forEach(i -> longPool[i] = GenerateHelper.getId());
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

    public long getSensorId() {
        return longPool[number];
    }

    public TechnicalData getTechnicalData() {
        return technicalData;
    }
}
