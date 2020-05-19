package ru.datana.benchmark.postgres.helper;

public class SensorIdHolder {
    private int sensorId;
    private int count;

    public SensorIdHolder(int count) {
        this.sensorId = -1;
        this.count = count;
    }

    public int getNextSensorId() {
        sensorId++;
        if (sensorId == count) sensorId = 0;
        return sensorId;
    }
}
