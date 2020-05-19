package ru.datana.benchmark.postgres.filler;

import lombok.extern.slf4j.Slf4j;
import ru.datana.benchmark.postgres.ToolsParameters;
import ru.datana.benchmark.postgres.helper.GenerateHelper;
import ru.datana.benchmark.postgres.helper.SensorPackageHolder;
import ru.datana.benchmark.postgres.model.SingleSensorDataModel;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Slf4j
public class SingleSensorToRowFiller extends AbstractFiller {
    public SingleSensorToRowFiller(ToolsParameters parameters) throws SQLException {
        super(parameters);
    }

    @Override
    public void fillDatabase() throws SQLException {
        try {
            if (parameters.isForceRecreate()) datalakeRepository.createSingleSensorStructure();
            SensorPackageHolder sensorPackageHolder = new SensorPackageHolder(parameters.getNumberOfSensors());
            var ref = new Object() {
                long totallyInserted = 0;
                long insertedBeforeLog = 0;
            };
            PreparedStatement preparedStatement = datalakeRepository.createPreparedStatementForSingleSensorDataPackage(parameters.getPackageSize());
            long start = System.currentTimeMillis();
            if (parameters.getNumberOfPackages() == ToolsParameters.UNLIMITED_PACKAGES) {
                //noinspection InfiniteLoopStatement
                while (true) {
                    fillPackageAndSaveIt(preparedStatement, parameters.getPackageSize(), sensorPackageHolder);
                    ref.totallyInserted += parameters.getPackageSize();
                    ref.insertedBeforeLog += parameters.getPackageSize();
                    if (ref.insertedBeforeLog >= 100000) {
                        ref.insertedBeforeLog %= 100000;
                        log.info((ref.totallyInserted / 1000) + "к");
                    }
                }
            } else {
                IntStream.range(0, parameters.getNumberOfPackages()).forEach(i -> {
                    fillPackageAndSaveIt(preparedStatement, parameters.getPackageSize(), sensorPackageHolder);
                    ref.totallyInserted += parameters.getPackageSize();
                    ref.insertedBeforeLog += parameters.getPackageSize();
                    if (ref.insertedBeforeLog >= 50000) {
                        ref.insertedBeforeLog %= 50000;
                        log.info((ref.totallyInserted / 1000) + "к");
                    }
                });
            }
            long stop = System.currentTimeMillis();
            log.info("Time to generate " + ref.totallyInserted + " rows - " + (stop - start) + " ms");
        } finally {
            closeConnection();
        }
    }

    private void fillPackageAndSaveIt(PreparedStatement preparedStatement, int packageSize, SensorPackageHolder sensorPackageHolder) {
        datalakeRepository.insertSingleSensorDataPackageWithPreparedStatement(preparedStatement, IntStream.range(0, packageSize)
                .mapToObj(i -> SingleSensorDataModel.builder()
                        .technicalData(sensorPackageHolder.next().getTechnicalData())
                        .sensorData(GenerateHelper.generateSensorData(sensorPackageHolder.getSensorId()))
                        .build())
                .collect(Collectors.toList()));
    }

}
