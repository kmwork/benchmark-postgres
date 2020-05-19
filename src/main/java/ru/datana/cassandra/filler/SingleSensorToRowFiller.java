package ru.datana.cassandra.filler;

import lombok.extern.slf4j.Slf4j;
import ru.datana.cassandra.ToolsParameters;
import ru.datana.cassandra.helper.GenerateHelper;
import ru.datana.cassandra.helper.SensorPackageHolder;
import ru.datana.cassandra.model.SingleSensorDataModel;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Slf4j
public class SingleSensorToRowFiller extends AbstractFiller {
    @Override
    public void fillDatabase(ToolsParameters parameters) throws SQLException {
        try {
            connect(parameters.getNodes(), parameters.getPort(), parameters.getKeyspace());
            if (parameters.isForceRecreate()) datalakeRepository.createSingleSensorStructure(true);
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
