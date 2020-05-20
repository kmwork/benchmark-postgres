package ru.datana.benchmark.postgres.filler;

import lombok.extern.slf4j.Slf4j;
import ru.datana.benchmark.postgres.ToolsParameters;
import ru.datana.benchmark.postgres.helper.GenerateHelper;
import ru.datana.benchmark.postgres.model.MultiSensorDataModel;
import ru.datana.benchmark.postgres.model.SensorData;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
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
            var ref = new Object() {
                long totallyInserted = 0;
                long insertedBeforeLog = 0;
            };
            PreparedStatement preparedStatement = datalakeRepository.createPreparedStatementForSingleSensorDataPackage(parameters.getPackageSize());
            long start = System.currentTimeMillis();
            if (parameters.getNumberOfPackages() == ToolsParameters.UNLIMITED_PACKAGES) {
                //noinspection InfiniteLoopStatement
                while (true) {
                    fillPackageAndSaveIt(preparedStatement, parameters.getPackageSize());
                    ref.totallyInserted += parameters.getPackageSize();
                    ref.insertedBeforeLog += parameters.getPackageSize();
                    if (ref.insertedBeforeLog >= 100000) {
                        ref.insertedBeforeLog %= 100000;
                        log.info((ref.totallyInserted / 1000) + "к");
                    }
                }
            } else {
                IntStream.range(0, parameters.getNumberOfPackages()).forEach(i -> {
                    try {
                        fillPackageAndSaveIt(preparedStatement, parameters.getPackageSize());
                    } catch (SQLException e) {
                       String msg = "Error in fillPackageAndSaveIt, i = "+i;
                       log.error(msg, e);
                       throw new RuntimeException(msg, e);
                    }
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

    private void fillPackageAndSaveIt(PreparedStatement preparedStatement, int packageSize) throws SQLException {
        List<MultiSensorDataModel> mList = new ArrayList<>(packageSize);
        for (int i = 0; i< packageSize; i++){
            List<SensorData> sensorList= new ArrayList<>(parameters.getNumberOfSensors());
            for (int s = 0; s< parameters.getNumberOfSensors(); s++) {
                var sensor = GenerateHelper.generateSensorData(s);
                sensorList.add(sensor);
            }
            MultiSensorDataModel m = new MultiSensorDataModel();
            m.setTechnicalData(GenerateHelper.generateTechnicalData());
            m.setSensorData(sensorList);
            mList.add(m);
        }

        datalakeRepository.insertSingleSensorDataPackageWithPreparedStatement(preparedStatement, mList);
    }

}
