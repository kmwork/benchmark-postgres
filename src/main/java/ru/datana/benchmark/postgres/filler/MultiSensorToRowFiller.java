package ru.datana.benchmark.postgres.filler;

import lombok.extern.slf4j.Slf4j;
import ru.datana.benchmark.postgres.ToolsParameters;
import ru.datana.benchmark.postgres.helper.GenerateHelper;
import ru.datana.benchmark.postgres.model.MultiSensorDataModel;

import java.sql.SQLException;
import java.util.stream.IntStream;

@Slf4j
public class MultiSensorToRowFiller extends AbstractFiller {
    @Override
    public void fillDatabase(ToolsParameters parameters) throws SQLException {
        try {
            connect(parameters.getHost(), parameters.getPort(), parameters.getSchema(), parameters.getLogin(), parameters.getPassword());
            if (parameters.isForceRecreate()) {
                datalakeRepository.createMultiSensorStructure(parameters.getPackageSize(), true);
            }
            long start = System.currentTimeMillis();
            if (parameters.getNumberOfPackages() == ToolsParameters.UNLIMITED_PACKAGES) {
                //noinspection InfiniteLoopStatement
                while (true) {
                    fillRowAndSaveIt(parameters.getPackageSize());
                }
            } else {
                IntStream.range(0, parameters.getNumberOfPackages()).forEach(i -> {
                    try {
                        fillRowAndSaveIt(parameters.getPackageSize());
                    } catch (SQLException e) {
                        String msg = "Error in lamda of fillDatabase";
                        log.error(msg, e);
                        throw new RuntimeException(msg, e);
                    }
                });
            }
            long stop = System.currentTimeMillis();
            log.error("Time to generate - " + (stop - start) + " ms");
        } finally {
            closeConnection();
        }
    }


    private void fillRowAndSaveIt(int packageSize) throws SQLException {
        MultiSensorDataModel.MultiSensorDataModelBuilder modelBuilder = MultiSensorDataModel.builder();
        modelBuilder.technicalData(GenerateHelper.generateTechnicalData());
        IntStream.range(0, packageSize)
                .mapToObj(GenerateHelper::generateSensorData)
                .forEach(modelBuilder::addSensorData);
        datalakeRepository.insertMultiSensorData(modelBuilder.build());
    }
}
