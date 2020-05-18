package ru.datana.cassandra.filler;

import ru.datana.cassandra.ToolsParameters;
import ru.datana.cassandra.helper.GenerateHelper;
import ru.datana.cassandra.model.MultiSensorDataModel;

import java.util.stream.IntStream;

public class MultiSensorToRowFiller extends AbstractFiller {
    @Override
    public void fillDatabase(ToolsParameters parameters) {
        try {
            connect(parameters.getNodes(), parameters.getPort(), parameters.getKeyspace());
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
                IntStream.range(0, parameters.getNumberOfPackages()).forEach(i -> fillRowAndSaveIt(parameters.getPackageSize()));
            }
            long stop = System.currentTimeMillis();
            System.out.println("Time to generate - " + (stop - start) + " ms");
        } finally {
            closeConnection();
        }
    }


    private void fillRowAndSaveIt(int packageSize) {
        MultiSensorDataModel.MultiSensorDataModelBuilder modelBuilder = MultiSensorDataModel.builder();
        modelBuilder.technicalData(GenerateHelper.generateTechnicalData());
        IntStream.range(0, packageSize)
                .mapToObj(GenerateHelper::generateSensorData)
                .forEach(modelBuilder::addSensorData);
        datalakeRepository.insertMultiSensorData(modelBuilder.build());
    }
}
