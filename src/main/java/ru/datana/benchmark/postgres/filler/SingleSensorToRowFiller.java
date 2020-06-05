package ru.datana.benchmark.postgres.filler;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import ru.datana.benchmark.postgres.ToolsParameters;
import ru.datana.benchmark.postgres.helper.GenerateHelper;
import ru.datana.benchmark.postgres.model.MultiSensorDataModel;
import ru.datana.benchmark.postgres.model.SensorData;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class SingleSensorToRowFiller extends AbstractFiller {

    @Getter
    @Setter
    private static long totalRowIndex;
    @Getter
    private static long rowCountMax;


    public SingleSensorToRowFiller(ToolsParameters parameters) throws SQLException {
        super(parameters);
    }

    @Override
    public void fillDatabase() throws SQLException {
        try {
            if (parameters.isForceRecreate()) datalakeRepository.createSingleSensorStructure();
            PreparedStatement p = datalakeRepository.createSQL();
            long start = System.currentTimeMillis();
            rowCountMax = parameters.getNumberOfPackages() * parameters.getPackageSize();
            totalRowIndex = 0;
            while (totalRowIndex < rowCountMax || rowCountMax <= 0) {
                fillSensorBlock(p);
            }
            long stop = System.currentTimeMillis();
            log.info("Time to generate " + totalRowIndex + " rows - " + (stop - start) + " ms");
        } finally {
            closeConnection();
        }
    }


    private void fillSensorBlock(PreparedStatement p) throws SQLException {
        List<SensorData> sensorList = new ArrayList(parameters.getNumberOfSensors());
        for (int s = 0; s < parameters.getNumberOfSensors(); s++) {
            var sensor = GenerateHelper.generateSensorData(s);
            sensorList.add(sensor);
        }
        MultiSensorDataModel m = new MultiSensorDataModel();
        m.setTechnicalData(GenerateHelper.generateTechnicalData(parameters.getMode() == ToolsParameters.ColumnMode.SINGLE));
        m.setSensorData(sensorList);

        datalakeRepository.insertData(p, m);
    }

}
