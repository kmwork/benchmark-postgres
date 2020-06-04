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
            PreparedStatement p = datalakeRepository.createSQL();
            long start = System.currentTimeMillis();
            long rowCountMax = parameters.getNumberOfPackages() * parameters.getPackageSize();
            totalRowIndex = 0;
            while (totalRowIndex < rowCountMax || rowCountMax <= 0) {
                fillSensorBlock(p);
                ref.totallyInserted += parameters.getPackageSize();
                ref.insertedBeforeLog += parameters.getPackageSize();
                if (ref.insertedBeforeLog >= 100000) {
                    ref.insertedBeforeLog %= 100000;
                    log.info((ref.totallyInserted / 1000) + "к");
                }
            }
            long stop = System.currentTimeMillis();
            log.info("Time to generate " + ref.totallyInserted + " rows - " + (stop - start) + " ms");
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
