package ru.datana.benchmark.postgres.repository;


import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import ru.datana.benchmark.postgres.model.MultiSensorDataModel;
import ru.datana.benchmark.postgres.model.TechnicalData;

import java.sql.*;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

@Slf4j
@AllArgsConstructor
public class DatalakeRepository {
    private static final String SENSOR_DATA_TYPE_NAME = "sensor_data";
    private static final String SINGLE_SENSOR_TABLE_NAME = "controllers_tasks_data_responses";
    private static final String MULTI_SENSOR_TABLE_NAME = "controllers_tasks_data_responses_multi";
    private static final DateTimeFormatter isoDate = DateTimeFormatter.ISO_DATE;

    protected Connection connection;
    protected String schemaName;

    /**
     * Создание структуры на 1 датчик на строку
     */
    public void createSingleSensorStructure() throws SQLException {

        StringBuilder tableBuilder = createStringBuilderForTableWithTechnicalPart(SINGLE_SENSOR_TABLE_NAME)
                .append("sensor_id bigint,")
                .append("data hstore,")
                .append("controller_datetime TIMESTAMP,")
                .append("status SMALLINT,")
                .append("errors VARCHAR(4000),")
                .append("PRIMARY KEY (partition_date, partition_hour, partition_minute, sensor_id)")
                .append(");");
        try (Statement st = connection.createStatement()) {
            log.debug("[SQL:Create] sql = " + tableBuilder);
            st.execute(tableBuilder.toString());
        }
    }


    /**
     * Вставка значений датчиков в строки пакетом
     *
     * @param sensorDataList - данные датчиков списком
     */
    public void insertSingleSensorDataPackage(List<MultiSensorDataModel> sensorDataList) throws SQLException {
        PreparedStatement p = createPreparedStatementForSingleSensorDataPackage(sensorDataList.size());
        insertSingleSensorDataPackageWithPreparedStatement(p, sensorDataList);
    }

    /**
     * Вставка значения единичной выборки датчиков в строку
     */


    public PreparedStatement createPreparedStatementForSingleSensorDataPackage(int packageSize) throws SQLException {
        StringBuilder sb = new StringBuilder(1024);
        IntStream.range(0, packageSize).forEach(i -> sb.append("INSERT INTO ")
                .append(schemaName).append(".").append(SINGLE_SENSOR_TABLE_NAME).append("(")
                .append("partition_date,")
                .append("partition_hour,")
                .append("partition_minute,")
                .append("request_id,")
                .append("controller_id,")
                .append("task_id,")
                .append("request_datetime,")
                .append("request_datetime_proxy,")
                .append("response_datetime,")
                .append("data,")
                .append(")")
                .append(" VALUES (?,?,?,?,?,?,?,?,?,?);")
        );
        return connection.prepareStatement(sb.toString());
    }

    public void insertSingleSensorDataPackageWithPreparedStatement(PreparedStatement preparedStatement, List<MultiSensorDataModel> sensorDataList) throws SQLException {
        for (MultiSensorDataModel m : sensorDataList) {
            StringBuilder sb = new StringBuilder(sensorDataList.size()*512);
            sb.append("'");
            int index = 0;
            for (var sd : m.getSensorData()) {
                index++;
                Map<String, Object> sensorMap = new HashMap<>();
                sb.append("\"").append(index).append("_sensor_id\" => \"").append(sd.getSensorId()).append("\"");
                sb.append(", \"").append(index).append("_data\" => \"").append(sd.getData()).append("\"");
                sb.append(", \"").append(index).append("_controller_datetime\" => \"").append(sd.getControllerDatetime().getTime()).append("\"");
                sb.append(", \"").append(index).append("_status\" => \"").append(sd.getStatus()).append("\"");
                sb.append(", \"").append(index).append("_errors\" => \"").append(sd.getErrors().toString()).append("\"");
            }
            sb.append("'");
            int paramIndex = 1;

            TechnicalData t = m.getTechnicalData();
            preparedStatement.setTimestamp(paramIndex++, new java.sql.Timestamp(t.getResponseDatetime().getTime()));
            preparedStatement.setInt(paramIndex++, t.getResponseDatetime().toLocalDateTime().getHour());
            preparedStatement.setInt(paramIndex++, t.getResponseDatetime().toLocalDateTime().getMinute());
            preparedStatement.setLong(paramIndex++, t.getRequestId());
            preparedStatement.setLong(paramIndex++, t.getControllerId());
            preparedStatement.setLong(paramIndex++, t.getTaskId());
            preparedStatement.setTimestamp(paramIndex++, new java.sql.Timestamp(t.getRequestDatetime().getTime()));
            preparedStatement.setTimestamp(paramIndex++, new java.sql.Timestamp(t.getRequestDatetimeProxy().getTime()));
            preparedStatement.setTimestamp(paramIndex++, new java.sql.Timestamp(t.getResponseDatetime().getTime()));
            preparedStatement.setString(paramIndex++, sb.toString());

            preparedStatement.execute();
        }
    }

//------------------------- private block -------------------------


    private StringBuilder createStringBuilderForTableWithTechnicalPart(String tableName) {
        return new StringBuilder("CREATE TABLE IF NOT EXISTS ")
                .append(schemaName).append(".").append(tableName)
                .append("(")
                .append("partition_date DATE,")
                .append("partition_hour SMALLINT,")
                .append("partition_minute SMALLINT,")
                .append("request_id bigint,")
                .append("controller_id bigint,")
                .append("task_id bigint,")
                .append("request_datetime TIMESTAMP,")
                .append("request_datetime_proxy TIMESTAMP,")
                .append("response_datetime TIMESTAMP,");
    }
}
