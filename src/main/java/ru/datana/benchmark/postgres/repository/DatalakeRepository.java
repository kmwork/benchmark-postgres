package ru.datana.benchmark.postgres.repository;


import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import ru.datana.benchmark.postgres.model.SensorData;
import ru.datana.benchmark.postgres.model.SingleSensorDataModel;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
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
                .append("PRIMARY KEY (partition_date, partition_hour, partition_minute)")
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
    public void insertSingleSensorDataPackage(List<SingleSensorDataModel> sensorDataList) throws SQLException {
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
                .append("sensor_id,")
                .append("data,")
                .append("controller_datetime,")
                .append("status,")
                .append("errors")
                .append(")")
                .append(" VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?);")
        );
        return connection.prepareStatement(sb.toString());
    }

    public void insertSingleSensorDataPackageWithPreparedStatement(PreparedStatement preparedStatement, List<SingleSensorDataModel> sensorDataList) throws SQLException {
        var ref = new Object() {
            int i = 1;
        };
        sensorDataList.forEach(sensorData -> {
            try {
                SensorData sd = sensorData.getSensorData();
                StringBuilder sb = new StringBuilder(1024);
                sb
                        .append("'")
                        .append("\"sensor_id\" => \"").append(sd.getSensorId()).append("\"")
                        .append(", \"data\" =>\"").append(sd.getData()).append("\"")
                        .append(", \"controller_datetime\" => \"").append(sd.getControllerDatetime().getTime()).append("\"")
                        .append(", \"status\" => \"").append(sd.getStatus()).append("\"")
                        .append(", \"errors\" => \"").append(getErrorsAsString(sd.getErrors())).append("\"")
                        .append("'");

                preparedStatement.setTimestamp(ref.i++, new java.sql.Timestamp(sensorData.getTechnicalData().getResponseDatetime().getTime()));
                preparedStatement.setInt(ref.i++, sensorData.getTechnicalData().getResponseDatetime().toLocalDateTime().getHour());
                preparedStatement.setInt(ref.i++, sensorData.getTechnicalData().getResponseDatetime().toLocalDateTime().getMinute());
                preparedStatement.setLong(ref.i++, sensorData.getTechnicalData().getRequestId());
                preparedStatement.setLong(ref.i++, sensorData.getTechnicalData().getControllerId());
                preparedStatement.setLong(ref.i++, sensorData.getTechnicalData().getTaskId());
                preparedStatement.setTimestamp(ref.i++, new java.sql.Timestamp(sensorData.getTechnicalData().getRequestDatetime().getTime()));
                preparedStatement.setTimestamp(ref.i++, new java.sql.Timestamp(sensorData.getTechnicalData().getRequestDatetimeProxy().getTime()));
                preparedStatement.setTimestamp(ref.i++, new java.sql.Timestamp(sensorData.getTechnicalData().getResponseDatetime().getTime()));
                preparedStatement.setLong(ref.i++, sensorData.getSensorData().getSensorId());
                preparedStatement.setString(ref.i++, sb.toString());
                preparedStatement.setTimestamp(ref.i++, new java.sql.Timestamp(sensorData.getSensorData().getControllerDatetime().getTime()));
                preparedStatement.setInt(ref.i++, sensorData.getSensorData().getStatus());
                preparedStatement.setString(ref.i++, sensorData.getSensorData().getErrors().toString());
            } catch (SQLException e) {
                String msg = "Error  in insertSingleSensorDataPackageWithPreparedStatement  of lamda i = " + ref.i;
                System.err.println(msg);
                e.printStackTrace();
                throw new RuntimeException(msg, e);
            }
        });
        preparedStatement.execute();
    }

    //------------------------- private block -------------------------

    @Deprecated
    private String getErrorsAsString(Set<String> errors) {
        return errors.stream()
                .map(error -> "'" + error + "'")
                .collect(Collectors.joining(",", "{", "}"));
    }


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
