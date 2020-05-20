package ru.datana.benchmark.postgres.repository;


import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import ru.datana.benchmark.postgres.model.MultiSensorDataModel;
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
                .append("sensor_id UUID,")
                .append("data DOUBLE,")
                .append("controller_datetime TIMESTAMP,")
                .append("status SMALLINT,")
                .append("errors VARCHAR(4000),")
                .append("PRIMARY KEY (partition_date, partition_hour, partition_minute)")
                .append(");");
        try (Statement st = connection.createStatement()) {
            st.execute(tableBuilder.toString());
        }
    }

    /**
     * Создание структуры на множество датчиков на строку.
     *
     * @param numberOfSensors количество датчиков в строке
     */
    public void createMultiSensorStructure(int numberOfSensors) throws SQLException {

        StringBuilder typeBuilder = new StringBuilder("CREATE TABLE IF NOT EXISTS ")
                .append(schemaName).append(".").append(SENSOR_DATA_TYPE_NAME).append("(")
                .append("sensor_id UUID,")
                .append("data double precision,")
                .append("controller_datetime TIMESTAMP,")
                .append("status SMALLINT,")
                .append("errors VARCHAR(4000),")
                .append("sensor_map hstore")
                .append(");");


        try (Statement st = connection.createStatement()) {
            log.debug("[SQL:Create] sql = "+typeBuilder);
            st.execute(typeBuilder.toString());
        }
    }

    /**
     * Вставка значения единичного датчика в строку
     *
     * @param sensorData данные датчика
     */
    @Deprecated
    public void insertSingleSensorData(SingleSensorDataModel sensorData) throws SQLException {
        try (Statement st = connection.createStatement()) {
            st.execute(prepareSingleSensorDataInsert(sensorData));
        }
    }

    /**
     * Вставка значений датчиков в строки пакетом
     *
     * @param sensorDataList - данные датчиков списком
     */
    @Deprecated
    public void insertSingleSensorDataPackage(List<SingleSensorDataModel> sensorDataList) throws SQLException {
        StringBuilder sb = new StringBuilder("BEGIN");
        sensorDataList.forEach(sensorData -> sb.append(prepareSingleSensorDataInsert(sensorData)));
        sb.append("APPLY BATCH;");
        try (Statement st = connection.createStatement()) {
            st.execute(sb.toString());
        }
    }

    /**
     * Вставка значения единичной выборки датчиков в строку
     *
     * @param sensorData данные выборки датчиков
     */




    public PreparedStatement createPreparedStatementForSingleSensorDataPackage(int packageSize) throws SQLException {
        StringBuilder sb = new StringBuilder("BEGIN BATCH ");
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
        sb.append("APPLY BATCH;");
        return connection.prepareStatement(sb.toString());
    }

    @SneakyThrows
    public void insertSingleSensorDataPackageWithPreparedStatement(PreparedStatement preparedStatement, List<SingleSensorDataModel> sensorDataList) {
        Object[] params = new Object[sensorDataList.size() * 14];
        var ref = new Object() {
            int i = 0;
        };
        sensorDataList.forEach(sensorData -> {
            try {
                preparedStatement.setTimestamp(ref.i++, new java.sql.Timestamp(sensorData.getTechnicalData().getResponseDatetime().getTime()));
                preparedStatement.setInt(ref.i++, sensorData.getTechnicalData().getResponseDatetime().toLocalDateTime().getHour());
                preparedStatement.setInt(ref.i++, sensorData.getTechnicalData().getResponseDatetime().toLocalDateTime().getMinute());
                preparedStatement.setString(ref.i++, sensorData.getTechnicalData().getRequestId().toString());
                preparedStatement.setString(ref.i++, sensorData.getTechnicalData().getControllerId().toString());
                preparedStatement.setString(ref.i++, sensorData.getTechnicalData().getTaskId().toString());
                preparedStatement.setTimestamp(ref.i++, new java.sql.Timestamp(sensorData.getTechnicalData().getRequestDatetime().getTime()));
                preparedStatement.setTimestamp(ref.i++, new java.sql.Timestamp(sensorData.getTechnicalData().getRequestDatetimeProxy().getTime()));
                preparedStatement.setTimestamp(ref.i++, new java.sql.Timestamp(sensorData.getTechnicalData().getResponseDatetime().getTime()));
                preparedStatement.setString(ref.i++, sensorData.getSensorData().getSensorId().toString());
                preparedStatement.setDouble(ref.i++, sensorData.getSensorData().getData());
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

    @Deprecated
    private String prepareSingleSensorDataInsert(SingleSensorDataModel sensorData) {
        return new StringBuilder("INSERT INTO ")
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
                .append(" VALUES (")
                .append("'").append(sensorData.getTechnicalData().getResponseDatetime().toLocalDateTime().format(isoDate)).append("',")
                .append(sensorData.getTechnicalData().getResponseDatetime().toLocalDateTime().getHour()).append(",")
                .append(sensorData.getTechnicalData().getResponseDatetime().toLocalDateTime().getMinute()).append(",")
                .append(sensorData.getTechnicalData().getRequestId().toString()).append(",")
                .append(sensorData.getTechnicalData().getControllerId().toString()).append(",")
                .append(sensorData.getTechnicalData().getTaskId().toString()).append(",")
                .append(sensorData.getTechnicalData().getRequestDatetime().getTime()).append(",")
                .append(sensorData.getTechnicalData().getRequestDatetimeProxy().getTime()).append(",")
                .append(sensorData.getTechnicalData().getResponseDatetime().getTime()).append(",")
                .append(sensorData.getSensorData().getSensorId().toString()).append(",")
                .append(sensorData.getSensorData().getData()).append(",")
                .append(sensorData.getSensorData().getControllerDatetime().getTime()).append(",")
                .append(sensorData.getSensorData().getStatus()).append(",")
                .append(getErrorsAsString(sensorData.getSensorData().getErrors()))
                .append(");").toString();
    }

    private StringBuilder createStringBuilderForTableWithTechnicalPart(String tableName) {
        return new StringBuilder("CREATE TABLE IF NOT EXISTS ")
                .append(schemaName).append(".").append(tableName)
                .append("(")
                .append("partition_date DATE,")
                .append("partition_hour TINYINT,")
                .append("partition_minute TINYINT,")
                .append("request_id UUID,")
                .append("controller_id UUID,")
                .append("task_id UUID,")
                .append("request_datetime TIMESTAMP,")
                .append("request_datetime_proxy TIMESTAMP,")
                .append("response_datetime TIMESTAMP,");
    }
}
