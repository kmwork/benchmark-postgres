package ru.datana.benchmark.postgres.repository;


import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import ru.datana.benchmark.postgres.model.MultiSensorDataModel;
import ru.datana.benchmark.postgres.model.TechnicalData;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.format.DateTimeFormatter;
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

        StringBuilder tableBuilder = new StringBuilder("CREATE TABLE IF NOT EXISTS ")
                .append(schemaName).append(".").append(SINGLE_SENSOR_TABLE_NAME)
                .append("(")
                .append("partition_date DATE NULL,")
                .append("partition_hour SMALLINT NULL,")
                .append("partition_minute SMALLINT NULL,")
                .append("request_id bigint NULL,")
                .append("controller_id bigint NULL,")
                .append("task_id bigint NULL,")
                .append("request_datetime TIMESTAMP NULL,")
                .append("request_datetime_proxy TIMESTAMP NULL,")
                .append("response_datetime TIMESTAMP NULL,")
                .append("data hstore null,")
                .append("PRIMARY KEY (partition_date, partition_hour, partition_minute, request_id)")
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
                .append("data")
                .append(")")
                .append(" VALUES (?,?,?,?,?,?,?,?,?,?);")

        );
        log.debug("[SQL:Prepared-Insert] sql = "+sb);
        return connection.prepareStatement(sb.toString());
    }

    public void insertSingleSensorDataPackageWithPreparedStatement(PreparedStatement preparedStatement, List<MultiSensorDataModel> sensorDataList) throws SQLException {
        log.info("[SQL:Insert] size of batch = " + sensorDataList.size());
        for (MultiSensorDataModel m : sensorDataList) {
            StringBuilder sb = new StringBuilder(sensorDataList.size() * 512);
            sb.append("'");
            int index = 0;
            for (var sd : m.getSensorData()) {

                if (index != 0)
                    sb.append(",");
                index++;

                Map<String, Object> sensorMap = new HashMap<>();
                sb.append("\"").append(index).append("_sensor_id\" => \"").append(sd.getSensorId()).append("\"");
                sb.append(", \"").append(index).append("_data\" => \"").append(sd.getData()).append("\"");
                sb.append(", \"").append(index).append("_controller_datetime\" => \"").append(sd.getControllerDatetime().getTime()).append("\"");
                sb.append(", \"").append(index).append("_status\" => \"").append(sd.getStatus()).append("\"");
                sb.append(", \"").append(index).append("_errors\" => \"").append(sd.getErrors().toString()).append("\"");
            }
            sb.append("'");

            log.debug("[Insert:Data] m = " + m);

            TechnicalData t = m.getTechnicalData();

            preparedStatement.setTimestamp(1, new java.sql.Timestamp(t.getResponseDatetime().getTime()));
            preparedStatement.setInt(2, t.getResponseDatetime().toLocalDateTime().getHour());
            preparedStatement.setInt(3, t.getResponseDatetime().toLocalDateTime().getMinute());
            preparedStatement.setLong(4, t.getRequestId());
            preparedStatement.setLong(5, t.getControllerId());
            preparedStatement.setLong(6, t.getTaskId());
            preparedStatement.setTimestamp(7, new java.sql.Timestamp(t.getRequestDatetime().getTime()));
            preparedStatement.setTimestamp(8, new java.sql.Timestamp(t.getRequestDatetimeProxy().getTime()));
            preparedStatement.setTimestamp(9, new java.sql.Timestamp(t.getResponseDatetime().getTime()));
            preparedStatement.setString(10, sb.toString());

            preparedStatement.execute();
        }
    }

//------------------------- private block -------------------------



}
