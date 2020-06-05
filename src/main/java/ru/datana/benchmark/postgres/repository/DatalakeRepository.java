package ru.datana.benchmark.postgres.repository;


import lombok.extern.slf4j.Slf4j;
import ru.datana.benchmark.postgres.ToolsParameters;
import ru.datana.benchmark.postgres.filler.SingleSensorToRowFiller;
import ru.datana.benchmark.postgres.model.MultiSensorDataModel;
import ru.datana.benchmark.postgres.model.SensorData;
import ru.datana.benchmark.postgres.model.TechnicalData;

import java.sql.*;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class DatalakeRepository {
    private static final String SENSOR_DATA_TYPE_NAME = "sensor_data";
    private static final String SINGLE_SENSOR_TABLE_NAME = "controllers_tasks_data_responses";
    private static final String MULTI_SENSOR_TABLE_NAME = "controllers_tasks_data_responses_multi";
    private static final DateTimeFormatter isoDate = DateTimeFormatter.ISO_DATE;
    private final ToolsParameters parameters;

    protected final Connection connection;
    protected final String schemaName;

    public DatalakeRepository(ToolsParameters parameters, Connection connection, String schemaName) {
        this.parameters = parameters;
        this.connection = connection;
        this.schemaName = schemaName;
    }

    /**
     * Создание структуры на 1 датчик на строку
     */
    public void createSingleSensorStructure() throws SQLException {

        String dataAsText = parameters.getMode() == ToolsParameters.ColumnMode.SINGLE ? "data decimal null," : "data hstore null,";
        StringBuilder tableBuilder = new StringBuilder("CREATE TABLE IF NOT EXISTS ")
                .append(schemaName).append(".").append(SINGLE_SENSOR_TABLE_NAME)
                .append("(")
                .append("partition_date DATE NULL,")
                .append("partition_hour SMALLINT NULL,")
                .append("partition_minute SMALLINT NULL,")
                .append("request_id UUID NULL,")
                .append("controller_id UUID NULL,")
                .append("task_id UUID NULL,")
                .append("request_datetime TIMESTAMP NULL,")
                .append("sensor_id UUID NULL,")
                .append("request_datetime_proxy TIMESTAMP NULL,")
                .append("response_datetime TIMESTAMP NULL,")
                .append(dataAsText)
                .append("PRIMARY KEY (request_datetime, sensor_id)")
                .append(");");
        try (Statement st = connection.createStatement()) {
            log.debug("[SQL:Create] sql = " + tableBuilder);
            st.execute(tableBuilder.toString());
        }
    }

    /**
     * Вставка значения единичной выборки датчиков в строку
     */
    public PreparedStatement createSQL() throws SQLException {
        StringBuilder sb = new StringBuilder(1024);
        String dataType = parameters.getMode() == ToolsParameters.ColumnMode.SINGLE ? "?" : "cast(? as hstore)";
        sb.append("INSERT INTO ")
                .append(schemaName).append(".").append(SINGLE_SENSOR_TABLE_NAME).append("(")
                .append("partition_date,")
                .append("partition_hour,")
                .append("partition_minute,")
                .append("request_id,")
                .append("controller_id,")
                .append("task_id,")
                .append("request_datetime,")
                .append("sensor_id,")
                .append("request_datetime_proxy,")
                .append("response_datetime,")
                .append("data")
                .append(")")
                .append(" VALUES (?,?,?,?,?,?,?,?,?,?,")
                .append(dataType).append(");");

        log.debug("[SQL:Prepared-Insert] sql = " + sb);
        return connection.prepareStatement(sb.toString());
    }

    private String generateHStore(MultiSensorDataModel m) {
        StringBuilder sb = new StringBuilder(parameters.getNumberOfSensors() * 512);
        for (SensorData sensorDataSingle : m.getSensorData()) {

            int index = 0;
            for (var sd : m.getSensorData()) {

                if (sb.length() != 0)
                    sb.append(",");

                index++;

                Map<String, Object> sensorMap = new HashMap<>();
                sb.append("\"").append(index).append("_sensor_id\" => \"").append(sd.getSensorId()).append("\"");
                sb.append(", \"").append(index).append("_data\" => \"").append(sd.getData()).append("\"");
                sb.append(", \"").append(index).append("_controller_datetime\" => \"").append(sd.getControllerDatetime().getTime()).append("\"");
                sb.append(", \"").append(index).append("_status\" => \"").append(sd.getStatus()).append("\"");
                sb.append(", \"").append(index).append("_errors\" => \"").append(sd.getErrors().toString()).append("\"");

            }

        }
        return sb.toString();
    }

    private void setParamsToSQL(PreparedStatement p, TechnicalData t, SensorData sensorDataSingle) throws SQLException {
        p.setTimestamp(1, new java.sql.Timestamp(t.getResponseDatetime().getTime()));
        p.setInt(2, t.getResponseDatetime().toLocalDateTime().getHour());
        p.setInt(3, t.getResponseDatetime().toLocalDateTime().getMinute());
        p.setObject(4, t.getRequestId().toString(), Types.OTHER);
        p.setObject(5, t.getControllerId().toString(), Types.OTHER);
        p.setObject(6, t.getTaskId().toString(), Types.OTHER);
        p.setTimestamp(7, new java.sql.Timestamp(t.getRequestDatetime().getTime()));
        p.setObject(8, sensorDataSingle.getSensorId().toString(), Types.OTHER);
        p.setTimestamp(9, new java.sql.Timestamp(t.getRequestDatetimeProxy().getTime()));
        p.setTimestamp(10, new java.sql.Timestamp(t.getResponseDatetime().getTime()));
    }

    private boolean addBatch(PreparedStatement p) throws SQLException {
        p.addBatch();

        long sqlCount = SingleSensorToRowFiller.getTotalRowIndex();
        sqlCount++;
        SingleSensorToRowFiller.setTotalRowIndex(sqlCount);

        long maxCount = SingleSensorToRowFiller.getRowCountMax();
        boolean isLimit = maxCount > 0 && sqlCount >= maxCount;

        long rest = sqlCount % parameters.getPackageSize();
        if (rest == 0 || isLimit) {
            p.executeBatch();
            log.info("[SQL:Batch-Insert] size of batch = {}, all rows = {}, isLimit = {}", sqlCount / parameters.getPackageSize(), sqlCount, isLimit);
        }


        return isLimit;
    }

    public void insertData(PreparedStatement p, MultiSensorDataModel m) throws SQLException {

        if (parameters.getMode() == ToolsParameters.ColumnMode.MULTI) {
            String hstore = generateHStore(m);
            p.setString(11, hstore);
            setParamsToSQL(p, m.getTechnicalData(), m.getSensorData().get(0));
            addBatch(p);
        } else
            for (SensorData sensorDataSingle : m.getSensorData()) {
                setParamsToSQL(p, m.getTechnicalData(), sensorDataSingle);
                p.setDouble(11, sensorDataSingle.getData());
                boolean isLimit = addBatch(p);
                if (isLimit) break;
            }
    }

}
