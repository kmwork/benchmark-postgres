package ru.datana.cassandra.repository;

import com.datastax.driver.core.LocalDate;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import lombok.AllArgsConstructor;
import ru.datana.cassandra.model.MultiSensorDataModel;
import ru.datana.cassandra.model.SingleSensorDataModel;

import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@SuppressWarnings("StringBufferReplaceableByString")
@AllArgsConstructor
public class DatalakeRepository {
    private static final String SENSOR_DATA_TYPE_NAME = "sensor_data";
    private static final String SINGLE_SENSOR_TABLE_NAME = "controllers_tasks_data_responses";
    private static final String MULTI_SENSOR_TABLE_NAME = "controllers_tasks_data_responses_multi";
    private static final DateTimeFormatter isoDate = DateTimeFormatter.ISO_DATE;

    protected Session session;
    protected String schemaName;

    /**
     * Создание структуры на 1 датчик на строку
     */
    public void createSingleSensorStructure(boolean forceDrop) {
        StringBuilder dropTable = new StringBuilder("DROP TABLE IF EXISTS ")
                .append(schemaName).append(".").append(SINGLE_SENSOR_TABLE_NAME);

        StringBuilder tableBuilder = createStringBuilderForTableWithTechnicalPart(SINGLE_SENSOR_TABLE_NAME)
                .append("sensor_id UUID,")
                .append("data DOUBLE,")
                .append("controller_datetime TIMESTAMP,")
                .append("status SMALLINT,")
                .append("errors VARCHAR(4000),")
                .append("PRIMARY KEY (partition_date, partition_hour, partition_minute)")
                .append(");");

        if (forceDrop) session.execute(dropTable.toString());
        session.execute(tableBuilder.toString());
    }

    /**
     * Создание структуры на множество датчиков на строку.
     *
     * @param numberOfSensors количество датчиков в строке
     * @param forceDrop       нужно ли удалять структуру перед созданием, обязательно, если количество столбцов меняется
     */
    public void createMultiSensorStructure(int numberOfSensors, boolean forceDrop) {
        StringBuilder dropType = new StringBuilder("DROP TABLE IF EXISTS ")
                .append(schemaName).append(".").append(SENSOR_DATA_TYPE_NAME);


        StringBuilder typeBuilder = new StringBuilder("CREATE TABLE IF NOT EXISTS ")
                .append(schemaName).append(".").append(SENSOR_DATA_TYPE_NAME).append("(")
                .append("sensor_id UUID,")
                .append("data DOUBLE,")
                .append("controller_datetime TIMESTAMP,")
                .append("status SMALLINT,")
                .append("errors VARCHAR(4000),")
                .append("sensor_map hstore")
                .append(");");

        StringBuilder dropTable = new StringBuilder("DROP TABLE IF EXISTS ")
                .append(schemaName).append(".").append(MULTI_SENSOR_TABLE_NAME);

        if (forceDrop) {
            session.execute(dropTable.toString());
            session.execute(dropType.toString());
        }
        session.execute(typeBuilder.toString());
    }

    /**
     * Вставка значения единичного датчика в строку
     *
     * @param sensorData данные датчика
     */
    @Deprecated
    public void insertSingleSensorData(SingleSensorDataModel sensorData) {
        session.execute(prepareSingleSensorDataInsert(sensorData));
    }

    /**
     * Вставка значений датчиков в строки пакетом
     *
     * @param sensorDataList - данные датчиков списком
     */
    @Deprecated
    public void insertSingleSensorDataPackage(List<SingleSensorDataModel> sensorDataList) {
        StringBuilder sb = new StringBuilder("BEGIN BATCH ");
        sensorDataList.forEach(sensorData -> sb.append(prepareSingleSensorDataInsert(sensorData)));
        sb.append("APPLY BATCH;");
        session.execute(sb.toString());
    }

    /**
     * Вставка значения единичной выборки датчиков в строку
     *
     * @param sensorData данные выборки датчиков
     */
    public void insertMultiSensorData(MultiSensorDataModel sensorData) {

        StringBuilder sb = new StringBuilder("INSERT INTO ")
                .append(schemaName).append(".").append(MULTI_SENSOR_TABLE_NAME).append("(")
                .append("partition_date,")
                .append("partition_hour,")
                .append("partition_minute,")
                .append("request_id,")
                .append("controller_id,")
                .append("task_id,")
                .append("request_datetime,")
                .append("request_datetime_proxy,")
                .append("response_datetime,")
                .append("sensor_map");
        sb.append(")")
                .append(" VALUES (")
                .append("'").append(sensorData.getTechnicalData().getResponseDatetime().toLocalDateTime().format(isoDate)).append("',")
                .append(sensorData.getTechnicalData().getResponseDatetime().toLocalDateTime().getHour()).append(",")
                .append(sensorData.getTechnicalData().getResponseDatetime().toLocalDateTime().getMinute()).append(",")
                .append(sensorData.getTechnicalData().getRequestId().toString()).append(",")
                .append(sensorData.getTechnicalData().getControllerId().toString()).append(",")
                .append(sensorData.getTechnicalData().getTaskId().toString()).append(",")
                .append(sensorData.getTechnicalData().getRequestDatetime().getTime()).append(",")
                .append(sensorData.getTechnicalData().getRequestDatetimeProxy().getTime()).append(",")
                .append(sensorData.getTechnicalData().getResponseDatetime().getTime());
        sensorData.getSensorData().forEach(sensor -> sb
                .append(", '")
                .append("\"sensor_id\" => ").append(sensor.getSensorId().toString())
                .append(", \"data\" =>").append(sensor.getData())
                .append(", \"controller_datetime\" =>").append(sensor.getControllerDatetime().getTime())
                .append(", \"status\" =>").append(sensor.getStatus())
                .append(", \"errors\" =>").append(getErrorsAsString(sensor.getErrors()))
                .append("'")
        );
        sb.append(");");
        session.execute(sb.toString());
    }

    public PreparedStatement createPreparedStatementForSingleSensorDataPackage(int packageSize) {
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
        return session.prepare(sb.toString());
    }

    public void insertSingleSensorDataPackageWithPreparedStatement(PreparedStatement preparedStatement, List<SingleSensorDataModel> sensorDataList) {
        Object[] params = new Object[sensorDataList.size() * 14];
        var ref = new Object() {
            int i = 0;
        };
        sensorDataList.forEach(sensorData -> {
            params[ref.i++] = LocalDate.fromMillisSinceEpoch(sensorData.getTechnicalData().getResponseDatetime().getTime());
            params[ref.i++] = (byte) sensorData.getTechnicalData().getResponseDatetime().toLocalDateTime().getHour();
            params[ref.i++] = (byte) sensorData.getTechnicalData().getResponseDatetime().toLocalDateTime().getMinute();
            params[ref.i++] = sensorData.getTechnicalData().getRequestId();
            params[ref.i++] = sensorData.getTechnicalData().getControllerId();
            params[ref.i++] = sensorData.getTechnicalData().getTaskId();
            params[ref.i++] = sensorData.getTechnicalData().getRequestDatetime();
            params[ref.i++] = sensorData.getTechnicalData().getRequestDatetimeProxy();
            params[ref.i++] = sensorData.getTechnicalData().getResponseDatetime();
            params[ref.i++] = sensorData.getSensorData().getSensorId();
            params[ref.i++] = sensorData.getSensorData().getData();
            params[ref.i++] = sensorData.getSensorData().getControllerDatetime();
            params[ref.i++] = sensorData.getSensorData().getStatus();
            params[ref.i++] = sensorData.getSensorData().getErrors();
        });
        session.execute(preparedStatement.bind(params));
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
