package ru.datana.benchmark.postgres.filler;

import ru.datana.benchmark.postgres.ToolsParameters;
import ru.datana.benchmark.postgres.connector.PostgresConnector;
import ru.datana.benchmark.postgres.repository.DatalakeRepository;
import ru.datana.benchmark.postgres.repository.SchemaRepository;

import java.sql.Connection;
import java.sql.SQLException;

public abstract class AbstractFiller {
    protected PostgresConnector client;
    protected SchemaRepository schemaRepository;
    protected DatalakeRepository datalakeRepository;
    protected Connection connection;
    protected ToolsParameters parameters;

    public AbstractFiller(ToolsParameters parameters) throws SQLException {
        this.parameters = parameters;
        connect(parameters.getHost(), parameters.getPort(), parameters.getLogin(), parameters.getPassword(), parameters.getSchema());
        createSchema();
    }


    private void connect(String host, Integer port, String login, String password, String schemaName) {
        client = new PostgresConnector();
        client.connect(host, port, login, password);
        connection = client.getConnection();
        schemaRepository = new SchemaRepository(connection);
        datalakeRepository = new DatalakeRepository(connection, schemaName);
    }

    protected void closeConnection() throws SQLException {
        if (client != null) client.close();
    }

    public void createSchema() throws SQLException {
        if (parameters.isForceRecreate())
            schemaRepository.dropSchema(parameters.getSchema());
        schemaRepository.createSchema(parameters.getSchema());
    }

    public abstract void fillDatabase() throws SQLException;
}
