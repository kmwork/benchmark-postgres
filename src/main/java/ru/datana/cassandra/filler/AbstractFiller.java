package ru.datana.cassandra.filler;

import ru.datana.cassandra.ToolsParameters;
import ru.datana.cassandra.connector.PostgresConnector;
import ru.datana.cassandra.repository.DatalakeRepository;
import ru.datana.cassandra.repository.SchemaRepository;

import java.sql.Connection;
import java.util.List;

public abstract class AbstractFiller {
    protected PostgresConnector client;
    protected SchemaRepository schemaRepository;
    protected DatalakeRepository datalakeRepository;
    protected Connection connection;

    protected void connect(List<String> nodes, Integer port, String keyspaceName) {
        client = new PostgresConnector();
        client.connect(nodes, port);
        session = client.getSession();
        schemaRepository = new SchemaRepository(session);
        datalakeRepository = new DatalakeRepository(session, keyspaceName);
    }

    protected void closeConnection() {
        if (client != null) client.close();
    }

    public abstract void fillDatabase(ToolsParameters parameters);
}
