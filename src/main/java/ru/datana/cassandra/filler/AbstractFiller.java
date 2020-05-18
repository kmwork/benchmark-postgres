package ru.datana.cassandra.filler;

import com.datastax.driver.core.Session;
import ru.datana.cassandra.ToolsParameters;
import ru.datana.cassandra.connector.CassandraConnector;
import ru.datana.cassandra.repository.DatalakeRepository;
import ru.datana.cassandra.repository.SchemaRepository;

import java.util.List;

public abstract class AbstractFiller {
    protected CassandraConnector client;
    protected SchemaRepository schemaRepository;
    protected DatalakeRepository datalakeRepository;
    protected Session session;

    protected void connect(List<String> nodes, Integer port, String keyspaceName) {
        client = new CassandraConnector();
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
