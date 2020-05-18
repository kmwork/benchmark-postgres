package ru.datana.cassandra.repository;

import com.datastax.driver.core.Session;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class SchemaRepository {
    private Session session;

    public void createSchema(String keyspaceName, String replicationStrategy, int replicationFactor) {
        StringBuilder sb = new StringBuilder("CREATE SCHEMA IF NOT EXISTS ")
                .append(keyspaceName)
                .append(";");

        String query = sb.toString();
        session.execute(query);
    }
}
