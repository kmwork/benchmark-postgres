package ru.datana.cassandra.repository;

import lombok.AllArgsConstructor;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

@AllArgsConstructor
public class SchemaRepository {
    private Connection session;

    public void createSchema(String schemaName, String replicationStrategy, int replicationFactor) throws SQLException {
        StringBuilder sb = new StringBuilder("CREATE SCHEMA IF NOT EXISTS ")
                .append(schemaName)
                .append(";");

        String query = sb.toString();
        try(Statement stm = session.createStatement()) {
            stm.execute(query.toString());
        }
    }
}
