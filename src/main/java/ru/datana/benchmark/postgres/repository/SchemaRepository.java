package ru.datana.benchmark.postgres.repository;

import lombok.AllArgsConstructor;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

@AllArgsConstructor
public class SchemaRepository {
    private Connection connection;

    public void createSchema(String schemaName) throws SQLException {
        StringBuilder sb = new StringBuilder("CREATE SCHEMA IF NOT EXISTS ")
                .append(schemaName)
                .append(";");

        String query = sb.toString();
        try(Statement stm = connection.createStatement()) {
            stm.execute(query.toString());
        }
    }

    public void dropSchema(String schemaName) throws SQLException {
        StringBuilder sb = new StringBuilder("DROP SCHEMA CASCADE IF NOT EXISTS ")
                .append(schemaName)
                .append(";");

        String query = sb.toString();
        try(Statement stm = connection.createStatement()) {
            stm.execute(query.toString());
        }
    }
}
