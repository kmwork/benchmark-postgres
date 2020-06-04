package ru.datana.benchmark.postgres.repository;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

@Slf4j
@AllArgsConstructor
public class SchemaRepository {
    private Connection connection;

    public void createSchema(String schemaName) throws SQLException {
        StringBuilder sb = new StringBuilder("CREATE SCHEMA IF NOT EXISTS ")
                .append(schemaName)
                .append(";");

        String query = sb.toString();
        try (Statement stm = connection.createStatement()) {
            stm.execute(query);
        }
    }

    public void dropSchema(String schemaName) throws SQLException {
        StringBuilder sb = new StringBuilder("DROP SCHEMA IF EXISTS ")
                .append(schemaName)
                .append(" CASCADE;");

        String query = sb.toString();
        try (Statement stm = connection.createStatement()) {
            log.debug("[SQL:Drop] sql = " + query);
            stm.execute(query);
        }
    }
}
