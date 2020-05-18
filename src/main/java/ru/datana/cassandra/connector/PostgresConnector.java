package ru.datana.cassandra.connector;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;

@Slf4j
public class PostgresConnector {
    @Getter
    private Connection connection;

    public void connect(List<String> nodes, Integer port) {
        connect(nodes, port, null, null);
    }

    public void connect(List<String> nodes, Integer port, String login, String password) {

        log.debug("Testing connection to PostgreSQL JDBC");

        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            String msg = "PostgreSQL JDBC Driver is not found. Include it in your library path";
            log.error(msg, e);
            throw new RuntimeException(msg, e);
        }

        log.debug("PostgreSQL JDBC Driver successfully connected");
        Connection connection = null;

        String url = "jdbc:postgresql://" + nodes + ":" + port + "/postgres";
        try {
            connection = DriverManager.getConnection(url, login, password);

        } catch (SQLException e) {
            String msg = "Connection Failed";
            log.error(msg, e);
            throw new RuntimeException(msg, e);

        }

        if (connection != null) {
            log.info("You successfully connected to database now");
        } else {
            String msg = "Failed to make connection to database";
            throw new RuntimeException(msg);

        }
    }


    public void close() throws SQLException {
        if (connection != null) connection.close();
    }
//}
//
//
//
//
//        //  Database credentials
//        static final String DB_URL = "jdbc:postgresql://127.0.0.1:5432/postgres"
//        static final String USER = "username";
//        static final String PASS = "1";
//
//        public static void main(String[] argv) {
//
//        }
//    }
}