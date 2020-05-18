package ru.datana.cassandra.connector;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;

@Slf4j
public class PostgresConnector {
    private Connection session;

    public void connect(List<String> nodes, Integer port) {
        connect(nodes, port, null, null);
    }

    public void connect(List<String> nodes, Integer port, String login, String password) {

        log.debug("Testing connection to PostgreSQL JDBC");

        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            System.out.println("PostgreSQL JDBC Driver is not found. Include it in your library path ");
            e.printStackTrace();
            return;
        }

        System.out.println("PostgreSQL JDBC Driver successfully connected");
        Connection connection = null;

        String url = "jdbc:postgresql://" + nodes + ":" + port + "/postgres";
        try {
            connection = DriverManager
                    .getConnection(url, login, password);

        } catch (SQLException e) {
            log.debug("Connection Failed");
            e.printStackTrace();
            return;
        }

        if (connection != null) {
            log.debug("You successfully connected to database now");
        } else {
            log.debug("Failed to make connection to database");
        }
    }

    public Session getSession() {
        return this.session;
    }

    public void close() {
        if (session != null) session.close();
        if (cluster != null) cluster.close();
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