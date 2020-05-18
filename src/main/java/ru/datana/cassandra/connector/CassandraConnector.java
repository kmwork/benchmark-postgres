package ru.datana.cassandra.connector;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PlainTextAuthProvider;
import com.datastax.driver.core.Session;

import java.util.List;

public class CassandraConnector {
    private Cluster cluster;
    private Session session;

    public void connect(List<String> nodes, Integer port) {
        connect(nodes, port, null, null);
    }

    public void connect(List<String> nodes, Integer port, String login, String password) {
        Cluster.Builder builder = Cluster.builder();
        nodes.forEach(builder::addContactPoint);
        if (port != null) builder.withPort(port);
        if (login != null) builder.withAuthProvider(new PlainTextAuthProvider(login, password));
        cluster = builder.build();
        session = cluster.connect();
    }

    public Session getSession() {
        return this.session;
    }

    public void close() {
        if (session != null) session.close();
        if (cluster != null) cluster.close();
    }
}