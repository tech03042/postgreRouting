package kr.co.kdelab.postgre.routing.redfish.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class JDBConnectionInfo {
    private String url;
    private String user;
    private String password;
    private String schema;
    private String database;

    public JDBConnectionInfo(String url, String user, String password, String schema) {
        this.url = url;
        this.user = user;
        this.password = password;
        this.schema = schema;
    }

    public JDBConnectionInfo(String url, String user, String password, String schema, String database) {
        this.url = url + database;
        this.user = user;
        this.password = password;
        this.schema = schema;
        this.database = database;
    }

    public Connection createConnection() throws SQLException {
        Connection connection = DriverManager.getConnection(url, user, password);
        connection.setSchema(schema);
        return connection;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }
}
