package kr.co.kdelab.postgre.routing;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class KDELabConnectionClass implements AutoCloseable {
    private final String dbURL = "jdbc:postgresql://localhost:5432/";
    private final String dbUser = "postgres";
    private final String dbPassword = "icdwvb4j";
    private final String databaseName = "kdelab";
    private final String schemaName = "kdelab";
    private final Connection connection;

    public static void main(String[] args) {
        String sql =
                "WITH fop AS " +
                        "(UPDATE " + getTableNameTA() + " set f = true where nid = (select nid from " + getTableNameTA() + " where f=false and d2s=(select min(d2s) from " + getTableNameTA() + " where f=false) limit 1)" +
                        " RETURNING nid,d2s) " +
                        ",mop AS (INSERT INTO " + getTableNameTA() + "(nid, d2s, p2s, fwd, f) " +
                        "( SELECT tid as nid, (cost+fop.d2s) as d2s, fid as p2s, ? as fwd, false as f" +
                        " FROM " + getTableNameTE() + ", fop " +
                        "WHERE fid=fop.nid ) ON CONFLICT(nid) DO UPDATE SET d2s=excluded.d2s, p2s=excluded.p2s, fwd=excluded.fwd,f=excluded.f" +
                        " WHERE " + getTableNameTA() + ".d2s>excluded.d2s RETURNING nid, d2s)" +
                        "SELECT count(nid) as affected, min(d2s) as mind2s FROM mop;";
        System.out.println(sql);
    }

    public static String getTableNameTA() {
        return "ta";
    }

    public static String getTableNameTE() {
        return "te";
    }

    public KDELabConnectionClass(boolean dropSchema) throws SQLException {
        connection = DriverManager.getConnection(dbURL + databaseName, dbUser, dbPassword);
        if (dropSchema) {
            try (Statement statement = connection.createStatement()) {
                statement.execute("DROP SCHEMA IF EXISTS " + schemaName + " CASCADE");
                statement.execute("CREATE SCHEMA " + schemaName);
            }
        }
        connection.setSchema(schemaName);
    }


    public Connection createNewConnection() throws SQLException {
        Connection connection = DriverManager.getConnection(dbURL + databaseName, dbUser, dbPassword);
        connection.setSchema(schemaName);
        return connection;
    }


    public String getSchemaName() {
        return schemaName;
    }

    public Connection getConnection() {
        return connection;
    }

    @Override
    public void close() throws Exception {
        try {
            connection.close();
        } catch (Exception ignored) {

        }
    }
}
