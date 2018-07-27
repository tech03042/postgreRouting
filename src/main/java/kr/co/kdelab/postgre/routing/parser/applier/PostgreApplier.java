package kr.co.kdelab.postgre.routing.parser.applier;

import kr.co.kdelab.postgre.routing.parser.internal.TestSetFormat;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class PostgreApplier extends TableApplier {

    //    private String dataName;
    private String tableName;
    private int builderBufferSize = 20000000;

    public PostgreApplier() {
        this(-1, "TE");
    }

    public PostgreApplier(String tableName) {
        this(-1, tableName);
    }

    public PostgreApplier(int builderBufferSize, String tableName) {
        super();
        this.tableName = tableName;
//        this.dataName = dataName;
        if (builderBufferSize != -1)
            this.builderBufferSize = builderBufferSize;
    }

    @Override
    public void applyInTable(Connection connection) throws SQLException {
        StringBuilder baseSQL = new StringBuilder("INSERT INTO " + tableName + "(fid, tid, cost) values");
        StringBuilder appended = new StringBuilder();
        String tail = "ON CONFLICT (fid, tid) DO NOTHING";

        connection.setAutoCommit(false);

        try (Statement statement = connection.createStatement()) {
            statement.execute("DROP TABLE IF EXISTS " + tableName + " CASCADE");
            statement.execute("CREATE TABLE " + tableName + "(fid int, tid int, cost int, PRIMARY KEY (fid, tid))");

            TestSetFormat testSetFormat;
            while ((testSetFormat = testSetReader.readLine()) != null) {
                if (appended.length() != 0)
                    appended.append(",");
                appended.append("(").append(testSetFormat.getNodeId()).append(",").append(testSetFormat.getTargetId()).append(",").append((int) testSetFormat.getWeight()).append(")");

                if (appended.length() > builderBufferSize) {
                    statement.execute(baseSQL.toString() + appended.toString() + tail);
                    appended = new StringBuilder();
                    connection.commit();
                }
            }

            if (appended.length() != 0) {
                statement.execute(baseSQL.toString() + appended.toString() + tail);
            }
        }

        connection.commit();
        connection.setAutoCommit(true);

    }
}
