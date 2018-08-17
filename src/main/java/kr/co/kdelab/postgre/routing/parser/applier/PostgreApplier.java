package kr.co.kdelab.postgre.routing.parser.applier;

import kr.co.kdelab.postgre.routing.parser.internal.TestSet;
import kr.co.kdelab.postgre.routing.parser.internal.TestSetFormat;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class PostgreApplier extends TableApplier {

    private int builderBufferSize = 20000000;

    public PostgreApplier() {
        this(-1);
    }

    PostgreApplier(int builderBufferSize) {
        super();
        if (builderBufferSize != -1)
            this.builderBufferSize = builderBufferSize;
    }

    void teApply(Connection connection, Statement statement, TestSet testSet) throws SQLException {
        StringBuilder baseSQL = new StringBuilder("INSERT INTO TE(fid, tid, cost) values");
        StringBuilder appended = new StringBuilder();
        String tail = "ON CONFLICT (fid, tid) DO NOTHING";

        statement.execute("DROP TABLE IF EXISTS TE CASCADE");
        statement.execute("CREATE UNLOGGED TABLE TE(fid int, tid int, cost int, PRIMARY KEY (fid, tid))");

        TestSetFormat testSetFormat;
        while ((testSetFormat = testSet.readLine()) != null) {
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

        statement.execute("DELETE FROM TE WHERE fid=tid");
        connection.commit();

        statement.execute("CREATE INDEX IF NOT EXISTS TE_FID_IDX ON te USING hash(fid)");
        statement.execute("CREATE INDEX IF NOT EXISTS TE_TID_IDX ON te USING hash(tid)");
        connection.commit();
    }

    @Override
    public void applyInTable(Connection connection) throws SQLException {
        connection.setAutoCommit(false);

        try (Statement statement = connection.createStatement()) {
            teApply(connection, statement, getReader());
        }

        connection.commit();
        connection.setAutoCommit(true);

    }
}
