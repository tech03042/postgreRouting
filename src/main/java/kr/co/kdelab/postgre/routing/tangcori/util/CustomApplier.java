package kr.co.kdelab.postgre.routing.tangcori.util;

import kr.co.kdelab.postgre.routing.parser.applier.TableApplier;
import kr.co.kdelab.postgre.routing.parser.internal.TestSetFormat;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class CustomApplier extends TableApplier {

    private int builderBufferSize = 40000000;
    private int pts, pv;

    public CustomApplier(int pts, int pv) {
        this(-1);
        this.pts = pts;
        this.pv = pv;
    }

    private CustomApplier(int builderBufferSize) {
        super();
        if (builderBufferSize != -1)
            this.builderBufferSize = builderBufferSize;
    }

    //    0~pts
    @Override
    public void applyInTable(Connection connection) throws SQLException {
        StringBuilder baseSQL = new StringBuilder("INSERT INTO TE(fid, tid, cost) values");
        StringBuilder appended = new StringBuilder();
        String tail = "ON CONFLICT (fid, tid) DO NOTHING";

        connection.setAutoCommit(false);

        try (Statement statement = connection.createStatement()) {
            statement.execute("DROP TABLE IF EXISTS TE CASCADE");
            statement.execute("CREATE UNLOGGED TABLE TE(fid int, tid int, cost int, PRIMARY KEY (fid, tid))");

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
            connection.commit();

            for (int i = 0; i < pts; i++) {
                long min = pv * i, max = pv * (i + 1);
                statement.execute("CREATE UNLOGGED TABLE TE_" + (i + 1) + "(fid int, tid int, cost int, PRIMARY KEY (fid, tid))");
                statement.execute("INSERT INTO TE_" + (i + 1) + "(fid, tid, cost) SELECT fid, tid, cost FROM te WHERE cost >= " + min + " AND cost <=" + max + " ON CONFLICT DO NOTHING ");
            }
            connection.commit();
        }


        connection.setAutoCommit(true);
    }
}
