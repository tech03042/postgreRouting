package kr.co.kdelab.postgre.routing.tangcori.util;

import kr.co.kdelab.postgre.routing.parser.applier.TableApplier;
import kr.co.kdelab.postgre.routing.parser.internal.TestSetFormat;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class CustomApplier extends TableApplier {

    private int builderBufferSize = 40000000;
    private int pts, pv;
    private boolean usingFullTe;

    public CustomApplier(int pts, int pv, boolean usingFullTe) {
        this(-1);
        this.pts = pts;
        this.pv = pv;
        this.usingFullTe = usingFullTe;
    }

    private CustomApplier(int builderBufferSize) {
        super();
        if (builderBufferSize != -1)
            this.builderBufferSize = builderBufferSize;
    }

    //    0~pts
    @Override
    public void applyInTable(Connection connection) throws SQLException {
        final String createTeTail = "(fid int, tid int, cost int)";
        final String createTeHead = "create table IF NOT EXISTS te";

        connection.setAutoCommit(false);
        try (Statement statement = connection.createStatement()) {
            String baseSQL[] = new String[pts];
            StringBuilder appended[] = new StringBuilder[pts];
            for (int i = 0; i < pts; i++) {
                baseSQL[i] = String.format("INSERT INTO te_%d(fid, tid, cost) values", i + 1);
                appended[i] = new StringBuilder();


                statement.execute(createTeHead + "_" + (i + 1) + createTeTail);
                statement.execute("DELETE FROM TE_" + (i + 1));
            }

            String mainBaseSQL = null;
            StringBuilder mainAppended = null;
            if (usingFullTe) {
                statement.execute(createTeHead + createTeTail);
                statement.execute("DELETE FROM TE");

                mainBaseSQL = "INSERT INTO te(fid, tid, cost) values";
                mainAppended = new StringBuilder();
//            Main TE
            }


            TestSetFormat testSetFormat;
            while ((testSetFormat = testSetReader.readLine()) != null) {
                int index = (int) testSetFormat.getWeight() / pv;
                if (appended[index].length() != 0)
                    appended[index].append(",");
                appended[index].append("(").append(testSetFormat.getNodeId()).append(",").append(testSetFormat.getTargetId()).append(",").append((int) testSetFormat.getWeight()).append(")");

                if (appended[index].length() > builderBufferSize) {
                    statement.execute(baseSQL[index] + appended[index].toString());
                    appended[index] = new StringBuilder();
                    connection.commit();
                }
//                Partitioning Parts


                if (usingFullTe && mainAppended != null) {
                    if (mainAppended.length() != 0)
                        mainAppended.append(",");
                    mainAppended.append("(").append(testSetFormat.getNodeId()).append(",").append(testSetFormat.getTargetId()).append(",").append((int) testSetFormat.getWeight()).append(")");
                    if (mainAppended.length() > builderBufferSize) {
                        statement.execute(mainBaseSQL + mainAppended.toString());
                        mainAppended = new StringBuilder();
                        connection.commit();
                    }
                }

            }

            for (int i = 0; i < pts; i++)
                if (appended[i].length() != 0)
                    statement.execute(baseSQL[i] + appended[i].toString());

            if (usingFullTe && mainAppended != null) {
                if (mainAppended.length() != 0)
                    statement.execute(mainBaseSQL + mainAppended.toString());
            }
        }

        connection.commit();
        connection.setAutoCommit(true);

    }
}
