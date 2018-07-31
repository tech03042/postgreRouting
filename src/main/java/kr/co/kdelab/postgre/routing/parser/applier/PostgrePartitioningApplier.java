package kr.co.kdelab.postgre.routing.parser.applier;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class PostgrePartitioningApplier extends TableApplier {

    private int builderBufferSize = 20000000;
    private int pts, pv;

    public PostgrePartitioningApplier(int pts, int pv) {
        this(-1);
        this.pts = pts;
        this.pv = pv;
    }

    private PostgrePartitioningApplier(int builderBufferSize) {
        super();
        if (builderBufferSize != -1)
            this.builderBufferSize = builderBufferSize;
    }

    //    0~pts
    @Override
    public void applyInTable(Connection connection) throws SQLException {
        connection.setAutoCommit(false);
        PostgreApplier postgreApplier = new PostgreApplier(builderBufferSize);
        try (Statement statement = connection.createStatement()) {
            postgreApplier.teApply(connection, statement, getReader());
            for (int i = 0; i < pts; i++) {
                long min = pv * i, max = pv * (i + 1);
                int tePartitionNumber = i + 1;
                statement.execute("DROP TABLE IF EXISTS TE_" + tePartitionNumber + " CASCADE");
                statement.execute("CREATE UNLOGGED TABLE TE_" + tePartitionNumber + "(fid int, tid int, cost int, PRIMARY KEY (fid, tid))");
                statement.execute("INSERT INTO TE_" + tePartitionNumber + "(fid, tid, cost) SELECT fid, tid, cost FROM te WHERE cost >= " + min + " AND cost <=" + max + " ON CONFLICT DO NOTHING ");
                connection.commit();
            }
            connection.commit();
        }


        connection.setAutoCommit(true);

    }
}
