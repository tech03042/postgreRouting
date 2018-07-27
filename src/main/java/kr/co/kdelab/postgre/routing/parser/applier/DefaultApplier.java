package kr.co.kdelab.postgre.routing.parser.applier;

import kr.co.kdelab.postgre.routing.parser.internal.TestSet;
import kr.co.kdelab.postgre.routing.parser.internal.TestSetFormat;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class DefaultApplier extends TableApplier {
    public DefaultApplier(TestSet testSet) {
        super(testSet);
    }

    @Override
    public void applyInTable(Connection connection) throws SQLException {
        int batchCount = 0;
        connection.setAutoCommit(false);


        TestSetFormat dataPlaced = null;
        try (PreparedStatement insertPlaced = connection.prepareStatement("INSERT INTO TE(fid, tid, cost) VALUES(?, ?, ?)")) {
            while ((dataPlaced = testSetReader.readLine()) != null) {
                batchCount++;
//                if (dataPlaced.getWeight() == 143)
//                    System.out.println(dataPlaced.getWeight());
                insertPlaced.setInt(1, dataPlaced.getNodeId());
                insertPlaced.setInt(2, dataPlaced.getTargetId());
                insertPlaced.setDouble(3, dataPlaced.getWeight());
                insertPlaced.addBatch();

                if (batchCount > 1000000) {
                    batchCount = 0;
                    insertPlaced.executeBatch();
                }
            }
            insertPlaced.executeBatch();
        }

        connection.commit();


        connection.setAutoCommit(true);
    }
}
