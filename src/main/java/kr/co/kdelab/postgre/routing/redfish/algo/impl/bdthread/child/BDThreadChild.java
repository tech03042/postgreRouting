package kr.co.kdelab.postgre.routing.redfish.algo.impl.bdthread.child;

import kr.co.kdelab.postgre.routing.redfish.algo.impl.dataclass.MergeResult;
import kr.co.kdelab.postgre.routing.redfish.util.JDBConnectionInfo;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public abstract class BDThreadChild extends Thread {

    public BDThreadChild(JDBConnectionInfo jdbConnectionInfo, String tableNameTA, String tableNameTE) {
        this.jdbConnectionInfo = jdbConnectionInfo;
        this.tableNameTA = tableNameTA;
        this.tableNameTE = tableNameTE;
    }

    private String tableNameTA;
    private String tableNameTE;
    private boolean isTermination = false;

    private JDBConnectionInfo jdbConnectionInfo;

    private int iteration, dist, affected = Integer.MAX_VALUE;

    private int lastCheckIteration = 0;

    public int getIteration() {
        return iteration;
    }

    public int getDist() {
        return dist;
    }

    public int getAffected() {
        return affected;
    }

    public boolean isTermination() {
        return isTermination;
    }

    public void terminate() {
        isTermination = true;
    }

    private MergeResult p_mergeResult = new MergeResult(0, 0);

    String getTableNameTA() {
        return tableNameTA;
    }

    String getTableNameTE() {
        return tableNameTE;
    }

    public abstract String getFrontierSql();

    // PreparedStatement.Params = ( Frontier.d2s, fwd, Frontier.nid );
    public abstract String getExpandMergeSql();


    @Override
    public void run() {
        super.run();

        try (Connection connection = jdbConnectionInfo.createConnection()) {
//            PreparedStatement checkAllCheckedFF = connection.prepareStatement("SELECT nid FROM " + tableNameTA + " WHERE f=false limit 1");
            PreparedStatement frontier = connection.prepareStatement(
                    getFrontierSql());
            PreparedStatement expandMerge = connection.prepareStatement(getExpandMergeSql());

            while (!isTermination) {
                MergeResult mergeResult = fem(frontier, expandMerge, iteration);

                if (mergeResult == null) {
                    isTermination = true;
                    break;
                }

                affected = mergeResult.affected;
                dist = mergeResult.minCost;

                iteration++;
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        isTermination = true;
    }

    // IF TA TABLE IS EMPTY TERMINATE
    private MergeResult fem(PreparedStatement frontier, PreparedStatement expandMerge, int iteration) throws SQLException {
        int nid, d2s;
        try (ResultSet resultSet = frontier.executeQuery()) {
            if (resultSet.next()) {
                nid = resultSet.getInt(1);
                d2s = resultSet.getInt(2);
            } else
                return null;
        }


        expandMerge.setInt(1, d2s);
        expandMerge.setInt(2, iteration + 1);
        expandMerge.setInt(3, nid);

        try (ResultSet resultSet = expandMerge.executeQuery()) {
            if (resultSet.next()) {
                p_mergeResult.affected = resultSet.getInt(1);
                p_mergeResult.minCost = d2s;
                return p_mergeResult;
            }
        }
        return null;
    }

    public int getLastCheckIteration() {
        return lastCheckIteration;
    }

    public void setLastCheckIteration(int lastCheckIteration) {
        this.lastCheckIteration = lastCheckIteration;
    }

}
