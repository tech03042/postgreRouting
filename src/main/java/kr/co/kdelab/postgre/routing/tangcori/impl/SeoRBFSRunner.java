package kr.co.kdelab.postgre.routing.tangcori.impl;

import kr.co.kdelab.postgre.routing.redfish.algo.ShortestPathRunner;
import kr.co.kdelab.postgre.routing.redfish.algo.impl.dataclass.RunningResult;
import kr.co.kdelab.postgre.routing.redfish.algo.impl.dataclass.RunningResultError;
import kr.co.kdelab.postgre.routing.redfish.algo.impl.dataclass.RunningResultSuccess;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;

public abstract class SeoRBFSRunner extends ShortestPathRunner {

    private static final int FORWARD = 0;
    private static final int BACKWARD = 1;

    private PreparedStatement distSofar[] = new PreparedStatement[2];
    private PreparedStatement min_cost;
    private PreparedStatement locate_xid;
    private PreparedStatement verification;


    private int pts;
    private int pv;

    public int getPts() {
        return pts;
    }

    public int getPv() {
        return pv;
    }

    public SeoRBFSRunner(int pts, int pv) {
        this.pts = pts;
        this.pv = pv;
    }

    private void prepare() throws SQLException {
        distSofar[FORWARD] = addPreparedStatement(getConnection().prepareStatement("select  min(d2s) from ta where fwd = ?"));
        distSofar[BACKWARD] = addPreparedStatement(getConnection().prepareStatement("select  min(d2s) from ta2 where fwd = ?"));

        min_cost = addPreparedStatement(getConnection().prepareStatement(""
                + "select  min(TF.d2s+TB.d2s) "
                + "from ta as TF, ta2 as TB "
                + "where TF.nid = TB.nid "));


        locate_xid = getConnection().prepareStatement("SELECT TF.nid FROM ta as TF,ta2 as TB WHERE TF.nid = TB.nid " +
                "AND TF.d2s+TB.d2s = ?");

        verification = getConnection().prepareStatement(""
                + "select min(TF.d2s+cost+TB.d2s) "
                + "from ta as TF, ta2 as TB , TE "
                + "where TF.nid=TE.fid AND TE.tid=TB.nid "
        );
    }

    @Override
    public RunningResult run() throws Exception {
        long start_t = System.currentTimeMillis();

        float minCost = Integer.MAX_VALUE; // minCost
        float minCost0 = 0;
        float minCost1;

        float dist[] = new float[]{0, 0};
        int affected[] = new int[]{1, 1};
        int iteration[] = new int[]{1, 1};

        long loopCount[] = new long[]{0, 0};
        int innerCounter[] = new int[]{0, 0};


        int xid;
        try (Statement statement = getConnection().createStatement()) {
            statement.execute(
                    "insert into ta(nid,d2s,p2s,fwd) values (" + getSource() + ",0," + getSource() + ",1)");
            statement.execute(
                    "insert into ta2(nid,d2s,p2s,fwd) values (" + getTarget() + ",0," + getTarget() + ",1)");

        }


        while (dist[FORWARD] + dist[BACKWARD] <= minCost) {

            if (affected[FORWARD] == Integer.MAX_VALUE && affected[BACKWARD] == Integer.MAX_VALUE)
                break;

            int currentDirection = BACKWARD;
            int reverseDirection = FORWARD;
            if (affected[FORWARD] <= affected[BACKWARD]) {
                currentDirection = FORWARD;
                reverseDirection = BACKWARD;
            }
            affected[currentDirection] = perform(iteration[currentDirection], minCost, dist[reverseDirection], currentDirection == FORWARD);
            dist[currentDirection] = dist_sofar_RBFS(iteration[currentDirection], dist[currentDirection], currentDirection);
            iteration[currentDirection]++;
            loopCount[currentDirection]++;
            if (affected[currentDirection] == 0) {
                innerCounter[currentDirection]++;
                if ((innerCounter[currentDirection] == pts)) {
                    affected[currentDirection] = Integer.MAX_VALUE;
                    innerCounter[currentDirection] = 0;
                }
            } else {
                innerCounter[currentDirection] = 0;
            }


            // calculate minCost0
            minCost0 = minCost();
            if (minCost0 != 0) {
                minCost = minCost0;
            }

        } // end while

        // verification ( calculate minCost1 )
        minCost1 = verification();

        // Formula 2
        minCost = Math.min(minCost1, minCost0);

        //System.out.println("minCost="+minCost);
        // locate xid
        xid = locateXid(minCost);

        //System.out.println("xid="+xid);
        List<Integer> p1 = extractPathToXid(getSource(), getTarget(), xid, true);
        List<Integer> p2 = extractPathToXid(getSource(), getTarget(), xid, false);
//        RBFS_Process_T = System.currentTimeMillis() - start_t;
        if (xid == -1)
            return new RunningResultError("XID NOT FOUND");
        List<Integer> path = new LinkedList<>();
        path.addAll(p1);
        path.addAll(p2);
        return new RunningResultSuccess(start_t, System.currentTimeMillis(), getSource(), getTarget(), (int) loopCount[FORWARD], (int) loopCount[BACKWARD], path.toString(), "Seo-RBFS", (long) minCost);
    }

    abstract int perform(int iteration, float minCost, float dist_sofar, boolean isForward) throws Exception;

    private int locateXid(float minCost) throws SQLException {
        int xid = -1;
        locate_xid.setInt(1, (int) minCost);
        try (ResultSet rs = locate_xid.executeQuery()) {
            while (rs.next()) {
                xid = rs.getInt(1);
            }
        }
        return xid;
    }

    private float verification() throws SQLException {
        float minCost1 = -1;
        try (ResultSet rs = verification.executeQuery()) {
            while (rs.next()) {
                minCost1 = rs.getInt(1);
            }
        }
        return minCost1;
    }


    private float minCost() throws SQLException {
        float min = -1;

        try (ResultSet rs = min_cost.executeQuery()) {
            if (rs.next())
                min = rs.getInt(1);

        }
        return min;
    }


    private List<Integer> extractPathToXid(int source, int target, int xid, boolean dir) {
        List<Integer> path = new LinkedList<>();
        if (dir)
            path.add(xid);

        int current_node = xid;
        int sub_p2s;

        try {
            PreparedStatement subPath;
            if (dir)
                subPath = getConnection().prepareStatement("select p2s from ta where nid =  ?");
            else
                subPath = getConnection().prepareStatement("select p2s from ta2 where nid = ?");

            ResultSet rs;
            if (dir) {
                while (true) {
                    subPath.setInt(1, current_node);
                    rs = subPath.executeQuery();
                    while (rs.next()) {
                        sub_p2s = rs.getInt(1);
                        current_node = sub_p2s;

                        path.add(0, sub_p2s);
                        if (sub_p2s == source) {
                            rs.close();
                            return path;
                        }
                    }
                }
            } else {
                while (true) {
                    subPath.setInt(1, current_node);
                    rs = subPath.executeQuery();
                    while (rs.next()) {
                        sub_p2s = rs.getInt(1);
                        current_node = sub_p2s;

                        path.add(sub_p2s);
                        if (sub_p2s == target) {
                            rs.close();
                            return path;
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return path;
    }// end of method extractPathToXid

    private float dist_sofar_RBFS(int iteration, float previous_dist, int currentDirection) throws SQLException {
        int min_d2s = 0;
        PreparedStatement ps = distSofar[currentDirection];
        ps.setInt(1, iteration + 1);

        try (ResultSet rs = ps.executeQuery()) {

            if (rs.next()) {
                min_d2s = rs.getInt(1);
                if (min_d2s == 0) {
                    min_d2s = Integer.MAX_VALUE;
                }
            }
        }
        if (iteration == 1) {
            return Math.min(min_d2s, pv);
        } else {
            return Math.min(min_d2s, previous_dist + pv);
        }

    }

}
