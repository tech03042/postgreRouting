package kr.co.kdelab.postgre.routing.redfish.algo.impl.rbfs;

import kr.co.kdelab.postgre.routing.redfish.algo.ShortestPathRunner;
import kr.co.kdelab.postgre.routing.redfish.algo.impl.dataclass.RunningResult;
import kr.co.kdelab.postgre.routing.redfish.algo.impl.dataclass.RunningResultError;
import kr.co.kdelab.postgre.routing.redfish.algo.impl.dataclass.RunningResultSuccess;
import kr.co.kdelab.postgre.routing.redfish.algo.impl.parent.BidirectionImpl;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

public class BiRbfs extends ShortestPathRunner implements BidirectionImpl {
    private PreparedStatement[] minD2SOverlapStmt = new PreparedStatement[2];
    private PreparedStatement[] minD2SStmt = new PreparedStatement[2];
    private PreparedStatement verificationStmt;
    private PreparedStatement getMidNodeStmt; // Listing 4.6

    private int pts;
    private int pv;

    public void setPts(int pts) {
        this.pts = pts;
    }

    public void setPv(int pv) {
        this.pv = pv;
    }

    public BiRbfs(int pts, int pv) {
        super();
        this.pts = pts;
        this.pv = pv;
    }

    private final String algoTag = "Bi-R BFS";

    private void prepareStatement() throws SQLException {
        minD2SStmt[FORWARD] = addPreparedStatement(getConnection().prepareStatement("SELECT MIN(d2s) from ta0 WHERE fwd=?")); // Listing4.3
        minD2SStmt[BACKWARD] = addPreparedStatement(getConnection().prepareStatement("SELECT MIN(d2s) from ta1 WHERE fwd=?"));

//        minD2SOverlapStmtOrig = connection.prepareStatement("SELECT MIN(TA0.D2S+TA1.D2S) FROM TA0, TA1 WHERE TA0.nid = TA1.nid"); // Original
        minD2SOverlapStmt[FORWARD] = addPreparedStatement(getConnection().prepareStatement("SELECT MIN(TA0.D2S+TA1.D2S) FROM TA0, TA1 WHERE TA0.fwd=? and TA0.nid = TA1.nid")); // Optimization
        minD2SOverlapStmt[BACKWARD] = addPreparedStatement(getConnection().prepareStatement("SELECT MIN(TA0.D2S+TA1.D2S) FROM TA0, TA1 WHERE TA1.fwd=? and TA0.nid = TA1.nid")); // Optimization


        verificationStmt = addPreparedStatement(getConnection().prepareStatement("SELECT MIN(TA0.D2S+TE.COST+TA1.D2S) FROM TA0, TA1, TE WHERE TA0.nid=TE.fid and TA1.nid=TE.tid"));
        getMidNodeStmt = addPreparedStatement(getConnection().prepareStatement("SELECT TA0.nid from TA0, TA1 WHERE TA0.nid=TA1.nid and TA0.d2s+TA1.d2s = ?"));
    }

    @Override
    public RunningResult run() throws Exception {
        prepareStatement();


        int minCost = Integer.MAX_VALUE;
        int iteration[] = {1, 1}; // i, j
        int dist[] = {0, 0}; // lf_i, lf_j
        int affected[] = {1, 1}; // nf, nb;

        try (Statement statement = getConnection().createStatement()) {
            statement.execute("insert into ta0(nid,d2s,p2s,fwd) values ('" + getSource() + "',0,'" + getSource() + "',1)");
            statement.execute("insert into ta1(nid,d2s,p2s,fwd) values ('" + getTarget() + "',0,'" + getTarget() + "',1)");

        }

        long start = System.currentTimeMillis();
        while (dist[FORWARD] + dist[BACKWARD] <= minCost) {
            int direction = affected[0] <= affected[1] ? FORWARD : BACKWARD;
            int reverseDirection = direction == FORWARD ? BACKWARD : FORWARD;


            affected[direction] = expand(direction, iteration[direction], minCost, dist[reverseDirection]);

            int distTmp = getMinD2S(direction, iteration[direction]);
            if (iteration[direction] == 1)
                dist[direction] = Math.min(distTmp, pv);
            else {
                if (distTmp == 0)
                    dist[direction] = dist[direction] + pv;
                else
                    dist[direction] = distTmp;
            }

            int minCostTmp = getMinD2Overlap(direction, iteration[direction]);
            if (minCost > minCostTmp)
                minCost = minCostTmp;


            iteration[direction]++;
        }


        minCost = Math.min(minCost, verification());
        int midNode = getMidNode(minCost);
        // BACKWARD 로만 끝난 경우 S->M의 경로를 구할 필요 없음
        if (midNode == -1)
            return new RunningResultError(start, System.currentTimeMillis(), getSource(), getTarget(), iteration[FORWARD], iteration[BACKWARD], "PATH NOT FOUND", algoTag, minCost);

        ArrayList<Integer> path = new ArrayList<>(extractPath(getConnection(), midNode, FORWARD, true));
        path.remove(path.size() - 1); // 중간 노드가 겹침.
        path.addAll(extractPath(getConnection(), midNode, BACKWARD, true)); // FORWARD 로만 끝난 경우  M->T의 경로를 구할 필요 없음.


//        System.out.println(minCost);

        return new RunningResultSuccess(start, System.currentTimeMillis(), getSource(), getTarget(), iteration[FORWARD], iteration[BACKWARD], path.toString(), algoTag, minCost);

    }

    private int expand(int direction, int iteration, int minCost, int distanceReverse) throws SQLException {
//        Definition 5: Extended E-operator.
        int affected;
        int l = Math.max(1, iteration - pts + 1); // l be max(1, i − pts + 1),
        int idx = iteration - l + 1;
        int costExpression = minCost - distanceReverse;
//        int costExpression = minCost; // minCost - distReverse ... => some case make bug


        final String joinColumn = direction == FORWARD ? "fid" : "tid";
        final String joinColumnReverse = direction == FORWARD ? "tid" : "fid";
        final String taTableName = direction == FORWARD ? "ta0" : "ta1";

        final String[] expandMergePrebuilt = {
                "INSERT INTO " + taTableName + "(nid,d2s,p2s,fwd) (SELECT nid, cost, p2s, " + (iteration + 1) + " as fwd from (SELECT ER.ID as nid, ER.P2S as p2s, ER.D2S as cost, ROW_NUMBER() OVER (PARTITION BY id ORDER BY d2s ASC) AS rownum " +
                        "from ("
                , ") as ER(id, p2s, d2s)) as tmp WHERE rownum=1) ON CONFLICT(nid) DO UPDATE SET d2s=excluded.d2s, p2s=excluded.p2s, fwd=excluded.fwd where " + taTableName + ".d2s>excluded.d2s"};
        final String[] sqlPrebuilt = {
                "select TE." + joinColumnReverse + ", TE." + joinColumn + ", TF.D2S+TE.COST FROM " + taTableName + " as TF, te_",
                " as TE WHERE TF.fwd=",
                " and TF.nid=TE." + joinColumn + " and TF.D2S+TE.cost<" + costExpression //  and TF.nid=TE.fid and TF.D2S+TE.cost<
        };

        // TE UNION i-l to 0
        try (Statement statement = getConnection().createStatement()) {
            StringBuilder frontier = new StringBuilder();
            frontier.append(sqlPrebuilt[0]).append(idx--).append(sqlPrebuilt[1]).append(l++).append(sqlPrebuilt[2]);
            while (idx > 0)
                frontier.append(" union ").append(sqlPrebuilt[0]).append(idx--).append(sqlPrebuilt[1]).append(l++).append(sqlPrebuilt[2]);


//            System.out.println(expandMergePrebuilt[0] + frontier + expandMergePrebuilt[1]);
            //System.out.println(frontier.toString());
            affected = statement.executeUpdate(expandMergePrebuilt[0] + frontier + expandMergePrebuilt[1]);
        }

        return affected;
    }


    private int getMinD2Overlap(int direction, int iteration) throws SQLException {
        int minD2 = Integer.MAX_VALUE;
        minD2SOverlapStmt[direction].setInt(1, iteration + 1);
        try (ResultSet rs = minD2SOverlapStmt[direction].executeQuery()) {
            if (rs.next()) {
                int tmp = rs.getInt(1);
                if (tmp != 0) {
                    minD2 = tmp;
                }
            }
        }
        return minD2;
    }


    // Listing 4.3
    private int getMinD2S(int direction, int iteration) throws SQLException {
        minD2SStmt[direction].setInt(1, iteration + 1);
        try (ResultSet resultSet = minD2SStmt[direction].executeQuery()) {
            if (resultSet.next()) {
                return resultSet.getInt(1);
            }
        }
        return 0;
    }

//    S e l e c t min ( d2s+cost+d2t ) from TA f TF , TA b TB , TE
//    where TF . nid=TE . fid and TE . tid=TB . nid

    //    Listing 4.5
    private int verification() throws SQLException {
        try (ResultSet resultSet = verificationStmt.executeQuery()) {
            if (resultSet.next()) {
                return resultSet.getInt(1);
            }
        }
        return 0;
    }

    //    Listing 4.6
    private int getMidNode(int minCost) throws SQLException {
        getMidNodeStmt.setInt(1, minCost);
        try (ResultSet resultSet = getMidNodeStmt.executeQuery()) {
            if (resultSet.next()) {
                return resultSet.getInt(1);
            }
        }
        return -1;
    }
}
