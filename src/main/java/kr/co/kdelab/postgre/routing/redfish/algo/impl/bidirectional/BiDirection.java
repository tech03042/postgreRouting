package kr.co.kdelab.postgre.routing.redfish.algo.impl.bidirectional;

import kr.co.kdelab.postgre.routing.redfish.algo.ShortestPathRunner;
import kr.co.kdelab.postgre.routing.redfish.algo.impl.dataclass.*;
import kr.co.kdelab.postgre.routing.redfish.algo.impl.parent.BidirectionImpl;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;

public class BiDirection extends ShortestPathRunner implements BidirectionImpl, SingleThreadImpl {

    private PreparedStatement[] stmtSelectFrontier = new PreparedStatement[2];
    private PreparedStatement[] stmtSetVisited = new PreparedStatement[2];
    private PreparedStatement[] stmtIsVisited = new PreparedStatement[2];
    private PreparedStatement[] stmtERMergeOP = new PreparedStatement[2];

    private PreparedStatement stmtEquallyExpandedCost = null;

    private void createStatement() throws SQLException {
        stmtSelectFrontier[FORWARD] = addPreparedStatement(
                getConnection().prepareStatement("select nid,d2s from (select * from ta where nid not in ( select nid from ta where f=true ) ) as ta order by d2s asc limit 1")
        );
        stmtSelectFrontier[BACKWARD] = addPreparedStatement(
                getConnection().prepareStatement(
                        "select nid,d2s from (select * from ta2 where nid not in ( select nid from ta2 where f=true ) )  as ta2 order by d2s asc limit 1"));

        stmtERMergeOP[FORWARD] = addPreparedStatement(
                getConnection().prepareStatement("INSERT INTO ta(nid, d2s, p2s, fwd, f) (SELECT tid as nid, cost+? as d2s, ? as p2s, ? as fwd, false as f FROM TE WHERE fid=?) ON CONFLICT(nid)" +
                        "DO UPDATE SET  d2s=excluded.d2s, p2s=excluded.p2s, fwd=excluded.fwd,f=excluded.f " +
                        "WHERE ta.d2s>excluded.d2s"));
        stmtERMergeOP[BACKWARD] = addPreparedStatement(
                getConnection().prepareStatement("INSERT INTO ta2(nid, d2s, p2s, fwd, f) (SELECT tid as nid, cost+? as d2s, ? as p2s, ? as fwd, false as f FROM TE_B WHERE fid=?) ON CONFLICT(nid)" +
                        "DO UPDATE SET  d2s=excluded.d2s, p2s=excluded.p2s, fwd=excluded.fwd,f=excluded.f " +
                        "WHERE ta2.d2s>excluded.d2s"));


        stmtSetVisited[FORWARD] = addPreparedStatement(
                getConnection().prepareStatement("update ta set f = true where nid = ?"));
        stmtSetVisited[BACKWARD] = addPreparedStatement(
                getConnection().prepareStatement("update ta2 set f = true where nid = ?"));

        stmtEquallyExpandedCost = addPreparedStatement(
                getConnection().prepareStatement("select min(TF.d2s+TB.d2s) from ta as TF, ta2 as TB where TF.nid = TB.nid;"));

        stmtIsVisited[FORWARD] = addPreparedStatement(
                getConnection().prepareStatement("select nid from ta where nid = ? and f = true"));
        stmtIsVisited[BACKWARD] = addPreparedStatement(
                getConnection().prepareStatement("select nid from ta2 where nid = ? and f = true"));


    }

    @Override
    public RunningResult run() throws Exception {
        List<Integer> path = new LinkedList<>();
        createStatement();

        long start = System.currentTimeMillis();

        try (Statement statement = getConnection().createStatement()) {
            statement.execute("insert into ta(nid,d2s,p2s,fwd,f) values ('" + getSource() + "',0,'"
                    + getSource() + "',1,false)");
            statement.execute("insert into ta2(nid,d2s,p2s,fwd,f) values ('" + getTarget() + "',0,'"
                    + getTarget() + "',1,false)");
        }

        long minCost = Long.MAX_VALUE;
        long dist[] = {0, 0};
        int affected[] = {1, 1};
        int iteration[] = {1, 1};

        int shortFinish = -1;

        while (dist[FORWARD] + dist[BACKWARD] <= minCost) {
//            System.out.println(dist[FORWARD] + dist[BACKWARD]);
//            System.out.println(minCost);

            if (affected[FORWARD] == Integer.MAX_VALUE && affected[BACKWARD] == Integer.MAX_VALUE) {
                break;
            }

            boolean isForward = affected[FORWARD] <= affected[BACKWARD];
            int direction = isForward ? FORWARD : BACKWARD;
            int reverse = isForward ? BACKWARD : FORWARD;
            if (affected[reverse] != Integer.MAX_VALUE)
                affected[reverse]--;

            MergeResult mergeResult = singleFEM(stmtERMergeOP[direction], iteration[direction], direction);
            affected[direction] = mergeResult.affected;
            if (mergeResult.affected != Integer.MAX_VALUE) {
                if (mergeResult.minCost != 0)
                    dist[direction] = mergeResult.minCost;
            }
            iteration[direction]++;

            minCost = getMinCost(stmtEquallyExpandedCost);


//                S -> T에 접근하였는가?
//                T -> S에 접근하였는가?
            for (int i = 0; i < 2; i++) {
                stmtIsVisited[i].setInt(1, i == FORWARD ? getTarget() : getSource());
                try (ResultSet rs = stmtIsVisited[i].executeQuery()) {
                    if (rs.next()) {
                        shortFinish = i;
                        break;
                    }
                }
            }
        }


        int midNode = getMidNode(getConnection(), minCost);
        if (midNode == -1 && shortFinish == -1)
            return new RunningResultError(start, System.currentTimeMillis(), getSource(), getTarget(), iteration[FORWARD], iteration[BACKWARD], "PATH NOT FOUND", "BiDirectional-FEM", minCost);

        if (shortFinish == BACKWARD)
            midNode = getTarget();
        else if (shortFinish == FORWARD)
            midNode = getSource();


        if (shortFinish != BACKWARD)
            path.addAll(extractPath(getConnection(), midNode, FORWARD)); // BACKWARD 로만 끝난 경우 S->M의 경로를 구할 필요 없음
        if (shortFinish == -1) // shortFinish 가 아닌 경우에만 2개의 Path 가 합쳐짐.
            path.remove(path.size() - 1); // 중간 노드가 겹침.
        if (shortFinish != FORWARD)
            path.addAll(extractPath(getConnection(), midNode, BACKWARD)); // FORWARD 로만 끝난 경우  M->T의 경로를 구할 필요 없음.


        return new RunningResultSuccess(start, System.currentTimeMillis(), getSource(), getTarget(), iteration[FORWARD], iteration[BACKWARD], path.toString(), "BiDirectional-FEM", minCost);


    }


    @Override
    public FrontierResult getFrontier(int direction) throws SQLException {
        try (ResultSet rs = stmtSelectFrontier[direction].executeQuery()) {
            if (rs.next())
                return new FrontierResult(rs.getInt(1), rs.getInt(2));
        }
        return null;
    }

    @Override
    public void setVisited(int direction, int nodeId) throws SQLException {
        stmtSetVisited[direction].setInt(1, nodeId);
        stmtSetVisited[direction].executeUpdate();
    }


}
