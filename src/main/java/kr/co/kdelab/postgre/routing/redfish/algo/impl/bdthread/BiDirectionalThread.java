package kr.co.kdelab.postgre.routing.redfish.algo.impl.bdthread;

import kr.co.kdelab.postgre.routing.redfish.algo.ShortestPathRunner;
import kr.co.kdelab.postgre.routing.redfish.algo.impl.bdthread.child.*;
import kr.co.kdelab.postgre.routing.redfish.algo.impl.dataclass.RunningResult;
import kr.co.kdelab.postgre.routing.redfish.algo.impl.dataclass.RunningResultError;
import kr.co.kdelab.postgre.routing.redfish.algo.impl.dataclass.RunningResultSuccess;
import kr.co.kdelab.postgre.routing.redfish.algo.impl.parent.BidirectionImpl;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;

public class BiDirectionalThread extends ShortestPathRunner implements BidirectionImpl {

    public final static int THREAD_NORMAL = 0;
    public final static int THREAD_USE_RB = 1;
    public final static int THREAD_USE_TA_INDEX = 2;
    public final static int THREAD_USE_RB_TA_INDEX = 3;

    private final int DELAY_MAIN;
    private int threadFlag;
//    private boolean useRBTable;

    public BiDirectionalThread() {
        this(100, THREAD_NORMAL);
    }


    public BiDirectionalThread(int DELAY_MAIN) {
        this(DELAY_MAIN, THREAD_NORMAL);
    }

    public BiDirectionalThread(int DELAY_MAIN, int threadFlag) {
        this.DELAY_MAIN = DELAY_MAIN;
        this.threadFlag = threadFlag;
    }


    @Override
    public void close() throws IOException {
        super.close();
    }

    @Override
    public RunningResult run() throws Exception {
        long minCost = Long.MAX_VALUE;

        PreparedStatement[] stmtEquallyExpandedCost = new PreparedStatement[2];
        stmtEquallyExpandedCost[FORWARD] = addPreparedStatement(getConnection().prepareStatement("select min(TF.d2s+TB.d2s) from ta as TF, ta2 as TB where TF.fwd >= ? and TF.fwd <= ? and TF.nid = TB.nid "));
        stmtEquallyExpandedCost[BACKWARD] = addPreparedStatement(getConnection().prepareStatement("select min(TF.d2s+TB.d2s) from ta as TF, ta2 as TB where TB.fwd >= ? and TB.fwd <= ? and TF.nid = TB.nid "));

        PreparedStatement stmtMinCostVerify = addPreparedStatement(getConnection().prepareStatement("select min(TF.d2s+TB.d2s) from ta as TF, ta2 as TB where TF.nid = TB.nid "));


        long start = System.currentTimeMillis();

        try (Statement statement = getConnection().createStatement()) {
            statement.execute("insert into ta(nid,d2s,p2s,fwd,f) values ('" + getSource() + "',0,'"
                    + getSource() + "',1,false)");
            statement.execute("insert into ta2(nid,d2s,p2s,fwd,f) values ('" + getTarget() + "',0,'"
                    + getTarget() + "',1,false)");
        }

        BDThreadChild[] threads;
        switch (threadFlag) {
            case THREAD_USE_RB:
                threads = new BDThreadChild[]{
                        new ReachedBDThreadChild(getJdbConnectionInfo(), "TA", "TE"),
                        new ReachedBDThreadChild(getJdbConnectionInfo(), "TA2", "TE_B")};
                break;
            case THREAD_USE_TA_INDEX:
                threads = new BDThreadChild[]{
                        new TaIndexedThreadChild(getJdbConnectionInfo(), "TA", "TE"),
                        new TaIndexedThreadChild(getJdbConnectionInfo(), "TA2", "TE_B")};
                break;
            case THREAD_USE_RB_TA_INDEX:
                threads = new BDThreadChild[]{
                        new ReachedTaIndexedThreadChild(getJdbConnectionInfo(), "TA", "TE"),
                        new ReachedTaIndexedThreadChild(getJdbConnectionInfo(), "TA2", "TE_B")};
                break;
            default:
                threads = new BDThreadChild[]{
                        new NormalBDThreadChild(getJdbConnectionInfo(), "TA", "TE"),
                        new NormalBDThreadChild(getJdbConnectionInfo(), "TA2", "TE_B")};
        }
        for (BDThreadChild threadParent : threads)
            threadParent.start();

        int shuffleDirection = 1;
        while (threads[FORWARD].getDist() + threads[BACKWARD].getDist() <= minCost) {
            if ((threads[FORWARD].getAffected() == 0 && threads[BACKWARD].getAffected() == 0) || (threads[FORWARD].isTermination() && threads[BACKWARD].isTermination())) {
                break;
            }

            Thread.sleep(DELAY_MAIN);
            shuffleDirection = shuffleDirection == 0 ? 1 : 0;
            int chkItrX = threads[shuffleDirection].getLastCheckIteration() + 1, chkItrY = threads[shuffleDirection].getIteration();
            long tmpMinCost = getMinCost(stmtEquallyExpandedCost, shuffleDirection, chkItrX, chkItrY);
            if (minCost > tmpMinCost)
                minCost = tmpMinCost;
            threads[shuffleDirection].setLastCheckIteration(chkItrY);
        }

        minCost = getMinCost(stmtMinCostVerify); // Verify is that real minCost

        for (BDThreadChild thread : threads)
            thread.terminate();

        int midNode = getMidNode(getConnection(), minCost);
        if (midNode == -1) {
            return new RunningResultError("PATH NOT FOUND");
        }


        // BACKWARD 로만 끝난 경우 S->M의 경로를 구할 필요 없음
        List<Integer> path = new LinkedList<>(extractPath(getConnection(), midNode, FORWARD));
        path.remove(path.size() - 1); // 중간 노드가 겹침.
        path.addAll(extractPath(getConnection(), midNode, BACKWARD)); // FORWARD 로만 끝난 경우  M->T의 경로를 구할 필요 없음.
        long end = System.currentTimeMillis();

        String algoTag = "Bidirectional Thread-FEM";
        switch (threadFlag) {
            case THREAD_USE_RB:
                algoTag = "Reached Bidirectional Thread";
                break;
            case THREAD_USE_TA_INDEX:
                algoTag = "TA Indexed Bidirectional Thread-FEM";
                break;
            case THREAD_USE_RB_TA_INDEX:
                algoTag = "Reached TA Indexed Bidirectional Thread-FEM";
                break;
        }

        return new RunningResultSuccess(start, end, getSource(), getTarget(), threads[FORWARD].getIteration(), threads[BACKWARD].getIteration(), path.toString(), algoTag, minCost);

    }


}
