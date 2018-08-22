package kr.co.kdelab.postgre.routing.redfish.algo.test;

import kr.co.kdelab.postgre.routing.redfish.algo.ShortestPathBuilder;
import kr.co.kdelab.postgre.routing.redfish.algo.ShortestPathOptionType;
import kr.co.kdelab.postgre.routing.redfish.algo.ShortestPathRunner;
import kr.co.kdelab.postgre.routing.redfish.algo.impl.bdthread.BiDirectionalThread;
import kr.co.kdelab.postgre.routing.redfish.algo.impl.bdthread.options.PrepareBDThread;
import kr.co.kdelab.postgre.routing.redfish.algo.impl.bidirectional.BiDirection;
import kr.co.kdelab.postgre.routing.redfish.algo.impl.bidirectional.BiDirectionTAIndexed;
import kr.co.kdelab.postgre.routing.redfish.algo.impl.dataclass.RunningResult;
import kr.co.kdelab.postgre.routing.redfish.algo.impl.dataclass.RunningResultError;
import kr.co.kdelab.postgre.routing.redfish.algo.impl.dataclass.RunningResultSuccess;
import kr.co.kdelab.postgre.routing.redfish.algo.impl.rbfs.BiRbfs;
import kr.co.kdelab.postgre.routing.redfish.algo.impl.rbfs.ReachedBiRbfs;
import kr.co.kdelab.postgre.routing.redfish.algo.impl.rbfs.options.PrepareBiRbfs;
import kr.co.kdelab.postgre.routing.redfish.algo.impl.util.options.*;
import kr.co.kdelab.postgre.routing.redfish.reachability.impl.Submit1Calculator;
import kr.co.kdelab.postgre.routing.redfish.util.JDBConnectionInfo;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;

public class PaperTestCaseRunner {

    private static RunningResult birbfs(JDBConnectionInfo jdbConnectionInfo, boolean remainTe, String dataSet, int pts, int pv, int source, int target) throws Exception {
        try (ShortestPathBuilder shortestPathBuilder = new ShortestPathBuilder().JDBC(jdbConnectionInfo)) {

            if (!remainTe)
                shortestPathBuilder.Option(new TETableClear());

            shortestPathBuilder.Option(new TEViewClear());

            if (!remainTe)
                shortestPathBuilder.Option(new PartitioningImporter(dataSet, pts, pv));

            shortestPathBuilder
                    .Option(new TAClear(ShortestPathOptionType.RUNNING_PRE))
                    // TA Clear
                    .Option(new PrepareBiRbfs())
                    // BD Thread Table Prepare
                    .Runner(new BiRbfs(pts, pv));
            shortestPathBuilder.prepare();
            return shortestPathBuilder.run(source, target);
        }
    }

    private static RunningResult birbfsReached(JDBConnectionInfo jdbConnectionInfo, boolean remainTe, String dataSet, int pts, int pv, int source, int target) throws Exception {
        try (ShortestPathBuilder shortestPathBuilder = new ShortestPathBuilder().JDBC(jdbConnectionInfo)) {

            if (!remainTe)
                shortestPathBuilder.Option(new TETableClear());

            shortestPathBuilder.Option(new TEViewClear());

            if (!remainTe)
                shortestPathBuilder.Option(new PartitioningImporter(dataSet, pts, pv));


            boolean isDoubleUndirected = dataSet.endsWith(".gr");

            shortestPathBuilder
                    .Option(new TAClear(ShortestPathOptionType.RUNNING_PRE))
                    // TA Clear
                    .Option(new PrepareBiRbfs())
                    // BD Thread Table Prepare
                    .Option(new RechabilityImporter(new Submit1Calculator(jdbConnectionInfo, isDoubleUndirected)))
                    .Runner(new ReachedBiRbfs(pts, pv));
            shortestPathBuilder.prepare();
            return shortestPathBuilder.run(source, target);
        }
    }

    private static RunningResult bidirectional(JDBConnectionInfo jdbConnectionInfo, boolean remainTE, String dataSet, int source, int target) throws Exception {
        try (ShortestPathBuilder shortestPathBuilder = new ShortestPathBuilder().JDBC(jdbConnectionInfo)) {
            if (!remainTE)
                shortestPathBuilder.Option(new TETableClear());

            shortestPathBuilder.Option(new TEViewClear());

            if (!remainTE)
                shortestPathBuilder.Option(new NormalImporter(dataSet));


            shortestPathBuilder
                    // Build TE TABLE
                    .Option(new TAClear(ShortestPathOptionType.RUNNING_PRE))
                    // TA Clear
                    .Option(new PrepareBDThread())
                    // BD Thread Table Prepare
                    .Runner(new BiDirection());
            shortestPathBuilder.prepare();
            return shortestPathBuilder.run(source, target);
        }
    }


    private static RunningResult bidirectionalTaIndexed(JDBConnectionInfo jdbConnectionInfo, boolean remainTE, String dataSet, int source, int target) throws Exception {
        try (ShortestPathBuilder shortestPathBuilder = new ShortestPathBuilder().JDBC(jdbConnectionInfo)) {
            if (!remainTE)
                shortestPathBuilder.Option(new TETableClear());

            shortestPathBuilder.Option(new TEViewClear());

            if (!remainTE)
                shortestPathBuilder.Option(new NormalImporter(dataSet));


            shortestPathBuilder
                    // Build TE TABLE
                    .Option(new TAClear(ShortestPathOptionType.RUNNING_PRE))
                    // TA Clear
                    .Option(new PrepareBDThread(true))
                    // BD Thread Table Prepare
                    .Runner(new BiDirectionTAIndexed());
            shortestPathBuilder.prepare();
            return shortestPathBuilder.run(source, target);
        }
    }

    private static RunningResult bdThread(JDBConnectionInfo jdbConnectionInfo, boolean remainTE, String dataSet, int source, int target) throws Exception {
        try (ShortestPathBuilder shortestPathBuilder = new ShortestPathBuilder().JDBC(jdbConnectionInfo)) {

            if (!remainTE)
                shortestPathBuilder.Option(new TETableClear());

            shortestPathBuilder.Option(new TEViewClear());

            if (!remainTE)
                shortestPathBuilder.Option(new NormalImporter(dataSet));


            shortestPathBuilder
                    // Build TE TABLE
                    .Option(new TAClear(ShortestPathOptionType.RUNNING_PRE))
                    // TA Clear
                    .Option(new PrepareBDThread())
                    // BD Thread Table Prepare
                    .Runner(new BiDirectionalThread(100, BiDirectionalThread.THREAD_NORMAL));
            shortestPathBuilder.prepare();
            return shortestPathBuilder.run(source, target);
        }
    }

    private static RunningResult bdThreadTaIndexed(JDBConnectionInfo jdbConnectionInfo, boolean remainTE, String dataSet, int source, int target) throws Exception {
        try (ShortestPathBuilder shortestPathBuilder = new ShortestPathBuilder().JDBC(jdbConnectionInfo)) {

            if (!remainTE)
                shortestPathBuilder.Option(new TETableClear());

            shortestPathBuilder.Option(new TEViewClear());

            if (!remainTE)
                shortestPathBuilder.Option(new NormalImporter(dataSet));

            shortestPathBuilder
                    // Build TE TABLE
                    .Option(new TAClear(ShortestPathOptionType.RUNNING_PRE))
                    // TA Clear
                    .Option(new PrepareBDThread(true))
//                    .Option(new RechabilityImporter(new JoinCalculator(jdbConnectionInfo)))
                    // BD Thread Table Prepare
                    .Runner(new BiDirectionalThread(10, BiDirectionalThread.THREAD_USE_TA_INDEX));
            shortestPathBuilder.prepare();
            return shortestPathBuilder.run(source, target);
        }
    }

    private static RunningResult reachedBdThreadTaIndexed(JDBConnectionInfo jdbConnectionInfo, boolean remainTE, String dataSet, int source, int target) throws Exception {
        try (ShortestPathBuilder shortestPathBuilder = new ShortestPathBuilder().JDBC(jdbConnectionInfo)) {

            if (!remainTE)
                shortestPathBuilder.Option(new TETableClear());

            shortestPathBuilder.Option(new TEViewClear());

            if (!remainTE)
                shortestPathBuilder.Option(new NormalImporter(dataSet));

            boolean isDoubleUndirected = dataSet.endsWith(".gr");

            shortestPathBuilder
                    // Build TE TABLE
                    .Option(new TAClear(ShortestPathOptionType.RUNNING_PRE))
                    // TA Clear
                    .Option(new PrepareBDThread(true))
                    // BD Thread Table Prepare
                    .Option(new RechabilityImporter(new Submit1Calculator(jdbConnectionInfo, isDoubleUndirected)))
                    .Runner(new BiDirectionalThread(100, BiDirectionalThread.THREAD_USE_TA_INDEX | BiDirectionalThread.THREAD_USE_RB));
            shortestPathBuilder.prepare();
            return shortestPathBuilder.run(source, target);
        }
    }

    private static RunningResult bdThreadReached(JDBConnectionInfo jdbConnectionInfo, boolean remainTE, String dataSet, int source, int target) throws Exception {
        try (ShortestPathBuilder shortestPathBuilder = new ShortestPathBuilder().JDBC(jdbConnectionInfo)) {

            if (!remainTE)
                shortestPathBuilder.Option(new TETableClear());

            shortestPathBuilder.Option(new TEViewClear());

            if (!remainTE)
                shortestPathBuilder.Option(new NormalImporter(dataSet));

            boolean isDoubleUndirected = dataSet.endsWith(".gr");

            shortestPathBuilder
                    // Build TE TABLE
                    .Option(new TAClear(ShortestPathOptionType.RUNNING_PRE))
                    // TA Clear
                    .Option(new PrepareBDThread(true))
                    // BD Thread Table Prepare
                    .Option(new RechabilityImporter(new Submit1Calculator(jdbConnectionInfo, isDoubleUndirected)))
                    .Runner(new BiDirectionalThread(100, BiDirectionalThread.THREAD_USE_RB));
            shortestPathBuilder.prepare();
            return shortestPathBuilder.run(source, target);
        }
    }

    private static void dataPrepare(JDBConnectionInfo jdbConnectionInfo, String dataSet, int pts, int pv) throws Exception {
        try (ShortestPathBuilder shortestPathBuilder = new ShortestPathBuilder().JDBC(jdbConnectionInfo)) {
            shortestPathBuilder.Option(new TETableClear())
                    .Option(new TEViewClear())
                    .Option(new PartitioningImporter(dataSet, pts, pv))
                    // Build TE TABLE
                    .Option(new TAClear(ShortestPathOptionType.RUNNING_PRE))
                    // TA Clear
                    .Option(new PrepareBDThread());
            // BD Thread Table Prepare
//                    .Runner(new BiDirectionalThread(100));
            shortestPathBuilder.prepare();
        }
    }

    public static void main(String[] args) throws Exception {

        JDBConnectionInfo jdbConnectionInfo = new JDBConnectionInfo("jdbc:postgresql://localhost:5432/kdelab",
                "postgres", "icdwvb4j", "kdelab");

        // 대상 BI-FEM, RBFS, TA_INDEXED, BI-THREAD, TA-INDEXE+BI-THREA
        String BD_THREAD_NY = "1,237922;655,241877;920,252805;237884,1828;10720,139308;139308,11662;245745,236232;4019,243094;255868,187884;248015,13923";
        String BD_THREAD_COL = "22573,438;206709,26898;22729,1641;6049,33603;267355,332354;56615,29428;270661,15757;941,138005;138009,365252;1321,58979";
        String BD_THREAD_FLA = "13576,984045;245646,908759;245646,345;595,915269;4567,43102;9912,984331;7830,486256;6651,543923;14871,870433;485688,16189";
        String BD_THREAD_W = "1329,2158002;5520692,1981437;932518,908209;2066780,2064909;171561,1233592;46918,1178692;520621,1641337;170383,2110159;932528,2066880;46918,37621";

        String[] runner = {BD_THREAD_NY, BD_THREAD_COL, BD_THREAD_FLA, BD_THREAD_W};//{BD_THREAD_NY, BD_THREAD_COL, BD_THREAD_FLA, BD_THREAD_W};
        String[] runnerFiles = {"./resources/USA-road-t.NY.gr", "./resources/USA-road-t.COL.gr", "./resources/USA-road-t.FLA.gr", "./resources/USA-road-t.W.gr"};
        // BI FEM, RBFS, TA INDEXED, BI_THREAD

        int[] pts_pv = {
                50, 2500,
                100, 5000,
                200, 5000,
                200, 5000
        };


        ShortestPathBuilder[] shortestPathRunners = new ShortestPathBuilder[]{
//                new ShortestPathBuilder().JDBC(jdbConnectionInfo)
//                        .Option(new TEViewClear())
//                        .Option(new TAClear(ShortestPathOptionType.RUNNING_PRE))
//                        .Option(new PrepareBDThread(true))
//                        .Runner(new BiDirection()) // BiDirection
//                ,
                new ShortestPathBuilder().JDBC(jdbConnectionInfo)
                        .Option(new TEViewClear())
                        .Option(new TAClear(ShortestPathOptionType.RUNNING_PRE))
                        .Option(new PrepareBDThread(true))
                        .Runner(new BiDirectionTAIndexed()) // TA_INDEXED
                ,
//                new ShortestPathBuilder().JDBC(jdbConnectionInfo)
//                        .Option(new TEViewClear())
//                        .Option(new TAClear(ShortestPathOptionType.RUNNING_PRE))
//                        .Option(new PrepareBDThread())
//                        .Runner(new BiDirectionalThread(50, BiDirectionalThread.THREAD_NORMAL)), // BD_THREAD
                new ShortestPathBuilder().JDBC(jdbConnectionInfo).Option(new TEViewClear())
                        .Option(new TAClear(ShortestPathOptionType.RUNNING_PRE))
                        .Option(new PrepareBDThread(true))
                        .Runner(new BiDirectionalThread(10, BiDirectionalThread.THREAD_USE_TA_INDEX)), // BD_THREAD_TA_INDEXED
//                new ShortestPathBuilder().JDBC(jdbConnectionInfo)
//                        .Option(new TEViewClear())
//                        .Option(new TAClear(ShortestPathOptionType.RUNNING_PRE))
//                        // TA Clear
//                        .Option(new PrepareBiRbfs())
//                        // BD Thread Table Prepare
//                        .Runner(new BiRbfs(0, 0))
        };

        for (int i = 0; i < runner.length; i++) {
            String[] testSet = runner[i].split(";");
            String dataSet = runnerFiles[i];
            int pts = pts_pv[i * 2];
            int pv = pts_pv[i * 2 + 1];

            System.out.println(dataSet);
            System.out.printf("PTS = %d, PV = %d\n", pts, pv);

            dataPrepare(jdbConnectionInfo, dataSet, pts, pv);
            // PreparingDataSet
            Thread.sleep(10000);
            System.out.println("Sleep After data prepare");

            for (String curTestSet : testSet) {
                System.out.println("CurrentTestSet = " + curTestSet);

                String[] testSetSplited = curTestSet.split(",");
                int source = Integer.parseInt(testSetSplited[0]);
                int target = Integer.parseInt(testSetSplited[1]);
                for (ShortestPathBuilder shortestPathBuilder : shortestPathRunners) {
                    RunningResult runningResult = shortestPathBuilder.run(source, target, pts, pv);
                    if (runningResult instanceof RunningResultSuccess) {
                        ((RunningResultSuccess) runningResult).writeCSV("./log/paper_result.csv");
                    } else if (runningResult instanceof RunningResultError) {
                        ((RunningResultError) runningResult).writeCSV("./log/paper_result.csv");
                    } else
                        System.out.println(runningResult);

                    Thread.sleep(1000);
                }
            }


        }


    }

}
