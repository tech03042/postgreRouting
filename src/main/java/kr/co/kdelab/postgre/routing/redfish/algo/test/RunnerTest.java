package kr.co.kdelab.postgre.routing.redfish.algo.test;

import kr.co.kdelab.postgre.routing.redfish.algo.ShortestPathBuilder;
import kr.co.kdelab.postgre.routing.redfish.algo.ShortestPathOptionType;
import kr.co.kdelab.postgre.routing.redfish.algo.impl.bdthread.BiDirectionalThread;
import kr.co.kdelab.postgre.routing.redfish.algo.impl.bdthread.options.PrepareBDThread;
import kr.co.kdelab.postgre.routing.redfish.algo.impl.bidirectional.BiDirection;
import kr.co.kdelab.postgre.routing.redfish.algo.impl.bidirectional.BiDirectionTAIndexed;
import kr.co.kdelab.postgre.routing.redfish.algo.impl.bidirectional.ReachedBiDirection;
import kr.co.kdelab.postgre.routing.redfish.algo.impl.dataclass.RunningResult;
import kr.co.kdelab.postgre.routing.redfish.algo.impl.rbfs.BiRbfs;
import kr.co.kdelab.postgre.routing.redfish.algo.impl.rbfs.ReachedBiRbfs;
import kr.co.kdelab.postgre.routing.redfish.algo.impl.rbfs.options.PrepareBiRbfs;
import kr.co.kdelab.postgre.routing.redfish.algo.impl.util.options.*;
import kr.co.kdelab.postgre.routing.redfish.reachability.impl.Submit1Calculator;
import kr.co.kdelab.postgre.routing.redfish.util.JDBConnectionInfo;

import java.io.FileWriter;

public class RunnerTest {

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


        // logFile이 NULL 이 아니면 해당 파일을 Console Output으로 대체함.
        int pts = 10;
        int pv = 10;
        // NY


//        int pts = 75;
//        int pv = 7500;
//        //FLA

        int source = 0;
        int target = 0;
        String filename;

        //        filename = "./resources/papergraph.txt";
//        filename = "./resources/random_1000_10000_1.txt";
//        filename = "./resources/random_5000_50000_10.txt";
//        filename = "./resources/USA-road-t.NY.gr";
//        filename = "./resources/USA-road-t.W.gr"; // 17m
//        filename = "./resources/USA-road-t.COL.gr";
//        filename = "./resources/USA-road-t.FLA.gr"; // 1m 18s
//        filename = "./resources/USA-road-t.USA.gr";
//        filename = "./resources/directed_50000.txt";
        filename = "./resources/soc-LiveJournal1.txt";

        switch (filename) {
            case "./resources/papergraph.txt":
                source = 1;
                target = 11;
                break;
            case "./resources/random_1000_10000_1.txt":
                source = 30;
                target = 386;
                break;
            case "./resources/random_5000_50000_10.txt":
                source = 958;
                target = 3206;
                break;
            case "./resources/USA-road-t.NY.gr":
                source = 13576;
                target = 245646;
                break;
            case "./resources/USA-road-t.COL.gr":
                source = 13113;
                target = 435663;
                break;
            case "./resources/USA-road-t.USA.gr":
                source = 2097154;
                target = 23947340;
                break;
            case "./resources/USA-road-t.W.gr":
                source = 1985;
                target = 619343;
                break;
            case "./resources/USA-road-t.FLA.gr":
                source = 5422;
                target = 1062403;
                break;
            case "./resources/directed_50000.txt":
                source = 324;
                target = 45774;
                break;
            case "./resources/soc-LiveJournal1.txt":
                source = 282;
                target = 13134;
                break;
        }
//        dataPrepare(jdbConnectionInfo, filename, pts, pv);
//        System.out.println("Sleep after data insert");
//        Thread.sleep(10000);
//        System.out.println("Wake Up");

        try (FileWriter logFile = new FileWriter("log/log_runner_test.txt", true)) {
            RunningResult runningResult;

//            bidirectional(jdbConnectionInfo, filename, source, target);
            //            runningResult = birbfs(jdbConnectionInfo, true, filename, pts, pv, source, target);
//            System.out.println(runningResult);
//            logFile.append(runningResult.toString(filename)).append("\n");
//
//
//            runningResult = birbfsReached(jdbConnectionInfo, true, filename, pts, pv, source, target);
//            System.out.println(runningResult);
//            logFile.append(runningResult.toString(filename)).append("\n");
//
//
//            runningResult = bdThread(jdbConnectionInfo, true, filename, source, target);
//            System.out.println(runningResult);
//            logFile.append(runningResult.toString(filename)).append("\n");
//
//
//            runningResult = bdThreadReached(jdbConnectionInfo, true, filename, source, target);
//            System.out.println(runningResult);
//            logFile.append(runningResult.toString(filename)).append("\n");
//
//

            runningResult = bdThreadTaIndexed(jdbConnectionInfo, true, filename, source, target);
            System.out.println(runningResult);
            logFile.append(runningResult.toString(filename)).append("\n");

            runningResult = bidirectionalTaIndexed(jdbConnectionInfo, true, filename, source, target);
            System.out.println(runningResult);
            logFile.append(runningResult.toString(filename)).append("\n");

//            runningResult = bdThreadTaIndexed(jdbConnectionInfo, true, filename, source, target);
//            System.out.println(runningResult);
//            logFile.append(runningResult.toString(filename)).append("\n");
//
//
//            runningResult = bdThreadReached(jdbConnectionInfo, true, filename, source, target);
//            System.out.println(runningResult);
//            logFile.append(runningResult.toString(filename)).append("\n");
        }
    }

}
