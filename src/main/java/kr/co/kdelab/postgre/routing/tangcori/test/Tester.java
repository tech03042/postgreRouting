package kr.co.kdelab.postgre.routing.tangcori.test;

import kr.co.kdelab.postgre.routing.redfish.algo.ShortestPathBuilder;
import kr.co.kdelab.postgre.routing.redfish.algo.ShortestPathOptionType;
import kr.co.kdelab.postgre.routing.redfish.algo.impl.dataclass.ExpandableRunningResult;
import kr.co.kdelab.postgre.routing.redfish.algo.impl.dataclass.RunningResult;
import kr.co.kdelab.postgre.routing.redfish.algo.impl.util.options.*;
import kr.co.kdelab.postgre.routing.redfish.reachability.impl.Submit1Calculator;
import kr.co.kdelab.postgre.routing.redfish.util.JDBConnectionInfo;
import kr.co.kdelab.postgre.routing.tangcori.impl.PrepareSeoRBFS;
import kr.co.kdelab.postgre.routing.tangcori.impl.SeoRBFSRunnerReached;

import java.io.FileWriter;

public class Tester {
    private static RunningResult SeoRBFS(JDBConnectionInfo jdbConnectionInfo, int pts, int pv, int source, int target) throws Exception {
        try (ShortestPathBuilder shortestPathBuilder = new ShortestPathBuilder().JDBC(jdbConnectionInfo)) {
            shortestPathBuilder
                    .Option(new ERClear())
                    .Option(new TAClear(ShortestPathOptionType.RUNNING_PRE))
                    // TA Clear
                    .Option(new PrepareSeoRBFS())
                    // BD Thread Table Prepare
                    .Runner(new SeoRBFSRunnerReached(pts, pv));
            shortestPathBuilder.prepare();
            return shortestPathBuilder.run(source, target);
        }
    }

    public static void dataPrepare(JDBConnectionInfo jdbConnectionInfo, String dataSet, int pts, int pv) throws Exception {
        try (ShortestPathBuilder shortestPathBuilder = new ShortestPathBuilder().JDBC(jdbConnectionInfo)) {
            shortestPathBuilder.Option(new TETableClear())
                    .Option(new TEViewClear())
                    .Option(new PartitioningImporter(dataSet, pts, pv));
            shortestPathBuilder.prepare();
        }
    }

    public static void main(String[] args) throws Exception {
        JDBConnectionInfo jdbConnectionInfo = new JDBConnectionInfo("jdbc:postgresql://localhost:5432/kdelab",
                "postgres", "kdelab", "kdelab");


        // logFile이 NULL 이 아니면 해당 파일을 Console Output으로 대체함.
//        int pts = 40;
//        int pv = 2500;
        // NY


        int pts = 75;
        int pv = 7500;
        //FLA

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
        filename = "./resources/yago.yago";

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
            case "./resources/yago.yago":
                source = 21235;
                target = 234234;
                break;

        }
        dataPrepare(jdbConnectionInfo, filename, pts, pv);

        long t_reaching = System.currentTimeMillis();
        try (Submit1Calculator joinCalculator = new Submit1Calculator(jdbConnectionInfo, true)) {
            joinCalculator.calc(source, target);
        }
        System.out.println(System.currentTimeMillis() - t_reaching);

        try (FileWriter logFile = new FileWriter("log/log_runner_test.txt", true)) {
            RunningResult runningResult;
            runningResult = SeoRBFS(jdbConnectionInfo, pts, pv, source, target);

            if (runningResult instanceof ExpandableRunningResult) {
                ((ExpandableRunningResult) runningResult).writeCSV("log/test.csv", "TESTRB", filename, t_reaching);
            } else {
                System.out.println(runningResult);
                logFile.append(runningResult.toString(filename)).append("\n");
            }
        }

    }
}
