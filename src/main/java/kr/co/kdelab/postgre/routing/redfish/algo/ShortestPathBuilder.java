package kr.co.kdelab.postgre.routing.redfish.algo;

import kr.co.kdelab.postgre.routing.redfish.algo.dataclass.Point;
import kr.co.kdelab.postgre.routing.redfish.algo.dataclass.PointArray;
import kr.co.kdelab.postgre.routing.redfish.algo.impl.dataclass.RunningResult;
import kr.co.kdelab.postgre.routing.redfish.algo.impl.dataclass.RunningResultArray;
import kr.co.kdelab.postgre.routing.redfish.algo.impl.dataclass.RunningResultError;
import kr.co.kdelab.postgre.routing.redfish.algo.impl.dataclass.RunningResultSuccess;
import kr.co.kdelab.postgre.routing.redfish.algo.impl.util.options.RechabilityImporter;
import kr.co.kdelab.postgre.routing.redfish.reachability.dataclass.RechabilityResult;
import kr.co.kdelab.postgre.routing.redfish.util.JDBConnectionInfo;

import java.io.Closeable;
import java.io.IOException;

public class ShortestPathBuilder implements Closeable {

    private static final int OPTION_PRE = 0;
    private static final int OPTION_POST = 1;

    private ShortestPathOptionList shortestPathOptions[] = {
            new ShortestPathOptionList(), new ShortestPathOptionList()
    };
    private ShortestPathOptionList shortestPathRunningOptions[] = {
            new ShortestPathOptionList(), new ShortestPathOptionList()
    };


    private ShortestPathRunner shortestPathRunner;
    private JDBConnectionInfo jdbConnectionInfo;

    public ShortestPathBuilder Option(ShortestPathOption shortestPathOption) {
        int optionArrIdx = shortestPathOption.getShortestPathOptionType().isPostExecute() ? OPTION_POST : OPTION_PRE;
        ShortestPathOptionList[] optionLists = shortestPathOption.getShortestPathOptionType().isRunning()
                ? shortestPathRunningOptions : shortestPathOptions;

        optionLists[optionArrIdx].add(shortestPathOption);
        return this;
    }

    public ShortestPathBuilder JDBC(JDBConnectionInfo jdbConnectionInfo) {
        this.jdbConnectionInfo = jdbConnectionInfo;
        return this;
    }

    public ShortestPathBuilder Runner(ShortestPathRunner shortestPathRunner) {
        this.shortestPathRunner = shortestPathRunner;
        return this;
    }

    public RunningResult run(int source, int target) throws Exception {
        RechabilityResult rechabilityResult = null;
        for (ShortestPathOption shortestPathOption : shortestPathRunningOptions[0]) {
            DLog.debug("run-prepare-start", shortestPathOption.getClass().toString());
            shortestPathOption.setSource(source);
            shortestPathOption.setTarget(target);
            shortestPathOption.run(jdbConnectionInfo);
            DLog.debug("run-prepare-end", shortestPathOption.getClass().toString());

            if (shortestPathOption instanceof RechabilityImporter)
                rechabilityResult = ((RechabilityImporter) shortestPathOption).getRechabilityResult();
        }


        DLog.debug("run", "start");

        shortestPathRunner.setSource(source);
        shortestPathRunner.setTarget(target);

        RunningResult runningResult = shortestPathRunner.run(jdbConnectionInfo);

        DLog.debug("run", "end");

        for (ShortestPathOption shortestPathOption : shortestPathRunningOptions[1]) {
            DLog.debug("run-clean-start", shortestPathOption.getClass().toString());
            shortestPathOption.run(jdbConnectionInfo);
            DLog.debug("run-clean-end", shortestPathOption.getClass().toString());
        }

        if (runningResult instanceof RunningResultSuccess) {
            ((RunningResultSuccess) runningResult).setRechabilityResult(rechabilityResult);
        }
        return runningResult;
    }

    public RunningResultArray run(PointArray points) {
        RunningResultArray runningResults = new RunningResultArray();
        for (Point point : points) {
            RunningResult runningResult;
            try {
                for (ShortestPathOption shortestPathOption : shortestPathRunningOptions[0]) {
                    shortestPathOption.run(jdbConnectionInfo);
                }


                shortestPathRunner.setSource(point.getSource());
                shortestPathRunner.setTarget(point.getTarget());

                runningResult = shortestPathRunner.run(jdbConnectionInfo);


                for (ShortestPathOption shortestPathOption : shortestPathRunningOptions[1]) {
                    shortestPathOption.run(jdbConnectionInfo);
                }
            } catch (Exception e) {
                runningResult = new RunningResultError(e);
            }
            if (runningResult == null)
                runningResult = new RunningResultError(new NullPointerException());

            runningResults.add(runningResult);
        }

        return runningResults;
    }

    public void prepare() throws Exception {
        DLog.debug("prepare", "start");

        for (ShortestPathOption shortestPathOption : shortestPathOptions[0])
            shortestPathOption.run(jdbConnectionInfo);
        DLog.debug("prepare", "end");

    }

    @Override
    public void close() throws IOException {
        for (ShortestPathOption shortestPathOption : shortestPathOptions[1]) {
            try {
                shortestPathOption.run(jdbConnectionInfo);
            } catch (Exception e) {
                throw new IOException(e.getMessage());
            }
        }

        for (ShortestPathOption shortestPathOption : shortestPathOptions[0]) {
            shortestPathOption.close();
        }
        for (ShortestPathOption shortestPathOption : shortestPathOptions[1]) {
            shortestPathOption.close();
        }

        for (ShortestPathOption shortestPathOption : shortestPathRunningOptions[0]) {
            shortestPathOption.close();
        }
        for (ShortestPathOption shortestPathOption : shortestPathRunningOptions[1]) {
            shortestPathOption.close();
        }
    }
}
