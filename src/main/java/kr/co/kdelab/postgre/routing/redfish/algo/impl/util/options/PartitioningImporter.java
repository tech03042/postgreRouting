package kr.co.kdelab.postgre.routing.redfish.algo.impl.util.options;

import kr.co.kdelab.postgre.routing.parser.Random;
import kr.co.kdelab.postgre.routing.parser.USARoad;
import kr.co.kdelab.postgre.routing.parser.Yago;
import kr.co.kdelab.postgre.routing.parser.applier.PostgrePartitioningApplier;
import kr.co.kdelab.postgre.routing.parser.internal.TestSet;
import kr.co.kdelab.postgre.routing.redfish.algo.ShortestPathOption;
import kr.co.kdelab.postgre.routing.redfish.algo.ShortestPathOptionType;
import kr.co.kdelab.postgre.routing.redfish.util.JDBConnectionInfo;

import java.io.File;
import java.sql.Connection;

public class PartitioningImporter extends ShortestPathOption {

    private String dataSet;
    private int pts;
    private int pv;

    public PartitioningImporter(String dataSet, int pts, int pv) {
        super(ShortestPathOptionType.PRE_LOAD);
        this.dataSet = dataSet;
        this.pts = pts;
        this.pv = pv;
    }

    @Override
    public void run(JDBConnectionInfo jdbConnectionInfo) throws Exception {
        TestSet testSet;
        if (dataSet.endsWith(".gr"))
            testSet = new USARoad(new File(dataSet));
        else if (dataSet.endsWith(".yago"))
            testSet = new Yago(new File(dataSet));
        else
            testSet = new Random(new File(dataSet));

        testSet.setApplier(new PostgrePartitioningApplier(pts, pv));
        try (Connection connection = jdbConnectionInfo.createConnection()) {
            testSet.applyInTable(connection);
        }
        testSet.close();
    }
}
