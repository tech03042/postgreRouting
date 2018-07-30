package kr.co.kdelab.postgre.routing.tangcori.util;

import kr.co.kdelab.postgre.routing.parser.Random;
import kr.co.kdelab.postgre.routing.parser.USARoad;
import kr.co.kdelab.postgre.routing.parser.applier.PostgrePartitioningApplier;
import kr.co.kdelab.postgre.routing.parser.internal.TestSet;
import kr.co.kdelab.postgre.routing.redfish.algo.ShortestPathOption;
import kr.co.kdelab.postgre.routing.redfish.algo.ShortestPathOptionType;
import kr.co.kdelab.postgre.routing.redfish.util.JDBConnectionInfo;

import java.io.File;
import java.sql.Connection;

public class CustomImporter extends ShortestPathOption {

    private String dataSet;
    private int pts;
    private int pv;
    private boolean usingFullTe;

    public CustomImporter(String dataSet, int pts, int pv, boolean usingFullTe) {
        super(ShortestPathOptionType.PRE_LOAD);
        this.dataSet = dataSet;
        this.pts = pts;
        this.pv = pv;
        this.usingFullTe = usingFullTe;
    }

    @Override
    public void run(JDBConnectionInfo jdbConnectionInfo) throws Exception {
        TestSet testSet;
        if (dataSet.endsWith(".gr"))
            testSet = new USARoad(new File(dataSet));
        else
            testSet = new Random(new File(dataSet));

        testSet.setApplier(new CustomApplier(pts, pv, usingFullTe));
        try (Connection connection = jdbConnectionInfo.createConnection()) {
            testSet.applyInTable(connection);

        }
        testSet.close();
    }
}
