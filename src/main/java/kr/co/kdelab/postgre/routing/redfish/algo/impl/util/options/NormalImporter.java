package kr.co.kdelab.postgre.routing.redfish.algo.impl.util.options;

import kr.co.kdelab.postgre.routing.parser.Random;
import kr.co.kdelab.postgre.routing.parser.USARoad;
import kr.co.kdelab.postgre.routing.parser.Yago;
import kr.co.kdelab.postgre.routing.parser.applier.PostgreApplier;
import kr.co.kdelab.postgre.routing.parser.internal.TestSet;
import kr.co.kdelab.postgre.routing.redfish.algo.ShortestPathOption;
import kr.co.kdelab.postgre.routing.redfish.algo.ShortestPathOptionType;
import kr.co.kdelab.postgre.routing.redfish.util.JDBConnectionInfo;

import java.io.File;
import java.sql.Connection;

public class NormalImporter extends ShortestPathOption {

    private String dataSet;

    public NormalImporter(String dataSet) {
        super(ShortestPathOptionType.PRE_LOAD);
        this.dataSet = dataSet;
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

        testSet.setApplier(new PostgreApplier());
        try (Connection connection = jdbConnectionInfo.createConnection()) {
            testSet.applyInTable(connection);

        }
        testSet.close();
    }
}
