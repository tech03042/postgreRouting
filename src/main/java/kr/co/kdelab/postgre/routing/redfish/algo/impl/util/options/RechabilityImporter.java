package kr.co.kdelab.postgre.routing.redfish.algo.impl.util.options;

import kr.co.kdelab.postgre.routing.redfish.algo.ShortestPathOption;
import kr.co.kdelab.postgre.routing.redfish.algo.ShortestPathOptionType;
import kr.co.kdelab.postgre.routing.redfish.reachability.RechabilityCalculator;
import kr.co.kdelab.postgre.routing.redfish.reachability.dataclass.RechabilityResult;
import kr.co.kdelab.postgre.routing.redfish.util.JDBConnectionInfo;

import java.io.IOException;

public class RechabilityImporter extends ShortestPathOption {
    private RechabilityCalculator rechabilityCalculator;
    private RechabilityResult rechabilityResult;

    public RechabilityResult getRechabilityResult() {
        return rechabilityResult;
    }

    public RechabilityImporter(RechabilityCalculator rechabilityCalculator) {
        super(ShortestPathOptionType.RUNNING_PRE);
        this.rechabilityCalculator = rechabilityCalculator;
    }

    @Override
    public void run(JDBConnectionInfo jdbConnectionInfo) throws Exception {
        rechabilityResult = rechabilityCalculator.calc(getSource(), getTarget());
        rechabilityCalculator.clear();
    }

    @Override
    public void close() throws IOException {
        super.close();
        try {
            rechabilityCalculator.close();
        } catch (Exception ignored) {

        }
    }
}
