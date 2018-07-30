package kr.co.kdelab.postgre.routing.redfish.algo.impl.util.options;

import kr.co.kdelab.postgre.routing.redfish.algo.ShortestPathOptionType;

public class ERClear extends DropViewStartWith {
    public ERClear() {
        super(ShortestPathOptionType.RUNNING_PRE, "er");
    }
}
