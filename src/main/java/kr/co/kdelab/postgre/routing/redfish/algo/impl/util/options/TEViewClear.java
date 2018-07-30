package kr.co.kdelab.postgre.routing.redfish.algo.impl.util.options;

import kr.co.kdelab.postgre.routing.redfish.algo.ShortestPathOptionType;

public class TEViewClear extends DropViewStartWith {
    public TEViewClear() {
        super(ShortestPathOptionType.PRE_LOAD, "te");
    }
}
