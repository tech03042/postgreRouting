package kr.co.kdelab.postgre.routing.redfish.reachability.dataclass;

public class RechabilityResult {
    private long delay;
    private String rechabilityTagName;

    public RechabilityResult(String rechabilityTagName, long delay, long rbCount) {
        this.rechabilityTagName = rechabilityTagName;
        this.delay = delay;
        this.rbCount = rbCount;
    }

    private long rbCount;

    public long getDelay() {
        return delay;
    }

    public String getRechabilityTagName() {
        return rechabilityTagName;
    }

    public long getRbCount() {
        return rbCount;
    }

    @Override
    public String toString() {
        return String.format("Tag=%s, Delay=%d, COUNT(RB)=%d", rechabilityTagName, delay, rbCount);
    }
}
