package kr.co.kdelab.postgre.routing.redfish.algo.impl.bdthread.child;

import kr.co.kdelab.postgre.routing.redfish.util.JDBConnectionInfo;

public class ReachedTaIndexedThreadChild extends BDThreadChild {


    public ReachedTaIndexedThreadChild(JDBConnectionInfo jdbConnectionInfo, String getTableNameTA, String tableNameTE) {
        super(jdbConnectionInfo, getTableNameTA, tableNameTE);
    }

    @Override
    public String getFEMSql() {
        return "WITH fop AS " +
                "(UPDATE " + getTableNameTA() + " set f = true where nid = (select nid from " + getTableNameTA() + " where f=false order by f asc, d2s asc limit 1)" +
                " RETURNING nid,d2s) " +
                ",mop AS (INSERT INTO " + getTableNameTA() + "(nid, d2s, p2s, fwd, f) " +
                "( SELECT tid as nid, (cost+fop.d2s) as d2s, fid as p2s, ? as fwd, false as f" +
                " FROM " + getTableNameTE() + ", fop, rb " +
                "WHERE fid=fop.nid and tid=rb.nid ) ON CONFLICT(nid) DO UPDATE SET d2s=excluded.d2s, p2s=excluded.p2s, fwd=excluded.fwd,f=excluded.f" +
                " WHERE " + getTableNameTA() + ".d2s>excluded.d2s RETURNING nid, d2s)" +
                "SELECT count(nid) as affected, min(d2s) as mind2s FROM mop;";
    }
}
