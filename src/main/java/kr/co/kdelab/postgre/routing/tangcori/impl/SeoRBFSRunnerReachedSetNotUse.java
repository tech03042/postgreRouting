package kr.co.kdelab.postgre.routing.tangcori.impl;

import java.sql.SQLException;
import java.sql.Statement;

public class SeoRBFSRunnerReachedSetNotUse extends SeoRBFSRunner {
    public SeoRBFSRunnerReachedSetNotUse(int pts, int pv) {
        super(pts, pv);
    }

    @Override
    int perform(int iteration, float minCost, float dist_sofar, boolean isForward) throws SQLException {
        if (isForward)
            return perform_backward_setNotUse(iteration, minCost, dist_sofar);
        else
            return perform_backward_setNotUse(iteration, minCost, dist_sofar);
    }


    private int perform_forward_setNotUse(int iteration, float minCost, float dist_sofar) throws SQLException {
        int affected = 0;
        int[] k = new int[2];
        StringBuilder er_f_string;
        String ta_f_update_string;
        String ta_f_insert_string;

        int l = Math.max(1, iteration - getPts() + 1);
        int end_idx = iteration - l + 1;

        k[0] = end_idx; // int end_idx = iteration - l + 1;
        k[1] = l; // int l = Math.max(1, iteration-pts+1);

        er_f_string = new StringBuilder("" + "CREATE VIEW er_f_" + iteration + " (id,p2s,d2s) AS" + " "
                + "( SELECT TE.tid, TE.fid, TF.d2s+TE.cost" + " "
                + "FROM ta AS TF, te_" + k[0] + " AS TE" + " "
                + "WHERE TF.fwd = " + k[1] + " "
                + "AND TF.nid = TE.fid" + " "
                + "AND TF.d2s+TE.cost < "
                + (minCost - dist_sofar) + " ) ");
        k[0]--;
        k[1]++;

        while (k[0] > 0 && k[1] <= iteration) {
            er_f_string.append(" UNION " + "( SELECT TE.tid , TE.fid, TF.d2s+TE.cost" + " " + "FROM ta AS TF , te_").append(k[0]).append(" AS TE").append(" ").append("WHERE TF.fwd=").append(k[1]).append(" ").append("AND TF.nid = TE.fid").append(" ").append("AND TF.d2s+TE.cost < ").append(minCost - dist_sofar).append(" ) ");
            k[0]--;
            k[1]++;
        } // end while

        try (Statement statement = getConnection().createStatement()) {
            statement.execute(er_f_string.toString());


            // 4(2) fwd
            ta_f_update_string = ""
                    + "UPDATE ta "
                    + "SET d2s=source.cost, p2s=source.p2s, fwd=" + (iteration + 1) + " "
                    + "FROM ta AS target, "
                    + "(SELECT nid,p2s,cost "
                    + " FROM (SELECT er_f_" + iteration + ".id, "
                    + "	 er_f_" + iteration + ".p2s, "
                    + "  er_f_" + iteration + ".d2s, "
                    + "  ROW_NUMBER() OVER (PARTITION BY id ORDER BY d2s ASC) AS rownum "
                    //+ "FROM er_f_"+iteration+"  ) AS tmp1(nid,p2s,cost,rownum) "
                    //+ "FROM er_f_"+iteration+", rb WHERE er_f_" + iteration + ".id = rb.nid  ) AS tmp1(nid,p2s,cost,rownum) "//backup
                    + "FROM er_f_" + iteration + ", rb_p2s WHERE er_f_" + iteration + ".id = rb_p2s.nid and nu = false   ) AS tmp1(nid,p2s,cost,rownum) "
                    + " WHERE rownum=1) AS source(nid,p2s,cost) "
                    + "WHERE source.nid = ta.nid "
                    + "AND ta.d2s > source.cost ";

            affected += statement.executeUpdate(ta_f_update_string);

            ta_f_insert_string = ""
                    + "INSERT INTO ta(nid,d2s,p2s,fwd) "
                    + "SELECT source.nid , source.cost , source.p2s ," + (iteration + 1) + " "
                    + "FROM (SELECT nid,p2s,cost "
                    + " FROM (SELECT er_f_" + iteration + ".id, "
                    + "	 er_f_" + iteration + ".p2s, "
                    + "  er_f_" + iteration + ".d2s, "
                    + "  ROW_NUMBER() OVER (PARTITION BY id ORDER BY d2s ASC) AS rownum "
                    //+ "FROM er_f_"+iteration+" ) AS tmp1(nid,p2s,cost,rownum) "
                    //+ "FROM er_f_"+iteration+", rb WHERE er_f_" + iteration + ".id = rb.nid  ) AS tmp1(nid,p2s,cost,rownum) "
                    + "FROM er_f_" + iteration + ", rb_p2s WHERE er_f_" + iteration + ".id = rb_p2s.nid and nu = false ) AS tmp1(nid,p2s,cost,rownum) "

                    + " WHERE rownum=1) AS source(nid,p2s,cost) "
                    + "WHERE source.nid NOT IN (SELECT nid from ta) ";
            affected += statement.executeUpdate(ta_f_insert_string);
        }

        return affected;

    } // end perform

    private int perform_backward_setNotUse(int iteration, float minCost, float dist_sofar) throws SQLException {

        int affected = 0;
        int[] k = new int[2];
        StringBuilder er_b_string;
        String ta_b_update_string;
        String ta_b_insert_string;
        int l = Math.max(1, iteration - getPts() + 1);
        int end_idx = iteration - l + 1;

        // 4(1)
        k[0] = end_idx; // int end_idx = iteration - l + 1;
        k[1] = l; // int l = Math.max(1, iteration-pts+1);
        er_b_string = new StringBuilder("" + "CREATE VIEW er_b_" + iteration + " (id,p2s,d2s) AS" + " "
                + "( SELECT TE.fid, TE.tid, TF.d2s+TE.cost" + " " + "FROM ta2 AS TF, te_" + k[0]
                + " AS TE" + " " + "WHERE TF.fwd = " + k[1] + " " + "AND TF.nid = TE.tid" + " "
                + "AND TF.d2s+TE.cost < " + (minCost - dist_sofar) + " ) ");
        k[0]--;
        k[1]++;

        while (k[0] > 0 && k[1] <= iteration) {
            er_b_string.append(" UNION " + "(SELECT TE.fid , TE.tid, TF.d2s+TE.cost" + " " + "FROM ta2 AS TF , te_").append(k[0]).append(" AS TE").append(" ").append("WHERE TF.fwd=").append(k[1]).append(" ").append("AND TF.nid=TE.tid").append(" ").append("AND TF.d2s+TE.cost<").append(minCost - dist_sofar).append(") ");

            k[0]--;
            k[1]++;
        } // end while

        er_b_string = new StringBuilder(er_b_string.toString());
        try (Statement statement = getConnection().createStatement()) {
            statement.execute(er_b_string.toString());

            // 4(2) fwd
            ta_b_update_string = ""
                    + "UPDATE ta2 "
                    + "SET d2s=source.cost, p2s=source.p2s, fwd=" + (iteration + 1) + " "
                    + "FROM ta2 AS target, "
                    + "(SELECT nid,p2s,cost "
                    + " FROM (SELECT er_b_" + iteration + ".id, "
                    + "	 er_b_" + iteration + ".p2s, "
                    + "  er_b_" + iteration + ".d2s, "
                    + "  ROW_NUMBER() OVER (PARTITION BY id ORDER BY d2s ASC) AS rownum "
                    //+ "FROM er_b_"+iteration+"   ) AS tmp1(nid,p2s,cost,rownum) "
                    //+ "FROM er_b_"+iteration+", rb WHERE er_b_" + iteration + ".id = rb.nid  ) AS tmp1(nid,p2s,cost,rownum) "
                    + "FROM er_b_" + iteration + ", rb_p2s WHERE er_b_" + iteration + ".id = rb_p2s.nid and nu = false ) AS tmp1(nid,p2s,cost,rownum) "

                    + " WHERE rownum=1) AS source(nid,p2s,cost) "
                    + "WHERE source.nid = ta2.nid "
                    + "AND ta2.d2s > source.cost ";
            //WHERE er_f_" + iteration + ".id NOT IN rb
            affected += statement.executeUpdate(ta_b_update_string);

            ta_b_insert_string = ""
                    + "INSERT INTO ta2(nid,d2s,p2s,fwd) "
                    + "SELECT source.nid , source.cost , source.p2s ," + (iteration + 1) + " "
                    + "FROM (SELECT nid,p2s,cost "
                    + " FROM (SELECT er_b_" + iteration + ".id, "
                    + "	 er_b_" + iteration + ".p2s, "
                    + "  er_b_" + iteration + ".d2s, "
                    + "  ROW_NUMBER() OVER (PARTITION BY id ORDER BY d2s ASC) AS rownum "
                    //+ "FROM er_b_"+iteration+" ) AS tmp1(nid,p2s,cost,rownum) "
                    //+ "FROM er_b_"+iteration+", rb WHERE er_b_" + iteration + ".id = rb.nid ) AS tmp1(nid,p2s,cost,rownum) "
                    + "FROM er_b_" + iteration + ", rb_p2s WHERE er_b_" + iteration + ".id = rb_p2s.nid and nu = false ) AS tmp1(nid,p2s,cost,rownum) "

                    + " WHERE rownum=1) AS source(nid,p2s,cost) "
                    + "WHERE source.nid NOT IN (SELECT nid from ta2) ";

            affected += statement.executeUpdate(ta_b_insert_string);
        }
        return affected;
    } // end perform


}
