package kr.co.kdelab.postgre.routing.redfish.reachability.impl;

import kr.co.kdelab.postgre.routing.redfish.reachability.RechabliltyCalculator;
import kr.co.kdelab.postgre.routing.redfish.util.JDBConnectionInfo;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

public class Submit1Calculator extends RechabliltyCalculator {
    private boolean doubleUndirected;

    public Submit1Calculator(JDBConnectionInfo jdbConnectionInfo, boolean doubleUndirected) throws SQLException {
        super(jdbConnectionInfo.createConnection());
        this.doubleUndirected = doubleUndirected;
    }

    @Override
    public void calc(int source, int target) throws SQLException {
        PreparedStatement expander;
        PreparedStatement deleter;

        try (Statement statement = getConnection().createStatement()) {
            statement.execute(
                    "  CREATE UNLOGGED TABLE IF NOT EXISTS visited (\n" +
                            "    nid int,\n" +
                            "    p2s int,\n" +
                            "    PRIMARY KEY (nid, p2s)\n" +
                            "  );\n" +
                            "  CREATE UNLOGGED TABLE IF NOT EXISTS expandTarget (\n" +
                            "    nid int PRIMARY KEY\n" +
                            "  );\n" +
                            "  CREATE UNLOGGED TABLE IF NOT EXISTS expandTargetWrap (\n" +
                            "    nid INT PRIMARY KEY\n" +
                            "  );\n");
            statement.execute("TRUNCATE TABLE visited");
            statement.execute("TRUNCATE TABLE expandTarget");
            statement.execute("TRUNCATE TABLE expandTargetWrap");

            statement.execute("INSERT INTO expandTarget (nid) VALUES (" + source + ");");

            expander = newPreparedStatement(getConnection().prepareStatement("    WITH expandTargetClear AS (DELETE FROM expandTarget\n" +
                    "    RETURNING nid),\n" +
                    "         findNewTarget AS (INSERT INTO visited (nid, p2s)\n" +
                    "      SELECT te.tid, te.fid\n" +
                    "      FROM te,\n" +
                    "           expandTargetClear\n" +
                    "      WHERE expandTargetClear.nid = te.fid\n" +
                    "    ON CONFLICT DO NOTHING\n" +
                    "    RETURNING nid),\n" +
                    "         targetDistinct AS (INSERT INTO expandTargetWrap (nid) SELECT nid FROM findNewTarget\n" +
                    "    ON CONFLICT DO NOTHING\n" +
                    "    RETURNING nid)\n" +
                    "    INSERT INTO expandTarget (nid)\n" +
                    "    SELECT nid\n" +
                    "    FROM findNewTarget\n" +
                    "    ON CONFLICT DO NOTHING;\n"));
            while (expander.executeUpdate() != 0) ;

            statement.execute("TRUNCATE TABLE expandTarget;");
            statement.execute("TRUNCATE TABLE expandTargetWrap;");

            statement.execute("CREATE UNLOGGED TABLE IF NOT EXISTS deleteNids (nid int primary key);");
            statement.execute("TRUNCATE TABLE deleteNids;");
            statement.execute("ALTER TABLE te ADD setNotUse boolean DEFAULT false;");

            if (doubleUndirected) {
                deleter = newPreparedStatement(getConnection().prepareStatement("DELETE FROM deleteNids;\n" +
                        "INSERT INTO deleteNids (nid)\n" +
                        "SELECT nid\n" +
                        "FROM (SELECT visited.nid, count(*) as cnt\n" +
                        "FROM visited,\n" +
                        "     te\n" +
                        "WHERE (visited.nid = te.tid\n" +
                        "   or visited.nid = te.fid)\n" +
                        "  and te.setNotUse = false\n" +
                        "GROUP BY visited.nid) tmp\n" +
                        "WHERE tmp.cnt = 2\n" +
                        "  and tmp.nid <> " + source + "\n" +
                        "  and tmp.nid <> " + target + ";\n" +
                        "\n" +
                        "DELETE FROM visited USING deleteNids WHERE visited.nid = deleteNids.nid\n" +
                        "    or visited.p2s = deleteNids.nid;\n" +
                        "\n" +
                        "UPDATE te SET setNotUse = true FROM deleteNids WHERE te.fid = deleteNids.nid\n" +
                        "  or te.tid = deleteNids.nid;\n"));
            } else {
                deleter = newPreparedStatement(getConnection().prepareStatement("      WITH a as (DELETE FROM visited\n" +
                        "      USING (SELECT visited.nid, count(*) as cnt\n" +
                        "             FROM visited,\n" +
                        "                  te\n" +
                        "             WHERE (visited.nid = te.tid\n" +
                        "                      or visited.nid = te.fid)\n" +
                        "               and te.setNotUse = false\n" +
                        "             GROUP BY visited.nid) tmp\n" +
                        "      WHERE visited.nid = tmp.nid and cnt = 1\n" +
                        "            AND visited.nid <> " + source + " and visited.nid <> " + target + "\n" +
                        "      RETURNING visited.nid, visited.p2s)\n" +
                        "      UPDATE te\n" +
                        "      SET setNotUse = true\n" +
                        "      FROM a\n" +
                        "      WHERE te.tid = a.nid;\n"));
            }

            while (deleter.executeUpdate() != 0) ;

            statement.execute("ALTER TABLE te DROP setNotUse;");

            statement.execute("  CREATE UNLOGGED TABLE IF NOT EXISTS rb (\n" +
                    "    nid INT PRIMARY KEY)");
            statement.execute("TRUNCATE TABLE rb;");
            statement.execute("  INSERT INTO rb (nid) SELECT distinct nid FROM visited;");

        }
    }
}
