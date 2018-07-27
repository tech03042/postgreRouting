package kr.co.kdelab.postgre.routing.redfish.reachability.impl;

import kr.co.kdelab.postgre.routing.redfish.reachability.RechabliltyCalculator;
import kr.co.kdelab.postgre.routing.redfish.util.JDBConnectionInfo;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

public class Submit1Calculator extends RechabliltyCalculator {
    private final JDBConnectionInfo jdbConnectionInfo;
    private boolean doubleUndirected;

    public Submit1Calculator(JDBConnectionInfo jdbConnectionInfo, boolean doubleUndirected) throws SQLException {
        super(jdbConnectionInfo.createConnection());
        this.jdbConnectionInfo = jdbConnectionInfo;
        this.doubleUndirected = doubleUndirected;
    }

    @Override
    public boolean calc(int source, int target) throws SQLException {
        PreparedStatement expander = newPreparedStatement(getConnection().prepareStatement("    WITH expandTargetClear AS (DELETE FROM expandTarget\n" +
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
        PreparedStatement deleter;

        try (Statement statement = getConnection().createStatement()) {
            statement.execute("" +
                    "  DROP TABLE IF EXISTS visited;\n" +
                    "  DROP TABLE IF EXISTS expandTarget;\n" +
                    "  DROP TABLE IF EXISTS expandTargetWrap;\n" +
                    "  CREATE UNLOGGED TABLE visited (\n" +
                    "    nid int,\n" +
                    "    p2s int,\n" +
                    "    PRIMARY KEY (nid, p2s)\n" +
                    "  );\n" +
                    "  CREATE UNLOGGED TABLE expandTarget (\n" +
                    "    nid int PRIMARY KEY\n" +
                    "  );\n" +
                    "  CREATE UNLOGGED TABLE expandTargetWrap (\n" +
                    "    nid INT PRIMARY KEY\n" +
                    "  );\n");

            statement.execute("INSERT INTO expandTarget (nid) VALUES (" + source + ");");

            while (expander.executeUpdate() != 0) ;

            statement.execute("DROP TABLE IF EXISTS expandTarget; DROP TABLE IF EXISTS expandTargetWrap;");

            statement.execute("DROP TABLE IF EXISTS deleteNids;");
            statement.execute("CREATE UNLOGGED TABLE deleteNids (nid int primary key);");
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

            statement.execute("DROP TABLE IF EXISTS deleteNids; DROP TABLE IF EXISTS rb;" +
                    "  CREATE UNLOGGED TABLE rb (\n" +
                    "    nid INT PRIMARY KEY\n" +
                    "  );\n" +
                    "  INSERT INTO rb (nid) SELECT distinct nid FROM visited;\n" +
                    "\n" +
                    "  DROP TABLE IF EXISTS visited;");

        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }
}
