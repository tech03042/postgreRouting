package kr.co.kdelab.postgre.routing.redfish.reachability.impl;

import kr.co.kdelab.postgre.routing.redfish.reachability.RechabliltyCalculator;
import kr.co.kdelab.postgre.routing.redfish.util.JDBConnectionInfo;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

public class JoinCalculator extends RechabliltyCalculator {
    private JDBConnectionInfo jdbConnectionInfo;
    private PreparedStatement joinRB;


    public JoinCalculator(JDBConnectionInfo jdbConnectionInfo) throws SQLException {
        super(jdbConnectionInfo.createConnection());
        this.jdbConnectionInfo = jdbConnectionInfo;

        buildRechabilitySql(getConnection());
        buildSql(getConnection());

    }

    class Division extends Thread {
        private int source, target;
        private boolean isForward;

        private JoinCalculatorThreadState state = JoinCalculatorThreadState.PREPARE;


        Division(int source, int target, boolean isForward) {
            this.source = source;
            this.target = target;
            this.isForward = isForward;

        }

        @SuppressWarnings("all")
        @Override
        public void run() {
            super.run();

            try (Connection connection = jdbConnectionInfo.createConnection()) {
                try (Statement statement = connection.createStatement()) {
                    state = JoinCalculatorThreadState.RUNNING;
                    statement.execute("SELECT 1 FROM joinCalcFunction(" + source + ", " + target + ", " + String.valueOf(isForward) + ")");
                } catch (SQLException e) {
                    state = JoinCalculatorThreadState.ERROR;
                    e.printStackTrace();
                    return;
                }
                state = JoinCalculatorThreadState.SUCCESS;
            } catch (SQLException e) {
                state = JoinCalculatorThreadState.ERROR;
                e.printStackTrace();
            }
        }
    }

    public boolean calc(int source, int target) throws SQLException, InterruptedException {
        Division divisions[] = new Division[]{new Division(source, target, true), new Division(source, target, false)};
        for (Division division : divisions)
            division.start();
        while (divisions[0].state != JoinCalculatorThreadState.SUCCESS && divisions[1].state != JoinCalculatorThreadState.SUCCESS) {
            if (divisions[0].state == JoinCalculatorThreadState.ERROR || divisions[1].state == JoinCalculatorThreadState.ERROR) {
                return false;
            }
            Thread.sleep(10);
        }

        joinRB.execute();
        return true;
    }

    @SuppressWarnings("all")
    private void buildSql(Connection connection) throws SQLException {
        joinRB = newPreparedStatement(connection.prepareStatement("DROP TABLE IF EXISTS rb; CREATE UNLOGGED TABLE rb AS SELECT DISTINCT rb_f.nid FROM rb_f, rb_b WHERE rb_f.nid = rb_b.nid"));
    }

    @SuppressWarnings("all")
    private void buildRechabilitySql(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {

            statement.execute("DROP FUNCTION IF EXISTS joinCalcFunction( integer, integer, bool );\n" +
                    "\n" +
                    "-- 시작점, 도착점, (Forward: true,Backward: false) 구분\n" +
                    "CREATE FUNCTION joinCalcFunction(start integer, target integer, isForward bool)\n" +
                    "  RETURNS BOOLEAN\n" +
                    "AS $$\n" +
                    "DECLARE\n" +
                    "  affected integer;\n" +
                    "BEGIN\n" +
                    "  CREATE temp table visited (\n" +
                    "    nid int,\n" +
                    "    p2s int,\n" +
                    "    PRIMARY KEY (nid, p2s)\n" +
                    "  ) ON COMMIT DROP;\n" +
                    "  CREATE TEMP TABLE expandTarget (\n" +
                    "    nid int PRIMARY KEY\n" +
                    "  )\n" +
                    "  ON COMMIT DROP;\n" +
                    "\n" +
                    "  IF isForward -- Forward 는 시작 기준, Backward 는 도착 기준\n" +
                    "  THEN\n" +
                    "    INSERT INTO visited(nid, p2s) VALUES (start, start);\n" +
                    "    INSERT INTO expandTarget (nid) VALUES (start);\n" +
                    "  ELSE\n" +
                    "    INSERT INTO visited (nid, p2s) VALUES (target, target);\n" +
                    "    INSERT INTO expandTarget (nid) VALUES (target);\n" +
                    "  END IF;\n" +
                    "\n" +
                    "  LOOP\n" +
                    "    IF isForward -- Forward, Backward 분기\n" +
                    "    THEN\n" +
                    "      WITH expandTargetReset AS (\n" +
                    "        DELETE FROM expandTarget\n" +
                    "        RETURNING nid),\n" +
                    "          findNewTarget AS (\n" +
                    "          INSERT INTO visited (nid, p2s)\n" +
                    "            SELECT\n" +
                    "              te.tid,\n" +
                    "              te.fid\n" +
                    "            FROM te, expandTargetReset\n" +
                    "            WHERE expandTargetReset.nid = te.fid\n" +
                    "          ON CONFLICT do nothing\n" +
                    "          RETURNING nid )\n" +
                    "      INSERT INTO expandTarget (nid)\n" +
                    "        SELECT nid\n" +
                    "        FROM findNewTarget\n" +
                    "        WHERE\n" +
                    "          findNewTarget.nid <> target and findNewTarget.nid <> start\n" +
                    "      ON CONFLICT DO NOTHING;\n" +
                    "    ELSE\n" +
                    "      WITH expandTargetReset AS (\n" +
                    "        DELETE FROM expandTarget\n" +
                    "        RETURNING nid),\n" +
                    "          findNewTarget AS (\n" +
                    "          INSERT INTO visited (nid, p2s)\n" +
                    "            SELECT\n" +
                    "              te.fid,\n" +
                    "              te.tid -- tid, fid\n" +
                    "            FROM te, expandTargetReset\n" +
                    "            WHERE expandTargetReset.nid = te.tid -- fid\n" +
                    "          ON CONFLICT do nothing\n" +
                    "          RETURNING nid )\n" +
                    "      INSERT INTO expandTarget (nid)\n" +
                    "        SELECT nid\n" +
                    "        FROM findNewTarget\n" +
                    "        WHERE\n" +
                    "          findNewTarget.nid <> target and findNewTarget.nid <> start\n" +
                    "      ON CONFLICT DO NOTHING;\n" +
                    "    END IF;\n" +
                    "\n" +
                    "    GET DIAGNOSTICS affected = ROW_COUNT;\n" +
                    "\n" +
                    "    IF affected < 1\n" +
                    "    THEN\n" +
                    "      EXIT;\n" +
                    "    END IF;\n" +
                    "  END LOOP;\n" +
                    "\n" +
                    "  IF isForward\n" +
                    "  THEN\n" +
                    "    DROP TABLE IF EXISTS rb_f;\n" +
                    "    CREATE UNLOGGED TABLE rb_f  AS\n" +
                    "      SELECT DISTINCT nid\n" +
                    "      FROM visited;\n" +
                    "  ELSE\n" +
                    "    DROP TABLE IF EXISTS rb_b;\n" +
                    "    CREATE UNLOGGED TABLE rb_b AS\n" +
                    "      SELECT DISTINCT nid\n" +
                    "      FROM visited;\n" +
                    "  end if;\n" +
                    "  RETURN TRUE;\n" +
                    "END;\n" +
                    "$$\n" +
                    "LANGUAGE plpgsql;\n");
        }
    }

    @Override
    public void close() throws IOException {
        Statement statement = getCloseStatement();
        try {
            statement.execute("DROP FUNCTION IF EXISTS joinCalcFunction(integer, integer, bool);");
        } catch (SQLException e) {
            throw new IOException(e.getMessage(), e.getCause());
        }

        super.close(); // Must on bottom
    }
}
