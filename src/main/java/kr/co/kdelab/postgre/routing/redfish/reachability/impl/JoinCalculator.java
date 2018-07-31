package kr.co.kdelab.postgre.routing.redfish.reachability.impl;

import kr.co.kdelab.postgre.routing.redfish.reachability.RechabliltyCalculator;
import kr.co.kdelab.postgre.routing.redfish.util.JDBConnectionInfo;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

public class JoinCalculator extends RechabliltyCalculator {
    private JDBConnectionInfo jdbConnectionInfo;


    public JoinCalculator(JDBConnectionInfo jdbConnectionInfo) throws SQLException {
        super(jdbConnectionInfo.createConnection());
        this.jdbConnectionInfo = jdbConnectionInfo;

    }


    class Forward extends Division {
        Forward(int source, int target) {
            this.source = source;
            this.target = target;
        }

        @Override
        public void run() {
            super.run();

            try (Connection connection = jdbConnectionInfo.createConnection()) {
                try (Statement statement = connection.createStatement()) {
                    state = JoinCalculatorThreadState.RUNNING;
                    statement.execute("DROP TABLE IF EXISTS visited_f; CREATE UNLOGGED TABLE visited_f(nid int, p2s int, PRIMARY KEY (nid, p2s));");
                    statement.execute("DROP TABLE IF EXISTS expandTarget_f; CREATE UNLOGGED TABLE expandTarget_f(nid int primary key );");
                    statement.execute("INSERT INTO visited_f(nid, p2s) VALUES (" + source + "," + source + ")");
                    statement.execute("INSERT INTO expandTarget_f(nid) VALUES (" + source + ")");

                    try (PreparedStatement expander = connection.prepareStatement("WITH expandTargetReset AS (DELETE FROM expandTarget_f\n" +
                            "RETURNING nid),\n" +
                            "     findNewTarget AS (INSERT INTO visited_f (nid, p2s)\n" +
                            "  SELECT te.tid, te.fid\n" +
                            "  FROM te,\n" +
                            "       expandTargetReset\n" +
                            "  WHERE expandTargetReset.nid = te.fid\n" +
                            "ON CONFLICT do nothing\n" +
                            "RETURNING nid)\n" +
                            "INSERT INTO expandTarget_f (nid)\n" +
                            "SELECT nid\n" +
                            "FROM findNewTarget\n" +
                            "WHERE findNewTarget.nid <> " + source + "\n" +
                            "  and findNewTarget.nid <> " + target + "\n" +
                            "ON CONFLICT DO NOTHING;")) {
                        while (expander.executeUpdate() != 0) ;
                    }
                    statement.execute("DROP TABLE IF EXISTS expandTarget_f;");
                    state = JoinCalculatorThreadState.SUCCESS;
                }
            } catch (Exception e) {
                state = JoinCalculatorThreadState.ERROR;
            }
        }
    }

    class Backward extends Division {
        Backward(int source, int target) {
            this.source = source;
            this.target = target;
        }

        @Override
        public void run() {
            super.run();

            try (Connection connection = jdbConnectionInfo.createConnection()) {
                try (Statement statement = connection.createStatement()) {
                    state = JoinCalculatorThreadState.RUNNING;
                    statement.execute("DROP TABLE IF EXISTS visited_b; CREATE UNLOGGED TABLE visited_b(nid int, p2s int, PRIMARY KEY (nid, p2s));");
                    statement.execute("DROP TABLE IF EXISTS expandTarget_b; CREATE UNLOGGED TABLE expandTarget_b(nid int primary key );");
                    statement.execute("INSERT INTO visited_b(nid, p2s) VALUES (" + target + "," + target + ")");
                    statement.execute("INSERT INTO expandTarget_b(nid) VALUES (" + target + ")");

                    try (PreparedStatement expander = connection.prepareStatement("WITH expandTargetReset AS (DELETE FROM expandTarget_b\n" +
                            "RETURNING nid),\n" +
                            "     findNewTarget AS (INSERT INTO visited_b (nid, p2s)\n" +
                            "  SELECT te.fid, te.tid -- tid, fid\n" +
                            "  FROM te,\n" +
                            "       expandTargetReset\n" +
                            "  WHERE expandTargetReset.nid = te.tid -- fid\n" +
                            "ON CONFLICT do nothing\n" +
                            "RETURNING nid)\n" +
                            "INSERT INTO expandTarget_b (nid)\n" +
                            "SELECT nid\n" +
                            "FROM findNewTarget\n" +
                            "WHERE findNewTarget.nid <> " + source + "\n" +
                            "  and findNewTarget.nid <> " + target + "\n" +
                            "ON CONFLICT DO NOTHING;")) {
                        while (expander.executeUpdate() != 0) ;
                    }
                    statement.execute("DROP TABLE IF EXISTS expandTarget_b;");
                    state = JoinCalculatorThreadState.SUCCESS;
                }
            } catch (Exception e) {
                exception = e;
                state = JoinCalculatorThreadState.ERROR;
            }
        }
    }


    class Division extends Thread {
        Exception exception = null;
        int source, target;
        JoinCalculatorThreadState state = JoinCalculatorThreadState.PREPARE;
    }


    @Override
    public void calc(int source, int target) throws Exception {
        Division divisions[] = new Division[]{new Forward(source, target), new Backward(source, target)};
        for (Division division : divisions)
            division.start();
        while (divisions[0].state != JoinCalculatorThreadState.SUCCESS && divisions[1].state != JoinCalculatorThreadState.SUCCESS) {
            if (divisions[0].state == JoinCalculatorThreadState.ERROR || divisions[1].state == JoinCalculatorThreadState.ERROR) {
                if (divisions[0].exception != null)
                    throw divisions[0].exception;
                else
                    throw divisions[1].exception;
            }
            Thread.sleep(10);
        }

        try (Statement statement = getConnection().createStatement()) {
            statement.execute("DROP TABLE IF EXISTS rb; CREATE UNLOGGED TABLE rb(nid int primary key); " +
                    "insert into rb(nid) select distinct visited_f.nid FROM visited_f, visited_b WHERE visited_f.nid=visited_b.nid;" +
                    "DROP TABLE IF EXISTS visited_f; DROP TABLE IF EXISTS visited_b;");
        }
    }
}
