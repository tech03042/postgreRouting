package kr.co.kdelab.postgre.routing.redfish.reachability.test;

import kr.co.kdelab.postgre.routing.redfish.reachability.impl.JoinCalculator;
import kr.co.kdelab.postgre.routing.redfish.reachability.impl.Submit1Calculator;
import kr.co.kdelab.postgre.routing.redfish.util.JDBConnectionInfo;

import java.io.IOException;
import java.sql.SQLException;

public class JoinCalc {
    public static void main(String[] args) {
        System.out.println("DROP FUNCTION IF EXISTS joinCalcFunction( integer, integer, bool );\n" +
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


        long s = System.currentTimeMillis();
        try (JoinCalculator calculator = new JoinCalculator(new JDBConnectionInfo("jdbc:postgresql://localhost:5432/kdelab", "postgres", "icdwvb4j", "kdelab"))) {
            if (calculator.calc(13113, 435663))
                System.out.println("계산 끝");
        } catch (SQLException | IOException e) {
            e.printStackTrace();
        }
        System.out.println(System.currentTimeMillis() - s);
    }
}
