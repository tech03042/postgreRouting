package kr.co.kdelab.postgre.routing.tangcori;

import kr.co.kdelab.postgre.routing.redfish.reachability.RechabliltyCalculator;
import kr.co.kdelab.postgre.routing.redfish.reachability.impl.JoinCalculator;
import kr.co.kdelab.postgre.routing.redfish.reachability.impl.Submit1Calculator;
import kr.co.kdelab.postgre.routing.redfish.util.JDBConnectionInfo;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

// TODO: 18. 7. 30 !!작업중!!
public class RB {

    public RB(String target) {

    }

    public static String P_Basic_plsql(Connection conn, int source, int target) {

        try (Statement reachability = conn.createStatement()) {
            conn.setSchema("kdelab");

            reachability.execute(" " +
                    "DROP FUNCTION IF EXISTS rcalc( integer, integer );\n" +
                    "CREATE FUNCTION rcalc(source integer, target integer)\n" +
                    "  RETURNS SETOF integer AS $$\n" +
                    "DECLARE\n" +
                    "  affected integer;\n" +
                    "  tmpI     integer;\n" +
                    "BEGIN\n" +
                    "  CREATE TEMP TABLE visited (\n" +
                    "    nid int,\n" +
                    "    p2s int,\n" +
                    "    PRIMARY KEY (nid, p2s)\n" +
                    "  ) ON COMMIT DROP;\n" +
                    "  CREATE TEMP TABLE expandTarget (\n" +
                    "    nid int PRIMARY KEY\n" +
                    "  ) ON COMMIT DROP;\n" +
                    "  CREATE TEMP TABLE expandTargetUnique (\n" +
                    "    nid int PRIMARY KEY\n" +
                    "  ) ON COMMIT DROP;\n" +
                    "\n" +
                    "  INSERT INTO expandTargetUnique (nid) VALUES (source), (target);\n" +
                    "  INSERT INTO expandTarget (nid) VALUES (source);\n" +
                    "\n" +
                    "  LOOP\n" +
                    "    WITH expandTargetClear AS (\n" +
                    "      DELETE FROM expandTarget\n" +
                    "      RETURNING nid)\n" +
                    "      , findNewTarget AS (\n" +
                    "      INSERT INTO visited (nid, p2s)\n" +
                    "        SELECT\n" +
                    "          te.tid,\n" +
                    "          te.fid\n" +
                    "        FROM te, expandTargetClear\n" +
                    "        WHERE expandTargetClear.nid = te.fid\n" +
                    "      ON CONFLICT DO NOTHING\n" +
                    "      RETURNING nid)\n" +
                    "      , expandTargetUniqueWrap AS (\n" +
                    "      INSERT INTO expandTargetUnique (nid)\n" +
                    "        SELECT nid\n" +
                    "        FROM findNewTarget\n" +
                    "      ON CONFLICT (nid)\n" +
                    "        DO NOTHING\n" +
                    "      RETURNING nid)\n" +
                    "    INSERT INTO expandTarget (nid)\n" +
                    "      SELECT nid\n" +
                    "      FROM expandTargetUniqueWrap\n" +
                    "    ON CONFLICT DO NOTHING;\n" +
                    "\n" +
                    "\n" +
                    "    GET DIAGNOSTICS affected = ROW_COUNT;\n" +
                    "\n" +
                    "    IF affected < 1\n" +
                    "    THEN\n" +
                    "      EXIT;\n" +
                    "    END IF;\n" +
                    "\n" +
                    "  END LOOP;\n" +
                    "\n" +
                    "  DROP TABLE expandTargetUnique;\n" +
                    "  DROP TABLE expandTarget;\n" +
                    "\n" +
                    "  CREATE TEMP TABLE DeleteTmp (\n" +
                    "    nid int,\n" +
                    "    p2s int\n" +
                    "  ) ON COMMIT DROP;\n" +
                    "  CREATE TEMP TABLE DeleteSum (\n" +
                    "    nid int\n" +
                    "  ) ON COMMIT DROP;\n" +
                    "\n" +
                    "\n" +
                    "  WITH deleteFirst AS (\n" +
                    "    DELETE FROM visited\n" +
                    "    USING\n" +
                    "      (SELECT\n" +
                    "         visited.nid,\n" +
                    "         count(*) as cnt\n" +
                    "       FROM visited, te\n" +
                    "       WHERE visited.nid = te.fid\n" +
                    "       GROUP BY visited.nid) tmp\n" +
                    "    WHERE tmp.cnt = 1 and visited.nid = tmp.nid -- ������ ���� 0, USA_ROAD �����Ͱ� ��������� ����� ���¶� 1\n" +
                    "    RETURNING visited.nid, visited.p2s)\n" +
                    "    , affectedRows AS (\n" +
                    "    INSERT INTO DeleteTmp (nid, p2s) SELECT\n" +
                    "                                       nid,\n" +
                    "                                       p2s\n" +
                    "                                     FROM deleteFirst\n" +
                    "    RETURNING nid)\n" +
                    "  INSERT INTO DeleteSum (nid) SELECT nid\n" +
                    "                              FROM affectedRows;\n" +
                    "\n" +
                    "  tmpI = 2;\n" +
                    "  --   ������ ��� 1, USA_ROAD Ư���� ���� 2\n" +
                    "  LOOP\n" +
                    "    WITH\n" +
                    "        tmpClear AS (\n" +
                    "        DELETE FROM DeleteTmp\n" +
                    "        RETURNING nid, p2s),\n" +
                    "        deleteSecond AS (\n" +
                    "        DELETE FROM visited\n" +
                    "        USING (SELECT\n" +
                    "                 DeleteTmp.p2s,\n" +
                    "                 count(*) as cnt\n" +
                    "               FROM tmpClear as DeleteTmp, te\n" +
                    "               WHERE DeleteTmp.p2s = te.fid\n" +
                    "               GROUP BY DeleteTmp.p2s) tmp\n" +
                    "        WHERE visited.nid = tmp.p2s and tmp.cnt = tmpI\n" +
                    "        RETURNING visited.nid, visited.p2s),\n" +
                    "        affectedRows AS (\n" +
                    "        INSERT INTO DeleteTmp (nid, p2s) SELECT\n" +
                    "                                           nid,\n" +
                    "                                           p2s\n" +
                    "                                         FROM deleteSecond\n" +
                    "        RETURNING nid)\n" +
                    "    INSERT INTO DeleteSum (nid) SELECT nid\n" +
                    "                                FROM affectedRows;\n" +
                    "\n" +
                    "\n" +
                    "    GET DIAGNOSTICS affected = ROW_COUNT;\n" +
                    "    raise notice '%', affected;\n" +
                    "\n" +
                    "    IF affected < 1\n" +
                    "    THEN\n" +
                    "      EXIT;\n" +
                    "    END IF;\n" +
                    "  END LOOP;\n" +
                    "\n" +
                    "  RETURN QUERY (SELECT distinct nid\n" +
                    "                FROM visited);\n" +
                    "\n" +
                    "END;\n" +
                    "$$\n" +
                    "LANGUAGE plpgsql;");


            reachability.execute("DROP TABLE IF EXISTS rb; create table rb ( nid int primary key )");
            reachability.execute("INSERT INTO rb SELECT * FROM rcalc(" + source + "," + target + ")");

            //reachability.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return "P_Basic";
    }


    public static String P_Cost(Connection conn, int source, int target) {
        try (Statement reachability = conn.createStatement()) {
            conn.setSchema("kdelab");
            reachability.execute("DROP FUNCTION IF EXISTS rcalc( integer, integer );\n" +
                    "\n" +
                    "CREATE FUNCTION rcalc(start integer, target integer)\n" +
                    "  RETURNS SETOF integer\n" +
                    "AS $$\n" +
                    "DECLARE\n" +
                    "  tmpcnt      integer;\n" +
                    "  ranPathCost integer;\n" +
                    "BEGIN\n" +
                    "\n" +
                    "  CREATE temp table visited (\n" +
                    "    nid  int,\n" +
                    "    p2s  int,\n" +
                    "    cost int,\n" +
                    "    PRIMARY KEY (nid, p2s)\n" +
                    "  ) ON COMMIT DROP;\n" +
                    "  CREATE TEMP TABLE expandTarget (\n" +
                    "    nid  int PRIMARY KEY,\n" +
                    "    cost int\n" +
                    "  )\n" +
                    "  ON COMMIT DROP;\n" +
                    "  CREATE TEMP TABLE groupByResult (\n" +
                    "    nid int PRIMARY KEY,\n" +
                    "    cnt int\n" +
                    "  )\n" +
                    "  ON COMMIT DROP;\n" +
                    "\n" +
                    "  INSERT INTO expandTarget (nid, cost) VALUES (start, 0);\n" +
                    "\n" +
                    "  LOOP\n" +
                    "    IF ranPathCost is null\n" +
                    "    THEN\n" +
                    "      WITH expandTargetReset AS (\n" +
                    "        DELETE FROM expandTarget\n" +
                    "        RETURNING nid, cost),\n" +
                    "          findNewTarget AS (\n" +
                    "          INSERT INTO visited (nid, p2s, cost)\n" +
                    "            SELECT\n" +
                    "              te.tid,\n" +
                    "              te.fid,\n" +
                    "              te.cost + expandTargetReset.cost\n" +
                    "            FROM te, expandTargetReset\n" +
                    "            WHERE expandTargetReset.nid = te.fid\n" +
                    "          ON CONFLICT DO NOTHING\n" +
                    "          RETURNING nid, cost)\n" +
                    "      INSERT INTO expandTarget (nid, cost)\n" +
                    "        SELECT\n" +
                    "          nid,\n" +
                    "          cost\n" +
                    "        FROM findNewTarget\n" +
                    "        WHERE\n" +
                    "          findNewTarget.nid <> target\n" +
                    "      ON CONFLICT DO NOTHING;\n" +
                    "\n" +
                    "\n" +
                    "      GET DIAGNOSTICS tmpcnt = ROW_COUNT;\n" +
                    "\n" +
                    "      select min(cost)\n" +
                    "      into ranPathCost\n" +
                    "      from visited\n" +
                    "      where nid = target;\n" +
                    "    ELSE\n" +
                    "      WITH expandTargetReset AS (\n" +
                    "        DELETE FROM expandTarget\n" +
                    "        RETURNING nid, cost),\n" +
                    "          findNewTarget AS (\n" +
                    "          INSERT INTO visited (nid, p2s, cost)\n" +
                    "            SELECT\n" +
                    "              te.tid,\n" +
                    "              te.fid,\n" +
                    "              te.cost + expandTargetReset.cost as totalCost\n" +
                    "            FROM te, expandTargetReset\n" +
                    "            WHERE expandTargetReset.nid = te.fid and te.cost + expandTargetReset.cost <= ranPathCost\n" +
                    "          ON CONFLICT DO NOTHING\n" +
                    "          RETURNING nid, cost)\n" +
                    "      INSERT INTO expandTarget (nid, cost)\n" +
                    "        SELECT\n" +
                    "          nid,\n" +
                    "          cost\n" +
                    "        FROM findNewTarget\n" +
                    "        WHERE\n" +
                    "          findNewTarget.nid <> target\n" +
                    "      ON CONFLICT DO NOTHING;\n" +
                    "\n" +
                    "      GET DIAGNOSTICS tmpcnt = ROW_COUNT;\n" +
                    "    end if;\n" +
                    "\n" +
                    "    IF tmpcnt < 1\n" +
                    "    THEN\n" +
                    "      EXIT;\n" +
                    "    END IF;\n" +
                    "\n" +
                    "  END LOOP;\n" +
                    "\n" +
                    "  --   raise notice 'end %', ranPathCost;\n" +
                    "\n" +
                    "  INSERT INTO groupByResult (nid, cnt)\n" +
                    "    (SELECT\n" +
                    "       p2s        as nid,\n" +
                    "       count(nid) AS cnt\n" +
                    "     FROM\n" +
                    "       visited\n" +
                    "     GROUP BY\n" +
                    "       p2s);\n" +
                    "\n" +
                    "  LOOP\n" +
                    "    WITH deleteCntIsEnd AS (DELETE FROM visited\n" +
                    "    USING groupByResult\n" +
                    "    WHERE\n" +


                    "      groupByResult.cnt = 1 and\n" +     //DAG�ƴҋ��� 1�μ���
                    //"      groupByResult.cnt = 0 and\n" + //DAG�ϋ� 0���μ���

                    "      groupByResult.nid <> target and\n" +
                    "      groupByResult.nid = visited.nid\n" +
                    "    RETURNING visited.nid)\n" +
                    "    UPDATE groupByResult\n" +
                    "    set cnt = cnt - 1\n" +
                    "    FROM deleteCntIsEnd, visited\n" +
                    "    where deleteCntIsEnd.nid = visited.nid\n" +
                    "          and visited.p2s = groupByResult.nid;\n" +
                    "\n" +
                    "    GET DIAGNOSTICS tmpcnt = ROW_COUNT;\n" +
                    "    IF tmpcnt < 1\n" +
                    "    THEN\n" +
                    "      EXIT;\n" +
                    "    END IF;\n" +
                    "  END LOOP;\n" +
                    "\n" +
                    "  RETURN QUERY (\n" +
                    "    SELECT\n" +
                    "      DISTINCT nid\n" +
                    "    FROM\n" +
                    "      visited);\n" +
                    "END;\n" +
                    "$$\n" +
                    "LANGUAGE plpgsql");

            reachability.execute("create table IF NOT EXISTS rb ( nid int primary key )");
            reachability.execute("TRUNCATE rb");
            reachability.execute("INSERT INTO rb SELECT * FROM rcalc(" + source + "," + target + ")");
            reachability.close();


        } catch (Exception e) {
            e.printStackTrace();
        }

        return "P_Cost";

    }


    public static String P_setNotUse(Connection conn, int source, int target) {

        try {
            Statement setting = conn.createStatement();
            setting.execute("CREATE UNLOGGED TABLE IF NOT EXISTS rb_p2s (\n" +
                    "  nid int PRIMARY KEY,\n" +
                    "  cnt int  default 0,\n" +
                    "  nu  bool default false\n" +
                    ")");
            setting.execute("CREATE UNLOGGED TABLE IF NOT EXISTS rb_p2s_f (\n" +
                    "  nid int PRIMARY KEY\n" +
                    ")");
            setting.execute("CREATE UNLOGGED TABLE IF NOT EXISTS rb (\n" +
                    "  nid int,\n" +
                    "  p2s int,\n" +
                    "  PRIMARY KEY (nid, p2s)\n" +
                    ")");
            setting.execute("CREATE UNLOGGED TABLE IF NOT EXISTS tbRB (\n" +
                    "  nid int,\n" +
                    "  p2s int\n" +
                    ")");
            setting.execute("CREATE UNLOGGED TABLE IF NOT EXISTS tbRBCurrent (\n" +
                    "  nid int,\n" +
                    "  p2s int\n" +
                    ")");
            setting.execute("delete from rb_p2s_f");
            setting.execute("delete from rb_p2s");
            setting.execute("delete from rb");
            setting.execute("delete from tbRB");
            setting.execute("delete from tbRBCurrent");


//			setting.execute(	"DROP TABLE IF EXISTS te" );
//			setting.execute(	"CREATE UNLOGGED TABLE IF NOT EXISTS te AS "+ te_union );
            setting.execute("CREATE INDEX IF NOT EXISTS teIndex on te (fid)");

            Statement initianlize = conn.createStatement();
            initianlize.executeUpdate("insert into tbRBCurrent(nid) values(" + source + ")");
            initianlize.executeUpdate("insert into rb_p2s(nid) values(" + target + "), " + "(" + source + ")");
            initianlize.executeUpdate("insert into rb_p2s_f(nid) values(" + target + "), " + "(" + source + ")");


            ResultSet rs = null;
            Statement MSreachability = conn.createStatement();
            do {
                rs = MSreachability.executeQuery(
                        "WITH currentDelete AS (\n" +
                                "  DELETE FROM tbRBCurrent\n" +
                                "  RETURNING *\n" +
                                "), insertNewRB AS (\n" +
                                "  INSERT INTO tbRB (nid, p2s)\n" +
                                "    SELECT\n" +
                                "      te.tid,\n" +
                                "      te.fid\n" +
                                "    FROM te, currentDelete c\n" +
                                "    WHERE c.nid = te.fid\n" +
                                "  ON CONFLICT DO NOTHING\n" +
                                "  RETURNING nid\n" +
                                "), updateCurrent AS (\n" +
                                "  INSERT INTO rb_p2s (nid, cnt)\n" +
                                "    SELECT\n" +
                                "      nid,\n" +
                                "      cnt\n" +
                                "    FROM (SELECT\n" +
                                "            nid,\n" +
                                "            count(nid) as cnt\n" +
                                "          FROM insertNewRB\n" +
                                "          GROUP BY nid) target\n" +
                                "  ON CONFLICT (nid)\n" +
                                "    DO UPDATE SET cnt = rb_p2s.cnt + excluded.cnt\n" +
                                "  RETURNING nid\n" +
                                "), repackCurrent AS (\n" +
                                "  INSERT INTO rb_p2s_f (nid)\n" +
                                "    SELECT nid\n" +
                                "    FROM updateCurrent\n" +
                                "  ON CONFLICT DO NOTHING\n" +
                                "  RETURNING nid\n" +
                                "), putVisited AS (\n" +
                                "  INSERT INTO tbRBCurrent (nid) SELECT nid\n" +
                                "                                FROM repackCurrent\n" +
                                "  ON CONFLICT DO NOTHING\n" +
                                "  RETURNING nid\n" +
                                ")\n" +
                                "SELECT count(nid)\n" +
                                "FROM putVisited");
                if (rs.next()) {
                    if (rs.getInt(1) == 0)
                        break;
                } else
                    continue;

            } while (true);

            int notuse_result = 0;
            Statement notuse = conn.createStatement();
            do {
                notuse_result = notuse.executeUpdate(
                        "WITH setNotUSe AS (\n" +
                                "  UPDATE rb_p2s\n" +
                                "  SET cnt = -1, nu = true\n" +
                                "  WHERE cnt = 0 \n" +
                                "  RETURNING nid\n" +
                                ")\n" +
                                "UPDATE rb_p2s\n" +
                                "SET cnt = cnt - 1 From (SELECT p2s\n" +
                                "                        FROM tbRB, setNotUSe snu\n" +
                                "                        WHERE tbRB.nid = snu.nid) snur\n" +
                                "WHERE snur.p2s = rb_p2s.nid");
            } while (notuse_result != 0);

            setting.execute("update rb_p2s set nu = false where nid= " + source);

            MSreachability.close();

        } catch (Exception e) {
            e.printStackTrace();
        }

        return "P_setNotUse";
    }


    public static String joinR(JDBConnectionInfo jdbConnectionInfo, int source, int target) {
        try (JoinCalculator JCal = new JoinCalculator(jdbConnectionInfo);) {
            System.out.println(JCal.calc(source, target));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "joinR";
    }

    public static String P_Basic(JDBConnectionInfo jdbConnectionInfo, int source, int target, boolean isUndirected) {
        try (RechabliltyCalculator JCal = new Submit1Calculator(jdbConnectionInfo, isUndirected)) {
            System.out.println(JCal.calc(source, target));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "P_Basic";
    }

}
