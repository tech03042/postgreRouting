package kr.co.kdelab.postgre.routing.tangcori.test;

import kr.co.kdelab.postgre.routing.redfish.reachability.RechabliltyCalculator;
import kr.co.kdelab.postgre.routing.redfish.util.JDBConnectionInfo;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

public class yago_RouteExplorer {


    Connection conn;

    public yago_RouteExplorer() {

        try {
            this.conn = new JDBConnectionInfo("jdbc:postgresql://localhost:5432/kdelab",
                    "postgres", "kdelab", "kdelab").createConnection();

            this.conn.setSchema("kdelab");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args){

        new yago_RouteExplorer().find();
    }
    public void find() {


        try {
            int i=0;
            for (i = 1; i < 3000; i++) {
                String query = "set schema 'kdelab';\n" +
                        "\n" +
                        "DROP FUNCTION IF EXISTS exploration(integer, integer);\n" +
                        "DROP FUNCTION IF EXISTS exploration(integer, integer, boolean);\n" +
                        "DROP FUNCTION IF EXISTS exploration(integer, integer, boolean, boolean);\n" +
                        "\n" +
                        "\n" +
                        "CREATE FUNCTION exploration(startNode integer, finishNode integer)\n" +
                        "  RETURNS BOOLEAN AS $$\n" +
                        "DECLARE\n" +
                        "  routeCount integer;" +
                        "  affected integer;\n" +

                        "BEGIN\n" +
                        "  DROP TABLE IF EXISTS visited;\n" +
                        "  DROP TABLE IF EXISTS expandTarget;\n" +

                        "  CREATE UNLOGGED TABLE visited (\n" +
                        "    nid int,\n" +
                        "    p2s int,\n" +
                        "    cnt int,\n" +
                        "    PRIMARY KEY (nid,p2s)\n" +
                        "  );\n" +

                        "  CREATE UNLOGGED TABLE expandTarget (\n" +
                        "    nid int PRIMARY KEY, \n" +
                        "    cnt int \n" +
                        "  );\n" +

                        "  INSERT INTO expandTarget VALUES (startNode,1);\n" +
                        "\n" +
                        "  LOOP\n" +
                        "    WITH expandTargetClear AS (DELETE FROM expandTarget RETURNING nid,cnt ),\n" +
                        "         findNewTarget AS ( \n" +
                        "      INSERT INTO visited \n" +
                        "      SELECT te.tid, te.fid, (expandTargetClear.cnt+1) \n" +
                        "      FROM te, expandTargetClear\n" +
                        "      WHERE expandTargetClear.nid = te.fid\n" +
                        "    ON CONFLICT DO NOTHING\n" +
                        "    RETURNING nid,cnt ) \n" +
                        "    INSERT INTO expandTarget \n" +
                        "    SELECT nid,cnt \n" +
                        "    FROM findNewTarget\n" +
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


//                        "  DROP TABLE IF EXISTS explored;\n" +
//                        "  CREATE UNLOGGED TABLE explored (\n" +
//                        "    nid INT ,\n" +
//                        "    p2s INT ,\n" +
//                        "    cnt INT" +
//                        "  );\n" +
                        "  INSERT INTO explored (nid,p2s,cnt) SELECT * FROM visited;\n" +
                        "\n" +
                        "  DROP TABLE IF EXISTS visited;\n" +
                        "\n" +
                        "  RETURN TRUE;\n" +
                        "END;\n" +
                        "$$\n" +
                        "LANGUAGE plpgsql;\n" +
                        "select * from exploration(+"+i+",100);";

                this.conn.createStatement().execute(query);

            }
            } catch(Exception e){
                e.printStackTrace();
            }
        }

    }
