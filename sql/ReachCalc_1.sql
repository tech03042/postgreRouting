-- Progressing....

DROP FUNCTION IF EXISTS rcalc(integer, integer);
DROP FUNCTION IF EXISTS rcalc(integer, integer, boolean);
CREATE FUNCTION rcalc(source integer, target integer, double_undirected boolean)
  RETURNS BOOLEAN AS $$
DECLARE
  affected integer;
BEGIN
  CREATE TEMP TABLE visited (
    nid int,
    p2s int,
    PRIMARY KEY (nid, p2s)
  ) ON COMMIT DROP;
  CREATE TEMP TABLE expandTarget (
    nid int PRIMARY KEY
  ) ON COMMIT DROP;

  --   INSERT INTO expandTargetUnique (nid) VALUES (source), (target);
  INSERT INTO expandTarget (nid) VALUES (source);

  LOOP
    WITH expandTargetClear AS (DELETE FROM expandTarget
    RETURNING nid),
         findNewTarget AS (INSERT INTO visited (nid, p2s)
      SELECT te.tid, te.fid
      FROM te,
           expandTargetClear
      WHERE expandTargetClear.nid = te.fid
    ON CONFLICT DO NOTHING
    RETURNING nid)
    INSERT INTO expandTarget (nid)
    SELECT nid
    FROM findNewTarget
    ON CONFLICT DO NOTHING;


    GET DIAGNOSTICS affected = ROW_COUNT;

    IF affected < 1
    THEN
      EXIT;
    END IF;

  END LOOP;

  DROP TABLE expandTarget;

  IF double_undirected
  THEN
    CREATE TEMP TABLE deleteNids (
      nid int primary key
    );
  end if;

  LOOP
    IF double_undirected
    THEN
      --       USA_ROAD 분기
      DELETE FROM deleteNids;
      INSERT INTO deleteNids (nid)
      SELECT nid
      FROM (SELECT visited.nid, count(*) as cnt
            FROM visited,
                 te
            WHERE visited.nid = te.tid
               or visited.nid = te.fid
            GROUP BY visited.nid) tmp
      WHERE tmp.cnt = 2
        and tmp.nid <> source
        and tmp.nid <> target;

      DELETE FROM visited USING deleteNids WHERE visited.nid = deleteNids.nid
                                              or visited.p2s = deleteNids.nid;

      DELETE FROM te
          USING deleteNids WHERE te.fid = deleteNids.nid
                              or te.tid = deleteNids.nid;
    ELSE
      WITH a as (DELETE FROM visited
      USING (SELECT visited.nid, count(*) as cnt
             FROM visited,
                  te
             WHERE visited.nid = te.tid
                or visited.nid = te.fid
             GROUP BY visited.nid) tmp
      WHERE visited.nid = tmp.nid and cnt = 1
            AND visited.nid <> source and visited.nid <> target
      RETURNING visited.nid, visited.p2s)
      DELETE
      FROM te USING a
      WHERE te.tid = a.nid;
    end if;

    GET DIAGNOSTICS affected = ROW_COUNT;

    IF affected < 1
    THEN
      EXIT;
    END IF;
  END LOOP;

  DROP TABLE IF EXISTS rb;
  CREATE UNLOGGED TABLE rb (
    nid INT PRIMARY KEY
  );
  INSERT INTO rb (nid) SELECT distinct nid FROM visited;

  RETURN TRUE;
END;
$$
LANGUAGE plpgsql;


select *
from rcalc(13113, 435663, true);