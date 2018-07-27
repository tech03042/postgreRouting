DROP FUNCTION IF EXISTS rcalc(integer, integer);
DROP FUNCTION IF EXISTS rcalc(integer, integer, boolean);
DROP FUNCTION IF EXISTS rcalc(integer, integer, boolean, boolean);
CREATE FUNCTION rcalc(source integer, target integer, double_undirected boolean, use_te_flag boolean)
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

  raise notice 'END';
  CREATE TEMP TABLE deleteNids (
    nid int primary key
  ) ON COMMIT DROP;

  ALTER TABLE te
    ADD setNotUse boolean DEFAULT false;

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
            WHERE (visited.nid = te.tid
                     or visited.nid = te.fid)
              and te.setNotUse = false
            GROUP BY visited.nid) tmp
      WHERE tmp.cnt = 2
        and tmp.nid <> source
        and tmp.nid <> target;

      DELETE FROM visited USING deleteNids WHERE visited.nid = deleteNids.nid
                                              or visited.p2s = deleteNids.nid;

      UPDATE te
      SET te.setNotUse = true
      FROM deleteNids
      WHERE te.fid = deleteNids.nid
         or te.tid = deleteNids.nid;

    ELSE
      WITH a as (DELETE FROM visited
      USING (SELECT visited.nid, count(*) as cnt
             FROM visited,
                  te
             WHERE (visited.nid = te.tid
                      or visited.nid = te.fid)
               and te.setNoutUse = false
             GROUP BY visited.nid) tmp
      WHERE visited.nid = tmp.nid and cnt = 1
            AND visited.nid <> source and visited.nid <> target
      RETURNING visited.nid, visited.p2s)
      UPDATE te
      SET te.setNotUse = true
      FROM a
      WHERE te.tid = a.nid;
    end if;

    GET DIAGNOSTICS affected = ROW_COUNT;

    IF affected < 1
    THEN
      EXIT;
    END IF;
  END LOOP;

  IF !use_te_flag
  THEN
    ALTER TABLE te DROP setNotUse;
  END IF
  
  DROP TABLE IF EXISTS rb;
  CREATE UNLOGGED TABLE rb (
    nid INT PRIMARY KEY
  );
  INSERT INTO rb (nid) SELECT distinct nid FROM visited;

  RETURN TRUE;
END;
$$
LANGUAGE plpgsql;

