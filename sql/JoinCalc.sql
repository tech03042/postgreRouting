DROP FUNCTION IF EXISTS joinCalcFunction(integer, integer, bool);

-- 시작점, 도착점, (Forward: true,Backward: false) 구분
CREATE FUNCTION joinCalcFunction(start integer, target integer, isForward bool)
  RETURNS BOOLEAN
AS $$
DECLARE
  affected integer;
BEGIN
  CREATE temp table visited (
    nid int,
    p2s int,
    PRIMARY KEY (nid, p2s)
  ) ON COMMIT DROP;
  CREATE TEMP TABLE expandTarget (
    nid int PRIMARY KEY
  )
  ON COMMIT DROP;

  IF isForward -- Forward 는 시작 기준, Backward 는 도착 기준
  THEN
    INSERT INTO visited (nid, p2s) VALUES (start, start);
    INSERT INTO expandTarget (nid) VALUES (start);
  ELSE
    INSERT INTO visited (nid, p2s) VALUES (target, target);
    INSERT INTO expandTarget (nid) VALUES (target);
  END IF;

  LOOP
    IF isForward -- Forward, Backward 분기
    THEN
      WITH expandTargetReset AS (DELETE FROM expandTarget
      RETURNING nid),
           findNewTarget AS (INSERT INTO visited (nid, p2s)
        SELECT te.tid, te.fid
        FROM te,
             expandTargetReset
        WHERE expandTargetReset.nid = te.fid
      ON CONFLICT do nothing
      RETURNING nid)
      INSERT INTO expandTarget (nid)
      SELECT nid
      FROM findNewTarget
      WHERE findNewTarget.nid <> target
        and findNewTarget.nid <> start
      ON CONFLICT DO NOTHING;
    ELSE
      WITH expandTargetReset AS (DELETE FROM expandTarget
      RETURNING nid),
           findNewTarget AS (INSERT INTO visited (nid, p2s)
        SELECT te.fid, te.tid -- tid, fid
        FROM te,
             expandTargetReset
        WHERE expandTargetReset.nid = te.tid -- fid
      ON CONFLICT do nothing
      RETURNING nid)
      INSERT INTO expandTarget (nid)
      SELECT nid
      FROM findNewTarget
      WHERE findNewTarget.nid <> target
        and findNewTarget.nid <> start
      ON CONFLICT DO NOTHING;
    END IF;

    GET DIAGNOSTICS affected = ROW_COUNT;

    IF affected < 1
    THEN
      EXIT;
    END IF;
  END LOOP;

  IF isForward
  THEN
    DROP TABLE IF EXISTS rb_f;
    CREATE UNLOGGED TABLE rb_f  AS
      SELECT DISTINCT nid FROM visited;
  ELSE
    DROP TABLE IF EXISTS rb_b;
    CREATE UNLOGGED TABLE rb_b AS
      SELECT DISTINCT nid FROM visited;
  end if;
  RETURN TRUE;
END;
$$
LANGUAGE plpgsql;

-- AFTER
DROP TABLE IF EXISTS rb;
CREATE UNLOGGED TABLE rb AS
  SELECT DISTINCT rb_f.nid
  FROM rb_f,
       rb_b
  WHERE rb_f.nid = rb_b.nid;