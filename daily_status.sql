DO
$$
DECLARE
    v_commit_size  INT8 :=300;
    v_month        INT2 := 2;
    v_max_loop        INT2 := 0;
    v_limit_sec       INT2 := 10800;

    v_end_date     TIMESTAMP;
    v_duration         INTERVAL:=0;
    v_count        INT4:=0;
    v_row_count    INT4:=0;
    v_total_count  INT4:=0;
    v_loop_start_date   TIMESTAMP;
    v_record_count  INT4:=0;
    v_start_date    TIMESTAMP;
    v_recs INT[]:='{}';
    v_loop             INT2:=0;
    v_timeout bool;
    v_date            DATE :=  current_date - make_interval(months := v_month);
    v_lower int := @lb@;
    v_upper int := @up@;

begin
         RAISE INFO '% : Table placed before % are archived.',  to_char(clock_timestamp(), 'YYYY-MM-DD HH24:MI:SS'),v_date;
     RAISE INFO '% : The last digit of the order id is between % and %',  to_char(clock_timestamp(), 'YYYY-MM-DD HH24:MI:SS'),v_lower,v_upper;
     IF v_limit_sec<>0 THEN
        RAISE INFO '% : Time Limit : %',to_char(clock_timestamp(), 'YYYY-MM-DD HH24:MI:SS'),justify_interval(make_interval(secs := v_limit_sec));
END IF;
        v_loop_start_date:=clock_timestamp();
    LOOP
v_loop:=v_loop+1;
        v_start_date:=clock_timestamp();
                        RAISE INFO '';
                        RAISE INFO '% : [%/%]', to_char(clock_timestamp(), 'YYYY-MM-DD HH24:MI:SS'),v_loop,v_max_loop;

--      RAISE INFO 'v_lower %,v_upper %, %',v_lower,v_upper,current_date;
  --    PERFORM  pg_sleep(10);
--      exit;
WITH deleted AS (
DELETE FROM sch_cs_fulfillment_service.store_daily_status s
WHERE ctid IN (
    SELECT ctid
    FROM sch_cs_fulfillment_service.store_daily_status
    WHERE date < v_date
  AND  id % 10 BETWEEN v_lower AND v_upper
    LIMIT v_commit_size
    )
    RETURNING id,created_at,created_by,modified_at,modified_by,"version",is_deleted,status,"date",store_id
    )
INSERT INTO archive.store_daily_status (id,created_at,created_by,modified_at,modified_by,"version",is_deleted,status,"date",store_id)
SELECT id,created_at,created_by,modified_at,modified_by,"version",is_deleted,status,"date",store_id FROM deleted;

GET DIAGNOSTICS v_row_count = ROW_COUNT;
COMMIT;
--rollback;

v_end_date:=clock_timestamp();
                        v_duration:=v_duration+v_end_date-v_start_date;
                        v_total_count:=v_total_count+v_row_count;

                RAISE INFO '% : % record(s) archived.  Duration : %', to_char(clock_timestamp(), 'YYYY-MM-DD HH24:MI:SS'), v_row_count,v_end_date-v_start_date;

SELECT COUNT(*)
INTO v_count
FROM sch_cs_fulfillment_service.store_daily_status
WHERE date <  v_date
  AND id % 10 BETWEEN v_lower AND v_upper;

IF v_limit_sec<>0 AND EXTRACT(EPOCH FROM clock_timestamp() - v_loop_start_date) > v_limit_sec THEN
                                v_timeout:=true;
                                EXIT;
END IF;

        IF v_count = 0 THEN
            EXIT;
END IF;
       IF v_count = 0 OR (v_max_loop=v_loop AND v_max_loop<>0) THEN
                                EXIT;
END IF;
END LOOP;
                        RAISE INFO '';
                        IF v_timeout THEN
                                RAISE INFO '% : Completed. (The % second(s) time limit has been exceeded)', to_char(clock_timestamp(), 'YYYY-MM-DD HH24:MI:SS'),v_limit_sec;
ELSE
                                RAISE INFO '% : Completed', to_char(clock_timestamp(), 'YYYY-MM-DD HH24:MI:SS');
END IF;
                        RAISE INFO '% : Total', to_char(clock_timestamp(), 'YYYY-MM-DD HH24:MI:SS');
                        RAISE INFO '% : % record(s) archived.  Duration : %', to_char(clock_timestamp(), 'YYYY-MM-DD HH24:MI:SS'), v_total_count,v_duration;

END;
$$;