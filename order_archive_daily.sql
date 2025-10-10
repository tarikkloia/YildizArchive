DO
$$
        DECLARE
v_month           INT2 := 13; --13
                v_commit_size     INT4 := 20;
                v_max_loop        INT2 := 0;
                v_limit_sec       INT2 := 10800;
                v_sleep_ms        INT2 := 0;



                v_start_date    TIMESTAMP;
                v_loop_start_date   TIMESTAMP;
                v_end_date     TIMESTAMP;
                v_duration         INTERVAL:=0;
                v_count        INT4:=0;
                v_row_count    INT4:=0;
                v_total_count  INT4:=0;
                v_loop             INT2:=0;
                v_total_deleted_count  INT4:=0;
                v_order_deleted_count  INT4:=0;
                v_record_count  INT4:=0;
                v_date1 DATE;
                v_date2 DATE;
                v_date            DATE := current_date - make_interval(months := v_month);
                v_arr INT[];
                v_ords INT[]:='{}';
                v_timeout bool;
                v_lower int := @lb@;
                v_upper int := @up@;

BEGIN


                RAISE INFO '% : Orders placed before % are archived.',  to_char(clock_timestamp(), 'YYYY-MM-DD HH24:MI:SS'),v_date;
                RAISE INFO '% : The last digit of the order id is between % and %',  to_char(clock_timestamp(), 'YYYY-MM-DD HH24:MI:SS'),v_lower,v_upper;
                IF v_limit_sec<>0 THEN
                                RAISE INFO '% : Time Limit : %',to_char(clock_timestamp(), 'YYYY-MM-DD HH24:MI:SS'),justify_interval(make_interval(secs := v_limit_sec));
END IF;

                v_loop_start_date:=clock_timestamp();
                LOOP
v_loop:=v_loop+1;
                        v_start_date:=clock_timestamp();

                        v_ords:=ARRAY(SELECT o.id FROM sch_cs_fulfillment_service.orders o
                                                                WHERE o.delivery_date < v_date
                                                                AND  o.id % 10 BETWEEN v_lower AND v_upper
                                                        AND o.status IN('COMPLETED','CANCELLED','DELETED')
                                                                LIMIT v_commit_size);
                    --  RAISE INFO '% %',array_to_string(v_ords, ','),v_date;
        --PERFORM  pg_sleep(10);raise info 'bitti';
        --exit;
                        RAISE INFO '';
                        RAISE INFO '% : [%/%]', to_char(clock_timestamp(), 'YYYY-MM-DD HH24:MI:SS'),v_loop,v_max_loop;
                        RAISE INFO '% : Order(s) to be archived: [%]',  to_char(clock_timestamp(), 'YYYY-MM-DD HH24:MI:SS'),array_to_string(v_ords, ',');

WITH ords AS (
    SELECT * FROM unnest(v_ords) AS u(id)
),
     huit_i AS (
INSERT INTO archive.handling_unit_item_transaction (id,is_active,created_at,created_by,is_deleted,modified_at,modified_by,"version",status,handling_unit_transaction_id,order_item_id,ordered_quantity,collected_quantity)
SELECT id,is_active,created_at,created_by,is_deleted,modified_at,modified_by,"version",status,handling_unit_transaction_id,order_item_id,ordered_quantity,collected_quantity
FROM sch_cs_fulfillment_service.handling_unit_item_transaction a
WHERE EXISTS (
    SELECT 1
    FROM ords o, sch_cs_fulfillment_service.order_item oi
    WHERE o.id = oi.order_id AND oi.id = a.order_item_id
)
    RETURNING id
                                        ),
                                        coih_i AS (
INSERT INTO archive.cancelled_order_item_history (id,created_at,created_by,modified_at,modified_by,"version",is_deleted,is_active,order_id,product_code,product_name,erp_code,cancellation_reason,cancelled_date,"source",cancelled_quantity)
SELECT id,created_at,created_by,modified_at,modified_by,"version",is_deleted,is_active,order_id,product_code,product_name,erp_code,cancellation_reason,cancelled_date,"source",cancelled_quantity
FROM sch_cs_fulfillment_service.cancelled_order_item_history a
WHERE EXISTS (SELECT 1 FROM ords o WHERE o.id = a.order_id)
    RETURNING id
    ),
    ppo_i AS (
INSERT INTO archive.progress_payment_order (id,created_at,created_by,modified_at,modified_by,"version",is_deleted,progress_payment_id,order_id,base_amount,over_distance_amount,over_distance_quantity_in_meter,over_deci_amount,over_deci_quantity,special_product_amount,special_product_quantity,total_amount,over_weight_amount,over_weight_quantity,special_day_amount)
SELECT id,created_at,created_by,modified_at,modified_by,"version",is_deleted,progress_payment_id,order_id,base_amount,over_distance_amount,over_distance_quantity_in_meter,over_deci_amount,over_deci_quantity,special_product_amount,special_product_quantity,total_amount,over_weight_amount,over_weight_quantity,special_day_amount
FROM sch_cs_fulfillment_service.progress_payment_order a
WHERE EXISTS (SELECT 1 FROM ords o WHERE o.id = a.order_id)
    RETURNING id
    ),
    pa_i AS (
INSERT INTO archive.payment (id,created_at,created_by,modified_at,modified_by,"version",is_deleted,is_active,price,"type",order_id,refunded_price)
SELECT id,created_at,created_by,modified_at,modified_by,"version",is_deleted,is_active,price,"type",order_id,refunded_price
FROM sch_cs_fulfillment_service.payment a
WHERE EXISTS (SELECT 1 FROM ords o WHERE o.id = a.order_id)
    RETURNING id
    ),
    ca_i AS (
INSERT INTO archive.courier_assignment (id,created_at,created_by,modified_at,modified_by,"version",is_active,is_deleted,courier_task_id,courier_id,order_id,vehicle_id,route_task_id,delivery_sort_number,color_id,reason_id)
SELECT id,created_at,created_by,modified_at,modified_by,"version",is_active,is_deleted,courier_task_id,courier_id,order_id,vehicle_id,route_task_id,delivery_sort_number,color_id,reason_id
FROM sch_cs_fulfillment_service.courier_assignment a
WHERE EXISTS (SELECT 1 FROM ords o WHERE o.id = a.order_id)
    RETURNING id
    ),
    ht_i AS (
INSERT INTO archive.handling_unit_transaction (id,is_active,created_at,created_by,is_deleted,modified_at,modified_by,"version",status,handling_unit_id,order_id,location_id,parent_handling_unit_id)
SELECT id,is_active,created_at,created_by,is_deleted,modified_at,modified_by,"version",status,handling_unit_id,order_id,location_id,parent_handling_unit_id
FROM sch_cs_fulfillment_service.handling_unit_transaction a
WHERE EXISTS (SELECT 1 FROM ords o WHERE o.id = a.order_id)
    RETURNING id
    ),
    wto_i AS (
INSERT INTO archive.waiting_transfer_order (id,created_at,created_by,modified_at,modified_by,"version",is_deleted,is_active,order_id,reason)
SELECT id,created_at,created_by,modified_at,modified_by,"version",is_deleted,is_active,order_id,reason
FROM sch_cs_fulfillment_service.waiting_transfer_order a
WHERE EXISTS (SELECT 1 FROM ords o WHERE o.id = a.order_id)
    RETURNING id
    ),
    oah_i AS (
INSERT INTO archive.order_addition_history (id,created_at,created_by,modified_at,modified_by,"version",is_deleted,order_id,"date",status)
SELECT id,created_at,created_by,modified_at,modified_by,"version",is_deleted,order_id,"date",status
FROM sch_cs_fulfillment_service.order_addition_history a
WHERE EXISTS (SELECT 1 FROM ords o WHERE o.id = a.order_id)
    RETURNING id
    ),
    or_i AS (
INSERT INTO archive.order_review (id,created_at,created_by,is_deleted,modified_at,modified_by,"version",order_id,question_id,answer_id,answer_text,point)
SELECT id,created_at,created_by,is_deleted,modified_at,modified_by,"version",order_id,question_id,answer_id,answer_text,point
FROM sch_cs_fulfillment_service.order_review a
WHERE EXISTS (SELECT 1 FROM ords o WHERE o.id = a.order_id)
    RETURNING id
    ),
    op_i AS (
INSERT INTO archive.order_problem (id,is_active,created_at,created_by,is_deleted,modified_at,modified_by,"version",courier_note,latitude,longitude,courier_id,order_id,order_problem_type_id)
SELECT id,is_active,created_at,created_by,is_deleted,modified_at,modified_by,"version",courier_note,latitude,longitude,courier_id,order_id,order_problem_type_id
FROM sch_cs_fulfillment_service.order_problem a
WHERE EXISTS (SELECT 1 FROM ords o WHERE o.id = a.order_id)
    RETURNING id
    ),
    oa_i AS (
INSERT INTO archive.order_approval (id,is_active,created_at,created_by,is_deleted,modified_at,modified_by,"version",approval_time,order_approval_status,order_id,order_approval_reason_id)
SELECT id,is_active,created_at,created_by,is_deleted,modified_at,modified_by,"version",approval_time,order_approval_status,order_id,order_approval_reason_id
FROM sch_cs_fulfillment_service.order_approval a
WHERE EXISTS (SELECT 1 FROM ords o WHERE o.id = a.order_id)
    RETURNING id
    ),
    oh_i AS (
INSERT INTO archive.order_history (id,is_active,created_at,created_by,is_deleted,modified_at,modified_by,"version",status,order_id,process_latitude,process_longitude,distance_to_target_location,is_distant_process,is_manually_updated)
SELECT id,is_active,created_at,created_by,is_deleted,modified_at,modified_by,"version",status,order_id,process_latitude,process_longitude,distance_to_target_location,is_distant_process,is_manually_updated
FROM sch_cs_fulfillment_service.order_history a
WHERE EXISTS (SELECT 1 FROM ords o WHERE o.id = a.order_id)
    RETURNING id
    ),
    oi_i AS (
INSERT INTO archive.order_item (id,is_active,created_at,created_by,is_deleted,modified_at,modified_by,"version",barcodes,cancellation_reason,category,category_id,discount_price,erp_code,external_item_id,image_url,is_campaign,is_frozen,is_refundable,"name",product_code,price,quantity,quantity_type,status,order_id,return_on_delivery_reason,collected_quantity,campaign_name,product_price_normal,root_external_item_id,volumetric_weight,external_id,vat_rate,alternative_of_order_item_id,weight,delivery_condition,is_additional,change_reason_id,change_explanation,is_change_item_picked_up)
SELECT id,is_active,created_at,created_by,is_deleted,modified_at,modified_by,"version",barcodes,cancellation_reason,category,category_id,discount_price,erp_code,external_item_id,image_url,is_campaign,is_frozen,is_refundable,"name",product_code,price,quantity,quantity_type,status,order_id,return_on_delivery_reason,collected_quantity,campaign_name,product_price_normal,root_external_item_id,volumetric_weight,external_id,vat_rate,alternative_of_order_item_id,weight,delivery_condition,is_additional,change_reason_id,change_explanation,is_change_item_picked_up
FROM sch_cs_fulfillment_service.order_item a
WHERE EXISTS (SELECT 1 FROM ords o WHERE o.id = a.order_id)
    RETURNING id
    ),
    od_i AS (
INSERT INTO archive.orders (id,is_active,created_at,created_by,is_deleted,modified_at,modified_by,"version",collector_id,courier_id,delivery_date,delivery_fee,external_create_time,is_contains_frozen_product,is_fast_delivery,is_payment_success,latitude_on_delivery,longitude_on_delivery,manager_name,manager_phone,non_delivery_note,order_note,order_number,order_type,partner,payment_id,payment_name,payment_type,price,refunded_price,status,time_slot,address_id,non_delivery_reason_id,platform_id,slot_id,customer_order_number,delivery_code,is_payment_loyalty,loyalty_price,is_partner_card_payment,partner_card_price,charge_amount,is_delivery_from_store,cancellation_reason,estimated_distance,estimated_duration,source_id,routing_status,estimated_delivery_time,purchase_channel,tip_amount,external_order_id,root_order_number,channel,is_manually_updated,air_distance,alternative_product_preference,invoice_reference_no,collection_time,is_available_for_extra_payment,collection_type,final_total_price,promotion_type,planned_delivery_time,previous_collection_time,is_contains_hot_product,is_pending_addition,is_additional,courier_source,external_reference_id)
SELECT id,is_active,created_at,created_by,is_deleted,modified_at,modified_by,"version",collector_id,courier_id,delivery_date,delivery_fee,external_create_time,is_contains_frozen_product,is_fast_delivery,is_payment_success,latitude_on_delivery,longitude_on_delivery,manager_name,manager_phone,non_delivery_note,order_note,order_number,order_type,partner,payment_id,payment_name,payment_type,price,refunded_price,status,time_slot,address_id,non_delivery_reason_id,platform_id,slot_id,customer_order_number,delivery_code,is_payment_loyalty,loyalty_price,is_partner_card_payment,partner_card_price,charge_amount,is_delivery_from_store,cancellation_reason,estimated_distance,estimated_duration,source_id,routing_status,estimated_delivery_time,purchase_channel,tip_amount,external_order_id,root_order_number,channel,is_manually_updated,air_distance,alternative_product_preference,invoice_reference_no,collection_time,is_available_for_extra_payment,collection_type,final_total_price,promotion_type,planned_delivery_time,previous_collection_time,is_contains_hot_product,is_pending_addition,is_additional,courier_source,external_reference_id
FROM sch_cs_fulfillment_service.orders a
WHERE EXISTS (SELECT 1 FROM ords o WHERE o.id = a.id)
    RETURNING id
    ),
    huit_d AS (
DELETE FROM sch_cs_fulfillment_service.handling_unit_item_transaction a
WHERE EXISTS( SELECT b.id FROM huit_i b WHERE b.id=a.id)
    RETURNING id
    ),
    coih_d AS (
DELETE FROM sch_cs_fulfillment_service.cancelled_order_item_history a
WHERE EXISTS( SELECT b.id FROM coih_i b WHERE b.id=a.id)
    RETURNING id
    ),
    ppo_d AS (
DELETE FROM sch_cs_fulfillment_service.progress_payment_order a
WHERE EXISTS( SELECT b.id FROM ppo_i b WHERE b.id=a.id)
    RETURNING id
    ),
    pa_d AS (
DELETE FROM sch_cs_fulfillment_service.payment a
WHERE EXISTS( SELECT b.id FROM pa_i b WHERE b.id=a.id)
    RETURNING id
    ),
    ca_d AS (
DELETE FROM sch_cs_fulfillment_service.courier_assignment a
WHERE EXISTS( SELECT b.id FROM ca_i b WHERE b.id=a.id)
    RETURNING id
    ),
    ht_d AS (
DELETE FROM sch_cs_fulfillment_service.handling_unit_transaction a
WHERE EXISTS( SELECT b.id FROM ht_i b WHERE b.id=a.id)
    RETURNING id
    ),
    wto_d AS (
DELETE FROM sch_cs_fulfillment_service.waiting_transfer_order a
WHERE EXISTS( SELECT b.id FROM wto_i b WHERE b.id=a.id)
    RETURNING id
    ),
    oah_d AS (
DELETE FROM sch_cs_fulfillment_service.order_addition_history a
WHERE EXISTS( SELECT b.id FROM oah_i b WHERE b.id=a.id)
    RETURNING id
    ),
    or_d AS (
DELETE FROM sch_cs_fulfillment_service.order_review a
WHERE EXISTS( SELECT b.id FROM or_i b WHERE b.id=a.id)
    RETURNING id
    ),
    op_d AS (
DELETE FROM sch_cs_fulfillment_service.order_problem a
WHERE EXISTS( SELECT b.id FROM op_i b WHERE b.id=a.id)
    RETURNING id
    ),
    oa_d AS (
DELETE FROM sch_cs_fulfillment_service.order_approval a
WHERE EXISTS( SELECT b.id FROM oa_i b WHERE b.id=a.id)
    RETURNING id
    ),
    oh_d AS (
DELETE FROM sch_cs_fulfillment_service.order_history a
WHERE EXISTS( SELECT b.id FROM oh_i b WHERE b.id=a.id)
    RETURNING id
    ),
    oi_d AS (
DELETE FROM sch_cs_fulfillment_service.order_item a
WHERE EXISTS( SELECT b.id FROM oi_i b WHERE b.id=a.id)
    RETURNING id
    ),
    od_d AS (
DELETE FROM sch_cs_fulfillment_service.orders a
WHERE EXISTS( SELECT b.id FROM od_i b WHERE b.id=a.id)
    RETURNING id
    )
SELECT
    (SELECT COUNT(*) FROM huit_d) +
    (SELECT COUNT(*) FROM coih_d) +
    (SELECT COUNT(*) FROM ppo_d) +
    (SELECT COUNT(*) FROM pa_d) +
    (SELECT COUNT(*) FROM ca_d) +
    (SELECT COUNT(*) FROM ht_d) +
    (SELECT COUNT(*) FROM wto_d) +
    (SELECT COUNT(*) FROM oah_d) +
    (SELECT COUNT(*) FROM or_d) +
    (SELECT COUNT(*) FROM op_d) +
    (SELECT COUNT(*) FROM oa_d) +
    (SELECT COUNT(*) FROM oh_d) +
    (SELECT COUNT(*) FROM oi_d) +
    (SELECT COUNT(*) FROM od_d),(SELECT COUNT(*) FROM od_d), ARRAY(SELECT id FROM od_d)
INTO v_total_deleted_count,v_order_deleted_count,v_arr;


--GET DIAGNOSTICS v_row_count = ROW_COUNT;
COMMIT;
--ROLLBACK;
v_row_count:=v_order_deleted_count;
                        v_end_date:=clock_timestamp();
                        v_duration:=v_duration+v_end_date-v_start_date;
                        v_total_count:=v_total_count+v_row_count;
                        v_record_count:=v_record_count+v_total_deleted_count;
                        RAISE INFO '% : % order(s),% record(s) archived.  Duration : %', to_char(clock_timestamp(), 'YYYY-MM-DD HH24:MI:SS'), v_row_count,v_total_deleted_count,v_end_date-v_start_date;

SELECT COUNT(id)
INTO v_count
FROM sch_cs_fulfillment_service.orders o
WHERE o.delivery_date < v_date
  AND o.id % 10 BETWEEN v_lower AND v_upper
                        AND o.status IN('COMPLETED','CANCELLED','DELETED');

IF v_limit_sec<>0 AND EXTRACT(EPOCH FROM clock_timestamp() - v_loop_start_date) > v_limit_sec THEN
                                v_timeout:=true;
                                EXIT;
END IF;

                        IF v_count = 0 OR (v_max_loop=v_loop AND v_max_loop<>0) THEN
                                EXIT;
END IF;
                        PERFORM  pg_sleep(v_sleep_ms/1000::DOUBLE PRECISION);
END LOOP;
                        RAISE INFO '';
                        IF v_timeout THEN
                                RAISE INFO '% : Completed. (The % second(s) time limit has been exceeded)', to_char(clock_timestamp(), 'YYYY-MM-DD HH24:MI:SS'),v_limit_sec;
ELSE
                                RAISE INFO '% : Completed', to_char(clock_timestamp(), 'YYYY-MM-DD HH24:MI:SS');
END IF;
                        RAISE INFO '% : Total', to_char(clock_timestamp(), 'YYYY-MM-DD HH24:MI:SS');
                        RAISE INFO '% : % order(s),% record(s) archived.  Duration : %', to_char(clock_timestamp(), 'YYYY-MM-DD HH24:MI:SS'), v_total_count,v_record_count,v_duration;

END;
        $$;