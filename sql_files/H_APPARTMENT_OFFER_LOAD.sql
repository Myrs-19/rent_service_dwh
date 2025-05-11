-- комментарий

create temp table wrk_1 as 
select 
    '{{ run_id }}' as dataflow_id,
    now()::timestamp as dataflow_dttm,
    '001' as source_system_dk,
    md5(id_offer::varchar || '#' || address::varchar) as apartment_offer_rk,
    id_offer::varchar || '#' || address::varchar as hub_key
from ods.c_rostov
where dataflow_dttm > '{{ ti.xcom_pull(task_ids="get_cut_param")}}'::timestamp
group by id_offer::varchar || '#' || address::varchar
;

-- выбираем новые записи 
create temp table wrk_2 as 
select apartment_offer_rk
from wrk_1
except
select apartment_offer_rk 
from rv.h_apartment_offers
;

-- очищаем от дублей и null
create temp table wrk_3 as 
with t as (
    select wrk_1.*, row_number() over(partition by wrk_1.apartment_offer_rk) as rn
    from wrk_1
    join wrk_2
        on wrk_1.apartment_offer_rk = wrk_2.apartment_offer_rk
)
select 
    dataflow_id,
    dataflow_dttm,
    source_system_dk,
    apartment_offer_rk,
    hub_key
from t
where t.rn = 1;

insert into rv.h_apartment_offers
select wrk_3.* from wrk_3
;