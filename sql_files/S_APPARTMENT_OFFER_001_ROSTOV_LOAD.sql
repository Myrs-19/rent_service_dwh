-- комментарий

/* как расчитывается сателит 
    1) сравнение на измененные записи - join rk ников и сравнение хешей
    2) добавление новых записе
    3) поиск удаленных записей
    4) обновление флагов актуальностей - запись актуальна, если среди таких же rk имеет макимальный dataflow_dttm
*/

-- подрезка
create temp table wrk_1 as 
select 
    '{{ run_id }}' as dataflow_id,
    now()::timestamp as dataflow_dttm,
    '001' as source_system_dk,
    md5(id_offer::varchar || '#' || address::varchar) as apartment_offer_rk,
    now()::timestamp valid_from,
    md5(coalesce(id_offer::varchar, '')::varchar || '#' ||  coalesce(amount_rooms::varchar, '')::varchar || '#' ||  coalesce(offer_type::varchar, '')::varchar || '#' ||  coalesce(address::varchar, '')::varchar || '#' ||  coalesce(square::varchar, '')::varchar || '#' ||  coalesce(house_address::varchar, '')::varchar || '#' ||  coalesce(parking_space::varchar, '')::varchar || '#' ||  coalesce(price::varchar, '')::varchar || '#' ||  coalesce(phones::varchar, '')::varchar || '#' ||  coalesce(description::varchar, '')::varchar || '#' ||  coalesce(repair::varchar, '')::varchar || '#' ||  coalesce(square_rooms::varchar, '')::varchar || '#' ||  coalesce(balcony::varchar, '')::varchar || '#' ||  coalesce(windows_oriention::varchar, '')::varchar || '#' ||  coalesce(bathroom::varchar, '')::varchar || '#' ||  coalesce(is_possible_with_kids_animals::varchar, '')::varchar || '#' ||  coalesce(additional_description::varchar, '')::varchar || '#' ||  coalesce(residential_complex_title::varchar, '')::varchar || '#' ||  coalesce(ceiling_height::varchar, '')::varchar || '#' ||  coalesce(lift::varchar, '')::varchar || '#' ||  coalesce(garbage_chute::varchar, '')::varchar || '#' ||  coalesce(link_to_offer::varchar, '')::varchar) hashdiff_key,
	id_offer,
	amount_rooms,
	offer_type,
	address,
	square,
	house_address,
	parking_space,
	price,
	phones,
	description,
	repair,
	square_rooms,
	balcony,
	windows_oriention,
	bathroom,
	is_possible_with_kids_animals,
	additional_description,
	residential_complex_title,
	ceiling_height,
	lift,
	garbage_chute,
	link_to_offer
from ods.c_rostov
where dataflow_dttm > '{{ ti.xcom_pull(task_ids="get_cut_param")}}'::timestamp
;

-- расчет измененных записей
create temp table changed_rows as 
select 
    t1.dataflow_id,
    t1.dataflow_dttm,
    t1.source_system_dk,
    t1.apartment_offer_rk,
    t1.valid_from,
    t1.hashdiff_key,
    t1.id_offer,
	t1.amount_rooms,
	t1.offer_type,
	t1.address,
	t1.square,
	t1.house_address,
	t1.parking_space,
	t1.price,
	t1.phones,
	t1.description,
	t1.repair,
	t1.square_rooms,
	t1.balcony,
	t1.windows_oriention,
	t1.bathroom,
	t1.is_possible_with_kids_animals,
	t1.additional_description,
	t1.residential_complex_title,
	t1.ceiling_height,
	t1.lift,
	t1.garbage_chute,
	t1.link_to_offer,
    0 delete_flg
from rv.s_appartment_offer_001 as t
join wrk_1 as t1
    on t.apartment_offer_rk = t1.apartment_offer_rk
        and t.hashdiff_key <> t1.hashdiff_key
        and t.actual_flg = 1
        and t.delete_flg = 0  
;

-- расчет новых записей
create temp table new_rows as 
select t1.*, 0 delete_flg
from rv.s_appartment_offer_001 as t
right join wrk_1 as t1
    on t1.apartment_offer_rk = t.apartment_offer_rk and t.actual_flg = 1 and t.delete_flg = 0
where 1=1
    and t.apartment_offer_rk is null
;

-- расчет удаленных записей
create temp table delete_rows as 
select 
    t.dataflow_id,
    t.dataflow_dttm,
    t.source_system_dk,
    t.apartment_offer_rk,
    t.valid_from,
    t.hashdiff_key,
    t.actual_flg,
    t.id_offer,
	t.amount_rooms,
	t.offer_type,
	t.address,
	t.square,
	t.house_address,
	t.parking_space,
	t.price,
	t.phones,
	t.description,
	t.repair,
	t.square_rooms,
	t.balcony,
	t.windows_oriention,
	t.bathroom,
	t.is_possible_with_kids_animals,
	t.additional_description,
	t.residential_complex_title,
	t.ceiling_height,
	t.lift,
	t.garbage_chute,
	t.link_to_offer, 
	1 delete_flg
from rv.s_appartment_offer_001 as t
left join wrk_1 as t1
    on t1.apartment_offer_rk = t.apartment_offer_rk and t.actual_flg = 1 and t.delete_flg = 0
left join new_rows as t2
    on t1.apartment_offer_rk = t2.apartment_offer_rk
where 1=1
    and t1.apartment_offer_rk is null
    and t2.apartment_offer_rk is null
;

create temp table all_rows as 
select 
    dataflow_id,
    dataflow_dttm,
    source_system_dk,
    apartment_offer_rk,
    valid_from,
    hashdiff_key,
    1 actual_flg,
    delete_flg,
    id_offer,
	amount_rooms,
	offer_type,
	address,
	square,
	house_address,
	parking_space,
	price,
	phones,
	description,
	repair,
	square_rooms,
	balcony,
	windows_oriention,
	bathroom,
	is_possible_with_kids_animals,
	additional_description,
	residential_complex_title,
	ceiling_height,
	lift,
	garbage_chute,
	link_to_offer
from changed_rows
union all 
select 
    dataflow_id,
    dataflow_dttm,
    source_system_dk,
    apartment_offer_rk,
    valid_from,
    hashdiff_key,
    1 actual_flg,
    delete_flg,
    id_offer,
	amount_rooms,
	offer_type,
	address,
	square,
	house_address,
	parking_space,
	price,
	phones,
	description,
	repair,
	square_rooms,
	balcony,
	windows_oriention,
	bathroom,
	is_possible_with_kids_animals,
	additional_description,
	residential_complex_title,
	ceiling_height,
	lift,
	garbage_chute,
	link_to_offer
from delete_rows
union all
select 
    dataflow_id,
    dataflow_dttm,
    source_system_dk,
    apartment_offer_rk,
    valid_from,
    hashdiff_key,
    1 actual_flg,
    delete_flg,
    id_offer,
	amount_rooms,
	offer_type,
	address,
	square,
	house_address,
	parking_space,
	price,
	phones,
	description,
	repair,
	square_rooms,
	balcony,
	windows_oriention,
	bathroom,
	is_possible_with_kids_animals,
	additional_description,
	residential_complex_title,
	ceiling_height,
	lift,
	garbage_chute,
	link_to_offer
from new_rows
;

-- обновление флагов актуальности
create temp table update_flg as 
with 
-- обновление флагов по dataflow_dttm
update_dataflow_dttm as (
    select t.*, row_number() over(partition by apartment_offer_rk order by dataflow_dttm desc) as rn_dtfl
    from all_rows as t
)
-- обновление флагов по дублям
, update_doubles as (
    select t.*, row_number() over(partition by apartment_offer_rk) as rn_dbls
    from update_dataflow_dttm as t
    where rn_dtfl = 1
)
select 
    dataflow_id,
    dataflow_dttm,
    source_system_dk,
    apartment_offer_rk,
    valid_from,
    hashdiff_key,
    1 actual_flg,
    delete_flg,
    id_offer,
	amount_rooms,
	offer_type,
	address,
	square,
	house_address,
	parking_space,
	price,
	phones,
	description,
	repair,
	square_rooms,
	balcony,
	windows_oriention,
	bathroom,
	is_possible_with_kids_animals,
	additional_description,
	residential_complex_title,
	ceiling_height,
	lift,
	garbage_chute,
	link_to_offer
from update_doubles
where rn_dbls = 1;


-- загрузка
insert into rv.s_appartment_offer_001(
    dataflow_id,
    dataflow_dttm,
    source_system_dk,
    apartment_offer_rk,
    valid_from,
    hashdiff_key,
    actual_flg,
    delete_flg,
    id_offer,
	amount_rooms,
	offer_type,
	address,
	square,
	house_address,
	parking_space,
	price,
	phones,
	description,
	repair,
	square_rooms,
	balcony,
	windows_oriention,
	bathroom,
	is_possible_with_kids_animals,
	additional_description,
	residential_complex_title,
	ceiling_height,
	lift,
	garbage_chute,
	link_to_offer
)
select 
    dataflow_id,
    dataflow_dttm,
    source_system_dk,
    apartment_offer_rk,
    valid_from,
    hashdiff_key,
    actual_flg,
    delete_flg,
    id_offer,
	amount_rooms,
	offer_type,
	address,
	square,
	house_address,
	parking_space,
	price,
	phones,
	description,
	repair,
	square_rooms,
	balcony,
	windows_oriention,
	bathroom,
	is_possible_with_kids_animals,
	additional_description,
	residential_complex_title,
	ceiling_height,
	lift,
	garbage_chute,
	link_to_offer
from update_flg;