-- комментарий

-- добавление подрезки
delete from dwh_meta.cut_param where job_nm = 'S_APPARTMENT_OFFER_001_ROSTOV_LOAD';
insert into dwh_meta.cut_param
select 
	'S_APPARTMENT_OFFER_001_ROSTOV_LOAD' as job_nm,
	'rv.h_apartment_offers' as table_nm,
	'dataflow_dttm' as cut_field_nm,
	(now() - interval '100 days')::varchar as cut_field_value
;

-- создание таблицы сателита
drop table if exists rv.s_appartment_offer_001;
create table rv.s_appartment_offer_001 (
	dataflow_id varchar,
	dataflow_dttm timestamp,
	source_system_dk varchar,
	apartment_offer_rk varchar,
	valid_from timestamp,
	hashdiff_key varchar,
	actual_flg smallint,
	delete_flg smallint,
	id_offer int,
	amount_rooms varchar,
	offer_type varchar,
	address varchar,
	square varchar,
	house_address varchar,
	parking_space varchar,
	price varchar,
	phones varchar,
	description text,
	repair varchar,
	square_rooms varchar,
	balcony varchar,
	windows_oriention varchar,
	bathroom varchar,
	is_possible_with_kids_animals varchar,
	additional_description text,
	residential_complex_title varchar,
	ceiling_height varchar,
	lift varchar,
	garbage_chute varchar,
	link_to_offer varchar
);