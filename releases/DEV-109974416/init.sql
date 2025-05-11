-- комментарий

----------------------------------------------------
/* Создание таблицы подрезок */
----------------------------------------------------
dwh=# create schema dwh_meta;
CREATE SCHEMA
dwh=# grant all on schema dwh_mea to "srv.etl.dwh";
ОШИБКА:  схема "dwh_mea" не существует
dwh=# grant all on schema dwh_meta to "srv.etl.dwh";
GRANT
dwh=# grant all on schema dwh_meta to mseleznev;

drop table if exists dwh_meta.cut_param;
create table dwh_meta.cut_param(
	job_nm varchar,
	table_nm varchar,
	cut_field_nm varchar,
	cut_field_value varchar
);

----------------------------------------------------
/* Добавление записи для хаба  */
----------------------------------------------------
-- Updated Rows	1
insert into dwh_meta.cut_param
select 
	'H_APPARTMENT_OFFER_LOAD' as job_nm,
	'ods.c_rostov' as table_nm,
	'dataflow_dttm' as cut_field_nm,
	(now() - interval '100 days')::varchar as cut_field_value
;