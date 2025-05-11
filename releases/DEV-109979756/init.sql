-- комментарий

----------------------------------------------------
/* создание схем и суз */
----------------------------------------------------

postgres=# create database dwh;
CREATE DATABASE

postgres=# create user "srv.etl.dwh" with password 'dwh-Jbn$123#!17';
CREATE ROLE

postgres=# grant all privileges on database dwh to mseleznev;
GRANT

postgres=# \c dwh
Вы подключены к базе данных "dwh" как пользователь "postgres".

dwh=# create schema ods;
CREATE SCHEMA

dwh=# create schema rv;
CREATE SCHEMA

dwh=# create schema bv;
CREATE SCHEMA

dwh=# create schema em;
CREATE SCHEMA

dwh=# create schema rep;
CREATE SCHEMA

dwh=# grant all privileges on database dwh to "srv.etl.dwh";
GRANT

dwh=# grant all on schema ods to "srv.etl.dwh";
GRANT

dwh=# grant all on schema rv to "srv.etl.dwh";
GRANT

dwh=# grant all on schema bv to "srv.etl.dwh";
GRANT

dwh=# grant all on schema em to "srv.etl.dwh";
GRANT

dwh=# grant all on schema rep to "srv.etl.dwh";
GRANT

----------------------------------------------------
/* Создание таблицы ODS для циан */
----------------------------------------------------
drop table if exists ods.c_rostov;
create table ods.c_rostov(
	dataflow_id varchar,
	dataflow_dttm timestamp,
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

----------------------------------------------------
/* Создание таблицы хаба для квартир */
----------------------------------------------------

drop table if exists rv.h_apartment_offers;
create table rv.h_apartment_offers(
	dataflow_id varchar,
	dataflow_dttm timestamp,
	source_system_dk varchar,
	apartment_offer_rk varchar,
	hub_key varchar
);