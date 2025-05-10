-- Комментарий

drop table if exists for_test.read_csv;
create table for_test.read_csv(
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

select * from for_test.read_csv;