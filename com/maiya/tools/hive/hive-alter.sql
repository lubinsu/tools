alter table myd.my_credit_hulu_application_check drop partition (adddate='20171009');
alter table myd.my_credit_hulu_application_check add partition (adddate='20171009') location '20171009';
alter table myd.my_credit_hulu_behavior_check drop partition (adddate='20171009');
alter table myd.my_credit_hulu_behavior_check add partition (adddate='20171009') location '20171009';
alter table myd.my_credit_hulu_cell_behavior drop partition (adddate='20171009');
alter table myd.my_credit_hulu_cell_behavior add partition (adddate='20171009') location '20171009';
alter table myd.my_credit_hulu_collection_contact drop partition (adddate='20171009');
alter table myd.my_credit_hulu_collection_contact add partition (adddate='20171009') location '20171009';
alter table myd.my_credit_hulu_common drop partition (adddate='20171009');
alter table myd.my_credit_hulu_common add partition (adddate='20171009') location '20171009';
alter table myd.my_credit_hulu_contact_list drop partition (adddate='20171009');
alter table myd.my_credit_hulu_contact_list add partition (adddate='20171009') location '20171009';
alter table myd.my_credit_hulu_contact_region drop partition (adddate='20171009');
alter table myd.my_credit_hulu_contact_region add partition (adddate='20171009') location '20171009';
alter table myd.my_credit_hulu_data_source drop partition (adddate='20171009');
alter table myd.my_credit_hulu_data_source add partition (adddate='20171009') location '20171009';
alter table myd.my_credit_hulu_deliver_address drop partition (adddate='20171009');
alter table myd.my_credit_hulu_deliver_address add partition (adddate='20171009') location '20171009';
alter table myd.my_credit_hulu_main_service drop partition (adddate='20171009');
alter table myd.my_credit_hulu_main_service add partition (adddate='20171009') location '20171009';
alter table myd.my_credit_hulu_person drop partition (adddate='20171009');
alter table myd.my_credit_hulu_person add partition (adddate='20171009') location '20171009';
alter table myd.my_credit_hulu_trip_consume drop partition (adddate='20171009');
alter table myd.my_credit_hulu_trip_consume add partition (adddate='20171009') location '20171009';
alter table myd.my_credit_hulu_trip_info drop partition (adddate='20171009');
alter table myd.my_credit_hulu_trip_info add partition (adddate='20171009') location '20171009';
