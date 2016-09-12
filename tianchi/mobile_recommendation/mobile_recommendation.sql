#\. D:\\Documents\\SQL\\tianchi\\mobile_recommendation\\mobile_recommendation.sql

drop table if exists tianchi_fresh_comp_train_user;
create table tianchi_fresh_comp_train_user
(user_id bigint
,item_id bigint
,behavior_type int
,user_geohash varchar(50)
,item_category bigint
,time varchar(50)
)ENGINE=MyISAM DEFAULT CHARSET=utf8;

LOAD DATA LOCAL INFILE 'D:\\Documents\\SQL\\tianchi\\mobile_recommendation\\tianchi_fresh_comp_train_user.csv'
INTO TABLE tianchi_fresh_comp_train_user
FIELDS TERMINATED BY ','
ESCAPED BY ''
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES;

alter table tianchi_fresh_comp_train_user add index idx_item (item_id);



drop table if exists tianchi_fresh_comp_train_item;
create table tianchi_fresh_comp_train_item
(item_id bigint
,item_geohash varchar(50)
,item_category bigint
)ENGINE=MyISAM DEFAULT CHARSET=utf8;

LOAD DATA LOCAL INFILE 'D:\\Documents\\SQL\\tianchi\\mobile_recommendation\\tianchi_fresh_comp_train_item.csv'
INTO TABLE tianchi_fresh_comp_train_item
FIELDS TERMINATED BY ','
ESCAPED BY ''
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES;

alter table tianchi_fresh_comp_train_item add index idx_item (item_id);


'''
#in Óï¾äÌ«Âý£¬Ë÷ÒýÎÞÓÃ
drop table if exists tianchi_fresh_comp_train_P;
create table tianchi_fresh_comp_train_P as
select * from tianchi_fresh_comp_train_user
where item_id in (select item_id from tianchi_fresh_comp_train_item);
'''

# 4 min 14.03 sec    15 min 10.76 sec
drop table if exists tianchi_fresh_comp_train_P;
create table tianchi_fresh_comp_train_P as
select a.user_id,a.item_id,a.behavior_type,a.time
from tianchi_fresh_comp_train_user a,tianchi_fresh_comp_train_item b 
where a.item_id=b.item_id;

# 9 min 46.10 sec
drop table if exists tianchi_fresh_comp_train_P;
create table tianchi_fresh_comp_train_P as
select a.user_id,a.item_id,a.behavior_type,a.time 
from tianchi_fresh_comp_train_user a join tianchi_fresh_comp_train_item b 
on a.item_id=b.item_id;


alter table tianchi_fresh_comp_train_P add index idx_u_i_t (user_id,item_id,time);
alter table tianchi_fresh_comp_train_P add index idx_time (time);


drop table if exists temp;
create table temp as 
select user_id,item_id,behavior_type
,concat(substring(time,6,2),substring(time,9,2),substring(time,12,2)) as time_int
from tianchi_fresh_comp_train_P;

alter table temp modify column time_int int;

alter table temp add index idx_u_i_t (user_id,item_id,time_int);



drop table if exists tianchi_fresh_comp_train_P_R;
create table tianchi_fresh_comp_train_P_R as
select user_id,item_id,time_int
,sum(if(behavior_type=1,1,0)) browse
,sum(if(behavior_type=2,1,0)) collect
,sum(if(behavior_type=3,1,0)) addCart
,sum(if(behavior_type=4,1,0)) buy
from temp group by user_id,item_id,time_int;
