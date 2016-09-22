#\. D:\\Documents\\SQL\\tianchi\\mobile_recommendation\\mobile_recommendation.sql

# 录入原始表 取交集 把交集变为横表

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
alter table tianchi_fresh_comp_train_user add index idx_user (user_id);


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
#in 语句太慢，索引无用
drop table if exists tianchi_fresh_comp_train_P;
create table tianchi_fresh_comp_train_P as
select * from tianchi_fresh_comp_train_user
where item_id in (select item_id from tianchi_fresh_comp_train_item);
'''

#--两种求交集方式---还有exists方法-where exists ()-----------------------------------------------
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

#-----------------------------------------------------------------------------------

#--尝试减少时间字符长度，但没卵用-----------------------------------------
drop table if exists temp;
create table temp as 
select user_id,item_id,behavior_type
,concat(substring(time,6,2),substring(time,9,2),substring(time,12,2)) as time_int
from tianchi_fresh_comp_train_P;
alter table temp modify column time_int int;
alter table temp add index idx_u_i_t (user_id,item_id,time_int);

#---------------------------------------------------------------------------------


#--原打算一次性建好横表----------------------------------------------------
drop table if exists tianchi_fresh_comp_train_P_R;
create table tianchi_fresh_comp_train_P_R as
select user_id,item_id,time_int
,sum(if(behavior_type=1,1,0)) browse
,sum(if(behavior_type=2,1,0)) collect
,sum(if(behavior_type=3,1,0)) addCart
,sum(if(behavior_type=4,1,0)) buy
from temp group by user_id,item_id,time_int;

#--尝试改为分天建表再合并------------------------------------------------------
drop table if exists tianchi_fresh_comp_train_P_R;
create table tianchi_fresh_comp_train_P_R
(user_id bigint
,item_id bigint
,time varchar(50)
,browse int
,collect int
,addCart int
,buy int
)ENGINE=MyISAM DEFAULT CHARSET=utf8;

#--1 min 4.27 sec-----time字段应该改为substring(time)----------------------------------------------
# 要手动一天天insert into，好麻烦，中间出错就完蛋了！！！！下面有循环测试
#--18 19 20 21 22 23 24 25 26 27 28 29 30 01 02 03 04 05 06 07 08 09 10 11 13 14 15 16 17 18---------------------
insert into tianchi_fresh_comp_train_P_R
select * from (
select user_id,item_id,time
,sum(if(behavior_type=1,1,0)) browse
,sum(if(behavior_type=2,1,0)) collect
,sum(if(behavior_type=3,1,0)) addCart
,sum(if(behavior_type=4,1,0)) buy
from tianchi_fresh_comp_train_P
where time like '2014-12-18 %' 
group by user_id,item_id)t;

#--12号特殊对待----------------------------------------------------------------
drop table if exists temp;
create table temp as
select * from tianchi_fresh_comp_train_P where time like '2014-12-12 %';
alter table temp add index idx_u_t (user_id,item_id);
# 3 min 39.37 sec
insert into tianchi_fresh_comp_train_P_R
select * from (
select user_id,item_id,time
,sum(if(behavior_type=1,1,0)) browse
,sum(if(behavior_type=2,1,0)) collect
,sum(if(behavior_type=3,1,0)) addCart
,sum(if(behavior_type=4,1,0)) buy
from temp group by user_id,item_id)t;
drop table temp;

# 加索引
alter table tianchi_fresh_comp_train_P_R add index idx_time (time);
alter table tianchi_fresh_comp_train_P_R add index idx_u_t (user_id,item_id);
alter table tianchi_fresh_comp_train_P_R add index idx_t (item_id);


# 查看
select * from tianchi_fresh_comp_train_P_R where time like '2014-12-12 %' limit 10;


#--试用循环while插入----------------------
drop table if exists test;
create table test
(user_id bigint
,item_id bigint
,time varchar(50)
,browse int
,collect int
,addCart int
,buy int
)ENGINE=MyISAM DEFAULT CHARSET=utf8;

DELIMITER // 
drop procedure if exists pro;
create procedure pro()
begin
declare i int(2) zerofill;   # zerofill ---很关键-------------------------
set i=9;
while i<=10 do
insert into test
select * from (
select user_id,item_id,time
,sum(if(behavior_type=1,1,0)) browse
,sum(if(behavior_type=2,1,0)) collect
,sum(if(behavior_type=3,1,0)) addCart
,sum(if(behavior_type=4,1,0)) buy
from tianchi_fresh_comp_train_P
where time like concat('2014-12-',i,' %') 
group by user_id,item_id)t;
set i=i+1;
select i;
end while;
end;//
DELIMITER ;
# 1 min 48.00 sec
call pro(); 
drop procedure if exists pro;
select * from test where time like '2014-12-10 %' limit 10;
drop table test;


