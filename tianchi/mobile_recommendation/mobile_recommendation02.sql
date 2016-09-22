#\. D:\\Documents\\SQL\\tianchi\\mobile_recommendation\\mobile_recommendation02.sql

# 选特征-》抽样


#--构建特征-------------------------------------------------------------------

#--训练集------------------------------------------------------------------
set @last_day:='2014-12-17';
set @3_day:=@last_day-interval 2 day;
set @5_day:=@last_day-interval 4 day;
set @label_day:=@last_day+interval 1 day;

drop table if exists tianchi_fresh_comp_1days;
create table tianchi_fresh_comp_1days as
select user_id,item_id
,sum(browse) browse ,sum(collect) collect ,sum(addCart) addCart,sum(buy) buy
from tianchi_fresh_comp_train_filtering
where substring(time,1,10)=@last_day group by user_id,item_id; 
alter table tianchi_fresh_comp_1days add index idx_u_t (user_id,item_id);

drop table if exists tianchi_fresh_comp_3days;
create table tianchi_fresh_comp_3days as
select user_id,item_id
,sum(browse) browse ,sum(collect) collect ,sum(addCart) addCart,sum(buy) buy
from tianchi_fresh_comp_train_filtering
where @3_day<=substring(time,1,10) and substring(time,1,10)<=@last_day group by user_id,item_id;
alter table tianchi_fresh_comp_3days add index idx_u_t (user_id,item_id);

drop table if exists tianchi_fresh_comp_5days;
create table tianchi_fresh_comp_5days as
select user_id,item_id
,sum(browse) browse ,sum(collect) collect ,sum(addCart) addCart,sum(buy) buy
from tianchi_fresh_comp_train_filtering
where @5_day<=substring(time,1,10) and substring(time,1,10)<=@last_day group by user_id,item_id;
alter table tianchi_fresh_comp_5days add index idx_u_t (user_id,item_id);

drop table if exists tianchi_fresh_comp_label;
create table tianchi_fresh_comp_label as
select user_id,item_id,sum(buy) label from tianchi_fresh_comp_train_filtering 
where substring(time,1,10)=@label_day group by user_id,item_id;
alter table tianchi_fresh_comp_label add index idx_u_t (user_id,item_id);

drop table if exists tianchi_fresh_comp_train_X;
create table tianchi_fresh_comp_train_X as
select a.user_id,a.item_id 
,c.browse br_1,c.collect co_1,c.addCart ad_1,c.buy bu_1
,b.browse br_3,b.collect co_3,b.addCart ad_3,b.buy bu_3
,a.browse br_5,a.collect co_5,a.addCart ad_5,a.buy bu_5
,if(d.label>0,1,0) label
from tianchi_fresh_comp_5days a
left join tianchi_fresh_comp_3days b on a.user_id=b.user_id and a.item_id=b.item_id
left join tianchi_fresh_comp_1days c on a.user_id=c.user_id and a.item_id=c.item_id
left join tianchi_fresh_comp_label d on a.user_id=d.user_id and a.item_id=d.item_id;

alter table tianchi_fresh_comp_train_X add index idx_u (user_id);
alter table tianchi_fresh_comp_train_X add index idx_i (item_id);

select 'user_id','item_id'
,'br_1','co_1','ad_1','bu_1','br_3','co_3','ad_3','bu_3'
,'br_5','co_5','ad_5','bu_5','label'
union all
select * from tianchi_fresh_comp_train_X
INTO OUTFILE 'D:\\Documents\\SQL\\tianchi\\mobile_recommendation\\train.csv'
FIELDS TERMINATED BY ','
ESCAPED BY ''
LINES TERMINATED BY '\r\n';


#--预测集-----------------------------------------------------------------
set @last_day:='2014-12-18';
set @3_day:=@last_day-interval 2 day;
set @5_day:=@last_day-interval 4 day;

drop table if exists tianchi_fresh_comp_1days;
create table tianchi_fresh_comp_1days as
select user_id,item_id
,sum(browse) browse ,sum(collect) collect ,sum(addCart) addCart,sum(buy) buy
from tianchi_fresh_comp_train_filtering
where substring(time,1,10)=@last_day group by user_id,item_id; 
alter table tianchi_fresh_comp_1days add index idx_u_t (user_id,item_id);

drop table if exists tianchi_fresh_comp_3days;
create table tianchi_fresh_comp_3days as
select user_id,item_id
,sum(browse) browse ,sum(collect) collect ,sum(addCart) addCart,sum(buy) buy
from tianchi_fresh_comp_train_filtering
where @3_day<=substring(time,1,10) and substring(time,1,10)<=@last_day group by user_id,item_id;
alter table tianchi_fresh_comp_3days add index idx_u_t (user_id,item_id);

drop table if exists tianchi_fresh_comp_5days;
create table tianchi_fresh_comp_5days as
select user_id,item_id
,sum(browse) browse ,sum(collect) collect ,sum(addCart) addCart,sum(buy) buy
from tianchi_fresh_comp_train_filtering
where @5_day<=substring(time,1,10) and substring(time,1,10)<=@last_day group by user_id,item_id;
alter table tianchi_fresh_comp_5days add index idx_u_t (user_id,item_id);

drop table if exists tianchi_fresh_comp_predict_X;
create table tianchi_fresh_comp_predict_X as
select a.user_id,a.item_id 
,c.browse br_1,c.collect co_1,c.addCart ad_1,c.buy bu_1
,b.browse br_3,b.collect co_3,b.addCart ad_3,b.buy bu_3
,a.browse br_5,a.collect co_5,a.addCart ad_5,a.buy bu_5
from tianchi_fresh_comp_5days a
left join tianchi_fresh_comp_3days b on a.user_id=b.user_id and a.item_id=b.item_id
left join tianchi_fresh_comp_1days c on a.user_id=c.user_id and a.item_id=c.item_id;

select 'user_id','item_id'
,'br_1','co_1','ad_1','bu_1','br_3','co_3','ad_3','bu_3'
,'br_5','co_5','ad_5','bu_5'
union all
select * from tianchi_fresh_comp_predict_X
INTO OUTFILE 'D:\\Documents\\SQL\\tianchi\\mobile_recommendation\\predict.csv'
FIELDS TERMINATED BY ','
ESCAPED BY ''
LINES TERMINATED BY '\r\n';


#--测试集------------------------------------------------------------------
#  11-18--11-23 11-24--11-29 11-30--12-05 12-06--12-11  
set @last_day:='2014-12-05';
set @3_day:=@last_day-interval 2 day;
set @5_day:=@last_day-interval 4 day;
set @label_day:=@last_day+interval 1 day;

drop table if exists tianchi_fresh_comp_1days;
create table tianchi_fresh_comp_1days as
select user_id,item_id
,sum(browse) browse ,sum(collect) collect ,sum(addCart) addCart,sum(buy) buy
from tianchi_fresh_comp_train_filtering
where substring(time,1,10)=@last_day group by user_id,item_id; 
alter table tianchi_fresh_comp_1days add index idx_u_t (user_id,item_id);

drop table if exists tianchi_fresh_comp_3days;
create table tianchi_fresh_comp_3days as
select user_id,item_id
,sum(browse) browse ,sum(collect) collect ,sum(addCart) addCart,sum(buy) buy
from tianchi_fresh_comp_train_filtering
where @3_day<=substring(time,1,10) and substring(time,1,10)<=@last_day group by user_id,item_id;
alter table tianchi_fresh_comp_3days add index idx_u_t (user_id,item_id);

drop table if exists tianchi_fresh_comp_5days;
create table tianchi_fresh_comp_5days as
select user_id,item_id
,sum(browse) browse ,sum(collect) collect ,sum(addCart) addCart,sum(buy) buy
from tianchi_fresh_comp_train_filtering
where @5_day<=substring(time,1,10) and substring(time,1,10)<=@last_day group by user_id,item_id;
alter table tianchi_fresh_comp_5days add index idx_u_t (user_id,item_id);

drop table if exists tianchi_fresh_comp_label;
create table tianchi_fresh_comp_label as
select user_id,item_id,sum(buy) label from tianchi_fresh_comp_train_filtering 
where substring(time,1,10)=@label_day group by user_id,item_id;
alter table tianchi_fresh_comp_label add index idx_u_t (user_id,item_id);

drop table if exists tianchi_fresh_comp_test_X;
create table tianchi_fresh_comp_test_X as
select a.user_id,a.item_id 
,c.browse br_1,c.collect co_1,c.addCart ad_1,c.buy bu_1
,b.browse br_3,b.collect co_3,b.addCart ad_3,b.buy bu_3
,a.browse br_5,a.collect co_5,a.addCart ad_5,a.buy bu_5
,if(d.label>0,1,0) label
from tianchi_fresh_comp_5days a
left join tianchi_fresh_comp_3days b on a.user_id=b.user_id and a.item_id=b.item_id
left join tianchi_fresh_comp_1days c on a.user_id=c.user_id and a.item_id=c.item_id
left join tianchi_fresh_comp_label d on a.user_id=d.user_id and a.item_id=d.item_id;

select 'user_id','item_id'
,'br_1','co_1','ad_1','bu_1','br_3','co_3','ad_3','bu_3'
,'br_5','co_5','ad_5','bu_5','label'
union all
select * from tianchi_fresh_comp_test_X
INTO OUTFILE 'D:\\Documents\\SQL\\tianchi\\mobile_recommendation\\test2.csv'
FIELDS TERMINATED BY ','
ESCAPED BY ''
LINES TERMINATED BY '\r\n';

