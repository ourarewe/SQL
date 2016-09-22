#\. D:\\Documents\\SQL\\tianchi\\mobile_recommendation\\mobile_recommendation_subtable.sql

#--生成left join用的子表
set @3_day:=@last_day-interval 2 day;
set @5_day:=@last_day-interval 4 day;
set @label_day:=@last_day+interval 1 day;


#--用户商品对，之后还要用到02的1days、3days、5days表，使用filtering表的UI对----@5_day<=substring(time,1,10) and ---------------------
drop table if exists tuples;
create table tuples as
select user_id,item_id from tianchi_fresh_comp_train_filtering
where substring(time,1,10)<=@label_day group by user_id,item_id;
alter table tuples add index idx_u_t (user_id,item_id);

#--用户特征------------------------------------------------------
drop table if exists tianchi_fresh_comp_user_feature;
create table tianchi_fresh_comp_user_feature as
select user_id,sum(browse) browse,sum(collect) collect,sum(addCart) addCart,sum(buy) buy
from tianchi_fresh_comp_train_filtering 
where @5_day<=substring(time,1,10) and substring(time,1,10)<=@last_day group by user_id;
alter table tianchi_fresh_comp_user_feature add index idx_u (user_id);

#--商品特征-----------------------------------------------------------------
drop table if exists tianchi_fresh_comp_item_feature;
create table tianchi_fresh_comp_item_feature as
select item_id,sum(browse) browse,sum(collect) collect,sum(addCart) addCart,sum(buy) buy
from tianchi_fresh_comp_train_filtering
where @5_day<=substring(time,1,10) and substring(time,1,10)<=@last_day group by item_id;
alter table tianchi_fresh_comp_item_feature add index idx_i (item_id);


#----1 3 5 days---------------------------------------------------------------------
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


#--把以上表合并----------------------------------------------
drop table if exists tianchi_fresh_comp_feature_Basic;
create table tianchi_fresh_comp_feature_Basic as
select a.*
,t1.browse t1_1, t1.collect t1_2, t1.addCart t1_3, t1.buy t1_4
,t3.browse t3_1, t3.collect t3_2, t3.addCart t3_3, t3.buy t3_4
,t5.browse t5_1, t5.collect t5_2, t5.addCart t5_3, t5.buy t5_4
,tu.browse u_1, tu.collect u_2, tu.addCart u_3, tu.buy u_4
,ti.browse i_1, ti.collect i_2, ti.addCart i_3, ti.buy i_4
from tuples a
left join tianchi_fresh_comp_1days t1 on a.user_id=t1.user_id and a.item_id=t1.item_id
left join tianchi_fresh_comp_3days t3 on a.user_id=t3.user_id and a.item_id=t3.item_id
left join tianchi_fresh_comp_5days t5 on a.user_id=t5.user_id and a.item_id=t5.item_id
left join tianchi_fresh_comp_user_feature tu on a.user_id=tu.user_id
left join tianchi_fresh_comp_item_feature ti on a.item_id=ti.item_id;
