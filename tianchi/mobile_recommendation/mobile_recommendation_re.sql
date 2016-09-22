#\. D:\\Documents\\SQL\\tianchi\\mobile_recommendation\\mobile_recommendation_re.sql


drop table if exists tuples;
create table tuples as
select user_id,item_id from tianchi_fresh_comp_train_P
group by user_id,item_id;
alter table tuples add index idx_u_t (user_id,item_id);

drop table if exists t31;
create table t31 as
select * from tianchi_fresh_comp_train_P 
where substring(time,1,10) between '2014-12-15' and '2014-12-17' and behavior_type=1;
alter table t31 add index idx_u_t (user_id,item_id);

drop table if exists t32;
create table t32 as
select * from tianchi_fresh_comp_train_P 
where substring(time,1,10) between '2014-12-15' and '2014-12-17' and behavior_type=2;
alter table t32 add index idx_u_t (user_id,item_id);

drop table if exists t33;
create table t33 as
select * from tianchi_fresh_comp_train_P 
where substring(time,1,10) between '2014-12-15' and '2014-12-17' and behavior_type=3;
alter table t33 add index idx_u_t (user_id,item_id);

drop table if exists t34;
create table t34 as
select * from tianchi_fresh_comp_train_P 
where substring(time,1,10) between '2014-12-15' and '2014-12-17' and behavior_type=4;
alter table t34 add index idx_u_t (user_id,item_id);

drop table if exists t51;
create table t51 as
select * from tianchi_fresh_comp_train_P 
where substring(time,1,10) between '2014-12-13' and '2014-12-17' and behavior_type=1;
alter table t51 add index idx_u_t (user_id,item_id);

drop table if exists t52;
create table t52 as
select * from tianchi_fresh_comp_train_P 
where substring(time,1,10) between '2014-12-13' and '2014-12-17' and behavior_type=2;
alter table t52 add index idx_u_t (user_id,item_id);

drop table if exists t53;
create table t53 as
select * from tianchi_fresh_comp_train_P 
where substring(time,1,10) between '2014-12-13' and '2014-12-17' and behavior_type=3;
alter table t53 add index idx_u_t (user_id,item_id);

drop table if exists t54;
create table t54 as
select * from tianchi_fresh_comp_train_P 
where substring(time,1,10) between '2014-12-13' and '2014-12-17' and behavior_type=4;
alter table t54 add index idx_u_t (user_id,item_id);

drop table if exists t_label;
create table t_label as
select * from tianchi_fresh_comp_train_P 
where time like '2014-12-18 %' and behavior_type=4;
alter table t_label add index idx_u_t (user_id,item_id);


drop table if exists old_train_set;
create table old_train_set as
select aa.user_id,aa.item_id
,t11.cnt t11, t12.cnt t12, t13.cnt t13, t14.cnt t14
,t31.cnt t31, t32.cnt t32, t33.cnt t33, t34.cnt t34
,t51.cnt t51, t52.cnt t52, t53.cnt t53, t54.cnt t54
,tl.label
from tuples aa
left join (select user_id,item_id,count(1) cnt from tianchi_fresh_comp_train_P where time like '2014-12-17 %' and behavior_type=1 group by user_id,item_id)t11
on aa.user_id=t11.user_id and aa.item_id=t11.item_id
left join (select user_id,item_id,count(1) cnt from tianchi_fresh_comp_train_P where time like '2014-12-17 %' and behavior_type=2 group by user_id,item_id)t12
on aa.user_id=t12.user_id and aa.item_id=t12.item_id
left join (select user_id,item_id,count(1) cnt from tianchi_fresh_comp_train_P where time like '2014-12-17 %' and behavior_type=3 group by user_id,item_id)t13
on aa.user_id=t13.user_id and aa.item_id=t13.item_id
left join (select user_id,item_id,count(1) cnt from tianchi_fresh_comp_train_P where time like '2014-12-17 %' and behavior_type=4 group by user_id,item_id)t14
on aa.user_id=t14.user_id and aa.item_id=t14.item_id
left join (select user_id,item_id,count(1) cnt from t31 group by user_id,item_id)t31
on aa.user_id=t31.user_id and aa.item_id=t31.item_id
left join (select user_id,item_id,count(1) cnt from t32 group by user_id,item_id)t32
on aa.user_id=t32.user_id and aa.item_id=t32.item_id
left join (select user_id,item_id,count(1) cnt from t33 group by user_id,item_id)t33
on aa.user_id=t33.user_id and aa.item_id=t33.item_id
left join (select user_id,item_id,count(1) cnt from t34 group by user_id,item_id)t34
on aa.user_id=t34.user_id and aa.item_id=t34.item_id
left join (select user_id,item_id,count(1) cnt from t51 group by user_id,item_id)t51
on aa.user_id=t51.user_id and aa.item_id=t51.item_id
left join (select user_id,item_id,count(1) cnt from t52 group by user_id,item_id)t52
on aa.user_id=t52.user_id and aa.item_id=t52.item_id
left join (select user_id,item_id,count(1) cnt from t53 group by user_id,item_id)t53
on aa.user_id=t53.user_id and aa.item_id=t53.item_id
left join (select user_id,item_id,count(1) cnt from t54 group by user_id,item_id)t54
on aa.user_id=t54.user_id and aa.item_id=t54.item_id
left join (select user_id,item_id,1 label from t_label group by user_id,item_id)tl
on aa.user_id=tl.user_id and aa.item_id=tl.item_id;


drop table if exists old_train_set_backup;
create table old_train_set_backup as select * from old_train_set;

drop table if exists old_train_set;
create table old_train_set as select * from old_train_set_backup;

select count(1) from old_train_set;

delete from old_train_set where t11 is null and t12 is null and t13 is null and t14 is null and t31 is null and t32 is null and t33 is null and t34 is null and t51 is null and t52 is null and t53 is null and t54 is null;

#delete from old_train_set where t52 is null and t53 is null and t54 is null;

select 'user_id','item_id'
,'br_1','co_1','ad_1','bu_1','br_3','co_3','ad_3','bu_3'
,'br_5','co_5','ad_5','bu_5','label'
union all
select * from old_train_set
INTO OUTFILE 'D:\\Documents\\SQL\\tianchi\\mobile_recommendation\\train_old.csv'
FIELDS TERMINATED BY ','
ESCAPED BY ''
LINES TERMINATED BY '\r\n';




