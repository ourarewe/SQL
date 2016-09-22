#\. D:\\Documents\\SQL\\tianchi\\mobile_recommendation\\mobile_recommendation03.sql

'''
#--过滤掉只看不买和不看就买的----18 min 30.64 sec-------------------------
#--统计个行为总数太花时间，但直接从表tianchi_fresh_comp_train_P_R统计特征更慢-------------------------
drop table if exists tianchi_fresh_comp_train_filtering;
create table tianchi_fresh_comp_train_filtering as 
select * from tianchi_fresh_comp_train_P_R t where not exists (
select * from (
select user_id,item_id from (
select *,co+ad+bu flag1,br+co+ad flag2 from (
select *,sum(browse) br ,sum(collect) co ,sum(addCart) ad,sum(buy) bu
from tianchi_fresh_comp_train_P_R group by user_id,item_id order by null
)a )b where flag1=0 or flag2=0 )c where user_id=t.user_id and item_id=t.item_id
);
alter table tianchi_fresh_comp_train_filtering add index idx_u_t (user_id,item_id);
#--时间做索引耗时太长，不现实
alter table tianchi_fresh_comp_train_filtering add index idx_time (time);
'''


#  尝试增加其他特征
#--很多ui的1、2、3、4各种行为都是当天发生的，5天内有交互行为且第六天有购买的只有100+条，所以可能还是要tuples



#--基础特征UI、U、I的1234四种行为统计----要用到02的1days、3days、5days表--------------------------------------------------
# train last_day: 12-17
# test last_day: 11-22  11-28  12-04  12-10
set @last_day:='2014-12-04';
\. D:\\Documents\\SQL\\tianchi\\mobile_recommendation\\mobile_recommendation_subtable.sql

#  train  test 
select a.user_id,a.item_id
, a.t1_1, a.t1_2, a.t1_3, a.t1_4
, a.t3_1, a.t3_2, a.t3_3, a.t3_4
, a.t5_1, a.t5_2, a.t5_3, a.t5_4
, a.t5_3/a.t5_1*10.0 ui1, a.t5_3/a.t5_2*10.0 ui2, a.t5_4/a.t5_1*10.0 ui3, a.t5_4/a.t5_2*10.0 ui4, a.t5_4/a.t5_3*10.0 ui5
, a.t5_1/a.u_1*10.0 u1, a.t5_2/a.u_2*10.0 u2, a.t5_3/a.u_3*10.0 u3, a.t5_4/a.u_4*10.0 u4
, a.t5_1/a.i_1*10.0 i1, a.t5_2/a.i_2*10.0 i2, a.t5_3/a.i_3*10.0 i3, a.t5_4/a.i_4*10.0 i4
, if(b.label>0,1,0) label
from tianchi_fresh_comp_feature_Basic a 
left join tianchi_fresh_comp_label b on a.user_id=b.user_id and a.item_id=b.item_id
INTO OUTFILE 'D:\\Documents\\SQL\\tianchi\\mobile_recommendation\\train.csv'   
FIELDS TERMINATED BY ','
ESCAPED BY ''
LINES TERMINATED BY '\r\n';



#  predict
set @last_day:='2014-12-18';
\. D:\\Documents\\SQL\\tianchi\\mobile_recommendation\\mobile_recommendation_subtable.sql
select a.user_id,a.item_id
, a.t1_1, a.t1_2, a.t1_3, a.t1_4
, a.t3_1, a.t3_2, a.t3_3, a.t3_4
, a.t5_1, a.t5_2, a.t5_3, a.t5_4
, a.t5_3/a.t5_1*10.0 ui1, a.t5_3/a.t5_2*10.0 ui2, a.t5_4/a.t5_1*10.0 ui3, a.t5_4/a.t5_2*10.0 ui4, a.t5_4/a.t5_3*10.0 ui5
, a.t5_1/a.u_1*10.0 u1, a.t5_2/a.u_2*10.0 u2, a.t5_3/a.u_3*10.0 u3, a.t5_4/a.u_4*10.0 u4
, a.t5_1/a.i_1*10.0 i1, a.t5_2/a.i_2*10.0 i2, a.t5_3/a.i_3*10.0 i3, a.t5_4/a.i_4*10.0 i4
from tianchi_fresh_comp_feature_Basic a 
left join tianchi_fresh_comp_label b on a.user_id=b.user_id and a.item_id=b.item_id
INTO OUTFILE 'D:\\Documents\\SQL\\tianchi\\mobile_recommendation\\predict.csv'   
FIELDS TERMINATED BY ','
ESCAPED BY ''
LINES TERMINATED BY '\r\n';

