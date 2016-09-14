#\. D:\\Documents\\SQL\\tianchi\\mobile_recommendation\\mobile_recommendation.sql

# 选特征-》抽样

#--过滤掉只看不买和不看就买的----18 min 30.64 sec-------------------------
#--统计个行为总数太花时间，还是构建特征后再过滤吧-------------------------
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


#--构建特征-------------------------------------------------------------------

