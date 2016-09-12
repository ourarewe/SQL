#---  '061916'<=@t<='062223'  ------------

set @t:='061919';
DROP TABLE IF EXISTS rrd_period;
CREATE TABLE rrd_period as
select * from rrd_data_20160622084846 
where substring(actiontime,5,6)=@t
group by host_id,object,subname;

create index ix_subname on rrd_period (subname);


drop table if exists rrd_value_tholdup;
create table rrd_value_tholdup as 
select a.host_id,a.object,a.subname,a.value,ifnull(b.tholdup,100) as tholdup,ifnull(b.warninglevel,4) as warninglevel from(
select * from rrd_period 
where subname='cpu_usage' 
or subname='iobusy'
or subname='mem_usage'
or subname='partition_usage'
or subname='swap_usage')a
left join task_ph b on a.host_id=b.host_id and a.object=b.object and a.subname=b.subname;

delete from rrd_value_tholdup where value is null or value<0 or value>100;

drop table if exists rrd_value_tholdup_grade;
create table rrd_value_tholdup_grade as 
select *
,if(value>tholdup,60-10*(4-warninglevel)-20*(value-tholdup)/(100-tholdup),60+40*(tholdup-value)/tholdup) as score 
from rrd_value_tholdup;


drop table if exists rrd_grade_t4;
create table rrd_grade_t4 as 
select host_id,avg(score) as score from rrd_value_tholdup_grade group by host_id order by score;



drop table if exists rrd_grade_predict;
create table rrd_grade_predict as 
select a.host_id,b.score as t1,c.score as t2,d.score as t3,e.score as t4
from (select * from rrd_tuples group by host_id)a
left join rrd_grade_t1 b on a.host_id=b.host_id
left join rrd_grade_t2 c on a.host_id=c.host_id
left join rrd_grade_t3 d on a.host_id=d.host_id
left join rrd_grade_t4 e on a.host_id=e.host_id;

delete from rrd_grade_predict where t1 is null or t2 is null or t3 is null;

alter table rrd_grade_predict add predict double;
alter table rrd_grade_predict add diff double;

set @s:=2.0/(3.0+1.0);
UPDATE rrd_grade_predict SET predict:=(@s*@s*t1+@s*t2+t3)/(@s*@s+@s+1.0),diff:=t4-predict;

select 'host_id','06-19 16','06-19 17','06-19 18','06-19 19','predict','difference'
union all
select * from rrd_grade_predict
INTO OUTFILE 'rrd_grade_predict.csv'
FIELDS TERMINATED BY ','
ESCAPED BY ''
LINES TERMINATED BY '\r\n';

