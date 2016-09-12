
#---某各小时分数----6948 17994 16192 15674 15201 11331----
#-------------------15042 ewma预测分差正最大  8846 ewma预测分差负最大---
#-------------------方差最大：15042 12332 14894
set @h:=14894;
DROP TABLE IF EXISTS rrd_host_x;
create table rrd_host_x as
select *
,if(value>tholdup,60-10*(4-warninglevel)-20*(value-tholdup)/(100-tholdup),60+40*(tholdup-value)/tholdup) as score
from(
select substring(a.actiontime,5,6) as t,a.object,a.subname,a.value
,ifnull(b.tholdup,100) as tholdup,ifnull(b.warninglevel,4) as warninglevel
from (select * from rrd_data_20160622084846
where host_id=@h
and (subname='cpu_usage' or subname='iobusy' or subname='mem_usage' or subname='partition_usage' or subname='swap_usage')
group by substring(actiontime,5,6),object,subname) a
left join task_ph b on b.host_id=@h and a.object=b.object and a.subname=b.subname)t;

set @tmp_sql:=concat("
select 't','object','subname','value','tholdup','warninglevel','score'
union all
select * from rrd_host_x
INTO OUTFILE 'rrd_host_",@h,".csv'
FIELDS TERMINATED BY ','
ESCAPED BY ''
LINES TERMINATED BY '\r\n';");
PREPARE s1 FROM @tmp_sql;
EXECUTE s1;
DROP PREPARE s1;

#----检查----------------------------------------------
select * from rrd_data_20160622084846 where host_id=@h and substring(actiontime,5,8)='06191628' 


