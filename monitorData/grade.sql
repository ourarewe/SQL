

#--task_ph-23sec-----111898个--39055个subname是null---------------
DROP TABLE IF EXISTS task_ph;
CREATE TABLE task_ph as
select a.host_id
,ifnull(a.objects,'') as object
,a.collectindex_id
,b.name_en as subname
,a.id as task_id
,ifnull(c.tholddown,0) as tholddown
,ifnull(c.tholdup,100) as tholdup
,ifnull(c.warninglevel,4) as warninglevel
from task a left join (select * from collectindex where bigtemplate='os') b 
on a.collectindex_id=b.id
left join thold c on a.id=c.task_id;

create index ix_subname_warn on task_ph (subname,warninglevel);


#--rrd_tuples---从rrd_data_20160622084846里提取元组要2分半，30823个-其实就4000台服务器---
DROP TABLE IF EXISTS rrd_tuples;
CREATE TABLE rrd_tuples as
select host_id,object from rrd_data_20160622084846 group by host_id,object;

create index ix_host_object on rrd_tuples (host_id,object);


#--------rrd_tuples与task_ph联合----30868 rows affected (12 min 38.59 sec)----
DROP TABLE IF EXISTS rrd_tholdcollect;
CREATE TABLE rrd_tholdcollect as
select a.* 
,ifnull(c3.tholdup,100) as cu3 ,ifnull(c2.tholdup,100) as cu2 ,ifnull(c1.tholdup,100) as cu1 ,ifnull(c0.tholdup,100) as cu0
,ifnull(i3.tholdup,100) as iu3 ,ifnull(i2.tholdup,100) as iu2 ,ifnull(i1.tholdup,100) as iu1 ,ifnull(i0.tholdup,100) as iu0
,ifnull(m3.tholdup,100) as mu3 ,ifnull(m2.tholdup,100) as mu2 ,ifnull(m1.tholdup,100) as mu1 ,ifnull(m0.tholdup,100) as mu0
,ifnull(p3.tholdup,100) as pu3 ,ifnull(p2.tholdup,100) as pu2 ,ifnull(p1.tholdup,100) as pu1 ,ifnull(p0.tholdup,100) as pu0
,ifnull(s3.tholdup,100) as su3 ,ifnull(s2.tholdup,100) as su2 ,ifnull(s1.tholdup,100) as su1 ,ifnull(s0.tholdup,100) as su0
from rrd_tuples a
left join (select * from task_ph where subname='cpu_usage' and warninglevel=3)c3 on a.host_id=c3.host_id and a.object=c3.object
left join (select * from task_ph where subname='cpu_usage' and warninglevel=2)c2 on a.host_id=c2.host_id and a.object=c2.object
left join (select * from task_ph where subname='cpu_usage' and warninglevel=1)c1 on a.host_id=c1.host_id and a.object=c1.object
left join (select * from task_ph where subname='cpu_usage' and warninglevel=0)c0 on a.host_id=c0.host_id and a.object=c0.object
left join (select * from task_ph where subname='iobusy' and warninglevel=3)i3 on a.host_id=i3.host_id and a.object=i3.object
left join (select * from task_ph where subname='iobusy' and warninglevel=2)i2 on a.host_id=i2.host_id and a.object=i2.object
left join (select * from task_ph where subname='iobusy' and warninglevel=1)i1 on a.host_id=i1.host_id and a.object=i1.object
left join (select * from task_ph where subname='iobusy' and warninglevel=0)i0 on a.host_id=i0.host_id and a.object=i0.object
left join (select * from task_ph where subname='mem_usage' and warninglevel=3)m3 on a.host_id=m3.host_id and a.object=m3.object
left join (select * from task_ph where subname='mem_usage' and warninglevel=2)m2 on a.host_id=m2.host_id and a.object=m2.object
left join (select * from task_ph where subname='mem_usage' and warninglevel=1)m1 on a.host_id=m1.host_id and a.object=m1.object
left join (select * from task_ph where subname='mem_usage' and warninglevel=0)m0 on a.host_id=m0.host_id and a.object=m0.object
left join (select * from task_ph where subname='partition_usage' and warninglevel=3)p3 on a.host_id=p3.host_id and a.object=p3.object
left join (select * from task_ph where subname='partition_usage' and warninglevel=2)p2 on a.host_id=p2.host_id and a.object=p2.object
left join (select * from task_ph where subname='partition_usage' and warninglevel=1)p1 on a.host_id=p1.host_id and a.object=p1.object
left join (select * from task_ph where subname='partition_usage' and warninglevel=0)p0 on a.host_id=p0.host_id and a.object=p0.object
left join (select * from task_ph where subname='swap_usage' and warninglevel=3)s3 on a.host_id=s3.host_id and a.object=s3.object
left join (select * from task_ph where subname='swap_usage' and warninglevel=2)s2 on a.host_id=s2.host_id and a.object=s2.object
left join (select * from task_ph where subname='swap_usage' and warninglevel=1)s1 on a.host_id=s1.host_id and a.object=s1.object
left join (select * from task_ph where subname='swap_usage' and warninglevel=0)s0 on a.host_id=s0.host_id and a.object=s0.object;

create index ix_host_object on rrd_tholdcollect (host_id,object);


#--告警等级越高阈值越低
'''
UPDATE rrd_tholdcollect SET cd1=cd0 WHERE cd1>cd0;
UPDATE rrd_tholdcollect SET cd2=cd1 WHERE cd2>cd1;
UPDATE rrd_tholdcollect SET cd3=cd2 WHERE cd3>cd2;
'''
UPDATE rrd_tholdcollect SET cu1=cu0 WHERE cu1>cu0;
UPDATE rrd_tholdcollect SET cu2=cu1 WHERE cu2>cu1;
UPDATE rrd_tholdcollect SET cu3=cu2 WHERE cu3>cu2;
UPDATE rrd_tholdcollect SET iu1=iu0 WHERE iu1>iu0;
UPDATE rrd_tholdcollect SET iu2=iu1 WHERE iu2>iu1;
UPDATE rrd_tholdcollect SET iu3=iu2 WHERE iu3>iu2;
UPDATE rrd_tholdcollect SET mu1=mu0 WHERE mu1>mu0;
UPDATE rrd_tholdcollect SET mu2=mu1 WHERE mu2>mu1;
UPDATE rrd_tholdcollect SET mu3=mu2 WHERE mu3>mu2;
UPDATE rrd_tholdcollect SET pu1=pu0 WHERE pu1>pu0;
UPDATE rrd_tholdcollect SET pu2=pu1 WHERE pu2>pu1;
UPDATE rrd_tholdcollect SET pu3=pu2 WHERE pu3>pu2;
UPDATE rrd_tholdcollect SET su1=su0 WHERE su1>su0;
UPDATE rrd_tholdcollect SET su2=su1 WHERE su2>su1;
UPDATE rrd_tholdcollect SET su3=su2 WHERE su3>su2;


#  20160619160000--20160622235958 
#--rrd_period--5分钟/1小时数据---27591 rows affected (2 min 10.23 sec)-52020 rows affected (1 min 8.71 sec)---有些是每隔一分钟采一次----------------------------
set @start='201606191600',@end='201606191700';
DROP TABLE IF EXISTS rrd_period;
CREATE TABLE rrd_period as
select * from rrd_data_20160622084846 
where @start<=substring(actiontime,1,12) and substring(actiontime,1,12)<@end
group by host_id,object,subname;

create index ix_subname on rrd_period (subname);

#--rrd_period--5分钟/1小时数据-均值----1小时：53442 rows affected (1 min 50.09 sec)----5min:52020 rows affected (1 min 0.30 sec)------------------
set @start='201606191600',@end='201606191605';
DROP TABLE IF EXISTS rrd_period;
CREATE TABLE rrd_period as
select host_id,object,subname,value from rrd_data_20160622084846 
where @start<=substring(actiontime,1,12) and substring(actiontime,1,12)<@end
group by host_id,object,subname;

create index ix_subname on rrd_period (subname);

#---阈值加上5min/1h数据--30868 rows affected (3 min 38.12 sec)-----------
DROP TABLE IF EXISTS rrd_period_thold;
CREATE TABLE rrd_period_thold as
select a.host_id,a.object
,cpu.value as cpu,a.cu3,a.cu2,a.cu1,a.cu0
,iobusy.value as iobusy,a.iu3,a.iu2,a.iu1,a.iu0
,mem.value as mem,a.mu3,a.mu2,a.mu1,a.mu0
,part.value as part,a.pu3,a.pu2,a.pu1,a.pu0
,swap.value as swap,a.su3,a.su2,a.su1,a.su0
from rrd_tholdcollect a
left join (select * from rrd_period where subname='cpu_usage')cpu on a.host_id=cpu.host_id and a.object=cpu.object
left join (select * from rrd_period where subname='iobusy')iobusy on a.host_id=iobusy.host_id and a.object=iobusy.object
left join (select * from rrd_period where subname='mem_usage')mem on a.host_id=mem.host_id and a.object=mem.object
left join (select * from rrd_period where subname='partition_usage')part on a.host_id=part.host_id and a.object=part.object
left join (select * from rrd_period where subname='swap_usage')swap on a.host_id=swap.host_id and a.object=swap.object;



#---过滤全为空的、有负数的---------
delete from rrd_period_thold where cpu is null and iobusy is null and mem is null and part is null and swap is null;
delete from rrd_period_thold where cpu<0 or iobusy<0 or mem<0 or part<0 or swap<0;
delete from rrd_period_thold where cpu>100 or iobusy>100 or mem>100 or part>100 or swap>100;

#---缺失值补平均分-------------------
select avg(cpu),@ca:=avg(cpu) from rrd_period_thold where cpu is not null;
select avg(iobusy),@ia:=avg(iobusy) from rrd_period_thold where iobusy is not null;
select avg(mem),@ma:=avg(mem) from rrd_period_thold where mem is not null;
select avg(part),@pa:=avg(part) from rrd_period_thold where part is not null;
select avg(swap),@sa:=avg(swap) from rrd_period_thold where swap is not null;

UPDATE rrd_period_thold SET cpu=@ca WHERE cpu is null;
UPDATE rrd_period_thold SET iobusy=@ia WHERE iobusy is null;
UPDATE rrd_period_thold SET mem=@ma WHERE mem is null;
UPDATE rrd_period_thold SET part=@pa WHERE part is null;
UPDATE rrd_period_thold SET swap=@sa WHERE swap is null;



#--评分---12802 rows affected (2.88 sec)---12758 rows affected (3.25 sec)-----
DROP TABLE IF EXISTS rrd_grade;
CREATE TABLE rrd_grade as
select *
,(cu0+cu1+cu2+cu3-4*cpu)/20-(if(cu0-cpu<0,1,0)+if(cu1-cpu<0,1,0)+if(cu2-cpu<0,1,0)+if(cu3-cpu<0,1,0))*10
+(iu0+iu1+iu2+iu3-4*iobusy)/20-(if(iu0-iobusy<0,1,0)+if(iu1-iobusy<0,1,0)+if(iu2-iobusy<0,1,0)+if(iu3-iobusy<0,1,0))*10
+(mu0+mu1+mu2+mu3-4*mem)/20-(if(mu0-mem<0,1,0)+if(mu1-mem<0,1,0)+if(mu2-mem<0,1,0)+if(mu3-mem<0,1,0))*10
+(pu0+pu1+pu2+pu3-4*part)/20-(if(pu0-part<0,1,0)+if(pu1-part<0,1,0)+if(pu2-part<0,1,0)+if(pu3-part<0,1,0))*10
+(su0+su1+su2+su3-4*swap)/20-(if(su0-swap<0,1,0)+if(su1-swap<0,1,0)+if(su2-swap<0,1,0)+if(su3-swap<0,1,0))*10
as score from (select * from rrd_period_thold group by host_id,object)t;

select 'host_id','object'
,'cpu','cu3','cu2','cu1','cu0'
,'iobusy','iu3','iu2','iu1','iu0'
,'mem','mu3','mu2','mu1','mu0'
,'partition','pu3','pu2','pu1','pu0'
,'swap','su3','su2','su1','su0'
,'score'
union all
select * from rrd_grade order by score
INTO OUTFILE 'rrd_grade.csv'
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\r\n';


#--------按服务器host_id评分----
drop table if exists rrd_grade_host;
create table rrd_grade_host as 
select host_id,avg(score) as score from rrd_grade group by host_id order by score;

select 'host_id','score'
union all
select * from rrd_grade_host
INTO OUTFILE 'rrd_grade_host.csv'
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\r\n';


#--数据库查看最高分、最低分--------------------------
select * from rrd_grade order by score desc limit 0,3 \G;



#---算均值，忽略缺失值---------------
create index ix_host_object_subname on task_ph (host_id,object,subname);
create index ix_host_object_subname on rrd_period (host_id,object,subname);

# 111898 rows affected (4.92 sec) 24612 rows affected (2.24 sec)
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

select 'host_id',"object","subname","value","tholdup","warninglevel"
union all
select * from rrd_value_tholdup
INTO OUTFILE 'rrd_value_tholdup.csv'
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\r\n';

# grade
drop table if exists rrd_value_tholdup_grade;
create table rrd_value_tholdup_grade as 
select *
,if(value>tholdup,60-10*(4-warninglevel)-20*(value-tholdup)/(100-tholdup),60+40*(tholdup-value)/tholdup) as score 
from rrd_value_tholdup;

select 'host_id',"object","subname","value","tholdup","warninglevel","score"
union all
select * from rrd_value_tholdup_grade
INTO OUTFILE 'rrd_value_tholdup_grade.csv'
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\r\n';

#--------按服务器host_id评分----
drop table if exists rrd_grade_host2;
create table rrd_grade_host2 as 
select host_id,avg(score) as score from rrd_value_tholdup_grade group by host_id order by score;

select 'host_id','score'
union all
select * from rrd_grade_host2
INTO OUTFILE 'rrd_grade_host2.csv'
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\r\n';

#查具体得分
select * from rrd_value_tholdup_grade where host_id=16192 \G;

select * from rrd_value_tholdup_grade where value>tholdup limit 0,5;


# period内出告警的
drop table if exists rrd_warn_in_period;
create table rrd_warn_in_period as 
select host_id,actiontime,content,priority,status from warning_item 
where @start<=substring(actiontime,1,12) and substring(actiontime,1,12)<@end;

select 'host_id','actiontime','content','priority','status'
union all
select * from rrd_warn_in_period
INTO OUTFILE 'rrd_warn_in_period.csv'
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\r\n';


#---全部服务器全时间段分数（按小时）-----------------------------------------------------------------------------------------
#-----全部：'061916'--'062223' ：3505271 rows affected (1 hour 4 min 20.12 sec)
DROP TABLE IF EXISTS rrd_period;
CREATE TABLE rrd_period as
select * from rrd_data_20160622084846 
group by substring(actiontime,5,6),host_id,object,subname;

create index ix_subname on rrd_period (subname);

#-----加阈值----1675461 rows affected (1 min 29.84 sec)--------------------------
drop table if exists rrd_value_tholdup;
create table rrd_value_tholdup as 
select substring(a.actiontime,5,6) as actiontime,a.host_id,a.object,a.subname,a.value
,ifnull(b.tholdup,100) as tholdup,ifnull(b.warninglevel,4) as warninglevel from(
select * from rrd_period 
where subname='cpu_usage' 
or subname='iobusy'
or subname='mem_usage'
or subname='partition_usage'
or subname='swap_usage')a
left join task_ph b on a.host_id=b.host_id and a.object=b.object and a.subname=b.subname;

delete from rrd_value_tholdup where value is null or value<0 or value>100;

#----------算分----1674888 rows affected (29.52 sec)-----------------------------------
drop table if exists rrd_value_tholdup_grade;
create table rrd_value_tholdup_grade as 
select *
,if(value>tholdup,60-10*(4-warninglevel)-20*(value-tholdup)/(100-tholdup),60+40*(tholdup-value)/tholdup) as score 
from rrd_value_tholdup;

create index ix_time_host on rrd_value_tholdup_grade (actiontime,host_id);

#--------按时间、服务器host_id评分--19.86 sec--
drop table if exists rrd_time_host_score;
create table rrd_time_host_score as 
select actiontime,host_id,avg(score) as score from rrd_value_tholdup_grade group by actiontime,host_id order by score;

select 'actiontime','host_id','score'
union all
select * from rrd_time_host_score
INTO OUTFILE 'rrd_time_host_score.csv'
FIELDS TERMINATED BY ','
ESCAPED BY ''
LINES TERMINATED BY '\r\n';


#---行列转换---------------------------
drop table if exists rrd_host_time;
set @sql:='create table rrd_host_time as select host_id,';
select @sql:=concat(
@sql,GROUP_CONCAT(
distinct concat('max(if(actiontime = ''',actiontime,''',score,0)) t_',actiontime)
)
)
from (select distinct actiontime from rrd_time_host_score order by actiontime)tmp;
set @sql:=concat(@sql,' from rrd_time_host_score group by host_id;');
select @sql;

PREPARE stmt FROM @sql;
EXECUTE stmt;               #--3427 rows affected (19.14 sec)
DEALLOCATE PREPARE stmt;
select * from rrd_host_time limit 0,3 \G;

# 写入csv
set @sql:='select ''host_id'',';
select @sql:=concat(
@sql,GROUP_CONCAT(
distinct concat('''t_',actiontime,'''')
)
)
from (select distinct actiontime from rrd_time_host_score order by actiontime)tmp;
set @sql:=concat(@sql,'
union all 
select * from rrd_host_time
INTO OUTFILE ''rrd_host_time.csv''
FIELDS TERMINATED BY '',''
ESCAPED BY ''''
LINES TERMINATED BY ''\\r\\n'';');
select @sql;

PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

#-----------------------------------------------------------------------------------------------------------------------------

