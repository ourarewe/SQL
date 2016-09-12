

DROP TABLE IF EXISTS rrd_19;
CREATE TABLE rrd_19 as 
select host_id,actiontime,subname,if(object='',null,object) as object,value 
from rrd_data_20160622084846 where actiontime<20160620000000;

#alter table rrd_19 alter object set default NULL;



#-------------元组-----------------------
drop table if exists rrd_test2;
create table rrd_test2 as 
select host_id,object from rrd_data1 
       group by host_id,object;


SELECT 'host_id','object'
UNION ALL
select host_id,object from rrd_test2
     INTO OUTFILE 'rrd_test2.csv'
     FIELDS TERMINATED BY ',' 
     LINES TERMINATED BY '\r\n';

#--------------------------------------

#--------------阈值集合-------------------
drop table if exists rrd_thold;
create table rrd_thold as
select a.host_id,a.objects,b.name_en as subname,c.tholdup,c.tholddown,ifnull(c.warninglevel,4) as warninglevel
       from task a
       left join (select * from collectindex where bigtemplate='os') b on a.collectindex_id=b.id
       left join thold c on a.id=c.task_id;

create index ix_host_object on rrd_thold (host_id,objects);
create index ix_subname on rrd_thold (subname);

SELECT 'host_id','objects','subname','tholdup','tholddown','warninglevel'
UNION ALL
select * from rrd_thold
     INTO OUTFILE 'rrd_thold.csv'
     FIELDS TERMINATED BY ',' 
     LINES TERMINATED BY '\r\n';

#-------------------------------------------------------------

#---------------------cpu---------------------------------------
drop table if exists rrd_cpu;
create table rrd_cpu as
select a.host_id,a.object,a.value,b.tholdup,b.tholddown,b.warninglevel
       from (select * from rrd_data1 where subname='cpu_usage' and value<=100 and value>=0) a
       left join rrd_thold b on a.subname=b.subname   
       group by host_id,object;

SELECT 'host_id','object','value','tholdup','tholddown','warninglevel'
UNION ALL
select * from rrd_cpu
     INTO OUTFILE 'rrd_cpu3.csv'
     FIELDS TERMINATED BY ',' 
     LINES TERMINATED BY '\r\n';

#---------------------------------------------------------------------

#------------------------disk------------------------------------
drop table if exists rrd_disk;
create table rrd_disk as
select a.host_id,a.object,a.value,b.tholdup,b.tholddown 
       from (select * from rrd_data1 where subname='disk_usage' and value<=100 and value>=0) a
       left join rrd_thold b on a.subname=b.subname
       group by host_id,object;

SELECT 'host_id','object','value','tholdup','tholddown'
UNION ALL
select * from rrd_disk
     INTO OUTFILE 'rrd_disk.csv'
     FIELDS TERMINATED BY ',' 
     LINES TERMINATED BY '\r\n';

#----------------------------------------------------------------

#---------------------------mem---------------------------------
drop table if exists rrd_mem;
create table rrd_mem as
select a.host_id,a.object,a.value,b.tholdup,b.tholddown 
       from (select * from rrd_data1 where subname='mem_usage' and value<=100 and value>=0) a
       left join rrd_thold b on a.subname=b.subname
       group by host_id,object;

SELECT 'host_id','object','value','tholdup','tholddown'
UNION ALL
select * from rrd_mem
     INTO OUTFILE 'rrd_mem.csv'
     FIELDS TERMINATED BY ',' 
     LINES TERMINATED BY '\r\n';

#------------------------------------------------------------------

#---------------------------------partition---------------------
drop table if exists rrd_partition;
create table rrd_partition as
select a.host_id,a.object,a.value,b.tholdup,b.tholddown 
       from (select * from rrd_data1 where subname='partition_usage' and value<=100 and value>=0) a
       left join rrd_thold b on a.subname=b.subname
       group by host_id,object;

SELECT 'host_id','object','value','tholdup','tholddown'
UNION ALL
select * from rrd_partition
     INTO OUTFILE 'rrd_partition.csv'
     FIELDS TERMINATED BY ',' 
     LINES TERMINATED BY '\r\n';

#-------------------------------------------------------------

#----------------------------swap--------------------------------
drop table if exists rrd_swap;
create table rrd_swap as
select a.host_id,a.object,a.value,b.tholdup,b.tholddown 
       from (select * from rrd_data1 where subname='swap_usage' and value<=100 and value>=0) a
       left join rrd_thold b on a.subname=b.subname
       group by host_id,object;

SELECT 'host_id','object','value','tholdup','tholddown'
UNION ALL
select * from rrd_swap
     INTO OUTFILE 'rrd_swap.csv'
     FIELDS TERMINATED BY ',' 
     LINES TERMINATED BY '\r\n';

#--------------------------------------------------------------

#---------------------加索引-----------------------
create index ix_host_object on rrd_cpu (host_id,object);
create index ix_host_object on rrd_disk (host_id,object);
create index ix_host_object on rrd_mem (host_id,object);
create index ix_host_object on rrd_partition (host_id,object);
create index ix_host_object on rrd_swap (host_id,object);
create index ix_host on rrd_data1_all (host_id);
#--------------------------------------------------

#--------------汇总--------------------------------------
drop table if exists rrd_data1_all;
create table rrd_data1_all as
select aa.host_id,aa.object
      ,ifnull(a1.tholddown,0) as cpu_tholddown, a1.value as cpu, ifnull(a1.tholdup,100) as cpu_tholdup
      ,ifnull(a2.tholddown,0) as disk_tholddown, a2.value as disk, ifnull(a2.tholdup,100) as disk_tholdup
      ,ifnull(a3.tholddown,0) as mem_tholddown, a3.value as mem, ifnull(a3.tholdup,100) as mem_tholdup
      ,ifnull(a4.tholddown,0) as part_tholddown, a4.value as partition, ifnull(a4.tholdup,100) as part_tholdup
      ,ifnull(a5.tholddown,0) as swap_tholddown, a5.value as swap, ifnull(a5.tholdup,100) as swap_tholdup
      ,ifnull(a6.priority,4) as priority
       from rrd_test2 aa 
       left join rrd_cpu a1 on aa.host_id=a1.host_id and aa.object=a1.object
       left join rrd_disk a2 on aa.host_id=a2.host_id and aa.object=a2.object
       left join rrd_mem a3 on aa.host_id=a3.host_id and aa.object=a3.object
       left join rrd_partition a4 on aa.host_id=a4.host_id and aa.object=a4.object
       left join rrd_swap a5 on aa.host_id=a5.host_id and aa.object=a5.object
       left join warning_item a6 on aa.host_id=a6.host_id;


delete from rrd_data1_all where cpu is null and disk is null and mem is null and partition is null and swap is null;


SELECT 'host_id','object'
      ,'cpu_tholddown','cpu','cpu_tholdup'
      ,'disk_tholddown','disk','disk_tholdup'
      ,'mem_tholddown','mem','mem_tholdup'
      ,'partition_tholddown','partition','partition_tholdup'
      ,'swap_tholddown','swap','swap_tholdup'
      ,'priority'
UNION ALL
select * from rrd_data1_all
     INTO OUTFILE 'rrd_data1_all.csv'
     FIELDS TERMINATED BY ',' 
     LINES TERMINATED BY '\r\n';


drop table if exists rrd_data1_all2;
create table rrd_data1_all2 as
selecr a.*,b.warninglevel from rrd_data1_all a
left join rrd_thold on a.host_id=b.host_id and a.object=b.objects and a.subname

#------------------------------------------------------

#--------------------评分-----------------------
drop table if exists rrd_data1_scr1;
create table rrd_data1_scr1 as 
select host_id,object
,ifnull(cpu_scr,0) as cpu_scr
,ifnull(mem_scr,0) as mem_scr
,ifnull(disk_scr,0) as disk_scr
,ifnull(partition_scr,0) as partition_scr
,ifnull(swap_scr,0) as swap_scr
from  (select host_id,object
,cpu-cpu_thold as cpu_scr
,disk-disk_thold as disk_scr
,mem-mem_thold as mem_scr	                     
,partition-part_thold as partition_scr
,swap-swap_thold as swap_scr
from rrd_data1_all
) t1
where cpu_scr>0 or disk_scr>0 or mem_scr>0 or partition_scr>0 or swap_scr>0;

SELECT 'host_id','object'
      ,'cpu_scr'
      ,'disk_scr'
      ,'mem_scr'
      ,'partition_scr'
      ,'swap_scr'
UNION ALL
select * from rrd_data1_scr1
     INTO OUTFILE 'rrd_data1_scr1.csv'
     FIELDS TERMINATED BY ',' 
     LINES TERMINATED BY '\r\n';


drop table if exists rrd_data1_scr2;
create table rrd_data1_scr2 as 
select host_id,object,100-cpu_scr*0.2-disk_scr*0.2-mem_scr*0.2-partition_scr*0.2-swap_scr*0.2 as score from rrd_data1_scr1;

SELECT 'host_id','object','score'
UNION ALL
select * from rrd_data1_scr2
     INTO OUTFILE 'rrd_data1_scr2.csv'
     FIELDS TERMINATED BY ',' 
     LINES TERMINATED BY '\r\n';


'''
drop table if exists rrd_data1_scr3;
create table rrd_data1_scr3 as 
select host_id,score from (
										               select a.host_id,a.object
										                     ,if(@host=host_id,@rank:=@rank+1,@rank:=1) as id
										                     ,if(@host=host_id,@scr:=@scr+score,@scr:=score) as zf
										                     ,@scr/@rank as score
										                     ,@host:=host_id
										                     from rrd_data1_scr2 a,(select @rank:=0,@host:=null,@scr:=0)b
										              )t
         group by host_id
         order by id desc;

'''
drop table if exists rrd_data1_scr3;
create table rrd_data1_scr3 as 
select host_id,avg(score) as score from rrd_data1_scr2
         group by host_id;

SELECT 'host_id','score'
UNION ALL
select * from rrd_data1_scr3
     INTO OUTFILE 'rrd_data1_scr3.csv'
     FIELDS TERMINATED BY ',' 
     LINES TERMINATED BY '\r\n';


drop table if exists rrd_data1_scr4;
create table rrd_data1_scr4 as 
select host_id,object,cpu_scr*0.2+disk_scr*0.2+mem_scr*0.2+part_scr*0.2+swap_scr*0.2 as score
from (
select host_id,object
,IFNULL(cpu_thold-cpu,100) as cpu_scr
,IFNULL(disk_thold-disk,100) as disk_scr
,IFNULL(mem_thold-mem,100) as mem_scr
,IFNULL(part_thold-partition,100) as part_scr
,IFNULL(swap_thold-swap,100) as swap_scr 
from rrd_data1_all
)t;

select 'host_id','object','score'
union all
select * from rrd_data1_scr4
     INTO OUTFILE 'rrd_data1_scr4.csv'
     FIELDS TERMINATED BY ',' 
     LINES TERMINATED BY '\r\n';

#--------------------------------------------------

#----------------分数分布统计------------------------
drop table if exists rrd_data1_plot;
create table rrd_data1_plot as 
select a.host_id,ifnull(b.score,100) as score from( 
                               select host_id from rrd_test2 group by host_id
                             ) a
       left join rrd_data1_scr3 b on a.host_id=b.host_id;

SELECT 'host_id','score'
UNION ALL
select * from rrd_data1_plot
     INTO OUTFILE 'rrd_data1_plot.csv'
     FIELDS TERMINATED BY ',' 
     LINES TERMINATED BY '\r\n';


drop table if exists rrd_data1_plot2;
create table rrd_data1_plot2 as 
select host_id,avg(score) as score from rrd_data1_scr4 group by host_id;

select 'host_id','score'
union all
select * from rrd_data1_plot2
INTO OUTFILE 'rrd_data1_plot2.csv'
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\r\n';


#---------------------------------------------------

#-------------查看最低分最高分特征值--------------------------
drop table if exists rrd_data1_rank;
create table rrd_data1_rank as 
select * from (
select a.*,ifnull(b.score,100) as score from rrd_data1_all a left join rrd_data1_plot2 b on a.host_id=b.host_id
)t order by score;


select 'host_id','object'
      ,'cpu','cpu_thold'
      ,'disk','disk_thold'
      ,'mem','mem_thold'
      ,'partition','partition_thold'
      ,'swap','swap_thold'
      ,'score'
union all
select * from rrd_data1_rank
     INTO OUTFILE 'rrd_data1_rank.csv'
     FIELDS TERMINATED BY ',' 
     LINES TERMINATED BY '\r\n';  


#------------------------------------------------------------
