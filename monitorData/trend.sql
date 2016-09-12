#指标数值趋势  6948-17994   20160619160000--20160622235958 

set @h:=12332;
#查指标
select subname from (
select * from rrd_data_20160622084846 where host_id=@h group by subname
)t;

# 查object
select object from (
select * from rrd_data_20160622084846 where host_id=@h group by object
)t;

# 查告警
select * from warning_item where host_id=@h and actiontime>='20160619160000' and actiontime<='20160622235958' \G;


# 输出csv---22 sec
set @sql:='select ''actiontime'',';
select @sql:=concat(@sql,group_concat('''',subname,''''),
' union all 
select * from (
select actiontime,',group_concat('max(case subname when ''',subname,''' then value else 0 end) as ',subname),
' from rrd_data_20160622084846 where host_id=@h and object='''' group by actiontime)t
INTO OUTFILE ''rrd_',@h,'.csv''
FIELDS TERMINATED BY '',''
ESCAPED BY ''''
LINES TERMINATED BY ''\\r\\n'';')
from (
select * from rrd_data_20160622084846 where host_id=@h group by subname
)t;
select @sql;

PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;




# 从数据库看峰值时间点
set @h:=11331; set @ob='E:'
select actiontime,value from rrd_data_20160622084846 where host_id=@h and subname='cpu_usage' order by actiontime limit 0,20;


# 6948 无告警 swap_usage和onlineusers是定值  'disk'循环上升  'mem'循环
set @h:=6948;
select "actiontime","cpu" ,"disk","mem","part"
union all
select * from(
select a.actiontime,a.value as cpu,b.value as disk,c.value as mem,d.value as part 
from(select actiontime,value from rrd_data_20160622084846 where host_id=@h and object='' and subname='cpu_usage' order by actiontime)a
left join (select actiontime,value from rrd_data_20160622084846 where host_id=@h and object='' and subname='disk_usage')b
on a.actiontime=b.actiontime
left join (select actiontime,value from rrd_data_20160622084846 where host_id=@h and object='' and subname='mem_usage')c
on a.actiontime=c.actiontime
left join (select actiontime,value from rrd_data_20160622084846 where host_id=@h and object='' and subname='partition_usage')d
on a.actiontime=d.actiontime
)t
INTO OUTFILE 'rrd_6948.csv'
FIELDS TERMINATED BY ','
ESCAPED BY ''
LINES TERMINATED BY '\r\n';


# 17994   无告警 777 rows affected (3.76 sec)    'disk' 循环   'swap'定值
# 11331 20160619161238 20160620095235 分区占用率超出阈值90
set @h:=11331; set @ob='E:';
select "actiontime","cpu","disk","mem","part","swap"
union all
select * from(
select aa.actiontime,a.value as cpu,b.value as disk,c.value as mem,d.value as part,e.value as swap
from(select actiontime,value from rrd_data_20160622084846 where host_id=@h and object=@ob order by actiontime)aa 
left join (select actiontime,value from rrd_data_20160622084846 where host_id=@h and object=@ob and subname='disk_usage')a
on aa.actiontime=a.actiontime
left join (select actiontime,value from rrd_data_20160622084846 where host_id=@h and object=@ob and subname='disk_usage')b
on aa.actiontime=b.actiontime
left join (select actiontime,value from rrd_data_20160622084846 where host_id=@h and object=@ob and subname='mem_usage')c
on aa.actiontime=c.actiontime
left join (select actiontime,value from rrd_data_20160622084846 where host_id=@h and object=@ob and subname='partition_usage')d
on aa.actiontime=d.actiontime
left join (select actiontime,value from rrd_data_20160622084846 where host_id=@h and object=@ob and subname='swap_usage')e
on aa.actiontime=e.actiontime
)t
INTO OUTFILE 'rrd_11331.csv'
FIELDS TERMINATED BY ','
ESCAPED BY ''
LINES TERMINATED BY '\r\n';


# 16192  20160620031842、20160620092847、20160620131851、20160620142852、20160621172919、20160622085936 cpu超阈值90
set @h:=16192;
select "actiontime","cpu","mem"
union all
select * from(
select a.actiontime,a.value as cpu,b.value as mem
from(select actiontime,value from rrd_data_20160622084846 where host_id=@h and object='' and subname='cpu_usage' order by actiontime)a 
left join (select actiontime,value from rrd_data_20160622084846 where host_id=@h and object='' and subname='mem_usage')b
on a.actiontime=b.actiontime
)t
INTO OUTFILE 'rrd_16192.csv'
FIELDS TERMINATED BY ','
ESCAPED BY ''
LINES TERMINATED BY '\r\n';


# 10033   20160620001545 分区占用率超阈值95  disk mem 突变
set @h:=10033;
select "actiontime","cpu","disk","mem","part"
union all
select * from(
select a.actiontime,a.value as cpu,b.value as disk,c.value as mem,d.value as partition
from(select actiontime,value from rrd_data_20160622084846 where host_id=@h and object='' and subname='cpu_usage' order by actiontime)a 
left join (select actiontime,value from rrd_data_20160622084846 where host_id=@h and object='' and subname='disk_usage')b
on a.actiontime=b.actiontime
left join (select actiontime,value from rrd_data_20160622084846 where host_id=@h and object='' and subname='mem_usage')c
on a.actiontime=c.actiontime
left join (select actiontime,value from rrd_data_20160622084846 where host_id=@h and object='' and subname='partition_usage')d
on a.actiontime=d.actiontime
)t
INTO OUTFILE 'rrd_10033.csv'
FIELDS TERMINATED BY ','
ESCAPED BY ''
LINES TERMINATED BY '\r\n';


# 15674 cpu超阈值60 78次   disk mem swap 少数值
set @h:=15674;
select "actiontime","cpu","disk","mem","part","swap"
union all
select * from(
select a.actiontime,a.value as cpu, b.value as disk, c.value as mem, d.value as partition, e.value as swap
from(select actiontime,value from rrd_data_20160622084846 where host_id=@h and object='' and subname='cpu_usage' order by actiontime)a 
left join (select actiontime,value from rrd_data_20160622084846 where host_id=@h and object='' and subname='disk_usage')b
on a.actiontime=b.actiontime
left join (select actiontime,value from rrd_data_20160622084846 where host_id=@h and object='' and subname='mem_usage')c
on a.actiontime=c.actiontime
left join (select actiontime,value from rrd_data_20160622084846 where host_id=@h and object='' and subname='partition_usage')d
on a.actiontime=d.actiontime
left join (select actiontime,value from rrd_data_20160622084846 where host_id=@h and object='' and subname='swap_usage')e
on a.actiontime=e.actiontime
)t
INTO OUTFILE 'rrd_15674.csv'
FIELDS TERMINATED BY ','
ESCAPED BY ''
LINES TERMINATED BY '\r\n';


# 15201 mem超阈值80  56次  cpu disk 循环
set @h:=15201;
select "actiontime","cpu","disk","mem","partition"
union all
select * from(
select a.actiontime,a.value as cpu, b.value as disk, c.value as mem, d.value as partition
from(select actiontime,value from rrd_data_20160622084846 where host_id=@h and object='' and subname='cpu_usage' order by actiontime)a 
left join (select actiontime,value from rrd_data_20160622084846 where host_id=@h and object='' and subname='disk_usage')b
on a.actiontime=b.actiontime
left join (select actiontime,value from rrd_data_20160622084846 where host_id=@h and object='' and subname='mem_usage')c
on a.actiontime=c.actiontime
left join (select actiontime,value from rrd_data_20160622084846 where host_id=@h and object='' and subname='partition_usage')d
on a.actiontime=d.actiontime
)t
INTO OUTFILE 'rrd_15201.csv'
FIELDS TERMINATED BY ','
ESCAPED BY ''
LINES TERMINATED BY '\r\n';


