#------------------------------17112------------------------------------------
select host_id,objects,id,collectindex_id from task where host_id=17112;

select * from thold where task_id=143682 or task_id=143719 or task_id=143756
         or task_id=143793 or task_id=143830 or task_id=143867 or task_id=143969 
         or task_id=143970 or task_id=143971 or task_id=143972;

select id,name_en,name_zh,bigtemplate from collectindex where id=14 or id=1 
       or id=4 or id=10 or id=42 or id=43 or id=5;


select a.*,b.warninglevel,b.tholdup,c.name_en,c.bigtemplate from 
(select host_id,objects,id,collectindex_id from task where host_id=17112)a
left join thold b on a.id=b.task_id
left join collectindex c on a.collectindex_id=c.id;


#-----------------------------13623---------------------------------------------
select host_id,objects,id,collectindex_id from task where host_id=13623;

select * from thold where task_id=88516 or task_id=88518 or task_id=88519 
         or task_id=88520 or task_id=88526 or task_id=89513 or task_id=90140 
         or task_id=127343;

select id,name_en,name_zh,bigtemplate from collectindex where id=4 or id=5 
       or id=43 or id=14 or id=1 or id=42;

select a.*,b.warninglevel,b.tholdup,c.name_en,c.bigtemplate from 
(select host_id,objects,id,collectindex_id from task where host_id=13623)a
left join thold b on a.id=b.task_id
left join collectindex c on a.collectindex_id=c.id;

select a.*,b.warninglevel,b.tholdup,c.name_en,c.bigtemplate,d.value from 
(select host_id,objects,id,collectindex_id from task where host_id=13623)a
left join thold b on a.id=b.task_id
left join collectindex c on a.collectindex_id=c.id
left join rrd_data1 d on a.host_id=d.host_id and a.objects=d.object and c.name_en=d.subname;


#---每隔一小时是否出告警--16、17：4000 rows affected (22.10 sec)--4001 rows affected (4.67 sec)---------------------
set @t:='061916';
select "host_id",'t0',"t1" ,"t2",'t3','t4','t5','t6','t7'
union all
select aa.host_id
,if(a0.id is null,0,1) as t0
,if(a1.id is null,0,1) as t1
,if(a2.id is null,0,1) as t2
,if(a3.id is null,0,1) as t3
,if(a4.id is null,0,1) as t4
,if(a5.id is null,0,1) as t5
,if(a6.id is null,0,1) as t6
,if(a6.id is null,0,1) as t6
from(select host_id from rrd_data_20160622084846 group by host_id)aa
left join (select * from warning_item where substring(actiontime,5,6)=@t group by host_id) a0 on aa.host_id=a0.host_id
left join (select * from warning_item where substring(actiontime,5,6)=@t+1 group by host_id) a1 on aa.host_id=a1.host_id
left join (select * from warning_item where substring(actiontime,5,6)=@t+2 group by host_id) a2 on aa.host_id=a2.host_id
left join (select * from warning_item where substring(actiontime,5,6)=@t+3 group by host_id) a3 on aa.host_id=a3.host_id
left join (select * from warning_item where substring(actiontime,5,6)=@t+4 group by host_id) a4 on aa.host_id=a4.host_id
left join (select * from warning_item where substring(actiontime,5,6)=@t+5 group by host_id) a5 on aa.host_id=a5.host_id
left join (select * from warning_item where substring(actiontime,5,6)=@t+6 group by host_id) a6 on aa.host_id=a6.host_id
left join (select * from warning_item where substring(actiontime,5,6)=@t+7 group by host_id) a7 on aa.host_id=a7.host_id
INTO OUTFILE 'rrd_warning_label.csv'
FIELDS TERMINATED BY ','
ESCAPED BY ''
LINES TERMINATED BY '\r\n';


