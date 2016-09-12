# policeService ->region_popul_static  
# 2015-10-21 17:08:42 -> 2016-08-09 11:31:00  poi_id:16->18
select 'time','体育中心时尚天河','体育中心内场','体育中心外场'
union all
select concat(substring(actiontime,1,15)
,if((substring(actiontime,15,1)=0)or(substring(actiontime,15,1)=3),'0','5')
,':00')
,max(case poi_id when 16 then personnumber else 0 end) 时尚天河
,max(case poi_id when 17 then personnumber else 0 end) 内场
,max(case poi_id when 18 then personnumber else 0 end) 外场
from region_popul_static group by substring(actiontime,1,15)
INTO OUTFILE 'region_popul_static.csv'
FIELDS TERMINATED BY ','
ESCAPED BY ''
LINES TERMINATED BY '\r\n';


# datamining -> poi_forecast
select 'id','poi','time','real_count','forecast_count'
union all 
select * from poi_forecast
INTO OUTFILE 'poi_forecast.csv'
FIELDS TERMINATED BY ','
ESCAPED BY ''
LINES TERMINATED BY '\r\n';

# 行列转换   real_count   forecast_count
select 'time','体育中心时尚天河','体育中心内场','体育中心外场'
union all
select substring(time,1,16)
,max(case poi when'体育中心时尚天河' then real_count else 0 end) 时尚天河
,max(case poi when'体育中心内场' then real_count else 0 end) 内场
,max(case poi when'体育中心外场' then real_count else 0 end) 外场
from poi_forecast group by substring(time,12,5)
INTO OUTFILE 'poi_forecast_8_10.csv'
FIELDS TERMINATED BY ','
ESCAPED BY ''
LINES TERMINATED BY '\r\n';

select 'time','体育中心时尚天河','体育中心内场','体育中心外场'
union all
select substring(time,1,16)
,max(case poi when'体育中心时尚天河' then forecast_count else 0 end) 时尚天河
,max(case poi when'体育中心内场' then forecast_count else 0 end) 内场
,max(case poi when'体育中心外场' then forecast_count else 0 end) 外场
from poi_forecast group by substring(time,12,5)
INTO OUTFILE 'poi_forecast_8_10_forecast_count.csv'
FIELDS TERMINATED BY ','
ESCAPED BY ''
LINES TERMINATED BY '\r\n';


#-----建表-----------------------------------
DROP TABLE IF EXISTS poi_forecast_ph;
CREATE TABLE poi_forecast_ph as
select * from poi_forecast;
update poi_forecast_ph set forecast_count = 0;

# 增加字段
alter table poi_forecast add forecast_count_ph int;
update poi_forecast set forecast_count_ph = 0;

# 删除字段
alter table poi_forecast drop column forecast_count_ph; 


DROP TABLE IF EXISTS poi_forecast_log;
CREATE TABLE poi_forecast_log
(actiontime varchar(50)
,poi varchar(50) comment '区域'
,t0 int default 0 comment '当前时刻'
,t1 int default 0 comment '预测时刻'
,t2 int default 0 comment '预测时刻'
,t3 int default 0 comment '预测时刻'
,t4 int default 0 comment '预测时刻'
,t5 int default 0 comment '预测时刻'
,t6 int default 0 comment '预测时刻'
,t7 int default 0 comment '预测时刻'
,t8 int default 0 comment '预测时刻'
)ENGINE=InnoDB DEFAULT CHARSET=utf8;


DROP TABLE IF EXISTS poi_forecast_ph;
CREATE TABLE poi_forecast_ph 
(poi varchar(50) comment '区域'
,time varchar(50) comment '启动时间'
,real_count int comment '真实人数'
,forecast_count_1 int comment '预测人数'
,forecast_count_2 int
,forecast_count_3 int
,forecast_count_4 int
,forecast_count_5 int
,forecast_count_6 int
,forecast_count_7 int
,forecast_count_8 int
)ENGINE=InnoDB DEFAULT CHARSET=utf8;


# 给poi_forecast_log增加自增主键
set @i:=0;
create table log_temp as select @i:=@i+1 as id,a.* from poi_forecast_log a;

drop table poi_forecast_log;
create table poi_forecast_log as select * from log_temp;

alter table poi_forecast_log add key(id);
alter table poi_forecast_log add primary key(id);
alter table poi_forecast_log change id id int not null auto_increment;

#alter table poi_forecast_log modify id int first;


# 建表记录误差
drop table if exists poi_forecast_errors;
create table poi_forecast_errors
(id int primary key not null auto_increment 
,actiontime varchar(50)
,var1 double
,var2 double
,var3 double
,var4 double
,var5 double
,var6 double
,var7 double
,var8 double
,max_error bigint
);



select @max_e:=max(e1) from(
select time,pow(real_count-forecast_count_1,2) as e1 from poi_forecast_ph where poi='体育中心时尚天河'
)t;
select sqrt(sum(e1)/96),max(if(e1=@max_e,time,0)) from(
select time,pow(real_count-forecast_count_1,2) as e1 from poi_forecast_ph where poi='体育中心时尚天河'
)t;

select @max_e:=max(e1) from(
select time,pow(real_count-forecast_count_1,2) as e1 from poi_forecast_ph where poi='体育中心内场'
)t;
select sqrt(sum(e1)/96),max(if(e1=@max_e,time,0)) from(
select time,pow(real_count-forecast_count_1,2) as e1 from poi_forecast_ph where poi='体育中心内场'
)t;

select @max_e:=max(e1) from(
select time,pow(real_count-forecast_count_1,2) as e1 from poi_forecast_ph where poi='体育中心外场'
)t;
select sqrt(sum(e1)/96),max(if(e1=@max_e,time,0)) from(
select time,pow(real_count-forecast_count_1,2) as e1 from poi_forecast_ph where poi='体育中心外场'
)t;


select poi,time,abs(real_count-forecast_count)/real_count*100 from poi_forecast where real_count!=0;
