
#--user_terminal-------------------------------------------------------------
drop table if exists user_terminal;
create table user_terminal
(mac_address varchar(50) default NULL
,imsi varchar(50) default NULL
,imei varchar(50) default NULL
,os varchar(50) default NULL
,os_version varchar(50) default NULL
,term_manufator varchar(50) default NULL
,term_model varchar(50) default NULL
,term_resolution varchar(50) default NULL
,app_curr_version float default 0.0
,mobile_operator varchar(50) default NULL
,mobile_standard varchar(50) default NULL
,mobile_network varchar(50) default NULL
,mobile_generation int default 0
,current_apn varchar(1) default NULL
,current_using_wifi varchar(1) default NULL
,visit_ip_address varchar(50) default NULL
,msg_first_time Datetime default NULL
,msg_last_time Datetime default NULL
,msg_count int default 0
)ENGINE=MyISAM DEFAULT CHARSET=utf8;
LOAD DATA LOCAL INFILE 'D:\\Documents\\SQL\\aclog\\user_terminal.csv'
INTO TABLE user_terminal
FIELDS TERMINATED BY ','
ESCAPED BY ''
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES;

alter table user_terminal add index idx_ip (visit_ip_address);
alter table user_terminal add index idx_mac (mac_address);


#--ticket 可更换不同日期的ticket表-----------------------------------------------------------------
drop table if exists ticket;
create table ticket
(user varchar(50) default null
,biling_id varchar(50) default null
,services varchar(50) default null
,port bigint default 0
,VLAN int default 0
,calling_ip varchar(50) default null  #----------------ip----------------
,calling_mac varchar(50) default null #----------------mac 地址---------------
,time_start Datetime default null
,time_end Datetime default null
,duration bigint default 0 
,in_M float default 0.0
,out_M float default 0.0
,in_bag bigint default 0
,out_bag bigint default 0
,free_in_M int default 0
,free_out_M int default 0
,charge_out_M int default 0
,charge_in_M int default 0
,cost int default 0
,NASID varchar(50) default null
);

LOAD DATA LOCAL INFILE 'D:\\Documents\\SQL\\aclog\\ticket_20160909.csv' # 更换日期
INTO TABLE ticket
FIELDS TERMINATED BY ','
ESCAPED BY ''
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES;
alter table ticket add index idx_ip (calling_ip);
alter table ticket add index idx_mac (calling_mac);


# tricket的ip数 157303
select count(1) from(select distinct ip from ticket)t;



# 想在服务器上直接建个表，但磁盘已满
drop table if exists ip_table;
create table ip_table
(id int not null AUTO_INCREMENT
,host_ip varchar(50)
,dst_ip varchar(50)
,PRIMARY KEY (id)
);
select host_ip,dst_ip into ip_table from 20160908_domain_flux;


# 从服务器导出数据
select host_ip from 20160908_domain_flux
INTO OUTFILE '/var/ftp//School/20160908_domain_flux.csv'
FIELDS TERMINATED BY ','
ESCAPED BY ''
LINES TERMINATED BY '\r\n';

# 导入本地数据库
drop table if exists ip_table;
create table ip_table
(host_ip varchar(50));
LOAD DATA LOCAL INFILE 'D:\\Documents\\SQL\\aclog\\20160908_domain_flux.csv'
INTO TABLE ip_table
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\r\n';

alter table ip_table add index idx_ip (host_ip);

# 交集数 ip字段名已改
select count(1) from (
select distinct calling_ip from ticket a where exists (
select distinct host_ip from ip_table where host_ip=a.calling_ip )
)t;

# 示例
select * from ticket a where exists (
select * from ip_table where host_ip=a.calling_ip ) 
limit 10;

# 时间测试 字段类型已改
select msg_last_time,msg_first_time from user_terminal limit 10;
select datediff(last,first) from (
select str_to_date(msg_last_time,'%Y-%m-%d %H:%i:%s') as last
,str_to_date(msg_first_time,'%Y-%m-%d %H:%i:%s') as first from user_terminal limit 10)t;



# 从user_terminal中选出近3个月还在用且使用时间超1年的用户
select 'ip','msg_first_time','msg_last_time','use_days'
union all
select * from (
select ip,first,last,datediff(last,first) as days from (
select visit_ip_address as ip,msg_first_time as first,msg_last_time as last 
from user_terminal where substring(msg_last_time,1,7)>='2016-07' )a
) b where days>=365
INTO OUTFILE 'D:\\Documents\\SQL\\aclog\\user_1year.csv'
FIELDS TERMINATED BY ','
ESCAPED BY ''
LINES TERMINATED BY '\r\n';


#--测试user_terminal和ticket的mac地址是否匹配 17个
select mac_address from (
select mac_address from user_terminal ut where exists (
select * from ticket where calling_mac=ut.mac_address)
) a;

select count(1) from (
select distinct calling_mac from ticket  ut where calling_mac in (
select mac_address from user_terminal)
) a;

