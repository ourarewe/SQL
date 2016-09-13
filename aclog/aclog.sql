

drop table if exists user_terminal;
create table user_terminal
(os varchar(50) default NULL
,os_version varchar(50) default NULL
,term_manufator varchar(50) default NULL
,term_model varchar(50) default NULL
,term_resolution varchar(50) default NULL
,app_curr_version float default 0.0
,mobile_operator varchar(50) default NULL
,mobile_standard varchar(50) default NULL
,mobile_network varchar(50) default NULL
,visit_ip_address varchar(50) default NULL
,msg_first_time varchar(50) default NULL
,msg_last_time varchar(50) default NULL
,msg_count int default 0
)ENGINE=MyISAM DEFAULT CHARSET=utf8;

LOAD DATA LOCAL INFILE 'C:\\Users\\Administrator\\Desktop\\user_terminal.csv'
INTO TABLE user_terminal
FIELDS TERMINATED BY ','
ESCAPED BY ''
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES;

alter table user_terminal add index idx_ip (visit_ip_address);


drop table if exists ticket_20160908;
create table ticket_20160908
(user varchar(50)
,services varchar(50)
,ip varchar(50)
);
LOAD DATA LOCAL INFILE 'C:\\Users\\Administrator\\Desktop\\ticket_20160908.csv'
INTO TABLE ticket_20160908
FIELDS TERMINATED BY ','
ESCAPED BY ''
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES;

alter table ticket_20160908 add index idx_ip (ip);

# tricket的ip数 157303
select count(1) from(select distinct ip from ticket_20160908)t;



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
LOAD DATA LOCAL INFILE 'C:\\Users\\Administrator\\Desktop\\20160908_domain_flux.csv'
INTO TABLE ip_table
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\r\n';

alter table ip_table add index idx_ip (host_ip);

# 交集数
select count(1) from (
select distinct ip from ticket_20160908 a where exists (
select distinct host_ip from ip_table where host_ip=a.ip )
)t;

# 示例
select * from ticket_20160908 a where exists (
select * from ip_table where host_ip=a.ip ) 
limit 10;

