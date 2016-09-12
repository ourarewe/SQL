

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
ESCAPED BY ''''
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES;


drop table if exists ip_table;
create table ip_table
(id int not null AUTO_INCREMENT
,host_ip varchar(50)
,dst_ip varchar(50)
,PRIMARY KEY (id)
);

select host_ip,dst_ip into ip_table from 20160908_domain_flux;
