#\.D:\Documents\SQL\monitorData\rrd.sql



DROP TABLE IF EXISTS rrd_grade_lb;
CREATE TABLE rrd_grade_lb (host_id int(11)
                  ,subname varchar(50)
                  ,object varchar(250)
                  ,value double
                  ,score double
                  ,total_lb double
                  ,total_ph double
                  ,diff double
                 );
LOAD DATA LOCAL INFILE 'D:\\Documents\\sendi\\compare.csv' INTO TABLE rrd_grade_lb
FIELDS TERMINATED BY ','
ESCAPED BY ''
LINES TERMINATED BY '\r\n';


DROP TABLE IF EXISTS rrd_grade_host;
CREATE TABLE rrd_grade_host (host_id int(11)
                  ,score double
                 );
LOAD DATA LOCAL INFILE 'D:\\Documents\\sendi\\rrd_grade_host.csv' INTO TABLE rrd_grade_host
FIELDS TERMINATED BY ','
ESCAPED BY ''
LINES TERMINATED BY '\r\n';



DROP TABLE IF EXISTS rrd;
CREATE TABLE rrd (id int(11)
                  ,actiontime varchar(14)
                  ,subname varchar(50)
                  ,object varchar(250)
                  ,value double
                  ,host_id int(11)
                  ,branch_id int(11)
                 );

LOAD DATA LOCAL INFILE '/home/ourarewe/share/rrd.csv' INTO TABLE rrd
FIELDS TERMINATED BY ','
ESCAPED BY ''
LINES TERMINATED BY '\r\n';


# rrd_period_thold
select 'host_id','object'
,'cpu','cu3','cu2','cu1','cu0'
,'iobusy','iu3','iu2','iu1','iu0'
,'mem','mu3','mu2','mu1','mu0'
,'partition','pu3','pu2','pu1','pu0'
,'swap','su3','su2','su1','su0'
union all
select * from rrd_period_thold
INTO OUTFILE 'rrd_period_thold.csv'
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\r\n';



# rrd_grade
select 'host_id','object'
,'cpu','cu3','cu2','cu1','cu0'
,'disk','du3','du2','du1','du0'
,'mem','mu3','mu2','mu1','mu0'
,'partition','pu3','pu2','pu1','pu0'
,'swap','su3','su2','su1','su0'
,'score'
union all
select * from rrd_grade
INTO OUTFILE 'rrd_grade.csv'
FIELDS TERMINATED BY ',' 
ESCAPED BY ''
LINES TERMINATED BY '\r\n';


#--rrd_period
select 'host_id','object','subname','value'
union all
select * from rrd_period
INTO OUTFILE 'rrd_period.csv'
FIELDS TERMINATED BY ',' 
ESCAPED BY ''
LINES TERMINATED BY '\r\n';



# rrd_collect
select 'host_id','object'
,'cd0','cd1','cd2','cd3','cpu','cu3','cu2','cu1','cu0'
,'disk','du3','du2','du1','du0'
,'mem','mu3','mu2','mu1','mu0'
,'partition','pu3','pu2','pu1','pu0'
,'swap','su3','su2','su1','su0'
union all
select * from rrd_collect
INTO OUTFILE 'rrd_collect.csv'
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\r\n';


# task_ph
select 'host_id','object','collectindex_id','subname'
,'task_id','tholddown','tholdup','warninglevel'
union all
select * from task_ph
INTO OUTFILE 'task_ph.csv'
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\r\n';




# rrd_19
SELECT 'actiontime','host_id','object','subname','value'
UNION ALL
SELECT actiontime,host_id,object,subname,value 
FROM rrd_19 
where actiontime='20160619160000' or actiontime='20160619160001'
group by actiontime,host_id,object,subname,value
INTO OUTFILE 'rrd_19.csv'
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\r\n';




# thold
SELECT 'id','task_id','warninglevel','tholdup','tholddown','trigertimes','hours','hour'
UNION ALL
SELECT * FROM thold INTO OUTFILE 'thold.csv'
     FIELDS TERMINATED BY ',' 
     LINES TERMINATED BY '\r\n';


# warning_item
SELECT 'id','actiontime','content','dbtime','isdelete','level_','logtype','priority','solution','source','status','oldId'
      ,'selected','branch_id','host_id','acknowledged_time','acknowledged_user','occurtimes','emailsent','smssent','subject'
      ,'task_id','warning_position','warning_cause','analyse_process','sendstatus','relatestatus','warnRestoreTime','bussRestoreTime'
UNION ALL
SELECT * FROM warning_item INTO OUTFILE 'warning_item.csv'
     FIELDS TERMINATED BY ',' 
     LINES TERMINATED BY '\r\n';


# cpu_usage
SELECT 'value'
UNION ALL
SELECT value FROM( 
               select * from
               rrd_data_20160622084846 where subname='cpu_usage' and value>=0 and value<=100
             )t 
     INTO OUTFILE 'cpu_value.csv'
     FIELDS TERMINATED BY ',' 
     LINES TERMINATED BY '\r\n';



# mem_usage
SELECT 'value'
UNION ALL
SELECT value FROM( 
               select * from
               rrd_data_20160622084846 where subname='mem_usage' and value>=0
             )t 
     INTO OUTFILE 'mem_value.csv'
     FIELDS TERMINATED BY ',' 
     LINES TERMINATED BY '\r\n';
     

# swap_usage
SELECT 'value'
UNION ALL
SELECT value FROM( 
               select * from
               rrd_data_20160622084846 where subname='swap_usage' and value>=0
             )t 
     INTO OUTFILE 'swap_value.csv'
     FIELDS TERMINATED BY ',' 
     LINES TERMINATED BY '\r\n';


# partition_usage
SELECT 'value'
UNION ALL
SELECT value FROM( 
               select * from
               rrd_data_20160622084846 where subname='partition_usage' and value>=0
             )t 
     INTO OUTFILE 'partition_value.csv'
     FIELDS TERMINATED BY ',' 
     LINES TERMINATED BY '\r\n';


# disk_usage
SELECT 'value'
UNION ALL
SELECT value FROM( 
               select * from
               rrd_data_20160622084846 where subname='disk_usage' and value>=0
             )t 
     INTO OUTFILE 'disk_value.csv'
     FIELDS TERMINATED BY ',' 
     LINES TERMINATED BY '\r\n';

'''
# disk_usage
select 'host_id'
       ,'collectindex_id'
       ,'objects'
       ,'name_en'
       ,'task_id'
       ,'warninglevel'
       ,'tholdup'
       ,'tholddown'
       ,'trigertimes'
union all
SELECT host_id
       ,collectindex_id
       ,objects,name_en
       ,task_id,warninglevel
       ,ifnull(tholdup,-1)
       ,ifnull(tholddown,-1)
       ,trigertimes 
     FROM yuzhi0 where tholdup<100 
     INTO OUTFILE '/data/mysql/monitorData/yuzhi.csv'
     FIELDS TERMINATED BY ',' 
     LINES TERMINATED BY '\r\n';

'''



