#\. D:\\Documents\\SQL\\tianchi\\profile_predict\\Purchase_Redemption.sql

drop table if exists PandR;
create table PandR
(user_id bigint
,report_date varchar(50)
,tBalance bigint
,yBalance bigint
,total_purchase_amt bigint
,direct_purchase_amt bigint
,purchase_bal_amt bigint
,purchase_bank_amt bigint
,total_redeem_amt bigint
,consume_amt bigint
,transfer_amt bigint
,tftobal_amt bigint
,tftocard_amt bigint
,share_amt bigint
,category1 bigint
,category2 bigint
,category3 bigint
,category4 bigint
)ENGINE=InnoDB DEFAULT CHARSET=utf8;

LOAD DATA LOCAL INFILE 'D:\\Documents\\SQL\\tianchi\\profile_predict\\user_balance_table.csv'
INTO TABLE PandR
FIELDS TERMINATED BY ','
ESCAPED BY ''
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES;


# 统计每天总额
select 'time','purchase','redemption'
union all
select report_date,sum(total_purchase_amt) as purchase,sum(total_redeem_amt) as redemption
from pandr group by report_date
INTO OUTFILE 'D:\\Documents\\SQL\\tianchi\\profile_predict\\all_per_day.csv'
FIELDS TERMINATED BY ','
ESCAPED BY ''
LINES TERMINATED BY '\r\n';

