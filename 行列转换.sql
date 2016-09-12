# set group_concat_max_len=18446744073709551615

create table tb
(
   Name    varchar(10) ,
   Subject varchar(10) ,
   Result  int
);

insert into tb(Name , Subject , Result) values('张三' , '语文' , 74);
insert into tb(Name , Subject , Result) values('张三' , '数学' , 83);
insert into tb(Name , Subject , Result) values('张三' , '物理' , 93);
insert into tb(Name , Subject , Result) values('李四' , '语文' , 74);
insert into tb(Name , Subject , Result) values('李四' , '数学' , 84);
insert into tb(Name , Subject , Result) values('李四' , '物理' , 94);


#------静态--------------------------------------------------------------------------------------
select name 姓名,
  max(case subject when'语文'then result else 0 end) 语文,
  max(case subject when'数学'then result else 0 end) 数学,
  max(case subject when'物理'then result else 0 end) 物理
from tb
group by name;


#------动态--------------------------------------------------------------------------------------
set @sql:='select Name as 姓名,';
select @sql:=concat(
@sql,GROUP_CONCAT(
distinct concat('max(case Subject when ''', Subject ,''' then Result else 0 end) as ',Subject)
)
)
from (select distinct Subject from tb)a \G;
set @sql:=concat(@sql,' from tb group by name;');
select @sql;

PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;


#----制表------------------------------
drop table if exists tb2;
set @sql:='create table tb2 as select Name as 姓名,';
select @sql:=concat(
@sql,GROUP_CONCAT(
distinct concat('max(case Subject when ''', Subject ,''' then Result else 0 end) as ',Subject)
)
)
from (select distinct Subject from tb)a \G;
set @sql:=concat(@sql,' from tb group by name;');
select @sql;

PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
select * from tb2;


