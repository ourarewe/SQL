#----------存储过程------------------------------------------------------------
#----------循环测试----------------------------------------------------------
create table t1 (filed int);

DELIMITER // 
create procedure pro10()
begin
declare i int;
set i=0;
while i<5 do
insert into t1(filed) values(i);
set i=i+1;
end while;
end;//
DELIMITER ;

call pro10();
select * from t1;
drop table t1;
drop procedure if exists pro10;

#----------------------------------------------------------------------------


#-----准备语句测试-----------------------------------------------------------
drop procedure if exists pro10;
DELIMITER // 
create procedure pro10()
begin
set @tmp_sql:=concat("
select * from task_ph where subname='cpu_usage' limit 0,1;
");
PREPARE s1 FROM @tmp_sql;
EXECUTE s1;
DROP PREPARE s1;
end;//
DELIMITER ;
call pro10();
drop procedure if exists pro10;

#--------------------------------------------------------------------------


#--------------------------------------------------------------------------
drop procedure if exists pro
DELIMITER // 
create procedure pro
begin
	set @t:='061916';
	while @t<='062223' do
	
	end while;
end;//
DELIMITER ;
call pro();
drop procedure if exists pro;
#--------------------------------------------------------------------------