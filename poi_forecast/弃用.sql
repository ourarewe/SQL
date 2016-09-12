# 弃用
set @t:=now();
insert into poi_forecast_variance(actiontime
,var1,var2,var3,var4,var5,var6,var7,var8,max_error,when_
) select @t,t.v1,t.v2,t.v3,t.v4,t.v5,t.v6,t.v7,t.v8,t.m,t.w from(
select 
sqrt(sum(e1))/96,sqrt(sum(e2))/96,sqrt(sum(e3))/96,sqrt(sum(e4))/96,sqrt(sum(e5))/96,sqrt(sum(e6))/96,sqrt(sum(e7))/96,sqrt(sum(e8))/96
,max(e1),max(if(e1=max(e1),time,0))
from (select time
,pow(real_count-forecast_count_1,2) as e1,pow(real_count-forecast_count_2,2) as e2
,pow(real_count-forecast_count_3,2) as e3,pow(real_count-forecast_count_4,2) as e4
,pow(real_count-forecast_count_5,2) as e5,pow(real_count-forecast_count_6,2) as e6
,pow(real_count-forecast_count_7,2) as e7,pow(real_count-forecast_count_8,2) as e8
from poi_forecast_ph where poi='体育中心时尚天河')a
)t;
