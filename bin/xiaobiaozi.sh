#----车型画像
#spark-sql --master yarn-client --driver-memory 6g --executor-memory 6g --num-executors 20 --executor-cores 1 -e "
#use ipsos;
#add jar /home/ipsos/general/bin/transform-date.jar;
#  CREATE TEMPORARY FUNCTION tran_date AS 'com.changxi.transformDate.TransformDate';
#  CREATE TEMPORARY FUNCTION mon_diff AS 'com.changxi.transformDate.MonDiff';
#  CREATE TEMPORARY FUNCTION ifnullipsos AS 'com.changxi.transformDate.Ret';
#  CREATE TEMPORARY FUNCTION isnullipsos AS 'com.changxi.transformDate.IsNull';
  
  
  
#1----db_base---
spark-sql --master yarn-client --driver-memory 6g --executor-memory 6g --num-executors 20 --executor-cores 1 -e "
use ipsos;
add jar /home/ipsos/general/bin/transform-date.jar;
  CREATE TEMPORARY FUNCTION tran_date AS 'com.changxi.transformDate.TransformDate';
  CREATE TEMPORARY FUNCTION mon_diff AS 'com.changxi.transformDate.MonDiff';
  CREATE TEMPORARY FUNCTION ifnullipsos AS 'com.changxi.transformDate.Ret';
  CREATE TEMPORARY FUNCTION isnullipsos AS 'com.changxi.transformDate.IsNull';
insert overwrite table db_base
select s.*,
case when t.vin is null then 'NO' else t.bnjk  end  as bnjk,
case when t.vin is not null then t.bwjk
when t.vin is null and s.invoicedate<'2013-10-01' then 'NO'
when t.vin is null and s.invoicedate>='2013-10-01' then 'NA'
else null end as bwjk
 from (
 select s.*,t.invoicedate,
int(mon_diff('2017-03-01',t.invoicedate)/12) as age
 from (
select u.asccode ch_code,m.asc,m.vin,q.yjcx
 from customer_uniq m,customer_mark q ,distributor u
where m.makes=q.marks and m.series=q.series
and m.model=q.model
and m.ch_code=u.chcode
and u.asccode in('2200096',
'2200099',
'2200256',
'2200621',
'2200127',
'2200218',
'2200104',
'2200180',
'2200594',
'2200124',
'2200691',
'2200762',
'2200167',
'2200233')
) s ,
(select vin,min(tran_date(INVOICE_DATE)) as invoicedate from label_doss group by vin)t
 where  s.vin=t.vin
 and int(mon_diff('2017-03-01',t.invoicedate)/12)>0 
 )  s
left join 
(select distinct asc_code,vin,bnjk,bwjk from label_order_bnwjk  where asc_code in('2200096',
'2200099',
'2200256',
'2200621',
'2200127',
'2200218',
'2200104',
'2200180',
'2200594',
'2200124',
'2200691',
'2200762',
'2200167',
'2200233')) t
on s.vin =t.vin and s.ch_code=t.asc_code;
" > ../log/db_base 2>&1




#2----客户数----
spark-sql --master yarn-client --driver-memory 6g --executor-memory 6g --num-executors 20 --executor-cores 1 -e "
use ipsos;
add jar /home/ipsos/general/bin/transform-date.jar;
  CREATE TEMPORARY FUNCTION tran_date AS 'com.changxi.transformDate.TransformDate';
  CREATE TEMPORARY FUNCTION mon_diff AS 'com.changxi.transformDate.MonDiff';
  CREATE TEMPORARY FUNCTION ifnullipsos AS 'com.changxi.transformDate.Ret';
  CREATE TEMPORARY FUNCTION isnullipsos AS 'com.changxi.transformDate.IsNull';
select ch_code,yjcx,
count(*) as bnzt,
sum(case when bnjk='YES' then 1 else 0 end ) as bnjk,
sum(case when bwjk in('YES','NO') then 1 else 0 end ) as bwzt,
sum(case when bwjk='YES' then 1 else 0 end ) as bwjk
 from db_base
 group by yjcx,ch_code;
 " > kehushu 2>../log/kehushu 
 
#3----产值(不含事故)----
spark-sql --master yarn-client --driver-memory 6g --executor-memory 6g --num-executors 20 --executor-cores 1 -e "
use ipsos;
add jar /home/ipsos/general/bin/transform-date.jar;
  CREATE TEMPORARY FUNCTION tran_date AS 'com.changxi.transformDate.TransformDate';
  CREATE TEMPORARY FUNCTION mon_diff AS 'com.changxi.transformDate.MonDiff';
  CREATE TEMPORARY FUNCTION ifnullipsos AS 'com.changxi.transformDate.Ret';
  CREATE TEMPORARY FUNCTION isnullipsos AS 'com.changxi.transformDate.IsNull';
select ch_code,yjcx,
sum(case when invoicedate<'2013-10-01' then ifnullipsos(cz_cl0)+ifnullipsos(cz_cl1)
 when invoicedate>='2013-10-01' then ifnullipsos(cz_cl0)+ifnullipsos(cz_cl1)+ifnullipsos(cz_cl2) else null end)  as bnzt,
sum(case when invoicedate<'2013-10-01' and bnjk='YES' then ifnullipsos(cz_cl0)+ifnullipsos(cz_cl1)
 when invoicedate>='2013-10-01' and bnjk='YES' then ifnullipsos(cz_cl0)+ifnullipsos(cz_cl1)+ifnullipsos(cz_cl2) else null end ) as bnzt,
sum(case when invoicedate<'2013-10-01' and bwjk in('YES','NO') then ifnullipsos(cz_cl2)+ifnullipsos(cz_cl3)+ifnullipsos(cz_cl4)
 when invoicedate>='2013-10-01' and bwjk in('YES','NO') then ifnullipsos(cz_cl3)+ifnullipsos(cz_cl4)+ifnullipsos(cz_cl5) else null end ) as bwzt,
sum(case when invoicedate<'2013-10-01' and bwjk in('YES') then ifnullipsos(cz_cl2)+ifnullipsos(cz_cl3)+ifnullipsos(cz_cl4)
 when invoicedate>='2013-10-01' and bwjk in('YES') then ifnullipsos(cz_cl3)+ifnullipsos(cz_cl4)+ifnullipsos(cz_cl5) else null end ) as bwjk
 from (
select s.*,
case when s.age>=1 then ifnullipsos(cl0) else null end as cz_cl0,
case when s.age>=2 then ifnullipsos(cl1) else null end as cz_cl1, 
case when s.age>=3 then ifnullipsos(cl2) else null end as cz_cl2, 
case when s.age>=4 then ifnullipsos(cl3) else null end as cz_cl3, 
case when s.age>=5 then ifnullipsos(cl4) else null end as cz_cl4, 
case when s.age>=6 then ifnullipsos(cl5) else null end as cz_cl5  
 from db_base s
left join(
select asc_code,vin,
sum(case when age_type2=0 then amount else 0 end ) as cl0,
sum(case when age_type2=1 then amount else 0 end ) as cl1,
sum(case when age_type2=2 then amount else 0 end ) as cl2, 
sum(case when age_type2=3 then amount else 0 end ) as cl3, 
sum(case when age_type2=4 then amount else 0 end ) as cl4,
sum(case when age_type2=5 then amount else 0 end ) as cl5  
 from (
select asc_code,vin,regexp_replace(age_type2,'年','') as age_type2,
sum(order_balance_amount) as amount 
from label_order where asc_code in('2200096',
'2200099',
'2200256',
'2200621',
'2200127',
'2200218',
'2200104',
'2200180',
'2200594',
'2200124',
'2200691',
'2200762',
'2200167',
'2200233')
and MAINT_TYPE1 not in('删除','事故','装潢')
and regexp_replace(age_type2,'年','')<=5
group by asc_code,vin,regexp_replace(age_type2,'年','')
 ) s 
 group by vin,asc_code
 ) t on s.vin=t.vin and s.ch_code=t.asc_code
 ) m 
 group by yjcx,ch_code;
 " >chanzhibuhanshigu 2>../log/chanzhibuhanshigu
 
 
#4----年均产值(不含事故装潢)----
spark-sql --master yarn-client --driver-memory 6g --executor-memory 6g --num-executors 20 --executor-cores 1 -e "
use ipsos;
add jar /home/ipsos/general/bin/transform-date.jar;
  CREATE TEMPORARY FUNCTION tran_date AS 'com.changxi.transformDate.TransformDate';
  CREATE TEMPORARY FUNCTION mon_diff AS 'com.changxi.transformDate.MonDiff';
  CREATE TEMPORARY FUNCTION ifnullipsos AS 'com.changxi.transformDate.Ret';
  CREATE TEMPORARY FUNCTION isnullipsos AS 'com.changxi.transformDate.IsNull';
select ch_code,yjcx,sum(aa)/count(*) as bnzt,
sum(case when bnjk='YES' then aa else 0 end )/ sum(case when bnjk='YES' then 1 else 0 end) as bnjk, 
sum(case when bwjk in('YES','NO') then bb else 0 end )/sum(case when bwjk in('YES','NO') then 1 else 0 end) as bwzt,
sum(case when bwjk in('YES') then bb else 0 end )/sum(case when bwjk in('YES') then 1 else 0 end) as bwjk
from (
select ch_code,yjcx,bnjk,bwjk,
(case when invoicedate<'2013-10-01' then ifnullipsos(cz_cl0)+ifnullipsos(cz_cl1)
else ifnullipsos(cz_cl0)+ifnullipsos(cz_cl1)+ifnullipsos(cz_cl2)  end) /(
case when invoicedate<'2013-10-01' then isnullipsos(cz_cl0)+isnullipsos(cz_cl1) else isnullipsos(cz_cl0)+isnullipsos(cz_cl1)+isnullipsos(cz_cl2) end 
) as aa,
(case when invoicedate<'2013-10-01' then ifnullipsos(cz_cl2)+ifnullipsos(cz_cl3)+ifnullipsos(cz_cl4)
else  ifnullipsos(cz_cl3)+ifnullipsos(cz_cl4)+ifnullipsos(cz_cl5)  end)/(
case when invoicedate<'2013-10-01' then isnullipsos(cz_cl2)+isnullipsos(cz_cl3)+isnullipsos(cz_cl4)
else  isnullipsos(cz_cl3)+isnullipsos(cz_cl4)+isnullipsos(cz_cl5)  end
) as bb
 from (
select s.*,
case when s.age>=1 then ifnullipsos(cl0) else null end as cz_cl0,
case when s.age>=2 then ifnullipsos(cl1) else null end as cz_cl1, 
case when s.age>=3 then ifnullipsos(cl2) else null end as cz_cl2, 
case when s.age>=4 then ifnullipsos(cl3) else null end as cz_cl3, 
case when s.age>=5 then ifnullipsos(cl4) else null end as cz_cl4, 
case when s.age>=6 then ifnullipsos(cl5) else null end as cz_cl5  
 from db_base s
left join(
select asc_code,vin,
sum(case when age_type2=0 then amount else 0 end ) as cl0,
sum(case when age_type2=1 then amount else 0 end ) as cl1,
sum(case when age_type2=2 then amount else 0 end ) as cl2, 
sum(case when age_type2=3 then amount else 0 end ) as cl3, 
sum(case when age_type2=4 then amount else 0 end ) as cl4,
sum(case when age_type2=5 then amount else 0 end ) as cl5  
 from (
select asc_code,vin,regexp_replace(age_type2,'年','') as age_type2,
sum(order_balance_amount) as amount 
from label_order where asc_code in('2200096',
'2200099',
'2200256',
'2200621',
'2200127',
'2200218',
'2200104',
'2200180',
'2200594',
'2200124',
'2200691',
'2200762',
'2200167',
'2200233') 
and MAINT_TYPE1 not in('删除','事故','装潢')
and regexp_replace(age_type2,'年','')<=5
group by asc_code,vin,regexp_replace(age_type2,'年','')
 ) s 
 group by vin,asc_code
 ) t on s.vin=t.vin and s.ch_code=t.asc_code
 ) m )p group by ch_code,yjcx;
 
" > nianjunchanzhibuhanshiguzh 2>../log/nianjunchanzhibhsgzh
#5----年均产值----
spark-sql --master yarn-client --driver-memory 6g --executor-memory 6g --num-executors 20 --executor-cores 1 -e "
use ipsos;
add jar /home/ipsos/general/bin/transform-date.jar;
  CREATE TEMPORARY FUNCTION tran_date AS 'com.changxi.transformDate.TransformDate';
  CREATE TEMPORARY FUNCTION mon_diff AS 'com.changxi.transformDate.MonDiff';
  CREATE TEMPORARY FUNCTION ifnullipsos AS 'com.changxi.transformDate.Ret';
  CREATE TEMPORARY FUNCTION isnullipsos AS 'com.changxi.transformDate.IsNull';
 select ch_code,yjcx,sum(aa)/count(*) as bnzt,
sum(case when bnjk='YES' then aa else 0 end )/ sum(case when bnjk='YES' then 1 else 0 end) as bnjk, 
sum(case when bwjk in('YES','NO') then bb else 0 end )/sum(case when bwjk in('YES','NO') then 1 else 0 end) as bwzt,
sum(case when bwjk in('YES') then bb else 0 end )/sum(case when bwjk in('YES') then 1 else 0 end) as bwjk
from (
select ch_code,yjcx,bnjk,bwjk,
(case when invoicedate<'2013-10-01' then ifnullipsos(cz_cl0)+ifnullipsos(cz_cl1)
else ifnullipsos(cz_cl0)+ifnullipsos(cz_cl1)+ifnullipsos(cz_cl2)  end) /(
case when invoicedate<'2013-10-01' then isnullipsos(cz_cl0)+isnullipsos(cz_cl1) else isnullipsos(cz_cl0)+isnullipsos(cz_cl1)+isnullipsos(cz_cl2) end 
) as aa,
(case when invoicedate<'2013-10-01' then ifnullipsos(cz_cl2)+ifnullipsos(cz_cl3)+ifnullipsos(cz_cl4)
else  ifnullipsos(cz_cl3)+ifnullipsos(cz_cl4)+ifnullipsos(cz_cl5)  end)/(
case when invoicedate<'2013-10-01' then isnullipsos(cz_cl2)+isnullipsos(cz_cl3)+isnullipsos(cz_cl4)
else  isnullipsos(cz_cl3)+isnullipsos(cz_cl4)+isnullipsos(cz_cl5)  end
) as bb
 from (
select s.*,
case when s.age>=1 then ifnullipsos(cl0) else null end as cz_cl0,
case when s.age>=2 then ifnullipsos(cl1) else null end as cz_cl1, 
case when s.age>=3 then ifnullipsos(cl2) else null end as cz_cl2, 
case when s.age>=4 then ifnullipsos(cl3) else null end as cz_cl3, 
case when s.age>=5 then ifnullipsos(cl4) else null end as cz_cl4, 
case when s.age>=6 then ifnullipsos(cl5) else null end as cz_cl5  
 from db_base s
left join(
select asc_code,vin,
sum(case when age_type2=0 then amount else 0 end ) as cl0,
sum(case when age_type2=1 then amount else 0 end ) as cl1,
sum(case when age_type2=2 then amount else 0 end ) as cl2, 
sum(case when age_type2=3 then amount else 0 end ) as cl3, 
sum(case when age_type2=4 then amount else 0 end ) as cl4,
sum(case when age_type2=5 then amount else 0 end ) as cl5  
 from (
select asc_code,vin,regexp_replace(age_type2,'年','') as age_type2,
sum(order_balance_amount) as amount 
from label_order where asc_code in('2200096',
'2200099',
'2200256',
'2200621',
'2200127',
'2200218',
'2200104',
'2200180',
'2200594',
'2200124',
'2200691',
'2200762',
'2200167',
'2200233') 
and MAINT_TYPE1 not in('删除')
and regexp_replace(age_type2,'年','')<=5
group by asc_code,vin,regexp_replace(age_type2,'年','')
 ) s 
 group by vin,asc_code
 ) t on s.vin=t.vin and s.ch_code=t.asc_code
 ) m )p group by ch_code,yjcx;
 ">njcz 2> ../log/njcz 
 
#6 ----年均客单价(不含事故装潢)----
 spark-sql --master yarn-client --driver-memory 6g --executor-memory 6g --num-executors 20 --executor-cores 1 -e "
use ipsos;
add jar /home/ipsos/general/bin/transform-date.jar;
  CREATE TEMPORARY FUNCTION tran_date AS 'com.changxi.transformDate.TransformDate';
  CREATE TEMPORARY FUNCTION mon_diff AS 'com.changxi.transformDate.MonDiff';
  CREATE TEMPORARY FUNCTION ifnullipsos AS 'com.changxi.transformDate.Ret';
  CREATE TEMPORARY FUNCTION isnullipsos AS 'com.changxi.transformDate.IsNull';
 select ch_code,yjcx,sum(aa)/count(*) as bnzt,
sum(case when bnjk='YES' then aa else 0 end )/ sum(case when bnjk='YES' then 1 else 0 end) as bnjk, 
sum(case when bwjk in('YES','NO') then bb else 0 end )/sum(case when bwjk in('YES','NO') then 1 else 0 end) as bwzt,
sum(case when bwjk in('YES') then bb else 0 end )/sum(case when bwjk in('YES') then 1 else 0 end) as bwjk
from (
select ch_code,yjcx,bnjk,bwjk,
(case when invoicedate<'2013-10-01' then ifnullipsos(cz_cl0)+ifnullipsos(cz_cl1)
else ifnullipsos(cz_cl0)+ifnullipsos(cz_cl1)+ifnullipsos(cz_cl2)  end) /(
case when invoicedate<'2013-10-01' then isnullipsos(cz_cl0)+isnullipsos(cz_cl1) else isnullipsos(cz_cl0)+isnullipsos(cz_cl1)+isnullipsos(cz_cl2) end 
) as aa,
(case when invoicedate<'2013-10-01' then ifnullipsos(cz_cl2)+ifnullipsos(cz_cl3)+ifnullipsos(cz_cl4)
else  ifnullipsos(cz_cl3)+ifnullipsos(cz_cl4)+ifnullipsos(cz_cl5)  end)/(
case when invoicedate<'2013-10-01' then isnullipsos(cz_cl2)+isnullipsos(cz_cl3)+isnullipsos(cz_cl4)
else  isnullipsos(cz_cl3)+isnullipsos(cz_cl4)+isnullipsos(cz_cl5)  end
) as bb
 from (
select s.*,
case when s.age>=1 then ifnullipsos(cl0) else null end as cz_cl0,
case when s.age>=2 then ifnullipsos(cl1) else null end as cz_cl1, 
case when s.age>=3 then ifnullipsos(cl2) else null end as cz_cl2, 
case when s.age>=4 then ifnullipsos(cl3) else null end as cz_cl3, 
case when s.age>=5 then ifnullipsos(cl4) else null end as cz_cl4, 
case when s.age>=6 then ifnullipsos(cl5) else null end as cz_cl5  
 from db_base s
left join(
select asc_code,vin,
sum(case when age_type2=0 then amount else 0 end ) as cl0,
sum(case when age_type2=1 then amount else 0 end ) as cl1,
sum(case when age_type2=2 then amount else 0 end ) as cl2, 
sum(case when age_type2=3 then amount else 0 end ) as cl3, 
sum(case when age_type2=4 then amount else 0 end ) as cl4,
sum(case when age_type2=5 then amount else 0 end ) as cl5  
 from (
select asc_code,vin,regexp_replace(age_type2,'年','') as age_type2,sum(order_balance_amount)/count(distinct tran_date(order_balance_date)) as amount 
from label_order where asc_code in('2200096',
'2200099',
'2200256',
'2200621',
'2200127',
'2200218',
'2200104',
'2200180',
'2200594',
'2200124',
'2200691',
'2200762',
'2200167',
'2200233') 
and MAINT_TYPE1 not in('删除','事故','装潢')
and regexp_replace(age_type2,'年','')<=5
group by asc_code,vin,regexp_replace(age_type2,'年','')
 ) s 
 group by vin,asc_code
 ) t on s.vin=t.vin and s.ch_code=t.asc_code
 ) m )p group by ch_code,yjcx;
" >njkdjbhsgzh 2>../log/njkdjbhsgzh
 
#7----年均保养次数----
spark-sql --master yarn-client --driver-memory 6g --executor-memory 6g --num-executors 20 --executor-cores 1 -e "
use ipsos;
add jar /home/ipsos/general/bin/transform-date.jar;
  CREATE TEMPORARY FUNCTION tran_date AS 'com.changxi.transformDate.TransformDate';
  CREATE TEMPORARY FUNCTION mon_diff AS 'com.changxi.transformDate.MonDiff';
  CREATE TEMPORARY FUNCTION ifnullipsos AS 'com.changxi.transformDate.Ret';
  CREATE TEMPORARY FUNCTION isnullipsos AS 'com.changxi.transformDate.IsNull';
select ch_code,yjcx,sum(aa)/count(*) as bnzt,
sum(case when bnjk='YES' then aa else 0 end )/ sum(case when bnjk='YES' then 1 else 0 end) as bnjk, 
sum(case when bwjk in('YES','NO') then bb else 0 end )/sum(case when bwjk in('YES','NO') then 1 else 0 end) as bwzt,
sum(case when bwjk in('YES') then bb else 0 end )/sum(case when bwjk in('YES') then 1 else 0 end) as bwjk
from (
select ch_code,yjcx,bnjk,bwjk,
(case when invoicedate<'2013-10-01' then ifnullipsos(cz_cl0)+ifnullipsos(cz_cl1)
else ifnullipsos(cz_cl0)+ifnullipsos(cz_cl1)+ifnullipsos(cz_cl2)  end) /(
case when invoicedate<'2013-10-01' then isnullipsos(cz_cl0)+isnullipsos(cz_cl1) else isnullipsos(cz_cl0)+isnullipsos(cz_cl1)+isnullipsos(cz_cl2) end 
) as aa,
(case when invoicedate<'2013-10-01' then ifnullipsos(cz_cl2)+ifnullipsos(cz_cl3)+ifnullipsos(cz_cl4)
else  ifnullipsos(cz_cl3)+ifnullipsos(cz_cl4)+ifnullipsos(cz_cl5)  end)/(
case when invoicedate<'2013-10-01' then isnullipsos(cz_cl2)+isnullipsos(cz_cl3)+isnullipsos(cz_cl4)
else  isnullipsos(cz_cl3)+isnullipsos(cz_cl4)+isnullipsos(cz_cl5)  end
) as bb
 from (
select s.*,
case when s.age>=1 then ifnullipsos(cl0) else null end as cz_cl0,
case when s.age>=2 then ifnullipsos(cl1) else null end as cz_cl1, 
case when s.age>=3 then ifnullipsos(cl2) else null end as cz_cl2, 
case when s.age>=4 then ifnullipsos(cl3) else null end as cz_cl3, 
case when s.age>=5 then ifnullipsos(cl4) else null end as cz_cl4, 
case when s.age>=6 then ifnullipsos(cl5) else null end as cz_cl5  
 from db_base s
left join(
select asc_code,vin,
sum(case when age_type2=0 then amount else 0 end ) as cl0,
sum(case when age_type2=1 then amount else 0 end ) as cl1,
sum(case when age_type2=2 then amount else 0 end ) as cl2, 
sum(case when age_type2=3 then amount else 0 end ) as cl3, 
sum(case when age_type2=4 then amount else 0 end ) as cl4,
sum(case when age_type2=5 then amount else 0 end ) as cl5  
 from (
select asc_code,vin,regexp_replace(age_type2,'年','') as age_type2,
count(distinct tran_date(order_balance_date)) as amount 
from label_order where asc_code in('2200096',
'2200099',
'2200256',
'2200621',
'2200127',
'2200218',
'2200104',
'2200180',
'2200594',
'2200124',
'2200691',
'2200762',
'2200167',
'2200233')
and MAINT_TYPE1='保养'
and regexp_replace(age_type2,'年','')<=5
group by asc_code,vin,regexp_replace(age_type2,'年','')
 ) s 
 group by vin,asc_code
 ) t on s.vin=t.vin and s.ch_code=t.asc_code
 ) m )p group by ch_code,yjcx;
 " > njbycs 2>../log/njbycs
 
 
#8----年均养护次数----
spark-sql --master yarn-client --driver-memory 6g --executor-memory 6g --num-executors 20 --executor-cores 1 -e "
use ipsos;
add jar /home/ipsos/general/bin/transform-date.jar;
  CREATE TEMPORARY FUNCTION tran_date AS 'com.changxi.transformDate.TransformDate';
  CREATE TEMPORARY FUNCTION mon_diff AS 'com.changxi.transformDate.MonDiff';
  CREATE TEMPORARY FUNCTION ifnullipsos AS 'com.changxi.transformDate.Ret';
  CREATE TEMPORARY FUNCTION isnullipsos AS 'com.changxi.transformDate.IsNull';
select ch_code,yjcx,sum(aa)/count(*) as bnzt,
sum(case when bnjk='YES' then aa else 0 end )/ sum(case when bnjk='YES' then 1 else 0 end) as bnjk, 
sum(case when bwjk in('YES','NO') then bb else 0 end )/sum(case when bwjk in('YES','NO') then 1 else 0 end) as bwzt,
sum(case when bwjk in('YES') then bb else 0 end )/sum(case when bwjk in('YES') then 1 else 0 end) as bwjk
from (
select ch_code,yjcx,bnjk,bwjk,
(case when invoicedate<'2013-10-01' then ifnullipsos(cz_cl0)+ifnullipsos(cz_cl1)
else ifnullipsos(cz_cl0)+ifnullipsos(cz_cl1)+ifnullipsos(cz_cl2)  end) /(
case when invoicedate<'2013-10-01' then isnullipsos(cz_cl0)+isnullipsos(cz_cl1) else isnullipsos(cz_cl0)+isnullipsos(cz_cl1)+isnullipsos(cz_cl2) end 
) as aa,
(case when invoicedate<'2013-10-01' then ifnullipsos(cz_cl2)+ifnullipsos(cz_cl3)+ifnullipsos(cz_cl4)
else  ifnullipsos(cz_cl3)+ifnullipsos(cz_cl4)+ifnullipsos(cz_cl5)  end)/(
case when invoicedate<'2013-10-01' then isnullipsos(cz_cl2)+isnullipsos(cz_cl3)+isnullipsos(cz_cl4)
else  isnullipsos(cz_cl3)+isnullipsos(cz_cl4)+isnullipsos(cz_cl5)  end
) as bb
 from (
select s.*,
case when s.age>=1 then ifnullipsos(cl0) else null end as cz_cl0,
case when s.age>=2 then ifnullipsos(cl1) else null end as cz_cl1, 
case when s.age>=3 then ifnullipsos(cl2) else null end as cz_cl2, 
case when s.age>=4 then ifnullipsos(cl3) else null end as cz_cl3, 
case when s.age>=5 then ifnullipsos(cl4) else null end as cz_cl4, 
case when s.age>=6 then ifnullipsos(cl5) else null end as cz_cl5  
 from db_base s
left join(
select asc_code,vin,
sum(case when age_type2=0 then amount else 0 end ) as cl0,
sum(case when age_type2=1 then amount else 0 end ) as cl1,
sum(case when age_type2=2 then amount else 0 end ) as cl2, 
sum(case when age_type2=3 then amount else 0 end ) as cl3, 
sum(case when age_type2=4 then amount else 0 end ) as cl4,
sum(case when age_type2=5 then amount else 0 end ) as cl5  
 from (
select s.asc_code,s.vin,regexp_replace(age_type2,'年','') as age_type2,
count(distinct tran_date(s.order_balance_date)) as amount 
from label_order s,(select distinct asc_code,order_number from part t, maintnance m 
where t.asc_code in('2200096',
'2200099',
'2200256',
'2200621',
'2200127',
'2200218',
'2200104',
'2200180',
'2200594',
'2200124',
'2200691',
'2200762',
'2200167',
'2200233') and t.PART_NUMBER=m.part_num ) k 
where s.asc_code in('2200096',
'2200099',
'2200256',
'2200621',
'2200127',
'2200218',
'2200104',
'2200180',
'2200594',
'2200124',
'2200691',
'2200762',
'2200167',
'2200233') 
and s.MAINT_TYPE1<>'删除'
and regexp_replace(age_type2,'年','')<=5
and s.asc_code=k.asc_code
and s.order_number=k.order_number	
group by s.asc_code,s.vin,regexp_replace(age_type2,'年','')
 ) s 
 group by vin,asc_code
 ) t on s.vin=t.vin and s.ch_code=t.asc_code
 ) m )p group by ch_code,yjcx;
 " >njyhcs 2>../log/njyhcs
 
#9----年均高流件次数----
spark-sql --master yarn-client --driver-memory 6g --executor-memory 6g --num-executors 20 --executor-cores 1 -e "
use ipsos;
add jar /home/ipsos/general/bin/transform-date.jar;
  CREATE TEMPORARY FUNCTION tran_date AS 'com.changxi.transformDate.TransformDate';
  CREATE TEMPORARY FUNCTION mon_diff AS 'com.changxi.transformDate.MonDiff';
  CREATE TEMPORARY FUNCTION ifnullipsos AS 'com.changxi.transformDate.Ret';
  CREATE TEMPORARY FUNCTION isnullipsos AS 'com.changxi.transformDate.IsNull';
select ch_code,yjcx,sum(aa)/count(*) as bnzt,
sum(case when bnjk='YES' then aa else 0 end )/ sum(case when bnjk='YES' then 1 else 0 end) as bnjk, 
sum(case when bwjk in('YES','NO') then bb else 0 end )/sum(case when bwjk in('YES','NO') then 1 else 0 end) as bwzt,
sum(case when bwjk in('YES') then bb else 0 end )/sum(case when bwjk in('YES') then 1 else 0 end) as bwjk
from (
select ch_code,yjcx,bnjk,bwjk,
(case when invoicedate<'2013-10-01' then ifnullipsos(cz_cl0)+ifnullipsos(cz_cl1)
else ifnullipsos(cz_cl0)+ifnullipsos(cz_cl1)+ifnullipsos(cz_cl2)  end) /(
case when invoicedate<'2013-10-01' then isnullipsos(cz_cl0)+isnullipsos(cz_cl1) else isnullipsos(cz_cl0)+isnullipsos(cz_cl1)+isnullipsos(cz_cl2) end 
) as aa,
(case when invoicedate<'2013-10-01' then ifnullipsos(cz_cl2)+ifnullipsos(cz_cl3)+ifnullipsos(cz_cl4)
else  ifnullipsos(cz_cl3)+ifnullipsos(cz_cl4)+ifnullipsos(cz_cl5)  end)/(
case when invoicedate<'2013-10-01' then isnullipsos(cz_cl2)+isnullipsos(cz_cl3)+isnullipsos(cz_cl4)
else  isnullipsos(cz_cl3)+isnullipsos(cz_cl4)+isnullipsos(cz_cl5)  end
) as bb
 from (
select s.*,
case when s.age>=1 then ifnullipsos(cl0) else null end as cz_cl0,
case when s.age>=2 then ifnullipsos(cl1) else null end as cz_cl1, 
case when s.age>=3 then ifnullipsos(cl2) else null end as cz_cl2, 
case when s.age>=4 then ifnullipsos(cl3) else null end as cz_cl3, 
case when s.age>=5 then ifnullipsos(cl4) else null end as cz_cl4, 
case when s.age>=6 then ifnullipsos(cl5) else null end as cz_cl5  
 from db_base s
left join(
select asc_code,vin,
sum(case when age_type2=0 then amount else 0 end ) as cl0,
sum(case when age_type2=1 then amount else 0 end ) as cl1,
sum(case when age_type2=2 then amount else 0 end ) as cl2, 
sum(case when age_type2=3 then amount else 0 end ) as cl3, 
sum(case when age_type2=4 then amount else 0 end ) as cl4,
sum(case when age_type2=5 then amount else 0 end ) as cl5  
 from (
select s.asc_code,s.vin,regexp_replace(age_type2,'年','') as  age_type2,
count(distinct tran_date(s.order_balance_date)) as amount 
from label_order s,(select distinct asc_code,order_number from part t, high_flow_parts m 
where t.asc_code='2200124' and t.PART_NUMBER=m.part_num 
and (m.type in('DianC','LunT') or m.type1 in('刹车盘','刹车片','火花塞'))) k 
where s.asc_code in('2200096',
'2200099',
'2200256',
'2200621',
'2200127',
'2200218',
'2200104',
'2200180',
'2200594',
'2200124',
'2200691',
'2200762',
'2200167',
'2200233') 
and s.MAINT_TYPE1<>'删除'
and regexp_replace(age_type2,'年','')<=5
and s.asc_code=k.asc_code
and s.order_number=k.order_number	
group by s.asc_code,s.vin,regexp_replace(age_type2,'年','')
 ) s 
 group by vin,asc_code
 ) t on s.vin=t.vin and s.ch_code=t.asc_code
 ) m )p group by ch_code,yjcx;
 " > njgljcs 2>../log/njgljcs
 
 
#10 ----出保后流失比例----
 spark-sql --master yarn-client --driver-memory 6g --executor-memory 6g --num-executors 20 --executor-cores 1 -e "
use ipsos;
add jar /home/ipsos/general/bin/transform-date.jar;
  CREATE TEMPORARY FUNCTION tran_date AS 'com.changxi.transformDate.TransformDate';
  CREATE TEMPORARY FUNCTION mon_diff AS 'com.changxi.transformDate.MonDiff';
  CREATE TEMPORARY FUNCTION ifnullipsos AS 'com.changxi.transformDate.Ret';
  CREATE TEMPORARY FUNCTION isnullipsos AS 'com.changxi.transformDate.IsNull';
 select ch_code,yjcx,
sum(case when lskh='流失' then 1 else 0 end)/count(*) as bnzt,
sum(case when lskh='流失' and bnjk='YES' then 1 else 0 end)/(
sum(case when bnjk='YES' then 1 else 0 end)
) as bnjk
 from (
 select s.*,
 t.lskh,
 case when t.lskh='未流失' then null 
 else int(mon_diff(t.maxdate,s.invoicedate)/12)+1 end as lscl
 from db_base s ,
 (
 select s.*,
 case when mon_diff('2017-03-01',maxdate)>=12 then '流失'
 when mon_diff('2017-03-01',maxdate)>=6 and
  mon_diff('2017-03-01',maxdate)<12 then '濒临流失'
  else '未流失' end as lskh
 from (
 select asc_code,vin,max(tran_date(order_balance_date)) as maxdate
 from label_order
 where MAINT_TYPE1<>'删除'
 and asc_code in('2200096',
'2200099',
'2200256',
'2200621',
'2200127',
'2200218',
'2200104',
'2200180',
'2200594',
'2200124',
'2200691',
'2200762',
'2200167',
'2200233')
 group by vin,asc_code) s
 ) t where  s.vin = t.vin and s.ch_code=t.asc_code
 ) s 
 where s.invoicedate<'2013-10-01'
 group by ch_code,yjcx;
 
 " >cbhlsbl 2>../log/cbhlsbl
#11  ----出保后濒临流失比例----
  spark-sql --master yarn-client --driver-memory 6g --executor-memory 6g --num-executors 20 --executor-cores 1 -e "
use ipsos;
add jar /home/ipsos/general/bin/transform-date.jar;
  CREATE TEMPORARY FUNCTION tran_date AS 'com.changxi.transformDate.TransformDate';
  CREATE TEMPORARY FUNCTION mon_diff AS 'com.changxi.transformDate.MonDiff';
  CREATE TEMPORARY FUNCTION ifnullipsos AS 'com.changxi.transformDate.Ret';
  CREATE TEMPORARY FUNCTION isnullipsos AS 'com.changxi.transformDate.IsNull';
select ch_code,s.yjcx,
sum(case when lskh='濒临流失' then 1 else 0 end)/count(*) as bnzt,
sum(case when lskh='濒临流失' and bnjk='YES' then 1 else 0 end)/(
sum(case when bnjk='YES' then 1 else 0 end)
) as bnjk
 from (
 select s.*,
 t.lskh,
 case when t.lskh='未流失' then null 
 else int(mon_diff(t.maxdate,s.invoicedate)/12)+1 end as lscl
 from db_base s ,(
 select s.*,
 case when mon_diff('2017-03-01',maxdate)>=12 then '流失'
 when mon_diff('2017-03-01',maxdate)>=6 and
  mon_diff('2017-03-01',maxdate)<12 then '濒临流失'
  else '未流失' end as lskh
 from (
 select asc_code,vin,max(tran_date(order_balance_date)) as maxdate
 from label_order
 where MAINT_TYPE1<>'删除'
 and asc_code in('2200096',
'2200099',
'2200256',
'2200621',
'2200127',
'2200218',
'2200104',
'2200180',
'2200594',
'2200124',
'2200691',
'2200762',
'2200167',
'2200233')
 group by vin,asc_code) s
 ) t where  s.vin = t.vin and s.ch_code=t.asc_code
 ) s where s.invoicedate<'2013-10-01'
 group by ch_code,yjcx;
 " > cbhbllsbl 2>../log/cbhbllsbl
 
#12 ----年均行驶里程----
spark-sql --master yarn-client --driver-memory 6g --executor-memory 6g --num-executors 20 --executor-cores 1 -e "
use ipsos;
add jar /home/ipsos/general/bin/transform-date.jar;
  CREATE TEMPORARY FUNCTION tran_date AS 'com.changxi.transformDate.TransformDate';
  CREATE TEMPORARY FUNCTION mon_diff AS 'com.changxi.transformDate.MonDiff';
  CREATE TEMPORARY FUNCTION ifnullipsos AS 'com.changxi.transformDate.Ret';
  CREATE TEMPORARY FUNCTION isnullipsos AS 'com.changxi.transformDate.IsNull';
select 
s.ch_code,s.yjcx,
sum(ifnullipsos(bnlc))/sum(case when ifnullipsos(bnlc)=0 then 0 else 1 end) as bnzt,
sum(case when bnjk='YES' then ifnullipsos(bnlc) else 0 end)/sum(case when bnjk='YES' and ifnullipsos(bnlc)<>0 then 1 else 0 end ) as bnjk,
sum(case when bwjk in('YES','NO') then  ifnullipsos(bwlc) else 0 end)/sum(case when bwjk in('YES','NO') and ifnullipsos(bwlc)<>0 then 1 else 0 end) as bwzt,
sum(case when bwjk='YES' then ifnullipsos(bwlc) else 0 end)/sum(case when bwjk='YES' and ifnullipsos(bwlc)<>0 then 1 else 0 end ) as bwjk
 from db_base s left join
(
select asc_code,vin,
(bnmaxmile/mon_diff(bnmaxdate,outdate))*12 as bnlc,
((bwmaxmile-bnmaxmile)/mon_diff(bwmaxdate,bnmaxdate))*12 as bwlc
from (
 select asc_code,vin,outdate,
max(case when bn='保内' then mileage else 0 end) as bnmaxmile,
max(case when bn='保内' then tran_date(order_balance_date) else outdate end) as bnmaxdate,
max(case when bn='保外' then mileage else 0 end) as bwmaxmile,
max(case when bn='保外' then tran_date(order_balance_date) else outdate end) as bwmaxdate
 from label_order_bnwjk
where MAINT_TYPE1<>'删除'
 and asc_code in('2200096',
'2200099',
'2200256',
'2200621',
'2200127',
'2200218',
'2200104',
'2200180',
'2200594',
'2200124',
'2200691',
'2200762',
'2200167',
'2200233')
group by asc_code,vin,outdate
 ) k
 ) t on s.ch_code=t.asc_code and s.vin =t.vin 
 group by ch_code,s.yjcx;
 "> njxslc 2>../log/njxslc
 

 