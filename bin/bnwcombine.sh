﻿#----保内外健康标签----
spark-sql --master yarn --driver-memory 6g --executor-memory 6g --num-executors 20 --executor-cores 1 -e "
use ori;
	add jar /home/ori/general/bin/transform-date.jar;
  CREATE TEMPORARY FUNCTION tran_date AS 'com.gm.transformDate.TransformDate';
  CREATE TEMPORARY FUNCTION mon_diff AS 'com.gm.transformDate.MonDiff';
  CREATE TEMPORARY FUNCTION ifnullipsos AS 'com.gm.transformDate.Ret';
  CREATE TEMPORARY FUNCTION isnullipsos AS 'com.gm.transformDate.IsNull';
  
insert overwrite table label_order_bnwjk
select q.rank_number,q.order_status,q.order_bill_date,q.order_number,q.balance_no,q.receptionist,q.lnumber,q.makes,q.series,q.model,q.maint_amount,q.acceptance,q.mileage,q.owner_name,q.returner_name,q.returner_sexual,q.returner_phone_areacode,q.returner_phone,q.returner_mobile,q.predicted_delivery_date,q.complete_date,q.delivery_date,q.owner_code,q.owner_type,q.claim_order_number,q.trialer,q.vin,q.asc_code,q.order_balance_date,q.order_clear_date,q.order_balance_amount,q.paid_amount,q.maint_desc,q.order_desc,q.order_type,q.maint_type,q.balance_no_1,q.maint_type1,q.first_maintnance,q.claim,q.outdate,q.number_of_month,q.age_type,q.age_type2,q.bn,q.primary_classification,q.second_level_classification,case when q.outdate='未知' then '未知'
 when int(mon_diff('2017-04-01',q.outdate)/12)=0 then 'NA'
 when int(mon_diff('2017-04-01',q.outdate)/12)=1 and e.cl0>=2 then 'YES'
 when int(mon_diff('2017-04-01',q.outdate)/12)=2 and e.cl0>=2 and e.cl1>=2 then 'YES'
 when int(mon_diff('2017-04-01',q.outdate)/12)>=3 and q.outdate<'2013-10-01' and e.cl0>=2 and e.cl1>=2 then 'YES'
 when int(mon_diff('2017-04-01',q.outdate)/12)>=3 and q.outdate>='2013-10-01' and e.cl0>=2 and e.cl1>=2 and e.cl2>=2 then 'YES'
 ELSE 'NO' end as bnjk,
 case when q.outdate='未知' then '未知'
 when int(mon_diff('2017-04-01',q.outdate)/12)<=2 then 'NA'
 when int(mon_diff('2017-04-01',q.outdate)/12)=3 and q.outdate<'2013-10-01' and r.cl2>=1 then 'YES'
 when int(mon_diff('2017-04-01',q.outdate)/12)=3 and q.outdate>='2013-10-01' then 'NA'
 when int(mon_diff('2017-04-01',q.outdate)/12)=4 and q.outdate<'2013-10-01' and r.cl2>=1 and r.cl3>=1 then 'YES'
 when int(mon_diff('2017-04-01',q.outdate)/12)=4 and q.outdate>='2013-10-01' and r.cl3>=1 then 'YES'
 when int(mon_diff('2017-04-01',q.outdate)/12)>=5 and q.outdate<'2013-10-01' and r.cl2>=1 and r.cl3>=1 and r.cl4>=1 then  'YES'
 when int(mon_diff('2017-04-01',q.outdate)/12)=5 and q.outdate>='2013-10-01' and r.cl3>=1 and r.cl4>=1 then  'YES'
 when int(mon_diff('2017-04-01',q.outdate)/12)>=6 and q.outdate>='2013-10-01' and r.cl3>=1 and r.cl4>=1 and r.cl5>=1 then 'YES'
 else 'NO' end as bwjk
 from  label_order q left join 
(select   s.asc_code,
s.vin,
sum(case when age_type2='0年' then cnt else 0 end) as cl0,
sum(case when age_type2='1年' then cnt else 0 end) as cl1,
sum(case when age_type2='2年' then cnt else 0 end) as cl2
 from (
select s.asc_code,
s.vin,
age_type2,
count(distinct tran_date(s.order_balance_date)) as cnt
from label_order s
where MAINT_TYPE1='保养'
and bn='保内'
group by s.asc_code,
s.vin,
age_type2
having count(distinct tran_date(s.order_balance_date))>=2
) s 
group by  s.asc_code,
s.vin) e on q.asc_code=e.asc_code
 and q.vin=e.vin
left join 
(
select   s.asc_code,
s.vin,
sum(case when age_type2='2年' then cnt else 0 end) as cl2,
sum(case when age_type2='3年' then cnt else 0 end) as cl3,
sum(case when age_type2='4年' then cnt else 0 end) as cl4,
sum(case when age_type2='5年' then cnt else 0 end) as cl5
 from (
select s.asc_code,
s.vin,
age_type2,
count(distinct tran_date(s.order_balance_date)) as cnt
from label_order s
where 1=1
and bn='保外'
group by s.asc_code,
s.vin,
age_type2
) s 
group by  s.asc_code,
s.vin) r on q.asc_code=r.asc_code
 and q.vin=r.vin
 where q.MAINT_TYPE1<>'删除';
 " > log 2>&1 