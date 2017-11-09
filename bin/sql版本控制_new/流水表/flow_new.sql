add jar /home/ori/general/bin/transform-date.jar;
CREATE TEMPORARY FUNCTION mon_diff AS 'com.gm.transformDate.MonDiff';
CREATE TEMPORARY FUNCTION tran_date AS 'com.gm.transformDate.TransformDate';
set hive.exec.dynamic.partition.mode=nonstrict;
set spark.sql.crossJoin.enabled=true;

insert overwrite table flow_tmp
select a.*, b.type2 from (
select q.order_number,
q.vin,
q.LNUMBER number,
q.owner_code,
q.birthdate,
q.sexual,
q.province,
q.city,
q.asc_code,
q.asc,
q.outdate,
q.deal_year,
q.deal_date,
q.part_number,
w.name,
'1' quantity,
q.SALES_AMOUNT,
q.age,
case when q.age is null then '未知'
 when q.age=0 then 'NA'
 when q.age=1 and e.cl0>=2 then 'YES'
 when q.age=2 and e.cl0>=2 and e.cl1>=2 then 'YES'
 when q.age>=3 and q.outdate<'2013-10-01' and e.cl0>=2 and e.cl1>=2 then 'YES'
 when q.age>=3 and q.outdate>='2013-10-01' and e.cl0>=2 and e.cl1>=2 and e.cl2>=2 then 'YES'
 ELSE 'NO' end AS BNJK,
case when q.age is null then '未知'
 when q.age<=2 then 'NA'
 when q.age=3 and q.outdate<'2013-10-01' and r.cl2>=1 then 'YES'
 when q.age=3 and q.outdate>='2013-10-01' then 'NA'
 when q.age=4 and q.outdate<'2013-10-01' and r.cl2>=1 and r.cl3>=1 then 'YES'
 when q.age=4 and q.outdate>='2013-10-01' and r.cl3>=1 then 'YES'
 when q.age>=5 and q.outdate<'2013-10-01' and r.cl2>=1 and r.cl3>=1 and r.cl4>=1 then  'YES'
 when q.age=5 and q.outdate>='2013-10-01' and r.cl3>=1 and r.cl4>=1 then  'YES'
 when q.age>=6 and q.outdate>='2013-10-01' and r.cl3>=1 and r.cl4>=1 and r.cl5>=1 then 'YES'
 else 'NO' end as BWJK,
q.mileage,
q.MAINT_TYPE1,
q.primary_classification,
q.second_level_classification
 from(
select s.order_number,
s.vin,
s.LNUMBER,
s.owner_code,
tran_date(m.birthdate) as birthdate,
m.sexual,
m.province,
m.city,
s.asc_code,
s.asc,
s.outdate ,
substr(tran_date(s.order_balance_date),1,4) as deal_year,
substr(tran_date(s.order_balance_date),6,5) as deal_date,
t.part_number,
t.SALES_AMOUNT,
int(mon_diff('2016-12-01',s.outdate)/12) as age,
s.mileage,
case when claim = '1' then '索赔' else s.MAINT_TYPE1 end MAINT_TYPE1,
s.primary_classification,
s.second_level_classification
 from (
select s.*,p.chcode,p.asc from ${pub_db}.label_order s ,(select * from ${pub_db}.distributor tb left semi join (select max(version) as version from ${pub_db}.distributor) ta on ta.version=tb.version) p
where s.asc_code=p.asccode
and s.MAINT_TYPE1<>'删除'
and p.chcode <>''
) s
left join ${pub_db}.part t
join ${pub_db}.customer_uniq m
on s.asc_code=t.asc_code
and s.order_number=t.order_number
and s.chcode=m.ch_code
and s.vin = m.vin
) q left join
(
select distinct '机油' as type,
part_num,
concat(type,'机油') as name ,
version
from (select * from ${pub_db}.engine_oil tb left semi join (select max(version) as version from ${pub_db}.engine_oil) ta on ta.version=tb.version) tc
union all
select distinct '机滤' as type,
part_num,
'机滤' as name,
version
from (select * from ${pub_db}.filter tb left semi join (select max(version) as version from ${pub_db}.filter) ta on ta.version=tb.version) tc
union all
select distinct '养护品' as type,
part_num,
type as name,
version
from (select * from ${pub_db}.maintnance tb left semi join (select max(version) as version from ${pub_db}.maintnance) ta on ta.version=tb.version) tc
union all
select distinct '附件' as type,
part_num,
name_chinese as name,
version
from (select * from ${pub_db}.enclosure tb left semi join (select max(version) as version from ${pub_db}.enclosure) ta on ta.version=tb.version) tc
union all
select distinct '高流件' as type,
part_num,
case when type ='LunT' then '轮胎'
when type='DianC' then '蓄电池'
when type='QiT' and type1<>'' then type1
else '其他高流件' END as name,
version
from (select * from ${pub_db}.high_flow_parts tb left semi join (select max(version) as version from ${pub_db}.high_flow_parts) ta on ta.version=tb.version) tc
) w on q.part_number=w.part_num
left join
(
select   s.asc_code,
s.vin,
sum(case when age_type2='0年' then cnt else 0 end) as cl0,
sum(case when age_type2='1年' then cnt else 0 end) as cl1,
sum(case when age_type2='2年' then cnt else 0 end) as cl2
 from (
select s.asc_code,
s.vin,
age_type2,
count(distinct tran_date(s.order_balance_date)) as cnt
from ${pub_db}.label_order s
where MAINT_TYPE1='保养'
and bn='保内'
group by s.asc_code,
s.vin,
age_type2
having count(distinct tran_date(s.order_balance_date))>=2
) s
group by  s.asc_code,
s.vin
) e on q.asc_code=e.asc_code and q.vin=e.vin
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
from ${pub_db}.label_order s
where 1=1
and bn='保外'
group by s.asc_code,
s.vin,
age_type2
) s
group by  s.asc_code,
s.vin
) r on q.asc_code=r.asc_code and q.vin=r.vin

) a
left join (select part_num,type type2 from (select * from ${pub_db}.engine_oil tb left semi join (select max(version) as version from ${pub_db}.engine_oil) ta on ta.version=tb.version) tc
union all
select part_num,name_chinese type2 from (select * from ${pub_db}.filter tb left semi join (select max(version) as version from ${pub_db}.filter) ta on ta.version=tb.version) tc
union all
select part_num,type1 type2 from (select * from ${pub_db}.high_flow_parts tb left semi join (select max(version) as version from ${pub_db}.high_flow_parts) ta on ta.version=tb.version) tc
union all
select part_num,type type2 from (select * from ${pub_db}.maintnance tb left semi join (select max(version) as version from ${pub_db}.maintnance) ta on ta.version=tb.version) tc
union all
select part_num,name_chinese type2 from (select * from ${pub_db}.enclosure tb left semi join (select max(version) as version from ${pub_db}.enclosure) ta on ta.version=tb.version) tc) b on b.part_num=a.part_number
;




----
insert overwrite table flow_pre
select order_number,vin,number,owner_code,birthdate,t2.id,t3.id,t4.id,asc_code,
asc,outdate,deal_year,deal_date,part_number,name,quantity,sales_amount,age,bnjk,
bwjk,mileage,primary_classification,second_level_classification,maint_type1,type2
from
(select order_number,vin,number,owner_code,birthdate,sexual,province,city,asc_code,asc,outdate,deal_year,deal_date,part_number,
case when name='' or name is null then maint_type
else name end as name
,quantity,sales_amount,age,bnjk,bwjk,mileage,primary_classification,second_level_classification,maint_type maint_type1,
case when type2='' or type2 is null then maint_type
else type2 end as type2
from flow_tmp)t1
join ${pub_db}.sexual t2
join ${pub_db}.province t3
join ${pub_db}.city t4
on t1.sexual=t2.sexual
and t1.province=t3.province
and t1.city=t4.city;



----
insert overwrite table flow
select order_number,vin,number,owner_code,birthdate,sexual,province,city,asc_code,
asc,outdate,deal_year,deal_date,part_number,t5.id,quantity,sales_amount,age,bnjk,
bwjk,mileage,t6.id,t7.id,t8.id,t9.id from
flow_pre t1
join ${pub_db}.name t5
join ${pub_db}.primary_classification t6
join ${pub_db}.second_level_classification t7
join ${pub_db}.maint_type1 t8
join ${pub_db}.type2 t9
on t1.name=t5.name
and t1.primary_classification=t6.primary_classification
and t1.second_level_classification=t7.second_level_classification
and t1.maint_type1=t8.maint_type1
and t1.type2=t9.type2;
