#!/bin/bash

spark-sql --master yarn-client --driver-memory 3g --executor-memory 3g --num-executors 20 --executor-cores 1 -e "
  use ipsos;
  set spark.sql.crossJoin.enabled=true;
  insert overwrite table  asc_baseline
select kpi,a_level,
case 
when a_level=b_level then concat('ALL-',a_level) 
when b_level='全新科鲁兹' then '科鲁兹新款'
when b_level='老款科鲁兹' then '科鲁兹老款'
when b_level='经典科鲁兹' then '科鲁兹经典'
when b_level='新赛欧' then '赛欧新款'
when b_level='老款赛欧' then '赛欧老款'
else b_level end as b_level,
age_type,
mon,
sum(case when asc_code='2200124' then num else 0 end) as ynlh_mon,
avg(num) as bzz_mon
 from kpi_b_agetype  
where asc_code in('2200124','2200162','2200318','2200053','2200903','2200605','2200293','2200345','2200241','2200306','2200092','2200267','2200573','2200555','2200088')
group by kpi,
a_level,
case 
when a_level=b_level then concat('ALL-',a_level) 
when b_level='全新科鲁兹' then '科鲁兹新款'
when b_level='老款科鲁兹' then '科鲁兹老款'
when b_level='经典科鲁兹' then '科鲁兹经典'
when b_level='新赛欧' then '赛欧新款'
when b_level='老款赛欧' then '赛欧老款'
else b_level end ,
age_type,
mon
union all 
select kpi,a_level,
case 
when a_level=b_level then concat('ALL-',a_level) 
when b_level='全新科鲁兹' then '科鲁兹新款'
when b_level='老款科鲁兹' then '科鲁兹老款'
when b_level='经典科鲁兹' then '科鲁兹经典'
when b_level='新赛欧' then '赛欧新款'
when b_level='老款赛欧' then '赛欧老款'
else b_level end as b_level,
bnbw as age_type,
mon,
sum(case when asc_code='2200124' then num else 0 end) as ynlh_mon,
avg(num) as bzz_mon
 from kpi_b_bnbw  
where asc_code in('2200124','2200162','2200318','2200053','2200903','2200605','2200293','2200345','2200241','2200306','2200092','2200267','2200573','2200555','2200088')
group by kpi,
a_level,
case 
when a_level=b_level then concat('ALL-',a_level) 
when b_level='全新科鲁兹' then '科鲁兹新款'
when b_level='老款科鲁兹' then '科鲁兹老款'
when b_level='经典科鲁兹' then '科鲁兹经典'
when b_level='新赛欧' then '赛欧新款'
when b_level='老款赛欧' then '赛欧老款'
else b_level end,
bnbw,
mon
union all
select kpi,
a_level,
concat('ALL-',a_level) as b_level,
age_type,
mon,
sum(case when asc_code='2200124' then num else 0 end) as ynlh_mon,
avg(num) as bzz_mon
 from kpi_a_agetype  
where asc_code in('2200124','2200162','2200318','2200053','2200903','2200605','2200293','2200345','2200241','2200306','2200092','2200267','2200573','2200555','2200088')
and a_level in('科鲁兹','赛欧','迈锐宝')
group by kpi,
a_level,
age_type,
mon
union all
select kpi,
a_level,
concat('ALL-',a_level) as b_level,
bnbw as age_type,
mon,
sum(case when asc_code='2200124' then num else 0 end) as ynlh_mon,
avg(num) as bzz_mon
 from kpi_a_bnbw  
where asc_code in('2200124','2200162','2200318','2200053','2200903','2200605','2200293','2200345','2200241','2200306','2200092','2200267','2200573','2200555','2200088')
and a_level in('科鲁兹','赛欧','迈锐宝')
group by kpi,
a_level,
bnbw,
mon
union all 
select kpi,
'ALL' AS a_level,
'ALL' as b_level,
'ALL' as age_type,
mon,
sum(case when asc_code='2200124' then num else 0 end) as ynlh_mon,
avg(num) as bzz_mon
from kpi_total
where asc_code in('2200124','2200162','2200318','2200053','2200903','2200605','2200293','2200345','2200241','2200306','2200092','2200267','2200573','2200555','2200088')
group by kpi,
mon
union all 
select kpi,
a_level,
case 
when a_level=b_level then concat('ALL-',a_level) 
when b_level='全新科鲁兹' then '科鲁兹新款'
when b_level='老款科鲁兹' then '科鲁兹老款'
when b_level='经典科鲁兹' then '科鲁兹经典'
when b_level='新赛欧' then '赛欧新款'
when b_level='老款赛欧' then '赛欧老款'
else b_level end as b_level,
'ALL' as age_type,
mon,
sum(case when asc_code='2200124' then num else 0 end) as ynlh_mon,
avg(num) as bzz_mon
 from kpi_b_level
where asc_code in('2200124','2200162','2200318','2200053','2200903','2200605','2200293','2200345','2200241','2200306','2200092','2200267','2200573','2200555','2200088')
group by kpi,
a_level,
case 
when a_level=b_level then concat('ALL-',a_level) 
when b_level='全新科鲁兹' then '科鲁兹新款'
when b_level='老款科鲁兹' then '科鲁兹老款'
when b_level='经典科鲁兹' then '科鲁兹经典'
when b_level='新赛欧' then '赛欧新款'
when b_level='老款赛欧' then '赛欧老款'
else b_level end,
mon
union all 
select kpi,
a_level ,
concat('ALL-',a_level) as b_level,
'ALL' as age_type,
mon,
sum(case when asc_code='2200124' then num else 0 end) as ynlh_mon,
avg(num) as bzz_mon
 from kpi_a_level  
where asc_code in('2200124','2200162','2200318','2200053','2200903','2200605','2200293','2200345','2200241','2200306','2200092','2200267','2200573','2200555','2200088')
and a_level in('科鲁兹','赛欧','迈锐宝')
and mon_p='2010-04_2016-12'
group by kpi,
a_level,
mon
union all 
select kpi,
'ALL' as a_level,
'ALL' as b_level,
age_type,
mon,
sum(case when asc_code='2200124' then num else 0 end) as ynlh_mon,
avg(num) as bzz_mon
from kpi_agetype
where asc_code in('2200124','2200162','2200318','2200053','2200903','2200605','2200293','2200345','2200241','2200306','2200092','2200267','2200573','2200555','2200088')
group by kpi,
age_type,
mon
union all 
select kpi,
'ALL' as a_level,
'ALL' as b_level,
bnbw as age_type,
mon,
sum(case when asc_code='2200124' then num else 0 end) as ynlh_mon,
avg(num) as bzz_mon
from kpi_bnbw
where asc_code in('2200124','2200162','2200318','2200053','2200903','2200605','2200293','2200345','2200241','2200306','2200092','2200267','2200573','2200555','2200088')
group by kpi,
bnbw,
mon
 " > ../log/asc_baseline 2>&1 


spark-sql --master yarn-client --driver-memory 3g --executor-memory 3g --num-executors 20 --executor-cores 1 -e "
  use ipsos;
  set spark.sql.crossJoin.enabled=true;
  select distinct m.a_level,m.b_level,m.age_type,n.KPI,n.KPI_Name,m.mon,m.own_num,m.quar_own_num,m.year_own_num,m.base_num,m.quar_base_num,m.year_base_num from (
select s.kpi,
s.a_level,
s.b_level,
s.age_type,
s.mon,
s.own_num,
t1.own_num as quar_own_num,
t2.own_num as year_own_num,
s.base_num,
t1.base_num as quar_base_num,
t2.base_num as year_base_num
 from asc_baseline s
left join 
(
select q.kpi,q.a_level,q.b_level,q.age_type,substr(m.mon_label,1,7) as mon,q.own_num,q.base_num
 from date_label m,(
select t.kpi,
t.a_level,
t.b_level,
t.age_type,
s.quarter_label,
avg(t.own_num) as own_num,
avg(t.base_num) as base_num 
from date_label s, asc_baseline t 
where substr(s.mon_label,1,7)=t.mon
group by t.kpi,t.a_level,
t.b_level,
t.age_type,
s.quarter_label
) q where m.quarter_label=q.quarter_label
) t1 on 
s.kpi=t1.kpi
and s.a_level=t1.a_level
and s.b_level=t1.b_level
and s.age_type=t1.age_type
and s.mon=t1.mon
left join
(
select q.kpi,q.a_level,q.b_level,q.age_type,substr(m.mon_label,1,7) as mon,q.own_num,q.base_num
 from date_label m,(
select t.kpi,
t.a_level,
t.b_level,
t.age_type,
s.year_label,
avg(t.own_num) as own_num,
avg(t.base_num) as base_num 
from date_label s, asc_baseline t 
where substr(s.mon_label,1,7)=t.mon
group by t.kpi,
t.a_level,
t.b_level,
t.age_type,
s.year_label
) q where m.year_label=q.year_label
) t2 on 
s.kpi=t2.kpi
and s.a_level=t2.a_level
and s.b_level=t2.b_level
and s.age_type=t2.age_type
and s.mon=t2.mon
) m join base_kpi n on m.kpi=n.kpi_kpi

" >xjy_res 2>xjy.log 
