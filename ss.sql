
use ipsos_test1;
set spark.sql.crossJoin.enabled=true;
select m.a_level,m.b_level,m.age_type,m.kpi,n.kpi_name,m.mon,m.asc_code,
own_num
from

(

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
num as own_num,
asc_code
from kpi_b_agetype_8areas
where region='everymonth_whole_country'

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
num as own_num,
asc_code
from kpi_b_bnbw_8areas
where region='everymonth_whole_country'

union all

select kpi,
a_level,
concat('ALL-',a_level) as b_level,
age_type,
mon,
num as own_num,
asc_code
from kpi_a_agetype_8areas  
where a_level in('科鲁兹','赛欧','迈锐宝','科帕奇')
and region='everymonth_whole_country'

union all

select kpi,
a_level,
concat('ALL-',a_level) as b_level,
bnbw as age_type,
mon,
num as own_num,
asc_code
from kpi_a_bnbw_8areas  
where a_level in('科鲁兹','赛欧','迈锐宝','科帕奇')
and region='everymonth_whole_country'

union all

select kpi,
'ALL' AS a_level,
'ALL' as b_level,
'ALL' as age_type,
mon,
num as own_num,
asc_code
from kpi_total_8areas
where region='everymonth_whole_country'

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
num as own_num,
asc_code
from kpi_b_level_8areas
where region='everymonth_whole_country'

union all

select kpi,
a_level ,
concat('ALL-',a_level) as b_level,
'ALL' as age_type,
mon,
num as own_num,
asc_code
from kpi_a_level_8areas  
where a_level in('科鲁兹','赛欧','迈锐宝','科帕奇')
and region='everymonth_whole_country'

union all

select kpi,
'ALL' as a_level,
'ALL' as b_level,
age_type,
mon,
num as own_num,
asc_code
from kpi_agetype_8areas
where region='everymonth_whole_country'
union all 
select kpi,
'ALL' as a_level,
'ALL' as b_level,
bnbw as age_type,
mon,
num as own_num,
asc_code
from kpi_bnbw_8areas
where region='everymonth_whole_country') m

join base_kpi n on m.kpi=n.kpi_kpi;

