# coding=utf-8
import traceback
import datetime
from pyspark import SparkConf, SparkContext, StorageLevel
from pyspark.sql import *

sconf = SparkConf().setAppName("gather")
sc = SparkContext(conf=sconf)
spark = HiveContext(sc)
spark.sql("use ipsos_test1")
spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
spark.sql("set spark.sql.crossJoin.enabled=true")

# own_month 部分
own_month = """select m.a_level,m.b_level,m.age_type,m.kpi,n.kpi_name,m.mon,m.asc_code,
own_num,a.region,tier
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
where region='everymonth_asc'
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
where region='everymonth_asc'
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
and region='everymonth_asc'
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
and region='everymonth_asc'
union all
select kpi,
'ALL' AS a_level,
'ALL' as b_level,
'ALL' as age_type,
mon,
num as own_num,
asc_code
from kpi_total_8areas
where region='everymonth_asc'
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
where region='everymonth_asc'
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
and region='everymonth_asc'
union all
select kpi,
'ALL' as a_level,
'ALL' as b_level,
age_type,
mon,
num as own_num,
asc_code
from kpi_agetype_8areas
where region='everymonth_asc'
union all
select kpi,
'ALL' as a_level,
'ALL' as b_level,
bnbw as age_type,
mon,
num as own_num,
asc_code
from kpi_bnbw_8areas
where region='everymonth_asc') m
join base_kpi n on m.kpi=n.kpi_kpi
 join ori.asc_reg_tier a on a.asc_code=m.asc_code
"""
own_month_sql = spark.sql(own_month)
own_month_sql.registerTempTable("own_month")
own_month_sql.persist(storageLevel=StorageLevel.DISK_ONLY_2)

# own_quar
own_quarter = """select m.a_level,m.b_level,m.age_type,m.kpi,n.kpi_name,s.mon,m.asc_code,
quar_own_num,region,tier
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
num as quar_own_num,
asc_code
from kpi_b_agetype_8areas
where region='quarter_asc'
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
num as quar_own_num,
asc_code
from kpi_b_bnbw_8areas
where region='quarter_asc'
union all
select kpi,
a_level,
concat('ALL-',a_level) as b_level,
age_type,
mon,
num as quar_own_num,
asc_code
from kpi_a_agetype_8areas
where a_level in('科鲁兹','赛欧','迈锐宝','科帕奇')
and region='quarter_asc'
union all
select kpi,
a_level,
concat('ALL-',a_level) as b_level,
bnbw as age_type,
mon,
num as quar_own_num,
asc_code
from kpi_a_bnbw_8areas
where a_level in('科鲁兹','赛欧','迈锐宝','科帕奇')
and region='quarter_asc'
union all
select kpi,
'ALL' AS a_level,
'ALL' as b_level,
'ALL' as age_type,
mon,
num as quar_own_num,
asc_code
from kpi_total_8areas
where region='quarter_asc'
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
num as quar_own_num,
asc_code
from kpi_b_level_8areas
where region='quarter_asc'
union all
select kpi,
a_level ,
concat('ALL-',a_level) as b_level,
'ALL' as age_type,
mon,
num as quar_own_num,
asc_code
from kpi_a_level_8areas
where a_level in('科鲁兹','赛欧','迈锐宝','科帕奇')
and region='quarter_asc'
union all
select kpi,
'ALL' as a_level,
'ALL' as b_level,
age_type,
mon,
num as quar_own_num,
asc_code
from kpi_agetype_8areas
where region='quarter_asc'
union all
select kpi,
'ALL' as a_level,
'ALL' as b_level,
bnbw as age_type,
mon,
num as quar_own_num,
asc_code
from kpi_bnbw_8areas
where region='quarter_asc') m
join base_kpi n on m.kpi=n.kpi_kpi
 join ori.mon_mapping s on s.time=m.mon and version='quarter'
 join ori.asc_reg_tier a on a.asc_code=m.asc_code"""

own_quarter_sql = spark.sql(own_quarter)
own_quarter_sql.registerTempTable("own_quarter")
own_quarter_sql.persist(StorageLevel.DISK_ONLY_2)

# own_year
own_year = """select m.a_level,m.b_level,m.age_type,m.kpi,n.kpi_name,s.mon,m.asc_code,
year_own_num,region,tier
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
num as year_own_num,
asc_code
from kpi_b_agetype_8areas
where region='year_asc'
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
num as year_own_num,
asc_code
from kpi_b_bnbw_8areas
where region='year_asc'
union all
select kpi,
a_level,
concat('ALL-',a_level) as b_level,
age_type,
mon,
num as year_own_num,
asc_code
from kpi_a_agetype_8areas
where a_level in('科鲁兹','赛欧','迈锐宝','科帕奇')
and region='year_asc'
union all
select kpi,
a_level,
concat('ALL-',a_level) as b_level,
bnbw as age_type,
mon,
num as year_own_num,
asc_code
from kpi_a_bnbw_8areas
where a_level in('科鲁兹','赛欧','迈锐宝','科帕奇')
and region='year_asc'
union all
select kpi,
'ALL' AS a_level,
'ALL' as b_level,
'ALL' as age_type,
mon,
num as year_own_num,
asc_code
from kpi_total_8areas
where region='year_asc'
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
num as year_own_num,
asc_code
from kpi_b_level_8areas
where region='year_asc'
union all
select kpi,
a_level ,
concat('ALL-',a_level) as b_level,
'ALL' as age_type,
mon,
num as year_own_num,
asc_code
from kpi_a_level_8areas
where a_level in('科鲁兹','赛欧','迈锐宝','科帕奇')
and region='year_asc'
union all
select kpi,
'ALL' as a_level,
'ALL' as b_level,
age_type,
mon,
num as year_own_num,
asc_code
from kpi_agetype_8areas
where region='year_asc'
union all
select kpi,
'ALL' as a_level,
'ALL' as b_level,
bnbw as age_type,
mon,
num as year_own_num,
asc_code
from kpi_bnbw_8areas
where region='year_asc') m
join base_kpi n on m.kpi=n.kpi_kpi
 join ori.mon_mapping s on s.time=m.mon and version='year'
 join ori.asc_reg_tier a on a.asc_code=m.asc_code"""

own_year_sql = spark.sql(own_year)
own_year_sql.registerTempTable("own_year")
own_year_sql.persist(StorageLevel.DISK_ONLY_2)

# base_month
base_month = """select m.a_level,m.b_level,m.age_type,m.kpi,n.kpi_name,m.mon,
avg(own_num) base_num
,a.region,tier
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
where region='everymonth_asc'
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
where region='everymonth_asc'
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
and region='everymonth_asc'
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
and region='everymonth_asc'
union all
select kpi,
'ALL' AS a_level,
'ALL' as b_level,
'ALL' as age_type,
mon,
num as own_num,
asc_code
from kpi_total_8areas
where region='everymonth_asc'
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
where region='everymonth_asc'
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
and region='everymonth_asc'
union all
select kpi,
'ALL' as a_level,
'ALL' as b_level,
age_type,
mon,
num as own_num,
asc_code
from kpi_agetype_8areas
where region='everymonth_asc'
union all
select kpi,
'ALL' as a_level,
'ALL' as b_level,
bnbw as age_type,
mon,
num as own_num,
asc_code
from kpi_bnbw_8areas
where region='everymonth_asc') m
join base_kpi n on m.kpi=n.kpi_kpi
 join ori.asc_reg_tier a on a.asc_code=m.asc_code
group by m.a_level,m.b_level,m.age_type,m.kpi,n.kpi_name,m.mon,a.region,tier"""

base_month_sql = spark.sql(base_month)
base_month_sql.registerTempTable("base_month")
base_month_sql.persist(StorageLevel.DISK_ONLY_2)
# base_quarter
base_quarter = """select m.a_level,m.b_level,m.age_type,m.kpi,n.kpi_name,s.mon,
avg(quar_own_num)  quar_base_own_num
,region,tier
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
num as quar_own_num,
asc_code
from kpi_b_agetype_8areas
where region='quarter_asc'
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
num as quar_own_num,
asc_code
from kpi_b_bnbw_8areas
where region='quarter_asc'
union all
select kpi,
a_level,
concat('ALL-',a_level) as b_level,
age_type,
mon,
num as quar_own_num,
asc_code
from kpi_a_agetype_8areas
where a_level in('科鲁兹','赛欧','迈锐宝','科帕奇')
and region='quarter_asc'
union all
select kpi,
a_level,
concat('ALL-',a_level) as b_level,
bnbw as age_type,
mon,
num as quar_own_num,
asc_code
from kpi_a_bnbw_8areas
where a_level in('科鲁兹','赛欧','迈锐宝','科帕奇')
and region='quarter_asc'
union all
select kpi,
'ALL' AS a_level,
'ALL' as b_level,
'ALL' as age_type,
mon,
num as quar_own_num,
asc_code
from kpi_total_8areas
where region='quarter_asc'
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
num as quar_own_num,
asc_code
from kpi_b_level_8areas
where region='quarter_asc'
union all
select kpi,
a_level ,
concat('ALL-',a_level) as b_level,
'ALL' as age_type,
mon,
num as quar_own_num,
asc_code
from kpi_a_level_8areas
where a_level in('科鲁兹','赛欧','迈锐宝','科帕奇')
and region='quarter_asc'
union all
select kpi,
'ALL' as a_level,
'ALL' as b_level,
age_type,
mon,
num as quar_own_num,
asc_code
from kpi_agetype_8areas
where region='quarter_asc'
union all
select kpi,
'ALL' as a_level,
'ALL' as b_level,
bnbw as age_type,
mon,
num as quar_own_num,
asc_code
from kpi_bnbw_8areas
where region='quarter_asc') m
join base_kpi n on m.kpi=n.kpi_kpi
 join ori.mon_mapping s on s.time=m.mon and version='quarter'
 join ori.asc_reg_tier a on a.asc_code=m.asc_code
group by m.a_level,m.b_level,m.age_type,m.kpi,n.kpi_name,s.mon,a.region,tier"""

base_quarter_sql = spark.sql(base_quarter)
base_quarter_sql.registerTempTable("base_quarter")
base_quarter_sql.persist(StorageLevel.DISK_ONLY_2)
# base_year
base_year = """select m.a_level,m.b_level,m.age_type,m.kpi,n.kpi_name,s.mon,
avg(year_own_num)  year_base_num
,region,tier
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
num as year_own_num,
asc_code
from kpi_b_agetype_8areas
where region='year_asc'
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
num as year_own_num,
asc_code
from kpi_b_bnbw_8areas
where region='year_asc'
union all
select kpi,
a_level,
concat('ALL-',a_level) as b_level,
age_type,
mon,
num as year_own_num,
asc_code
from kpi_a_agetype_8areas
where a_level in('科鲁兹','赛欧','迈锐宝','科帕奇')
and region='year_asc'
union all
select kpi,
a_level,
concat('ALL-',a_level) as b_level,
bnbw as age_type,
mon,
num as year_own_num,
asc_code
from kpi_a_bnbw_8areas
where a_level in('科鲁兹','赛欧','迈锐宝','科帕奇')
and region='year_asc'
union all
select kpi,
'ALL' AS a_level,
'ALL' as b_level,
'ALL' as age_type,
mon,
num as year_own_num,
asc_code
from kpi_total_8areas
where region='year_asc'
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
num as year_own_num,
asc_code
from kpi_b_level_8areas
where region='year_asc'
union all
select kpi,
a_level ,
concat('ALL-',a_level) as b_level,
'ALL' as age_type,
mon,
num as year_own_num,
asc_code
from kpi_a_level_8areas
where a_level in('科鲁兹','赛欧','迈锐宝','科帕奇')
and region='year_asc'
union all
select kpi,
'ALL' as a_level,
'ALL' as b_level,
age_type,
mon,
num as year_own_num,
asc_code
from kpi_agetype_8areas
where region='year_asc'
union all
select kpi,
'ALL' as a_level,
'ALL' as b_level,
bnbw as age_type,
mon,
num as year_own_num,
asc_code
from kpi_bnbw_8areas
where region='year_asc') m
join  base_kpi n on m.kpi=n.kpi_kpi
 join ori.mon_mapping s on s.time=m.mon and version='year'
 join ori.asc_reg_tier a on a.asc_code=m.asc_code
group by m.a_level,m.b_level,m.age_type,m.kpi,n.kpi_name,s.mon,region,tier"""

base_year_sql = spark.sql(base_year)
base_year_sql.registerTempTable("base_year")
base_year_sql.persist(StorageLevel.DISK_ONLY_2)

gather = """insert overwrite table hebingbiao partition(region)
select h1.a_level,h1.b_level,h1.age_type,h1.kpi,h1.kpi_name,h1.mon,
h1.own_num,
h2.quar_own_num,
h3.year_own_num,
h4.base_num,
h5.quar_base_own_num,
h6.year_base_num,
h1.asc_code,
h1.tier,
h1.region
from own_month h1

------------------------------------------季度
 join own_quarter h2 on h1.kpi=h2.kpi
and h1.a_level=h2.a_level
and h1.b_level=h2.b_level
and h1.age_type=h2.age_type
and h1.mon=h2.mon
and h1.asc_code=h2.asc_code
and h1.region=h2.region
and h1.tier=h2.tier

---------------------------年度
join own_year h3 on h1.kpi=h2.kpi
and h1.a_level=h3.a_level
and h1.b_level=h3.b_level
and h1.age_type=h3.age_type
and h1.mon=h3.mon
and h1.asc_code=h3.asc_code
and h1.region=h3.region
and h1.tier=h3.tier


-------------标准值月
 join base_month h4
on h1.kpi=h4.kpi
and h1.a_level=h4.a_level
and h1.b_level=h4.b_level
and h1.age_type=h4.age_type
and h1.mon=h4.mon
and h1.region=h4.region
and h1.tier=h4.tier

------------------------------------------标准值季度
join base_quarter h5 on h1.kpi=h5.kpi
and h1.a_level=h5.a_level
and h1.b_level=h5.b_level
and h1.age_type=h5.age_type
and h1.mon=h5.mon
and h1.region=h5.region
and h1.tier=h5.tier

---------------------------标准值年度
join base_year h6 on h1.kpi=h6.kpi
and h1.a_level=h6.a_level
and h1.b_level=h6.b_level
and h1.age_type=h6.age_type
and h1.mon=h6.mon
and h1.region=h6.region
and h1.tier=h6.tier
where h1.tier is not null
"""
#合并最终结果
spark.sql(gather)
