nohup spark-sql --master yarn-client --driver-memory 6g --executor-memory 6g --num-executors 20 --executor-cores 1 -e "
use ipsos;
set hive.exec.dynamic.partition.mode=nonstrict;
set spark.sql.crossJoin.enabled=true;
INSERT overwrite table nine_merge
partition (reg)
select u.*,r.area
from
(select m.a_level,m.b_level,m.age_type,m.kpi,n.kpi_name,m.mon,m.asc_code as asc_c,m.own_num,'0' quar_own_num,'0' as year_own_num,'0' as base_num,'0' as quar_base_own_num,'0' as year_base_num
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
 from kpi_b_agetype_chev_final2
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
 from kpi_b_bnbw_chev_final2
union all
select kpi,
a_level,
concat('ALL-',a_level) as b_level,
age_type,
mon,
num as own_num,
asc_code
 from kpi_a_agetype_chev_final2
where a_level in('科鲁兹','赛欧','迈锐宝')
union all
select kpi,
a_level,
concat('ALL-',a_level) as b_level,
bnbw as age_type,
mon,
num as own_num,
asc_code
 from kpi_a_bnbw_chev_final2
where a_level in('科鲁兹','赛欧','迈锐宝')
union all
select kpi,
'ALL' AS a_level,
'ALL' as b_level,
'ALL' as age_type,
mon,
num as own_num,
asc_code
from kpi_total_chev_final2
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
 from kpi_b_level_chev_final2
union all
select kpi,
a_level ,
concat('ALL-',a_level) as b_level,
'ALL' as age_type,
mon,
num as own_num,
asc_code
 from kpi_a_level_chev_final2
where a_level in('科鲁兹','赛欧','迈锐宝')
union all
select kpi,
'ALL' as a_level,
'ALL' as b_level,
age_type,
mon,
num as own_num,
asc_code
from kpi_agetype_chev_final2
union all
select kpi,
'ALL' as a_level,
'ALL' as b_level,
bnbw as age_type,
mon,
num as own_num,
asc_code
from kpi_bnbw_chev_final2) m join base_kpi n on m.kpi=n.kpi_kpi) u join region r on u.asc_c=r.asc_code;" >log2 2>&1 &


spark-sql --master yarn --driver-memory 6g --executor-memory 6g --num-executors 20 --executor-cores 1 -e "
use ipsos;
set spark.sql.crossJoin.enabled=true;
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table nine_merge
partition(reg)
select n.*,r.area
from nine_merge2 n inner join region r on n.asc_code=r.asc_code
" > log 2>&1 &