#!/bin/bash
#本脚本要求在 ori用户下执行
#零件编号新增查询
database="ori"
conf_dir="/home/ori/general/bin/conf"
function part_check() {
spark-sql --master yarn --driver-memory 6g --executor-memory 6g --num-executors 20 --executor-cores 1 -e "
use ${database};
select DISTINCT *
from
(select t1.part_number as pn1,t2.part_num as pn2
from part t1 left join
(select distinct part_num
from
(select part_num
from enclosure
union all
select part_num
from engine_oil
union all
select part_num
from high_flow_parts
union all
select part_num
from maintnance)) t2
on t1.part_number=t2.part_num) t3
where pn2 is null;
" > part_no_new
}

#品牌车型车型 label_order check
function car_order_check() {
spark-sql --master yarn --driver-memory 6g --executor-memory 6g --num-executors 20 --executor-cores 1 -e "
use ${database};
select distinct makes,series,model
from label_order
" > mark_tmp
cat ${conf_dir}/CHE | awk -F "\t" '{print $1"\t"$2"\t"$3}' | sort -u >> mark_tmp
cat mark_tmp |grep -v '\t'|grep -v java | sort |uniq -u > mark_new
}


#doss 车系车型
function car_doss_check() {
spark-sql --master yarn --driver-memory 6g --executor-memory 6g --num-executors 20 --executor-cores 1 -e "
use ${database};
select distinct makes,series
from label_doss
" > mark_doss_tmp
cat ${conf_dir}/mark_doss |awk -F "\t" '{print $1"\t"$2}' >>mark_doss_tmp
cat mark_doss_tmp |grep -v '\t'|grep -v java | sort |uniq -u > mark_doss_new
}


#flow表的辅助表制作
function flow_dim(){
#sexual
spark-sql -e "
use ${database};
select t1.*
from
(select distinct trim(sexual) as sexual
from flow_tmp) t1 left join (select distinct trim(sexual) as sexual from sexual) t2
on t1.sexual=t2.sexual
where t2.sexual is null;
" | grep -v '\t' | grep -v java >sexual_new

#province
spark-sql -e "
use ${database};
select t1.*
from
(select distinct trim(province) as province
from flow_tmp) t1 left join (select distinct trim(province) as province from province) t2
on t1.province=t2.province
where t2.province is null;
" | grep -v '\t' | grep -v java >province_new

#city
spark-sql -e "
use ${database};
select t1.*
from
(select distinct trim(city) as city
from flow_tmp) t1 left join (select distinct trim(city) as city from city) t2
on t1.city=t2.city
where t2.city is null;
" | grep -v '\t' | grep -v java >city_new

#name
spark-sql -e "
use ${database};
select t1.*
from
(select distinct trim(name) as name
from flow_tmp) t1 left join (select distinct trim(name) as name from name) t2
on t1.name=t2.name
where t2.name is null;
" | grep -v '\t' | grep -v java >name_new

#primary_classification
spark-sql -e "
use ${database};
select t1.*
from
(select distinct trim(primary_classification) as primary_classification
from flow_tmp) t1 left join (select distinct trim(primary_classification) as primary_classification from primary_classification) t2
on t1.primary_classification=t2.primary_classification
where t2.primary_classification is null;
" | grep -v '\t' | grep -v java >primary_classification_new

#second_level_classification
spark-sql -e "
use ${database};
select t1.*
from
(select distinct trim(second_level_classification) as second_level_classification
from flow_tmp) t1 left join (select distinct trim(second_level_classification) as second_level_classification from second_level_classification) t2
on t1.second_level_classification=t2.second_level_classification
where t2.second_level_classification is null;
" | grep -v '\t' | grep -v java >second_level_classification_new

#distributor
spark-sql -e "
use ${database};
set spark.sql.crossJoin.enabled = true;
select  t1.chcode,t1.asccode,t1.asc
from
 (select distinct trim(ch_code) as chcode,trim(asc_code) as asccode,trim(asc) as asc
 from claim_uniq) t1
left join (select distinct chcode,asccode,asc from distributor) t2
on t1.asccode=t2.asccode and t1.chcode=t2.chcode and t1.asc=t2.asc
where t2.chcode is null or t2.asccode is null or t2.asc is null;
" | grep -v '\t' | grep -v java >distributor_new
}


