#!/bin/bash
source /etc/profile

recordFile=../log/time_record_delete
pub_db=ori
database=ori
function flow_kpi(){
 #开始时间
 date1=$(date  +"%Y-%m-%d %H:%M:%S")
 time1=$(date +"%s")
  hive -e "
  use ${database};
add jar /home/${database}/general/bin/transform-date.jar;
CREATE TEMPORARY FUNCTION mon_diff AS 'com.gm.transformDate.MonDiff';
CREATE TEMPORARY FUNCTION tran_date AS 'com.gm.transformDate.TransformDate';
set hive.exec.dynamic.partition.mode=nonstrict;
set spark.sql.crossJoin.enabled=true;

 select asc_code,substr(tran_date(order_balance_date),1,7) date,delete_status,count(1) num
 from delete_counter
 group by asc_code,substr(tran_date(order_balance_date),1,7), delete_status 
 having delete_status is not null
 order by asc_code,date,delete_status;

	
" > ../log/delete.log 2>&1
  echo "delete.log  $?" >> ../log/res_test
#结束时间
time2=$(date +"%s")
date2=$(date +"%Y-%m-%d %H:%M:%S")

#计算时间
let time=time2-time1
let min_time=time/3600
echo -e "delete"+min_time >>${recordFile}
echo "----------------------------" >>${recordFile}
 
}
flow_kpi


