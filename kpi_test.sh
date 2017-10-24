#!/bin/bash
source /etc/profile

#----kpi_5.52(养护品产值_燃油养护_total)
function kpi_total_kpi_5_52(){
 begin_date=$1
 end_date=$2
 if [ ${begin_date} = ${end_date} ] 
 then
  mon_p=${end_date}
 else
  mon_p=${begin_date}_${end_date}
 fi
 kpi="kpi_5_52"
 hive -e "
  use ipsos;
  add jar /home/ipsos/general/bin/transform-date.jar;
  CREATE TEMPORARY FUNCTION tran_date AS 'com.gm.transformDate.TransformDate';
  CREATE TEMPORARY FUNCTION mon_diff AS 'com.gm.transformDate.MonDiff';
  alter table kpi_total drop if exists partition(mon_p='${mon_p}',kpi='${kpi}');
  alter table kpi_total add partition (mon_p='${mon_p}',kpi='${kpi}') location '${mon_p}/${kpi}';
explain  insert overwrite table  kpi_total partition(mon_p='${mon_p}',kpi='kpi_5_52')
  select substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,maintnance y where x.part_number=y.part_num and y.type='燃油养护') m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code; " > ../log/kpi_total_${mon_p}_${kpi} 2>&1
  echo "kpi_total_${mon_p}_${kpi}  $?" >> ../log/res
}

function run_all() {
 kpi_total_kpi_5_52 $1 $2
 
}
