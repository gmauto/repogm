use ipsos_test1;
  add jar /home/ipsos_test1/general/bin/transform-date.jar;
  CREATE TEMPORARY FUNCTION tran_date AS 'com.gm.transformDate.TransformDate';
  CREATE TEMPORARY FUNCTION mon_diff AS 'com.gm.transformDate.MonDiff';
  set spark.sql.crossJoin.enabled=true;



explain insert overwrite table kpi_total_tmp_8areas_k1 partition(mon_p='2010-01',kpi='kpi_3_42')
  select
  substr(tran_date(s.order_balance_date),1,7) as mon,
  count(distinct case when tasc_code is not null then s.vin else null end) as num,1.0 as den ,
  tasc_code as asc_code
  from

  (select s.*,t.asc_code as tasc_code
  from ori.label_order s
  left join (
  select distinct t.asc_code,t.order_number,m1.month from ori.part t join ori.maintnance x
  join (select month,version from ori.mapping where fact_table='label_order' and dim_table='maintnance') m1
  on t.part_number=x.part_num
  and x.version=m1.version
  and substr(tran_date(t.order_balance_date),1,7)=m1.month
  where x.type='燃油养护'
  ) t
  on s.order_number=t.order_number
  and s.asc_code=t.asc_code
  where substr(tran_date(s.order_balance_date),1,7)>='2010-01'
  and substr(tran_date(s.order_balance_date),1,7)<='2018-01'
  and s.MAINT_TYPE1<>'删除' )s


  group by s.asc_code,substr(tran_date(s.order_balance_date),1,7);