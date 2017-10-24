#coding=utf-8
import sys
reload(sys)
sys.setdefaultencoding('utf8')
from pyspark import SparkConf, SparkContext
from pyspark.sql import *
sconf = SparkConf().setAppName("ipsos-kpi")
sc = SparkContext(conf=sconf)
spark = HiveContext(sc)
spark.sql("add jar /data2/u_lx_tst2/test/hive_udf_ideal_20140307.jar");
spark.sql("create temporary function getBase64 as 'com.ideal.hive.udf.BASE64DE'")
spark.sql("use u_lx_tst2")
#----kpi_1(进站台次_total)
def kpi_total_kpi_1(begin_day, end_day):
    line = """  insert overwrite table  kpi_total partition(mon_p='${mon_p}',kpi='kpi_1')
  select ----'1' as kpi,
  substr(tran_date(order_balance_date),1,7) as mon,
  count(distinct vin,tran_date(order_balance_date)) as cnt,
  asc_code
   from label_order  
  where MAINT_TYPE1<>'删除'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(order_balance_date),1,7),asc_code;"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    spark.sql(line)
function run_all() {
 kpi_total_kpi_1 $1 $2
 
}
