# coding=utf-8
import sys
import traceback

reload(sys)
sys.setdefaultencoding('utf8')
from pyspark import SparkConf, SparkContext
from pyspark.sql import *

sconf = SparkConf().setAppName("ipsos-kpi")
sc = SparkContext(conf=sconf)
sc.setLogLevel("WARN")
spark = HiveContext(sc)
spark.sql("add jar /home/u_ipsos/general/bin/transform-date.jar")
spark.sql("CREATE TEMPORARY FUNCTION mon_diff AS 'com.gm.transformDate.MonDiff'")
spark.sql("CREATE TEMPORARY FUNCTION tran_date AS 'com.gm.transformDate.TransformDate'")
spark.sql("use dl_ipsos")


# ----kpi_1(进站台次_total)
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
  group by substr(tran_date(order_balance_date),1,7),asc_code"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1(进站台次_一级)
def kpi_a_level_kpi_1(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_level partition(mon_p='${mon_p}',kpi='kpi_1')
  select ----'1' as kpi,
  primary_classification,
  substr(tran_date(order_balance_date),1,7) as mon,
  count(distinct vin,tran_date(order_balance_date)) as cnt,
  asc_code
   from label_order  
  where MAINT_TYPE1<>'删除'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(order_balance_date),1,7),asc_code,primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1(进站台次_二级)
def kpi_b_level_kpi_1(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_level partition(mon_p='${mon_p}',kpi='kpi_1')
  select ----'1' as kpi,
  primary_classification,
  second_level_classification,
  substr(tran_date(order_balance_date),1,7) as mon,
  count(distinct vin,tran_date(order_balance_date)) as cnt,
  asc_code
   from label_order  
  where MAINT_TYPE1<>'删除'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(order_balance_date),1,7),asc_code,primary_classification,second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1(进站台次_车龄)
def kpi_agetype_kpi_1(begin_day, end_day):
    line = """  insert overwrite table  kpi_agetype partition(mon_p='${mon_p}',kpi='kpi_1')
  select ----'1' as kpi,
  age_type,
  substr(tran_date(order_balance_date),1,7) as mon,
  count(distinct vin,tran_date(order_balance_date)) as cnt,
  asc_code
   from label_order  
  where MAINT_TYPE1<>'删除'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(order_balance_date),1,7),asc_code,age_type"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1(进站台次_保内外)
def kpi_bnbw_kpi_1(begin_day, end_day):
    line = """  insert overwrite table  kpi_bnbw partition(mon_p='${mon_p}',kpi='kpi_1')
  select ----'1' as kpi,
  bn,
  substr(tran_date(order_balance_date),1,7) as mon,
  count(distinct vin,tran_date(order_balance_date)) as cnt,
  asc_code
   from label_order  
  where MAINT_TYPE1<>'删除'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(order_balance_date),1,7),asc_code,bn"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1(进站台次_一级_车龄)
def kpi_a_agetype_kpi_1(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_agetype partition(mon_p='${mon_p}',kpi='kpi_1')
  select ----'1' as kpi,
  primary_classification,
  age_type,
  substr(tran_date(order_balance_date),1,7) as mon,
  count(distinct vin,tran_date(order_balance_date)) as cnt,
  asc_code
   from label_order  
  where MAINT_TYPE1<>'删除'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(order_balance_date),1,7),asc_code,primary_classification,age_type"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1(进站台次_一级_保内外)
def kpi_a_bnbw_kpi_1(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_bnbw partition(mon_p='${mon_p}',kpi='kpi_1')
  select ----'1' as kpi,
  primary_classification,
  bn,
  substr(tran_date(order_balance_date),1,7) as mon,
  count(distinct vin,tran_date(order_balance_date)) as cnt,
  asc_code
   from label_order  
  where MAINT_TYPE1<>'删除'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(order_balance_date),1,7),asc_code,primary_classification,bn"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1(进站台次_二级_车龄)
def kpi_b_agetype_kpi_1(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_agetype partition(mon_p='${mon_p}',kpi='kpi_1')
  select ----'1' as kpi,
  primary_classification,
  second_level_classification,
  age_type,
  substr(tran_date(order_balance_date),1,7) as mon,
  count(distinct vin,tran_date(order_balance_date)) as cnt,
  asc_code
   from label_order  
  where MAINT_TYPE1<>'删除'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(order_balance_date),1,7),asc_code,primary_classification,second_level_classification,age_type"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1(进站台次_二级_保内外)
def kpi_b_bnbw_kpi_1(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_bnbw partition(mon_p='${mon_p}',kpi='kpi_1')
  select ----'1' as kpi,
  primary_classification,
  second_level_classification,
  bn,
  substr(tran_date(order_balance_date),1,7) as mon,
  count(distinct vin,tran_date(order_balance_date)) as cnt,
  asc_code
   from label_order  
  where MAINT_TYPE1<>'删除'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(order_balance_date),1,7),asc_code,primary_classification,second_level_classification,bn"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.1(保养台次_total)
def kpi_total_kpi_1_1(begin_day, end_day):
    line = """  insert overwrite table  kpi_total partition(mon_p='${mon_p}',kpi='kpi_1_1')
  select ----'1.1' as kpi,
  substr(tran_date(order_balance_date),1,7) as mon,
  count(distinct vin,tran_date(order_balance_date)) as cnt,
  asc_code
   from label_order  
  where MAINT_TYPE1='保养'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(order_balance_date),1,7),asc_code"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.1(保养台次_一级)
def kpi_a_level_kpi_1_1(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_level partition(mon_p='${mon_p}',kpi='kpi_1_1')
  select ----'1.1' as kpi,
  primary_classification,
  substr(tran_date(order_balance_date),1,7) as mon,
  count(distinct vin,tran_date(order_balance_date)) as cnt,
  asc_code
   from label_order  
  where MAINT_TYPE1='保养'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(order_balance_date),1,7),asc_code,primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.1(保养台次_二级)
def kpi_b_level_kpi_1_1(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_level partition(mon_p='${mon_p}',kpi='kpi_1_1')
  select ----'1.1' as kpi,
  primary_classification,
  second_level_classification,
  substr(tran_date(order_balance_date),1,7) as mon,
  count(distinct vin,tran_date(order_balance_date)) as cnt,
  asc_code
   from label_order  
  where MAINT_TYPE1='保养'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(order_balance_date),1,7),asc_code,primary_classification,second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.1(保养台次_车龄)
def kpi_agetype_kpi_1_1(begin_day, end_day):
    line = """  insert overwrite table  kpi_agetype partition(mon_p='${mon_p}',kpi='kpi_1_1')
  select ----'1.1' as kpi,
  age_type,
  substr(tran_date(order_balance_date),1,7) as mon,
  count(distinct vin,tran_date(order_balance_date)) as cnt,
  asc_code
   from label_order  
  where MAINT_TYPE1='保养'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(order_balance_date),1,7),asc_code,age_type"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.1(保养台次_保内外)
def kpi_bnbw_kpi_1_1(begin_day, end_day):
    line = """  insert overwrite table  kpi_bnbw partition(mon_p='${mon_p}',kpi='kpi_1_1')
  select ----'1.1' as kpi,
  bn,
  substr(tran_date(order_balance_date),1,7) as mon,
  count(distinct vin,tran_date(order_balance_date)) as cnt,
  asc_code
   from label_order  
  where MAINT_TYPE1='保养'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(order_balance_date),1,7),asc_code,bn"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.1(保养台次_一级_车龄)
def kpi_a_agetype_kpi_1_1(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_agetype partition(mon_p='${mon_p}',kpi='kpi_1_1')
  select ----'1.1' as kpi,
  primary_classification,
  age_type,
  substr(tran_date(order_balance_date),1,7) as mon,
  count(distinct vin,tran_date(order_balance_date)) as cnt,
  asc_code
   from label_order  
  where MAINT_TYPE1='保养'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(order_balance_date),1,7),asc_code,primary_classification,age_type"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.1(保养台次_一级_保内外)
def kpi_a_bnbw_kpi_1_1(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_bnbw partition(mon_p='${mon_p}',kpi='kpi_1_1')
  select ----'1.1' as kpi,
  primary_classification,
  bn,
  substr(tran_date(order_balance_date),1,7) as mon,
  count(distinct vin,tran_date(order_balance_date)) as cnt,
  asc_code
   from label_order  
  where MAINT_TYPE1='保养'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(order_balance_date),1,7),asc_code,primary_classification,bn"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.1(保养台次_二级_车龄)
def kpi_b_agetype_kpi_1_1(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_agetype partition(mon_p='${mon_p}',kpi='kpi_1_1')
  select ----'1.1' as kpi,
  primary_classification,
  second_level_classification,
  age_type,
  substr(tran_date(order_balance_date),1,7) as mon,
  count(distinct vin,tran_date(order_balance_date)) as cnt,
  asc_code
   from label_order  
  where MAINT_TYPE1='保养'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(order_balance_date),1,7),asc_code,primary_classification,second_level_classification,age_type"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.1(保养台次_二级_保内外)
def kpi_b_bnbw_kpi_1_1(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_bnbw partition(mon_p='${mon_p}',kpi='kpi_1_1')
  select ----'1.1' as kpi,
  primary_classification,
  second_level_classification,
  bn,
  substr(tran_date(order_balance_date),1,7) as mon,
  count(distinct vin,tran_date(order_balance_date)) as cnt,
  asc_code
   from label_order  
  where MAINT_TYPE1='保养'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(order_balance_date),1,7),asc_code,primary_classification,second_level_classification,bn"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.11(平价机油台次_total)
def kpi_total_kpi_1_11(begin_day, end_day):
    line = """  insert overwrite table  kpi_total partition(mon_p='${mon_p}',kpi='kpi_1_11')
  select ----'1.11' as kpi,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  count(distinct s.vin,tran_date(s.order_balance_date)) as cnt,
  s.asc_code
   from label_order s,part t,engine_oil m
  where s.MAINT_TYPE1='保养'
  and s.asc_code=t.asc_code
  and s.order_number=t.order_number
  and t.part_number=m.part_num
  and m.type='低端'
  and substr(tran_date(s.order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(s.order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.11(平价机油台次_一级)
def kpi_a_level_kpi_1_11(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_level partition(mon_p='${mon_p}',kpi='kpi_1_11')
  select ----'1.11' as kpi,
  s.primary_classification,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  count(distinct s.vin,tran_date(s.order_balance_date)) as cnt,
  s.asc_code
   from label_order s,part t,engine_oil m
  where s.MAINT_TYPE1='保养'
  and s.asc_code=t.asc_code
  and s.order_number=t.order_number
  and t.part_number=m.part_num
  and m.type='低端'
  and substr(tran_date(s.order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(s.order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,s.primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.11(平价机油台次_二级)
def kpi_b_level_kpi_1_11(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_level partition(mon_p='${mon_p}',kpi='kpi_1_11')
  select ----'1.11' as kpi,
  s.primary_classification,
  s.second_level_classification,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  count(distinct s.vin,tran_date(s.order_balance_date)) as cnt,
  s.asc_code
   from label_order s,part t,engine_oil m
  where s.MAINT_TYPE1='保养'
  and s.asc_code=t.asc_code
  and s.order_number=t.order_number
  and t.part_number=m.part_num
  and m.type='低端'
  and substr(tran_date(s.order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(s.order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,s.primary_classification,s.second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.11(平价机油台次_车龄)
def kpi_agetype_kpi_1_11(begin_day, end_day):
    line = """  insert overwrite table  kpi_agetype partition(mon_p='${mon_p}',kpi='kpi_1_11')
  select ----'1.11' as kpi,
  s.age_type,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  count(distinct s.vin,tran_date(s.order_balance_date)) as cnt,
  s.asc_code
   from label_order s,part t,engine_oil m
  where s.MAINT_TYPE1='保养'
  and s.asc_code=t.asc_code
  and s.order_number=t.order_number
  and t.part_number=m.part_num
  and m.type='低端'
  and substr(tran_date(s.order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(s.order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,s.age_type"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.11(平价机油台次_保内外)
def kpi_bnbw_kpi_1_11(begin_day, end_day):
    line = """  insert overwrite table  kpi_bnbw partition(mon_p='${mon_p}',kpi='kpi_1_11')
  select ----'1.11' as kpi,
  s.bn,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  count(distinct s.vin,tran_date(s.order_balance_date)) as cnt,
  s.asc_code
   from label_order s,part t,engine_oil m
  where s.MAINT_TYPE1='保养'
  and s.asc_code=t.asc_code
  and s.order_number=t.order_number
  and t.part_number=m.part_num
  and m.type='低端'
  and substr(tran_date(s.order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(s.order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,s.bn"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.11(平价机油台次_一级_车龄)
def kpi_a_agetype_kpi_1_11(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_agetype partition(mon_p='${mon_p}',kpi='kpi_1_11')
  select ----'1.11' as kpi,
  s.primary_classification,
  s.age_type,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  count(distinct s.vin,tran_date(s.order_balance_date)) as cnt,
  s.asc_code
   from label_order s,part t,engine_oil m
  where s.MAINT_TYPE1='保养'
  and s.asc_code=t.asc_code
  and s.order_number=t.order_number
  and t.part_number=m.part_num
  and m.type='低端'
  and substr(tran_date(s.order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(s.order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,s.primary_classification,s.age_type"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.11(平价机油台次_一级_保内外)
def kpi_a_bnbw_kpi_1_11(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_bnbw partition(mon_p='${mon_p}',kpi='kpi_1_11')
  select ----'1.11' as kpi,
  s.primary_classification,
  s.bn,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  count(distinct s.vin,tran_date(s.order_balance_date)) as cnt,
  s.asc_code
   from label_order s,part t,engine_oil m
  where s.MAINT_TYPE1='保养'
  and s.asc_code=t.asc_code
  and s.order_number=t.order_number
  and t.part_number=m.part_num
  and m.type='低端'
  and substr(tran_date(s.order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(s.order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,s.primary_classification,s.bn"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.11(平价机油台次_二级_车龄)
def kpi_b_agetype_kpi_1_11(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_agetype partition(mon_p='${mon_p}',kpi='kpi_1_11')
  select ----'1.11' as kpi,
  s.primary_classification,
  s.second_level_classification,
  s.age_type,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  count(distinct s.vin,tran_date(s.order_balance_date)) as cnt,
  s.asc_code
   from label_order s,part t,engine_oil m
  where s.MAINT_TYPE1='保养'
  and s.asc_code=t.asc_code
  and s.order_number=t.order_number
  and t.part_number=m.part_num
  and m.type='低端'
  and substr(tran_date(s.order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(s.order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,s.primary_classification,s.second_level_classification,s.age_type"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.11(平价机油台次_二级_保内外)
def kpi_b_bnbw_kpi_1_11(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_bnbw partition(mon_p='${mon_p}',kpi='kpi_1_11')
  select ----'1.11' as kpi,
  s.primary_classification,
  s.second_level_classification,
  s.bn,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  count(distinct s.vin,tran_date(s.order_balance_date)) as cnt,
  s.asc_code
   from label_order s,part t,engine_oil m
  where s.MAINT_TYPE1='保养'
  and s.asc_code=t.asc_code
  and s.order_number=t.order_number
  and t.part_number=m.part_num
  and m.type='低端'
  and substr(tran_date(s.order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(s.order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,s.primary_classification,s.second_level_classification,s.bn"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.12(中端机油台次_total)
def kpi_total_kpi_1_12(begin_day, end_day):
    line = """  insert overwrite table  kpi_total partition(mon_p='${mon_p}',kpi='kpi_1_12')
  select ----'1.12' as kpi,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  count(distinct s.vin,tran_date(s.order_balance_date)) as cnt,
  s.asc_code
   from label_order s,part t,engine_oil m
  where s.MAINT_TYPE1='保养'
  and s.asc_code=t.asc_code
  and s.order_number=t.order_number
  and t.part_number=m.part_num
  and m.type='中端'
  and substr(tran_date(s.order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(s.order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.12(中端机油台次_一级)
def kpi_a_level_kpi_1_12(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_level partition(mon_p='${mon_p}',kpi='kpi_1_12')
  select ----'1.12' as kpi,
  s.primary_classification,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  count(distinct s.vin,tran_date(s.order_balance_date)) as cnt,
  s.asc_code
   from label_order s,part t,engine_oil m
  where s.MAINT_TYPE1='保养'
  and s.asc_code=t.asc_code
  and s.order_number=t.order_number
  and t.part_number=m.part_num
  and m.type='中端'
  and substr(tran_date(s.order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(s.order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,s.primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.12(中端机油台次_二级)
def kpi_b_level_kpi_1_12(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_level partition(mon_p='${mon_p}',kpi='kpi_1_12')
  select ----'1.12' as kpi,
  s.primary_classification,
  s.second_level_classification,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  count(distinct s.vin,tran_date(s.order_balance_date)) as cnt,
  s.asc_code
   from label_order s,part t,engine_oil m
  where s.MAINT_TYPE1='保养'
  and s.asc_code=t.asc_code
  and s.order_number=t.order_number
  and t.part_number=m.part_num
  and m.type='中端'
  and substr(tran_date(s.order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(s.order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,s.primary_classification,s.second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.12(中端机油台次_车龄)
def kpi_agetype_kpi_1_12(begin_day, end_day):
    line = """  insert overwrite table  kpi_agetype partition(mon_p='${mon_p}',kpi='kpi_1_12')
  select ----'1.12' as kpi,
  s.age_type,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  count(distinct s.vin,tran_date(s.order_balance_date)) as cnt,
  s.asc_code
   from label_order s,part t,engine_oil m
  where s.MAINT_TYPE1='保养'
  and s.asc_code=t.asc_code
  and s.order_number=t.order_number
  and t.part_number=m.part_num
  and m.type='中端'
  and substr(tran_date(s.order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(s.order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,s.age_type"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.12(中端机油台次_保内外)
def kpi_bnbw_kpi_1_12(begin_day, end_day):
    line = """  insert overwrite table  kpi_bnbw partition(mon_p='${mon_p}',kpi='kpi_1_12')
  select ----'1.12' as kpi,
  s.bn,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  count(distinct s.vin,tran_date(s.order_balance_date)) as cnt,
  s.asc_code
   from label_order s,part t,engine_oil m
  where s.MAINT_TYPE1='保养'
  and s.asc_code=t.asc_code
  and s.order_number=t.order_number
  and t.part_number=m.part_num
  and m.type='中端'
  and substr(tran_date(s.order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(s.order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,s.bn"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.12(中端机油台次_一级_车龄)
def kpi_a_agetype_kpi_1_12(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_agetype partition(mon_p='${mon_p}',kpi='kpi_1_12')
  select ----'1.12' as kpi,
  s.primary_classification,
  s.age_type,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  count(distinct s.vin,tran_date(s.order_balance_date)) as cnt,
  s.asc_code
   from label_order s,part t,engine_oil m
  where s.MAINT_TYPE1='保养'
  and s.asc_code=t.asc_code
  and s.order_number=t.order_number
  and t.part_number=m.part_num
  and m.type='中端'
  and substr(tran_date(s.order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(s.order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,s.primary_classification,s.age_type"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.12(中端机油台次_一级_保内外)
def kpi_a_bnbw_kpi_1_12(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_bnbw partition(mon_p='${mon_p}',kpi='kpi_1_12')
  select ----'1.12' as kpi,
  s.primary_classification,
  s.bn,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  count(distinct s.vin,tran_date(s.order_balance_date)) as cnt,
  s.asc_code
   from label_order s,part t,engine_oil m
  where s.MAINT_TYPE1='保养'
  and s.asc_code=t.asc_code
  and s.order_number=t.order_number
  and t.part_number=m.part_num
  and m.type='中端'
  and substr(tran_date(s.order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(s.order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,s.primary_classification,s.bn"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.12(中端机油台次_二级_车龄)
def kpi_b_agetype_kpi_1_12(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_agetype partition(mon_p='${mon_p}',kpi='kpi_1_12')
  select ----'1.12' as kpi,
  s.primary_classification,
  s.second_level_classification,
  s.age_type,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  count(distinct s.vin,tran_date(s.order_balance_date)) as cnt,
  s.asc_code
   from label_order s,part t,engine_oil m
  where s.MAINT_TYPE1='保养'
  and s.asc_code=t.asc_code
  and s.order_number=t.order_number
  and t.part_number=m.part_num
  and m.type='中端'
  and substr(tran_date(s.order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(s.order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,s.primary_classification,s.second_level_classification,s.age_type"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.12(中端机油台次_二级_保内外)
def kpi_b_bnbw_kpi_1_12(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_bnbw partition(mon_p='${mon_p}',kpi='kpi_1_12')
  select ----'1.12' as kpi,
  s.primary_classification,
  s.second_level_classification,
  s.bn,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  count(distinct s.vin,tran_date(s.order_balance_date)) as cnt,
  s.asc_code
   from label_order s,part t,engine_oil m
  where s.MAINT_TYPE1='保养'
  and s.asc_code=t.asc_code
  and s.order_number=t.order_number
  and t.part_number=m.part_num
  and m.type='中端'
  and substr(tran_date(s.order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(s.order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,s.primary_classification,s.second_level_classification,s.bn"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.13(高端机油台次_total)
def kpi_total_kpi_1_13(begin_day, end_day):
    line = """  insert overwrite table  kpi_total partition(mon_p='${mon_p}',kpi='kpi_1_13')
  select ----'1.13' as kpi,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  count(distinct s.vin,tran_date(s.order_balance_date)) as cnt,
  s.asc_code
   from label_order s,part t,engine_oil m
  where s.MAINT_TYPE1='保养'
  and s.asc_code=t.asc_code
  and s.order_number=t.order_number
  and t.part_number=m.part_num
  and m.type='高端'
  and substr(tran_date(s.order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(s.order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.13(高端机油台次_一级)
def kpi_a_level_kpi_1_13(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_level partition(mon_p='${mon_p}',kpi='kpi_1_13')
  select ----'1.13' as kpi,
  s.primary_classification,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  count(distinct s.vin,tran_date(s.order_balance_date)) as cnt,
  s.asc_code
   from label_order s,part t,engine_oil m
  where s.MAINT_TYPE1='保养'
  and s.asc_code=t.asc_code
  and s.order_number=t.order_number
  and t.part_number=m.part_num
  and m.type='高端'
  and substr(tran_date(s.order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(s.order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,s.primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.13(高端机油台次_二级)
def kpi_b_level_kpi_1_13(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_level partition(mon_p='${mon_p}',kpi='kpi_1_13')
  select ----'1.13' as kpi,
  s.primary_classification,
  s.second_level_classification,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  count(distinct s.vin,tran_date(s.order_balance_date)) as cnt,
  s.asc_code
   from label_order s,part t,engine_oil m
  where s.MAINT_TYPE1='保养'
  and s.asc_code=t.asc_code
  and s.order_number=t.order_number
  and t.part_number=m.part_num
  and m.type='高端'
  and substr(tran_date(s.order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(s.order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,s.primary_classification,s.second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.13(高端机油台次_车龄)
def kpi_agetype_kpi_1_13(begin_day, end_day):
    line = """  insert overwrite table  kpi_agetype partition(mon_p='${mon_p}',kpi='kpi_1_13')
  select ----'1.13' as kpi,
  s.age_type,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  count(distinct s.vin,tran_date(s.order_balance_date)) as cnt,
  s.asc_code
   from label_order s,part t,engine_oil m
  where s.MAINT_TYPE1='保养'
  and s.asc_code=t.asc_code
  and s.order_number=t.order_number
  and t.part_number=m.part_num
  and m.type='高端'
  and substr(tran_date(s.order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(s.order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,s.age_type"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.13(高端机油台次_保内外)
def kpi_bnbw_kpi_1_13(begin_day, end_day):
    line = """  insert overwrite table  kpi_bnbw partition(mon_p='${mon_p}',kpi='kpi_1_13')
  select ----'1.13' as kpi,
  s.bn,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  count(distinct s.vin,tran_date(s.order_balance_date)) as cnt,
  s.asc_code
   from label_order s,part t,engine_oil m
  where s.MAINT_TYPE1='保养'
  and s.asc_code=t.asc_code
  and s.order_number=t.order_number
  and t.part_number=m.part_num
  and m.type='高端'
  and substr(tran_date(s.order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(s.order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,s.bn"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.13(高端机油台次_一级_车龄)
def kpi_a_agetype_kpi_1_13(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_agetype partition(mon_p='${mon_p}',kpi='kpi_1_13')
  select ----'1.13' as kpi,
  s.primary_classification,
  s.age_type,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  count(distinct s.vin,tran_date(s.order_balance_date)) as cnt,
  s.asc_code
   from label_order s,part t,engine_oil m
  where s.MAINT_TYPE1='保养'
  and s.asc_code=t.asc_code
  and s.order_number=t.order_number
  and t.part_number=m.part_num
  and m.type='高端'
  and substr(tran_date(s.order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(s.order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,s.primary_classification,s.age_type"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.13(高端机油台次_一级_保内外)
def kpi_a_bnbw_kpi_1_13(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_bnbw partition(mon_p='${mon_p}',kpi='kpi_1_13')
  select ----'1.13' as kpi,
  s.primary_classification,
  s.bn,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  count(distinct s.vin,tran_date(s.order_balance_date)) as cnt,
  s.asc_code
   from label_order s,part t,engine_oil m
  where s.MAINT_TYPE1='保养'
  and s.asc_code=t.asc_code
  and s.order_number=t.order_number
  and t.part_number=m.part_num
  and m.type='高端'
  and substr(tran_date(s.order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(s.order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,s.primary_classification,s.bn"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.13(高端机油台次_二级_车龄)
def kpi_b_agetype_kpi_1_13(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_agetype partition(mon_p='${mon_p}',kpi='kpi_1_13')
  select ----'1.13' as kpi,
  s.primary_classification,
  s.second_level_classification,
  s.age_type,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  count(distinct s.vin,tran_date(s.order_balance_date)) as cnt,
  s.asc_code
   from label_order s,part t,engine_oil m
  where s.MAINT_TYPE1='保养'
  and s.asc_code=t.asc_code
  and s.order_number=t.order_number
  and t.part_number=m.part_num
  and m.type='高端'
  and substr(tran_date(s.order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(s.order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,s.primary_classification,s.second_level_classification,s.age_type"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.13(高端机油台次_二级_保内外)
def kpi_b_bnbw_kpi_1_13(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_bnbw partition(mon_p='${mon_p}',kpi='kpi_1_13')
  select ----'1.13' as kpi,
  s.primary_classification,
  s.second_level_classification,
  s.bn,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  count(distinct s.vin,tran_date(s.order_balance_date)) as cnt,
  s.asc_code
   from label_order s,part t,engine_oil m
  where s.MAINT_TYPE1='保养'
  and s.asc_code=t.asc_code
  and s.order_number=t.order_number
  and t.part_number=m.part_num
  and m.type='高端'
  and substr(tran_date(s.order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(s.order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,s.primary_classification,s.second_level_classification,s.bn"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.14(保养台次占比_total)
def kpi_total_kpi_1_14(begin_day, end_day):
    line = """  insert overwrite table  kpi_total partition(mon_p='${mon_p}',kpi='kpi_1_14')
  select ----'1.14' as kpi,
   s.mon,
   s.num/t.num as num,
   s.asc_code
  from (
  select * from kpi_total 
  where kpi='kpi_1_1'
    and mon>='${begin_date}'
    and mon<='${end_date}'
   ) s,(
  select * from kpi_total 
  where kpi='kpi_1'
    and mon>='${begin_date}'
    and mon<='${end_date}'
   ) t 
  where s.asc_code=t.asc_code
    and s.mon=t.mon """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.14(保养台次占比_一级)
def kpi_a_level_kpi_1_14(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_level partition(mon_p='${mon_p}',kpi='kpi_1_14')
  select ----'1.14' as kpi,
   s.a_level,
   s.mon,
   s.num/t.num as num,
   s.asc_code
  from (
  select * from kpi_a_level 
  where kpi='kpi_1_1'
    and mon>='${begin_date}'
    and mon<='${end_date}'
   ) s,(
  select * from kpi_a_level 
  where kpi='kpi_1'
    and mon>='${begin_date}'
    and mon<='${end_date}'
   ) t 
  where s.asc_code=t.asc_code
    and s.mon=t.mon 
    and s.a_level=t.a_level """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.14(保养台次占比_二级)
def kpi_b_level_kpi_1_14(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_level partition(mon_p='${mon_p}',kpi='kpi_1_14')
  select ----'1.14' as kpi,
   s.a_level,
   s.b_level,
   s.mon,
   s.num/t.num as num,
   s.asc_code
  from (
  select * from kpi_b_level 
  where kpi='kpi_1_1'
    and mon>='${begin_date}'
    and mon<='${end_date}'
   ) s,(
  select * from kpi_b_level 
  where kpi='kpi_1'
    and mon>='${begin_date}'
    and mon<='${end_date}'
   ) t 
  where s.asc_code=t.asc_code
    and s.mon=t.mon 
    and s.a_level=t.a_level 
    and s.b_level=t.b_level"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.14(保养台次占比_车龄)
def kpi_agetype_kpi_1_14(begin_day, end_day):
    line = """  insert overwrite table  kpi_agetype partition(mon_p='${mon_p}',kpi='kpi_1_14')
  select ----'1.14' as kpi,
   s.age_type,
   s.mon,
   s.num/t.num as num,
   s.asc_code
  from (
  select * from kpi_agetype 
  where kpi='kpi_1_1'
    and mon>='${begin_date}'
    and mon<='${end_date}'
   ) s,(
  select * from kpi_agetype 
  where kpi='kpi_1'
    and mon>='${begin_date}'
    and mon<='${end_date}'
   ) t 
  where s.asc_code=t.asc_code
    and s.mon=t.mon 
    and s.age_type=t.age_type """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.14(保养台次占比_保内外)
def kpi_bnbw_kpi_1_14(begin_day, end_day):
    line = """  insert overwrite table  kpi_bnbw partition(mon_p='${mon_p}',kpi='kpi_1_14')
  select ----'1.14' as kpi,
   s.bnbw,
   s.mon,
   s.num/t.num as num,
   s.asc_code
  from (
  select * from kpi_bnbw 
  where kpi='kpi_1_1'
    and mon>='${begin_date}'
    and mon<='${end_date}'
   ) s,(
  select * from kpi_bnbw 
  where kpi='kpi_1'
    and mon>='${begin_date}'
    and mon<='${end_date}'
   ) t 
  where s.asc_code=t.asc_code
    and s.mon=t.mon 
    and s.bnbw=t.bnbw """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.14(保养台次占比_一级_车龄)
def kpi_a_agetype_kpi_1_14(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_agetype partition(mon_p='${mon_p}',kpi='kpi_1_14')
  select ----'1.14' as kpi,
   s.a_level,
   s.age_type,
   s.mon,
   s.num/t.num as num,
   s.asc_code
  from (
  select * from kpi_a_agetype 
  where kpi='kpi_1_1'
    and mon>='${begin_date}'
    and mon<='${end_date}'
   ) s,(
  select * from kpi_a_agetype 
  where kpi='kpi_1'
    and mon>='${begin_date}'
    and mon<='${end_date}'
   ) t 
  where s.asc_code=t.asc_code
    and s.mon=t.mon 
    and s.a_level=t.a_level
    and s.age_type=t.age_type """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.14(保养台次占比_一级_保内外)
def kpi_a_bnbw_kpi_1_14(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_bnbw partition(mon_p='${mon_p}',kpi='kpi_1_14')
  select ----'1.14' as kpi,
   s.a_level,
   s.bnbw,
   s.mon,
   s.num/t.num as num,
   s.asc_code
  from (
  select * from kpi_a_bnbw 
  where kpi='kpi_1_1'
    and mon>='${begin_date}'
    and mon<='${end_date}'
   ) s,(
  select * from kpi_a_bnbw 
  where kpi='kpi_1'
    and mon>='${begin_date}'
    and mon<='${end_date}'
   ) t 
  where s.asc_code=t.asc_code
    and s.mon=t.mon 
    and s.a_level=t.a_level
    and s.bnbw=t.bnbw """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.14(保养台次占比_二级_车龄)
def kpi_b_agetype_kpi_1_14(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_agetype partition(mon_p='${mon_p}',kpi='kpi_1_14')
  select ----'1.14' as kpi,
   s.a_level,
   s.b_level,
   s.age_type,
   s.mon,
   s.num/t.num as num,
   s.asc_code
  from (
  select * from kpi_b_agetype 
  where kpi='kpi_1_1'
    and mon>='${begin_date}'
    and mon<='${end_date}'
   ) s,(
  select * from kpi_b_agetype 
  where kpi='kpi_1'
    and mon>='${begin_date}'
    and mon<='${end_date}'
   ) t 
  where s.asc_code=t.asc_code
    and s.mon=t.mon 
    and s.a_level=t.a_level
    and s.b_level=t.b_level
    and s.age_type=t.age_type """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.14(保养台次占比_二级_保内外)
def kpi_b_bnbw_kpi_1_14(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_bnbw partition(mon_p='${mon_p}',kpi='kpi_1_14')
  select ----'1.14' as kpi,
   s.a_level,
   s.b_level,
   s.bnbw,
   s.mon,
   s.num/t.num as num,
   s.asc_code
  from (
  select * from kpi_b_bnbw 
  where kpi='kpi_1_1'
    and mon>='${begin_date}'
    and mon<='${end_date}'
   ) s,(
  select * from kpi_b_bnbw 
  where kpi='kpi_1'
    and mon>='${begin_date}'
    and mon<='${end_date}'
   ) t 
  where s.asc_code=t.asc_code
    and s.mon=t.mon 
    and s.a_level=t.a_level
    and s.b_level=t.b_level
    and s.bnbw=t.bnbw """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.2(维修台次_total)
def kpi_total_kpi_1_2(begin_day, end_day):
    line = """  insert overwrite table  kpi_total partition(mon_p='${mon_p}',kpi='kpi_1_2')
  select ----'1.2' as kpi,
  substr(tran_date(order_balance_date),1,7) as mon,
  count(distinct vin,tran_date(order_balance_date)) as cnt,
  asc_code
   from label_order  
  where MAINT_TYPE1='维修'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(order_balance_date),1,7),asc_code"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.2(维修台次_一级)
def kpi_a_level_kpi_1_2(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_level partition(mon_p='${mon_p}',kpi='kpi_1_2')
  select ----'1.2' as kpi,
  primary_classification,
  substr(tran_date(order_balance_date),1,7) as mon,
  count(distinct vin,tran_date(order_balance_date)) as cnt,
  asc_code
   from label_order  
  where MAINT_TYPE1='维修'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(order_balance_date),1,7),asc_code,primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.2(维修台次_二级)
def kpi_b_level_kpi_1_2(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_level partition(mon_p='${mon_p}',kpi='kpi_1_2')
  select ----'1.2' as kpi,
  primary_classification,
  second_level_classification,
  substr(tran_date(order_balance_date),1,7) as mon,
  count(distinct vin,tran_date(order_balance_date)) as cnt,
  asc_code
   from label_order  
  where MAINT_TYPE1='维修'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(order_balance_date),1,7),asc_code,primary_classification,second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.2(维修台次_车龄)
def kpi_agetype_kpi_1_2(begin_day, end_day):
    line = """  insert overwrite table  kpi_agetype partition(mon_p='${mon_p}',kpi='kpi_1_2')
  select ----'1.2' as kpi,
  age_type,
  substr(tran_date(order_balance_date),1,7) as mon,
  count(distinct vin,tran_date(order_balance_date)) as cnt,
  asc_code
   from label_order  
  where MAINT_TYPE1='维修'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(order_balance_date),1,7),asc_code,age_type"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.2(维修台次_保内外)
def kpi_bnbw_kpi_1_2(begin_day, end_day):
    line = """  insert overwrite table  kpi_bnbw partition(mon_p='${mon_p}',kpi='kpi_1_2')
  select ----'1.2' as kpi,
  bn,
  substr(tran_date(order_balance_date),1,7) as mon,
  count(distinct vin,tran_date(order_balance_date)) as cnt,
  asc_code
   from label_order  
  where MAINT_TYPE1='维修'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(order_balance_date),1,7),asc_code,bn"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.2(维修台次_一级_车龄)
def kpi_a_agetype_kpi_1_2(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_agetype partition(mon_p='${mon_p}',kpi='kpi_1_2')
  select ----'1.2' as kpi,
  primary_classification,
  age_type,
  substr(tran_date(order_balance_date),1,7) as mon,
  count(distinct vin,tran_date(order_balance_date)) as cnt,
  asc_code
   from label_order  
  where MAINT_TYPE1='维修'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(order_balance_date),1,7),asc_code,primary_classification,age_type"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.2(维修台次_一级_保内外)
def kpi_a_bnbw_kpi_1_2(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_bnbw partition(mon_p='${mon_p}',kpi='kpi_1_2')
  select ----'1.2' as kpi,
  primary_classification,
  bn,
  substr(tran_date(order_balance_date),1,7) as mon,
  count(distinct vin,tran_date(order_balance_date)) as cnt,
  asc_code
   from label_order  
  where MAINT_TYPE1='维修'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(order_balance_date),1,7),asc_code,primary_classification,bn"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.2(维修台次_二级_车龄)
def kpi_b_agetype_kpi_1_2(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_agetype partition(mon_p='${mon_p}',kpi='kpi_1_2')
  select ----'1.2' as kpi,
  primary_classification,
  second_level_classification,
  age_type,
  substr(tran_date(order_balance_date),1,7) as mon,
  count(distinct vin,tran_date(order_balance_date)) as cnt,
  asc_code
   from label_order  
  where MAINT_TYPE1='维修'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(order_balance_date),1,7),asc_code,primary_classification,second_level_classification,age_type"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.2(维修台次_二级_保内外)
def kpi_b_bnbw_kpi_1_2(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_bnbw partition(mon_p='${mon_p}',kpi='kpi_1_2')
  select ----'1.2' as kpi,
  primary_classification,
  second_level_classification,
  bn,
  substr(tran_date(order_balance_date),1,7) as mon,
  count(distinct vin,tran_date(order_balance_date)) as cnt,
  asc_code
   from label_order  
  where MAINT_TYPE1='维修'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(order_balance_date),1,7),asc_code,primary_classification,second_level_classification,bn"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.21(维修台次占比_total)
def kpi_total_kpi_1_21(begin_day, end_day):
    line = """  insert overwrite table  kpi_total partition(mon_p='${mon_p}',kpi='kpi_1_21')
  select ----'1.21' as kpi,
   s.mon,
   s.num/t.num as num,
   s.asc_code
  from (
  select * from kpi_total 
  where kpi='kpi_1_2'
    and mon>='${begin_date}'
    and mon<='${end_date}'
   ) s,(
  select * from kpi_total 
  where kpi='kpi_1'
    and mon>='${begin_date}'
    and mon<='${end_date}'
   ) t 
  where s.asc_code=t.asc_code
    and s.mon=t.mon """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.21(维修台次占比_一级)
def kpi_a_level_kpi_1_21(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_level partition(mon_p='${mon_p}',kpi='kpi_1_21')
  select ----'1.21' as kpi,
   s.a_level,
   s.mon,
   s.num/t.num as num,
   s.asc_code
  from (
  select * from kpi_a_level 
  where kpi='kpi_1_2'
    and mon>='${begin_date}'
    and mon<='${end_date}'
   ) s,(
  select * from kpi_a_level 
  where kpi='kpi_1'
    and mon>='${begin_date}'
    and mon<='${end_date}'
   ) t 
  where s.asc_code=t.asc_code
    and s.mon=t.mon 
    and s.a_level=t.a_level """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.21(维修台次占比_二级)
def kpi_b_level_kpi_1_21(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_level partition(mon_p='${mon_p}',kpi='kpi_1_21')
  select ----'1.21' as kpi,
   s.a_level,
   s.b_level,
   s.mon,
   s.num/t.num as num,
   s.asc_code
  from (
  select * from kpi_b_level 
  where kpi='kpi_1_2'
    and mon>='${begin_date}'
    and mon<='${end_date}'
   ) s,(
  select * from kpi_b_level 
  where kpi='kpi_1'
    and mon>='${begin_date}'
    and mon<='${end_date}'
   ) t 
  where s.asc_code=t.asc_code
    and s.mon=t.mon 
    and s.a_level=t.a_level 
    and s.b_level=t.b_level"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.21(维修台次占比_车龄)
def kpi_agetype_kpi_1_21(begin_day, end_day):
    line = """  insert overwrite table  kpi_agetype partition(mon_p='${mon_p}',kpi='kpi_1_21')
  select ----'1.21' as kpi,
   s.age_type,
   s.mon,
   s.num/t.num as num,
   s.asc_code
  from (
  select * from kpi_agetype 
  where kpi='kpi_1_2'
    and mon>='${begin_date}'
    and mon<='${end_date}'
   ) s,(
  select * from kpi_agetype 
  where kpi='kpi_1'
    and mon>='${begin_date}'
    and mon<='${end_date}'
   ) t 
  where s.asc_code=t.asc_code
    and s.mon=t.mon 
    and s.age_type=t.age_type """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.21(维修台次占比_保内外)
def kpi_bnbw_kpi_1_21(begin_day, end_day):
    line = """  insert overwrite table  kpi_bnbw partition(mon_p='${mon_p}',kpi='kpi_1_21')
  select ----'1.21' as kpi,
   s.bnbw,
   s.mon,
   s.num/t.num as num,
   s.asc_code
  from (
  select * from kpi_bnbw 
  where kpi='kpi_1_2'
    and mon>='${begin_date}'
    and mon<='${end_date}'
   ) s,(
  select * from kpi_bnbw 
  where kpi='kpi_1'
    and mon>='${begin_date}'
    and mon<='${end_date}'
   ) t 
  where s.asc_code=t.asc_code
    and s.mon=t.mon 
    and s.bnbw=t.bnbw """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.21(维修台次占比_一级_车龄)
def kpi_a_agetype_kpi_1_21(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_agetype partition(mon_p='${mon_p}',kpi='kpi_1_21')
  select ----'1.21' as kpi,
   s.a_level,
   s.age_type,
   s.mon,
   s.num/t.num as num,
   s.asc_code
  from (
  select * from kpi_a_agetype 
  where kpi='kpi_1_2'
    and mon>='${begin_date}'
    and mon<='${end_date}'
   ) s,(
  select * from kpi_a_agetype 
  where kpi='kpi_1'
    and mon>='${begin_date}'
    and mon<='${end_date}'
   ) t 
  where s.asc_code=t.asc_code
    and s.mon=t.mon 
    and s.a_level=t.a_level
    and s.age_type=t.age_type """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.21(维修台次占比_一级_保内外)
def kpi_a_bnbw_kpi_1_21(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_bnbw partition(mon_p='${mon_p}',kpi='kpi_1_21')
  select ----'1.21' as kpi,
   s.a_level,
   s.bnbw,
   s.mon,
   s.num/t.num as num,
   s.asc_code
  from (
  select * from kpi_a_bnbw 
  where kpi='kpi_1_2'
    and mon>='${begin_date}'
    and mon<='${end_date}'
   ) s,(
  select * from kpi_a_bnbw 
  where kpi='kpi_1'
    and mon>='${begin_date}'
    and mon<='${end_date}'
   ) t 
  where s.asc_code=t.asc_code
    and s.mon=t.mon 
    and s.a_level=t.a_level
    and s.bnbw=t.bnbw """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.21(维修台次占比_二级_车龄)
def kpi_b_agetype_kpi_1_21(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_agetype partition(mon_p='${mon_p}',kpi='kpi_1_21')
  select ----'1.21' as kpi,
   s.a_level,
   s.b_level,
   s.age_type,
   s.mon,
   s.num/t.num as num,
   s.asc_code
  from (
  select * from kpi_b_agetype 
  where kpi='kpi_1_2'
    and mon>='${begin_date}'
    and mon<='${end_date}'
   ) s,(
  select * from kpi_b_agetype 
  where kpi='kpi_1'
    and mon>='${begin_date}'
    and mon<='${end_date}'
   ) t 
  where s.asc_code=t.asc_code
    and s.mon=t.mon 
    and s.a_level=t.a_level
    and s.b_level=t.b_level
    and s.age_type=t.age_type """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.21(维修台次占比_二级_保内外)
def kpi_b_bnbw_kpi_1_21(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_bnbw partition(mon_p='${mon_p}',kpi='kpi_1_21')
  select ----'1.21' as kpi,
   s.a_level,
   s.b_level,
   s.bnbw,
   s.mon,
   s.num/t.num as num,
   s.asc_code
  from (
  select * from kpi_b_bnbw 
  where kpi='kpi_1_2'
    and mon>='${begin_date}'
    and mon<='${end_date}'
   ) s,(
  select * from kpi_b_bnbw 
  where kpi='kpi_1'
    and mon>='${begin_date}'
    and mon<='${end_date}'
   ) t 
  where s.asc_code=t.asc_code
    and s.mon=t.mon 
    and s.a_level=t.a_level
    and s.b_level=t.b_level
    and s.bnbw=t.bnbw """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.3(事故台次_total)
def kpi_total_kpi_1_3(begin_day, end_day):
    line = """  insert overwrite table  kpi_total partition(mon_p='${mon_p}',kpi='kpi_1_3')
  select ----'1.3' as kpi,
  substr(tran_date(order_balance_date),1,7) as mon,
  count(distinct vin,tran_date(order_balance_date)) as cnt,
  asc_code
   from label_order  
  where MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(order_balance_date),1,7),asc_code"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.3(事故台次_一级)
def kpi_a_level_kpi_1_3(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_level partition(mon_p='${mon_p}',kpi='kpi_1_3')
  select ----'1.3' as kpi,
  primary_classification,
  substr(tran_date(order_balance_date),1,7) as mon,
  count(distinct vin,tran_date(order_balance_date)) as cnt,
  asc_code
   from label_order  
  where MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(order_balance_date),1,7),asc_code,primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.3(事故台次_二级)
def kpi_b_level_kpi_1_3(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_level partition(mon_p='${mon_p}',kpi='kpi_1_3')
  select ----'1.3' as kpi,
  primary_classification,
  second_level_classification,
  substr(tran_date(order_balance_date),1,7) as mon,
  count(distinct vin,tran_date(order_balance_date)) as cnt,
  asc_code
   from label_order  
  where MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(order_balance_date),1,7),asc_code,primary_classification,second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.3(事故台次_车龄)
def kpi_agetype_kpi_1_3(begin_day, end_day):
    line = """  insert overwrite table  kpi_agetype partition(mon_p='${mon_p}',kpi='kpi_1_3')
  select ----'1.3' as kpi,
  age_type,
  substr(tran_date(order_balance_date),1,7) as mon,
  count(distinct vin,tran_date(order_balance_date)) as cnt,
  asc_code
   from label_order  
  where MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(order_balance_date),1,7),asc_code,age_type"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.3(事故台次_保内外)
def kpi_bnbw_kpi_1_3(begin_day, end_day):
    line = """  insert overwrite table  kpi_bnbw partition(mon_p='${mon_p}',kpi='kpi_1_3')
  select ----'1.3' as kpi,
  bn,
  substr(tran_date(order_balance_date),1,7) as mon,
  count(distinct vin,tran_date(order_balance_date)) as cnt,
  asc_code
   from label_order  
  where MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(order_balance_date),1,7),asc_code,bn"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.3(事故台次_一级_车龄)
def kpi_a_agetype_kpi_1_3(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_agetype partition(mon_p='${mon_p}',kpi='kpi_1_3')
  select ----'1.3' as kpi,
  primary_classification,
  age_type,
  substr(tran_date(order_balance_date),1,7) as mon,
  count(distinct vin,tran_date(order_balance_date)) as cnt,
  asc_code
   from label_order  
  where MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(order_balance_date),1,7),asc_code,primary_classification,age_type"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.3(事故台次_一级_保内外)
def kpi_a_bnbw_kpi_1_3(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_bnbw partition(mon_p='${mon_p}',kpi='kpi_1_3')
  select ----'1.3' as kpi,
  primary_classification,
  bn,
  substr(tran_date(order_balance_date),1,7) as mon,
  count(distinct vin,tran_date(order_balance_date)) as cnt,
  asc_code
   from label_order  
  where MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(order_balance_date),1,7),asc_code,primary_classification,bn"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.3(事故台次_二级_车龄)
def kpi_b_agetype_kpi_1_3(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_agetype partition(mon_p='${mon_p}',kpi='kpi_1_3')
  select ----'1.3' as kpi,
  primary_classification,
  second_level_classification,
  age_type,
  substr(tran_date(order_balance_date),1,7) as mon,
  count(distinct vin,tran_date(order_balance_date)) as cnt,
  asc_code
   from label_order  
  where MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(order_balance_date),1,7),asc_code,primary_classification,second_level_classification,age_type"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.3(事故台次_二级_保内外)
def kpi_b_bnbw_kpi_1_3(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_bnbw partition(mon_p='${mon_p}',kpi='kpi_1_3')
  select ----'1.3' as kpi,
  primary_classification,
  second_level_classification,
  bn,
  substr(tran_date(order_balance_date),1,7) as mon,
  count(distinct vin,tran_date(order_balance_date)) as cnt,
  asc_code
   from label_order  
  where MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(order_balance_date),1,7),asc_code,primary_classification,second_level_classification,bn"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.31(799元以下事故_total)
def kpi_total_kpi_1_31(begin_day, end_day):
    line = """  insert overwrite table  kpi_total partition(mon_p='${mon_p}',kpi='kpi_1_31')
  select ----'1.31' as kpi,
  substr(order_date,1,7) as mon,
  count(*) as cnt,
  asc_code
   from (
   select asc_code,
   tran_date(order_balance_date) as order_date,
   vin,
   sum(order_balance_amount) as sumamount  
   from label_order  
  where MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by asc_code,tran_date(order_balance_date),vin
      ) s 
  where sumamount<800
  group by substr(order_date,1,7),asc_code"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.31(799元以下事故_一级)
def kpi_a_level_kpi_1_31(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_level partition(mon_p='${mon_p}',kpi='kpi_1_31')
  select ----'1.31' as kpi,
  primary_classification,
  substr(order_date,1,7) as mon,
  count(*) as cnt,
  asc_code
   from (
   select asc_code,
   primary_classification,
   tran_date(order_balance_date) as order_date,
   vin,
   sum(order_balance_amount) as sumamount  
   from label_order  
  where MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by asc_code,tran_date(order_balance_date),vin,primary_classification) s 
  where sumamount<800
  group by substr(order_date,1,7),asc_code,primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.31(799元以下事故_二级)
def kpi_b_level_kpi_1_31(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_level partition(mon_p='${mon_p}',kpi='kpi_1_31')
  select ----'1.31' as kpi,
  primary_classification,
  second_level_classification,
  substr(order_date,1,7) as mon,
  count(*) as cnt,
  asc_code
   from (
   select asc_code,
   primary_classification,
   second_level_classification,
   tran_date(order_balance_date) as order_date,
   vin,
   sum(order_balance_amount) as sumamount  
   from label_order  
  where MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by asc_code,tran_date(order_balance_date),vin,primary_classification,second_level_classification) s 
  where sumamount<800
  group by substr(order_date,1,7),asc_code,primary_classification,second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.31(799元以下事故_车龄)
def kpi_agetype_kpi_1_31(begin_day, end_day):
    line = """  insert overwrite table  kpi_agetype partition(mon_p='${mon_p}',kpi='kpi_1_31')
  select ----'1.31' as kpi,
  age_type,
  substr(order_date,1,7) as mon,
  count(*) as cnt,
  asc_code
   from (
   select asc_code,
   age_type,
   tran_date(order_balance_date) as order_date,
   vin,
   sum(order_balance_amount) as sumamount  
   from label_order  
  where MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by asc_code,tran_date(order_balance_date),vin,age_type) s 
  where sumamount<800
  group by substr(order_date,1,7),asc_code,age_type"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.31(799元以下事故_保内外)
def kpi_bnbw_kpi_1_31(begin_day, end_day):
    line = """  insert overwrite table  kpi_bnbw partition(mon_p='${mon_p}',kpi='kpi_1_31')
  select ----'1.31' as kpi,
  bn,
  substr(order_date,1,7) as mon,
  count(*) as cnt,
  asc_code
   from (
   select asc_code,
   bn,
   tran_date(order_balance_date) as order_date,
   vin,
   sum(order_balance_amount) as sumamount  
   from label_order  
  where MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by asc_code,tran_date(order_balance_date),vin,bn) s 
  where sumamount<800
  group by substr(order_date,1,7),asc_code,bn"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.31(799元以下事故_一级_车龄)
def kpi_a_agetype_kpi_1_31(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_agetype partition(mon_p='${mon_p}',kpi='kpi_1_31')
  select ----'1.31' as kpi,
  primary_classification,
  age_type,
  substr(order_date,1,7) as mon,
  count(*) as cnt,
  asc_code
   from (
   select asc_code,
   primary_classification,
   age_type,
   tran_date(order_balance_date) as order_date,
   vin,
   sum(order_balance_amount) as sumamount  
   from label_order  
  where MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by asc_code,tran_date(order_balance_date),vin,age_type,primary_classification) s 
  where sumamount<800
  group by substr(order_date,1,7),asc_code,age_type,primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.31(799元以下事故_一级_保内外)
def kpi_a_bnbw_kpi_1_31(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_bnbw partition(mon_p='${mon_p}',kpi='kpi_1_31')
  select ----'1.31' as kpi,
  primary_classification,
  bn,
  substr(order_date,1,7) as mon,
  count(*) as cnt,
  asc_code
   from (
   select asc_code,
   primary_classification,
   bn,
   tran_date(order_balance_date) as order_date,
   vin,
   sum(order_balance_amount) as sumamount  
   from label_order  
  where MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by asc_code,tran_date(order_balance_date),vin,bn,primary_classification) s 
  where sumamount<800
  group by substr(order_date,1,7),asc_code,bn,primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.31(799元以下事故_二级_车龄)
def kpi_b_agetype_kpi_1_31(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_agetype partition(mon_p='${mon_p}',kpi='kpi_1_31')
  select ----'1.31' as kpi,
  primary_classification,
  second_level_classification,
  age_type,
  substr(order_date,1,7) as mon,
  count(*) as cnt,
  asc_code
   from (
   select asc_code,
   primary_classification,
   second_level_classification,
   age_type,
   tran_date(order_balance_date) as order_date,
   vin,
   sum(order_balance_amount) as sumamount  
   from label_order  
  where MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by asc_code,tran_date(order_balance_date),vin,age_type,primary_classification,second_level_classification) s 
  where sumamount<800
  group by substr(order_date,1,7),asc_code,age_type,primary_classification,second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.31(799元以下事故_二级_保内外)
def kpi_b_bnbw_kpi_1_31(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_bnbw partition(mon_p='${mon_p}',kpi='kpi_1_31')
  select ----'1.31' as kpi,
  primary_classification,
  second_level_classification,
  bn,
  substr(order_date,1,7) as mon,
  count(*) as cnt,
  asc_code
   from (
   select asc_code,
   primary_classification,
   second_level_classification,
   bn,
   tran_date(order_balance_date) as order_date,
   vin,
   sum(order_balance_amount) as sumamount  
   from label_order  
  where MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by asc_code,tran_date(order_balance_date),vin,bn,primary_classification,second_level_classification) s 
  where sumamount<800
  group by substr(order_date,1,7),asc_code,bn,primary_classification,second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.32(800-1999元事故_total)
def kpi_total_kpi_1_32(begin_day, end_day):
    line = """  insert overwrite table  kpi_total partition(mon_p='${mon_p}',kpi='kpi_1_32')
  select ----'1.32' as kpi,
  substr(order_date,1,7) as mon,
  count(*) as cnt,
  asc_code
   from (
   select asc_code,
   tran_date(order_balance_date) as order_date,
   vin,
   sum(order_balance_amount) as sumamount  
   from label_order  
  where MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by asc_code,tran_date(order_balance_date),vin
      ) s 
  where sumamount>=800 and sumamount<2000 
  group by substr(order_date,1,7),asc_code"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.32(800-1999元事故_一级)
def kpi_a_level_kpi_1_32(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_level partition(mon_p='${mon_p}',kpi='kpi_1_32')
  select ----'1.32' as kpi,
  primary_classification,
  substr(order_date,1,7) as mon,
  count(*) as cnt,
  asc_code
   from (
   select asc_code,
   primary_classification,
   tran_date(order_balance_date) as order_date,
   vin,
   sum(order_balance_amount) as sumamount  
   from label_order  
  where MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by asc_code,tran_date(order_balance_date),vin,primary_classification) s 
  where sumamount>=800 and sumamount<2000 
  group by substr(order_date,1,7),asc_code,primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.32(800-1999元事故_二级)
def kpi_b_level_kpi_1_32(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_level partition(mon_p='${mon_p}',kpi='kpi_1_32')
  select ----'1.32' as kpi,
  primary_classification,
  second_level_classification,
  substr(order_date,1,7) as mon,
  count(*) as cnt,
  asc_code
   from (
   select asc_code,
   primary_classification,
   second_level_classification,
   tran_date(order_balance_date) as order_date,
   vin,
   sum(order_balance_amount) as sumamount  
   from label_order  
  where MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by asc_code,tran_date(order_balance_date),vin,primary_classification,second_level_classification) s 
  where sumamount>=800 and sumamount<2000 
  group by substr(order_date,1,7),asc_code,primary_classification,second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.32(800-1999元事故_车龄)
def kpi_agetype_kpi_1_32(begin_day, end_day):
    line = """  insert overwrite table  kpi_agetype partition(mon_p='${mon_p}',kpi='kpi_1_32')
  select ----'1.32' as kpi,
  age_type,
  substr(order_date,1,7) as mon,
  count(*) as cnt,
  asc_code
   from (
   select asc_code,
   age_type,
   tran_date(order_balance_date) as order_date,
   vin,
   sum(order_balance_amount) as sumamount  
   from label_order  
  where MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by asc_code,tran_date(order_balance_date),vin,age_type) s 
  where sumamount>=800 and sumamount<2000 
  group by substr(order_date,1,7),asc_code,age_type"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.32(800-1999元事故_保内外)
def kpi_bnbw_kpi_1_32(begin_day, end_day):
    line = """  insert overwrite table  kpi_bnbw partition(mon_p='${mon_p}',kpi='kpi_1_32')
  select ----'1.32' as kpi,
  bn,
  substr(order_date,1,7) as mon,
  count(*) as cnt,
  asc_code
   from (
   select asc_code,
   bn,
   tran_date(order_balance_date) as order_date,
   vin,
   sum(order_balance_amount) as sumamount  
   from label_order  
  where MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by asc_code,tran_date(order_balance_date),vin,bn) s 
  where sumamount>=800 and sumamount<2000 
  group by substr(order_date,1,7),asc_code,bn"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.32(800-1999元事故_一级_车龄)
def kpi_a_agetype_kpi_1_32(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_agetype partition(mon_p='${mon_p}',kpi='kpi_1_32')
  select ----'1.32' as kpi,
  primary_classification,
  age_type,
  substr(order_date,1,7) as mon,
  count(*) as cnt,
  asc_code
   from (
   select asc_code,
   primary_classification,
   age_type,
   tran_date(order_balance_date) as order_date,
   vin,
   sum(order_balance_amount) as sumamount  
   from label_order  
  where MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by asc_code,tran_date(order_balance_date),vin,age_type,primary_classification) s 
  where sumamount>=800 and sumamount<2000 
  group by substr(order_date,1,7),asc_code,age_type,primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.32(800-1999元事故_一级_保内外)
def kpi_a_bnbw_kpi_1_32(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_bnbw partition(mon_p='${mon_p}',kpi='kpi_1_32')
  select ----'1.32' as kpi,
  primary_classification,
  bn,
  substr(order_date,1,7) as mon,
  count(*) as cnt,
  asc_code
   from (
   select asc_code,
   primary_classification,
   bn,
   tran_date(order_balance_date) as order_date,
   vin,
   sum(order_balance_amount) as sumamount  
   from label_order  
  where MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by asc_code,tran_date(order_balance_date),vin,bn,primary_classification) s 
  where sumamount>=800 and sumamount<2000 
  group by substr(order_date,1,7),asc_code,bn,primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.32(800-1999元事故_二级_车龄)
def kpi_b_agetype_kpi_1_32(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_agetype partition(mon_p='${mon_p}',kpi='kpi_1_32')
  select ----'1.32' as kpi,
  primary_classification,
  second_level_classification,
  age_type,
  substr(order_date,1,7) as mon,
  count(*) as cnt,
  asc_code
   from (
   select asc_code,
   primary_classification,
   second_level_classification,
   age_type,
   tran_date(order_balance_date) as order_date,
   vin,
   sum(order_balance_amount) as sumamount  
   from label_order  
  where MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by asc_code,tran_date(order_balance_date),vin,age_type,primary_classification,second_level_classification) s 
  where sumamount>=800 and sumamount<2000 
  group by substr(order_date,1,7),asc_code,age_type,primary_classification,second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.32(800-1999元事故_二级_保内外)
def kpi_b_bnbw_kpi_1_32(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_bnbw partition(mon_p='${mon_p}',kpi='kpi_1_32')
  select ----'1.32' as kpi,
  primary_classification,
  second_level_classification,
  bn,
  substr(order_date,1,7) as mon,
  count(*) as cnt,
  asc_code
   from (
   select asc_code,
   primary_classification,
   second_level_classification,
   bn,
   tran_date(order_balance_date) as order_date,
   vin,
   sum(order_balance_amount) as sumamount  
   from label_order  
  where MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by asc_code,tran_date(order_balance_date),vin,bn,primary_classification,second_level_classification) s 
  where sumamount>=800 and sumamount<2000 
  group by substr(order_date,1,7),asc_code,bn,primary_classification,second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.33(2000-4999元事故_total)
def kpi_total_kpi_1_33(begin_day, end_day):
    line = """  insert overwrite table  kpi_total partition(mon_p='${mon_p}',kpi='kpi_1_33')
  select ----'1.33' as kpi,
  substr(order_date,1,7) as mon,
  count(*) as cnt,
  asc_code
   from (
   select asc_code,
   tran_date(order_balance_date) as order_date,
   vin,
   sum(order_balance_amount) as sumamount  
   from label_order  
  where MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by asc_code,tran_date(order_balance_date),vin
      ) s 
  where sumamount>=2000 and sumamount<5000 
  group by substr(order_date,1,7),asc_code"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.33(2000-4999元事故_一级)
def kpi_a_level_kpi_1_33(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_level partition(mon_p='${mon_p}',kpi='kpi_1_33')
  select ----'1.33' as kpi,
  primary_classification,
  substr(order_date,1,7) as mon,
  count(*) as cnt,
  asc_code
   from (
   select asc_code,
   primary_classification,
   tran_date(order_balance_date) as order_date,
   vin,
   sum(order_balance_amount) as sumamount  
   from label_order  
  where MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by asc_code,tran_date(order_balance_date),vin,primary_classification) s 
  where sumamount>=2000 and sumamount<5000 
  group by substr(order_date,1,7),asc_code,primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.33(2000-4999元事故_二级)
def kpi_b_level_kpi_1_33(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_level partition(mon_p='${mon_p}',kpi='kpi_1_33')
  select ----'1.33' as kpi,
  primary_classification,
  second_level_classification,
  substr(order_date,1,7) as mon,
  count(*) as cnt,
  asc_code
   from (
   select asc_code,
   primary_classification,
   second_level_classification,
   tran_date(order_balance_date) as order_date,
   vin,
   sum(order_balance_amount) as sumamount  
   from label_order  
  where MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by asc_code,tran_date(order_balance_date),vin,primary_classification,second_level_classification) s 
  where sumamount>=2000 and sumamount<5000 
  group by substr(order_date,1,7),asc_code,primary_classification,second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.33(2000-4999元事故_车龄)
def kpi_agetype_kpi_1_33(begin_day, end_day):
    line = """  insert overwrite table  kpi_agetype partition(mon_p='${mon_p}',kpi='kpi_1_33')
  select ----'1.33' as kpi,
  age_type,
  substr(order_date,1,7) as mon,
  count(*) as cnt,
  asc_code
   from (
   select asc_code,
   age_type,
   tran_date(order_balance_date) as order_date,
   vin,
   sum(order_balance_amount) as sumamount  
   from label_order  
  where MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by asc_code,tran_date(order_balance_date),vin,age_type) s 
  where sumamount>=2000 and sumamount<5000 
  group by substr(order_date,1,7),asc_code,age_type"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.33(2000-4999元事故_保内外)
def kpi_bnbw_kpi_1_33(begin_day, end_day):
    line = """  insert overwrite table  kpi_bnbw partition(mon_p='${mon_p}',kpi='kpi_1_33')
  select ----'1.33' as kpi,
  bn,
  substr(order_date,1,7) as mon,
  count(*) as cnt,
  asc_code
   from (
   select asc_code,
   bn,
   tran_date(order_balance_date) as order_date,
   vin,
   sum(order_balance_amount) as sumamount  
   from label_order  
  where MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by asc_code,tran_date(order_balance_date),vin,bn) s 
  where sumamount>=2000 and sumamount<5000 
  group by substr(order_date,1,7),asc_code,bn"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.33(2000-4999元事故_一级_车龄)
def kpi_a_agetype_kpi_1_33(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_agetype partition(mon_p='${mon_p}',kpi='kpi_1_33')
  select ----'1.33' as kpi,
  primary_classification,
  age_type,
  substr(order_date,1,7) as mon,
  count(*) as cnt,
  asc_code
   from (
   select asc_code,
   primary_classification,
   age_type,
   tran_date(order_balance_date) as order_date,
   vin,
   sum(order_balance_amount) as sumamount  
   from label_order  
  where MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by asc_code,tran_date(order_balance_date),vin,age_type,primary_classification) s 
  where sumamount>=2000 and sumamount<5000 
  group by substr(order_date,1,7),asc_code,age_type,primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.33(2000-4999元事故_一级_保内外)
def kpi_a_bnbw_kpi_1_33(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_bnbw partition(mon_p='${mon_p}',kpi='kpi_1_33')
  select ----'1.33' as kpi,
  primary_classification,
  bn,
  substr(order_date,1,7) as mon,
  count(*) as cnt,
  asc_code
   from (
   select asc_code,
   primary_classification,
   bn,
   tran_date(order_balance_date) as order_date,
   vin,
   sum(order_balance_amount) as sumamount  
   from label_order  
  where MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by asc_code,tran_date(order_balance_date),vin,bn,primary_classification) s 
  where sumamount>=2000 and sumamount<5000 
  group by substr(order_date,1,7),asc_code,bn,primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.33(2000-4999元事故_二级_车龄)
def kpi_b_agetype_kpi_1_33(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_agetype partition(mon_p='${mon_p}',kpi='kpi_1_33')
  select ----'1.33' as kpi,
  primary_classification,
  second_level_classification,
  age_type,
  substr(order_date,1,7) as mon,
  count(*) as cnt,
  asc_code
   from (
   select asc_code,
   primary_classification,
   second_level_classification,
   age_type,
   tran_date(order_balance_date) as order_date,
   vin,
   sum(order_balance_amount) as sumamount  
   from label_order  
  where MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by asc_code,tran_date(order_balance_date),vin,age_type,primary_classification,second_level_classification) s 
  where sumamount>=2000 and sumamount<5000 
  group by substr(order_date,1,7),asc_code,age_type,primary_classification,second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.33(2000-4999元事故_二级_保内外)
def kpi_b_bnbw_kpi_1_33(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_bnbw partition(mon_p='${mon_p}',kpi='kpi_1_33')
  select ----'1.33' as kpi,
  primary_classification,
  second_level_classification,
  bn,
  substr(order_date,1,7) as mon,
  count(*) as cnt,
  asc_code
   from (
   select asc_code,
   primary_classification,
   second_level_classification,
   bn,
   tran_date(order_balance_date) as order_date,
   vin,
   sum(order_balance_amount) as sumamount  
   from label_order  
  where MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by asc_code,tran_date(order_balance_date),vin,bn,primary_classification,second_level_classification) s 
  where sumamount>=2000 and sumamount<5000 
  group by substr(order_date,1,7),asc_code,bn,primary_classification,second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.34(5000-9999元事故_total)
def kpi_total_kpi_1_34(begin_day, end_day):
    line = """  insert overwrite table  kpi_total partition(mon_p='${mon_p}',kpi='kpi_1_34')
  select ----'1.34' as kpi,
  substr(order_date,1,7) as mon,
  count(*) as cnt,
  asc_code
   from (
   select asc_code,
   tran_date(order_balance_date) as order_date,
   vin,
   sum(order_balance_amount) as sumamount  
   from label_order  
  where MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by asc_code,tran_date(order_balance_date),vin
      ) s 
  where sumamount>=5000 and sumamount<10000 
  group by substr(order_date,1,7),asc_code"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.34(5000-9999元事故_一级)
def kpi_a_level_kpi_1_34(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_level partition(mon_p='${mon_p}',kpi='kpi_1_34')
  select ----'1.34' as kpi,
  primary_classification,
  substr(order_date,1,7) as mon,
  count(*) as cnt,
  asc_code
   from (
   select asc_code,
   primary_classification,
   tran_date(order_balance_date) as order_date,
   vin,
   sum(order_balance_amount) as sumamount  
   from label_order  
  where MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by asc_code,tran_date(order_balance_date),vin,primary_classification) s 
  where sumamount>=5000 and sumamount<10000 
  group by substr(order_date,1,7),asc_code,primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.34(5000-9999元事故_二级)
def kpi_b_level_kpi_1_34(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_level partition(mon_p='${mon_p}',kpi='kpi_1_34')
  select ----'1.34' as kpi,
  primary_classification,
  second_level_classification,
  substr(order_date,1,7) as mon,
  count(*) as cnt,
  asc_code
   from (
   select asc_code,
   primary_classification,
   second_level_classification,
   tran_date(order_balance_date) as order_date,
   vin,
   sum(order_balance_amount) as sumamount  
   from label_order  
  where MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by asc_code,tran_date(order_balance_date),vin,primary_classification,second_level_classification) s 
  where sumamount>=5000 and sumamount<10000 
  group by substr(order_date,1,7),asc_code,primary_classification,second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.34(5000-9999元事故_车龄)
def kpi_agetype_kpi_1_34(begin_day, end_day):
    line = """  insert overwrite table  kpi_agetype partition(mon_p='${mon_p}',kpi='kpi_1_34')
  select ----'1.34' as kpi,
  age_type,
  substr(order_date,1,7) as mon,
  count(*) as cnt,
  asc_code
   from (
   select asc_code,
   age_type,
   tran_date(order_balance_date) as order_date,
   vin,
   sum(order_balance_amount) as sumamount  
   from label_order  
  where MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by asc_code,tran_date(order_balance_date),vin,age_type) s 
  where sumamount>=5000 and sumamount<10000 
  group by substr(order_date,1,7),asc_code,age_type"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.34(5000-9999元事故_保内外)
def kpi_bnbw_kpi_1_34(begin_day, end_day):
    line = """  insert overwrite table  kpi_bnbw partition(mon_p='${mon_p}',kpi='kpi_1_34')
  select ----'1.34' as kpi,
  bn,
  substr(order_date,1,7) as mon,
  count(*) as cnt,
  asc_code
   from (
   select asc_code,
   bn,
   tran_date(order_balance_date) as order_date,
   vin,
   sum(order_balance_amount) as sumamount  
   from label_order  
  where MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by asc_code,tran_date(order_balance_date),vin,bn) s 
  where sumamount>=5000 and sumamount<10000 
  group by substr(order_date,1,7),asc_code,bn"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.34(5000-9999元事故_一级_车龄)
def kpi_a_agetype_kpi_1_34(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_agetype partition(mon_p='${mon_p}',kpi='kpi_1_34')
  select ----'1.34' as kpi,
  primary_classification,
  age_type,
  substr(order_date,1,7) as mon,
  count(*) as cnt,
  asc_code
   from (
   select asc_code,
   primary_classification,
   age_type,
   tran_date(order_balance_date) as order_date,
   vin,
   sum(order_balance_amount) as sumamount  
   from label_order  
  where MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by asc_code,tran_date(order_balance_date),vin,age_type,primary_classification) s 
  where sumamount>=5000 and sumamount<10000 
  group by substr(order_date,1,7),asc_code,age_type,primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.34(5000-9999元事故_一级_保内外)
def kpi_a_bnbw_kpi_1_34(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_bnbw partition(mon_p='${mon_p}',kpi='kpi_1_34')
  select ----'1.34' as kpi,
  primary_classification,
  bn,
  substr(order_date,1,7) as mon,
  count(*) as cnt,
  asc_code
   from (
   select asc_code,
   primary_classification,
   bn,
   tran_date(order_balance_date) as order_date,
   vin,
   sum(order_balance_amount) as sumamount  
   from label_order  
  where MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by asc_code,tran_date(order_balance_date),vin,bn,primary_classification) s 
  where sumamount>=5000 and sumamount<10000 
  group by substr(order_date,1,7),asc_code,bn,primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.34(5000-9999元事故_二级_车龄)
def kpi_b_agetype_kpi_1_34(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_agetype partition(mon_p='${mon_p}',kpi='kpi_1_34')
  select ----'1.34' as kpi,
  primary_classification,
  second_level_classification,
  age_type,
  substr(order_date,1,7) as mon,
  count(*) as cnt,
  asc_code
   from (
   select asc_code,
   primary_classification,
   second_level_classification,
   age_type,
   tran_date(order_balance_date) as order_date,
   vin,
   sum(order_balance_amount) as sumamount  
   from label_order  
  where MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by asc_code,tran_date(order_balance_date),vin,age_type,primary_classification,second_level_classification) s 
  where sumamount>=5000 and sumamount<10000 
  group by substr(order_date,1,7),asc_code,age_type,primary_classification,second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.34(5000-9999元事故_二级_保内外)
def kpi_b_bnbw_kpi_1_34(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_bnbw partition(mon_p='${mon_p}',kpi='kpi_1_34')
  select ----'1.34' as kpi,
  primary_classification,
  second_level_classification,
  bn,
  substr(order_date,1,7) as mon,
  count(*) as cnt,
  asc_code
   from (
   select asc_code,
   primary_classification,
   second_level_classification,
   bn,
   tran_date(order_balance_date) as order_date,
   vin,
   sum(order_balance_amount) as sumamount  
   from label_order  
  where MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by asc_code,tran_date(order_balance_date),vin,bn,primary_classification,second_level_classification) s 
  where sumamount>=5000 and sumamount<10000 
  group by substr(order_date,1,7),asc_code,bn,primary_classification,second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.35(10000元以上事故_total)
def kpi_total_kpi_1_35(begin_day, end_day):
    line = """  insert overwrite table  kpi_total partition(mon_p='${mon_p}',kpi='kpi_1_35')
  select ----'1.35' as kpi,
  substr(order_date,1,7) as mon,
  count(*) as cnt,
  asc_code
   from (
   select asc_code,
   tran_date(order_balance_date) as order_date,
   vin,
   sum(order_balance_amount) as sumamount  
   from label_order  
  where MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by asc_code,tran_date(order_balance_date),vin
      ) s 
  where sumamount>=10000 
  group by substr(order_date,1,7),asc_code"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.35(10000元以上事故_一级)
def kpi_a_level_kpi_1_35(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_level partition(mon_p='${mon_p}',kpi='kpi_1_35')
  select ----'1.35' as kpi,
  primary_classification,
  substr(order_date,1,7) as mon,
  count(*) as cnt,
  asc_code
   from (
   select asc_code,
   primary_classification,
   tran_date(order_balance_date) as order_date,
   vin,
   sum(order_balance_amount) as sumamount  
   from label_order  
  where MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by asc_code,tran_date(order_balance_date),vin,primary_classification) s 
  where sumamount>=10000 
  group by substr(order_date,1,7),asc_code,primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.35(10000元以上事故_二级)
def kpi_b_level_kpi_1_35(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_level partition(mon_p='${mon_p}',kpi='kpi_1_35')
  select ----'1.35' as kpi,
  primary_classification,
  second_level_classification,
  substr(order_date,1,7) as mon,
  count(*) as cnt,
  asc_code
   from (
   select asc_code,
   primary_classification,
   second_level_classification,
   tran_date(order_balance_date) as order_date,
   vin,
   sum(order_balance_amount) as sumamount  
   from label_order  
  where MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by asc_code,tran_date(order_balance_date),vin,primary_classification,second_level_classification) s 
  where sumamount>=10000 
  group by substr(order_date,1,7),asc_code,primary_classification,second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.35(10000元以上事故_车龄)
def kpi_agetype_kpi_1_35(begin_day, end_day):
    line = """  insert overwrite table  kpi_agetype partition(mon_p='${mon_p}',kpi='kpi_1_35')
  select ----'1.35' as kpi,
  age_type,
  substr(order_date,1,7) as mon,
  count(*) as cnt,
  asc_code
   from (
   select asc_code,
   age_type,
   tran_date(order_balance_date) as order_date,
   vin,
   sum(order_balance_amount) as sumamount  
   from label_order  
  where MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by asc_code,tran_date(order_balance_date),vin,age_type) s 
  where sumamount>=10000 
  group by substr(order_date,1,7),asc_code,age_type"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.35(10000元以上事故_保内外)
def kpi_bnbw_kpi_1_35(begin_day, end_day):
    line = """  insert overwrite table  kpi_bnbw partition(mon_p='${mon_p}',kpi='kpi_1_35')
  select ----'1.35' as kpi,
  bn,
  substr(order_date,1,7) as mon,
  count(*) as cnt,
  asc_code
   from (
   select asc_code,
   bn,
   tran_date(order_balance_date) as order_date,
   vin,
   sum(order_balance_amount) as sumamount  
   from label_order  
  where MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by asc_code,tran_date(order_balance_date),vin,bn) s 
  where sumamount>=10000 
  group by substr(order_date,1,7),asc_code,bn"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.35(10000元以上事故_一级_车龄)
def kpi_a_agetype_kpi_1_35(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_agetype partition(mon_p='${mon_p}',kpi='kpi_1_35')
  select ----'1.35' as kpi,
  primary_classification,
  age_type,
  substr(order_date,1,7) as mon,
  count(*) as cnt,
  asc_code
   from (
   select asc_code,
   primary_classification,
   age_type,
   tran_date(order_balance_date) as order_date,
   vin,
   sum(order_balance_amount) as sumamount  
   from label_order  
  where MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by asc_code,tran_date(order_balance_date),vin,age_type,primary_classification) s 
  where sumamount>=10000 
  group by substr(order_date,1,7),asc_code,age_type,primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.35(10000元以上事故_一级_保内外)
def kpi_a_bnbw_kpi_1_35(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_bnbw partition(mon_p='${mon_p}',kpi='kpi_1_35')
  select ----'1.35' as kpi,
  primary_classification,
  bn,
  substr(order_date,1,7) as mon,
  count(*) as cnt,
  asc_code
   from (
   select asc_code,
   primary_classification,
   bn,
   tran_date(order_balance_date) as order_date,
   vin,
   sum(order_balance_amount) as sumamount  
   from label_order  
  where MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by asc_code,tran_date(order_balance_date),vin,bn,primary_classification) s 
  where sumamount>=10000 
  group by substr(order_date,1,7),asc_code,bn,primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.35(10000元以上事故_二级_车龄)
def kpi_b_agetype_kpi_1_35(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_agetype partition(mon_p='${mon_p}',kpi='kpi_1_35')
  select ----'1.35' as kpi,
  primary_classification,
  second_level_classification,
  age_type,
  substr(order_date,1,7) as mon,
  count(*) as cnt,
  asc_code
   from (
   select asc_code,
   primary_classification,
   second_level_classification,
   age_type,
   tran_date(order_balance_date) as order_date,
   vin,
   sum(order_balance_amount) as sumamount  
   from label_order  
  where MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by asc_code,tran_date(order_balance_date),vin,age_type,primary_classification,second_level_classification) s 
  where sumamount>=10000 
  group by substr(order_date,1,7),asc_code,age_type,primary_classification,second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.35(10000元以上事故_二级_保内外)
def kpi_b_bnbw_kpi_1_35(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_bnbw partition(mon_p='${mon_p}',kpi='kpi_1_35')
  select ----'1.35' as kpi,
  primary_classification,
  second_level_classification,
  bn,
  substr(order_date,1,7) as mon,
  count(*) as cnt,
  asc_code
   from (
   select asc_code,
   primary_classification,
   second_level_classification,
   bn,
   tran_date(order_balance_date) as order_date,
   vin,
   sum(order_balance_amount) as sumamount  
   from label_order  
  where MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by asc_code,tran_date(order_balance_date),vin,bn,primary_classification,second_level_classification) s 
  where sumamount>=10000 
  group by substr(order_date,1,7),asc_code,bn,primary_classification,second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.36(事故台次占比_total)
def kpi_total_kpi_1_36(begin_day, end_day):
    line = """  insert overwrite table  kpi_total partition(mon_p='${mon_p}',kpi='kpi_1_36')
  select ----'1.36' as kpi,
   s.mon,
   s.num/t.num as num,
   s.asc_code
  from (
  select * from kpi_total 
  where kpi='kpi_1_3'
    and mon>='${begin_date}'
    and mon<='${end_date}'
   ) s,(
  select * from kpi_total 
  where kpi='kpi_1'
    and mon>='${begin_date}'
    and mon<='${end_date}'
   ) t 
  where s.asc_code=t.asc_code
    and s.mon=t.mon """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.36(事故台次占比_一级)
def kpi_a_level_kpi_1_36(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_level partition(mon_p='${mon_p}',kpi='kpi_1_36')
  select ----'1.36' as kpi,
   s.a_level,
   s.mon,
   s.num/t.num as num,
   s.asc_code
  from (
  select * from kpi_a_level 
  where kpi='kpi_1_3'
    and mon>='${begin_date}'
    and mon<='${end_date}'
   ) s,(
  select * from kpi_a_level 
  where kpi='kpi_1'
    and mon>='${begin_date}'
    and mon<='${end_date}'
   ) t 
  where s.asc_code=t.asc_code
    and s.mon=t.mon 
    and s.a_level=t.a_level """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.36(事故台次占比_二级)
def kpi_b_level_kpi_1_36(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_level partition(mon_p='${mon_p}',kpi='kpi_1_36')
  select ----'1.36' as kpi,
   s.a_level,
   s.b_level,
   s.mon,
   s.num/t.num as num,
   s.asc_code
  from (
  select * from kpi_b_level 
  where kpi='kpi_1_3'
    and mon>='${begin_date}'
    and mon<='${end_date}'
   ) s,(
  select * from kpi_b_level 
  where kpi='kpi_1'
    and mon>='${begin_date}'
    and mon<='${end_date}'
   ) t 
  where s.asc_code=t.asc_code
    and s.mon=t.mon 
    and s.a_level=t.a_level 
    and s.b_level=t.b_level"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.36(事故台次占比_车龄)
def kpi_agetype_kpi_1_36(begin_day, end_day):
    line = """  insert overwrite table  kpi_agetype partition(mon_p='${mon_p}',kpi='kpi_1_36')
  select ----'1.36' as kpi,
   s.age_type,
   s.mon,
   s.num/t.num as num,
   s.asc_code
  from (
  select * from kpi_agetype 
  where kpi='kpi_1_3'
    and mon>='${begin_date}'
    and mon<='${end_date}'
   ) s,(
  select * from kpi_agetype 
  where kpi='kpi_1'
    and mon>='${begin_date}'
    and mon<='${end_date}'
   ) t 
  where s.asc_code=t.asc_code
    and s.mon=t.mon 
    and s.age_type=t.age_type """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.36(事故台次占比_保内外)
def kpi_bnbw_kpi_1_36(begin_day, end_day):
    line = """  insert overwrite table  kpi_bnbw partition(mon_p='${mon_p}',kpi='kpi_1_36')
  select ----'1.36' as kpi,
   s.bnbw,
   s.mon,
   s.num/t.num as num,
   s.asc_code
  from (
  select * from kpi_bnbw 
  where kpi='kpi_1_3'
    and mon>='${begin_date}'
    and mon<='${end_date}'
   ) s,(
  select * from kpi_bnbw 
  where kpi='kpi_1'
    and mon>='${begin_date}'
    and mon<='${end_date}'
   ) t 
  where s.asc_code=t.asc_code
    and s.mon=t.mon 
    and s.bnbw=t.bnbw """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.36(事故台次占比_一级_车龄)
def kpi_a_agetype_kpi_1_36(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_agetype partition(mon_p='${mon_p}',kpi='kpi_1_36')
  select ----'1.36' as kpi,
   s.a_level,
   s.age_type,
   s.mon,
   s.num/t.num as num,
   s.asc_code
  from (
  select * from kpi_a_agetype 
  where kpi='kpi_1_3'
    and mon>='${begin_date}'
    and mon<='${end_date}'
   ) s,(
  select * from kpi_a_agetype 
  where kpi='kpi_1'
    and mon>='${begin_date}'
    and mon<='${end_date}'
   ) t 
  where s.asc_code=t.asc_code
    and s.mon=t.mon 
    and s.a_level=t.a_level
    and s.age_type=t.age_type """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.36(事故台次占比_一级_保内外)
def kpi_a_bnbw_kpi_1_36(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_bnbw partition(mon_p='${mon_p}',kpi='kpi_1_36')
  select ----'1.36' as kpi,
   s.a_level,
   s.bnbw,
   s.mon,
   s.num/t.num as num,
   s.asc_code
  from (
  select * from kpi_a_bnbw 
  where kpi='kpi_1_3'
    and mon>='${begin_date}'
    and mon<='${end_date}'
   ) s,(
  select * from kpi_a_bnbw 
  where kpi='kpi_1'
    and mon>='${begin_date}'
    and mon<='${end_date}'
   ) t 
  where s.asc_code=t.asc_code
    and s.mon=t.mon 
    and s.a_level=t.a_level
    and s.bnbw=t.bnbw """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.36(事故台次占比_二级_车龄)
def kpi_b_agetype_kpi_1_36(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_agetype partition(mon_p='${mon_p}',kpi='kpi_1_36')
  select ----'1.36' as kpi,
   s.a_level,
   s.b_level,
   s.age_type,
   s.mon,
   s.num/t.num as num,
   s.asc_code
  from (
  select * from kpi_b_agetype 
  where kpi='kpi_1_3'
    and mon>='${begin_date}'
    and mon<='${end_date}'
   ) s,(
  select * from kpi_b_agetype 
  where kpi='kpi_1'
    and mon>='${begin_date}'
    and mon<='${end_date}'
   ) t 
  where s.asc_code=t.asc_code
    and s.mon=t.mon 
    and s.a_level=t.a_level
    and s.b_level=t.b_level
    and s.age_type=t.age_type """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.36(事故台次占比_二级_保内外)
def kpi_b_bnbw_kpi_1_36(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_bnbw partition(mon_p='${mon_p}',kpi='kpi_1_36')
  select ----'1.36' as kpi,
   s.a_level,
   s.b_level,
   s.bnbw,
   s.mon,
   s.num/t.num as num,
   s.asc_code
  from (
  select * from kpi_b_bnbw 
  where kpi='kpi_1_3'
    and mon>='${begin_date}'
    and mon<='${end_date}'
   ) s,(
  select * from kpi_b_bnbw 
  where kpi='kpi_1'
    and mon>='${begin_date}'
    and mon<='${end_date}'
   ) t 
  where s.asc_code=t.asc_code
    and s.mon=t.mon 
    and s.a_level=t.a_level
    and s.b_level=t.b_level
    and s.bnbw=t.bnbw """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.4(索赔台次_total)
def kpi_total_kpi_1_4(begin_day, end_day):
    line = """  insert overwrite table  kpi_total partition(mon_p='${mon_p}',kpi='kpi_1_4')
  select ----'1.4' as kpi,
  substr(tran_date(order_balance_date),1,7) as mon,
  count(distinct vin,tran_date(order_balance_date)) as cnt,
  asc_code
   from label_order  
  where claim='1'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(order_balance_date),1,7),asc_code"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.4(索赔台次_一级)
def kpi_a_level_kpi_1_4(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_level partition(mon_p='${mon_p}',kpi='kpi_1_4')
  select ----'1.4' as kpi,
  primary_classification,
  substr(tran_date(order_balance_date),1,7) as mon,
  count(distinct vin,tran_date(order_balance_date)) as cnt,
  asc_code
   from label_order  
  where claim='1'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(order_balance_date),1,7),asc_code,primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.4(索赔台次_二级)
def kpi_b_level_kpi_1_4(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_level partition(mon_p='${mon_p}',kpi='kpi_1_4')
  select ----'1.4' as kpi,
  primary_classification,
  second_level_classification,
  substr(tran_date(order_balance_date),1,7) as mon,
  count(distinct vin,tran_date(order_balance_date)) as cnt,
  asc_code
   from label_order  
  where claim='1'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(order_balance_date),1,7),asc_code,primary_classification,second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.4(索赔台次_车龄)
def kpi_agetype_kpi_1_4(begin_day, end_day):
    line = """  insert overwrite table  kpi_agetype partition(mon_p='${mon_p}',kpi='kpi_1_4')
  select ----'1.4' as kpi,
  age_type,
  substr(tran_date(order_balance_date),1,7) as mon,
  count(distinct vin,tran_date(order_balance_date)) as cnt,
  asc_code
   from label_order  
  where claim='1'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(order_balance_date),1,7),asc_code,age_type"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.4(索赔台次_保内外)
def kpi_bnbw_kpi_1_4(begin_day, end_day):
    line = """  insert overwrite table  kpi_bnbw partition(mon_p='${mon_p}',kpi='kpi_1_4')
  select ----'1.4' as kpi,
  bn,
  substr(tran_date(order_balance_date),1,7) as mon,
  count(distinct vin,tran_date(order_balance_date)) as cnt,
  asc_code
   from label_order  
  where claim='1'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(order_balance_date),1,7),asc_code,bn"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.4(索赔台次_一级_车龄)
def kpi_a_agetype_kpi_1_4(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_agetype partition(mon_p='${mon_p}',kpi='kpi_1_4')
  select ----'1.4' as kpi,
  primary_classification,
  age_type,
  substr(tran_date(order_balance_date),1,7) as mon,
  count(distinct vin,tran_date(order_balance_date)) as cnt,
  asc_code
   from label_order  
  where claim='1'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(order_balance_date),1,7),asc_code,primary_classification,age_type"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.4(索赔台次_一级_保内外)
def kpi_a_bnbw_kpi_1_4(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_bnbw partition(mon_p='${mon_p}',kpi='kpi_1_4')
  select ----'1.4' as kpi,
  primary_classification,
  bn,
  substr(tran_date(order_balance_date),1,7) as mon,
  count(distinct vin,tran_date(order_balance_date)) as cnt,
  asc_code
   from label_order  
  where claim='1'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(order_balance_date),1,7),asc_code,primary_classification,bn"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.4(索赔台次_二级_车龄)
def kpi_b_agetype_kpi_1_4(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_agetype partition(mon_p='${mon_p}',kpi='kpi_1_4')
  select ----'1.4' as kpi,
  primary_classification,
  second_level_classification,
  age_type,
  substr(tran_date(order_balance_date),1,7) as mon,
  count(distinct vin,tran_date(order_balance_date)) as cnt,
  asc_code
   from label_order  
  where claim='1'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(order_balance_date),1,7),asc_code,primary_classification,second_level_classification,age_type"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.4(索赔台次_二级_保内外)
def kpi_b_bnbw_kpi_1_4(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_bnbw partition(mon_p='${mon_p}',kpi='kpi_1_4')
  select ----'1.4' as kpi,
  primary_classification,
  second_level_classification,
  bn,
  substr(tran_date(order_balance_date),1,7) as mon,
  count(distinct vin,tran_date(order_balance_date)) as cnt,
  asc_code
   from label_order  
  where claim='1'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(order_balance_date),1,7),asc_code,primary_classification,second_level_classification,bn"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.41(索赔台次占比_total)
def kpi_total_kpi_1_41(begin_day, end_day):
    line = """  insert overwrite table  kpi_total partition(mon_p='${mon_p}',kpi='kpi_1_41')
  select ----'1.41' as kpi,
   s.mon,
   s.num/t.num as num,
   s.asc_code
  from (
  select * from kpi_total 
  where kpi='kpi_1_4'
    and mon>='${begin_date}'
    and mon<='${end_date}'
   ) s,(
  select * from kpi_total 
  where kpi='kpi_1'
    and mon>='${begin_date}'
    and mon<='${end_date}'
   ) t 
  where s.asc_code=t.asc_code
    and s.mon=t.mon """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.41(索赔台次占比_一级)
def kpi_a_level_kpi_1_41(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_level partition(mon_p='${mon_p}',kpi='kpi_1_41')
  select ----'1.41' as kpi,
   s.a_level,
   s.mon,
   s.num/t.num as num,
   s.asc_code
  from (
  select * from kpi_a_level 
  where kpi='kpi_1_4'
    and mon>='${begin_date}'
    and mon<='${end_date}'
   ) s,(
  select * from kpi_a_level 
  where kpi='kpi_1'
    and mon>='${begin_date}'
    and mon<='${end_date}'
   ) t 
  where s.asc_code=t.asc_code
    and s.mon=t.mon 
    and s.a_level=t.a_level """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.41(索赔台次占比_二级)
def kpi_b_level_kpi_1_41(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_level partition(mon_p='${mon_p}',kpi='kpi_1_41')
  select ----'1.41' as kpi,
   s.a_level,
   s.b_level,
   s.mon,
   s.num/t.num as num,
   s.asc_code
  from (
  select * from kpi_b_level 
  where kpi='kpi_1_4'
    and mon>='${begin_date}'
    and mon<='${end_date}'
   ) s,(
  select * from kpi_b_level 
  where kpi='kpi_1'
    and mon>='${begin_date}'
    and mon<='${end_date}'
   ) t 
  where s.asc_code=t.asc_code
    and s.mon=t.mon 
    and s.a_level=t.a_level 
    and s.b_level=t.b_level"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.41(索赔台次占比_车龄)
def kpi_agetype_kpi_1_41(begin_day, end_day):
    line = """  insert overwrite table  kpi_agetype partition(mon_p='${mon_p}',kpi='kpi_1_41')
  select ----'1.41' as kpi,
   s.age_type,
   s.mon,
   s.num/t.num as num,
   s.asc_code
  from (
  select * from kpi_agetype 
  where kpi='kpi_1_4'
    and mon>='${begin_date}'
    and mon<='${end_date}'
   ) s,(
  select * from kpi_agetype 
  where kpi='kpi_1'
    and mon>='${begin_date}'
    and mon<='${end_date}'
   ) t 
  where s.asc_code=t.asc_code
    and s.mon=t.mon 
    and s.age_type=t.age_type """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.41(索赔台次占比_保内外)
def kpi_bnbw_kpi_1_41(begin_day, end_day):
    line = """  insert overwrite table  kpi_bnbw partition(mon_p='${mon_p}',kpi='kpi_1_41')
  select ----'1.41' as kpi,
   s.bnbw,
   s.mon,
   s.num/t.num as num,
   s.asc_code
  from (
  select * from kpi_bnbw 
  where kpi='kpi_1_4'
    and mon>='${begin_date}'
    and mon<='${end_date}'
   ) s,(
  select * from kpi_bnbw 
  where kpi='kpi_1'
    and mon>='${begin_date}'
    and mon<='${end_date}'
   ) t 
  where s.asc_code=t.asc_code
    and s.mon=t.mon 
    and s.bnbw=t.bnbw """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.41(索赔台次占比_一级_车龄)
def kpi_a_agetype_kpi_1_41(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_agetype partition(mon_p='${mon_p}',kpi='kpi_1_41')
  select ----'1.41' as kpi,
   s.a_level,
   s.age_type,
   s.mon,
   s.num/t.num as num,
   s.asc_code
  from (
  select * from kpi_a_agetype 
  where kpi='kpi_1_4'
    and mon>='${begin_date}'
    and mon<='${end_date}'
   ) s,(
  select * from kpi_a_agetype 
  where kpi='kpi_1'
    and mon>='${begin_date}'
    and mon<='${end_date}'
   ) t 
  where s.asc_code=t.asc_code
    and s.mon=t.mon 
    and s.a_level=t.a_level
    and s.age_type=t.age_type """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.41(索赔台次占比_一级_保内外)
def kpi_a_bnbw_kpi_1_41(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_bnbw partition(mon_p='${mon_p}',kpi='kpi_1_41')
  select ----'1.41' as kpi,
   s.a_level,
   s.bnbw,
   s.mon,
   s.num/t.num as num,
   s.asc_code
  from (
  select * from kpi_a_bnbw 
  where kpi='kpi_1_4'
    and mon>='${begin_date}'
    and mon<='${end_date}'
   ) s,(
  select * from kpi_a_bnbw 
  where kpi='kpi_1'
    and mon>='${begin_date}'
    and mon<='${end_date}'
   ) t 
  where s.asc_code=t.asc_code
    and s.mon=t.mon 
    and s.a_level=t.a_level
    and s.bnbw=t.bnbw """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.41(索赔台次占比_二级_车龄)
def kpi_b_agetype_kpi_1_41(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_agetype partition(mon_p='${mon_p}',kpi='kpi_1_41')
  select ----'1.41' as kpi,
   s.a_level,
   s.b_level,
   s.age_type,
   s.mon,
   s.num/t.num as num,
   s.asc_code
  from (
  select * from kpi_b_agetype 
  where kpi='kpi_1_4'
    and mon>='${begin_date}'
    and mon<='${end_date}'
   ) s,(
  select * from kpi_b_agetype 
  where kpi='kpi_1'
    and mon>='${begin_date}'
    and mon<='${end_date}'
   ) t 
  where s.asc_code=t.asc_code
    and s.mon=t.mon 
    and s.a_level=t.a_level
    and s.b_level=t.b_level
    and s.age_type=t.age_type """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_1.41(索赔台次占比_二级_保内外)
def kpi_b_bnbw_kpi_1_41(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_bnbw partition(mon_p='${mon_p}',kpi='kpi_1_41')
  select ----'1.41' as kpi,
   s.a_level,
   s.b_level,
   s.bnbw,
   s.mon,
   s.num/t.num as num,
   s.asc_code
  from (
  select * from kpi_b_bnbw 
  where kpi='kpi_1_4'
    and mon>='${begin_date}'
    and mon<='${end_date}'
   ) s,(
  select * from kpi_b_bnbw 
  where kpi='kpi_1'
    and mon>='${begin_date}'
    and mon<='${end_date}'
   ) t 
  where s.asc_code=t.asc_code
    and s.mon=t.mon 
    and s.a_level=t.a_level
    and s.b_level=t.b_level
    and s.bnbw=t.bnbw """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_2(进站频次_total)
def kpi_total_kpi_2(begin_day, end_day):
    line = """  insert overwrite table  kpi_total partition(mon_p='${mon_p}',kpi='kpi_2')
  select substr(t.mon_label,1,7) as mon,
   count(distinct s.vin,tran_date(s.order_balance_date))/count(distinct s.vin) as num,
   s.asc_code
   from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    group by substr(t.mon_label,1,7),s.asc_code"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_2(进站频次_一级)
def kpi_a_level_kpi_2(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_level partition(mon_p='${mon_p}',kpi='kpi_2')
  select primary_classification,
   substr(t.mon_label,1,7) as mon,
   count(distinct s.vin,tran_date(s.order_balance_date))/count(distinct s.vin) as num,
   s.asc_code
   from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    group by substr(t.mon_label,1,7),s.asc_code,primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_2(进站频次_二级)
def kpi_b_level_kpi_2(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_level partition(mon_p='${mon_p}',kpi='kpi_2')
  select primary_classification,
   second_level_classification,
   substr(t.mon_label,1,7) as mon,
   count(distinct s.vin,tran_date(s.order_balance_date))/count(distinct s.vin) as num,
   s.asc_code
   from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    group by substr(t.mon_label,1,7),s.asc_code,primary_classification,second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_2(进站频次_车龄)
def kpi_agetype_kpi_2(begin_day, end_day):
    line = """  insert overwrite table  kpi_agetype partition(mon_p='${mon_p}',kpi='kpi_2')
  select case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end as agetype,
   substr(t.mon_label,1,7) as mon,
   count(distinct s.vin,tran_date(s.order_balance_date))/count(distinct s.vin) as num,
   s.asc_code
   from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    group by substr(t.mon_label,1,7),s.asc_code,case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_2(进站频次_保内外)
def kpi_bnbw_kpi_2(begin_day, end_day):
    line = """  insert overwrite table  kpi_bnbw partition(mon_p='${mon_p}',kpi='kpi_2')
  select case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
      and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
      when s.outdate<'2013-10-01' 
      and mon_diff(t.mon_label,s.outdate)>23 then '保外'
      when s.outdate>='2013-10-01' 
      and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
      when s.outdate>='2013-10-01' 
      and mon_diff(t.mon_label,s.outdate)>35 then '保外'
      else '' end as bn,
   substr(t.mon_label,1,7) as mon,
   count(distinct s.vin,tran_date(s.order_balance_date))/count(distinct s.vin) as num,
   s.asc_code
   from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    group by substr(t.mon_label,1,7),s.asc_code,case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
      and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
      when s.outdate<'2013-10-01' 
      and mon_diff(t.mon_label,s.outdate)>23 then '保外'
      when s.outdate>='2013-10-01' 
      and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
      when s.outdate>='2013-10-01' 
      and mon_diff(t.mon_label,s.outdate)>35 then '保外'
      else '' end"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_2(进站频次_一级_车龄)
def kpi_a_agetype_kpi_2(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_agetype partition(mon_p='${mon_p}',kpi='kpi_2')
  select primary_classification,
   case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end as agetype,
   substr(t.mon_label,1,7) as mon,
   count(distinct s.vin,tran_date(s.order_balance_date))/count(distinct s.vin) as num,
   s.asc_code
   from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    group by substr(t.mon_label,1,7),s.asc_code,primary_classification,case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_2(进站频次_一级_保内外)
def kpi_a_bnbw_kpi_2(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_bnbw partition(mon_p='${mon_p}',kpi='kpi_2')
  select primary_classification,
   case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
      and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
      when s.outdate<'2013-10-01' 
      and mon_diff(t.mon_label,s.outdate)>23 then '保外'
      when s.outdate>='2013-10-01' 
      and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
      when s.outdate>='2013-10-01' 
      and mon_diff(t.mon_label,s.outdate)>35 then '保外'
      else '' end as bn,
   substr(t.mon_label,1,7) as mon,
   count(distinct s.vin,tran_date(s.order_balance_date))/count(distinct s.vin) as num,
   s.asc_code
   from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    group by substr(t.mon_label,1,7),s.asc_code,primary_classification,case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
      and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
      when s.outdate<'2013-10-01' 
      and mon_diff(t.mon_label,s.outdate)>23 then '保外'
      when s.outdate>='2013-10-01' 
      and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
      when s.outdate>='2013-10-01' 
      and mon_diff(t.mon_label,s.outdate)>35 then '保外'
      else '' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_2(进站频次_二级_车龄)
def kpi_b_agetype_kpi_2(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_agetype partition(mon_p='${mon_p}',kpi='kpi_2')
  select primary_classification,
   second_level_classification,
   case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end as agetype,
   substr(t.mon_label,1,7) as mon,
   count(distinct s.vin,tran_date(s.order_balance_date))/count(distinct s.vin) as num,
   s.asc_code
   from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    group by substr(t.mon_label,1,7),s.asc_code,primary_classification,second_level_classification,case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_2(进站频次_二级_保内外)
def kpi_b_bnbw_kpi_2(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_bnbw partition(mon_p='${mon_p}',kpi='kpi_2')
  select primary_classification,
   second_level_classification,
   case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
      and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
      when s.outdate<'2013-10-01' 
      and mon_diff(t.mon_label,s.outdate)>23 then '保外'
      when s.outdate>='2013-10-01' 
      and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
      when s.outdate>='2013-10-01' 
      and mon_diff(t.mon_label,s.outdate)>35 then '保外'
      else '' end as bn,
   substr(t.mon_label,1,7) as mon,
   count(distinct s.vin,tran_date(s.order_balance_date))/count(distinct s.vin) as num,
   s.asc_code
   from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    group by substr(t.mon_label,1,7),s.asc_code,primary_classification,second_level_classification,case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
      and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
      when s.outdate<'2013-10-01' 
      and mon_diff(t.mon_label,s.outdate)>23 then '保外'
      when s.outdate>='2013-10-01' 
      and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
      when s.outdate>='2013-10-01' 
      and mon_diff(t.mon_label,s.outdate)>35 then '保外'
      else '' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_2.1(五年基盘客户数_total)
def kpi_total_kpi_2_1(begin_day, end_day):
    line = """  insert overwrite table  kpi_total partition(mon_p='${mon_p}',kpi='kpi_2_1')
  select substr(t.mon_label,1,7) as mon,
   count(distinct s.vin) as num,
   s.asc_code
   from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-59),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    group by substr(t.mon_label,1,7),s.asc_code"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_2.1(五年基盘客户数_一级)
def kpi_a_level_kpi_2_1(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_level partition(mon_p='${mon_p}',kpi='kpi_2_1')
  select primary_classification,
   substr(t.mon_label,1,7) as mon,
   count(distinct s.vin) as num,
   s.asc_code
   from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-59),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    group by substr(t.mon_label,1,7),s.asc_code,primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_2.1(五年基盘客户数_二级)
def kpi_b_level_kpi_2_1(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_level partition(mon_p='${mon_p}',kpi='kpi_2_1')
  select primary_classification,
   second_level_classification,
   substr(t.mon_label,1,7) as mon,
   count(distinct s.vin) as num,
   s.asc_code
   from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-59),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    group by substr(t.mon_label,1,7),s.asc_code,primary_classification,second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_2.1(五年基盘客户数_车龄)
def kpi_agetype_kpi_2_1(begin_day, end_day):
    line = """  insert overwrite table  kpi_agetype partition(mon_p='${mon_p}',kpi='kpi_2_1')
  select case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end as agetype,
   substr(t.mon_label,1,7) as mon,
   count(distinct s.vin) as num,
   s.asc_code
   from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-59),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    group by substr(t.mon_label,1,7),s.asc_code,case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_2.1(五年基盘客户数_保内外)
def kpi_bnbw_kpi_2_1(begin_day, end_day):
    line = """  insert overwrite table  kpi_bnbw partition(mon_p='${mon_p}',kpi='kpi_2_1')
  select case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
      and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
      when s.outdate<'2013-10-01' 
      and mon_diff(t.mon_label,s.outdate)>23 then '保外'
      when s.outdate>='2013-10-01' 
      and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
      when s.outdate>='2013-10-01' 
      and mon_diff(t.mon_label,s.outdate)>35 then '保外'
      else '' end as bn,
   substr(t.mon_label,1,7) as mon,
   count(distinct s.vin) as num,
   s.asc_code
   from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-59),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    group by substr(t.mon_label,1,7),s.asc_code,case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
      and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
      when s.outdate<'2013-10-01' 
      and mon_diff(t.mon_label,s.outdate)>23 then '保外'
      when s.outdate>='2013-10-01' 
      and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
      when s.outdate>='2013-10-01' 
      and mon_diff(t.mon_label,s.outdate)>35 then '保外'
      else '' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_2.1(五年基盘客户数_一级_车龄)
def kpi_a_agetype_kpi_2_1(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_agetype partition(mon_p='${mon_p}',kpi='kpi_2_1')
  select primary_classification,
   case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end as agetype,
   substr(t.mon_label,1,7) as mon,
   count(distinct s.vin) as num,
   s.asc_code
   from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-59),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    group by substr(t.mon_label,1,7),s.asc_code,primary_classification,case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_2.1(五年基盘客户数_一级_保内外)
def kpi_a_bnbw_kpi_2_1(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_bnbw partition(mon_p='${mon_p}',kpi='kpi_2_1')
  select primary_classification,
   case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
      and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
      when s.outdate<'2013-10-01' 
      and mon_diff(t.mon_label,s.outdate)>23 then '保外'
      when s.outdate>='2013-10-01' 
      and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
      when s.outdate>='2013-10-01' 
      and mon_diff(t.mon_label,s.outdate)>35 then '保外'
      else '' end as bn,
   substr(t.mon_label,1,7) as mon,
   count(distinct s.vin) as num,
   s.asc_code
   from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-59),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    group by substr(t.mon_label,1,7),s.asc_code,primary_classification,case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
      and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
      when s.outdate<'2013-10-01' 
      and mon_diff(t.mon_label,s.outdate)>23 then '保外'
      when s.outdate>='2013-10-01' 
      and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
      when s.outdate>='2013-10-01' 
      and mon_diff(t.mon_label,s.outdate)>35 then '保外'
      else '' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_2.1(五年基盘客户数_二级_车龄)
def kpi_b_agetype_kpi_2_1(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_agetype partition(mon_p='${mon_p}',kpi='kpi_2_1')
  select primary_classification,
   second_level_classification,
   case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end as agetype,
   substr(t.mon_label,1,7) as mon,
   count(distinct s.vin) as num,
   s.asc_code
   from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-59),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    group by substr(t.mon_label,1,7),s.asc_code,primary_classification,second_level_classification,case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_2.1(五年基盘客户数_二级_保内外)
def kpi_b_bnbw_kpi_2_1(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_bnbw partition(mon_p='${mon_p}',kpi='kpi_2_1')
  select primary_classification,
   second_level_classification,
   case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
      and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
      when s.outdate<'2013-10-01' 
      and mon_diff(t.mon_label,s.outdate)>23 then '保外'
      when s.outdate>='2013-10-01' 
      and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
      when s.outdate>='2013-10-01' 
      and mon_diff(t.mon_label,s.outdate)>35 then '保外'
      else '' end as bn,
   substr(t.mon_label,1,7) as mon,
   count(distinct s.vin) as num,
   s.asc_code
   from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-59),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    group by substr(t.mon_label,1,7),s.asc_code,primary_classification,second_level_classification,case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
      and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
      when s.outdate<'2013-10-01' 
      and mon_diff(t.mon_label,s.outdate)>23 then '保外'
      when s.outdate>='2013-10-01' 
      and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
      when s.outdate>='2013-10-01' 
      and mon_diff(t.mon_label,s.outdate)>35 then '保外'
      else '' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_2.2(一年有效基盘_total)
def kpi_total_kpi_2_2(begin_day, end_day):
    line = """  insert overwrite table  kpi_total partition(mon_p='${mon_p}',kpi='kpi_2_2')
  select substr(t.mon_label,1,7) as mon,
   count(distinct s.vin) as num,
   s.asc_code
   from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    group by substr(t.mon_label,1,7),s.asc_code"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_2.2(一年有效基盘_一级)
def kpi_a_level_kpi_2_2(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_level partition(mon_p='${mon_p}',kpi='kpi_2_2')
  select primary_classification,
   substr(t.mon_label,1,7) as mon,
   count(distinct s.vin) as num,
   s.asc_code
   from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    group by substr(t.mon_label,1,7),s.asc_code,primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_2.2(一年有效基盘_二级)
def kpi_b_level_kpi_2_2(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_level partition(mon_p='${mon_p}',kpi='kpi_2_2')
  select primary_classification,
   second_level_classification,
   substr(t.mon_label,1,7) as mon,
   count(distinct s.vin) as num,
   s.asc_code
   from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    group by substr(t.mon_label,1,7),s.asc_code,primary_classification,second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_2.2(一年有效基盘_车龄)
def kpi_agetype_kpi_2_2(begin_day, end_day):
    line = """  insert overwrite table  kpi_agetype partition(mon_p='${mon_p}',kpi='kpi_2_2')
  select case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end as agetype,
   substr(t.mon_label,1,7) as mon,
   count(distinct s.vin) as num,
   s.asc_code
   from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    group by substr(t.mon_label,1,7),s.asc_code,case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_2.2(一年有效基盘_保内外)
def kpi_bnbw_kpi_2_2(begin_day, end_day):
    line = """  insert overwrite table  kpi_bnbw partition(mon_p='${mon_p}',kpi='kpi_2_2')
  select case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
      and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
      when s.outdate<'2013-10-01' 
      and mon_diff(t.mon_label,s.outdate)>23 then '保外'
      when s.outdate>='2013-10-01' 
      and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
      when s.outdate>='2013-10-01' 
      and mon_diff(t.mon_label,s.outdate)>35 then '保外'
      else '' end as bn,
   substr(t.mon_label,1,7) as mon,
   count(distinct s.vin) as num,
   s.asc_code
   from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    group by substr(t.mon_label,1,7),s.asc_code,case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
      and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
      when s.outdate<'2013-10-01' 
      and mon_diff(t.mon_label,s.outdate)>23 then '保外'
      when s.outdate>='2013-10-01' 
      and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
      when s.outdate>='2013-10-01' 
      and mon_diff(t.mon_label,s.outdate)>35 then '保外'
      else '' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_2.2(一年有效基盘_一级_车龄)
def kpi_a_agetype_kpi_2_2(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_agetype partition(mon_p='${mon_p}',kpi='kpi_2_2')
  select primary_classification,
   case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end as agetype,
   substr(t.mon_label,1,7) as mon,
   count(distinct s.vin) as num,
   s.asc_code
   from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    group by substr(t.mon_label,1,7),s.asc_code,primary_classification,case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_2.2(一年有效基盘_一级_保内外)
def kpi_a_bnbw_kpi_2_2(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_bnbw partition(mon_p='${mon_p}',kpi='kpi_2_2')
  select primary_classification,
   case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
      and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
      when s.outdate<'2013-10-01' 
      and mon_diff(t.mon_label,s.outdate)>23 then '保外'
      when s.outdate>='2013-10-01' 
      and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
      when s.outdate>='2013-10-01' 
      and mon_diff(t.mon_label,s.outdate)>35 then '保外'
      else '' end as bn,
   substr(t.mon_label,1,7) as mon,
   count(distinct s.vin) as num,
   s.asc_code
   from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    group by substr(t.mon_label,1,7),s.asc_code,primary_classification,case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
      and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
      when s.outdate<'2013-10-01' 
      and mon_diff(t.mon_label,s.outdate)>23 then '保外'
      when s.outdate>='2013-10-01' 
      and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
      when s.outdate>='2013-10-01' 
      and mon_diff(t.mon_label,s.outdate)>35 then '保外'
      else '' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_2.2(一年有效基盘_二级_车龄)
def kpi_b_agetype_kpi_2_2(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_agetype partition(mon_p='${mon_p}',kpi='kpi_2_2')
  select primary_classification,
   second_level_classification,
   case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end as agetype,
   substr(t.mon_label,1,7) as mon,
   count(distinct s.vin) as num,
   s.asc_code
   from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    group by substr(t.mon_label,1,7),s.asc_code,primary_classification,second_level_classification,case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_2.2(一年有效基盘_二级_保内外)
def kpi_b_bnbw_kpi_2_2(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_bnbw partition(mon_p='${mon_p}',kpi='kpi_2_2')
  select primary_classification,
   second_level_classification,
   case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
      and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
      when s.outdate<'2013-10-01' 
      and mon_diff(t.mon_label,s.outdate)>23 then '保外'
      when s.outdate>='2013-10-01' 
      and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
      when s.outdate>='2013-10-01' 
      and mon_diff(t.mon_label,s.outdate)>35 then '保外'
      else '' end as bn,
   substr(t.mon_label,1,7) as mon,
   count(distinct s.vin) as num,
   s.asc_code
   from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    group by substr(t.mon_label,1,7),s.asc_code,primary_classification,second_level_classification,case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
      and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
      when s.outdate<'2013-10-01' 
      and mon_diff(t.mon_label,s.outdate)>23 then '保外'
      when s.outdate>='2013-10-01' 
      and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
      when s.outdate>='2013-10-01' 
      and mon_diff(t.mon_label,s.outdate)>35 then '保外'
      else '' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_2.3(年进站保养频次_total)
def kpi_total_kpi_2_3(begin_day, end_day):
    line = """  insert overwrite table  kpi_total partition(mon_p='${mon_p}',kpi='kpi_2_3')
  select substr(t.mon_label,1,7) as mon,
   count(distinct s.vin,tran_date(s.order_balance_date))/count(distinct s.vin) as num,
   s.asc_code
   from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1='保养'
    group by substr(t.mon_label,1,7),s.asc_code"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_2.3(年进站保养频次_一级)
def kpi_a_level_kpi_2_3(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_level partition(mon_p='${mon_p}',kpi='kpi_2_3')
  select primary_classification,
   substr(t.mon_label,1,7) as mon,
   count(distinct s.vin,tran_date(s.order_balance_date))/count(distinct s.vin) as num,
   s.asc_code
   from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1='保养'
    group by substr(t.mon_label,1,7),s.asc_code,primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_2.3(年进站保养频次_二级)
def kpi_b_level_kpi_2_3(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_level partition(mon_p='${mon_p}',kpi='kpi_2_3')
  select primary_classification,
   second_level_classification,
   substr(t.mon_label,1,7) as mon,
   count(distinct s.vin,tran_date(s.order_balance_date))/count(distinct s.vin) as num,
   s.asc_code
   from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1='保养'
    group by substr(t.mon_label,1,7),s.asc_code,primary_classification,second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_2.3(年进站保养频次_车龄)
def kpi_agetype_kpi_2_3(begin_day, end_day):
    line = """  insert overwrite table  kpi_agetype partition(mon_p='${mon_p}',kpi='kpi_2_3')
  select case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end as agetype,
   substr(t.mon_label,1,7) as mon,
   count(distinct s.vin,tran_date(s.order_balance_date))/count(distinct s.vin) as num,
   s.asc_code
   from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1='保养'
    group by substr(t.mon_label,1,7),s.asc_code,case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_2.3(年进站保养频次_保内外)
def kpi_bnbw_kpi_2_3(begin_day, end_day):
    line = """  insert overwrite table  kpi_bnbw partition(mon_p='${mon_p}',kpi='kpi_2_3')
  select case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
      and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
      when s.outdate<'2013-10-01' 
      and mon_diff(t.mon_label,s.outdate)>23 then '保外'
      when s.outdate>='2013-10-01' 
      and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
      when s.outdate>='2013-10-01' 
      and mon_diff(t.mon_label,s.outdate)>35 then '保外'
      else '' end as bn,
   substr(t.mon_label,1,7) as mon,
   count(distinct s.vin,tran_date(s.order_balance_date))/count(distinct s.vin) as num,
   s.asc_code
   from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1='保养'
    group by substr(t.mon_label,1,7),s.asc_code,case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
      and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
      when s.outdate<'2013-10-01' 
      and mon_diff(t.mon_label,s.outdate)>23 then '保外'
      when s.outdate>='2013-10-01' 
      and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
      when s.outdate>='2013-10-01' 
      and mon_diff(t.mon_label,s.outdate)>35 then '保外'
      else '' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_2.3(年进站保养频次_一级_车龄)
def kpi_a_agetype_kpi_2_3(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_agetype partition(mon_p='${mon_p}',kpi='kpi_2_3')
  select primary_classification,
   case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end as agetype,
   substr(t.mon_label,1,7) as mon,
   count(distinct s.vin,tran_date(s.order_balance_date))/count(distinct s.vin) as num,
   s.asc_code
   from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1='保养'
    group by substr(t.mon_label,1,7),s.asc_code,primary_classification,case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_2.3(年进站保养频次_一级_保内外)
def kpi_a_bnbw_kpi_2_3(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_bnbw partition(mon_p='${mon_p}',kpi='kpi_2_3')
  select primary_classification,
   case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
      and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
      when s.outdate<'2013-10-01' 
      and mon_diff(t.mon_label,s.outdate)>23 then '保外'
      when s.outdate>='2013-10-01' 
      and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
      when s.outdate>='2013-10-01' 
      and mon_diff(t.mon_label,s.outdate)>35 then '保外'
      else '' end as bn,
   substr(t.mon_label,1,7) as mon,
   count(distinct s.vin,tran_date(s.order_balance_date))/count(distinct s.vin) as num,
   s.asc_code
   from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1='保养'
    group by substr(t.mon_label,1,7),s.asc_code,primary_classification,case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
      and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
      when s.outdate<'2013-10-01' 
      and mon_diff(t.mon_label,s.outdate)>23 then '保外'
      when s.outdate>='2013-10-01' 
      and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
      when s.outdate>='2013-10-01' 
      and mon_diff(t.mon_label,s.outdate)>35 then '保外'
      else '' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_2.3(年进站保养频次_二级_车龄)
def kpi_b_agetype_kpi_2_3(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_agetype partition(mon_p='${mon_p}',kpi='kpi_2_3')
  select primary_classification,
   second_level_classification,
   case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end as agetype,
   substr(t.mon_label,1,7) as mon,
   count(distinct s.vin,tran_date(s.order_balance_date))/count(distinct s.vin) as num,
   s.asc_code
   from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1='保养'
    group by substr(t.mon_label,1,7),s.asc_code,primary_classification,second_level_classification,case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_2.3(年进站保养频次_二级_保内外)
def kpi_b_bnbw_kpi_2_3(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_bnbw partition(mon_p='${mon_p}',kpi='kpi_2_3')
  select primary_classification,
   second_level_classification,
   case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
      and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
      when s.outdate<'2013-10-01' 
      and mon_diff(t.mon_label,s.outdate)>23 then '保外'
      when s.outdate>='2013-10-01' 
      and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
      when s.outdate>='2013-10-01' 
      and mon_diff(t.mon_label,s.outdate)>35 then '保外'
      else '' end as bn,
   substr(t.mon_label,1,7) as mon,
   count(distinct s.vin,tran_date(s.order_balance_date))/count(distinct s.vin) as num,
   s.asc_code
   from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1='保养'
    group by substr(t.mon_label,1,7),s.asc_code,primary_classification,second_level_classification,case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
      and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
      when s.outdate<'2013-10-01' 
      and mon_diff(t.mon_label,s.outdate)>23 then '保外'
      when s.outdate>='2013-10-01' 
      and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
      when s.outdate>='2013-10-01' 
      and mon_diff(t.mon_label,s.outdate)>35 then '保外'
      else '' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_2.4(五年新车销售数_total)
def kpi_total_kpi_2_4(begin_day, end_day):
    line = """  insert overwrite table  kpi_total partition(mon_p='${mon_p}',kpi='kpi_2_4')
  select substr(t.mon_label,1,7) as mon,
   count(distinct s.vin) as num,
   s.asc_code
   from date_label t,label_doss s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.report_date),1,7)>=substr(add_months(t.mon_label,-59),1,7)
    and substr(tran_date(s.report_date),1,7)<=substr(t.mon_label,1,7)
    and s.primary_classification<>'' and s.asc_code<>''
    group by substr(t.mon_label,1,7),s.asc_code"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_2.4(五年新车销售数_一级)
def kpi_a_level_kpi_2_4(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_level partition(mon_p='${mon_p}',kpi='kpi_2_4')
  select primary_classification,
   substr(t.mon_label,1,7) as mon,
   count(distinct s.vin) as num,
   s.asc_code
   from date_label t,label_doss s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.report_date),1,7)>=substr(add_months(t.mon_label,-59),1,7)
    and substr(tran_date(s.report_date),1,7)<=substr(t.mon_label,1,7)
    and s.primary_classification<>'' and s.asc_code<>''
    group by substr(t.mon_label,1,7),s.asc_code,primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_2.4(五年新车销售数_二级)
def kpi_b_level_kpi_2_4(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_level partition(mon_p='${mon_p}',kpi='kpi_2_4')
  select primary_classification,
   second_level_classification,
   substr(t.mon_label,1,7) as mon,
   count(distinct s.vin) as num,
   s.asc_code
   from date_label t,label_doss s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.report_date),1,7)>=substr(add_months(t.mon_label,-59),1,7)
    and substr(tran_date(s.report_date),1,7)<=substr(t.mon_label,1,7)
    and s.primary_classification<>'' and s.asc_code<>''
    group by substr(t.mon_label,1,7),s.asc_code,primary_classification,second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_2.4(五年新车销售数_车龄)
def kpi_agetype_kpi_2_4(begin_day, end_day):
    line = """  insert overwrite table  kpi_agetype partition(mon_p='${mon_p}',kpi='kpi_2_4')
  select case when tran_date(s.invoice_date)='未知' then '未知'
      when mon_diff(t.mon_label,tran_date(s.invoice_date))<=11 then '0-1年'
      when mon_diff(t.mon_label,tran_date(s.invoice_date))>=12
      and mon_diff(t.mon_label,tran_date(s.invoice_date))<=23 then '1-2年'
      when mon_diff(t.mon_label,tran_date(s.invoice_date))>=24
      and mon_diff(t.mon_label,tran_date(s.invoice_date))<=35 then '2-3年'
      when mon_diff(t.mon_label,tran_date(s.invoice_date))>=36
      and mon_diff(t.mon_label,tran_date(s.invoice_date))<=47 then '3-4年'
      when mon_diff(t.mon_label,tran_date(s.invoice_date))>=48
      and mon_diff(t.mon_label,tran_date(s.invoice_date))<=59 then '4-5年'
      else '5+年' end as agetype,
   substr(t.mon_label,1,7) as mon,
   count(distinct s.vin) as num,
   s.asc_code
   from date_label t,label_doss s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.report_date),1,7)>=substr(add_months(t.mon_label,-59),1,7)
    and substr(tran_date(s.report_date),1,7)<=substr(t.mon_label,1,7)
    and s.primary_classification<>'' and s.asc_code<>''
    group by substr(t.mon_label,1,7),s.asc_code,case when tran_date(s.invoice_date)='未知' then '未知'
      when mon_diff(t.mon_label,tran_date(s.invoice_date))<=11 then '0-1年'
      when mon_diff(t.mon_label,tran_date(s.invoice_date))>=12
      and mon_diff(t.mon_label,tran_date(s.invoice_date))<=23 then '1-2年'
      when mon_diff(t.mon_label,tran_date(s.invoice_date))>=24
      and mon_diff(t.mon_label,tran_date(s.invoice_date))<=35 then '2-3年'
      when mon_diff(t.mon_label,tran_date(s.invoice_date))>=36
      and mon_diff(t.mon_label,tran_date(s.invoice_date))<=47 then '3-4年'
      when mon_diff(t.mon_label,tran_date(s.invoice_date))>=48
      and mon_diff(t.mon_label,tran_date(s.invoice_date))<=59 then '4-5年'
      else '5+年' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_2.4(五年新车销售数_保内外)
def kpi_bnbw_kpi_2_4(begin_day, end_day):
    line = """  insert overwrite table  kpi_bnbw partition(mon_p='${mon_p}',kpi='kpi_2_4')
  select case when tran_date(s.invoice_date)='未知' then '未知'
      when tran_date(s.invoice_date)<'2013-10-01' 
      and mon_diff(t.mon_label,tran_date(s.invoice_date))<=23 then '保内'
      when tran_date(s.invoice_date)<'2013-10-01' 
      and mon_diff(t.mon_label,tran_date(s.invoice_date))>23 then '保外'
      when tran_date(s.invoice_date)>='2013-10-01' 
      and mon_diff(t.mon_label,tran_date(s.invoice_date))<=35 then '保内'
      when tran_date(s.invoice_date)>='2013-10-01' 
      and mon_diff(t.mon_label,tran_date(s.invoice_date))>35 then '保外'
      else '' end as bn,
   substr(t.mon_label,1,7) as mon,
   count(distinct s.vin) as num,
   s.asc_code
   from date_label t,label_doss s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.report_date),1,7)>=substr(add_months(t.mon_label,-59),1,7)
    and substr(tran_date(s.report_date),1,7)<=substr(t.mon_label,1,7)
    and s.primary_classification<>'' and s.asc_code<>''
    group by substr(t.mon_label,1,7),s.asc_code,case when tran_date(s.invoice_date)='未知' then '未知'
      when tran_date(s.invoice_date)<'2013-10-01' 
      and mon_diff(t.mon_label,tran_date(s.invoice_date))<=23 then '保内'
      when tran_date(s.invoice_date)<'2013-10-01' 
      and mon_diff(t.mon_label,tran_date(s.invoice_date))>23 then '保外'
      when tran_date(s.invoice_date)>='2013-10-01' 
      and mon_diff(t.mon_label,tran_date(s.invoice_date))<=35 then '保内'
      when tran_date(s.invoice_date)>='2013-10-01' 
      and mon_diff(t.mon_label,tran_date(s.invoice_date))>35 then '保外'
      else '' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_2.4(五年新车销售数_一级_车龄)
def kpi_a_agetype_kpi_2_4(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_agetype partition(mon_p='${mon_p}',kpi='kpi_2_4')
  select primary_classification,
   case when tran_date(s.invoice_date)='未知' then '未知'
      when mon_diff(t.mon_label,tran_date(s.invoice_date))<=11 then '0-1年'
      when mon_diff(t.mon_label,tran_date(s.invoice_date))>=12
      and mon_diff(t.mon_label,tran_date(s.invoice_date))<=23 then '1-2年'
      when mon_diff(t.mon_label,tran_date(s.invoice_date))>=24
      and mon_diff(t.mon_label,tran_date(s.invoice_date))<=35 then '2-3年'
      when mon_diff(t.mon_label,tran_date(s.invoice_date))>=36
      and mon_diff(t.mon_label,tran_date(s.invoice_date))<=47 then '3-4年'
      when mon_diff(t.mon_label,tran_date(s.invoice_date))>=48
      and mon_diff(t.mon_label,tran_date(s.invoice_date))<=59 then '4-5年'
      else '5+年' end as agetype,
   substr(t.mon_label,1,7) as mon,
   count(distinct s.vin) as num,
   s.asc_code
   from date_label t,label_doss s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.report_date),1,7)>=substr(add_months(t.mon_label,-59),1,7)
    and substr(tran_date(s.report_date),1,7)<=substr(t.mon_label,1,7)
    and s.primary_classification<>'' and s.asc_code<>''
    group by substr(t.mon_label,1,7),s.asc_code,primary_classification,case when tran_date(s.invoice_date)='未知' then '未知'
      when mon_diff(t.mon_label,tran_date(s.invoice_date))<=11 then '0-1年'
      when mon_diff(t.mon_label,tran_date(s.invoice_date))>=12
      and mon_diff(t.mon_label,tran_date(s.invoice_date))<=23 then '1-2年'
      when mon_diff(t.mon_label,tran_date(s.invoice_date))>=24
      and mon_diff(t.mon_label,tran_date(s.invoice_date))<=35 then '2-3年'
      when mon_diff(t.mon_label,tran_date(s.invoice_date))>=36
      and mon_diff(t.mon_label,tran_date(s.invoice_date))<=47 then '3-4年'
      when mon_diff(t.mon_label,tran_date(s.invoice_date))>=48
      and mon_diff(t.mon_label,tran_date(s.invoice_date))<=59 then '4-5年'
      else '5+年' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_2.4(五年新车销售数_一级_保内外)
def kpi_a_bnbw_kpi_2_4(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_bnbw partition(mon_p='${mon_p}',kpi='kpi_2_4')
  select primary_classification,
   case when tran_date(s.invoice_date)='未知' then '未知'
      when tran_date(s.invoice_date)<'2013-10-01' 
      and mon_diff(t.mon_label,tran_date(s.invoice_date))<=23 then '保内'
      when tran_date(s.invoice_date)<'2013-10-01' 
      and mon_diff(t.mon_label,tran_date(s.invoice_date))>23 then '保外'
      when tran_date(s.invoice_date)>='2013-10-01' 
      and mon_diff(t.mon_label,tran_date(s.invoice_date))<=35 then '保内'
      when tran_date(s.invoice_date)>='2013-10-01' 
      and mon_diff(t.mon_label,tran_date(s.invoice_date))>35 then '保外'
      else '' end as bn,
   substr(t.mon_label,1,7) as mon,
   count(distinct s.vin) as num,
   s.asc_code
   from date_label t,label_doss s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.report_date),1,7)>=substr(add_months(t.mon_label,-59),1,7)
    and substr(tran_date(s.report_date),1,7)<=substr(t.mon_label,1,7)
    and s.primary_classification<>'' and s.asc_code<>''
    group by substr(t.mon_label,1,7),s.asc_code,primary_classification,case when tran_date(s.invoice_date)='未知' then '未知'
      when tran_date(s.invoice_date)<'2013-10-01' 
      and mon_diff(t.mon_label,tran_date(s.invoice_date))<=23 then '保内'
      when tran_date(s.invoice_date)<'2013-10-01' 
      and mon_diff(t.mon_label,tran_date(s.invoice_date))>23 then '保外'
      when tran_date(s.invoice_date)>='2013-10-01' 
      and mon_diff(t.mon_label,tran_date(s.invoice_date))<=35 then '保内'
      when tran_date(s.invoice_date)>='2013-10-01' 
      and mon_diff(t.mon_label,tran_date(s.invoice_date))>35 then '保外'
      else '' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_2.4(五年新车销售数_二级_车龄)
def kpi_b_agetype_kpi_2_4(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_agetype partition(mon_p='${mon_p}',kpi='kpi_2_4')
  select primary_classification,
   second_level_classification,
   case when tran_date(s.invoice_date)='未知' then '未知'
      when mon_diff(t.mon_label,tran_date(s.invoice_date))<=11 then '0-1年'
      when mon_diff(t.mon_label,tran_date(s.invoice_date))>=12
      and mon_diff(t.mon_label,tran_date(s.invoice_date))<=23 then '1-2年'
      when mon_diff(t.mon_label,tran_date(s.invoice_date))>=24
      and mon_diff(t.mon_label,tran_date(s.invoice_date))<=35 then '2-3年'
      when mon_diff(t.mon_label,tran_date(s.invoice_date))>=36
      and mon_diff(t.mon_label,tran_date(s.invoice_date))<=47 then '3-4年'
      when mon_diff(t.mon_label,tran_date(s.invoice_date))>=48
      and mon_diff(t.mon_label,tran_date(s.invoice_date))<=59 then '4-5年'
      else '5+年' end as agetype,
   substr(t.mon_label,1,7) as mon,
   count(distinct s.vin) as num,
   s.asc_code
   from date_label t,label_doss s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.report_date),1,7)>=substr(add_months(t.mon_label,-59),1,7)
    and substr(tran_date(s.report_date),1,7)<=substr(t.mon_label,1,7)
    and s.primary_classification<>'' and s.asc_code<>''
    group by substr(t.mon_label,1,7),s.asc_code,primary_classification,second_level_classification,case when tran_date(s.invoice_date)='未知' then '未知'
      when mon_diff(t.mon_label,tran_date(s.invoice_date))<=11 then '0-1年'
      when mon_diff(t.mon_label,tran_date(s.invoice_date))>=12
      and mon_diff(t.mon_label,tran_date(s.invoice_date))<=23 then '1-2年'
      when mon_diff(t.mon_label,tran_date(s.invoice_date))>=24
      and mon_diff(t.mon_label,tran_date(s.invoice_date))<=35 then '2-3年'
      when mon_diff(t.mon_label,tran_date(s.invoice_date))>=36
      and mon_diff(t.mon_label,tran_date(s.invoice_date))<=47 then '3-4年'
      when mon_diff(t.mon_label,tran_date(s.invoice_date))>=48
      and mon_diff(t.mon_label,tran_date(s.invoice_date))<=59 then '4-5年'
      else '5+年' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_2.4(五年新车销售数_二级_保内外)
def kpi_b_bnbw_kpi_2_4(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_bnbw partition(mon_p='${mon_p}',kpi='kpi_2_4')
  select primary_classification,
   second_level_classification,
   case when tran_date(s.invoice_date)='未知' then '未知'
      when tran_date(s.invoice_date)<'2013-10-01' 
      and mon_diff(t.mon_label,tran_date(s.invoice_date))<=23 then '保内'
      when tran_date(s.invoice_date)<'2013-10-01' 
      and mon_diff(t.mon_label,tran_date(s.invoice_date))>23 then '保外'
      when tran_date(s.invoice_date)>='2013-10-01' 
      and mon_diff(t.mon_label,tran_date(s.invoice_date))<=35 then '保内'
      when tran_date(s.invoice_date)>='2013-10-01' 
      and mon_diff(t.mon_label,tran_date(s.invoice_date))>35 then '保外'
      else '' end as bn,
   substr(t.mon_label,1,7) as mon,
   count(distinct s.vin) as num,
   s.asc_code
   from date_label t,label_doss s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.report_date),1,7)>=substr(add_months(t.mon_label,-59),1,7)
    and substr(tran_date(s.report_date),1,7)<=substr(t.mon_label,1,7)
    and s.primary_classification<>'' and s.asc_code<>''
    group by substr(t.mon_label,1,7),s.asc_code,primary_classification,second_level_classification,case when tran_date(s.invoice_date)='未知' then '未知'
      when tran_date(s.invoice_date)<'2013-10-01' 
      and mon_diff(t.mon_label,tran_date(s.invoice_date))<=23 then '保内'
      when tran_date(s.invoice_date)<'2013-10-01' 
      and mon_diff(t.mon_label,tran_date(s.invoice_date))>23 then '保外'
      when tran_date(s.invoice_date)>='2013-10-01' 
      and mon_diff(t.mon_label,tran_date(s.invoice_date))<=35 then '保内'
      when tran_date(s.invoice_date)>='2013-10-01' 
      and mon_diff(t.mon_label,tran_date(s.invoice_date))>35 then '保外'
      else '' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_2.41(五年年均新车销售数_total)
def kpi_total_kpi_2_41(begin_day, end_day):
    line = """  insert overwrite table  kpi_total partition(mon_p='${mon_p}',kpi='kpi_2_41')
  select mon,
  num*12/(case when yc<=60 then yc else 60 end) as num,
  asc_code
  from (
  select s.mon,
  mon_diff(concat(s.mon,'-01'),concat(t.minmon,'-01'))+1 as yc,
  s.num,
  s.asc_code
  from kpi_total s ,
  (
  select min(mon) as minmon,asc_code from kpi_total
  where kpi='kpi_2_4'
  group by asc_code
  ) t where s.asc_code=t.asc_code and s.kpi='kpi_2_4' and s.mon>='${begin_date}'
    and s.mon<='${end_date}'
  ) s """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_2.41(五年年均新车销售数_一级)
def kpi_a_level_kpi_2_41(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_level partition(mon_p='${mon_p}',kpi='kpi_2_41')
  select a_level,
  mon,
  num*12/(case when yc<=60 then yc else 60 end) as num,
  asc_code
  from (
  select s.a_level,s.mon,
  mon_diff(concat(s.mon,'-01'),concat(t.minmon,'-01'))+1 as yc,
  s.num,
  s.asc_code
  from kpi_a_level s ,
  (
  select min(mon) as minmon,asc_code,a_level from kpi_a_level
  where kpi='kpi_2_4'
  group by asc_code,a_level
  ) t where s.asc_code=t.asc_code and s.kpi='kpi_2_4' and s.a_level=t.a_level and s.mon>='${begin_date}'
    and s.mon<='${end_date}'
  ) s """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_2.41(五年年均新车销售数_二级)
def kpi_b_level_kpi_2_41(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_level partition(mon_p='${mon_p}',kpi='kpi_2_41')
  select a_level,
  b_level,
  mon,
  num*12/(case when yc<=60 then yc else 60 end) as num,
  asc_code
  from (
  select s.a_level,s.b_level,s.mon,
  mon_diff(concat(s.mon,'-01'),concat(t.minmon,'-01'))+1 as yc,
  s.num,
  s.asc_code
  from kpi_b_level s ,
  (
  select min(mon) as minmon,asc_code,a_level,b_level from kpi_b_level
  where kpi='kpi_2_4'
  group by asc_code,a_level,b_level
  ) t where s.asc_code=t.asc_code and s.kpi='kpi_2_4' and s.a_level=t.a_level and s.b_level=t.b_level
  and s.mon>='${begin_date}'
    and s.mon<='${end_date}'
  ) s """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_2.41(五年年均新车销售数_车龄)
def kpi_agetype_kpi_2_41(begin_day, end_day):
    line = """  insert overwrite table  kpi_agetype partition(mon_p='${mon_p}',kpi='kpi_2_41')
  select age_type,
  mon,
  num*12/(case when yc<=60 then yc else 60 end) as num,
  asc_code
  from (
  select s.age_type,s.mon,
  mon_diff(concat(s.mon,'-01'),concat(t.minmon,'-01'))+1 as yc,
  s.num,
  s.asc_code
  from kpi_agetype s ,
  (
  select min(mon) as minmon,asc_code,age_type from kpi_agetype
  where kpi='kpi_2_4'
  group by asc_code,age_type
  ) t where s.asc_code=t.asc_code and s.kpi='kpi_2_4' and s.age_type=t.age_type
  and s.mon>='${begin_date}'
    and s.mon<='${end_date}'
  ) s """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_2.41(五年年均新车销售数_保内外)
def kpi_bnbw_kpi_2_41(begin_day, end_day):
    line = """  insert overwrite table  kpi_bnbw partition(mon_p='${mon_p}',kpi='kpi_2_41')
  select bnbw,
  mon,
  num*12/(case when yc<=60 then yc else 60 end) as num,
  asc_code
  from (
  select s.bnbw,s.mon,
  mon_diff(concat(s.mon,'-01'),concat(t.minmon,'-01'))+1 as yc,
  s.num,
  s.asc_code
  from kpi_bnbw s ,
  (
  select min(mon) as minmon,asc_code,bnbw from kpi_bnbw
  where kpi='kpi_2_4'
  group by asc_code,bnbw
  ) t where s.asc_code=t.asc_code and s.kpi='kpi_2_4' and s.bnbw=t.bnbw
  and s.mon>='${begin_date}'
    and s.mon<='${end_date}'
  ) s """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_2.41(五年年均新车销售数_一级_车龄)
def kpi_a_agetype_kpi_2_41(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_agetype partition(mon_p='${mon_p}',kpi='kpi_2_41')
  select a_level,
  age_type,
  mon,
  num*12/(case when yc<=60 then yc else 60 end) as num,
  asc_code
  from (
  select s.a_level,s.age_type,s.mon,
  mon_diff(concat(s.mon,'-01'),concat(t.minmon,'-01'))+1 as yc,
  s.num,
  s.asc_code
  from kpi_a_agetype s ,
  (
  select min(mon) as minmon,asc_code,a_level,age_type from kpi_a_agetype
  where kpi='kpi_2_4'
  group by asc_code,a_level,age_type
  ) t where s.asc_code=t.asc_code and s.kpi='kpi_2_4' and s.a_level=t.a_level and s.age_type=t.age_type
  and s.mon>='${begin_date}'
    and s.mon<='${end_date}'
  ) s """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_2.41(五年年均新车销售数_一级_保内外)
def kpi_a_bnbw_kpi_2_41(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_bnbw partition(mon_p='${mon_p}',kpi='kpi_2_41')
  select a_level,
  bnbw,
  mon,
  num*12/(case when yc<=60 then yc else 60 end) as num,
  asc_code
  from (
  select s.a_level,s.bnbw,s.mon,
  mon_diff(concat(s.mon,'-01'),concat(t.minmon,'-01'))+1 as yc,
  s.num,
  s.asc_code
  from kpi_a_bnbw s ,
  (
  select min(mon) as minmon,asc_code,a_level,bnbw from kpi_a_bnbw
  where kpi='kpi_2_4'
  group by asc_code,a_level,bnbw
  ) t where s.asc_code=t.asc_code and s.kpi='kpi_2_4' and s.a_level=t.a_level and s.bnbw=t.bnbw
  and s.mon>='${begin_date}'
    and s.mon<='${end_date}'
  ) s """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_2.41(五年年均新车销售数_二级_车龄)
def kpi_b_agetype_kpi_2_41(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_agetype partition(mon_p='${mon_p}',kpi='kpi_2_41')
  select a_level,
  b_level,
  age_type,
  mon,
  num*12/(case when yc<=60 then yc else 60 end) as num,
  asc_code
  from (
  select s.a_level,s.b_level,s.age_type,s.mon,
  mon_diff(concat(s.mon,'-01'),concat(t.minmon,'-01'))+1 as yc,
  s.num,
  s.asc_code
  from kpi_b_agetype s ,
  (
  select min(mon) as minmon,asc_code,a_level,b_level,age_type from kpi_b_agetype
  where kpi='kpi_2_4'
  group by asc_code,a_level,age_type,b_level
  ) t where s.asc_code=t.asc_code and s.kpi='kpi_2_4' and s.a_level=t.a_level and s.age_type=t.age_type
  and s.mon>='${begin_date}'
    and s.mon<='${end_date}'
  and s.b_level=t.b_level
  ) s """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_2.41(五年年均新车销售数_二级_保内外)
def kpi_b_bnbw_kpi_2_41(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_bnbw partition(mon_p='${mon_p}',kpi='kpi_2_41')
  select a_level,
  b_level,
  bnbw,
  mon,
  num*12/(case when yc<=60 then yc else 60 end) as num,
  asc_code
  from (
  select s.a_level,s.b_level,s.bnbw,s.mon,
  mon_diff(concat(s.mon,'-01'),concat(t.minmon,'-01'))+1 as yc,
  s.num,
  s.asc_code
  from kpi_b_bnbw s ,
  (
  select min(mon) as minmon,asc_code,a_level,b_level,bnbw from kpi_b_bnbw
  where kpi='kpi_2_4'
  group by asc_code,a_level,bnbw,b_level
  ) t where s.asc_code=t.asc_code and s.kpi='kpi_2_4' and s.a_level=t.a_level and s.bnbw=t.bnbw
  and s.b_level=t.b_level
  and s.mon>='${begin_date}'
    and s.mon<='${end_date}'
  ) s """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_2.42(本年新车销售数_total)
def kpi_total_kpi_2_42(begin_day, end_day):
    line = """  insert overwrite table  kpi_total partition(mon_p='${mon_p}',kpi='kpi_2_42')
  select substr(t.mon_label,1,7) as mon,
   count(distinct s.vin) as num,
   s.asc_code
   from date_label t,label_doss s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.report_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.report_date),1,7)<=substr(t.mon_label,1,7)
    and s.primary_classification<>'' and s.asc_code<>''
    group by substr(t.mon_label,1,7),s.asc_code"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_2.42(本年新车销售数_一级)
def kpi_a_level_kpi_2_42(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_level partition(mon_p='${mon_p}',kpi='kpi_2_42')
  select primary_classification,
   substr(t.mon_label,1,7) as mon,
   count(distinct s.vin) as num,
   s.asc_code
   from date_label t,label_doss s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.report_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.report_date),1,7)<=substr(t.mon_label,1,7)
    and s.primary_classification<>'' and s.asc_code<>''
    group by substr(t.mon_label,1,7),s.asc_code,primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_2.42(本年新车销售数_二级)
def kpi_b_level_kpi_2_42(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_level partition(mon_p='${mon_p}',kpi='kpi_2_42')
  select primary_classification,
   second_level_classification,
   substr(t.mon_label,1,7) as mon,
   count(distinct s.vin) as num,
   s.asc_code
   from date_label t,label_doss s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.report_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.report_date),1,7)<=substr(t.mon_label,1,7)
    and s.primary_classification<>'' and s.asc_code<>''
    group by substr(t.mon_label,1,7),s.asc_code,primary_classification,second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_2.42(本年新车销售数_车龄)
def kpi_agetype_kpi_2_42(begin_day, end_day):
    line = """  insert overwrite table  kpi_agetype partition(mon_p='${mon_p}',kpi='kpi_2_42')
  select case when tran_date(s.invoice_date)='未知' then '未知'
      when mon_diff(t.mon_label,tran_date(s.invoice_date))<=11 then '0-1年'
      when mon_diff(t.mon_label,tran_date(s.invoice_date))>=12
      and mon_diff(t.mon_label,tran_date(s.invoice_date))<=23 then '1-2年'
      when mon_diff(t.mon_label,tran_date(s.invoice_date))>=24
      and mon_diff(t.mon_label,tran_date(s.invoice_date))<=35 then '2-3年'
      when mon_diff(t.mon_label,tran_date(s.invoice_date))>=36
      and mon_diff(t.mon_label,tran_date(s.invoice_date))<=47 then '3-4年'
      when mon_diff(t.mon_label,tran_date(s.invoice_date))>=48
      and mon_diff(t.mon_label,tran_date(s.invoice_date))<=59 then '4-5年'
      else '5+年' end as agetype,
   substr(t.mon_label,1,7) as mon,
   count(distinct s.vin) as num,
   s.asc_code
   from date_label t,label_doss s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.report_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.report_date),1,7)<=substr(t.mon_label,1,7)
    and s.primary_classification<>'' and s.asc_code<>''
    group by substr(t.mon_label,1,7),s.asc_code,case when tran_date(s.invoice_date)='未知' then '未知'
      when mon_diff(t.mon_label,tran_date(s.invoice_date))<=11 then '0-1年'
      when mon_diff(t.mon_label,tran_date(s.invoice_date))>=12
      and mon_diff(t.mon_label,tran_date(s.invoice_date))<=23 then '1-2年'
      when mon_diff(t.mon_label,tran_date(s.invoice_date))>=24
      and mon_diff(t.mon_label,tran_date(s.invoice_date))<=35 then '2-3年'
      when mon_diff(t.mon_label,tran_date(s.invoice_date))>=36
      and mon_diff(t.mon_label,tran_date(s.invoice_date))<=47 then '3-4年'
      when mon_diff(t.mon_label,tran_date(s.invoice_date))>=48
      and mon_diff(t.mon_label,tran_date(s.invoice_date))<=59 then '4-5年'
      else '5+年' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_2.42(本年新车销售数_保内外)
def kpi_bnbw_kpi_2_42(begin_day, end_day):
    line = """  insert overwrite table  kpi_bnbw partition(mon_p='${mon_p}',kpi='kpi_2_42')
  select case when tran_date(s.invoice_date)='未知' then '未知'
      when tran_date(s.invoice_date)<'2013-10-01' 
      and mon_diff(t.mon_label,tran_date(s.invoice_date))<=23 then '保内'
      when tran_date(s.invoice_date)<'2013-10-01' 
      and mon_diff(t.mon_label,tran_date(s.invoice_date))>23 then '保外'
      when tran_date(s.invoice_date)>='2013-10-01' 
      and mon_diff(t.mon_label,tran_date(s.invoice_date))<=35 then '保内'
      when tran_date(s.invoice_date)>='2013-10-01' 
      and mon_diff(t.mon_label,tran_date(s.invoice_date))>35 then '保外'
      else '' end as bn,
   substr(t.mon_label,1,7) as mon,
   count(distinct s.vin) as num,
   s.asc_code
   from date_label t,label_doss s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.report_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.report_date),1,7)<=substr(t.mon_label,1,7)
    and s.primary_classification<>'' and s.asc_code<>''
    group by substr(t.mon_label,1,7),s.asc_code,case when tran_date(s.invoice_date)='未知' then '未知'
      when tran_date(s.invoice_date)<'2013-10-01' 
      and mon_diff(t.mon_label,tran_date(s.invoice_date))<=23 then '保内'
      when tran_date(s.invoice_date)<'2013-10-01' 
      and mon_diff(t.mon_label,tran_date(s.invoice_date))>23 then '保外'
      when tran_date(s.invoice_date)>='2013-10-01' 
      and mon_diff(t.mon_label,tran_date(s.invoice_date))<=35 then '保内'
      when tran_date(s.invoice_date)>='2013-10-01' 
      and mon_diff(t.mon_label,tran_date(s.invoice_date))>35 then '保外'
      else '' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_2.42(本年新车销售数_一级_车龄)
def kpi_a_agetype_kpi_2_42(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_agetype partition(mon_p='${mon_p}',kpi='kpi_2_42')
  select primary_classification,
   case when tran_date(s.invoice_date)='未知' then '未知'
      when mon_diff(t.mon_label,tran_date(s.invoice_date))<=11 then '0-1年'
      when mon_diff(t.mon_label,tran_date(s.invoice_date))>=12
      and mon_diff(t.mon_label,tran_date(s.invoice_date))<=23 then '1-2年'
      when mon_diff(t.mon_label,tran_date(s.invoice_date))>=24
      and mon_diff(t.mon_label,tran_date(s.invoice_date))<=35 then '2-3年'
      when mon_diff(t.mon_label,tran_date(s.invoice_date))>=36
      and mon_diff(t.mon_label,tran_date(s.invoice_date))<=47 then '3-4年'
      when mon_diff(t.mon_label,tran_date(s.invoice_date))>=48
      and mon_diff(t.mon_label,tran_date(s.invoice_date))<=59 then '4-5年'
      else '5+年' end as agetype,
   substr(t.mon_label,1,7) as mon,
   count(distinct s.vin) as num,
   s.asc_code
   from date_label t,label_doss s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.report_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.report_date),1,7)<=substr(t.mon_label,1,7)
    and s.primary_classification<>'' and s.asc_code<>''
    group by substr(t.mon_label,1,7),s.asc_code,primary_classification,case when tran_date(s.invoice_date)='未知' then '未知'
      when mon_diff(t.mon_label,tran_date(s.invoice_date))<=11 then '0-1年'
      when mon_diff(t.mon_label,tran_date(s.invoice_date))>=12
      and mon_diff(t.mon_label,tran_date(s.invoice_date))<=23 then '1-2年'
      when mon_diff(t.mon_label,tran_date(s.invoice_date))>=24
      and mon_diff(t.mon_label,tran_date(s.invoice_date))<=35 then '2-3年'
      when mon_diff(t.mon_label,tran_date(s.invoice_date))>=36
      and mon_diff(t.mon_label,tran_date(s.invoice_date))<=47 then '3-4年'
      when mon_diff(t.mon_label,tran_date(s.invoice_date))>=48
      and mon_diff(t.mon_label,tran_date(s.invoice_date))<=59 then '4-5年'
      else '5+年' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_2.42(本年新车销售数_一级_保内外)
def kpi_a_bnbw_kpi_2_42(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_bnbw partition(mon_p='${mon_p}',kpi='kpi_2_42')
  select primary_classification,
   case when tran_date(s.invoice_date)='未知' then '未知'
      when tran_date(s.invoice_date)<'2013-10-01' 
      and mon_diff(t.mon_label,tran_date(s.invoice_date))<=23 then '保内'
      when tran_date(s.invoice_date)<'2013-10-01' 
      and mon_diff(t.mon_label,tran_date(s.invoice_date))>23 then '保外'
      when tran_date(s.invoice_date)>='2013-10-01' 
      and mon_diff(t.mon_label,tran_date(s.invoice_date))<=35 then '保内'
      when tran_date(s.invoice_date)>='2013-10-01' 
      and mon_diff(t.mon_label,tran_date(s.invoice_date))>35 then '保外'
      else '' end as bn,
   substr(t.mon_label,1,7) as mon,
   count(distinct s.vin) as num,
   s.asc_code
   from date_label t,label_doss s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.report_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.report_date),1,7)<=substr(t.mon_label,1,7)
    and s.primary_classification<>'' and s.asc_code<>''
    group by substr(t.mon_label,1,7),s.asc_code,primary_classification,case when tran_date(s.invoice_date)='未知' then '未知'
      when tran_date(s.invoice_date)<'2013-10-01' 
      and mon_diff(t.mon_label,tran_date(s.invoice_date))<=23 then '保内'
      when tran_date(s.invoice_date)<'2013-10-01' 
      and mon_diff(t.mon_label,tran_date(s.invoice_date))>23 then '保外'
      when tran_date(s.invoice_date)>='2013-10-01' 
      and mon_diff(t.mon_label,tran_date(s.invoice_date))<=35 then '保内'
      when tran_date(s.invoice_date)>='2013-10-01' 
      and mon_diff(t.mon_label,tran_date(s.invoice_date))>35 then '保外'
      else '' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_2.42(本年新车销售数_二级_车龄)
def kpi_b_agetype_kpi_2_42(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_agetype partition(mon_p='${mon_p}',kpi='kpi_2_42')
  select primary_classification,
   second_level_classification,
   case when tran_date(s.invoice_date)='未知' then '未知'
      when mon_diff(t.mon_label,tran_date(s.invoice_date))<=11 then '0-1年'
      when mon_diff(t.mon_label,tran_date(s.invoice_date))>=12
      and mon_diff(t.mon_label,tran_date(s.invoice_date))<=23 then '1-2年'
      when mon_diff(t.mon_label,tran_date(s.invoice_date))>=24
      and mon_diff(t.mon_label,tran_date(s.invoice_date))<=35 then '2-3年'
      when mon_diff(t.mon_label,tran_date(s.invoice_date))>=36
      and mon_diff(t.mon_label,tran_date(s.invoice_date))<=47 then '3-4年'
      when mon_diff(t.mon_label,tran_date(s.invoice_date))>=48
      and mon_diff(t.mon_label,tran_date(s.invoice_date))<=59 then '4-5年'
      else '5+年' end as agetype,
   substr(t.mon_label,1,7) as mon,
   count(distinct s.vin) as num,
   s.asc_code
   from date_label t,label_doss s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.report_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.report_date),1,7)<=substr(t.mon_label,1,7)
    and s.primary_classification<>'' and s.asc_code<>''
    group by substr(t.mon_label,1,7),s.asc_code,primary_classification,second_level_classification,case when tran_date(s.invoice_date)='未知' then '未知'
      when mon_diff(t.mon_label,tran_date(s.invoice_date))<=11 then '0-1年'
      when mon_diff(t.mon_label,tran_date(s.invoice_date))>=12
      and mon_diff(t.mon_label,tran_date(s.invoice_date))<=23 then '1-2年'
      when mon_diff(t.mon_label,tran_date(s.invoice_date))>=24
      and mon_diff(t.mon_label,tran_date(s.invoice_date))<=35 then '2-3年'
      when mon_diff(t.mon_label,tran_date(s.invoice_date))>=36
      and mon_diff(t.mon_label,tran_date(s.invoice_date))<=47 then '3-4年'
      when mon_diff(t.mon_label,tran_date(s.invoice_date))>=48
      and mon_diff(t.mon_label,tran_date(s.invoice_date))<=59 then '4-5年'
      else '5+年' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_2.42(本年新车销售数_二级_保内外)
def kpi_b_bnbw_kpi_2_42(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_bnbw partition(mon_p='${mon_p}',kpi='kpi_2_42')
  select primary_classification,
   second_level_classification,
   case when tran_date(s.invoice_date)='未知' then '未知'
      when tran_date(s.invoice_date)<'2013-10-01' 
      and mon_diff(t.mon_label,tran_date(s.invoice_date))<=23 then '保内'
      when tran_date(s.invoice_date)<'2013-10-01' 
      and mon_diff(t.mon_label,tran_date(s.invoice_date))>23 then '保外'
      when tran_date(s.invoice_date)>='2013-10-01' 
      and mon_diff(t.mon_label,tran_date(s.invoice_date))<=35 then '保内'
      when tran_date(s.invoice_date)>='2013-10-01' 
      and mon_diff(t.mon_label,tran_date(s.invoice_date))>35 then '保外'
      else '' end as bn,
   substr(t.mon_label,1,7) as mon,
   count(distinct s.vin) as num,
   s.asc_code
   from date_label t,label_doss s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.report_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.report_date),1,7)<=substr(t.mon_label,1,7)
    and s.primary_classification<>'' and s.asc_code<>''
    group by substr(t.mon_label,1,7),s.asc_code,primary_classification,second_level_classification,case when tran_date(s.invoice_date)='未知' then '未知'
      when tran_date(s.invoice_date)<'2013-10-01' 
      and mon_diff(t.mon_label,tran_date(s.invoice_date))<=23 then '保内'
      when tran_date(s.invoice_date)<'2013-10-01' 
      and mon_diff(t.mon_label,tran_date(s.invoice_date))>23 then '保外'
      when tran_date(s.invoice_date)>='2013-10-01' 
      and mon_diff(t.mon_label,tran_date(s.invoice_date))<=35 then '保内'
      when tran_date(s.invoice_date)>='2013-10-01' 
      and mon_diff(t.mon_label,tran_date(s.invoice_date))>35 then '保外'
      else '' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3(客户忠诚率_total)
def kpi_total_kpi_3(begin_day, end_day):
    line = """  insert overwrite table  kpi_total partition(mon_p='${mon_p}',kpi='kpi_3')
   select s.mon,
   s.cnt/t.num as num,
   s.asc_code
   from (
   select s.asc_code,s.mon,count(*) as cnt from ( 
    select s.asc_code,
    s.vin,
    substr(t.mon_label,1,7) as mon,
    count(distinct case when s.MAINT_TYPE1='保养' and  s.order_balance_amount>0 then tran_date(s.order_balance_date) else null end) 
  from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    group by s.asc_code,
    s.vin,
    substr(t.mon_label,1,7)
    having count(distinct case when s.MAINT_TYPE1='保养' and  s.order_balance_amount>0 then tran_date(s.order_balance_date) else null end)>=2
    ) s group by s.asc_code,s.mon
    ) s ,
    (select * from kpi_total where kpi='kpi_2_2') t
    where s.asc_code=t.asc_code and s.mon=t.mon"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3(客户忠诚率_一级)
def kpi_a_level_kpi_3(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_level partition(mon_p='${mon_p}',kpi='kpi_3')
  select s.primary_classification,
   s.mon,
   s.cnt/t.num as num,
   s.asc_code
   from (
   select s.asc_code,s.primary_classification,s.mon,count(*) as cnt from 
   ( 
    select s.asc_code,
    s.primary_classification,
    s.vin,
    substr(t.mon_label,1,7) as mon,
    count(distinct case when s.MAINT_TYPE1='保养' and  s.order_balance_amount>0 then tran_date(s.order_balance_date) else null end) 
  from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    group by s.asc_code,
    s.vin,
    substr(t.mon_label,1,7),
    s.primary_classification
    having count(distinct case when s.MAINT_TYPE1='保养' and  s.order_balance_amount>0 then tran_date(s.order_balance_date) else null end)>=2
    ) s group by s.asc_code,s.mon,primary_classification
    ) s ,
    (select * from kpi_a_level where kpi='kpi_2_2') t
    where s.asc_code=t.asc_code and s.mon=t.mon and s.primary_classification=t.a_level"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3(客户忠诚率_二级)
def kpi_b_level_kpi_3(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_level partition(mon_p='${mon_p}',kpi='kpi_3')
  select s.primary_classification,
   s.second_level_classification,
   s.mon,
   s.cnt/t.num as num,
   s.asc_code
   from (
   select s.asc_code,s.primary_classification,s.second_level_classification,s.mon,count(*) as cnt from 
   ( 
    select s.asc_code,
    s.primary_classification,
    s.second_level_classification,
    s.vin,
    substr(t.mon_label,1,7) as mon,
    count(distinct case when s.MAINT_TYPE1='保养' and  s.order_balance_amount>0 then tran_date(s.order_balance_date) else null end) 
  from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    group by s.asc_code,
    s.vin,
    substr(t.mon_label,1,7),
    s.primary_classification,
    s.second_level_classification
    having count(distinct case when s.MAINT_TYPE1='保养' and  s.order_balance_amount>0 then tran_date(s.order_balance_date) else null end)>=2
    ) s group by s.asc_code,s.mon,primary_classification,s.second_level_classification
    ) s ,
    (select * from kpi_b_level where kpi='kpi_2_2') t
    where s.asc_code=t.asc_code and s.mon=t.mon and s.primary_classification=t.a_level
    and s.second_level_classification=t.b_level
    """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3(客户忠诚率_车龄)
def kpi_agetype_kpi_3(begin_day, end_day):
    line = """  insert overwrite table  kpi_agetype partition(mon_p='${mon_p}',kpi='kpi_3')
  select s.agetype,
   s.mon,
   s.cnt/t.num as num,
   s.asc_code
   from (
   select s.asc_code,s.agetype,s.mon,count(*) as cnt from 
   ( 
    select s.asc_code,
    case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end as agetype,
    s.vin,
    substr(t.mon_label,1,7) as mon,
    count(distinct case when s.MAINT_TYPE1='保养' and  s.order_balance_amount>0 then tran_date(s.order_balance_date) else null end) 
  from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    group by s.asc_code,
    s.vin,
    substr(t.mon_label,1,7),
    case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end
    having count(distinct case when s.MAINT_TYPE1='保养' and  s.order_balance_amount>0 then tran_date(s.order_balance_date) else null end)>=2
    ) s group by s.asc_code,s.mon,agetype
    ) s ,
    (select * from kpi_agetype where kpi='kpi_2_2') t
    where s.asc_code=t.asc_code and s.mon=t.mon and s.agetype=t.age_type"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3(客户忠诚率_保内外)
def kpi_bnbw_kpi_3(begin_day, end_day):
    line = """  insert overwrite table  kpi_bnbw partition(mon_p='${mon_p}',kpi='kpi_3')
  select s.bn,
   s.mon,
   s.cnt/t.num as num,
   s.asc_code
   from (
   select s.asc_code,s.bn,s.mon,count(*) as cnt from 
   ( 
    select s.asc_code,
    case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end as bn,
    s.vin,
    substr(t.mon_label,1,7) as mon,
    count(distinct case when s.MAINT_TYPE1='保养' and  s.order_balance_amount>0 then tran_date(s.order_balance_date) else null end) 
  from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    group by s.asc_code,
    s.vin,
    substr(t.mon_label,1,7),
    case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end 
    having count(distinct case when s.MAINT_TYPE1='保养' and  s.order_balance_amount>0 then tran_date(s.order_balance_date) else null end)>=2
    ) s group by s.asc_code,s.mon,bn
    ) s ,
    (select * from kpi_bnbw where kpi='kpi_2_2') t
    where s.asc_code=t.asc_code and s.mon=t.mon and s.bn=t.bnbw"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3(客户忠诚率_一级_车龄)
def kpi_a_agetype_kpi_3(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_agetype partition(mon_p='${mon_p}',kpi='kpi_3')
  select s.primary_classification,s.agetype,
   s.mon,
   s.cnt/t.num as num,
   s.asc_code
   from (
   select s.asc_code,s.primary_classification,s.agetype,s.mon,count(*) as cnt from 
   ( 
    select s.asc_code,primary_classification,
    case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end as agetype,
    s.vin,
    substr(t.mon_label,1,7) as mon,
    count(distinct case when s.MAINT_TYPE1='保养' and  s.order_balance_amount>0 then tran_date(s.order_balance_date) else null end) 
  from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    group by s.asc_code,primary_classification,
    s.vin,
    substr(t.mon_label,1,7),
    case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end
    having count(distinct case when s.MAINT_TYPE1='保养' and  s.order_balance_amount>0 then tran_date(s.order_balance_date) else null end)>=2
    ) s group by s.asc_code,s.mon,agetype,primary_classification
    ) s ,
    (select * from kpi_a_agetype where kpi='kpi_2_2') t
    where s.asc_code=t.asc_code and s.mon=t.mon and s.agetype=t.age_type and s.primary_classification=t.a_level"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3(客户忠诚率_一级_保内外)
def kpi_a_bnbw_kpi_3(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_bnbw partition(mon_p='${mon_p}',kpi='kpi_3')
  select s.primary_classification,s.bn,
   s.mon,
   s.cnt/t.num as num,
   s.asc_code
   from (
   select s.asc_code,s.primary_classification,s.bn,s.mon,count(*) as cnt from 
   ( 
    select s.asc_code,primary_classification,
    case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end as bn,
    s.vin,
    substr(t.mon_label,1,7) as mon,
    count(distinct case when s.MAINT_TYPE1='保养' and  s.order_balance_amount>0 then tran_date(s.order_balance_date) else null end) 
  from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    group by s.asc_code,primary_classification,
    s.vin,
    substr(t.mon_label,1,7),
    case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end
    having count(distinct case when s.MAINT_TYPE1='保养' and  s.order_balance_amount>0 then tran_date(s.order_balance_date) else null end)>=2
    ) s group by s.asc_code,s.mon,bn,primary_classification
    ) s ,
    (select * from kpi_a_bnbw where kpi='kpi_2_2') t
    where s.asc_code=t.asc_code and s.mon=t.mon and s.bn=t.bnbw and s.primary_classification=t.a_level"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3(客户忠诚率_二级_车龄)
def kpi_b_agetype_kpi_3(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_agetype partition(mon_p='${mon_p}',kpi='kpi_3')
  select s.primary_classification,s.second_level_classification,s.agetype,
   s.mon,
   s.cnt/t.num as num,
   s.asc_code
   from (
   select s.asc_code,s.primary_classification,s.second_level_classification,
   s.agetype,s.mon,count(*) as cnt from 
   ( 
    select s.asc_code,primary_classification,s.second_level_classification,
    case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end as agetype,
    s.vin,
    substr(t.mon_label,1,7) as mon,
    count(distinct case when s.MAINT_TYPE1='保养' and  s.order_balance_amount>0 then tran_date(s.order_balance_date) else null end) 
  from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    group by s.asc_code,primary_classification,s.second_level_classification,
    s.vin,
    substr(t.mon_label,1,7),
    case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end
    having count(distinct case when s.MAINT_TYPE1='保养' and  s.order_balance_amount>0 then tran_date(s.order_balance_date) else null end)>=2
    ) s group by s.asc_code,s.mon,agetype,primary_classification,s.second_level_classification
    ) s ,
    (select * from kpi_b_agetype where kpi='kpi_2_2') t
    where s.asc_code=t.asc_code and s.mon=t.mon and s.agetype=t.age_type and s.primary_classification=t.a_level
    and s.second_level_classification=t.b_level
    """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3(客户忠诚率_二级_保内外)
def kpi_b_bnbw_kpi_3(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_bnbw partition(mon_p='${mon_p}',kpi='kpi_3')
  select s.primary_classification,s.second_level_classification,s.bn,
   s.mon,
   s.cnt/t.num as num,
   s.asc_code
   from (
   select s.asc_code,s.primary_classification,s.second_level_classification,
   s.bn,s.mon,count(*) as cnt from 
   ( 
    select s.asc_code,primary_classification,s.second_level_classification,
    case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end as bn,
    s.vin,
    substr(t.mon_label,1,7) as mon,
    count(distinct case when s.MAINT_TYPE1='保养' and  s.order_balance_amount>0 then tran_date(s.order_balance_date) else null end) 
  from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    group by s.asc_code,primary_classification,s.second_level_classification,
    s.vin,
    substr(t.mon_label,1,7),
    case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end 
    having count(distinct case when s.MAINT_TYPE1='保养' and  s.order_balance_amount>0 then tran_date(s.order_balance_date) else null end)>=2
    ) s group by s.asc_code,s.mon,bn,primary_classification,s.second_level_classification
    ) s ,
    (select * from kpi_b_bnbw where kpi='kpi_2_2') t
    where s.asc_code=t.asc_code and s.mon=t.mon and s.bn=t.bnbw and s.primary_classification=t.a_level
    and s.second_level_classification=t.b_level
    """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.1(首保进站率_total)
def kpi_total_kpi_3_1(begin_day, end_day):
    line = """  insert overwrite table  kpi_total partition(mon_p='${mon_p}',kpi='kpi_3_1')
    select s.mon,
    sum(case when t.vin is not null then 1 else 0 end)/count(s.vin) as num,
    s.asc_code
    from (
    select s.asc_code,
    s.vin,
    substr(t.mon_label,1,7) as mon 
    from date_label t,label_doss s
    where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.report_date),1,7)=substr(add_months(t.mon_label,-7),1,7)
    and s.primary_classification<>''
    ) s left join(  
    select distinct s.asc_code,
    s.vin,
    substr(t.mon_label,1,7) as mon 
    from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-6),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and first_maintnance=1
    ) t on s.asc_code=t.asc_code and s.vin=t.vin and s.mon=t.mon
    group by s.mon,s.asc_code"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.1(首保进站率_一级)
def kpi_a_level_kpi_3_1(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_level partition(mon_p='${mon_p}',kpi='kpi_3_1')
    select s.primary_classification,s.mon,
    sum(case when t.vin is not null then 1 else 0 end)/count(s.vin) as num,
    s.asc_code 
    from (
    select s.asc_code,s.primary_classification,
    s.vin,
    substr(t.mon_label,1,7) as mon 
    from date_label t,label_doss s
    where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.report_date),1,7)=substr(add_months(t.mon_label,-7),1,7)
    and s.primary_classification<>''
    ) s left join(  
    select distinct s.asc_code,s.primary_classification,
    s.vin,
    substr(t.mon_label,1,7) as mon 
    from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-6),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and first_maintnance=1
    ) t on s.asc_code=t.asc_code and s.vin=t.vin and s.mon=t.mon 
    and s.primary_classification=t.primary_classification
    group by s.mon,s.asc_code,s.primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.1(首保进站率_二级)
def kpi_b_level_kpi_3_1(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_level partition(mon_p='${mon_p}',kpi='kpi_3_1')
  select s.primary_classification,s.second_level_classification,s.mon,
    sum(case when t.vin is not null then 1 else 0 end)/count(s.vin) as num,
    s.asc_code
    from (
    select s.asc_code,s.primary_classification,s.second_level_classification,
    s.vin,
    substr(t.mon_label,1,7) as mon 
    from date_label t,label_doss s
    where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.report_date),1,7)=substr(add_months(t.mon_label,-7),1,7)
    and s.primary_classification<>''
    ) s left join(  
    select distinct s.asc_code,s.primary_classification,s.second_level_classification,
    s.vin,
    substr(t.mon_label,1,7) as mon 
    from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-6),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and first_maintnance=1
    ) t on s.asc_code=t.asc_code and s.vin=t.vin and s.mon=t.mon 
    and s.primary_classification=t.primary_classification
    and s.second_level_classification=t.second_level_classification
    group by s.mon,s.asc_code,s.primary_classification,s.second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.1(首保进站率_车龄)
def kpi_agetype_kpi_3_1(begin_day, end_day):
    line = """  insert overwrite table  kpi_agetype partition(mon_p='${mon_p}',kpi='kpi_3_1')
  select s.age_type,s.mon,
    sum(case when t.vin is not null then 1 else 0 end)/count(s.vin) as num,
    s.asc_code
    from (
    select s.asc_code,case when tran_date(s.invoice_date)='未知' then '未知'
      when mon_diff(t.mon_label,tran_date(s.invoice_date))<=11 then '0-1年'
      when mon_diff(t.mon_label,tran_date(s.invoice_date))>=12
      and mon_diff(t.mon_label,tran_date(s.invoice_date))<=23 then '1-2年'
      when mon_diff(t.mon_label,tran_date(s.invoice_date))>=24
      and mon_diff(t.mon_label,tran_date(s.invoice_date))<=35 then '2-3年'
      when mon_diff(t.mon_label,tran_date(s.invoice_date))>=36
      and mon_diff(t.mon_label,tran_date(s.invoice_date))<=47 then '3-4年'
      when mon_diff(t.mon_label,tran_date(s.invoice_date))>=48
      and mon_diff(t.mon_label,tran_date(s.invoice_date))<=59 then '4-5年'
      else '5+年' end as age_type,
    s.vin,
    substr(t.mon_label,1,7) as mon 
    from date_label t,label_doss s
    where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.report_date),1,7)=substr(add_months(t.mon_label,-7),1,7)
    and s.primary_classification<>''
    ) s left join(  
    select distinct s.asc_code,case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end as age_type,
    s.vin,
    substr(t.mon_label,1,7) as mon 
    from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-6),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and first_maintnance=1
    ) t on s.asc_code=t.asc_code and s.vin=t.vin and s.mon=t.mon 
    and s.age_type=t.age_type
    group by s.mon,s.asc_code,s.age_type"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.1(首保进站率_保内外)
def kpi_bnbw_kpi_3_1(begin_day, end_day):
    line = """  insert overwrite table  kpi_bnbw partition(mon_p='${mon_p}',kpi='kpi_3_1')
  select s.bn,s.mon,
    sum(case when t.vin is not null then 1 else 0 end)/count(s.vin) as num,
    s.asc_code
    from (
    select s.asc_code,case when tran_date(s.invoice_date)='未知' then '未知'
      when tran_date(s.invoice_date)<'2013-10-01' 
  	and mon_diff(t.mon_label,tran_date(s.invoice_date))<=23 then '保内'
  	when tran_date(s.invoice_date)<'2013-10-01' 
  	and mon_diff(t.mon_label,tran_date(s.invoice_date))>23 then '保外'
  	when tran_date(s.invoice_date)>='2013-10-01' 
  	and mon_diff(t.mon_label,tran_date(s.invoice_date))<=35 then '保内'
  	when tran_date(s.invoice_date)>='2013-10-01' 
  	and mon_diff(t.mon_label,tran_date(s.invoice_date))>35 then '保外'
  	else '' end as bn,
    s.vin,
    substr(t.mon_label,1,7) as mon 
    from date_label t,label_doss s
    where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.report_date),1,7)=substr(add_months(t.mon_label,-7),1,7)
    and s.primary_classification<>''
    ) s left join(  
    select distinct s.asc_code,case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end as bn,
    s.vin,
    substr(t.mon_label,1,7) as mon 
    from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-6),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and first_maintnance=1
    ) t on s.asc_code=t.asc_code and s.vin=t.vin and s.mon=t.mon 
    and s.bn=t.bn
    group by s.mon,s.asc_code,s.bn"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.1(首保进站率_一级_车龄)
def kpi_a_agetype_kpi_3_1(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_agetype partition(mon_p='${mon_p}',kpi='kpi_3_1')
  select s.primary_classification,s.age_type,s.mon,
    sum(case when t.vin is not null then 1 else 0 end)/count(s.vin) as num,
    s.asc_code
    from (
    select s.asc_code,case when tran_date(s.invoice_date)='未知' then '未知'
      when mon_diff(t.mon_label,tran_date(s.invoice_date))<=11 then '0-1年'
      when mon_diff(t.mon_label,tran_date(s.invoice_date))>=12
      and mon_diff(t.mon_label,tran_date(s.invoice_date))<=23 then '1-2年'
      when mon_diff(t.mon_label,tran_date(s.invoice_date))>=24
      and mon_diff(t.mon_label,tran_date(s.invoice_date))<=35 then '2-3年'
      when mon_diff(t.mon_label,tran_date(s.invoice_date))>=36
      and mon_diff(t.mon_label,tran_date(s.invoice_date))<=47 then '3-4年'
      when mon_diff(t.mon_label,tran_date(s.invoice_date))>=48
      and mon_diff(t.mon_label,tran_date(s.invoice_date))<=59 then '4-5年'
      else '5+年' end as age_type,
  	s.primary_classification,
    s.vin,
    substr(t.mon_label,1,7) as mon 
    from date_label t,label_doss s
    where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.report_date),1,7)=substr(add_months(t.mon_label,-7),1,7)
    and s.primary_classification<>''
    ) s left join(  
    select distinct s.asc_code,case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end as age_type,
    s.vin,
    substr(t.mon_label,1,7) as mon ,
    s.primary_classification
    from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-6),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and first_maintnance=1
    ) t on s.asc_code=t.asc_code and s.vin=t.vin and s.mon=t.mon 
    and s.age_type=t.age_type and s.primary_classification=t.primary_classification
    group by s.mon,s.asc_code,s.age_type,s.primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.1(首保进站率_一级_保内外)
def kpi_a_bnbw_kpi_3_1(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_bnbw partition(mon_p='${mon_p}',kpi='kpi_3_1')
  select s.primary_classification,s.bn,s.mon,
    sum(case when t.vin is not null then 1 else 0 end)/count(s.vin) as num,
    s.asc_code
    from (
    select s.primary_classification,
      s.asc_code,case when tran_date(s.invoice_date)='未知' then '未知'
      when tran_date(s.invoice_date)<'2013-10-01' 
  	and mon_diff(t.mon_label,tran_date(s.invoice_date))<=23 then '保内'
  	when tran_date(s.invoice_date)<'2013-10-01' 
  	and mon_diff(t.mon_label,tran_date(s.invoice_date))>23 then '保外'
  	when tran_date(s.invoice_date)>='2013-10-01' 
  	and mon_diff(t.mon_label,tran_date(s.invoice_date))<=35 then '保内'
  	when tran_date(s.invoice_date)>='2013-10-01' 
  	and mon_diff(t.mon_label,tran_date(s.invoice_date))>35 then '保外'
  	else '' end as bn,
    s.vin,
    substr(t.mon_label,1,7) as mon 
    from date_label t,label_doss s
    where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.report_date),1,7)=substr(add_months(t.mon_label,-7),1,7)
    and s.primary_classification<>''
    ) s left join(  
    select distinct s.primary_classification,
      s.asc_code,case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end as bn,
    s.vin,
    substr(t.mon_label,1,7) as mon 
    from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-6),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and first_maintnance=1
    ) t on s.asc_code=t.asc_code and s.vin=t.vin and s.mon=t.mon 
    and s.bn=t.bn and s.primary_classification=t.primary_classification
    group by s.mon,s.asc_code,s.bn,s.primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.1(首保进站率_二级_车龄)
def kpi_b_agetype_kpi_3_1(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_agetype partition(mon_p='${mon_p}',kpi='kpi_3_1')
  select s.primary_classification,s.second_level_classification,s.age_type,s.mon,
    sum(case when t.vin is not null then 1 else 0 end)/count(s.vin) as num,
    s.asc_code
    from (
    select s.asc_code,case when tran_date(s.invoice_date)='未知' then '未知'
      when mon_diff(t.mon_label,tran_date(s.invoice_date))<=11 then '0-1年'
      when mon_diff(t.mon_label,tran_date(s.invoice_date))>=12
      and mon_diff(t.mon_label,tran_date(s.invoice_date))<=23 then '1-2年'
      when mon_diff(t.mon_label,tran_date(s.invoice_date))>=24
      and mon_diff(t.mon_label,tran_date(s.invoice_date))<=35 then '2-3年'
      when mon_diff(t.mon_label,tran_date(s.invoice_date))>=36
      and mon_diff(t.mon_label,tran_date(s.invoice_date))<=47 then '3-4年'
      when mon_diff(t.mon_label,tran_date(s.invoice_date))>=48
      and mon_diff(t.mon_label,tran_date(s.invoice_date))<=59 then '4-5年'
      else '5+年' end as age_type,
  	s.primary_classification,
  	s.second_level_classification,
    s.vin,
    substr(t.mon_label,1,7) as mon 
    from date_label t,label_doss s
    where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.report_date),1,7)=substr(add_months(t.mon_label,-7),1,7)
    and s.primary_classification<>''
    ) s left join(  
    select distinct s.asc_code,case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end as age_type,
    s.vin,
    substr(t.mon_label,1,7) as mon ,
    s.primary_classification,
    s.second_level_classification
    from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-6),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and first_maintnance=1
    ) t on s.asc_code=t.asc_code and s.vin=t.vin and s.mon=t.mon 
    and s.age_type=t.age_type and s.primary_classification=t.primary_classification
    and s.second_level_classification=t.second_level_classification
    group by s.mon,s.asc_code,s.age_type,s.primary_classification,s.second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.1(首保进站率_二级_保内外)
def kpi_b_bnbw_kpi_3_1(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_bnbw partition(mon_p='${mon_p}',kpi='kpi_3_1')
  select s.primary_classification,second_level_classification,s.bn,s.mon,
    sum(case when t.vin is not null then 1 else 0 end)/count(s.vin) as num,
    s.asc_code
    from (
    select s.primary_classification,s.second_level_classification,
      s.asc_code,case when tran_date(s.invoice_date)='未知' then '未知'
      when tran_date(s.invoice_date)<'2013-10-01' 
  	and mon_diff(t.mon_label,tran_date(s.invoice_date))<=23 then '保内'
  	when tran_date(s.invoice_date)<'2013-10-01' 
  	and mon_diff(t.mon_label,tran_date(s.invoice_date))>23 then '保外'
  	when tran_date(s.invoice_date)>='2013-10-01' 
  	and mon_diff(t.mon_label,tran_date(s.invoice_date))<=35 then '保内'
  	when tran_date(s.invoice_date)>='2013-10-01' 
  	and mon_diff(t.mon_label,tran_date(s.invoice_date))>35 then '保外'
  	else '' end as bn,
    s.vin,
    substr(t.mon_label,1,7) as mon 
    from date_label t,label_doss s
    where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.report_date),1,7)=substr(add_months(t.mon_label,-7),1,7)
    and s.primary_classification<>''
    ) s left join(  
    select distinct s.primary_classification,s.second_level_classification,
      s.asc_code,case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end as bn,
    s.vin,
    substr(t.mon_label,1,7) as mon 
    from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-6),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and first_maintnance=1
    ) t on s.asc_code=t.asc_code and s.vin=t.vin and s.mon=t.mon 
    and s.bn=t.bn and s.primary_classification=t.primary_classification
    and s.second_level_classification=t.second_level_classification
    group by s.mon,s.asc_code,s.bn,s.primary_classification,s.second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.12(本店销售车辆首保回站率_total)
def kpi_total_kpi_3_12(begin_day, end_day):
    line = """  insert overwrite table  kpi_total partition(mon_p='${mon_p}',kpi='kpi_3_12')
    select s.mon,
    sum(case when t.vin is not null then 1 else 0 end)/count(s.vin) as num,
    s.asc_code
    from (
    select s.asc_code,
    s.vin,
    substr(t.mon_label,1,7) as mon 
    from date_label t,label_doss s
    where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.report_date),1,7)>=substr(add_months(t.mon_label,-17),1,7)
    and substr(tran_date(s.report_date),1,7)<=substr(t.mon_label,1,7)
    and s.primary_classification<>''
    ) s left join(  
    select distinct s.asc_code,
    s.vin,
    substr(t.mon_label,1,7) as mon 
    from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-17),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and first_maintnance=1
    ) t on s.asc_code=t.asc_code and s.vin=t.vin and s.mon=t.mon
    group by s.mon,s.asc_code"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.12(本店销售车辆首保回站率_一级)
def kpi_a_level_kpi_3_12(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_level partition(mon_p='${mon_p}',kpi='kpi_3_12')
    select s.primary_classification,s.mon,
    sum(case when t.vin is not null then 1 else 0 end)/count(s.vin) as num,
    s.asc_code
    from (
    select s.asc_code,s.primary_classification,
    s.vin,
    substr(t.mon_label,1,7) as mon 
    from date_label t,label_doss s
    where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.report_date),1,7)>=substr(add_months(t.mon_label,-17),1,7)
    and substr(tran_date(s.report_date),1,7)<=substr(t.mon_label,1,7)
    and s.primary_classification<>''
    ) s left join(  
    select distinct s.asc_code,s.primary_classification,
    s.vin,
    substr(t.mon_label,1,7) as mon 
    from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-17),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and first_maintnance=1
    ) t on s.asc_code=t.asc_code and s.vin=t.vin and s.mon=t.mon 
    and s.primary_classification=t.primary_classification
    group by s.mon,s.asc_code,s.primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.12(本店销售车辆首保回站率_二级)
def kpi_b_level_kpi_3_12(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_level partition(mon_p='${mon_p}',kpi='kpi_3_12')
  select s.primary_classification,s.second_level_classification,s.mon,
    sum(case when t.vin is not null then 1 else 0 end)/count(s.vin) as num,
    s.asc_code
    from (
    select s.asc_code,s.primary_classification,s.second_level_classification,
    s.vin,
    substr(t.mon_label,1,7) as mon 
    from date_label t,label_doss s
    where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.report_date),1,7)>=substr(add_months(t.mon_label,-17),1,7)
    and substr(tran_date(s.report_date),1,7)<=substr(t.mon_label,1,7)
    and s.primary_classification<>''
    ) s left join(  
    select distinct s.asc_code,s.primary_classification,s.second_level_classification,
    s.vin,
    substr(t.mon_label,1,7) as mon 
    from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-17),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and first_maintnance=1
    ) t on s.asc_code=t.asc_code and s.vin=t.vin and s.mon=t.mon 
    and s.primary_classification=t.primary_classification
    and s.second_level_classification=t.second_level_classification
    group by s.mon,s.asc_code,s.primary_classification,s.second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.12(本店销售车辆首保回站率_车龄)
def kpi_agetype_kpi_3_12(begin_day, end_day):
    line = """  insert overwrite table  kpi_agetype partition(mon_p='${mon_p}',kpi='kpi_3_12')
  select s.age_type,s.mon,
    sum(case when t.vin is not null then 1 else 0 end)/count(s.vin) as num,
    s.asc_code
    from (
    select s.asc_code,case when tran_date(s.invoice_date)='未知' then '未知'
      when mon_diff(t.mon_label,tran_date(s.invoice_date))<=11 then '0-1年'
      when mon_diff(t.mon_label,tran_date(s.invoice_date))>=12
      and mon_diff(t.mon_label,tran_date(s.invoice_date))<=23 then '1-2年'
      when mon_diff(t.mon_label,tran_date(s.invoice_date))>=24
      and mon_diff(t.mon_label,tran_date(s.invoice_date))<=35 then '2-3年'
      when mon_diff(t.mon_label,tran_date(s.invoice_date))>=36
      and mon_diff(t.mon_label,tran_date(s.invoice_date))<=47 then '3-4年'
      when mon_diff(t.mon_label,tran_date(s.invoice_date))>=48
      and mon_diff(t.mon_label,tran_date(s.invoice_date))<=59 then '4-5年'
      else '5+年' end as age_type,
    s.vin,
    substr(t.mon_label,1,7) as mon 
    from date_label t,label_doss s
    where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.report_date),1,7)>=substr(add_months(t.mon_label,-17),1,7)
    and substr(tran_date(s.report_date),1,7)<=substr(t.mon_label,1,7)
    and s.primary_classification<>''
    ) s left join(  
    select distinct s.asc_code,case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end as age_type,
    s.vin,
    substr(t.mon_label,1,7) as mon 
    from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-17),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and first_maintnance=1
    ) t on s.asc_code=t.asc_code and s.vin=t.vin and s.mon=t.mon 
    and s.age_type=t.age_type
    group by s.mon,s.asc_code,s.age_type"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.12(本店销售车辆首保回站率_保内外)
def kpi_bnbw_kpi_3_12(begin_day, end_day):
    line = """  insert overwrite table  kpi_bnbw partition(mon_p='${mon_p}',kpi='kpi_3_12')
  select s.bn,s.mon,
    sum(case when t.vin is not null then 1 else 0 end)/count(s.vin) as num,
    s.asc_code
    from (
    select s.asc_code,case when tran_date(s.invoice_date)='未知' then '未知'
      when tran_date(s.invoice_date)<'2013-10-01' 
  	and mon_diff(t.mon_label,tran_date(s.invoice_date))<=23 then '保内'
  	when tran_date(s.invoice_date)<'2013-10-01' 
  	and mon_diff(t.mon_label,tran_date(s.invoice_date))>23 then '保外'
  	when tran_date(s.invoice_date)>='2013-10-01' 
  	and mon_diff(t.mon_label,tran_date(s.invoice_date))<=35 then '保内'
  	when tran_date(s.invoice_date)>='2013-10-01' 
  	and mon_diff(t.mon_label,tran_date(s.invoice_date))>35 then '保外'
  	else '' end as bn,
    s.vin,
    substr(t.mon_label,1,7) as mon 
    from date_label t,label_doss s
    where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.report_date),1,7)>=substr(add_months(t.mon_label,-17),1,7)
    and substr(tran_date(s.report_date),1,7)<=substr(t.mon_label,1,7)
    and s.primary_classification<>''
    ) s left join(  
    select distinct s.asc_code,case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end as bn,
    s.vin,
    substr(t.mon_label,1,7) as mon 
    from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-17),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and first_maintnance=1
    ) t on s.asc_code=t.asc_code and s.vin=t.vin and s.mon=t.mon 
    and s.bn=t.bn
    group by s.mon,s.asc_code,s.bn"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.12(本店销售车辆首保回站率_一级_车龄)
def kpi_a_agetype_kpi_3_12(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_agetype partition(mon_p='${mon_p}',kpi='kpi_3_12')
  select s.primary_classification,s.age_type,s.mon,
    sum(case when t.vin is not null then 1 else 0 end)/count(s.vin) as num,
    s.asc_code
    from (
    select s.asc_code,case when tran_date(s.invoice_date)='未知' then '未知'
      when mon_diff(t.mon_label,tran_date(s.invoice_date))<=11 then '0-1年'
      when mon_diff(t.mon_label,tran_date(s.invoice_date))>=12
      and mon_diff(t.mon_label,tran_date(s.invoice_date))<=23 then '1-2年'
      when mon_diff(t.mon_label,tran_date(s.invoice_date))>=24
      and mon_diff(t.mon_label,tran_date(s.invoice_date))<=35 then '2-3年'
      when mon_diff(t.mon_label,tran_date(s.invoice_date))>=36
      and mon_diff(t.mon_label,tran_date(s.invoice_date))<=47 then '3-4年'
      when mon_diff(t.mon_label,tran_date(s.invoice_date))>=48
      and mon_diff(t.mon_label,tran_date(s.invoice_date))<=59 then '4-5年'
      else '5+年' end as age_type,
  	s.primary_classification,
    s.vin,
    substr(t.mon_label,1,7) as mon 
    from date_label t,label_doss s
    where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.report_date),1,7)>=substr(add_months(t.mon_label,-17),1,7)
    and substr(tran_date(s.report_date),1,7)<=substr(t.mon_label,1,7)
    and s.primary_classification<>''
    ) s left join(  
    select distinct s.asc_code,case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end as age_type,
    s.vin,
    substr(t.mon_label,1,7) as mon ,
    s.primary_classification
    from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-17),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and first_maintnance=1
    ) t on s.asc_code=t.asc_code and s.vin=t.vin and s.mon=t.mon 
    and s.age_type=t.age_type and s.primary_classification=t.primary_classification
    group by s.mon,s.asc_code,s.age_type,s.primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.12(本店销售车辆首保回站率_一级_保内外)
def kpi_a_bnbw_kpi_3_12(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_bnbw partition(mon_p='${mon_p}',kpi='kpi_3_12')
  select s.primary_classification,s.bn,s.mon,
    sum(case when t.vin is not null then 1 else 0 end)/count(s.vin) as num,
    s.asc_code
    from (
    select s.primary_classification,
      s.asc_code,case when tran_date(s.invoice_date)='未知' then '未知'
      when tran_date(s.invoice_date)<'2013-10-01' 
  	and mon_diff(t.mon_label,tran_date(s.invoice_date))<=23 then '保内'
  	when tran_date(s.invoice_date)<'2013-10-01' 
  	and mon_diff(t.mon_label,tran_date(s.invoice_date))>23 then '保外'
  	when tran_date(s.invoice_date)>='2013-10-01' 
  	and mon_diff(t.mon_label,tran_date(s.invoice_date))<=35 then '保内'
  	when tran_date(s.invoice_date)>='2013-10-01' 
  	and mon_diff(t.mon_label,tran_date(s.invoice_date))>35 then '保外'
  	else '' end as bn,
    s.vin,
    substr(t.mon_label,1,7) as mon 
    from date_label t,label_doss s
    where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.report_date),1,7)>=substr(add_months(t.mon_label,-17),1,7)
    and substr(tran_date(s.report_date),1,7)<=substr(t.mon_label,1,7)
    and s.primary_classification<>''
    ) s left join(  
    select distinct s.primary_classification,
      s.asc_code,case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end as bn,
    s.vin,
    substr(t.mon_label,1,7) as mon 
    from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-17),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and first_maintnance=1
    ) t on s.asc_code=t.asc_code and s.vin=t.vin and s.mon=t.mon 
    and s.bn=t.bn and s.primary_classification=t.primary_classification
    group by s.mon,s.asc_code,s.bn,s.primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.12(本店销售车辆首保回站率_二级_车龄)
def kpi_b_agetype_kpi_3_12(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_agetype partition(mon_p='${mon_p}',kpi='kpi_3_12')
  select s.primary_classification,s.second_level_classification,s.age_type,s.mon,
    sum(case when t.vin is not null then 1 else 0 end)/count(s.vin) as num,
    s.asc_code
    from (
    select s.asc_code,case when tran_date(s.invoice_date)='未知' then '未知'
      when mon_diff(t.mon_label,tran_date(s.invoice_date))<=11 then '0-1年'
      when mon_diff(t.mon_label,tran_date(s.invoice_date))>=12
      and mon_diff(t.mon_label,tran_date(s.invoice_date))<=23 then '1-2年'
      when mon_diff(t.mon_label,tran_date(s.invoice_date))>=24
      and mon_diff(t.mon_label,tran_date(s.invoice_date))<=35 then '2-3年'
      when mon_diff(t.mon_label,tran_date(s.invoice_date))>=36
      and mon_diff(t.mon_label,tran_date(s.invoice_date))<=47 then '3-4年'
      when mon_diff(t.mon_label,tran_date(s.invoice_date))>=48
      and mon_diff(t.mon_label,tran_date(s.invoice_date))<=59 then '4-5年'
      else '5+年' end as age_type,
  	s.primary_classification,
  	s.second_level_classification,
    s.vin,
    substr(t.mon_label,1,7) as mon 
    from date_label t,label_doss s
    where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.report_date),1,7)>=substr(add_months(t.mon_label,-17),1,7)
    and substr(tran_date(s.report_date),1,7)<=substr(t.mon_label,1,7)
    and s.primary_classification<>''
    ) s left join(  
    select distinct s.asc_code,case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end as age_type,
    s.vin,
    substr(t.mon_label,1,7) as mon ,
    s.primary_classification,
    s.second_level_classification
    from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-17),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and first_maintnance=1
    ) t on s.asc_code=t.asc_code and s.vin=t.vin and s.mon=t.mon 
    and s.age_type=t.age_type and s.primary_classification=t.primary_classification
    and s.second_level_classification=t.second_level_classification
    group by s.mon,s.asc_code,s.age_type,s.primary_classification,s.second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.12(本店销售车辆首保回站率_二级_保内外)
def kpi_b_bnbw_kpi_3_12(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_bnbw partition(mon_p='${mon_p}',kpi='kpi_3_12')
  select s.primary_classification,second_level_classification,s.bn,s.mon,
    sum(case when t.vin is not null then 1 else 0 end)/count(s.vin) as num,
    s.asc_code
    from (
    select s.primary_classification,s.second_level_classification,
      s.asc_code,case when tran_date(s.invoice_date)='未知' then '未知'
      when tran_date(s.invoice_date)<'2013-10-01' 
  	and mon_diff(t.mon_label,tran_date(s.invoice_date))<=23 then '保内'
  	when tran_date(s.invoice_date)<'2013-10-01' 
  	and mon_diff(t.mon_label,tran_date(s.invoice_date))>23 then '保外'
  	when tran_date(s.invoice_date)>='2013-10-01' 
  	and mon_diff(t.mon_label,tran_date(s.invoice_date))<=35 then '保内'
  	when tran_date(s.invoice_date)>='2013-10-01' 
  	and mon_diff(t.mon_label,tran_date(s.invoice_date))>35 then '保外'
  	else '' end as bn,
    s.vin,
    substr(t.mon_label,1,7) as mon 
    from date_label t,label_doss s
    where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.report_date),1,7)>=substr(add_months(t.mon_label,-17),1,7)
    and substr(tran_date(s.report_date),1,7)<=substr(t.mon_label,1,7)
    and s.primary_classification<>''
    ) s left join(  
    select distinct s.primary_classification,s.second_level_classification,
      s.asc_code,case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end as bn,
    s.vin,
    substr(t.mon_label,1,7) as mon 
    from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-17),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and first_maintnance=1
    ) t on s.asc_code=t.asc_code and s.vin=t.vin and s.mon=t.mon 
    and s.bn=t.bn and s.primary_classification=t.primary_classification
    and s.second_level_classification=t.second_level_classification
    group by s.mon,s.asc_code,s.bn,s.primary_classification,s.second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.2(首次付费保养进站率_total)
def kpi_total_kpi_3_2(begin_day, end_day):
    line = """  insert overwrite table  kpi_total partition(mon_p='${mon_p}',kpi='kpi_3_2')
  select s.mon,
  sum(case when t.vin is not null then 1 else 0 end)/count(s.vin) as num,
  s.asc_code 
   from (
  select distinct s.asc_code,
    s.vin,
    substr(t.mon_label,1,7) as mon
   from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)=substr(add_months(t.mon_label,-7),1,7)
    and s.MAINT_TYPE1<>'删除'
    and first_maintnance=1
  ) s left join (
  select distinct s.asc_code,
    s.vin,
    substr(t.mon_label,1,7) as mon
    from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-6),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1='保养'
    and s.order_balance_amount>0
    ) t on s.asc_code=t.asc_code and s.mon=t.mon and s.vin=t.vin
    group by s.mon,s.asc_code """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.2(首次付费保养进站率_一级)
def kpi_a_level_kpi_3_2(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_level partition(mon_p='${mon_p}',kpi='kpi_3_2')
    select s.primary_classification,s.mon,
  sum(case when t.vin is not null then 1 else 0 end)/count(s.vin) as num,
  s.asc_code 
   from (
  select distinct s.asc_code,s.primary_classification,
    s.vin,
    substr(t.mon_label,1,7) as mon
   from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)=substr(add_months(t.mon_label,-7),1,7)
    and s.MAINT_TYPE1<>'删除'
    and first_maintnance=1
  ) s left join (
  select distinct s.asc_code,s.primary_classification,
    s.vin,
    substr(t.mon_label,1,7) as mon
    from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-6),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1='保养'
    and s.order_balance_amount>0
    ) t on s.asc_code=t.asc_code and s.mon=t.mon and s.vin=t.vin 
    and s.primary_classification=t.primary_classification
    group by s.mon,s.asc_code,s.primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.2(首次付费保养进站率_二级)
def kpi_b_level_kpi_3_2(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_level partition(mon_p='${mon_p}',kpi='kpi_3_2')
   select s.primary_classification,s.second_level_classification,s.mon,
  sum(case when t.vin is not null then 1 else 0 end)/count(s.vin) as num,
  s.asc_code 
   from (
  select distinct s.asc_code,s.primary_classification,s.second_level_classification,
    s.vin,
    substr(t.mon_label,1,7) as mon
   from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)=substr(add_months(t.mon_label,-7),1,7)
    and s.MAINT_TYPE1<>'删除'
    and first_maintnance=1
  ) s left join (
  select distinct s.asc_code,s.primary_classification,s.second_level_classification,
    s.vin,
    substr(t.mon_label,1,7) as mon
    from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-6),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1='保养'
    and s.order_balance_amount>0
    ) t on s.asc_code=t.asc_code and s.mon=t.mon and s.vin=t.vin 
    and s.primary_classification=t.primary_classification
    and s.second_level_classification=t.second_level_classification
    group by s.mon,s.asc_code,s.primary_classification,s.second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.2(首次付费保养进站率_车龄)
def kpi_agetype_kpi_3_2(begin_day, end_day):
    line = """  insert overwrite table  kpi_agetype partition(mon_p='${mon_p}',kpi='kpi_3_2')
  select s.age_type,s.mon,
  sum(case when t.vin is not null then 1 else 0 end)/count(s.vin) as num,
  s.asc_code 
   from (
  select distinct s.asc_code,case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end as age_type,
    s.vin,
    substr(t.mon_label,1,7) as mon
   from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)=substr(add_months(t.mon_label,-7),1,7)
    and s.MAINT_TYPE1<>'删除'
    and first_maintnance=1
  ) s left join (
  select distinct s.asc_code,case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end as age_type,
    s.vin,
    substr(t.mon_label,1,7) as mon
    from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-6),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1='保养'
    and s.order_balance_amount>0
    ) t on s.asc_code=t.asc_code and s.mon=t.mon and s.vin=t.vin 
    and s.age_type=t.age_type
    group by s.mon,s.asc_code,s.age_type"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.2(首次付费保养进站率_保内外)
def kpi_bnbw_kpi_3_2(begin_day, end_day):
    line = """  insert overwrite table  kpi_bnbw partition(mon_p='${mon_p}',kpi='kpi_3_2')
  select s.bn,s.mon,
  sum(case when t.vin is not null then 1 else 0 end)/count(s.vin) as num,
  s.asc_code 
   from (
  select distinct s.asc_code,case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end as bn,
    s.vin,
    substr(t.mon_label,1,7) as mon
   from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)=substr(add_months(t.mon_label,-7),1,7)
    and s.MAINT_TYPE1<>'删除'
    and first_maintnance=1
  ) s left join (
  select distinct s.asc_code,case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end as bn,
    s.vin,
    substr(t.mon_label,1,7) as mon
    from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-6),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1='保养'
    and s.order_balance_amount>0
    ) t on s.asc_code=t.asc_code and s.mon=t.mon and s.vin=t.vin 
    and s.bn=t.bn
    group by s.mon,s.asc_code,s.bn"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.2(首次付费保养进站率_一级_车龄)
def kpi_a_agetype_kpi_3_2(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_agetype partition(mon_p='${mon_p}',kpi='kpi_3_2')
  select s.primary_classification,s.age_type,s.mon,
  sum(case when t.vin is not null then 1 else 0 end)/count(s.vin) as num,
  s.asc_code 
   from (
  select distinct s.asc_code,s.primary_classification,
      case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end as age_type,
    s.vin,
    substr(t.mon_label,1,7) as mon
   from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)=substr(add_months(t.mon_label,-7),1,7)
    and s.MAINT_TYPE1<>'删除'
    and first_maintnance=1
  ) s left join (
  select distinct s.asc_code,s.primary_classification,
      case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end as age_type,
    s.vin,
    substr(t.mon_label,1,7) as mon
    from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-6),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1='保养'
    and s.order_balance_amount>0
    ) t on s.asc_code=t.asc_code and s.mon=t.mon and s.vin=t.vin 
    and s.age_type=t.age_type and s.primary_classification=t.primary_classification
    group by s.mon,s.asc_code,s.age_type,s.primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.2(首次付费保养进站率_一级_保内外)
def kpi_a_bnbw_kpi_3_2(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_bnbw partition(mon_p='${mon_p}',kpi='kpi_3_2')
  select s.primary_classification,s.bn,s.mon,
  sum(case when t.vin is not null then 1 else 0 end)/count(s.vin) as num,
  s.asc_code 
   from (
  select distinct s.asc_code,s.primary_classification,
      case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end as bn,
    s.vin,
    substr(t.mon_label,1,7) as mon
   from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)=substr(add_months(t.mon_label,-7),1,7)
    and s.MAINT_TYPE1<>'删除'
    and first_maintnance=1
  ) s left join (
  select distinct s.asc_code,s.primary_classification,
      case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end as bn,
    s.vin,
    substr(t.mon_label,1,7) as mon
    from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-6),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1='保养'
    and s.order_balance_amount>0
    ) t on s.asc_code=t.asc_code and s.mon=t.mon and s.vin=t.vin 
    and s.bn=t.bn and s.primary_classification=t.primary_classification
    group by s.mon,s.asc_code,s.bn,s.primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.2(首次付费保养进站率_二级_车龄)
def kpi_b_agetype_kpi_3_2(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_agetype partition(mon_p='${mon_p}',kpi='kpi_3_2')
  select s.primary_classification,s.second_level_classification,s.age_type,s.mon,
  sum(case when t.vin is not null then 1 else 0 end)/count(s.vin) as num,
  s.asc_code 
   from (
  select distinct s.asc_code,s.primary_classification,s.second_level_classification,
      case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end as age_type,
    s.vin,
    substr(t.mon_label,1,7) as mon
   from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)=substr(add_months(t.mon_label,-7),1,7)
    and s.MAINT_TYPE1<>'删除'
    and first_maintnance=1
  ) s left join (
  select distinct s.asc_code,s.primary_classification,s.second_level_classification,
      case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end as age_type,
    s.vin,
    substr(t.mon_label,1,7) as mon
    from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-6),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1='保养'
    and s.order_balance_amount>0
    ) t on s.asc_code=t.asc_code and s.mon=t.mon and s.vin=t.vin 
    and s.age_type=t.age_type and s.primary_classification=t.primary_classification
    and s.second_level_classification=t.second_level_classification
    group by s.mon,s.asc_code,s.age_type,s.primary_classification,s.second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.2(首次付费保养进站率_二级_保内外)
def kpi_b_bnbw_kpi_3_2(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_bnbw partition(mon_p='${mon_p}',kpi='kpi_3_2')
  select s.primary_classification,s.second_level_classification,s.bn,s.mon,
  sum(case when t.vin is not null then 1 else 0 end)/count(s.vin) as num,
  s.asc_code 
   from (
  select distinct s.asc_code,s.primary_classification,s.second_level_classification,
      case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end as bn,
    s.vin,
    substr(t.mon_label,1,7) as mon
   from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)=substr(add_months(t.mon_label,-7),1,7)
    and s.MAINT_TYPE1<>'删除'
    and first_maintnance=1
  ) s left join (
  select distinct s.asc_code,s.primary_classification,s.second_level_classification,
      case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end as bn,
    s.vin,
    substr(t.mon_label,1,7) as mon
    from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-6),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1='保养'
    and s.order_balance_amount>0
    ) t on s.asc_code=t.asc_code and s.mon=t.mon and s.vin=t.vin 
    and s.bn=t.bn and s.primary_classification=t.primary_classification
    and s.second_level_classification=t.second_level_classification
    group by s.mon,s.asc_code,s.bn,s.primary_classification,s.second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.4(养护品着装率_total)
def kpi_total_kpi_3_4(begin_day, end_day):
    line = """  insert overwrite table  kpi_total partition(mon_p='${mon_p}',kpi='kpi_3_4')
   select  
   substr(tran_date(s.order_balance_date),1,7) as mon,
   count(distinct case when t.asc_code is not null then s.vin else null end)/count(distinct s.vin) as num,
   s.asc_code
   from label_order s 
   left join (
    select distinct t.asc_code,t.order_number from part t ,maintnance x
   where  t.part_number=x.part_num 
     ) t 
    on  s.order_number=t.order_number
    and s.asc_code=t.asc_code
    where substr(tran_date(s.order_balance_date),1,7)>='${begin_date}'
    and substr(tran_date(s.order_balance_date),1,7)<='${end_date}'
    and s.MAINT_TYPE1<>'删除'
    group by substr(tran_date(s.order_balance_date),1,7),s.asc_code"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.4(养护品着装率_一级)
def kpi_a_level_kpi_3_4(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_level partition(mon_p='${mon_p}',kpi='kpi_3_4')
     select  primary_classification,
   substr(tran_date(s.order_balance_date),1,7) as mon,
   count(distinct case when t.asc_code is not null then s.vin else null end)/count(distinct s.vin) as num,
   s.asc_code
   from label_order s 
   left join (
    select distinct t.asc_code,t.order_number from part t ,maintnance x
   where  t.part_number=x.part_num 
     ) t 
    on  s.order_number=t.order_number
    and s.asc_code=t.asc_code
    where substr(tran_date(s.order_balance_date),1,7)>='${begin_date}'
    and substr(tran_date(s.order_balance_date),1,7)<='${end_date}'
    and s.MAINT_TYPE1<>'删除'
    group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.4(养护品着装率_二级)
def kpi_b_level_kpi_3_4(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_level partition(mon_p='${mon_p}',kpi='kpi_3_4')
     select  primary_classification,second_level_classification,
   substr(tran_date(s.order_balance_date),1,7) as mon,
   count(distinct case when t.asc_code is not null then s.vin else null end)/count(distinct s.vin) as num,
   s.asc_code
   from label_order s 
   left join (
    select distinct t.asc_code,t.order_number from part t ,maintnance x
   where  t.part_number=x.part_num 
     ) t 
    on  s.order_number=t.order_number
    and s.asc_code=t.asc_code
    where substr(tran_date(s.order_balance_date),1,7)>='${begin_date}'
    and substr(tran_date(s.order_balance_date),1,7)<='${end_date}'
    and s.MAINT_TYPE1<>'删除'
    group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,primary_classification,second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.4(养护品着装率_车龄)
def kpi_agetype_kpi_3_4(begin_day, end_day):
    line = """  insert overwrite table  kpi_agetype partition(mon_p='${mon_p}',kpi='kpi_3_4')
   select  age_type,
   substr(tran_date(s.order_balance_date),1,7) as mon,
   count(distinct case when t.asc_code is not null then s.vin else null end)/count(distinct s.vin) as num,
   s.asc_code
   from label_order s 
   left join (
    select distinct t.asc_code,t.order_number from part t ,maintnance x
   where  t.part_number=x.part_num 
     ) t 
    on  s.order_number=t.order_number
    and s.asc_code=t.asc_code
    where substr(tran_date(s.order_balance_date),1,7)>='${begin_date}'
    and substr(tran_date(s.order_balance_date),1,7)<='${end_date}'
    and s.MAINT_TYPE1<>'删除'
    group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,age_type"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.4(养护品着装率_保内外)
def kpi_bnbw_kpi_3_4(begin_day, end_day):
    line = """  insert overwrite table  kpi_bnbw partition(mon_p='${mon_p}',kpi='kpi_3_4')
  select  bn,
   substr(tran_date(s.order_balance_date),1,7) as mon,
   count(distinct case when t.asc_code is not null then s.vin else null end)/count(distinct s.vin) as num,
   s.asc_code
   from label_order s 
   left join (
    select distinct t.asc_code,t.order_number from part t ,maintnance x
   where  t.part_number=x.part_num 
     ) t 
    on  s.order_number=t.order_number
    and s.asc_code=t.asc_code
    where substr(tran_date(s.order_balance_date),1,7)>='${begin_date}'
    and substr(tran_date(s.order_balance_date),1,7)<='${end_date}'
    and s.MAINT_TYPE1<>'删除'
    group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,bn"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.4(养护品着装率_一级_车龄)
def kpi_a_agetype_kpi_3_4(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_agetype partition(mon_p='${mon_p}',kpi='kpi_3_4')
  select  primary_classification, age_type,
   substr(tran_date(s.order_balance_date),1,7) as mon,
   count(distinct case when t.asc_code is not null then s.vin else null end)/count(distinct s.vin) as num,
   s.asc_code
   from label_order s 
   left join (
    select distinct t.asc_code,t.order_number from part t ,maintnance x
   where  t.part_number=x.part_num 
     ) t 
    on  s.order_number=t.order_number
    and s.asc_code=t.asc_code
    where substr(tran_date(s.order_balance_date),1,7)>='${begin_date}'
    and substr(tran_date(s.order_balance_date),1,7)<='${end_date}'
    and s.MAINT_TYPE1<>'删除'
    group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,age_type,primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.4(养护品着装率_一级_保内外)
def kpi_a_bnbw_kpi_3_4(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_bnbw partition(mon_p='${mon_p}',kpi='kpi_3_4')
  select  primary_classification, bn,
   substr(tran_date(s.order_balance_date),1,7) as mon,
   count(distinct case when t.asc_code is not null then s.vin else null end)/count(distinct s.vin) as num,
   s.asc_code
   from label_order s 
   left join (
    select distinct t.asc_code,t.order_number from part t ,maintnance x
   where  t.part_number=x.part_num 
     ) t 
    on  s.order_number=t.order_number
    and s.asc_code=t.asc_code
    where substr(tran_date(s.order_balance_date),1,7)>='${begin_date}'
    and substr(tran_date(s.order_balance_date),1,7)<='${end_date}'
    and s.MAINT_TYPE1<>'删除'
    group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,bn,primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.4(养护品着装率_二级_车龄)
def kpi_b_agetype_kpi_3_4(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_agetype partition(mon_p='${mon_p}',kpi='kpi_3_4')
  select  primary_classification,second_level_classification, age_type,
   substr(tran_date(s.order_balance_date),1,7) as mon,
   count(distinct case when t.asc_code is not null then s.vin else null end)/count(distinct s.vin) as num,
   s.asc_code
   from label_order s 
   left join (
    select distinct t.asc_code,t.order_number from part t ,maintnance x
   where  t.part_number=x.part_num 
     ) t 
    on  s.order_number=t.order_number
    and s.asc_code=t.asc_code
    where substr(tran_date(s.order_balance_date),1,7)>='${begin_date}'
    and substr(tran_date(s.order_balance_date),1,7)<='${end_date}'
    and s.MAINT_TYPE1<>'删除'
    group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,age_type,primary_classification,second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.4(养护品着装率_二级_保内外)
def kpi_b_bnbw_kpi_3_4(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_bnbw partition(mon_p='${mon_p}',kpi='kpi_3_4')
  select  primary_classification,second_level_classification, bn,
   substr(tran_date(s.order_balance_date),1,7) as mon,
   count(distinct case when t.asc_code is not null then s.vin else null end)/count(distinct s.vin) as num,
   s.asc_code
   from label_order s 
   left join (
    select distinct t.asc_code,t.order_number from part t ,maintnance x
   where  t.part_number=x.part_num 
     ) t 
    on  s.order_number=t.order_number
    and s.asc_code=t.asc_code
    where substr(tran_date(s.order_balance_date),1,7)>='${begin_date}'
    and substr(tran_date(s.order_balance_date),1,7)<='${end_date}'
    and s.MAINT_TYPE1<>'删除'
    group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,bn,primary_classification,second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.5(高流失件渗透率_total)
def kpi_total_kpi_3_5(begin_day, end_day):
    line = """  insert overwrite table  kpi_total partition(mon_p='${mon_p}',kpi='kpi_3_5')
   select  
   substr(tran_date(s.order_balance_date),1,7) as mon,
   count(distinct case when t.asc_code is not null then s.vin else null end)/count(distinct s.vin) as num,
   s.asc_code
   from label_order s 
   left join (
    select distinct t.asc_code,t.order_number from part t ,high_flow_parts x
   where  t.part_number=x.part_num 
     ) t 
    on  s.order_number=t.order_number
    and s.asc_code=t.asc_code
    where substr(tran_date(s.order_balance_date),1,7)>='${begin_date}'
    and substr(tran_date(s.order_balance_date),1,7)<='${end_date}'
    and s.MAINT_TYPE1<>'删除'
    group by substr(tran_date(s.order_balance_date),1,7),s.asc_code"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.5(高流失件渗透率_一级)
def kpi_a_level_kpi_3_5(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_level partition(mon_p='${mon_p}',kpi='kpi_3_5')
     select  primary_classification,
   substr(tran_date(s.order_balance_date),1,7) as mon,
   count(distinct case when t.asc_code is not null then s.vin else null end)/count(distinct s.vin) as num,
   s.asc_code
   from label_order s 
   left join (
    select distinct t.asc_code,t.order_number from part t ,high_flow_parts x
   where  t.part_number=x.part_num 
     ) t 
    on  s.order_number=t.order_number
    and s.asc_code=t.asc_code
    where substr(tran_date(s.order_balance_date),1,7)>='${begin_date}'
    and substr(tran_date(s.order_balance_date),1,7)<='${end_date}'
    and s.MAINT_TYPE1<>'删除'
    group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.5(高流失件渗透率_二级)
def kpi_b_level_kpi_3_5(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_level partition(mon_p='${mon_p}',kpi='kpi_3_5')
     select  primary_classification,second_level_classification,
   substr(tran_date(s.order_balance_date),1,7) as mon,
   count(distinct case when t.asc_code is not null then s.vin else null end)/count(distinct s.vin) as num,
   s.asc_code
   from label_order s 
   left join (
    select distinct t.asc_code,t.order_number from part t ,high_flow_parts x
   where  t.part_number=x.part_num 
     ) t 
    on  s.order_number=t.order_number
    and s.asc_code=t.asc_code
    where substr(tran_date(s.order_balance_date),1,7)>='${begin_date}'
    and substr(tran_date(s.order_balance_date),1,7)<='${end_date}'
    and s.MAINT_TYPE1<>'删除'
    group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,primary_classification,second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.5(高流失件渗透率_车龄)
def kpi_agetype_kpi_3_5(begin_day, end_day):
    line = """  insert overwrite table  kpi_agetype partition(mon_p='${mon_p}',kpi='kpi_3_5')
   select  age_type,
   substr(tran_date(s.order_balance_date),1,7) as mon,
   count(distinct case when t.asc_code is not null then s.vin else null end)/count(distinct s.vin) as num,
   s.asc_code
   from label_order s 
   left join (
    select distinct t.asc_code,t.order_number from part t ,high_flow_parts x
   where  t.part_number=x.part_num 
     ) t 
    on  s.order_number=t.order_number
    and s.asc_code=t.asc_code
    where substr(tran_date(s.order_balance_date),1,7)>='${begin_date}'
    and substr(tran_date(s.order_balance_date),1,7)<='${end_date}'
    and s.MAINT_TYPE1<>'删除'
    group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,age_type"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.5(高流失件渗透率_保内外)
def kpi_bnbw_kpi_3_5(begin_day, end_day):
    line = """  insert overwrite table  kpi_bnbw partition(mon_p='${mon_p}',kpi='kpi_3_5')
  select  bn,
   substr(tran_date(s.order_balance_date),1,7) as mon,
   count(distinct case when t.asc_code is not null then s.vin else null end)/count(distinct s.vin) as num,
   s.asc_code
   from label_order s 
   left join (
    select distinct t.asc_code,t.order_number from part t ,high_flow_parts x
   where  t.part_number=x.part_num 
     ) t 
    on  s.order_number=t.order_number
    and s.asc_code=t.asc_code
    where substr(tran_date(s.order_balance_date),1,7)>='${begin_date}'
    and substr(tran_date(s.order_balance_date),1,7)<='${end_date}'
    and s.MAINT_TYPE1<>'删除'
    group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,bn"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.5(高流失件渗透率_一级_车龄)
def kpi_a_agetype_kpi_3_5(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_agetype partition(mon_p='${mon_p}',kpi='kpi_3_5')
  select  primary_classification, age_type,
   substr(tran_date(s.order_balance_date),1,7) as mon,
   count(distinct case when t.asc_code is not null then s.vin else null end)/count(distinct s.vin) as num,
   s.asc_code
   from label_order s 
   left join (
    select distinct t.asc_code,t.order_number from part t ,high_flow_parts x
   where  t.part_number=x.part_num 
     ) t 
    on  s.order_number=t.order_number
    and s.asc_code=t.asc_code
    where substr(tran_date(s.order_balance_date),1,7)>='${begin_date}'
    and substr(tran_date(s.order_balance_date),1,7)<='${end_date}'
    and s.MAINT_TYPE1<>'删除'
    group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,age_type,primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.5(高流失件渗透率_一级_保内外)
def kpi_a_bnbw_kpi_3_5(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_bnbw partition(mon_p='${mon_p}',kpi='kpi_3_5')
  select  primary_classification, bn,
   substr(tran_date(s.order_balance_date),1,7) as mon,
   count(distinct case when t.asc_code is not null then s.vin else null end)/count(distinct s.vin) as num,
   s.asc_code
   from label_order s 
   left join (
    select distinct t.asc_code,t.order_number from part t ,high_flow_parts x
   where  t.part_number=x.part_num 
     ) t 
    on  s.order_number=t.order_number
    and s.asc_code=t.asc_code
    where substr(tran_date(s.order_balance_date),1,7)>='${begin_date}'
    and substr(tran_date(s.order_balance_date),1,7)<='${end_date}'
    and s.MAINT_TYPE1<>'删除'
    group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,bn,primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.5(高流失件渗透率_二级_车龄)
def kpi_b_agetype_kpi_3_5(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_agetype partition(mon_p='${mon_p}',kpi='kpi_3_5')
  select  primary_classification,second_level_classification, age_type,
   substr(tran_date(s.order_balance_date),1,7) as mon,
   count(distinct case when t.asc_code is not null then s.vin else null end)/count(distinct s.vin) as num,
   s.asc_code
   from label_order s 
   left join (
    select distinct t.asc_code,t.order_number from part t ,high_flow_parts x
   where  t.part_number=x.part_num 
     ) t 
    on  s.order_number=t.order_number
    and s.asc_code=t.asc_code
    where substr(tran_date(s.order_balance_date),1,7)>='${begin_date}'
    and substr(tran_date(s.order_balance_date),1,7)<='${end_date}'
    and s.MAINT_TYPE1<>'删除'
    group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,age_type,primary_classification,second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.5(高流失件渗透率_二级_保内外)
def kpi_b_bnbw_kpi_3_5(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_bnbw partition(mon_p='${mon_p}',kpi='kpi_3_5')
  select  primary_classification,second_level_classification, bn,
   substr(tran_date(s.order_balance_date),1,7) as mon,
   count(distinct case when t.asc_code is not null then s.vin else null end)/count(distinct s.vin) as num,
   s.asc_code
   from label_order s 
   left join (
    select distinct t.asc_code,t.order_number from part t ,high_flow_parts x
   where  t.part_number=x.part_num 
     ) t 
    on  s.order_number=t.order_number
    and s.asc_code=t.asc_code
    where substr(tran_date(s.order_balance_date),1,7)>='${begin_date}'
    and substr(tran_date(s.order_balance_date),1,7)<='${end_date}'
    and s.MAINT_TYPE1<>'删除'
    group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,bn,primary_classification,second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.51(轮胎_total)
def kpi_total_kpi_3_51(begin_day, end_day):
    line = """  insert overwrite table  kpi_total partition(mon_p='${mon_p}',kpi='kpi_3_51')
   select  
   substr(tran_date(s.order_balance_date),1,7) as mon,
   count(distinct case when t.asc_code is not null then s.vin else null end)/count(distinct s.vin) as num,
   s.asc_code
   from label_order s 
   left join (
    select distinct t.asc_code,t.order_number from part t ,high_flow_parts x
   where  t.part_number=x.part_num  and x.type='LunT'
     ) t 
    on  s.order_number=t.order_number
    and s.asc_code=t.asc_code
    where substr(tran_date(s.order_balance_date),1,7)>='${begin_date}'
    and substr(tran_date(s.order_balance_date),1,7)<='${end_date}'
    and s.MAINT_TYPE1<>'删除'
    group by substr(tran_date(s.order_balance_date),1,7),s.asc_code"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.51(轮胎_一级)
def kpi_a_level_kpi_3_51(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_level partition(mon_p='${mon_p}',kpi='kpi_3_51')
     select  primary_classification,
   substr(tran_date(s.order_balance_date),1,7) as mon,
   count(distinct case when t.asc_code is not null then s.vin else null end)/count(distinct s.vin) as num,
   s.asc_code
   from label_order s 
   left join (
    select distinct t.asc_code,t.order_number from part t ,high_flow_parts x
   where  t.part_number=x.part_num  and x.type='LunT'
     ) t 
    on  s.order_number=t.order_number
    and s.asc_code=t.asc_code
    where substr(tran_date(s.order_balance_date),1,7)>='${begin_date}'
    and substr(tran_date(s.order_balance_date),1,7)<='${end_date}'
    and s.MAINT_TYPE1<>'删除'
    group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.51(轮胎_二级)
def kpi_b_level_kpi_3_51(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_level partition(mon_p='${mon_p}',kpi='kpi_3_51')
     select  primary_classification,second_level_classification,
   substr(tran_date(s.order_balance_date),1,7) as mon,
   count(distinct case when t.asc_code is not null then s.vin else null end)/count(distinct s.vin) as num,
   s.asc_code
   from label_order s 
   left join (
    select distinct t.asc_code,t.order_number from part t ,high_flow_parts x
   where  t.part_number=x.part_num  and x.type='LunT'
     ) t 
    on  s.order_number=t.order_number
    and s.asc_code=t.asc_code
    where substr(tran_date(s.order_balance_date),1,7)>='${begin_date}'
    and substr(tran_date(s.order_balance_date),1,7)<='${end_date}'
    and s.MAINT_TYPE1<>'删除'
    group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,primary_classification,second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.51(轮胎_车龄)
def kpi_agetype_kpi_3_51(begin_day, end_day):
    line = """  insert overwrite table  kpi_agetype partition(mon_p='${mon_p}',kpi='kpi_3_51')
   select  age_type,
   substr(tran_date(s.order_balance_date),1,7) as mon,
   count(distinct case when t.asc_code is not null then s.vin else null end)/count(distinct s.vin) as num,
   s.asc_code
   from label_order s 
   left join (
    select distinct t.asc_code,t.order_number from part t ,high_flow_parts x
   where  t.part_number=x.part_num  and x.type='LunT'
     ) t 
    on  s.order_number=t.order_number
    and s.asc_code=t.asc_code
    where substr(tran_date(s.order_balance_date),1,7)>='${begin_date}'
    and substr(tran_date(s.order_balance_date),1,7)<='${end_date}'
    and s.MAINT_TYPE1<>'删除'
    group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,age_type"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.51(轮胎_保内外)
def kpi_bnbw_kpi_3_51(begin_day, end_day):
    line = """  insert overwrite table  kpi_bnbw partition(mon_p='${mon_p}',kpi='kpi_3_51')
  select  bn,
   substr(tran_date(s.order_balance_date),1,7) as mon,
   count(distinct case when t.asc_code is not null then s.vin else null end)/count(distinct s.vin) as num,
   s.asc_code
   from label_order s 
   left join (
    select distinct t.asc_code,t.order_number from part t ,high_flow_parts x
   where  t.part_number=x.part_num  and x.type='LunT'
     ) t 
    on  s.order_number=t.order_number
    and s.asc_code=t.asc_code
    where substr(tran_date(s.order_balance_date),1,7)>='${begin_date}'
    and substr(tran_date(s.order_balance_date),1,7)<='${end_date}'
    and s.MAINT_TYPE1<>'删除'
    group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,bn"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.51(轮胎_一级_车龄)
def kpi_a_agetype_kpi_3_51(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_agetype partition(mon_p='${mon_p}',kpi='kpi_3_51')
  select  primary_classification, age_type,
   substr(tran_date(s.order_balance_date),1,7) as mon,
   count(distinct case when t.asc_code is not null then s.vin else null end)/count(distinct s.vin) as num,
   s.asc_code
   from label_order s 
   left join (
    select distinct t.asc_code,t.order_number from part t ,high_flow_parts x
   where  t.part_number=x.part_num  and x.type='LunT'
     ) t 
    on  s.order_number=t.order_number
    and s.asc_code=t.asc_code
    where substr(tran_date(s.order_balance_date),1,7)>='${begin_date}'
    and substr(tran_date(s.order_balance_date),1,7)<='${end_date}'
    and s.MAINT_TYPE1<>'删除'
    group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,age_type,primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.51(轮胎_一级_保内外)
def kpi_a_bnbw_kpi_3_51(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_bnbw partition(mon_p='${mon_p}',kpi='kpi_3_51')
  select  primary_classification, bn,
   substr(tran_date(s.order_balance_date),1,7) as mon,
   count(distinct case when t.asc_code is not null then s.vin else null end)/count(distinct s.vin) as num,
   s.asc_code
   from label_order s 
   left join (
    select distinct t.asc_code,t.order_number from part t ,high_flow_parts x
   where  t.part_number=x.part_num  and x.type='LunT'
     ) t 
    on  s.order_number=t.order_number
    and s.asc_code=t.asc_code
    where substr(tran_date(s.order_balance_date),1,7)>='${begin_date}'
    and substr(tran_date(s.order_balance_date),1,7)<='${end_date}'
    and s.MAINT_TYPE1<>'删除'
    group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,bn,primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.51(轮胎_二级_车龄)
def kpi_b_agetype_kpi_3_51(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_agetype partition(mon_p='${mon_p}',kpi='kpi_3_51')
  select  primary_classification,second_level_classification, age_type,
   substr(tran_date(s.order_balance_date),1,7) as mon,
   count(distinct case when t.asc_code is not null then s.vin else null end)/count(distinct s.vin) as num,
   s.asc_code
   from label_order s 
   left join (
    select distinct t.asc_code,t.order_number from part t ,high_flow_parts x
   where  t.part_number=x.part_num  and x.type='LunT'
     ) t 
    on  s.order_number=t.order_number
    and s.asc_code=t.asc_code
    where substr(tran_date(s.order_balance_date),1,7)>='${begin_date}'
    and substr(tran_date(s.order_balance_date),1,7)<='${end_date}'
    and s.MAINT_TYPE1<>'删除'
    group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,age_type,primary_classification,second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.51(轮胎_二级_保内外)
def kpi_b_bnbw_kpi_3_51(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_bnbw partition(mon_p='${mon_p}',kpi='kpi_3_51')
  select  primary_classification,second_level_classification, bn,
   substr(tran_date(s.order_balance_date),1,7) as mon,
   count(distinct case when t.asc_code is not null then s.vin else null end)/count(distinct s.vin) as num,
   s.asc_code
   from label_order s 
   left join (
    select distinct t.asc_code,t.order_number from part t ,high_flow_parts x
   where  t.part_number=x.part_num  and x.type='LunT'
     ) t 
    on  s.order_number=t.order_number
    and s.asc_code=t.asc_code
    where substr(tran_date(s.order_balance_date),1,7)>='${begin_date}'
    and substr(tran_date(s.order_balance_date),1,7)<='${end_date}'
    and s.MAINT_TYPE1<>'删除'
    group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,bn,primary_classification,second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.52(蓄电池_total)
def kpi_total_kpi_3_52(begin_day, end_day):
    line = """  insert overwrite table  kpi_total partition(mon_p='${mon_p}',kpi='kpi_3_52')
   select  
   substr(tran_date(s.order_balance_date),1,7) as mon,
   count(distinct case when t.asc_code is not null then s.vin else null end)/count(distinct s.vin) as num,
   s.asc_code
   from label_order s 
   left join (
    select distinct t.asc_code,t.order_number from part t ,high_flow_parts x
   where  t.part_number=x.part_num  and x.type='DianC'
     ) t 
    on  s.order_number=t.order_number
    and s.asc_code=t.asc_code
    where substr(tran_date(s.order_balance_date),1,7)>='${begin_date}'
    and substr(tran_date(s.order_balance_date),1,7)<='${end_date}'
    and s.MAINT_TYPE1<>'删除'
    group by substr(tran_date(s.order_balance_date),1,7),s.asc_code"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.52(蓄电池_一级)
def kpi_a_level_kpi_3_52(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_level partition(mon_p='${mon_p}',kpi='kpi_3_52')
     select  primary_classification,
   substr(tran_date(s.order_balance_date),1,7) as mon,
   count(distinct case when t.asc_code is not null then s.vin else null end)/count(distinct s.vin) as num,
   s.asc_code
   from label_order s 
   left join (
    select distinct t.asc_code,t.order_number from part t ,high_flow_parts x
   where  t.part_number=x.part_num  and x.type='DianC'
     ) t 
    on  s.order_number=t.order_number
    and s.asc_code=t.asc_code
    where substr(tran_date(s.order_balance_date),1,7)>='${begin_date}'
    and substr(tran_date(s.order_balance_date),1,7)<='${end_date}'
    and s.MAINT_TYPE1<>'删除'
    group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.52(蓄电池_二级)
def kpi_b_level_kpi_3_52(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_level partition(mon_p='${mon_p}',kpi='kpi_3_52')
     select  primary_classification,second_level_classification,
   substr(tran_date(s.order_balance_date),1,7) as mon,
   count(distinct case when t.asc_code is not null then s.vin else null end)/count(distinct s.vin) as num,
   s.asc_code
   from label_order s 
   left join (
    select distinct t.asc_code,t.order_number from part t ,high_flow_parts x
   where  t.part_number=x.part_num  and x.type='DianC'
     ) t 
    on  s.order_number=t.order_number
    and s.asc_code=t.asc_code
    where substr(tran_date(s.order_balance_date),1,7)>='${begin_date}'
    and substr(tran_date(s.order_balance_date),1,7)<='${end_date}'
    and s.MAINT_TYPE1<>'删除'
    group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,primary_classification,second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.52(蓄电池_车龄)
def kpi_agetype_kpi_3_52(begin_day, end_day):
    line = """  insert overwrite table  kpi_agetype partition(mon_p='${mon_p}',kpi='kpi_3_52')
   select  age_type,
   substr(tran_date(s.order_balance_date),1,7) as mon,
   count(distinct case when t.asc_code is not null then s.vin else null end)/count(distinct s.vin) as num,
   s.asc_code
   from label_order s 
   left join (
    select distinct t.asc_code,t.order_number from part t ,high_flow_parts x
   where  t.part_number=x.part_num  and x.type='DianC'
     ) t 
    on  s.order_number=t.order_number
    and s.asc_code=t.asc_code
    where substr(tran_date(s.order_balance_date),1,7)>='${begin_date}'
    and substr(tran_date(s.order_balance_date),1,7)<='${end_date}'
    and s.MAINT_TYPE1<>'删除'
    group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,age_type"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.52(蓄电池_保内外)
def kpi_bnbw_kpi_3_52(begin_day, end_day):
    line = """  insert overwrite table  kpi_bnbw partition(mon_p='${mon_p}',kpi='kpi_3_52')
  select  bn,
   substr(tran_date(s.order_balance_date),1,7) as mon,
   count(distinct case when t.asc_code is not null then s.vin else null end)/count(distinct s.vin) as num,
   s.asc_code
   from label_order s 
   left join (
    select distinct t.asc_code,t.order_number from part t ,high_flow_parts x
   where  t.part_number=x.part_num  and x.type='DianC'
     ) t 
    on  s.order_number=t.order_number
    and s.asc_code=t.asc_code
    where substr(tran_date(s.order_balance_date),1,7)>='${begin_date}'
    and substr(tran_date(s.order_balance_date),1,7)<='${end_date}'
    and s.MAINT_TYPE1<>'删除'
    group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,bn"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.52(蓄电池_一级_车龄)
def kpi_a_agetype_kpi_3_52(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_agetype partition(mon_p='${mon_p}',kpi='kpi_3_52')
  select  primary_classification, age_type,
   substr(tran_date(s.order_balance_date),1,7) as mon,
   count(distinct case when t.asc_code is not null then s.vin else null end)/count(distinct s.vin) as num,
   s.asc_code
   from label_order s 
   left join (
    select distinct t.asc_code,t.order_number from part t ,high_flow_parts x
   where  t.part_number=x.part_num  and x.type='DianC'
     ) t 
    on  s.order_number=t.order_number
    and s.asc_code=t.asc_code
    where substr(tran_date(s.order_balance_date),1,7)>='${begin_date}'
    and substr(tran_date(s.order_balance_date),1,7)<='${end_date}'
    and s.MAINT_TYPE1<>'删除'
    group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,age_type,primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.52(蓄电池_一级_保内外)
def kpi_a_bnbw_kpi_3_52(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_bnbw partition(mon_p='${mon_p}',kpi='kpi_3_52')
  select  primary_classification, bn,
   substr(tran_date(s.order_balance_date),1,7) as mon,
   count(distinct case when t.asc_code is not null then s.vin else null end)/count(distinct s.vin) as num,
   s.asc_code
   from label_order s 
   left join (
    select distinct t.asc_code,t.order_number from part t ,high_flow_parts x
   where  t.part_number=x.part_num  and x.type='DianC'
     ) t 
    on  s.order_number=t.order_number
    and s.asc_code=t.asc_code
    where substr(tran_date(s.order_balance_date),1,7)>='${begin_date}'
    and substr(tran_date(s.order_balance_date),1,7)<='${end_date}'
    and s.MAINT_TYPE1<>'删除'
    group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,bn,primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.52(蓄电池_二级_车龄)
def kpi_b_agetype_kpi_3_52(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_agetype partition(mon_p='${mon_p}',kpi='kpi_3_52')
  select  primary_classification,second_level_classification, age_type,
   substr(tran_date(s.order_balance_date),1,7) as mon,
   count(distinct case when t.asc_code is not null then s.vin else null end)/count(distinct s.vin) as num,
   s.asc_code
   from label_order s 
   left join (
    select distinct t.asc_code,t.order_number from part t ,high_flow_parts x
   where  t.part_number=x.part_num  and x.type='DianC'
     ) t 
    on  s.order_number=t.order_number
    and s.asc_code=t.asc_code
    where substr(tran_date(s.order_balance_date),1,7)>='${begin_date}'
    and substr(tran_date(s.order_balance_date),1,7)<='${end_date}'
    and s.MAINT_TYPE1<>'删除'
    group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,age_type,primary_classification,second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.52(蓄电池_二级_保内外)
def kpi_b_bnbw_kpi_3_52(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_bnbw partition(mon_p='${mon_p}',kpi='kpi_3_52')
  select  primary_classification,second_level_classification, bn,
   substr(tran_date(s.order_balance_date),1,7) as mon,
   count(distinct case when t.asc_code is not null then s.vin else null end)/count(distinct s.vin) as num,
   s.asc_code
   from label_order s 
   left join (
    select distinct t.asc_code,t.order_number from part t ,high_flow_parts x
   where  t.part_number=x.part_num  and x.type='DianC'
     ) t 
    on  s.order_number=t.order_number
    and s.asc_code=t.asc_code
    where substr(tran_date(s.order_balance_date),1,7)>='${begin_date}'
    and substr(tran_date(s.order_balance_date),1,7)<='${end_date}'
    and s.MAINT_TYPE1<>'删除'
    group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,bn,primary_classification,second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.53(轮胎+蓄电池_total)
def kpi_total_kpi_3_53(begin_day, end_day):
    line = """  insert overwrite table  kpi_total partition(mon_p='${mon_p}',kpi='kpi_3_53')
   select  
   substr(tran_date(s.order_balance_date),1,7) as mon,
   count(distinct case when t.asc_code is not null then s.vin else null end)/count(distinct s.vin) as num,
   s.asc_code
   from label_order s 
   left join (
    select distinct t.asc_code,t.order_number from part t ,high_flow_parts x
   where  t.part_number=x.part_num  and x.type in('DianC','LunT')
     ) t 
    on  s.order_number=t.order_number
    and s.asc_code=t.asc_code
    where substr(tran_date(s.order_balance_date),1,7)>='${begin_date}'
    and substr(tran_date(s.order_balance_date),1,7)<='${end_date}'
    and s.MAINT_TYPE1<>'删除'
    group by substr(tran_date(s.order_balance_date),1,7),s.asc_code"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.53(轮胎+蓄电池_一级)
def kpi_a_level_kpi_3_53(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_level partition(mon_p='${mon_p}',kpi='kpi_3_53')
     select  primary_classification,
   substr(tran_date(s.order_balance_date),1,7) as mon,
   count(distinct case when t.asc_code is not null then s.vin else null end)/count(distinct s.vin) as num,
   s.asc_code
   from label_order s 
   left join (
    select distinct t.asc_code,t.order_number from part t ,high_flow_parts x
   where  t.part_number=x.part_num  and x.type in('DianC','LunT')
     ) t 
    on  s.order_number=t.order_number
    and s.asc_code=t.asc_code
    where substr(tran_date(s.order_balance_date),1,7)>='${begin_date}'
    and substr(tran_date(s.order_balance_date),1,7)<='${end_date}'
    and s.MAINT_TYPE1<>'删除'
    group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.53(轮胎+蓄电池_二级)
def kpi_b_level_kpi_3_53(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_level partition(mon_p='${mon_p}',kpi='kpi_3_53')
     select  primary_classification,second_level_classification,
   substr(tran_date(s.order_balance_date),1,7) as mon,
   count(distinct case when t.asc_code is not null then s.vin else null end)/count(distinct s.vin) as num,
   s.asc_code
   from label_order s 
   left join (
    select distinct t.asc_code,t.order_number from part t ,high_flow_parts x
   where  t.part_number=x.part_num  and x.type in('DianC','LunT')
     ) t 
    on  s.order_number=t.order_number
    and s.asc_code=t.asc_code
    where substr(tran_date(s.order_balance_date),1,7)>='${begin_date}'
    and substr(tran_date(s.order_balance_date),1,7)<='${end_date}'
    and s.MAINT_TYPE1<>'删除'
    group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,primary_classification,second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.53(轮胎+蓄电池_车龄)
def kpi_agetype_kpi_3_53(begin_day, end_day):
    line = """  insert overwrite table  kpi_agetype partition(mon_p='${mon_p}',kpi='kpi_3_53')
   select  age_type,
   substr(tran_date(s.order_balance_date),1,7) as mon,
   count(distinct case when t.asc_code is not null then s.vin else null end)/count(distinct s.vin) as num,
   s.asc_code
   from label_order s 
   left join (
    select distinct t.asc_code,t.order_number from part t ,high_flow_parts x
   where  t.part_number=x.part_num  and x.type in('DianC','LunT')
     ) t 
    on  s.order_number=t.order_number
    and s.asc_code=t.asc_code
    where substr(tran_date(s.order_balance_date),1,7)>='${begin_date}'
    and substr(tran_date(s.order_balance_date),1,7)<='${end_date}'
    and s.MAINT_TYPE1<>'删除'
    group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,age_type"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.53(轮胎+蓄电池_保内外)
def kpi_bnbw_kpi_3_53(begin_day, end_day):
    line = """  insert overwrite table  kpi_bnbw partition(mon_p='${mon_p}',kpi='kpi_3_53')
  select  bn,
   substr(tran_date(s.order_balance_date),1,7) as mon,
   count(distinct case when t.asc_code is not null then s.vin else null end)/count(distinct s.vin) as num,
   s.asc_code
   from label_order s 
   left join (
    select distinct t.asc_code,t.order_number from part t ,high_flow_parts x
   where  t.part_number=x.part_num  and x.type in('DianC','LunT')
     ) t 
    on  s.order_number=t.order_number
    and s.asc_code=t.asc_code
    where substr(tran_date(s.order_balance_date),1,7)>='${begin_date}'
    and substr(tran_date(s.order_balance_date),1,7)<='${end_date}'
    and s.MAINT_TYPE1<>'删除'
    group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,bn"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.53(轮胎+蓄电池_一级_车龄)
def kpi_a_agetype_kpi_3_53(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_agetype partition(mon_p='${mon_p}',kpi='kpi_3_53')
  select  primary_classification, age_type,
   substr(tran_date(s.order_balance_date),1,7) as mon,
   count(distinct case when t.asc_code is not null then s.vin else null end)/count(distinct s.vin) as num,
   s.asc_code
   from label_order s 
   left join (
    select distinct t.asc_code,t.order_number from part t ,high_flow_parts x
   where  t.part_number=x.part_num  and x.type in('DianC','LunT')
     ) t 
    on  s.order_number=t.order_number
    and s.asc_code=t.asc_code
    where substr(tran_date(s.order_balance_date),1,7)>='${begin_date}'
    and substr(tran_date(s.order_balance_date),1,7)<='${end_date}'
    and s.MAINT_TYPE1<>'删除'
    group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,age_type,primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.53(轮胎+蓄电池_一级_保内外)
def kpi_a_bnbw_kpi_3_53(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_bnbw partition(mon_p='${mon_p}',kpi='kpi_3_53')
  select  primary_classification, bn,
   substr(tran_date(s.order_balance_date),1,7) as mon,
   count(distinct case when t.asc_code is not null then s.vin else null end)/count(distinct s.vin) as num,
   s.asc_code
   from label_order s 
   left join (
    select distinct t.asc_code,t.order_number from part t ,high_flow_parts x
   where  t.part_number=x.part_num  and x.type in('DianC','LunT')
     ) t 
    on  s.order_number=t.order_number
    and s.asc_code=t.asc_code
    where substr(tran_date(s.order_balance_date),1,7)>='${begin_date}'
    and substr(tran_date(s.order_balance_date),1,7)<='${end_date}'
    and s.MAINT_TYPE1<>'删除'
    group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,bn,primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.53(轮胎+蓄电池_二级_车龄)
def kpi_b_agetype_kpi_3_53(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_agetype partition(mon_p='${mon_p}',kpi='kpi_3_53')
  select  primary_classification,second_level_classification, age_type,
   substr(tran_date(s.order_balance_date),1,7) as mon,
   count(distinct case when t.asc_code is not null then s.vin else null end)/count(distinct s.vin) as num,
   s.asc_code
   from label_order s 
   left join (
    select distinct t.asc_code,t.order_number from part t ,high_flow_parts x
   where  t.part_number=x.part_num  and x.type in('DianC','LunT')
     ) t 
    on  s.order_number=t.order_number
    and s.asc_code=t.asc_code
    where substr(tran_date(s.order_balance_date),1,7)>='${begin_date}'
    and substr(tran_date(s.order_balance_date),1,7)<='${end_date}'
    and s.MAINT_TYPE1<>'删除'
    group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,age_type,primary_classification,second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.53(轮胎+蓄电池_二级_保内外)
def kpi_b_bnbw_kpi_3_53(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_bnbw partition(mon_p='${mon_p}',kpi='kpi_3_53')
  select  primary_classification,second_level_classification, bn,
   substr(tran_date(s.order_balance_date),1,7) as mon,
   count(distinct case when t.asc_code is not null then s.vin else null end)/count(distinct s.vin) as num,
   s.asc_code
   from label_order s 
   left join (
    select distinct t.asc_code,t.order_number from part t ,high_flow_parts x
   where  t.part_number=x.part_num  and x.type in('DianC','LunT')
     ) t 
    on  s.order_number=t.order_number
    and s.asc_code=t.asc_code
    where substr(tran_date(s.order_balance_date),1,7)>='${begin_date}'
    and substr(tran_date(s.order_balance_date),1,7)<='${end_date}'
    and s.MAINT_TYPE1<>'删除'
    group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,bn,primary_classification,second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.54(其他高流失件_total)
def kpi_total_kpi_3_54(begin_day, end_day):
    line = """  insert overwrite table  kpi_total partition(mon_p='${mon_p}',kpi='kpi_3_54')
   select  
   substr(tran_date(s.order_balance_date),1,7) as mon,
   count(distinct case when t.asc_code is not null then s.vin else null end)/count(distinct s.vin) as num,
   s.asc_code
   from label_order s 
   left join (
    select distinct t.asc_code,t.order_number from part t ,high_flow_parts x
   where  t.part_number=x.part_num  and x.type='QiT'
     ) t 
    on  s.order_number=t.order_number
    and s.asc_code=t.asc_code
    where substr(tran_date(s.order_balance_date),1,7)>='${begin_date}'
    and substr(tran_date(s.order_balance_date),1,7)<='${end_date}'
    and s.MAINT_TYPE1<>'删除'
    group by substr(tran_date(s.order_balance_date),1,7),s.asc_code"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.54(其他高流失件_一级)
def kpi_a_level_kpi_3_54(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_level partition(mon_p='${mon_p}',kpi='kpi_3_54')
     select  primary_classification,
   substr(tran_date(s.order_balance_date),1,7) as mon,
   count(distinct case when t.asc_code is not null then s.vin else null end)/count(distinct s.vin) as num,
   s.asc_code
   from label_order s 
   left join (
    select distinct t.asc_code,t.order_number from part t ,high_flow_parts x
   where  t.part_number=x.part_num  and x.type='QiT'
     ) t 
    on  s.order_number=t.order_number
    and s.asc_code=t.asc_code
    where substr(tran_date(s.order_balance_date),1,7)>='${begin_date}'
    and substr(tran_date(s.order_balance_date),1,7)<='${end_date}'
    and s.MAINT_TYPE1<>'删除'
    group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.54(其他高流失件_二级)
def kpi_b_level_kpi_3_54(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_level partition(mon_p='${mon_p}',kpi='kpi_3_54')
     select  primary_classification,second_level_classification,
   substr(tran_date(s.order_balance_date),1,7) as mon,
   count(distinct case when t.asc_code is not null then s.vin else null end)/count(distinct s.vin) as num,
   s.asc_code
   from label_order s 
   left join (
    select distinct t.asc_code,t.order_number from part t ,high_flow_parts x
   where  t.part_number=x.part_num  and x.type='QiT'
     ) t 
    on  s.order_number=t.order_number
    and s.asc_code=t.asc_code
    where substr(tran_date(s.order_balance_date),1,7)>='${begin_date}'
    and substr(tran_date(s.order_balance_date),1,7)<='${end_date}'
    and s.MAINT_TYPE1<>'删除'
    group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,primary_classification,second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.54(其他高流失件_车龄)
def kpi_agetype_kpi_3_54(begin_day, end_day):
    line = """  insert overwrite table  kpi_agetype partition(mon_p='${mon_p}',kpi='kpi_3_54')
   select  age_type,
   substr(tran_date(s.order_balance_date),1,7) as mon,
   count(distinct case when t.asc_code is not null then s.vin else null end)/count(distinct s.vin) as num,
   s.asc_code
   from label_order s 
   left join (
    select distinct t.asc_code,t.order_number from part t ,high_flow_parts x
   where  t.part_number=x.part_num  and x.type='QiT'
     ) t 
    on  s.order_number=t.order_number
    and s.asc_code=t.asc_code
    where substr(tran_date(s.order_balance_date),1,7)>='${begin_date}'
    and substr(tran_date(s.order_balance_date),1,7)<='${end_date}'
    and s.MAINT_TYPE1<>'删除'
    group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,age_type"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.54(其他高流失件_保内外)
def kpi_bnbw_kpi_3_54(begin_day, end_day):
    line = """  insert overwrite table  kpi_bnbw partition(mon_p='${mon_p}',kpi='kpi_3_54')
  select  bn,
   substr(tran_date(s.order_balance_date),1,7) as mon,
   count(distinct case when t.asc_code is not null then s.vin else null end)/count(distinct s.vin) as num,
   s.asc_code
   from label_order s 
   left join (
    select distinct t.asc_code,t.order_number from part t ,high_flow_parts x
   where  t.part_number=x.part_num  and x.type='QiT'
     ) t 
    on  s.order_number=t.order_number
    and s.asc_code=t.asc_code
    where substr(tran_date(s.order_balance_date),1,7)>='${begin_date}'
    and substr(tran_date(s.order_balance_date),1,7)<='${end_date}'
    and s.MAINT_TYPE1<>'删除'
    group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,bn"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.54(其他高流失件_一级_车龄)
def kpi_a_agetype_kpi_3_54(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_agetype partition(mon_p='${mon_p}',kpi='kpi_3_54')
  select  primary_classification, age_type,
   substr(tran_date(s.order_balance_date),1,7) as mon,
   count(distinct case when t.asc_code is not null then s.vin else null end)/count(distinct s.vin) as num,
   s.asc_code
   from label_order s 
   left join (
    select distinct t.asc_code,t.order_number from part t ,high_flow_parts x
   where  t.part_number=x.part_num  and x.type='QiT'
     ) t 
    on  s.order_number=t.order_number
    and s.asc_code=t.asc_code
    where substr(tran_date(s.order_balance_date),1,7)>='${begin_date}'
    and substr(tran_date(s.order_balance_date),1,7)<='${end_date}'
    and s.MAINT_TYPE1<>'删除'
    group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,age_type,primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.54(其他高流失件_一级_保内外)
def kpi_a_bnbw_kpi_3_54(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_bnbw partition(mon_p='${mon_p}',kpi='kpi_3_54')
  select  primary_classification, bn,
   substr(tran_date(s.order_balance_date),1,7) as mon,
   count(distinct case when t.asc_code is not null then s.vin else null end)/count(distinct s.vin) as num,
   s.asc_code
   from label_order s 
   left join (
    select distinct t.asc_code,t.order_number from part t ,high_flow_parts x
   where  t.part_number=x.part_num  and x.type='QiT'
     ) t 
    on  s.order_number=t.order_number
    and s.asc_code=t.asc_code
    where substr(tran_date(s.order_balance_date),1,7)>='${begin_date}'
    and substr(tran_date(s.order_balance_date),1,7)<='${end_date}'
    and s.MAINT_TYPE1<>'删除'
    group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,bn,primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.54(其他高流失件_二级_车龄)
def kpi_b_agetype_kpi_3_54(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_agetype partition(mon_p='${mon_p}',kpi='kpi_3_54')
  select  primary_classification,second_level_classification, age_type,
   substr(tran_date(s.order_balance_date),1,7) as mon,
   count(distinct case when t.asc_code is not null then s.vin else null end)/count(distinct s.vin) as num,
   s.asc_code
   from label_order s 
   left join (
    select distinct t.asc_code,t.order_number from part t ,high_flow_parts x
   where  t.part_number=x.part_num  and x.type='QiT'
     ) t 
    on  s.order_number=t.order_number
    and s.asc_code=t.asc_code
    where substr(tran_date(s.order_balance_date),1,7)>='${begin_date}'
    and substr(tran_date(s.order_balance_date),1,7)<='${end_date}'
    and s.MAINT_TYPE1<>'删除'
    group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,age_type,primary_classification,second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.54(其他高流失件_二级_保内外)
def kpi_b_bnbw_kpi_3_54(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_bnbw partition(mon_p='${mon_p}',kpi='kpi_3_54')
  select  primary_classification,second_level_classification, bn,
   substr(tran_date(s.order_balance_date),1,7) as mon,
   count(distinct case when t.asc_code is not null then s.vin else null end)/count(distinct s.vin) as num,
   s.asc_code
   from label_order s 
   left join (
    select distinct t.asc_code,t.order_number from part t ,high_flow_parts x
   where  t.part_number=x.part_num  and x.type='QiT'
     ) t 
    on  s.order_number=t.order_number
    and s.asc_code=t.asc_code
    where substr(tran_date(s.order_balance_date),1,7)>='${begin_date}'
    and substr(tran_date(s.order_balance_date),1,7)<='${end_date}'
    and s.MAINT_TYPE1<>'删除'
    group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,bn,primary_classification,second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.6(客户流失率_total)
def kpi_total_kpi_3_6(begin_day, end_day):
    line = """  insert overwrite table  kpi_total partition(mon_p='${mon_p}',kpi='kpi_3_6')
   select s.mon,
   (s.num-t.num)/s.num as num,
    s.asc_code
   from 
   (select * from kpi_total where kpi='kpi_2_1' and mon>='${begin_date}'
    and mon<='${end_date}') s 
   left join 
   (select * from kpi_total where kpi='kpi_2_2' and mon>='${begin_date}'
    and mon<='${end_date}') t
   on s.mon=t.mon and s.asc_code=t.asc_code"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.6(客户流失率_一级)
def kpi_a_level_kpi_3_6(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_level partition(mon_p='${mon_p}',kpi='kpi_3_6')
     select 
     s.a_level,
     s.mon,
   (s.num-t.num)/s.num as num,
    s.asc_code
   from 
   (select * from kpi_a_level where kpi='kpi_2_1' and mon>='${begin_date}'
    and mon<='${end_date}') s 
   left join 
   (select * from kpi_a_level where kpi='kpi_2_2' and mon>='${begin_date}'
    and mon<='${end_date}') t
   on s.mon=t.mon and s.asc_code=t.asc_code and s.a_level=t.a_level"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.6(客户流失率_二级)
def kpi_b_level_kpi_3_6(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_level partition(mon_p='${mon_p}',kpi='kpi_3_6')
     select 
     s.a_level,
     s.b_level,
     s.mon,
   (s.num-t.num)/s.num as num,
    s.asc_code
   from 
   (select * from kpi_b_level where kpi='kpi_2_1' and mon>='${begin_date}'
    and mon<='${end_date}') s 
   left join 
   (select * from kpi_b_level where kpi='kpi_2_2' and mon>='${begin_date}'
    and mon<='${end_date}') t
   on s.mon=t.mon and s.asc_code=t.asc_code and s.a_level=t.a_level and s.b_level=t.b_level"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.6(客户流失率_车龄)
def kpi_agetype_kpi_3_6(begin_day, end_day):
    line = """  insert overwrite table  kpi_agetype partition(mon_p='${mon_p}',kpi='kpi_3_6')
    select 
     s.age_type,
     s.mon,
   (s.num-t.num)/s.num as num,
    s.asc_code
   from 
   (select * from kpi_agetype where kpi='kpi_2_1' and mon>='${begin_date}'
    and mon<='${end_date}') s 
   left join 
   (select * from kpi_agetype where kpi='kpi_2_2' and mon>='${begin_date}'
    and mon<='${end_date}') t
   on s.mon=t.mon and s.asc_code=t.asc_code and s.age_type=t.age_type"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.6(客户流失率_保内外)
def kpi_bnbw_kpi_3_6(begin_day, end_day):
    line = """  insert overwrite table  kpi_bnbw partition(mon_p='${mon_p}',kpi='kpi_3_6')
  select 
     s.bnbw,
     s.mon,
   (s.num-t.num)/s.num as num,
    s.asc_code
   from 
   (select * from kpi_bnbw where kpi='kpi_2_1' and mon>='${begin_date}'
    and mon<='${end_date}') s 
   left join 
   (select * from kpi_bnbw where kpi='kpi_2_2' and mon>='${begin_date}'
    and mon<='${end_date}') t
   on s.mon=t.mon and s.asc_code=t.asc_code and s.bnbw=t.bnbw"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.6(客户流失率_一级_车龄)
def kpi_a_agetype_kpi_3_6(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_agetype partition(mon_p='${mon_p}',kpi='kpi_3_6')
  select
     s.a_level, 
     s.age_type,
     s.mon,
   (s.num-t.num)/s.num as num,
    s.asc_code
   from 
   (select * from kpi_a_agetype where kpi='kpi_2_1' and mon>='${begin_date}'
    and mon<='${end_date}') s 
   left join 
   (select * from kpi_a_agetype where kpi='kpi_2_2' and mon>='${begin_date}'
    and mon<='${end_date}') t
   on s.mon=t.mon and s.asc_code=t.asc_code and s.age_type=t.age_type and s.a_level=t.a_level"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.6(客户流失率_一级_保内外)
def kpi_a_bnbw_kpi_3_6(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_bnbw partition(mon_p='${mon_p}',kpi='kpi_3_6')
  select
     s.a_level, 
     s.bnbw,
     s.mon,
   (s.num-t.num)/s.num as num,
    s.asc_code
   from 
   (select * from kpi_a_bnbw where kpi='kpi_2_1' and mon>='${begin_date}'
    and mon<='${end_date}') s 
   left join 
   (select * from kpi_a_bnbw where kpi='kpi_2_2' and mon>='${begin_date}'
    and mon<='${end_date}') t
   on s.mon=t.mon and s.asc_code=t.asc_code and s.bnbw=t.bnbw and s.a_level=t.a_level"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.6(客户流失率_二级_车龄)
def kpi_b_agetype_kpi_3_6(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_agetype partition(mon_p='${mon_p}',kpi='kpi_3_6')
  select
     s.a_level,
     s.b_level,
     s.age_type,
     s.mon,
   (s.num-t.num)/s.num as num,
    s.asc_code
   from 
   (select * from kpi_b_agetype where kpi='kpi_2_1' and mon>='${begin_date}'
    and mon<='${end_date}') s 
   left join 
   (select * from kpi_b_agetype where kpi='kpi_2_2' and mon>='${begin_date}'
    and mon<='${end_date}') t
   on s.mon=t.mon and s.asc_code=t.asc_code and s.age_type=t.age_type and s.a_level=t.a_level and s.b_level=t.b_level"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.6(客户流失率_二级_保内外)
def kpi_b_bnbw_kpi_3_6(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_bnbw partition(mon_p='${mon_p}',kpi='kpi_3_6')
  select
     s.a_level,
     s.b_level,
     s.bnbw,
     s.mon,
   (s.num-t.num)/s.num as num,
    s.asc_code
   from 
   (select * from kpi_b_bnbw where kpi='kpi_2_1' and mon>='${begin_date}'
    and mon<='${end_date}') s 
   left join 
   (select * from kpi_b_bnbw where kpi='kpi_2_2' and mon>='${begin_date}'
    and mon<='${end_date}') t
   on s.mon=t.mon and s.asc_code=t.asc_code and s.bnbw=t.bnbw and s.a_level=t.a_level and s.b_level=t.b_level"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.61(一年客户流失率_total)
def kpi_total_kpi_3_61(begin_day, end_day):
    line = """  insert overwrite table  kpi_total partition(mon_p='${mon_p}',kpi='kpi_3_61')
    select s.mon,
    sum(case when t.vin is null then 1 else 0 end)/count(s.vin) as num,
    s.asc_code
    from (
    select distinct s.asc_code,
    substr(t.mon_label,1,7) as mon,
    s.vin
    from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)=substr(add_months(t.mon_label,-12),1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.order_balance_amount>0
    ) s left join 
    (
    select distinct s.asc_code,
    substr(t.mon_label,1,7) as mon,
    s.vin
    from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    ) t on s.asc_code=t.asc_code and s.mon=t.mon and s.vin=t.vin
    group by s.mon,s.asc_code"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.61(一年客户流失率_一级)
def kpi_a_level_kpi_3_61(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_level partition(mon_p='${mon_p}',kpi='kpi_3_61')
   select s.primary_classification,s.mon,
    sum(case when t.vin is null then 1 else 0 end)/count(s.vin) as num,
    s.asc_code
    from (
    select distinct s.asc_code,s.primary_classification,
    substr(t.mon_label,1,7) as mon,
    s.vin
    from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)=substr(add_months(t.mon_label,-12),1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.order_balance_amount>0
    ) s left join 
    (
    select distinct s.asc_code,s.primary_classification,
    substr(t.mon_label,1,7) as mon,
    s.vin
    from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    ) t on s.asc_code=t.asc_code and s.mon=t.mon and s.primary_classification=t.primary_classification and s.vin=t.vin
    group by s.mon,s.asc_code,s.primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.61(一年客户流失率_二级)
def kpi_b_level_kpi_3_61(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_level partition(mon_p='${mon_p}',kpi='kpi_3_61')
   select s.primary_classification,s.second_level_classification,s.mon,
    sum(case when t.vin is null then 1 else 0 end)/count(s.vin) as num,
    s.asc_code
    from (
    select distinct s.asc_code,s.primary_classification,second_level_classification,
    substr(t.mon_label,1,7) as mon,
    s.vin
    from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)=substr(add_months(t.mon_label,-12),1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.order_balance_amount>0
    ) s left join 
    (
    select distinct s.asc_code,s.primary_classification,second_level_classification,
    substr(t.mon_label,1,7) as mon,
    s.vin
    from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    ) t on s.asc_code=t.asc_code and s.mon=t.mon 
    and s.primary_classification=t.primary_classification
    and s.second_level_classification=t.second_level_classification and s.vin=t.vin
    group by s.mon,s.asc_code,s.primary_classification,s.second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.61(一年客户流失率_车龄)
def kpi_agetype_kpi_3_61(begin_day, end_day):
    line = """  insert overwrite table  kpi_agetype partition(mon_p='${mon_p}',kpi='kpi_3_61')
    select s.age_type,s.mon,
    sum(case when t.vin is null then 1 else 0 end)/count(s.vin) as num,
    s.asc_code
    from (
    select distinct s.asc_code,case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end as age_type,
    substr(t.mon_label,1,7) as mon,
    s.vin
    from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)=substr(add_months(t.mon_label,-12),1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.order_balance_amount>0
    ) s left join 
    (
    select distinct s.asc_code,case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end as age_type,
    substr(t.mon_label,1,7) as mon,
    s.vin
    from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    ) t on s.asc_code=t.asc_code and s.mon=t.mon and s.age_type=t.age_type and s.vin=t.vin
     group by s.mon,s.asc_code,s.age_type"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.61(一年客户流失率_保内外)
def kpi_bnbw_kpi_3_61(begin_day, end_day):
    line = """  insert overwrite table  kpi_bnbw partition(mon_p='${mon_p}',kpi='kpi_3_61')
  select s.bn,s.mon,
    sum(case when t.vin is null then 1 else 0 end)/count(s.vin) as num,
    s.asc_code
    from (
    select distinct s.asc_code,case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end as bn,
    substr(t.mon_label,1,7) as mon,
    s.vin
    from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)=substr(add_months(t.mon_label,-12),1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.order_balance_amount>0
    ) s left join 
    (
    select distinct s.asc_code,case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end as bn,
    substr(t.mon_label,1,7) as mon,
    s.vin
    from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    ) t on s.asc_code=t.asc_code and s.mon=t.mon and s.bn=t.bn and s.vin=t.vin
    group by s.mon,s.asc_code,s.bn"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.61(一年客户流失率_一级_车龄)
def kpi_a_agetype_kpi_3_61(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_agetype partition(mon_p='${mon_p}',kpi='kpi_3_61')
  select s.primary_classification,s.age_type,s.mon,
    sum(case when t.vin is null then 1 else 0 end)/count(s.vin) as num,
    s.asc_code
    from (
    select distinct s.asc_code,primary_classification,
    case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end as age_type,
    substr(t.mon_label,1,7) as mon,
    s.vin
    from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)=substr(add_months(t.mon_label,-12),1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.order_balance_amount>0
    ) s left join 
    (
    select distinct s.asc_code,primary_classification,
    case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end as age_type,
    substr(t.mon_label,1,7) as mon,
    s.vin
    from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    ) t on s.asc_code=t.asc_code and s.mon=t.mon and s.age_type=t.age_type 
    and s.primary_classification=t.primary_classification and s.vin=t.vin
    group by s.primary_classification,s.age_type,s.mon,s.asc_code"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.61(一年客户流失率_一级_保内外)
def kpi_a_bnbw_kpi_3_61(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_bnbw partition(mon_p='${mon_p}',kpi='kpi_3_61')
  select s.primary_classification,s.bn,s.mon,
    sum(case when t.vin is null then 1 else 0 end)/count(s.vin) as num,
    s.asc_code
    from (
    select distinct s.asc_code,primary_classification,
    case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end as bn,
    substr(t.mon_label,1,7) as mon,
    s.vin
    from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)=substr(add_months(t.mon_label,-12),1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.order_balance_amount>0
    ) s left join 
    (
    select distinct s.asc_code,primary_classification,
    case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end as bn,
    substr(t.mon_label,1,7) as mon,
    s.vin
    from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    ) t on s.asc_code=t.asc_code and s.mon=t.mon and s.bn=t.bn
    and s.primary_classification=t.primary_classification and s.vin=t.vin
    group by s.primary_classification,s.bn,s.mon,s.asc_code"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.61(一年客户流失率_二级_车龄)
def kpi_b_agetype_kpi_3_61(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_agetype partition(mon_p='${mon_p}',kpi='kpi_3_61')
  select s.primary_classification,s.second_level_classification,s.age_type,s.mon,
    sum(case when t.vin is null then 1 else 0 end)/count(s.vin) as num,
    s.asc_code
    from (
    select distinct s.asc_code,primary_classification,second_level_classification,
    case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end as age_type,
    substr(t.mon_label,1,7) as mon,
    s.vin
    from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)=substr(add_months(t.mon_label,-12),1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.order_balance_amount>0
    ) s left join 
    (
    select distinct s.asc_code,primary_classification,second_level_classification,
    case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end as age_type,
    substr(t.mon_label,1,7) as mon,
    s.vin
    from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    ) t on s.asc_code=t.asc_code and s.mon=t.mon and s.age_type=t.age_type 
    and s.primary_classification=t.primary_classification
    and s.second_level_classification=t.second_level_classification and s.vin=t.vin
    group by s.primary_classification,
    s.second_level_classification,s.age_type,s.mon,s.asc_code"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.61(一年客户流失率_二级_保内外)
def kpi_b_bnbw_kpi_3_61(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_bnbw partition(mon_p='${mon_p}',kpi='kpi_3_61')
  select s.primary_classification,s.second_level_classification,s.bn,s.mon,
    sum(case when t.vin is null then 1 else 0 end)/count(s.vin) as num,
    s.asc_code
    from (
    select distinct s.asc_code,primary_classification,second_level_classification,
    case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end as bn,
    substr(t.mon_label,1,7) as mon,
    s.vin
    from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)=substr(add_months(t.mon_label,-12),1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.order_balance_amount>0
    ) s left join 
    (
    select distinct s.asc_code,primary_classification,second_level_classification,
    case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end as bn,
    substr(t.mon_label,1,7) as mon,
    s.vin
    from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    ) t on s.asc_code=t.asc_code and s.mon=t.mon and s.bn=t.bn
    and s.primary_classification=t.primary_classification
    and s.second_level_classification=t.second_level_classification and s.vin=t.vin
    group by s.primary_classification,
    s.second_level_classification,s.bn,s.mon,s.asc_code"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.62(濒临流失客户率_total)
def kpi_total_kpi_3_62(begin_day, end_day):
    line = """  insert overwrite table  kpi_total partition(mon_p='${mon_p}',kpi='kpi_3_62')
    select s.mon,
    sum(case when t.vin is null then 1 else 0 end)/count(s.vin) as num,
    s.asc_code
    from (
    select distinct s.asc_code,
    substr(t.mon_label,1,7) as mon,
    s.vin
    from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)=substr(add_months(t.mon_label,-6),1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.order_balance_amount>0
    ) s left join 
    (
    select distinct s.asc_code,
    substr(t.mon_label,1,7) as mon,
    s.vin
    from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-5),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    ) t on s.asc_code=t.asc_code and s.mon=t.mon and s.vin=t.vin
    group by s.mon,s.asc_code"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.62(濒临流失客户率_一级)
def kpi_a_level_kpi_3_62(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_level partition(mon_p='${mon_p}',kpi='kpi_3_62')
   select s.primary_classification,s.mon,
    sum(case when t.vin is null then 1 else 0 end)/count(s.vin) as num,
    s.asc_code
    from (
    select distinct s.asc_code,s.primary_classification,
    substr(t.mon_label,1,7) as mon,
    s.vin
    from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)=substr(add_months(t.mon_label,-6),1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.order_balance_amount>0
    ) s left join 
    (
    select distinct s.asc_code,s.primary_classification,
    substr(t.mon_label,1,7) as mon,
    s.vin
    from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-5),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    ) t on s.asc_code=t.asc_code and s.mon=t.mon 
    and s.primary_classification=t.primary_classification and s.vin=t.vin
    group by s.primary_classification,s.mon,s.asc_code"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.62(濒临流失客户率_二级)
def kpi_b_level_kpi_3_62(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_level partition(mon_p='${mon_p}',kpi='kpi_3_62')
   select s.primary_classification,s.second_level_classification,s.mon,
    sum(case when t.vin is null then 1 else 0 end)/count(s.vin) as num,
    s.asc_code
    from (
    select distinct s.asc_code,s.primary_classification,second_level_classification,
    substr(t.mon_label,1,7) as mon,
    s.vin
    from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)=substr(add_months(t.mon_label,-6),1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.order_balance_amount>0
    ) s left join 
    (
    select distinct s.asc_code,s.primary_classification,second_level_classification,
    substr(t.mon_label,1,7) as mon,
    s.vin
    from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-5),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    ) t on s.asc_code=t.asc_code and s.mon=t.mon and s.primary_classification=t.primary_classification
    and s.second_level_classification=t.second_level_classification and s.vin=t.vin
    group by s.primary_classification,s.second_level_classification,s.mon,s.asc_code"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.62(濒临流失客户率_车龄)
def kpi_agetype_kpi_3_62(begin_day, end_day):
    line = """  insert overwrite table  kpi_agetype partition(mon_p='${mon_p}',kpi='kpi_3_62')
    select s.age_type,s.mon,
    sum(case when t.vin is null then 1 else 0 end)/count(s.vin) as num,
    s.asc_code
    from (
    select distinct s.asc_code,case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end as age_type,
    substr(t.mon_label,1,7) as mon,
    s.vin
    from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)=substr(add_months(t.mon_label,-6),1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.order_balance_amount>0
    ) s left join 
    (
    select distinct s.asc_code,case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end as age_type,
    substr(t.mon_label,1,7) as mon,
    s.vin
    from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-5),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    ) t on s.asc_code=t.asc_code and s.mon=t.mon and s.age_type=t.age_type and s.vin=t.vin
    group by s.age_type,s.mon,s.asc_code"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.62(濒临流失客户率_保内外)
def kpi_bnbw_kpi_3_62(begin_day, end_day):
    line = """  insert overwrite table  kpi_bnbw partition(mon_p='${mon_p}',kpi='kpi_3_62')
  select s.bn,s.mon,
    sum(case when t.vin is null then 1 else 0 end)/count(s.vin) as num,
    s.asc_code
    from (
    select distinct s.asc_code,case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end as bn,
    substr(t.mon_label,1,7) as mon,
    s.vin
    from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)=substr(add_months(t.mon_label,-6),1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.order_balance_amount>0
    ) s left join 
    (
    select distinct s.asc_code,case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end as bn,
    substr(t.mon_label,1,7) as mon,
    s.vin
    from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-5),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    ) t on s.asc_code=t.asc_code and s.mon=t.mon and s.bn=t.bn and s.vin=t.vin
    group by s.bn,s.mon,s.asc_code"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.62(濒临流失客户率_一级_车龄)
def kpi_a_agetype_kpi_3_62(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_agetype partition(mon_p='${mon_p}',kpi='kpi_3_62')
  select s.primary_classification,s.age_type,s.mon,
    sum(case when t.vin is null then 1 else 0 end)/count(s.vin) as num,
    s.asc_code
    from (
    select distinct s.asc_code,primary_classification,
    case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end as age_type,
    substr(t.mon_label,1,7) as mon,
    s.vin
    from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)=substr(add_months(t.mon_label,-6),1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.order_balance_amount>0
    ) s left join 
    (
    select distinct s.asc_code,primary_classification,
    case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end as age_type,
    substr(t.mon_label,1,7) as mon,
    s.vin
    from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-5),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    ) t on s.asc_code=t.asc_code and s.mon=t.mon and s.age_type=t.age_type 
    and s.primary_classification=t.primary_classification  and s.vin=t.vin
    group by s.primary_classification,s.age_type,s.mon,s.asc_code"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.62(濒临流失客户率_一级_保内外)
def kpi_a_bnbw_kpi_3_62(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_bnbw partition(mon_p='${mon_p}',kpi='kpi_3_62')
  select s.primary_classification,s.bn,s.mon,
    sum(case when t.vin is null then 1 else 0 end)/count(s.vin) as num,
    s.asc_code
    from (
    select distinct s.asc_code,primary_classification,
    case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end as bn,
    substr(t.mon_label,1,7) as mon,
    s.vin
    from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)=substr(add_months(t.mon_label,-6),1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.order_balance_amount>0
    ) s left join 
    (
    select distinct s.asc_code,primary_classification,
    case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end as bn,
    substr(t.mon_label,1,7) as mon,
    s.vin
    from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-5),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    ) t on s.asc_code=t.asc_code and s.mon=t.mon and s.bn=t.bn
    and s.primary_classification=t.primary_classification  and s.vin=t.vin
    group by s.primary_classification,s.bn,s.mon,s.asc_code"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.62(濒临流失客户率_二级_车龄)
def kpi_b_agetype_kpi_3_62(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_agetype partition(mon_p='${mon_p}',kpi='kpi_3_62')
  select s.primary_classification,s.second_level_classification,s.age_type,s.mon,
    sum(case when t.vin is null then 1 else 0 end)/count(s.vin) as num,
    s.asc_code
    from (
    select distinct s.asc_code,primary_classification,second_level_classification,
    case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end as age_type,
    substr(t.mon_label,1,7) as mon,
    s.vin
    from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)=substr(add_months(t.mon_label,-6),1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.order_balance_amount>0
    ) s left join 
    (
    select distinct s.asc_code,primary_classification,second_level_classification,
    case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end as age_type,
    substr(t.mon_label,1,7) as mon,
    s.vin
    from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-5),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    ) t on s.asc_code=t.asc_code and s.mon=t.mon and s.age_type=t.age_type 
    and s.primary_classification=t.primary_classification
    and s.second_level_classification=t.second_level_classification  and s.vin=t.vin
    group by s.primary_classification,
    s.second_level_classification,s.age_type,s.mon,s.asc_code"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_3.62(濒临流失客户率_二级_保内外)
def kpi_b_bnbw_kpi_3_62(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_bnbw partition(mon_p='${mon_p}',kpi='kpi_3_62')
  select s.primary_classification,s.second_level_classification,s.bn,s.mon,
    sum(case when t.vin is null then 1 else 0 end)/count(s.vin) as num,
    s.asc_code
    from (
    select distinct s.asc_code,primary_classification,second_level_classification,
    case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end as bn,
    substr(t.mon_label,1,7) as mon,
    s.vin
    from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)=substr(add_months(t.mon_label,-6),1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.order_balance_amount>0
    ) s left join 
    (
    select distinct s.asc_code,primary_classification,second_level_classification,
    case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end as bn,
    substr(t.mon_label,1,7) as mon,
    s.vin
    from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-5),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    ) t on s.asc_code=t.asc_code and s.mon=t.mon and s.bn=t.bn
    and s.primary_classification=t.primary_classification
    and s.second_level_classification=t.second_level_classification  and s.vin=t.vin
    group by s.primary_classification,s.second_level_classification,s.bn,s.mon,s.asc_code"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_0(售后总产值_total)
def kpi_total_kpi_0(begin_day, end_day):
    line = """  insert overwrite table  kpi_total partition(mon_p='${mon_p}',kpi='kpi_0')
  select substr(tran_date(order_balance_date),1,7) as mon,
  sum(order_balance_amount) as sumamount,
  asc_code
   from  label_order  
  where MAINT_TYPE1<>'删除'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(order_balance_date),1,7),asc_code"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_0(售后总产值_一级)
def kpi_a_level_kpi_0(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_level partition(mon_p='${mon_p}',kpi='kpi_0')
   select primary_classification,
   substr(tran_date(order_balance_date),1,7) as mon,
  sum(order_balance_amount) as sumamount,
  asc_code
   from  label_order  
  where MAINT_TYPE1<>'删除'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(order_balance_date),1,7),asc_code,primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_0(售后总产值_二级)
def kpi_b_level_kpi_0(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_level partition(mon_p='${mon_p}',kpi='kpi_0')
   select primary_classification,
   second_level_classification,
   substr(tran_date(order_balance_date),1,7) as mon,
  sum(order_balance_amount) as sumamount,
  asc_code
   from  label_order  
  where MAINT_TYPE1<>'删除'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(order_balance_date),1,7),asc_code,primary_classification,second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_0(售后总产值_车龄)
def kpi_agetype_kpi_0(begin_day, end_day):
    line = """  insert overwrite table  kpi_agetype partition(mon_p='${mon_p}',kpi='kpi_0')
    select age_type,
   substr(tran_date(order_balance_date),1,7) as mon,
  sum(order_balance_amount) as sumamount,
  asc_code
   from  label_order  
  where MAINT_TYPE1<>'删除'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(order_balance_date),1,7),asc_code,age_type"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_0(售后总产值_保内外)
def kpi_bnbw_kpi_0(begin_day, end_day):
    line = """  insert overwrite table  kpi_bnbw partition(mon_p='${mon_p}',kpi='kpi_0')
  select bn,
   substr(tran_date(order_balance_date),1,7) as mon,
  sum(order_balance_amount) as sumamount,
  asc_code
   from  label_order  
  where MAINT_TYPE1<>'删除'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(order_balance_date),1,7),asc_code,bn"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_0(售后总产值_一级_车龄)
def kpi_a_agetype_kpi_0(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_agetype partition(mon_p='${mon_p}',kpi='kpi_0')
  select primary_classification,
   age_type,
   substr(tran_date(order_balance_date),1,7) as mon,
  sum(order_balance_amount) as sumamount,
  asc_code
   from  label_order  
  where MAINT_TYPE1<>'删除'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(order_balance_date),1,7),asc_code,age_type,primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_0(售后总产值_一级_保内外)
def kpi_a_bnbw_kpi_0(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_bnbw partition(mon_p='${mon_p}',kpi='kpi_0')
  select primary_classification,
   bn,
   substr(tran_date(order_balance_date),1,7) as mon,
  sum(order_balance_amount) as sumamount,
  asc_code
   from  label_order  
  where MAINT_TYPE1<>'删除'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(order_balance_date),1,7),asc_code,bn,primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_0(售后总产值_二级_车龄)
def kpi_b_agetype_kpi_0(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_agetype partition(mon_p='${mon_p}',kpi='kpi_0')
  select primary_classification,
   second_level_classification,
   age_type,
   substr(tran_date(order_balance_date),1,7) as mon,
  sum(order_balance_amount) as sumamount,
  asc_code
   from  label_order  
  where MAINT_TYPE1<>'删除'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(order_balance_date),1,7),asc_code,age_type,
  primary_classification,second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_0(售后总产值_二级_保内外)
def kpi_b_bnbw_kpi_0(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_bnbw partition(mon_p='${mon_p}',kpi='kpi_0')
  select primary_classification,
   second_level_classification,
   bn,
   substr(tran_date(order_balance_date),1,7) as mon,
  sum(order_balance_amount) as sumamount,
  asc_code
   from  label_order  
  where MAINT_TYPE1<>'删除'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(order_balance_date),1,7),asc_code,bn,
  primary_classification,second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_0.1(售后配件总产值_total)
def kpi_total_kpi_0_1(begin_day, end_day):
    line = """  insert overwrite table  kpi_total partition(mon_p='${mon_p}',kpi='kpi_0_1')
  select substr(tran_date(s.order_balance_date),1,7) as mon,
  sum(t.sales_amount) as sumamount,
  s.asc_code
   from  label_order s,part t 
  where s.MAINT_TYPE1<>'删除'
  and s.asc_code=t.asc_code
  and s.order_number=t.order_number
  and substr(tran_date(s.order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(s.order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_0.1(售后配件总产值_一级)
def kpi_a_level_kpi_0_1(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_level partition(mon_p='${mon_p}',kpi='kpi_0_1')
  select s.primary_classification,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  sum(t.sales_amount) as sumamount,
  s.asc_code
   from  label_order s,part t 
  where s.MAINT_TYPE1<>'删除'
  and s.asc_code=t.asc_code
  and s.order_number=t.order_number
  and substr(tran_date(s.order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(s.order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,s.primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_0.1(售后配件总产值_二级)
def kpi_b_level_kpi_0_1(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_level partition(mon_p='${mon_p}',kpi='kpi_0_1')
  select s.primary_classification,
  s.second_level_classification,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  sum(t.sales_amount) as sumamount,
  s.asc_code
   from  label_order s,part t 
  where s.MAINT_TYPE1<>'删除'
  and s.asc_code=t.asc_code
  and s.order_number=t.order_number
  and substr(tran_date(s.order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(s.order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,s.primary_classification,s.second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_0.1(售后配件总产值_车龄)
def kpi_agetype_kpi_0_1(begin_day, end_day):
    line = """  insert overwrite table  kpi_agetype partition(mon_p='${mon_p}',kpi='kpi_0_1')
  select s.age_type,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  sum(t.sales_amount) as sumamount,
  s.asc_code
   from  label_order s,part t 
  where s.MAINT_TYPE1<>'删除'
  and s.asc_code=t.asc_code
  and s.order_number=t.order_number
  and substr(tran_date(s.order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(s.order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,s.age_type"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_0.1(售后配件总产值_保内外)
def kpi_bnbw_kpi_0_1(begin_day, end_day):
    line = """  insert overwrite table  kpi_bnbw partition(mon_p='${mon_p}',kpi='kpi_0_1')
  select s.bn,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  sum(t.sales_amount) as sumamount,
  s.asc_code
   from  label_order s,part t 
  where s.MAINT_TYPE1<>'删除'
  and s.asc_code=t.asc_code
  and s.order_number=t.order_number
  and substr(tran_date(s.order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(s.order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,s.bn"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_0.1(售后配件总产值_一级_车龄)
def kpi_a_agetype_kpi_0_1(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_agetype partition(mon_p='${mon_p}',kpi='kpi_0_1')
  select s.primary_classification,
  s.age_type,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  sum(t.sales_amount) as sumamount,
  s.asc_code
   from  label_order s,part t 
  where s.MAINT_TYPE1<>'删除'
  and s.asc_code=t.asc_code
  and s.order_number=t.order_number
  and substr(tran_date(s.order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(s.order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,s.age_type,s.primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_0.1(售后配件总产值_一级_保内外)
def kpi_a_bnbw_kpi_0_1(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_bnbw partition(mon_p='${mon_p}',kpi='kpi_0_1')
  select s.primary_classification,
  s.bn,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  sum(t.sales_amount) as sumamount,
  s.asc_code
   from  label_order s,part t 
  where s.MAINT_TYPE1<>'删除'
  and s.asc_code=t.asc_code
  and s.order_number=t.order_number
  and substr(tran_date(s.order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(s.order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,s.bn,s.primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_0.1(售后配件总产值_二级_车龄)
def kpi_b_agetype_kpi_0_1(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_agetype partition(mon_p='${mon_p}',kpi='kpi_0_1')
  select s.primary_classification,
  s.second_level_classification,
  s.age_type,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  sum(t.sales_amount) as sumamount,
  s.asc_code
   from  label_order s,part t 
  where s.MAINT_TYPE1<>'删除'
  and s.asc_code=t.asc_code
  and s.order_number=t.order_number
  and substr(tran_date(s.order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(s.order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,s.age_type,s.primary_classification,
  s.second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_0.1(售后配件总产值_二级_保内外)
def kpi_b_bnbw_kpi_0_1(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_bnbw partition(mon_p='${mon_p}',kpi='kpi_0_1')
  select s.primary_classification,
  s.second_level_classification,
  s.bn,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  sum(t.sales_amount) as sumamount,
  s.asc_code
   from  label_order s,part t 
  where s.MAINT_TYPE1<>'删除'
  and s.asc_code=t.asc_code
  and s.order_number=t.order_number
  and substr(tran_date(s.order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(s.order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,s.bn,s.primary_classification,
  s.second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_0.2(保养总产值_total)
def kpi_total_kpi_0_2(begin_day, end_day):
    line = """  insert overwrite table  kpi_total partition(mon_p='${mon_p}',kpi='kpi_0_2')
  select substr(tran_date(order_balance_date),1,7) as mon,
  sum(order_balance_amount) as sumamount,
  asc_code
   from  label_order  
  where MAINT_TYPE1='保养'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(order_balance_date),1,7),asc_code"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_0.2(保养总产值_一级)
def kpi_a_level_kpi_0_2(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_level partition(mon_p='${mon_p}',kpi='kpi_0_2')
   select primary_classification,
   substr(tran_date(order_balance_date),1,7) as mon,
  sum(order_balance_amount) as sumamount,
  asc_code
   from  label_order  
  where MAINT_TYPE1='保养'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(order_balance_date),1,7),asc_code,primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_0.2(保养总产值_二级)
def kpi_b_level_kpi_0_2(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_level partition(mon_p='${mon_p}',kpi='kpi_0_2')
   select primary_classification,
   second_level_classification,
   substr(tran_date(order_balance_date),1,7) as mon,
  sum(order_balance_amount) as sumamount,
  asc_code
   from  label_order  
  where MAINT_TYPE1='保养'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(order_balance_date),1,7),asc_code,primary_classification,second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_0.2(保养总产值_车龄)
def kpi_agetype_kpi_0_2(begin_day, end_day):
    line = """  insert overwrite table  kpi_agetype partition(mon_p='${mon_p}',kpi='kpi_0_2')
    select age_type,
   substr(tran_date(order_balance_date),1,7) as mon,
  sum(order_balance_amount) as sumamount,
  asc_code
   from  label_order  
  where MAINT_TYPE1='保养'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(order_balance_date),1,7),asc_code,age_type"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_0.2(保养总产值_保内外)
def kpi_bnbw_kpi_0_2(begin_day, end_day):
    line = """  insert overwrite table  kpi_bnbw partition(mon_p='${mon_p}',kpi='kpi_0_2')
  select bn,
   substr(tran_date(order_balance_date),1,7) as mon,
  sum(order_balance_amount) as sumamount,
  asc_code
   from  label_order  
  where MAINT_TYPE1='保养'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(order_balance_date),1,7),asc_code,bn"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_0.2(保养总产值_一级_车龄)
def kpi_a_agetype_kpi_0_2(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_agetype partition(mon_p='${mon_p}',kpi='kpi_0_2')
  select primary_classification,
   age_type,
   substr(tran_date(order_balance_date),1,7) as mon,
  sum(order_balance_amount) as sumamount,
  asc_code
   from  label_order  
  where MAINT_TYPE1='保养'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(order_balance_date),1,7),asc_code,age_type,primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_0.2(保养总产值_一级_保内外)
def kpi_a_bnbw_kpi_0_2(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_bnbw partition(mon_p='${mon_p}',kpi='kpi_0_2')
  select primary_classification,
   bn,
   substr(tran_date(order_balance_date),1,7) as mon,
  sum(order_balance_amount) as sumamount,
  asc_code
   from  label_order  
  where MAINT_TYPE1='保养'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(order_balance_date),1,7),asc_code,bn,primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_0.2(保养总产值_二级_车龄)
def kpi_b_agetype_kpi_0_2(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_agetype partition(mon_p='${mon_p}',kpi='kpi_0_2')
  select primary_classification,
   second_level_classification,
   age_type,
   substr(tran_date(order_balance_date),1,7) as mon,
  sum(order_balance_amount) as sumamount,
  asc_code
   from  label_order  
  where MAINT_TYPE1='保养'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(order_balance_date),1,7),asc_code,age_type,
  primary_classification,second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_0.2(保养总产值_二级_保内外)
def kpi_b_bnbw_kpi_0_2(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_bnbw partition(mon_p='${mon_p}',kpi='kpi_0_2')
  select primary_classification,
   second_level_classification,
   bn,
   substr(tran_date(order_balance_date),1,7) as mon,
  sum(order_balance_amount) as sumamount,
  asc_code
   from  label_order  
  where MAINT_TYPE1='保养'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(order_balance_date),1,7),asc_code,bn,
  primary_classification,second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_0.3(维修总产值_total)
def kpi_total_kpi_0_3(begin_day, end_day):
    line = """  insert overwrite table  kpi_total partition(mon_p='${mon_p}',kpi='kpi_0_3')
  select substr(tran_date(order_balance_date),1,7) as mon,
  sum(order_balance_amount) as sumamount,
  asc_code
   from  label_order  
  where MAINT_TYPE1='维修'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(order_balance_date),1,7),asc_code"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_0.3(维修总产值_一级)
def kpi_a_level_kpi_0_3(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_level partition(mon_p='${mon_p}',kpi='kpi_0_3')
   select primary_classification,
   substr(tran_date(order_balance_date),1,7) as mon,
  sum(order_balance_amount) as sumamount,
  asc_code
   from  label_order  
  where MAINT_TYPE1='维修'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(order_balance_date),1,7),asc_code,primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_0.3(维修总产值_二级)
def kpi_b_level_kpi_0_3(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_level partition(mon_p='${mon_p}',kpi='kpi_0_3')
   select primary_classification,
   second_level_classification,
   substr(tran_date(order_balance_date),1,7) as mon,
  sum(order_balance_amount) as sumamount,
  asc_code
   from  label_order  
  where MAINT_TYPE1='维修'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(order_balance_date),1,7),asc_code,primary_classification,second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_0.3(维修总产值_车龄)
def kpi_agetype_kpi_0_3(begin_day, end_day):
    line = """  insert overwrite table  kpi_agetype partition(mon_p='${mon_p}',kpi='kpi_0_3')
    select age_type,
   substr(tran_date(order_balance_date),1,7) as mon,
  sum(order_balance_amount) as sumamount,
  asc_code
   from  label_order  
  where MAINT_TYPE1='维修'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(order_balance_date),1,7),asc_code,age_type"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_0.3(维修总产值_保内外)
def kpi_bnbw_kpi_0_3(begin_day, end_day):
    line = """  insert overwrite table  kpi_bnbw partition(mon_p='${mon_p}',kpi='kpi_0_3')
  select bn,
   substr(tran_date(order_balance_date),1,7) as mon,
  sum(order_balance_amount) as sumamount,
  asc_code
   from  label_order  
  where MAINT_TYPE1='维修'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(order_balance_date),1,7),asc_code,bn"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_0.3(维修总产值_一级_车龄)
def kpi_a_agetype_kpi_0_3(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_agetype partition(mon_p='${mon_p}',kpi='kpi_0_3')
  select primary_classification,
   age_type,
   substr(tran_date(order_balance_date),1,7) as mon,
  sum(order_balance_amount) as sumamount,
  asc_code
   from  label_order  
  where MAINT_TYPE1='维修'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(order_balance_date),1,7),asc_code,age_type,primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_0.3(维修总产值_一级_保内外)
def kpi_a_bnbw_kpi_0_3(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_bnbw partition(mon_p='${mon_p}',kpi='kpi_0_3')
  select primary_classification,
   bn,
   substr(tran_date(order_balance_date),1,7) as mon,
  sum(order_balance_amount) as sumamount,
  asc_code
   from  label_order  
  where MAINT_TYPE1='维修'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(order_balance_date),1,7),asc_code,bn,primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_0.3(维修总产值_二级_车龄)
def kpi_b_agetype_kpi_0_3(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_agetype partition(mon_p='${mon_p}',kpi='kpi_0_3')
  select primary_classification,
   second_level_classification,
   age_type,
   substr(tran_date(order_balance_date),1,7) as mon,
  sum(order_balance_amount) as sumamount,
  asc_code
   from  label_order  
  where MAINT_TYPE1='维修'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(order_balance_date),1,7),asc_code,age_type,
  primary_classification,second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_0.3(维修总产值_二级_保内外)
def kpi_b_bnbw_kpi_0_3(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_bnbw partition(mon_p='${mon_p}',kpi='kpi_0_3')
  select primary_classification,
   second_level_classification,
   bn,
   substr(tran_date(order_balance_date),1,7) as mon,
  sum(order_balance_amount) as sumamount,
  asc_code
   from  label_order  
  where MAINT_TYPE1='维修'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(order_balance_date),1,7),asc_code,bn,
  primary_classification,second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_0.4(事故总产值_total)
def kpi_total_kpi_0_4(begin_day, end_day):
    line = """  insert overwrite table  kpi_total partition(mon_p='${mon_p}',kpi='kpi_0_4')
  select substr(tran_date(order_balance_date),1,7) as mon,
  sum(order_balance_amount) as sumamount,
  asc_code
   from  label_order  
  where MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(order_balance_date),1,7),asc_code"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_0.4(事故总产值_一级)
def kpi_a_level_kpi_0_4(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_level partition(mon_p='${mon_p}',kpi='kpi_0_4')
   select primary_classification,
   substr(tran_date(order_balance_date),1,7) as mon,
  sum(order_balance_amount) as sumamount,
  asc_code
   from  label_order  
  where MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(order_balance_date),1,7),asc_code,primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_0.4(事故总产值_二级)
def kpi_b_level_kpi_0_4(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_level partition(mon_p='${mon_p}',kpi='kpi_0_4')
   select primary_classification,
   second_level_classification,
   substr(tran_date(order_balance_date),1,7) as mon,
  sum(order_balance_amount) as sumamount,
  asc_code
   from  label_order  
  where MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(order_balance_date),1,7),asc_code,primary_classification,second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_0.4(事故总产值_车龄)
def kpi_agetype_kpi_0_4(begin_day, end_day):
    line = """  insert overwrite table  kpi_agetype partition(mon_p='${mon_p}',kpi='kpi_0_4')
    select age_type,
   substr(tran_date(order_balance_date),1,7) as mon,
  sum(order_balance_amount) as sumamount,
  asc_code
   from  label_order  
  where MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(order_balance_date),1,7),asc_code,age_type"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_0.4(事故总产值_保内外)
def kpi_bnbw_kpi_0_4(begin_day, end_day):
    line = """  insert overwrite table  kpi_bnbw partition(mon_p='${mon_p}',kpi='kpi_0_4')
  select bn,
   substr(tran_date(order_balance_date),1,7) as mon,
  sum(order_balance_amount) as sumamount,
  asc_code
   from  label_order  
  where MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(order_balance_date),1,7),asc_code,bn"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_0.4(事故总产值_一级_车龄)
def kpi_a_agetype_kpi_0_4(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_agetype partition(mon_p='${mon_p}',kpi='kpi_0_4')
  select primary_classification,
   age_type,
   substr(tran_date(order_balance_date),1,7) as mon,
  sum(order_balance_amount) as sumamount,
  asc_code
   from  label_order  
  where MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(order_balance_date),1,7),asc_code,age_type,primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_0.4(事故总产值_一级_保内外)
def kpi_a_bnbw_kpi_0_4(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_bnbw partition(mon_p='${mon_p}',kpi='kpi_0_4')
  select primary_classification,
   bn,
   substr(tran_date(order_balance_date),1,7) as mon,
  sum(order_balance_amount) as sumamount,
  asc_code
   from  label_order  
  where MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(order_balance_date),1,7),asc_code,bn,primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_0.4(事故总产值_二级_车龄)
def kpi_b_agetype_kpi_0_4(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_agetype partition(mon_p='${mon_p}',kpi='kpi_0_4')
  select primary_classification,
   second_level_classification,
   age_type,
   substr(tran_date(order_balance_date),1,7) as mon,
  sum(order_balance_amount) as sumamount,
  asc_code
   from  label_order  
  where MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(order_balance_date),1,7),asc_code,age_type,
  primary_classification,second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_0.4(事故总产值_二级_保内外)
def kpi_b_bnbw_kpi_0_4(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_bnbw partition(mon_p='${mon_p}',kpi='kpi_0_4')
  select primary_classification,
   second_level_classification,
   bn,
   substr(tran_date(order_balance_date),1,7) as mon,
  sum(order_balance_amount) as sumamount,
  asc_code
   from  label_order  
  where MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(order_balance_date),1,7),asc_code,bn,
  primary_classification,second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_0.5(索赔总产值_total)
def kpi_total_kpi_0_5(begin_day, end_day):
    line = """  insert overwrite table  kpi_total partition(mon_p='${mon_p}',kpi='kpi_0_5')
  select substr(tran_date(s.order_balance_date),1,7) as mon,
  sum(t.claim_accepted_amount) as sumamount,
  s.asc_code
   from  label_order s,claim_uniq t 
  where s.MAINT_TYPE1<>'删除'
  and s.asc_code=t.asc_code
  and s.claim_order_number=t.claim_order_number
  and s.claim='1' 
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_0.5(索赔总产值_一级)
def kpi_a_level_kpi_0_5(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_level partition(mon_p='${mon_p}',kpi='kpi_0_5')
  select s.primary_classification,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  sum(t.claim_accepted_amount) as sumamount,
  s.asc_code
   from  label_order s,claim_uniq t 
  where s.MAINT_TYPE1<>'删除'
  and s.asc_code=t.asc_code
  and s.claim_order_number=t.claim_order_number
  and s.claim='1'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,s.primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_0.5(索赔总产值_二级)
def kpi_b_level_kpi_0_5(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_level partition(mon_p='${mon_p}',kpi='kpi_0_5')
  select s.primary_classification,
  s.second_level_classification,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  sum(t.claim_accepted_amount) as sumamount,
  s.asc_code
   from  label_order s,claim_uniq t 
  where s.MAINT_TYPE1<>'删除'
  and s.asc_code=t.asc_code
  and s.claim_order_number=t.claim_order_number
  and s.claim='1'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,s.primary_classification,s.second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_0.5(索赔总产值_车龄)
def kpi_agetype_kpi_0_5(begin_day, end_day):
    line = """  insert overwrite table  kpi_agetype partition(mon_p='${mon_p}',kpi='kpi_0_5')
  select s.age_type,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  sum(t.claim_accepted_amount) as sumamount,
  s.asc_code
   from  label_order s,claim_uniq t 
  where s.MAINT_TYPE1<>'删除'
  and s.asc_code=t.asc_code
  and s.claim_order_number=t.claim_order_number
  and s.claim='1'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,s.age_type"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_0.5(索赔总产值_保内外)
def kpi_bnbw_kpi_0_5(begin_day, end_day):
    line = """  insert overwrite table  kpi_bnbw partition(mon_p='${mon_p}',kpi='kpi_0_5')
  select s.bn,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  sum(t.claim_accepted_amount) as sumamount,
  s.asc_code
   from  label_order s,claim_uniq t 
  where s.MAINT_TYPE1<>'删除'
  and s.asc_code=t.asc_code
  and s.claim_order_number=t.claim_order_number
  and s.claim='1'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,s.bn"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_0.5(索赔总产值_一级_车龄)
def kpi_a_agetype_kpi_0_5(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_agetype partition(mon_p='${mon_p}',kpi='kpi_0_5')
  select s.primary_classification,
  s.age_type,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  sum(t.claim_accepted_amount) as sumamount,
  s.asc_code
   from  label_order s,claim_uniq t 
  where s.MAINT_TYPE1<>'删除'
  and s.asc_code=t.asc_code
  and s.claim_order_number=t.claim_order_number
  and s.claim='1'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,s.age_type,s.primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_0.5(索赔总产值_一级_保内外)
def kpi_a_bnbw_kpi_0_5(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_bnbw partition(mon_p='${mon_p}',kpi='kpi_0_5')
  select s.primary_classification,
  s.bn,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  sum(t.claim_accepted_amount) as sumamount,
  s.asc_code
   from  label_order s,claim_uniq t 
  where s.MAINT_TYPE1<>'删除'
  and s.asc_code=t.asc_code
  and s.claim_order_number=t.claim_order_number
  and s.claim='1'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,s.bn,s.primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_0.5(索赔总产值_二级_车龄)
def kpi_b_agetype_kpi_0_5(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_agetype partition(mon_p='${mon_p}',kpi='kpi_0_5')
  select s.primary_classification,
  s.second_level_classification,
  s.age_type,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  sum(t.claim_accepted_amount) as sumamount,
  s.asc_code
   from  label_order s,claim_uniq t 
  where s.MAINT_TYPE1<>'删除'
  and s.asc_code=t.asc_code
  and s.claim_order_number=t.claim_order_number
  and s.claim='1'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,s.age_type,s.primary_classification,
  s.second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_0.5(索赔总产值_二级_保内外)
def kpi_b_bnbw_kpi_0_5(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_bnbw partition(mon_p='${mon_p}',kpi='kpi_0_5')
  select s.primary_classification,
  s.second_level_classification,
  s.bn,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  sum(t.claim_accepted_amount) as sumamount,
  s.asc_code
   from  label_order s,claim_uniq t 
  where s.MAINT_TYPE1<>'删除'
  and s.asc_code=t.asc_code
  and s.claim_order_number=t.claim_order_number
  and s.claim='1'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,s.bn,s.primary_classification,
  s.second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4(客单价_total)
def kpi_total_kpi_4(begin_day, end_day):
    line = """  insert overwrite table  kpi_total partition(mon_p='${mon_p}',kpi='kpi_4')
  select substr(tran_date(s.order_balance_date),1,7) as mon,
  sum(order_balance_amount)/count(distinct vin,tran_date(s.order_balance_date)) as num,
  s.asc_code
  from label_order s
  where s.MAINT_TYPE1<>'删除'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  and s.claim=''
  and (s.first_maintnance='' or (s.first_maintnance='1' and s.order_balance_amount<>0))
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4(客单价_一级)
def kpi_a_level_kpi_4(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_level partition(mon_p='${mon_p}',kpi='kpi_4')
  select s.primary_classification,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  sum(order_balance_amount)/count(distinct vin,tran_date(s.order_balance_date)) as num,
  s.asc_code
  from label_order s
  where s.MAINT_TYPE1<>'删除'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  and s.claim=''
  and (s.first_maintnance='' or (s.first_maintnance='1' and s.order_balance_amount<>0))
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,s.primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4(客单价_二级)
def kpi_b_level_kpi_4(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_level partition(mon_p='${mon_p}',kpi='kpi_4')
  select s.primary_classification,
  s.second_level_classification,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  sum(order_balance_amount)/count(distinct vin,tran_date(s.order_balance_date)) as num,
  s.asc_code
  from label_order s
  where s.MAINT_TYPE1<>'删除'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  and s.claim=''
  and (s.first_maintnance='' or (s.first_maintnance='1' and s.order_balance_amount<>0))
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,s.primary_classification,
  s.second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4(客单价_车龄)
def kpi_agetype_kpi_4(begin_day, end_day):
    line = """  insert overwrite table  kpi_agetype partition(mon_p='${mon_p}',kpi='kpi_4')
  select s.age_type,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  sum(order_balance_amount)/count(distinct vin,tran_date(s.order_balance_date)) as num,
  s.asc_code
  from label_order s
  where s.MAINT_TYPE1<>'删除'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  and s.claim=''
  and (s.first_maintnance='' or (s.first_maintnance='1' and s.order_balance_amount<>0))
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,s.age_type"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4(客单价_保内外)
def kpi_bnbw_kpi_4(begin_day, end_day):
    line = """  insert overwrite table  kpi_bnbw partition(mon_p='${mon_p}',kpi='kpi_4')
  select s.bn,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  sum(order_balance_amount)/count(distinct vin,tran_date(s.order_balance_date)) as num,
  s.asc_code
  from label_order s
  where s.MAINT_TYPE1<>'删除'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  and s.claim=''
  and (s.first_maintnance='' or (s.first_maintnance='1' and s.order_balance_amount<>0))
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,s.bn"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4(客单价_一级_车龄)
def kpi_a_agetype_kpi_4(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_agetype partition(mon_p='${mon_p}',kpi='kpi_4')
  select s.primary_classification,
  s.age_type,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  sum(order_balance_amount)/count(distinct vin,tran_date(s.order_balance_date)) as num,
  s.asc_code
  from label_order s
  where s.MAINT_TYPE1<>'删除'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  and s.claim=''
  and (s.first_maintnance='' or (s.first_maintnance='1' and s.order_balance_amount<>0))
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,s.age_type,s.primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4(客单价_一级_保内外)
def kpi_a_bnbw_kpi_4(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_bnbw partition(mon_p='${mon_p}',kpi='kpi_4')
  select s.primary_classification,
  s.bn,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  sum(order_balance_amount)/count(distinct vin,tran_date(s.order_balance_date)) as num,
  s.asc_code
  from label_order s
  where s.MAINT_TYPE1<>'删除'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  and s.claim=''
  and (s.first_maintnance='' or (s.first_maintnance='1' and s.order_balance_amount<>0))
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,s.bn,s.primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4(客单价_二级_车龄)
def kpi_b_agetype_kpi_4(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_agetype partition(mon_p='${mon_p}',kpi='kpi_4')
  select s.primary_classification,
  s.second_level_classification,
  s.age_type,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  sum(order_balance_amount)/count(distinct vin,tran_date(s.order_balance_date)) as num,
  s.asc_code
  from label_order s
  where s.MAINT_TYPE1<>'删除'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  and s.claim=''
  and (s.first_maintnance='' or (s.first_maintnance='1' and s.order_balance_amount<>0))
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,s.age_type,s.primary_classification,
  s.second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4(客单价_二级_保内外)
def kpi_b_bnbw_kpi_4(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_bnbw partition(mon_p='${mon_p}',kpi='kpi_4')
  select s.primary_classification,
  s.second_level_classification,
  s.bn,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  sum(order_balance_amount)/count(distinct vin,tran_date(s.order_balance_date)) as num,
  s.asc_code
  from label_order s
  where s.MAINT_TYPE1<>'删除'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  and s.claim=''
  and (s.first_maintnance='' or (s.first_maintnance='1' and s.order_balance_amount<>0))
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,s.bn,s.primary_classification,
  s.second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.1(保养客单价_total)
def kpi_total_kpi_4_1(begin_day, end_day):
    line = """  insert overwrite table  kpi_total partition(mon_p='${mon_p}',kpi='kpi_4_1')
  select substr(tran_date(s.order_balance_date),1,7) as mon,
  sum(order_balance_amount)/count(distinct vin,tran_date(s.order_balance_date)) as num,
  s.asc_code
  from label_order s
  where s.MAINT_TYPE1='保养'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  and (s.first_maintnance='' or (s.first_maintnance='1' and s.order_balance_amount<>0))
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.1(保养客单价_一级)
def kpi_a_level_kpi_4_1(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_level partition(mon_p='${mon_p}',kpi='kpi_4_1')
  select s.primary_classification,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  sum(order_balance_amount)/count(distinct vin,tran_date(s.order_balance_date)) as num,
  s.asc_code
  from label_order s
  where s.MAINT_TYPE1='保养'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  and (s.first_maintnance='' or (s.first_maintnance='1' and s.order_balance_amount<>0))
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,s.primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.1(保养客单价_二级)
def kpi_b_level_kpi_4_1(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_level partition(mon_p='${mon_p}',kpi='kpi_4_1')
  select s.primary_classification,
  s.second_level_classification,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  sum(order_balance_amount)/count(distinct vin,tran_date(s.order_balance_date)) as num,
  s.asc_code
  from label_order s
  where s.MAINT_TYPE1='保养'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  and (s.first_maintnance='' or (s.first_maintnance='1' and s.order_balance_amount<>0))
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,s.primary_classification,
  s.second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.1(保养客单价_车龄)
def kpi_agetype_kpi_4_1(begin_day, end_day):
    line = """  insert overwrite table  kpi_agetype partition(mon_p='${mon_p}',kpi='kpi_4_1')
  select s.age_type,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  sum(order_balance_amount)/count(distinct vin,tran_date(s.order_balance_date)) as num,
  s.asc_code
  from label_order s
  where s.MAINT_TYPE1='保养'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  and (s.first_maintnance='' or (s.first_maintnance='1' and s.order_balance_amount<>0))
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,s.age_type"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.1(保养客单价_保内外)
def kpi_bnbw_kpi_4_1(begin_day, end_day):
    line = """  insert overwrite table  kpi_bnbw partition(mon_p='${mon_p}',kpi='kpi_4_1')
  select s.bn,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  sum(order_balance_amount)/count(distinct vin,tran_date(s.order_balance_date)) as num,
  s.asc_code
  from label_order s
  where s.MAINT_TYPE1='保养'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  and (s.first_maintnance='' or (s.first_maintnance='1' and s.order_balance_amount<>0))
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,s.bn"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.1(保养客单价_一级_车龄)
def kpi_a_agetype_kpi_4_1(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_agetype partition(mon_p='${mon_p}',kpi='kpi_4_1')
  select s.primary_classification,
  s.age_type,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  sum(order_balance_amount)/count(distinct vin,tran_date(s.order_balance_date)) as num,
  s.asc_code
  from label_order s
  where s.MAINT_TYPE1='保养'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  and (s.first_maintnance='' or (s.first_maintnance='1' and s.order_balance_amount<>0))
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,s.age_type,s.primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.1(保养客单价_一级_保内外)
def kpi_a_bnbw_kpi_4_1(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_bnbw partition(mon_p='${mon_p}',kpi='kpi_4_1')
  select s.primary_classification,
  s.bn,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  sum(order_balance_amount)/count(distinct vin,tran_date(s.order_balance_date)) as num,
  s.asc_code
  from label_order s
  where s.MAINT_TYPE1='保养'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  and (s.first_maintnance='' or (s.first_maintnance='1' and s.order_balance_amount<>0))
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,s.bn,s.primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.1(保养客单价_二级_车龄)
def kpi_b_agetype_kpi_4_1(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_agetype partition(mon_p='${mon_p}',kpi='kpi_4_1')
  select s.primary_classification,
  s.second_level_classification,
  s.age_type,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  sum(order_balance_amount)/count(distinct vin,tran_date(s.order_balance_date)) as num,
  s.asc_code
  from label_order s
  where s.MAINT_TYPE1='保养'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  and (s.first_maintnance='' or (s.first_maintnance='1' and s.order_balance_amount<>0))
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,s.age_type,s.primary_classification,
  s.second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.1(保养客单价_二级_保内外)
def kpi_b_bnbw_kpi_4_1(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_bnbw partition(mon_p='${mon_p}',kpi='kpi_4_1')
  select s.primary_classification,
  s.second_level_classification,
  s.bn,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  sum(order_balance_amount)/count(distinct vin,tran_date(s.order_balance_date)) as num,
  s.asc_code
  from label_order s
  where s.MAINT_TYPE1='保养'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  and (s.first_maintnance='' or (s.first_maintnance='1' and s.order_balance_amount<>0))
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,s.bn,s.primary_classification,
  s.second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.11(平价机油_total)
def kpi_total_kpi_4_11(begin_day, end_day):
    line = """  insert overwrite table  kpi_total partition(mon_p='${mon_p}',kpi='kpi_4_11')
  select substr(tran_date(s.order_balance_date),1,7) as mon,
  sum(order_balance_amount)/count(distinct s.vin,tran_date(s.order_balance_date)) as num,
  s.asc_code
  from label_order s,
  (
  select distinct t.asc_code,t.order_number from part t,engine_oil p
  where t.part_number=p.part_num
  and p.type='低端'
  ) t
  where s.MAINT_TYPE1='保养'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  and (s.first_maintnance='' or (s.first_maintnance='1' and s.order_balance_amount<>0))
  and s.asc_code=t.asc_code
  and s.order_number=t.order_number
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.11(平价机油_一级)
def kpi_a_level_kpi_4_11(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_level partition(mon_p='${mon_p}',kpi='kpi_4_11')
  select s.primary_classification,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  sum(order_balance_amount)/count(distinct s.vin,tran_date(s.order_balance_date)) as num,
  s.asc_code
  from label_order s,
  (
  select distinct t.asc_code,t.order_number from part t,engine_oil p
  where t.part_number=p.part_num
  and p.type='低端'
  ) t
  where s.MAINT_TYPE1='保养'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  and (s.first_maintnance='' or (s.first_maintnance='1' and s.order_balance_amount<>0))
  and s.asc_code=t.asc_code
  and s.order_number=t.order_number
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,s.primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.11(平价机油_二级)
def kpi_b_level_kpi_4_11(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_level partition(mon_p='${mon_p}',kpi='kpi_4_11')
  select s.primary_classification,
  s.second_level_classification,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  sum(order_balance_amount)/count(distinct s.vin,tran_date(s.order_balance_date)) as num,
  s.asc_code
  from label_order s,
  (
  select distinct t.asc_code,t.order_number from part t,engine_oil p
  where t.part_number=p.part_num
  and p.type='低端'
  ) t
  where s.MAINT_TYPE1='保养'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  and (s.first_maintnance='' or (s.first_maintnance='1' and s.order_balance_amount<>0))
  and s.asc_code=t.asc_code
  and s.order_number=t.order_number
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,s.primary_classification,
  s.second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.11(平价机油_车龄)
def kpi_agetype_kpi_4_11(begin_day, end_day):
    line = """  insert overwrite table  kpi_agetype partition(mon_p='${mon_p}',kpi='kpi_4_11')
  select s.age_type,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  sum(order_balance_amount)/count(distinct s.vin,tran_date(s.order_balance_date)) as num,
  s.asc_code
  from label_order s,
  (
  select distinct t.asc_code,t.order_number from part t,engine_oil p
  where t.part_number=p.part_num
  and p.type='低端'
  ) t
  where s.MAINT_TYPE1='保养'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  and (s.first_maintnance='' or (s.first_maintnance='1' and s.order_balance_amount<>0))
  and s.asc_code=t.asc_code
  and s.order_number=t.order_number
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,s.age_type"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.11(平价机油_保内外)
def kpi_bnbw_kpi_4_11(begin_day, end_day):
    line = """  insert overwrite table  kpi_bnbw partition(mon_p='${mon_p}',kpi='kpi_4_11')
  select s.bn,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  sum(order_balance_amount)/count(distinct s.vin,tran_date(s.order_balance_date)) as num,
  s.asc_code
  from label_order s,
  (
  select distinct t.asc_code,t.order_number from part t,engine_oil p
  where t.part_number=p.part_num
  and p.type='低端'
  ) t
  where s.MAINT_TYPE1='保养'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  and (s.first_maintnance='' or (s.first_maintnance='1' and s.order_balance_amount<>0))
  and s.asc_code=t.asc_code
  and s.order_number=t.order_number
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,s.bn"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.11(平价机油_一级_车龄)
def kpi_a_agetype_kpi_4_11(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_agetype partition(mon_p='${mon_p}',kpi='kpi_4_11')
  select s.primary_classification,
  s.age_type,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  sum(order_balance_amount)/count(distinct s.vin,tran_date(s.order_balance_date)) as num,
  s.asc_code
  from label_order s,
  (
  select distinct t.asc_code,t.order_number from part t,engine_oil p
  where t.part_number=p.part_num
  and p.type='低端'
  ) t
  where s.MAINT_TYPE1='保养'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  and (s.first_maintnance='' or (s.first_maintnance='1' and s.order_balance_amount<>0))
  and s.asc_code=t.asc_code
  and s.order_number=t.order_number
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,s.age_type,s.primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.11(平价机油_一级_保内外)
def kpi_a_bnbw_kpi_4_11(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_bnbw partition(mon_p='${mon_p}',kpi='kpi_4_11')
  select s.primary_classification,
  s.bn,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  sum(order_balance_amount)/count(distinct s.vin,tran_date(s.order_balance_date)) as num,
  s.asc_code
  from label_order s,
  (
  select distinct t.asc_code,t.order_number from part t,engine_oil p
  where t.part_number=p.part_num
  and p.type='低端'
  ) t
  where s.MAINT_TYPE1='保养'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  and (s.first_maintnance='' or (s.first_maintnance='1' and s.order_balance_amount<>0))
  and s.asc_code=t.asc_code
  and s.order_number=t.order_number
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,s.bn,s.primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.11(平价机油_二级_车龄)
def kpi_b_agetype_kpi_4_11(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_agetype partition(mon_p='${mon_p}',kpi='kpi_4_11')
  select s.primary_classification,
  s.second_level_classification,
  s.age_type,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  sum(order_balance_amount)/count(distinct s.vin,tran_date(s.order_balance_date)) as num,
  s.asc_code
  from label_order s,
  (
  select distinct t.asc_code,t.order_number from part t,engine_oil p
  where t.part_number=p.part_num
  and p.type='低端'
  ) t
  where s.MAINT_TYPE1='保养'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  and (s.first_maintnance='' or (s.first_maintnance='1' and s.order_balance_amount<>0))
  and s.asc_code=t.asc_code
  and s.order_number=t.order_number
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,s.age_type,s.primary_classification,
  s.second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.11(平价机油_二级_保内外)
def kpi_b_bnbw_kpi_4_11(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_bnbw partition(mon_p='${mon_p}',kpi='kpi_4_11')
  select s.primary_classification,
  s.second_level_classification,
  s.bn,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  sum(order_balance_amount)/count(distinct s.vin,tran_date(s.order_balance_date)) as num,
  s.asc_code
  from label_order s,
  (
  select distinct t.asc_code,t.order_number from part t,engine_oil p
  where t.part_number=p.part_num
  and p.type='低端'
  ) t
  where s.MAINT_TYPE1='保养'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  and (s.first_maintnance='' or (s.first_maintnance='1' and s.order_balance_amount<>0))
  and s.asc_code=t.asc_code
  and s.order_number=t.order_number
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,s.bn,s.primary_classification,
  s.second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.12(中端机油_total)
def kpi_total_kpi_4_12(begin_day, end_day):
    line = """  insert overwrite table  kpi_total partition(mon_p='${mon_p}',kpi='kpi_4_12')
  select substr(tran_date(s.order_balance_date),1,7) as mon,
  sum(order_balance_amount)/count(distinct s.vin,tran_date(s.order_balance_date)) as num,
  s.asc_code
  from label_order s,
  (
  select distinct t.asc_code,t.order_number from part t,engine_oil p
  where t.part_number=p.part_num
  and p.type='中端'
  ) t
  where s.MAINT_TYPE1='保养'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  and (s.first_maintnance='' or (s.first_maintnance='1' and s.order_balance_amount<>0))
  and s.asc_code=t.asc_code
  and s.order_number=t.order_number
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.12(中端机油_一级)
def kpi_a_level_kpi_4_12(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_level partition(mon_p='${mon_p}',kpi='kpi_4_12')
  select s.primary_classification,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  sum(order_balance_amount)/count(distinct s.vin,tran_date(s.order_balance_date)) as num,
  s.asc_code
  from label_order s,
  (
  select distinct t.asc_code,t.order_number from part t,engine_oil p
  where t.part_number=p.part_num
  and p.type='中端'
  ) t
  where s.MAINT_TYPE1='保养'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  and (s.first_maintnance='' or (s.first_maintnance='1' and s.order_balance_amount<>0))
  and s.asc_code=t.asc_code
  and s.order_number=t.order_number
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,s.primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.12(中端机油_二级)
def kpi_b_level_kpi_4_12(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_level partition(mon_p='${mon_p}',kpi='kpi_4_12')
  select s.primary_classification,
  s.second_level_classification,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  sum(order_balance_amount)/count(distinct s.vin,tran_date(s.order_balance_date)) as num,
  s.asc_code
  from label_order s,
  (
  select distinct t.asc_code,t.order_number from part t,engine_oil p
  where t.part_number=p.part_num
  and p.type='中端'
  ) t
  where s.MAINT_TYPE1='保养'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  and (s.first_maintnance='' or (s.first_maintnance='1' and s.order_balance_amount<>0))
  and s.asc_code=t.asc_code
  and s.order_number=t.order_number
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,s.primary_classification,
  s.second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.12(中端机油_车龄)
def kpi_agetype_kpi_4_12(begin_day, end_day):
    line = """  insert overwrite table  kpi_agetype partition(mon_p='${mon_p}',kpi='kpi_4_12')
  select s.age_type,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  sum(order_balance_amount)/count(distinct s.vin,tran_date(s.order_balance_date)) as num,
  s.asc_code
  from label_order s,
  (
  select distinct t.asc_code,t.order_number from part t,engine_oil p
  where t.part_number=p.part_num
  and p.type='中端'
  ) t
  where s.MAINT_TYPE1='保养'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  and (s.first_maintnance='' or (s.first_maintnance='1' and s.order_balance_amount<>0))
  and s.asc_code=t.asc_code
  and s.order_number=t.order_number
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,s.age_type"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.12(中端机油_保内外)
def kpi_bnbw_kpi_4_12(begin_day, end_day):
    line = """  insert overwrite table  kpi_bnbw partition(mon_p='${mon_p}',kpi='kpi_4_12')
  select s.bn,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  sum(order_balance_amount)/count(distinct s.vin,tran_date(s.order_balance_date)) as num,
  s.asc_code
  from label_order s,
  (
  select distinct t.asc_code,t.order_number from part t,engine_oil p
  where t.part_number=p.part_num
  and p.type='中端'
  ) t
  where s.MAINT_TYPE1='保养'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  and (s.first_maintnance='' or (s.first_maintnance='1' and s.order_balance_amount<>0))
  and s.asc_code=t.asc_code
  and s.order_number=t.order_number
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,s.bn"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.12(中端机油_一级_车龄)
def kpi_a_agetype_kpi_4_12(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_agetype partition(mon_p='${mon_p}',kpi='kpi_4_12')
  select s.primary_classification,
  s.age_type,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  sum(order_balance_amount)/count(distinct s.vin,tran_date(s.order_balance_date)) as num,
  s.asc_code
  from label_order s,
  (
  select distinct t.asc_code,t.order_number from part t,engine_oil p
  where t.part_number=p.part_num
  and p.type='中端'
  ) t
  where s.MAINT_TYPE1='保养'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  and (s.first_maintnance='' or (s.first_maintnance='1' and s.order_balance_amount<>0))
  and s.asc_code=t.asc_code
  and s.order_number=t.order_number
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,s.age_type,s.primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.12(中端机油_一级_保内外)
def kpi_a_bnbw_kpi_4_12(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_bnbw partition(mon_p='${mon_p}',kpi='kpi_4_12')
  select s.primary_classification,
  s.bn,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  sum(order_balance_amount)/count(distinct s.vin,tran_date(s.order_balance_date)) as num,
  s.asc_code
  from label_order s,
  (
  select distinct t.asc_code,t.order_number from part t,engine_oil p
  where t.part_number=p.part_num
  and p.type='中端'
  ) t
  where s.MAINT_TYPE1='保养'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  and (s.first_maintnance='' or (s.first_maintnance='1' and s.order_balance_amount<>0))
  and s.asc_code=t.asc_code
  and s.order_number=t.order_number
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,s.bn,s.primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.12(中端机油_二级_车龄)
def kpi_b_agetype_kpi_4_12(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_agetype partition(mon_p='${mon_p}',kpi='kpi_4_12')
  select s.primary_classification,
  s.second_level_classification,
  s.age_type,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  sum(order_balance_amount)/count(distinct s.vin,tran_date(s.order_balance_date)) as num,
  s.asc_code
  from label_order s,
  (
  select distinct t.asc_code,t.order_number from part t,engine_oil p
  where t.part_number=p.part_num
  and p.type='中端'
  ) t
  where s.MAINT_TYPE1='保养'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  and (s.first_maintnance='' or (s.first_maintnance='1' and s.order_balance_amount<>0))
  and s.asc_code=t.asc_code
  and s.order_number=t.order_number
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,s.age_type,s.primary_classification,
  s.second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.12(中端机油_二级_保内外)
def kpi_b_bnbw_kpi_4_12(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_bnbw partition(mon_p='${mon_p}',kpi='kpi_4_12')
  select s.primary_classification,
  s.second_level_classification,
  s.bn,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  sum(order_balance_amount)/count(distinct s.vin,tran_date(s.order_balance_date)) as num,
  s.asc_code
  from label_order s,
  (
  select distinct t.asc_code,t.order_number from part t,engine_oil p
  where t.part_number=p.part_num
  and p.type='中端'
  ) t
  where s.MAINT_TYPE1='保养'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  and (s.first_maintnance='' or (s.first_maintnance='1' and s.order_balance_amount<>0))
  and s.asc_code=t.asc_code
  and s.order_number=t.order_number
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,s.bn,s.primary_classification,
  s.second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.13(高端机油_total)
def kpi_total_kpi_4_13(begin_day, end_day):
    line = """  insert overwrite table  kpi_total partition(mon_p='${mon_p}',kpi='kpi_4_13')
  select substr(tran_date(s.order_balance_date),1,7) as mon,
  sum(order_balance_amount)/count(distinct s.vin,tran_date(s.order_balance_date)) as num,
  s.asc_code
  from label_order s,
  (
  select distinct t.asc_code,t.order_number from part t,engine_oil p
  where t.part_number=p.part_num
  and p.type='高端'
  ) t
  where s.MAINT_TYPE1='保养'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  and (s.first_maintnance='' or (s.first_maintnance='1' and s.order_balance_amount<>0))
  and s.asc_code=t.asc_code
  and s.order_number=t.order_number
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.13(高端机油_一级)
def kpi_a_level_kpi_4_13(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_level partition(mon_p='${mon_p}',kpi='kpi_4_13')
  select s.primary_classification,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  sum(order_balance_amount)/count(distinct s.vin,tran_date(s.order_balance_date)) as num,
  s.asc_code
  from label_order s,
  (
  select distinct t.asc_code,t.order_number from part t,engine_oil p
  where t.part_number=p.part_num
  and p.type='高端'
  ) t
  where s.MAINT_TYPE1='保养'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  and (s.first_maintnance='' or (s.first_maintnance='1' and s.order_balance_amount<>0))
  and s.asc_code=t.asc_code
  and s.order_number=t.order_number
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,s.primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.13(高端机油_二级)
def kpi_b_level_kpi_4_13(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_level partition(mon_p='${mon_p}',kpi='kpi_4_13')
  select s.primary_classification,
  s.second_level_classification,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  sum(order_balance_amount)/count(distinct s.vin,tran_date(s.order_balance_date)) as num,
  s.asc_code
  from label_order s,
  (
  select distinct t.asc_code,t.order_number from part t,engine_oil p
  where t.part_number=p.part_num
  and p.type='高端'
  ) t
  where s.MAINT_TYPE1='保养'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  and (s.first_maintnance='' or (s.first_maintnance='1' and s.order_balance_amount<>0))
  and s.asc_code=t.asc_code
  and s.order_number=t.order_number
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,s.primary_classification,
  s.second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.13(高端机油_车龄)
def kpi_agetype_kpi_4_13(begin_day, end_day):
    line = """  insert overwrite table  kpi_agetype partition(mon_p='${mon_p}',kpi='kpi_4_13')
  select s.age_type,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  sum(order_balance_amount)/count(distinct s.vin,tran_date(s.order_balance_date)) as num,
  s.asc_code
  from label_order s,
  (
  select distinct t.asc_code,t.order_number from part t,engine_oil p
  where t.part_number=p.part_num
  and p.type='高端'
  ) t
  where s.MAINT_TYPE1='保养'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  and (s.first_maintnance='' or (s.first_maintnance='1' and s.order_balance_amount<>0))
  and s.asc_code=t.asc_code
  and s.order_number=t.order_number
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,s.age_type"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.13(高端机油_保内外)
def kpi_bnbw_kpi_4_13(begin_day, end_day):
    line = """  insert overwrite table  kpi_bnbw partition(mon_p='${mon_p}',kpi='kpi_4_13')
  select s.bn,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  sum(order_balance_amount)/count(distinct s.vin,tran_date(s.order_balance_date)) as num,
  s.asc_code
  from label_order s,
  (
  select distinct t.asc_code,t.order_number from part t,engine_oil p
  where t.part_number=p.part_num
  and p.type='高端'
  ) t
  where s.MAINT_TYPE1='保养'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  and (s.first_maintnance='' or (s.first_maintnance='1' and s.order_balance_amount<>0))
  and s.asc_code=t.asc_code
  and s.order_number=t.order_number
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,s.bn"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.13(高端机油_一级_车龄)
def kpi_a_agetype_kpi_4_13(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_agetype partition(mon_p='${mon_p}',kpi='kpi_4_13')
  select s.primary_classification,
  s.age_type,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  sum(order_balance_amount)/count(distinct s.vin,tran_date(s.order_balance_date)) as num,
  s.asc_code
  from label_order s,
  (
  select distinct t.asc_code,t.order_number from part t,engine_oil p
  where t.part_number=p.part_num
  and p.type='高端'
  ) t
  where s.MAINT_TYPE1='保养'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  and (s.first_maintnance='' or (s.first_maintnance='1' and s.order_balance_amount<>0))
  and s.asc_code=t.asc_code
  and s.order_number=t.order_number
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,s.age_type,s.primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.13(高端机油_一级_保内外)
def kpi_a_bnbw_kpi_4_13(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_bnbw partition(mon_p='${mon_p}',kpi='kpi_4_13')
  select s.primary_classification,
  s.bn,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  sum(order_balance_amount)/count(distinct s.vin,tran_date(s.order_balance_date)) as num,
  s.asc_code
  from label_order s,
  (
  select distinct t.asc_code,t.order_number from part t,engine_oil p
  where t.part_number=p.part_num
  and p.type='高端'
  ) t
  where s.MAINT_TYPE1='保养'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  and (s.first_maintnance='' or (s.first_maintnance='1' and s.order_balance_amount<>0))
  and s.asc_code=t.asc_code
  and s.order_number=t.order_number
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,s.bn,s.primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.13(高端机油_二级_车龄)
def kpi_b_agetype_kpi_4_13(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_agetype partition(mon_p='${mon_p}',kpi='kpi_4_13')
  select s.primary_classification,
  s.second_level_classification,
  s.age_type,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  sum(order_balance_amount)/count(distinct s.vin,tran_date(s.order_balance_date)) as num,
  s.asc_code
  from label_order s,
  (
  select distinct t.asc_code,t.order_number from part t,engine_oil p
  where t.part_number=p.part_num
  and p.type='高端'
  ) t
  where s.MAINT_TYPE1='保养'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  and (s.first_maintnance='' or (s.first_maintnance='1' and s.order_balance_amount<>0))
  and s.asc_code=t.asc_code
  and s.order_number=t.order_number
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,s.age_type,s.primary_classification,
  s.second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.13(高端机油_二级_保内外)
def kpi_b_bnbw_kpi_4_13(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_bnbw partition(mon_p='${mon_p}',kpi='kpi_4_13')
  select s.primary_classification,
  s.second_level_classification,
  s.bn,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  sum(order_balance_amount)/count(distinct s.vin,tran_date(s.order_balance_date)) as num,
  s.asc_code
  from label_order s,
  (
  select distinct t.asc_code,t.order_number from part t,engine_oil p
  where t.part_number=p.part_num
  and p.type='高端'
  ) t
  where s.MAINT_TYPE1='保养'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  and (s.first_maintnance='' or (s.first_maintnance='1' and s.order_balance_amount<>0))
  and s.asc_code=t.asc_code
  and s.order_number=t.order_number
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,s.bn,s.primary_classification,
  s.second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.2(维修客单价_total)
def kpi_total_kpi_4_2(begin_day, end_day):
    line = """  insert overwrite table  kpi_total partition(mon_p='${mon_p}',kpi='kpi_4_2')
  select substr(tran_date(s.order_balance_date),1,7) as mon,
  sum(order_balance_amount)/count(distinct vin,tran_date(s.order_balance_date)) as num,
  s.asc_code
  from label_order s
  where s.MAINT_TYPE1='维修'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.2(维修客单价_一级)
def kpi_a_level_kpi_4_2(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_level partition(mon_p='${mon_p}',kpi='kpi_4_2')
  select s.primary_classification,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  sum(order_balance_amount)/count(distinct vin,tran_date(s.order_balance_date)) as num,
  s.asc_code
  from label_order s
  where s.MAINT_TYPE1='维修'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,s.primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.2(维修客单价_二级)
def kpi_b_level_kpi_4_2(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_level partition(mon_p='${mon_p}',kpi='kpi_4_2')
  select s.primary_classification,
  s.second_level_classification,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  sum(order_balance_amount)/count(distinct vin,tran_date(s.order_balance_date)) as num,
  s.asc_code
  from label_order s
  where s.MAINT_TYPE1='维修'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,s.primary_classification,
  s.second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.2(维修客单价_车龄)
def kpi_agetype_kpi_4_2(begin_day, end_day):
    line = """  insert overwrite table  kpi_agetype partition(mon_p='${mon_p}',kpi='kpi_4_2')
  select s.age_type,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  sum(order_balance_amount)/count(distinct vin,tran_date(s.order_balance_date)) as num,
  s.asc_code
  from label_order s
  where s.MAINT_TYPE1='维修'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,s.age_type"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.2(维修客单价_保内外)
def kpi_bnbw_kpi_4_2(begin_day, end_day):
    line = """  insert overwrite table  kpi_bnbw partition(mon_p='${mon_p}',kpi='kpi_4_2')
  select s.bn,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  sum(order_balance_amount)/count(distinct vin,tran_date(s.order_balance_date)) as num,
  s.asc_code
  from label_order s
  where s.MAINT_TYPE1='维修'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,s.bn"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.2(维修客单价_一级_车龄)
def kpi_a_agetype_kpi_4_2(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_agetype partition(mon_p='${mon_p}',kpi='kpi_4_2')
  select s.primary_classification,
  s.age_type,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  sum(order_balance_amount)/count(distinct vin,tran_date(s.order_balance_date)) as num,
  s.asc_code
  from label_order s
  where s.MAINT_TYPE1='维修'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,s.age_type,s.primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.2(维修客单价_一级_保内外)
def kpi_a_bnbw_kpi_4_2(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_bnbw partition(mon_p='${mon_p}',kpi='kpi_4_2')
  select s.primary_classification,
  s.bn,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  sum(order_balance_amount)/count(distinct vin,tran_date(s.order_balance_date)) as num,
  s.asc_code
  from label_order s
  where s.MAINT_TYPE1='维修'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,s.bn,s.primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.2(维修客单价_二级_车龄)
def kpi_b_agetype_kpi_4_2(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_agetype partition(mon_p='${mon_p}',kpi='kpi_4_2')
  select s.primary_classification,
  s.second_level_classification,
  s.age_type,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  sum(order_balance_amount)/count(distinct vin,tran_date(s.order_balance_date)) as num,
  s.asc_code
  from label_order s
  where s.MAINT_TYPE1='维修'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,s.age_type,s.primary_classification,
  s.second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.2(维修客单价_二级_保内外)
def kpi_b_bnbw_kpi_4_2(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_bnbw partition(mon_p='${mon_p}',kpi='kpi_4_2')
  select s.primary_classification,
  s.second_level_classification,
  s.bn,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  sum(order_balance_amount)/count(distinct vin,tran_date(s.order_balance_date)) as num,
  s.asc_code
  from label_order s
  where s.MAINT_TYPE1='维修'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,s.bn,s.primary_classification,
  s.second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.3(事故客单价_total)
def kpi_total_kpi_4_3(begin_day, end_day):
    line = """  insert overwrite table  kpi_total partition(mon_p='${mon_p}',kpi='kpi_4_3')
  select substr(tran_date(s.order_balance_date),1,7) as mon,
  sum(order_balance_amount)/count(distinct vin,tran_date(s.order_balance_date)) as num,
  s.asc_code
  from label_order s
  where s.MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.3(事故客单价_一级)
def kpi_a_level_kpi_4_3(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_level partition(mon_p='${mon_p}',kpi='kpi_4_3')
  select s.primary_classification,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  sum(order_balance_amount)/count(distinct vin,tran_date(s.order_balance_date)) as num,
  s.asc_code
  from label_order s
  where s.MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,s.primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.3(事故客单价_二级)
def kpi_b_level_kpi_4_3(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_level partition(mon_p='${mon_p}',kpi='kpi_4_3')
  select s.primary_classification,
  s.second_level_classification,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  sum(order_balance_amount)/count(distinct vin,tran_date(s.order_balance_date)) as num,
  s.asc_code
  from label_order s
  where s.MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,s.primary_classification,
  s.second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.3(事故客单价_车龄)
def kpi_agetype_kpi_4_3(begin_day, end_day):
    line = """  insert overwrite table  kpi_agetype partition(mon_p='${mon_p}',kpi='kpi_4_3')
  select s.age_type,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  sum(order_balance_amount)/count(distinct vin,tran_date(s.order_balance_date)) as num,
  s.asc_code
  from label_order s
  where s.MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,s.age_type"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.3(事故客单价_保内外)
def kpi_bnbw_kpi_4_3(begin_day, end_day):
    line = """  insert overwrite table  kpi_bnbw partition(mon_p='${mon_p}',kpi='kpi_4_3')
  select s.bn,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  sum(order_balance_amount)/count(distinct vin,tran_date(s.order_balance_date)) as num,
  s.asc_code
  from label_order s
  where s.MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,s.bn"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.3(事故客单价_一级_车龄)
def kpi_a_agetype_kpi_4_3(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_agetype partition(mon_p='${mon_p}',kpi='kpi_4_3')
  select s.primary_classification,
  s.age_type,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  sum(order_balance_amount)/count(distinct vin,tran_date(s.order_balance_date)) as num,
  s.asc_code
  from label_order s
  where s.MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,s.age_type,s.primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.3(事故客单价_一级_保内外)
def kpi_a_bnbw_kpi_4_3(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_bnbw partition(mon_p='${mon_p}',kpi='kpi_4_3')
  select s.primary_classification,
  s.bn,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  sum(order_balance_amount)/count(distinct vin,tran_date(s.order_balance_date)) as num,
  s.asc_code
  from label_order s
  where s.MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,s.bn,s.primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.3(事故客单价_二级_车龄)
def kpi_b_agetype_kpi_4_3(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_agetype partition(mon_p='${mon_p}',kpi='kpi_4_3')
  select s.primary_classification,
  s.second_level_classification,
  s.age_type,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  sum(order_balance_amount)/count(distinct vin,tran_date(s.order_balance_date)) as num,
  s.asc_code
  from label_order s
  where s.MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,s.age_type,s.primary_classification,
  s.second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.3(事故客单价_二级_保内外)
def kpi_b_bnbw_kpi_4_3(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_bnbw partition(mon_p='${mon_p}',kpi='kpi_4_3')
  select s.primary_classification,
  s.second_level_classification,
  s.bn,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  sum(order_balance_amount)/count(distinct vin,tran_date(s.order_balance_date)) as num,
  s.asc_code
  from label_order s
  where s.MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,s.bn,s.primary_classification,
  s.second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.31(799元以下事故客单价_total)
def kpi_total_kpi_4_31(begin_day, end_day):
    line = """  insert overwrite table  kpi_total partition(mon_p='${mon_p}',kpi='kpi_4_31')
  select substr(order_balance_date,1,7) as mon,
  sum(sumamount)/count(distinct vin,order_balance_date) as num,
  asc_code
  from (
  select s.asc_code,
  s.vin,
  tran_date(order_balance_date) as order_balance_date,
  sum(order_balance_amount) as sumamount 
  from 
  label_order s
  where s.MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by s.asc_code,s.vin,tran_date(order_balance_date)
  ) s where sumamount<800
  group by substr(order_balance_date,1,7),asc_code"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.31(799元以下事故客单价_一级)
def kpi_a_level_kpi_4_31(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_level partition(mon_p='${mon_p}',kpi='kpi_4_31')
  select primary_classification,substr(order_balance_date,1,7) as mon,
  sum(sumamount)/count(distinct vin,order_balance_date) as num,
  asc_code
  from (
  select s.asc_code,primary_classification,
  s.vin,
  tran_date(order_balance_date) as order_balance_date,
  sum(order_balance_amount) as sumamount 
  from 
  label_order s
  where s.MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by s.asc_code,s.vin,tran_date(order_balance_date),primary_classification
  ) s where sumamount<800
  group by substr(order_balance_date,1,7),asc_code,primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.31(799元以下事故客单价_二级)
def kpi_b_level_kpi_4_31(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_level partition(mon_p='${mon_p}',kpi='kpi_4_31')
  select primary_classification,
  second_level_classification,
  substr(order_balance_date,1,7) as mon,
  sum(sumamount)/count(distinct vin,order_balance_date) as num,
  asc_code
  from (
  select s.asc_code,primary_classification,second_level_classification,
  s.vin,
  tran_date(order_balance_date) as order_balance_date,
  sum(order_balance_amount) as sumamount 
  from 
  label_order s
  where s.MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by s.asc_code,s.vin,tran_date(order_balance_date),primary_classification,
  second_level_classification
  ) s where sumamount<800
  group by substr(order_balance_date,1,7),asc_code,primary_classification,
  second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.31(799元以下事故客单价_车龄)
def kpi_agetype_kpi_4_31(begin_day, end_day):
    line = """  insert overwrite table  kpi_agetype partition(mon_p='${mon_p}',kpi='kpi_4_31')
  select age_type,substr(order_balance_date,1,7) as mon,
  sum(sumamount)/count(distinct vin,order_balance_date) as num,
  asc_code
  from (
  select s.asc_code,age_type,
  s.vin,
  tran_date(order_balance_date) as order_balance_date,
  sum(order_balance_amount) as sumamount 
  from 
  label_order s
  where s.MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by s.asc_code,s.vin,tran_date(order_balance_date),age_type
  ) s where sumamount<800
  group by substr(order_balance_date,1,7),asc_code,age_type"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.31(799元以下事故客单价_保内外)
def kpi_bnbw_kpi_4_31(begin_day, end_day):
    line = """  insert overwrite table  kpi_bnbw partition(mon_p='${mon_p}',kpi='kpi_4_31')
  select bn,substr(order_balance_date,1,7) as mon,
  sum(sumamount)/count(distinct vin,order_balance_date) as num,
  asc_code
  from (
  select s.asc_code,bn,
  s.vin,
  tran_date(order_balance_date) as order_balance_date,
  sum(order_balance_amount) as sumamount 
  from 
  label_order s
  where s.MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by s.asc_code,s.vin,tran_date(order_balance_date),bn
  ) s where sumamount<800
  group by substr(order_balance_date,1,7),asc_code,bn"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.31(799元以下事故客单价_一级_车龄)
def kpi_a_agetype_kpi_4_31(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_agetype partition(mon_p='${mon_p}',kpi='kpi_4_31')
  select primary_classification,
  age_type,
  substr(order_balance_date,1,7) as mon,
  sum(sumamount)/count(distinct vin,order_balance_date) as num,
  asc_code
  from (
  select s.asc_code,age_type,primary_classification,
  s.vin,
  tran_date(order_balance_date) as order_balance_date,
  sum(order_balance_amount) as sumamount 
  from 
  label_order s
  where s.MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by s.asc_code,s.vin,tran_date(order_balance_date),age_type,primary_classification
  ) s where sumamount<800
  group by substr(order_balance_date,1,7),asc_code,age_type,primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.31(799元以下事故客单价_一级_保内外)
def kpi_a_bnbw_kpi_4_31(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_bnbw partition(mon_p='${mon_p}',kpi='kpi_4_31')
  select primary_classification,
  bn,
  substr(order_balance_date,1,7) as mon,
  sum(sumamount)/count(distinct vin,order_balance_date) as num,
  asc_code
  from (
  select s.asc_code,bn,primary_classification,
  s.vin,
  tran_date(order_balance_date) as order_balance_date,
  sum(order_balance_amount) as sumamount 
  from 
  label_order s
  where s.MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by s.asc_code,s.vin,tran_date(order_balance_date),bn,primary_classification
  ) s where sumamount<800
  group by substr(order_balance_date,1,7),asc_code,bn,primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.31(799元以下事故客单价_二级_车龄)
def kpi_b_agetype_kpi_4_31(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_agetype partition(mon_p='${mon_p}',kpi='kpi_4_31')
  select primary_classification,
  second_level_classification,
  age_type,
  substr(order_balance_date,1,7) as mon,
  sum(sumamount)/count(distinct vin,order_balance_date) as num,
  asc_code
  from (
  select s.asc_code,age_type,primary_classification,
  second_level_classification,
  s.vin,
  tran_date(order_balance_date) as order_balance_date,
  sum(order_balance_amount) as sumamount 
  from 
  label_order s
  where s.MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by s.asc_code,s.vin,tran_date(order_balance_date),age_type,
  primary_classification,second_level_classification
  ) s where sumamount<800
  group by substr(order_balance_date,1,7),asc_code,age_type,
  primary_classification,second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.31(799元以下事故客单价_二级_保内外)
def kpi_b_bnbw_kpi_4_31(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_bnbw partition(mon_p='${mon_p}',kpi='kpi_4_31')
  select primary_classification,
  second_level_classification,
  bn,
  substr(order_balance_date,1,7) as mon,
  sum(sumamount)/count(distinct vin,order_balance_date) as num,
  asc_code
  from (
  select s.asc_code,bn,primary_classification,
  second_level_classification,
  s.vin,
  tran_date(order_balance_date) as order_balance_date,
  sum(order_balance_amount) as sumamount 
  from 
  label_order s
  where s.MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by s.asc_code,s.vin,tran_date(order_balance_date),bn,
  primary_classification,second_level_classification
  ) s where sumamount<800
  group by substr(order_balance_date,1,7),asc_code,bn,
  primary_classification,second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.32(800-1999元事故客单价_total)
def kpi_total_kpi_4_32(begin_day, end_day):
    line = """  insert overwrite table  kpi_total partition(mon_p='${mon_p}',kpi='kpi_4_32')
  select substr(order_balance_date,1,7) as mon,
  sum(sumamount)/count(distinct vin,order_balance_date) as num,
  asc_code
  from (
  select s.asc_code,
  s.vin,
  tran_date(order_balance_date) as order_balance_date,
  sum(order_balance_amount) as sumamount 
  from 
  label_order s
  where s.MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by s.asc_code,s.vin,tran_date(order_balance_date)
  ) s where sumamount>=800 and sumamount<2000
  group by substr(order_balance_date,1,7),asc_code"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.32(800-1999元事故客单价_一级)
def kpi_a_level_kpi_4_32(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_level partition(mon_p='${mon_p}',kpi='kpi_4_32')
  select primary_classification,substr(order_balance_date,1,7) as mon,
  sum(sumamount)/count(distinct vin,order_balance_date) as num,
  asc_code
  from (
  select s.asc_code,primary_classification,
  s.vin,
  tran_date(order_balance_date) as order_balance_date,
  sum(order_balance_amount) as sumamount 
  from 
  label_order s
  where s.MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by s.asc_code,s.vin,tran_date(order_balance_date),primary_classification
  ) s where sumamount>=800 and sumamount<2000
  group by substr(order_balance_date,1,7),asc_code,primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.32(800-1999元事故客单价_二级)
def kpi_b_level_kpi_4_32(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_level partition(mon_p='${mon_p}',kpi='kpi_4_32')
  select primary_classification,
  second_level_classification,
  substr(order_balance_date,1,7) as mon,
  sum(sumamount)/count(distinct vin,order_balance_date) as num,
  asc_code
  from (
  select s.asc_code,primary_classification,second_level_classification,
  s.vin,
  tran_date(order_balance_date) as order_balance_date,
  sum(order_balance_amount) as sumamount 
  from 
  label_order s
  where s.MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by s.asc_code,s.vin,tran_date(order_balance_date),primary_classification,
  second_level_classification
  ) s where sumamount>=800 and sumamount<2000
  group by substr(order_balance_date,1,7),asc_code,primary_classification,
  second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.32(800-1999元事故客单价_车龄)
def kpi_agetype_kpi_4_32(begin_day, end_day):
    line = """  insert overwrite table  kpi_agetype partition(mon_p='${mon_p}',kpi='kpi_4_32')
  select age_type,substr(order_balance_date,1,7) as mon,
  sum(sumamount)/count(distinct vin,order_balance_date) as num,
  asc_code
  from (
  select s.asc_code,age_type,
  s.vin,
  tran_date(order_balance_date) as order_balance_date,
  sum(order_balance_amount) as sumamount 
  from 
  label_order s
  where s.MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by s.asc_code,s.vin,tran_date(order_balance_date),age_type
  ) s where sumamount>=800 and sumamount<2000
  group by substr(order_balance_date,1,7),asc_code,age_type"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.32(800-1999元事故客单价_保内外)
def kpi_bnbw_kpi_4_32(begin_day, end_day):
    line = """  insert overwrite table  kpi_bnbw partition(mon_p='${mon_p}',kpi='kpi_4_32')
  select bn,substr(order_balance_date,1,7) as mon,
  sum(sumamount)/count(distinct vin,order_balance_date) as num,
  asc_code
  from (
  select s.asc_code,bn,
  s.vin,
  tran_date(order_balance_date) as order_balance_date,
  sum(order_balance_amount) as sumamount 
  from 
  label_order s
  where s.MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by s.asc_code,s.vin,tran_date(order_balance_date),bn
  ) s where sumamount>=800 and sumamount<2000
  group by substr(order_balance_date,1,7),asc_code,bn"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.32(800-1999元事故客单价_一级_车龄)
def kpi_a_agetype_kpi_4_32(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_agetype partition(mon_p='${mon_p}',kpi='kpi_4_32')
  select primary_classification,
  age_type,
  substr(order_balance_date,1,7) as mon,
  sum(sumamount)/count(distinct vin,order_balance_date) as num,
  asc_code
  from (
  select s.asc_code,age_type,primary_classification,
  s.vin,
  tran_date(order_balance_date) as order_balance_date,
  sum(order_balance_amount) as sumamount 
  from 
  label_order s
  where s.MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by s.asc_code,s.vin,tran_date(order_balance_date),age_type,primary_classification
  ) s where sumamount>=800 and sumamount<2000
  group by substr(order_balance_date,1,7),asc_code,age_type,primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.32(800-1999元事故客单价_一级_保内外)
def kpi_a_bnbw_kpi_4_32(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_bnbw partition(mon_p='${mon_p}',kpi='kpi_4_32')
  select primary_classification,
  bn,
  substr(order_balance_date,1,7) as mon,
  sum(sumamount)/count(distinct vin,order_balance_date) as num,
  asc_code
  from (
  select s.asc_code,bn,primary_classification,
  s.vin,
  tran_date(order_balance_date) as order_balance_date,
  sum(order_balance_amount) as sumamount 
  from 
  label_order s
  where s.MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by s.asc_code,s.vin,tran_date(order_balance_date),bn,primary_classification
  ) s where sumamount>=800 and sumamount<2000
  group by substr(order_balance_date,1,7),asc_code,bn,primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.32(800-1999元事故客单价_二级_车龄)
def kpi_b_agetype_kpi_4_32(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_agetype partition(mon_p='${mon_p}',kpi='kpi_4_32')
  select primary_classification,
  second_level_classification,
  age_type,
  substr(order_balance_date,1,7) as mon,
  sum(sumamount)/count(distinct vin,order_balance_date) as num,
  asc_code
  from (
  select s.asc_code,age_type,primary_classification,
  second_level_classification,
  s.vin,
  tran_date(order_balance_date) as order_balance_date,
  sum(order_balance_amount) as sumamount 
  from 
  label_order s
  where s.MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by s.asc_code,s.vin,tran_date(order_balance_date),age_type,
  primary_classification,second_level_classification
  ) s where sumamount>=800 and sumamount<2000
  group by substr(order_balance_date,1,7),asc_code,age_type,
  primary_classification,second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.32(800-1999元事故客单价_二级_保内外)
def kpi_b_bnbw_kpi_4_32(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_bnbw partition(mon_p='${mon_p}',kpi='kpi_4_32')
  select primary_classification,
  second_level_classification,
  bn,
  substr(order_balance_date,1,7) as mon,
  sum(sumamount)/count(distinct vin,order_balance_date) as num,
  asc_code
  from (
  select s.asc_code,bn,primary_classification,
  second_level_classification,
  s.vin,
  tran_date(order_balance_date) as order_balance_date,
  sum(order_balance_amount) as sumamount 
  from 
  label_order s
  where s.MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by s.asc_code,s.vin,tran_date(order_balance_date),bn,
  primary_classification,second_level_classification
  ) s where sumamount>=800 and sumamount<2000
  group by substr(order_balance_date,1,7),asc_code,bn,
  primary_classification,second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.33(2000-4999元事故客单价_total)
def kpi_total_kpi_4_33(begin_day, end_day):
    line = """  insert overwrite table  kpi_total partition(mon_p='${mon_p}',kpi='kpi_4_33')
  select substr(order_balance_date,1,7) as mon,
  sum(sumamount)/count(distinct vin,order_balance_date) as num,
  asc_code
  from (
  select s.asc_code,
  s.vin,
  tran_date(order_balance_date) as order_balance_date,
  sum(order_balance_amount) as sumamount 
  from 
  label_order s
  where s.MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by s.asc_code,s.vin,tran_date(order_balance_date)
  ) s where sumamount>=2000 and sumamount<5000
  group by substr(order_balance_date,1,7),asc_code"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.33(2000-4999元事故客单价_一级)
def kpi_a_level_kpi_4_33(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_level partition(mon_p='${mon_p}',kpi='kpi_4_33')
  select primary_classification,substr(order_balance_date,1,7) as mon,
  sum(sumamount)/count(distinct vin,order_balance_date) as num,
  asc_code
  from (
  select s.asc_code,primary_classification,
  s.vin,
  tran_date(order_balance_date) as order_balance_date,
  sum(order_balance_amount) as sumamount 
  from 
  label_order s
  where s.MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by s.asc_code,s.vin,tran_date(order_balance_date),primary_classification
  ) s where sumamount>=2000 and sumamount<5000
  group by substr(order_balance_date,1,7),asc_code,primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.33(2000-4999元事故客单价_二级)
def kpi_b_level_kpi_4_33(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_level partition(mon_p='${mon_p}',kpi='kpi_4_33')
  select primary_classification,
  second_level_classification,
  substr(order_balance_date,1,7) as mon,
  sum(sumamount)/count(distinct vin,order_balance_date) as num,
  asc_code
  from (
  select s.asc_code,primary_classification,second_level_classification,
  s.vin,
  tran_date(order_balance_date) as order_balance_date,
  sum(order_balance_amount) as sumamount 
  from 
  label_order s
  where s.MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by s.asc_code,s.vin,tran_date(order_balance_date),primary_classification,
  second_level_classification
  ) s where sumamount>=2000 and sumamount<5000
  group by substr(order_balance_date,1,7),asc_code,primary_classification,
  second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.33(2000-4999元事故客单价_车龄)
def kpi_agetype_kpi_4_33(begin_day, end_day):
    line = """  insert overwrite table  kpi_agetype partition(mon_p='${mon_p}',kpi='kpi_4_33')
  select age_type,substr(order_balance_date,1,7) as mon,
  sum(sumamount)/count(distinct vin,order_balance_date) as num,
  asc_code
  from (
  select s.asc_code,age_type,
  s.vin,
  tran_date(order_balance_date) as order_balance_date,
  sum(order_balance_amount) as sumamount 
  from 
  label_order s
  where s.MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by s.asc_code,s.vin,tran_date(order_balance_date),age_type
  ) s where sumamount>=2000 and sumamount<5000
  group by substr(order_balance_date,1,7),asc_code,age_type"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.33(2000-4999元事故客单价_保内外)
def kpi_bnbw_kpi_4_33(begin_day, end_day):
    line = """  insert overwrite table  kpi_bnbw partition(mon_p='${mon_p}',kpi='kpi_4_33')
  select bn,substr(order_balance_date,1,7) as mon,
  sum(sumamount)/count(distinct vin,order_balance_date) as num,
  asc_code
  from (
  select s.asc_code,bn,
  s.vin,
  tran_date(order_balance_date) as order_balance_date,
  sum(order_balance_amount) as sumamount 
  from 
  label_order s
  where s.MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by s.asc_code,s.vin,tran_date(order_balance_date),bn
  ) s where sumamount>=2000 and sumamount<5000
  group by substr(order_balance_date,1,7),asc_code,bn"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.33(2000-4999元事故客单价_一级_车龄)
def kpi_a_agetype_kpi_4_33(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_agetype partition(mon_p='${mon_p}',kpi='kpi_4_33')
  select primary_classification,
  age_type,
  substr(order_balance_date,1,7) as mon,
  sum(sumamount)/count(distinct vin,order_balance_date) as num,
  asc_code
  from (
  select s.asc_code,age_type,primary_classification,
  s.vin,
  tran_date(order_balance_date) as order_balance_date,
  sum(order_balance_amount) as sumamount 
  from 
  label_order s
  where s.MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by s.asc_code,s.vin,tran_date(order_balance_date),age_type,primary_classification
  ) s where sumamount>=2000 and sumamount<5000
  group by substr(order_balance_date,1,7),asc_code,age_type,primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.33(2000-4999元事故客单价_一级_保内外)
def kpi_a_bnbw_kpi_4_33(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_bnbw partition(mon_p='${mon_p}',kpi='kpi_4_33')
  select primary_classification,
  bn,
  substr(order_balance_date,1,7) as mon,
  sum(sumamount)/count(distinct vin,order_balance_date) as num,
  asc_code
  from (
  select s.asc_code,bn,primary_classification,
  s.vin,
  tran_date(order_balance_date) as order_balance_date,
  sum(order_balance_amount) as sumamount 
  from 
  label_order s
  where s.MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by s.asc_code,s.vin,tran_date(order_balance_date),bn,primary_classification
  ) s where sumamount>=2000 and sumamount<5000
  group by substr(order_balance_date,1,7),asc_code,bn,primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.33(2000-4999元事故客单价_二级_车龄)
def kpi_b_agetype_kpi_4_33(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_agetype partition(mon_p='${mon_p}',kpi='kpi_4_33')
  select primary_classification,
  second_level_classification,
  age_type,
  substr(order_balance_date,1,7) as mon,
  sum(sumamount)/count(distinct vin,order_balance_date) as num,
  asc_code
  from (
  select s.asc_code,age_type,primary_classification,
  second_level_classification,
  s.vin,
  tran_date(order_balance_date) as order_balance_date,
  sum(order_balance_amount) as sumamount 
  from 
  label_order s
  where s.MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by s.asc_code,s.vin,tran_date(order_balance_date),age_type,
  primary_classification,second_level_classification
  ) s where sumamount>=2000 and sumamount<5000
  group by substr(order_balance_date,1,7),asc_code,age_type,
  primary_classification,second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.33(2000-4999元事故客单价_二级_保内外)
def kpi_b_bnbw_kpi_4_33(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_bnbw partition(mon_p='${mon_p}',kpi='kpi_4_33')
  select primary_classification,
  second_level_classification,
  bn,
  substr(order_balance_date,1,7) as mon,
  sum(sumamount)/count(distinct vin,order_balance_date) as num,
  asc_code
  from (
  select s.asc_code,bn,primary_classification,
  second_level_classification,
  s.vin,
  tran_date(order_balance_date) as order_balance_date,
  sum(order_balance_amount) as sumamount 
  from 
  label_order s
  where s.MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by s.asc_code,s.vin,tran_date(order_balance_date),bn,
  primary_classification,second_level_classification
  ) s where sumamount>=2000 and sumamount<5000
  group by substr(order_balance_date,1,7),asc_code,bn,
  primary_classification,second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.34(5000-9999元事故客单价_total)
def kpi_total_kpi_4_34(begin_day, end_day):
    line = """  insert overwrite table  kpi_total partition(mon_p='${mon_p}',kpi='kpi_4_34')
  select substr(order_balance_date,1,7) as mon,
  sum(sumamount)/count(distinct vin,order_balance_date) as num,
  asc_code
  from (
  select s.asc_code,
  s.vin,
  tran_date(order_balance_date) as order_balance_date,
  sum(order_balance_amount) as sumamount 
  from 
  label_order s
  where s.MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by s.asc_code,s.vin,tran_date(order_balance_date)
  ) s where sumamount>=5000 and sumamount<10000
  group by substr(order_balance_date,1,7),asc_code"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.34(5000-9999元事故客单价_一级)
def kpi_a_level_kpi_4_34(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_level partition(mon_p='${mon_p}',kpi='kpi_4_34')
  select primary_classification,substr(order_balance_date,1,7) as mon,
  sum(sumamount)/count(distinct vin,order_balance_date) as num,
  asc_code
  from (
  select s.asc_code,primary_classification,
  s.vin,
  tran_date(order_balance_date) as order_balance_date,
  sum(order_balance_amount) as sumamount 
  from 
  label_order s
  where s.MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by s.asc_code,s.vin,tran_date(order_balance_date),primary_classification
  ) s where sumamount>=5000 and sumamount<10000
  group by substr(order_balance_date,1,7),asc_code,primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.34(5000-9999元事故客单价_二级)
def kpi_b_level_kpi_4_34(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_level partition(mon_p='${mon_p}',kpi='kpi_4_34')
  select primary_classification,
  second_level_classification,
  substr(order_balance_date,1,7) as mon,
  sum(sumamount)/count(distinct vin,order_balance_date) as num,
  asc_code
  from (
  select s.asc_code,primary_classification,second_level_classification,
  s.vin,
  tran_date(order_balance_date) as order_balance_date,
  sum(order_balance_amount) as sumamount 
  from 
  label_order s
  where s.MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by s.asc_code,s.vin,tran_date(order_balance_date),primary_classification,
  second_level_classification
  ) s where sumamount>=5000 and sumamount<10000
  group by substr(order_balance_date,1,7),asc_code,primary_classification,
  second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.34(5000-9999元事故客单价_车龄)
def kpi_agetype_kpi_4_34(begin_day, end_day):
    line = """  insert overwrite table  kpi_agetype partition(mon_p='${mon_p}',kpi='kpi_4_34')
  select age_type,substr(order_balance_date,1,7) as mon,
  sum(sumamount)/count(distinct vin,order_balance_date) as num,
  asc_code
  from (
  select s.asc_code,age_type,
  s.vin,
  tran_date(order_balance_date) as order_balance_date,
  sum(order_balance_amount) as sumamount 
  from 
  label_order s
  where s.MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by s.asc_code,s.vin,tran_date(order_balance_date),age_type
  ) s where sumamount>=5000 and sumamount<10000
  group by substr(order_balance_date,1,7),asc_code,age_type"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.34(5000-9999元事故客单价_保内外)
def kpi_bnbw_kpi_4_34(begin_day, end_day):
    line = """  insert overwrite table  kpi_bnbw partition(mon_p='${mon_p}',kpi='kpi_4_34')
  select bn,substr(order_balance_date,1,7) as mon,
  sum(sumamount)/count(distinct vin,order_balance_date) as num,
  asc_code
  from (
  select s.asc_code,bn,
  s.vin,
  tran_date(order_balance_date) as order_balance_date,
  sum(order_balance_amount) as sumamount 
  from 
  label_order s
  where s.MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by s.asc_code,s.vin,tran_date(order_balance_date),bn
  ) s where sumamount>=5000 and sumamount<10000
  group by substr(order_balance_date,1,7),asc_code,bn"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.34(5000-9999元事故客单价_一级_车龄)
def kpi_a_agetype_kpi_4_34(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_agetype partition(mon_p='${mon_p}',kpi='kpi_4_34')
  select primary_classification,
  age_type,
  substr(order_balance_date,1,7) as mon,
  sum(sumamount)/count(distinct vin,order_balance_date) as num,
  asc_code
  from (
  select s.asc_code,age_type,primary_classification,
  s.vin,
  tran_date(order_balance_date) as order_balance_date,
  sum(order_balance_amount) as sumamount 
  from 
  label_order s
  where s.MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by s.asc_code,s.vin,tran_date(order_balance_date),age_type,primary_classification
  ) s where sumamount>=5000 and sumamount<10000
  group by substr(order_balance_date,1,7),asc_code,age_type,primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.34(5000-9999元事故客单价_一级_保内外)
def kpi_a_bnbw_kpi_4_34(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_bnbw partition(mon_p='${mon_p}',kpi='kpi_4_34')
  select primary_classification,
  bn,
  substr(order_balance_date,1,7) as mon,
  sum(sumamount)/count(distinct vin,order_balance_date) as num,
  asc_code
  from (
  select s.asc_code,bn,primary_classification,
  s.vin,
  tran_date(order_balance_date) as order_balance_date,
  sum(order_balance_amount) as sumamount 
  from 
  label_order s
  where s.MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by s.asc_code,s.vin,tran_date(order_balance_date),bn,primary_classification
  ) s where sumamount>=5000 and sumamount<10000
  group by substr(order_balance_date,1,7),asc_code,bn,primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.34(5000-9999元事故客单价_二级_车龄)
def kpi_b_agetype_kpi_4_34(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_agetype partition(mon_p='${mon_p}',kpi='kpi_4_34')
  select primary_classification,
  second_level_classification,
  age_type,
  substr(order_balance_date,1,7) as mon,
  sum(sumamount)/count(distinct vin,order_balance_date) as num,
  asc_code
  from (
  select s.asc_code,age_type,primary_classification,
  second_level_classification,
  s.vin,
  tran_date(order_balance_date) as order_balance_date,
  sum(order_balance_amount) as sumamount 
  from 
  label_order s
  where s.MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by s.asc_code,s.vin,tran_date(order_balance_date),age_type,
  primary_classification,second_level_classification
  ) s where sumamount>=5000 and sumamount<10000
  group by substr(order_balance_date,1,7),asc_code,age_type,
  primary_classification,second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.34(5000-9999元事故客单价_二级_保内外)
def kpi_b_bnbw_kpi_4_34(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_bnbw partition(mon_p='${mon_p}',kpi='kpi_4_34')
  select primary_classification,
  second_level_classification,
  bn,
  substr(order_balance_date,1,7) as mon,
  sum(sumamount)/count(distinct vin,order_balance_date) as num,
  asc_code
  from (
  select s.asc_code,bn,primary_classification,
  second_level_classification,
  s.vin,
  tran_date(order_balance_date) as order_balance_date,
  sum(order_balance_amount) as sumamount 
  from 
  label_order s
  where s.MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by s.asc_code,s.vin,tran_date(order_balance_date),bn,
  primary_classification,second_level_classification
  ) s where sumamount>=5000 and sumamount<10000
  group by substr(order_balance_date,1,7),asc_code,bn,
  primary_classification,second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.35(10000元以上事故客单价_total)
def kpi_total_kpi_4_35(begin_day, end_day):
    line = """  insert overwrite table  kpi_total partition(mon_p='${mon_p}',kpi='kpi_4_35')
  select substr(order_balance_date,1,7) as mon,
  sum(sumamount)/count(distinct vin,order_balance_date) as num,
  asc_code
  from (
  select s.asc_code,
  s.vin,
  tran_date(order_balance_date) as order_balance_date,
  sum(order_balance_amount) as sumamount 
  from 
  label_order s
  where s.MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by s.asc_code,s.vin,tran_date(order_balance_date)
  ) s where sumamount>=10000
  group by substr(order_balance_date,1,7),asc_code"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.35(10000元以上事故客单价_一级)
def kpi_a_level_kpi_4_35(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_level partition(mon_p='${mon_p}',kpi='kpi_4_35')
  select primary_classification,substr(order_balance_date,1,7) as mon,
  sum(sumamount)/count(distinct vin,order_balance_date) as num,
  asc_code
  from (
  select s.asc_code,primary_classification,
  s.vin,
  tran_date(order_balance_date) as order_balance_date,
  sum(order_balance_amount) as sumamount 
  from 
  label_order s
  where s.MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by s.asc_code,s.vin,tran_date(order_balance_date),primary_classification
  ) s where sumamount>=10000
  group by substr(order_balance_date,1,7),asc_code,primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.35(10000元以上事故客单价_二级)
def kpi_b_level_kpi_4_35(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_level partition(mon_p='${mon_p}',kpi='kpi_4_35')
  select primary_classification,
  second_level_classification,
  substr(order_balance_date,1,7) as mon,
  sum(sumamount)/count(distinct vin,order_balance_date) as num,
  asc_code
  from (
  select s.asc_code,primary_classification,second_level_classification,
  s.vin,
  tran_date(order_balance_date) as order_balance_date,
  sum(order_balance_amount) as sumamount 
  from 
  label_order s
  where s.MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by s.asc_code,s.vin,tran_date(order_balance_date),primary_classification,
  second_level_classification
  ) s where sumamount>=10000
  group by substr(order_balance_date,1,7),asc_code,primary_classification,
  second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.35(10000元以上事故客单价_车龄)
def kpi_agetype_kpi_4_35(begin_day, end_day):
    line = """  insert overwrite table  kpi_agetype partition(mon_p='${mon_p}',kpi='kpi_4_35')
  select age_type,substr(order_balance_date,1,7) as mon,
  sum(sumamount)/count(distinct vin,order_balance_date) as num,
  asc_code
  from (
  select s.asc_code,age_type,
  s.vin,
  tran_date(order_balance_date) as order_balance_date,
  sum(order_balance_amount) as sumamount 
  from 
  label_order s
  where s.MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by s.asc_code,s.vin,tran_date(order_balance_date),age_type
  ) s where sumamount>=10000
  group by substr(order_balance_date,1,7),asc_code,age_type"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.35(10000元以上事故客单价_保内外)
def kpi_bnbw_kpi_4_35(begin_day, end_day):
    line = """  insert overwrite table  kpi_bnbw partition(mon_p='${mon_p}',kpi='kpi_4_35')
  select bn,substr(order_balance_date,1,7) as mon,
  sum(sumamount)/count(distinct vin,order_balance_date) as num,
  asc_code
  from (
  select s.asc_code,bn,
  s.vin,
  tran_date(order_balance_date) as order_balance_date,
  sum(order_balance_amount) as sumamount 
  from 
  label_order s
  where s.MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by s.asc_code,s.vin,tran_date(order_balance_date),bn
  ) s where sumamount>=10000
  group by substr(order_balance_date,1,7),asc_code,bn"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.35(10000元以上事故客单价_一级_车龄)
def kpi_a_agetype_kpi_4_35(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_agetype partition(mon_p='${mon_p}',kpi='kpi_4_35')
  select primary_classification,
  age_type,
  substr(order_balance_date,1,7) as mon,
  sum(sumamount)/count(distinct vin,order_balance_date) as num,
  asc_code
  from (
  select s.asc_code,age_type,primary_classification,
  s.vin,
  tran_date(order_balance_date) as order_balance_date,
  sum(order_balance_amount) as sumamount 
  from 
  label_order s
  where s.MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by s.asc_code,s.vin,tran_date(order_balance_date),age_type,primary_classification
  ) s where sumamount>=10000
  group by substr(order_balance_date,1,7),asc_code,age_type,primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.35(10000元以上事故客单价_一级_保内外)
def kpi_a_bnbw_kpi_4_35(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_bnbw partition(mon_p='${mon_p}',kpi='kpi_4_35')
  select primary_classification,
  bn,
  substr(order_balance_date,1,7) as mon,
  sum(sumamount)/count(distinct vin,order_balance_date) as num,
  asc_code
  from (
  select s.asc_code,bn,primary_classification,
  s.vin,
  tran_date(order_balance_date) as order_balance_date,
  sum(order_balance_amount) as sumamount 
  from 
  label_order s
  where s.MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by s.asc_code,s.vin,tran_date(order_balance_date),bn,primary_classification
  ) s where sumamount>=10000
  group by substr(order_balance_date,1,7),asc_code,bn,primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.35(10000元以上事故客单价_二级_车龄)
def kpi_b_agetype_kpi_4_35(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_agetype partition(mon_p='${mon_p}',kpi='kpi_4_35')
  select primary_classification,
  second_level_classification,
  age_type,
  substr(order_balance_date,1,7) as mon,
  sum(sumamount)/count(distinct vin,order_balance_date) as num,
  asc_code
  from (
  select s.asc_code,age_type,primary_classification,
  second_level_classification,
  s.vin,
  tran_date(order_balance_date) as order_balance_date,
  sum(order_balance_amount) as sumamount 
  from 
  label_order s
  where s.MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by s.asc_code,s.vin,tran_date(order_balance_date),age_type,
  primary_classification,second_level_classification
  ) s where sumamount>=10000
  group by substr(order_balance_date,1,7),asc_code,age_type,
  primary_classification,second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.35(10000元以上事故客单价_二级_保内外)
def kpi_b_bnbw_kpi_4_35(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_bnbw partition(mon_p='${mon_p}',kpi='kpi_4_35')
  select primary_classification,
  second_level_classification,
  bn,
  substr(order_balance_date,1,7) as mon,
  sum(sumamount)/count(distinct vin,order_balance_date) as num,
  asc_code
  from (
  select s.asc_code,bn,primary_classification,
  second_level_classification,
  s.vin,
  tran_date(order_balance_date) as order_balance_date,
  sum(order_balance_amount) as sumamount 
  from 
  label_order s
  where s.MAINT_TYPE1='事故'
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by s.asc_code,s.vin,tran_date(order_balance_date),bn,
  primary_classification,second_level_classification
  ) s where sumamount>=10000
  group by substr(order_balance_date,1,7),asc_code,bn,
  primary_classification,second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.4(索赔客单价_total)
def kpi_total_kpi_4_4(begin_day, end_day):
    line = """  insert overwrite table  kpi_total partition(mon_p='${mon_p}',kpi='kpi_4_4')
  select substr(tran_date(s.order_balance_date),1,7) as mon,
  sum(t.claim_accepted_amount)/count(distinct s.vin,tran_date(s.order_balance_date)) as num,
  s.asc_code
   from  label_order s,claim_uniq t 
  where s.MAINT_TYPE1<>'删除'
  and s.asc_code=t.asc_code
  and s.claim_order_number=t.claim_order_number
  and s.claim='1' and t.claim_result in('审核同意','33')
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.4(索赔客单价_一级)
def kpi_a_level_kpi_4_4(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_level partition(mon_p='${mon_p}',kpi='kpi_4_4')
  select s.primary_classification,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  sum(t.claim_accepted_amount)/count(distinct s.vin,tran_date(s.order_balance_date)) as num,
  s.asc_code
   from  label_order s,claim_uniq t 
  where s.MAINT_TYPE1<>'删除'
  and s.asc_code=t.asc_code
  and s.claim_order_number=t.claim_order_number
  and s.claim='1' and t.claim_result in('审核同意','33')
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,s.primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.4(索赔客单价_二级)
def kpi_b_level_kpi_4_4(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_level partition(mon_p='${mon_p}',kpi='kpi_4_4')
  select s.primary_classification,
  s.second_level_classification,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  sum(t.claim_accepted_amount)/count(distinct s.vin,tran_date(s.order_balance_date)) as num,
  s.asc_code
   from  label_order s,claim_uniq t 
  where s.MAINT_TYPE1<>'删除'
  and s.asc_code=t.asc_code
  and s.claim_order_number=t.claim_order_number
  and s.claim='1' and t.claim_result in('审核同意','33')
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,s.primary_classification,s.second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.4(索赔客单价_车龄)
def kpi_agetype_kpi_4_4(begin_day, end_day):
    line = """  insert overwrite table  kpi_agetype partition(mon_p='${mon_p}',kpi='kpi_4_4')
  select s.age_type,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  sum(t.claim_accepted_amount)/count(distinct s.vin,tran_date(s.order_balance_date)) as num,
  s.asc_code
   from  label_order s,claim_uniq t 
  where s.MAINT_TYPE1<>'删除'
  and s.asc_code=t.asc_code
  and s.claim_order_number=t.claim_order_number
  and s.claim='1' and t.claim_result in('审核同意','33')
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,s.age_type"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.4(索赔客单价_保内外)
def kpi_bnbw_kpi_4_4(begin_day, end_day):
    line = """  insert overwrite table  kpi_bnbw partition(mon_p='${mon_p}',kpi='kpi_4_4')
  select s.bn,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  sum(t.claim_accepted_amount)/count(distinct s.vin,tran_date(s.order_balance_date)) as num,
  s.asc_code
   from  label_order s,claim_uniq t 
  where s.MAINT_TYPE1<>'删除'
  and s.asc_code=t.asc_code
  and s.claim_order_number=t.claim_order_number
  and s.claim='1' and t.claim_result in('审核同意','33')
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,s.bn"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.4(索赔客单价_一级_车龄)
def kpi_a_agetype_kpi_4_4(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_agetype partition(mon_p='${mon_p}',kpi='kpi_4_4')
  select s.primary_classification,
  s.age_type,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  sum(t.claim_accepted_amount)/count(distinct s.vin,tran_date(s.order_balance_date)) as num,
  s.asc_code
   from  label_order s,claim_uniq t 
  where s.MAINT_TYPE1<>'删除'
  and s.asc_code=t.asc_code
  and s.claim_order_number=t.claim_order_number
  and s.claim='1' and t.claim_result in('审核同意','33')
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,s.age_type,s.primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.4(索赔客单价_一级_保内外)
def kpi_a_bnbw_kpi_4_4(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_bnbw partition(mon_p='${mon_p}',kpi='kpi_4_4')
  select s.primary_classification,
  s.bn,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  sum(t.claim_accepted_amount)/count(distinct s.vin,tran_date(s.order_balance_date)) as num,
  s.asc_code
   from  label_order s,claim_uniq t 
  where s.MAINT_TYPE1<>'删除'
  and s.asc_code=t.asc_code
  and s.claim_order_number=t.claim_order_number
  and s.claim='1' and t.claim_result in('审核同意','33')
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,s.bn,s.primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.4(索赔客单价_二级_车龄)
def kpi_b_agetype_kpi_4_4(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_agetype partition(mon_p='${mon_p}',kpi='kpi_4_4')
  select s.primary_classification,
  s.second_level_classification,
  s.age_type,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  sum(t.claim_accepted_amount)/count(distinct s.vin,tran_date(s.order_balance_date)) as num,
  s.asc_code
   from  label_order s,claim_uniq t 
  where s.MAINT_TYPE1<>'删除'
  and s.asc_code=t.asc_code
  and s.claim_order_number=t.claim_order_number
  and s.claim='1' and t.claim_result in('审核同意','33')
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,s.age_type,s.primary_classification,
  s.second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_4.4(索赔客单价_二级_保内外)
def kpi_b_bnbw_kpi_4_4(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_bnbw partition(mon_p='${mon_p}',kpi='kpi_4_4')
  select s.primary_classification,
  s.second_level_classification,
  s.bn,
  substr(tran_date(s.order_balance_date),1,7) as mon,
  sum(t.claim_accepted_amount)/count(distinct s.vin,tran_date(s.order_balance_date)) as num,
  s.asc_code
   from  label_order s,claim_uniq t 
  where s.MAINT_TYPE1<>'删除'
  and s.asc_code=t.asc_code
  and s.claim_order_number=t.claim_order_number
  and s.claim='1' and t.claim_result in('审核同意','33')
  and substr(tran_date(order_balance_date),1,7)>='${begin_date}'
  and substr(tran_date(order_balance_date),1,7)<='${end_date}'
  group by substr(tran_date(s.order_balance_date),1,7),s.asc_code,s.bn,s.primary_classification,
  s.second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5(单车产值_total)
def kpi_total_kpi_5(begin_day, end_day):
    line = """  insert overwrite table  kpi_total partition(mon_p='${mon_p}',kpi='kpi_5')
  select substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.claim=''
    and (s.first_maintnance='' or (s.first_maintnance='1' and s.order_balance_amount<>0))
    and s.MAINT_TYPE1<>'删除'
   group by substr(t.mon_label,1,7),s.asc_code"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5(单车产值_一级)
def kpi_a_level_kpi_5(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_level partition(mon_p='${mon_p}',kpi='kpi_5')
  select primary_classification,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.claim=''
    and (s.first_maintnance='' or (s.first_maintnance='1' and s.order_balance_amount<>0))
    and s.MAINT_TYPE1<>'删除'
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5(单车产值_二级)
def kpi_b_level_kpi_5(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_level partition(mon_p='${mon_p}',kpi='kpi_5')
  select primary_classification,
  second_level_classification,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.claim=''
    and (s.first_maintnance='' or (s.first_maintnance='1' and s.order_balance_amount<>0))
    and s.MAINT_TYPE1<>'删除'
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification,second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5(单车产值_车龄)
def kpi_agetype_kpi_5(begin_day, end_day):
    line = """  insert overwrite table  kpi_agetype partition(mon_p='${mon_p}',kpi='kpi_5')
  select case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end as age_type,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.claim=''
    and (s.first_maintnance='' or (s.first_maintnance='1' and s.order_balance_amount<>0))
    and s.MAINT_TYPE1<>'删除'
   group by substr(t.mon_label,1,7),s.asc_code,case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5(单车产值_保内外)
def kpi_bnbw_kpi_5(begin_day, end_day):
    line = """  insert overwrite table  kpi_bnbw partition(mon_p='${mon_p}',kpi='kpi_5')
  select case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end as bn,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.claim=''
    and (s.first_maintnance='' or (s.first_maintnance='1' and s.order_balance_amount<>0))
    and s.MAINT_TYPE1<>'删除'
   group by substr(t.mon_label,1,7),s.asc_code,case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5(单车产值_一级_车龄)
def kpi_a_agetype_kpi_5(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_agetype partition(mon_p='${mon_p}',kpi='kpi_5')
  select primary_classification,case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end as age_type,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.claim=''
    and (s.first_maintnance='' or (s.first_maintnance='1' and s.order_balance_amount<>0))
    and s.MAINT_TYPE1<>'删除'
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification,
   case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5(单车产值_一级_保内外)
def kpi_a_bnbw_kpi_5(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_bnbw partition(mon_p='${mon_p}',kpi='kpi_5')
  select primary_classification,
  case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end as bn,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.claim=''
    and (s.first_maintnance='' or (s.first_maintnance='1' and s.order_balance_amount<>0))
    and s.MAINT_TYPE1<>'删除'
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification,
   case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5(单车产值_二级_车龄)
def kpi_b_agetype_kpi_5(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_agetype partition(mon_p='${mon_p}',kpi='kpi_5')
  select primary_classification,second_level_classification,
  case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end as age_type,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.claim=''
    and (s.first_maintnance='' or (s.first_maintnance='1' and s.order_balance_amount<>0))
    and s.MAINT_TYPE1<>'删除'
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification,
   second_level_classification,
   case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5(单车产值_二级_保内外)
def kpi_b_bnbw_kpi_5(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_bnbw partition(mon_p='${mon_p}',kpi='kpi_5')
  select primary_classification,second_level_classification,
  case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end as bn,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.claim=''
    and (s.first_maintnance='' or (s.first_maintnance='1' and s.order_balance_amount<>0))
    and s.MAINT_TYPE1<>'删除'
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification,
   second_level_classification,
   case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.1(单车年保养产值_total)
def kpi_total_kpi_5_1(begin_day, end_day):
    line = """  insert overwrite table  kpi_total partition(mon_p='${mon_p}',kpi='kpi_5_1')
  select substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and (s.first_maintnance='' or (s.first_maintnance='1' and s.order_balance_amount<>0))
    and s.MAINT_TYPE1='保养'
   group by substr(t.mon_label,1,7),s.asc_code"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.1(单车年保养产值_一级)
def kpi_a_level_kpi_5_1(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_level partition(mon_p='${mon_p}',kpi='kpi_5_1')
  select primary_classification,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and (s.first_maintnance='' or (s.first_maintnance='1' and s.order_balance_amount<>0))
    and s.MAINT_TYPE1='保养'
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.1(单车年保养产值_二级)
def kpi_b_level_kpi_5_1(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_level partition(mon_p='${mon_p}',kpi='kpi_5_1')
  select primary_classification,
  second_level_classification,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and (s.first_maintnance='' or (s.first_maintnance='1' and s.order_balance_amount<>0))
    and s.MAINT_TYPE1='保养'
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification,second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.1(单车年保养产值_车龄)
def kpi_agetype_kpi_5_1(begin_day, end_day):
    line = """  insert overwrite table  kpi_agetype partition(mon_p='${mon_p}',kpi='kpi_5_1')
  select case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end as age_type,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and (s.first_maintnance='' or (s.first_maintnance='1' and s.order_balance_amount<>0))
    and s.MAINT_TYPE1='保养'
   group by substr(t.mon_label,1,7),s.asc_code,case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.1(单车年保养产值_保内外)
def kpi_bnbw_kpi_5_1(begin_day, end_day):
    line = """  insert overwrite table  kpi_bnbw partition(mon_p='${mon_p}',kpi='kpi_5_1')
  select case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end as bn,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and (s.first_maintnance='' or (s.first_maintnance='1' and s.order_balance_amount<>0))
    and s.MAINT_TYPE1='保养'
   group by substr(t.mon_label,1,7),s.asc_code,case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.1(单车年保养产值_一级_车龄)
def kpi_a_agetype_kpi_5_1(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_agetype partition(mon_p='${mon_p}',kpi='kpi_5_1')
  select primary_classification,case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end as age_type,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and (s.first_maintnance='' or (s.first_maintnance='1' and s.order_balance_amount<>0))
    and s.MAINT_TYPE1='保养'
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification,
   case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.1(单车年保养产值_一级_保内外)
def kpi_a_bnbw_kpi_5_1(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_bnbw partition(mon_p='${mon_p}',kpi='kpi_5_1')
  select primary_classification,
  case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end as bn,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and (s.first_maintnance='' or (s.first_maintnance='1' and s.order_balance_amount<>0))
    and s.MAINT_TYPE1='保养'
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification,
   case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.1(单车年保养产值_二级_车龄)
def kpi_b_agetype_kpi_5_1(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_agetype partition(mon_p='${mon_p}',kpi='kpi_5_1')
  select primary_classification,second_level_classification,
  case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end as age_type,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and (s.first_maintnance='' or (s.first_maintnance='1' and s.order_balance_amount<>0))
    and s.MAINT_TYPE1='保养'
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification,
   second_level_classification,
   case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.1(单车年保养产值_二级_保内外)
def kpi_b_bnbw_kpi_5_1(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_bnbw partition(mon_p='${mon_p}',kpi='kpi_5_1')
  select primary_classification,second_level_classification,
  case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end as bn,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and (s.first_maintnance='' or (s.first_maintnance='1' and s.order_balance_amount<>0))
    and s.MAINT_TYPE1='保养'
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification,
   second_level_classification,
   case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.11(单车年保养产值_平价机油_total)
def kpi_total_kpi_5_11(begin_day, end_day):
    line = """  insert overwrite table  kpi_total partition(mon_p='${mon_p}',kpi='kpi_5_11')
  select substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,engine_oil y where x.part_number=y.part_num and y.type='低端') m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and (s.first_maintnance='' or (s.first_maintnance='1' and s.order_balance_amount<>0))
    and s.MAINT_TYPE1='保养'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.11(单车年保养产值_平价机油_一级)
def kpi_a_level_kpi_5_11(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_level partition(mon_p='${mon_p}',kpi='kpi_5_11')
  select primary_classification,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,engine_oil y where x.part_number=y.part_num and y.type='低端') m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and (s.first_maintnance='' or (s.first_maintnance='1' and s.order_balance_amount<>0))
    and s.MAINT_TYPE1='保养'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.11(单车年保养产值_平价机油_二级)
def kpi_b_level_kpi_5_11(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_level partition(mon_p='${mon_p}',kpi='kpi_5_11')
  select primary_classification,
  second_level_classification,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,engine_oil y where x.part_number=y.part_num and y.type='低端') m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and (s.first_maintnance='' or (s.first_maintnance='1' and s.order_balance_amount<>0))
    and s.MAINT_TYPE1='保养'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification,second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.11(单车年保养产值_平价机油_车龄)
def kpi_agetype_kpi_5_11(begin_day, end_day):
    line = """  insert overwrite table  kpi_agetype partition(mon_p='${mon_p}',kpi='kpi_5_11')
  select case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end as age_type,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,engine_oil y where x.part_number=y.part_num and y.type='低端') m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and (s.first_maintnance='' or (s.first_maintnance='1' and s.order_balance_amount<>0))
    and s.MAINT_TYPE1='保养'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.11(单车年保养产值_平价机油_保内外)
def kpi_bnbw_kpi_5_11(begin_day, end_day):
    line = """  insert overwrite table  kpi_bnbw partition(mon_p='${mon_p}',kpi='kpi_5_11')
  select case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end as bn,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,engine_oil y where x.part_number=y.part_num and y.type='低端') m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and (s.first_maintnance='' or (s.first_maintnance='1' and s.order_balance_amount<>0))
    and s.MAINT_TYPE1='保养'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.11(单车年保养产值_平价机油_一级_车龄)
def kpi_a_agetype_kpi_5_11(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_agetype partition(mon_p='${mon_p}',kpi='kpi_5_11')
  select primary_classification,case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end as age_type,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,engine_oil y where x.part_number=y.part_num and y.type='低端') m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and (s.first_maintnance='' or (s.first_maintnance='1' and s.order_balance_amount<>0))
    and s.MAINT_TYPE1='保养'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification,
   case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.11(单车年保养产值_平价机油_一级_保内外)
def kpi_a_bnbw_kpi_5_11(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_bnbw partition(mon_p='${mon_p}',kpi='kpi_5_11')
  select primary_classification,
  case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end as bn,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,engine_oil y where x.part_number=y.part_num and y.type='低端') m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and (s.first_maintnance='' or (s.first_maintnance='1' and s.order_balance_amount<>0))
    and s.MAINT_TYPE1='保养'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification,
   case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.11(单车年保养产值_平价机油_二级_车龄)
def kpi_b_agetype_kpi_5_11(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_agetype partition(mon_p='${mon_p}',kpi='kpi_5_11')
  select primary_classification,second_level_classification,
  case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end as age_type,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,engine_oil y where x.part_number=y.part_num and y.type='低端') m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and (s.first_maintnance='' or (s.first_maintnance='1' and s.order_balance_amount<>0))
    and s.MAINT_TYPE1='保养'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification,
   second_level_classification,
   case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.11(单车年保养产值_平价机油_二级_保内外)
def kpi_b_bnbw_kpi_5_11(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_bnbw partition(mon_p='${mon_p}',kpi='kpi_5_11')
  select primary_classification,second_level_classification,
  case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end as bn,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,engine_oil y where x.part_number=y.part_num and y.type='低端') m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and (s.first_maintnance='' or (s.first_maintnance='1' and s.order_balance_amount<>0))
    and s.MAINT_TYPE1='保养'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification,
   second_level_classification,
   case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.12(单车年保养产值_中端机油_total)
def kpi_total_kpi_5_12(begin_day, end_day):
    line = """  insert overwrite table  kpi_total partition(mon_p='${mon_p}',kpi='kpi_5_12')
  select substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,engine_oil y where x.part_number=y.part_num and y.type='中端') m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and (s.first_maintnance='' or (s.first_maintnance='1' and s.order_balance_amount<>0))
    and s.MAINT_TYPE1='保养'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.12(单车年保养产值_中端机油_一级)
def kpi_a_level_kpi_5_12(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_level partition(mon_p='${mon_p}',kpi='kpi_5_12')
  select primary_classification,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,engine_oil y where x.part_number=y.part_num and y.type='中端') m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and (s.first_maintnance='' or (s.first_maintnance='1' and s.order_balance_amount<>0))
    and s.MAINT_TYPE1='保养'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.12(单车年保养产值_中端机油_二级)
def kpi_b_level_kpi_5_12(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_level partition(mon_p='${mon_p}',kpi='kpi_5_12')
  select primary_classification,
  second_level_classification,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,engine_oil y where x.part_number=y.part_num and y.type='中端') m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and (s.first_maintnance='' or (s.first_maintnance='1' and s.order_balance_amount<>0))
    and s.MAINT_TYPE1='保养'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification,second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.12(单车年保养产值_中端机油_车龄)
def kpi_agetype_kpi_5_12(begin_day, end_day):
    line = """  insert overwrite table  kpi_agetype partition(mon_p='${mon_p}',kpi='kpi_5_12')
  select case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end as age_type,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,engine_oil y where x.part_number=y.part_num and y.type='中端') m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and (s.first_maintnance='' or (s.first_maintnance='1' and s.order_balance_amount<>0))
    and s.MAINT_TYPE1='保养'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.12(单车年保养产值_中端机油_保内外)
def kpi_bnbw_kpi_5_12(begin_day, end_day):
    line = """  insert overwrite table  kpi_bnbw partition(mon_p='${mon_p}',kpi='kpi_5_12')
  select case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end as bn,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,engine_oil y where x.part_number=y.part_num and y.type='中端') m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and (s.first_maintnance='' or (s.first_maintnance='1' and s.order_balance_amount<>0))
    and s.MAINT_TYPE1='保养'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.12(单车年保养产值_中端机油_一级_车龄)
def kpi_a_agetype_kpi_5_12(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_agetype partition(mon_p='${mon_p}',kpi='kpi_5_12')
  select primary_classification,case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end as age_type,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,engine_oil y where x.part_number=y.part_num and y.type='中端') m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and (s.first_maintnance='' or (s.first_maintnance='1' and s.order_balance_amount<>0))
    and s.MAINT_TYPE1='保养'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification,
   case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.12(单车年保养产值_中端机油_一级_保内外)
def kpi_a_bnbw_kpi_5_12(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_bnbw partition(mon_p='${mon_p}',kpi='kpi_5_12')
  select primary_classification,
  case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end as bn,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,engine_oil y where x.part_number=y.part_num and y.type='中端') m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and (s.first_maintnance='' or (s.first_maintnance='1' and s.order_balance_amount<>0))
    and s.MAINT_TYPE1='保养'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification,
   case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.12(单车年保养产值_中端机油_二级_车龄)
def kpi_b_agetype_kpi_5_12(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_agetype partition(mon_p='${mon_p}',kpi='kpi_5_12')
  select primary_classification,second_level_classification,
  case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end as age_type,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,engine_oil y where x.part_number=y.part_num and y.type='中端') m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and (s.first_maintnance='' or (s.first_maintnance='1' and s.order_balance_amount<>0))
    and s.MAINT_TYPE1='保养'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification,
   second_level_classification,
   case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.12(单车年保养产值_中端机油_二级_保内外)
def kpi_b_bnbw_kpi_5_12(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_bnbw partition(mon_p='${mon_p}',kpi='kpi_5_12')
  select primary_classification,second_level_classification,
  case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end as bn,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,engine_oil y where x.part_number=y.part_num and y.type='中端') m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and (s.first_maintnance='' or (s.first_maintnance='1' and s.order_balance_amount<>0))
    and s.MAINT_TYPE1='保养'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification,
   second_level_classification,
   case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.13(单车年保养产值_高端机油_total)
def kpi_total_kpi_5_13(begin_day, end_day):
    line = """  insert overwrite table  kpi_total partition(mon_p='${mon_p}',kpi='kpi_5_13')
  select substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,engine_oil y where x.part_number=y.part_num and y.type='高端') m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and (s.first_maintnance='' or (s.first_maintnance='1' and s.order_balance_amount<>0))
    and s.MAINT_TYPE1='保养'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.13(单车年保养产值_高端机油_一级)
def kpi_a_level_kpi_5_13(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_level partition(mon_p='${mon_p}',kpi='kpi_5_13')
  select primary_classification,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,engine_oil y where x.part_number=y.part_num and y.type='高端') m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and (s.first_maintnance='' or (s.first_maintnance='1' and s.order_balance_amount<>0))
    and s.MAINT_TYPE1='保养'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.13(单车年保养产值_高端机油_二级)
def kpi_b_level_kpi_5_13(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_level partition(mon_p='${mon_p}',kpi='kpi_5_13')
  select primary_classification,
  second_level_classification,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,engine_oil y where x.part_number=y.part_num and y.type='高端') m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and (s.first_maintnance='' or (s.first_maintnance='1' and s.order_balance_amount<>0))
    and s.MAINT_TYPE1='保养'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification,second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.13(单车年保养产值_高端机油_车龄)
def kpi_agetype_kpi_5_13(begin_day, end_day):
    line = """  insert overwrite table  kpi_agetype partition(mon_p='${mon_p}',kpi='kpi_5_13')
  select case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end as age_type,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,engine_oil y where x.part_number=y.part_num and y.type='高端') m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and (s.first_maintnance='' or (s.first_maintnance='1' and s.order_balance_amount<>0))
    and s.MAINT_TYPE1='保养'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.13(单车年保养产值_高端机油_保内外)
def kpi_bnbw_kpi_5_13(begin_day, end_day):
    line = """  insert overwrite table  kpi_bnbw partition(mon_p='${mon_p}',kpi='kpi_5_13')
  select case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end as bn,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,engine_oil y where x.part_number=y.part_num and y.type='高端') m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and (s.first_maintnance='' or (s.first_maintnance='1' and s.order_balance_amount<>0))
    and s.MAINT_TYPE1='保养'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.13(单车年保养产值_高端机油_一级_车龄)
def kpi_a_agetype_kpi_5_13(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_agetype partition(mon_p='${mon_p}',kpi='kpi_5_13')
  select primary_classification,case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end as age_type,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,engine_oil y where x.part_number=y.part_num and y.type='高端') m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and (s.first_maintnance='' or (s.first_maintnance='1' and s.order_balance_amount<>0))
    and s.MAINT_TYPE1='保养'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification,
   case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.13(单车年保养产值_高端机油_一级_保内外)
def kpi_a_bnbw_kpi_5_13(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_bnbw partition(mon_p='${mon_p}',kpi='kpi_5_13')
  select primary_classification,
  case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end as bn,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,engine_oil y where x.part_number=y.part_num and y.type='高端') m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and (s.first_maintnance='' or (s.first_maintnance='1' and s.order_balance_amount<>0))
    and s.MAINT_TYPE1='保养'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification,
   case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.13(单车年保养产值_高端机油_二级_车龄)
def kpi_b_agetype_kpi_5_13(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_agetype partition(mon_p='${mon_p}',kpi='kpi_5_13')
  select primary_classification,second_level_classification,
  case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end as age_type,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,engine_oil y where x.part_number=y.part_num and y.type='高端') m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and (s.first_maintnance='' or (s.first_maintnance='1' and s.order_balance_amount<>0))
    and s.MAINT_TYPE1='保养'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification,
   second_level_classification,
   case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.13(单车年保养产值_高端机油_二级_保内外)
def kpi_b_bnbw_kpi_5_13(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_bnbw partition(mon_p='${mon_p}',kpi='kpi_5_13')
  select primary_classification,second_level_classification,
  case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end as bn,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,engine_oil y where x.part_number=y.part_num and y.type='高端') m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and (s.first_maintnance='' or (s.first_maintnance='1' and s.order_balance_amount<>0))
    and s.MAINT_TYPE1='保养'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification,
   second_level_classification,
   case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.2(单车年维修产值_total)
def kpi_total_kpi_5_2(begin_day, end_day):
    line = """  insert overwrite table  kpi_total partition(mon_p='${mon_p}',kpi='kpi_5_2')
  select substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1='维修'
   group by substr(t.mon_label,1,7),s.asc_code"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.2(单车年维修产值_一级)
def kpi_a_level_kpi_5_2(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_level partition(mon_p='${mon_p}',kpi='kpi_5_2')
  select primary_classification,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1='维修'
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.2(单车年维修产值_二级)
def kpi_b_level_kpi_5_2(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_level partition(mon_p='${mon_p}',kpi='kpi_5_2')
  select primary_classification,
  second_level_classification,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1='维修'
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification,second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.2(单车年维修产值_车龄)
def kpi_agetype_kpi_5_2(begin_day, end_day):
    line = """  insert overwrite table  kpi_agetype partition(mon_p='${mon_p}',kpi='kpi_5_2')
  select case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end as age_type,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1='维修'
   group by substr(t.mon_label,1,7),s.asc_code,case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.2(单车年维修产值_保内外)
def kpi_bnbw_kpi_5_2(begin_day, end_day):
    line = """  insert overwrite table  kpi_bnbw partition(mon_p='${mon_p}',kpi='kpi_5_2')
  select case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end as bn,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1='维修'
   group by substr(t.mon_label,1,7),s.asc_code,case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.2(单车年维修产值_一级_车龄)
def kpi_a_agetype_kpi_5_2(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_agetype partition(mon_p='${mon_p}',kpi='kpi_5_2')
  select primary_classification,case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end as age_type,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1='维修'
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification,
   case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.2(单车年维修产值_一级_保内外)
def kpi_a_bnbw_kpi_5_2(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_bnbw partition(mon_p='${mon_p}',kpi='kpi_5_2')
  select primary_classification,
  case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end as bn,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1='维修'
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification,
   case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.2(单车年维修产值_二级_车龄)
def kpi_b_agetype_kpi_5_2(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_agetype partition(mon_p='${mon_p}',kpi='kpi_5_2')
  select primary_classification,second_level_classification,
  case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end as age_type,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1='维修'
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification,
   second_level_classification,
   case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.2(单车年维修产值_二级_保内外)
def kpi_b_bnbw_kpi_5_2(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_bnbw partition(mon_p='${mon_p}',kpi='kpi_5_2')
  select primary_classification,second_level_classification,
  case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end as bn,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1='维修'
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification,
   second_level_classification,
   case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.3(单车年事故产值_total)
def kpi_total_kpi_5_3(begin_day, end_day):
    line = """  insert overwrite table  kpi_total partition(mon_p='${mon_p}',kpi='kpi_5_3')
  select substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1='事故'
   group by substr(t.mon_label,1,7),s.asc_code"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.3(单车年事故产值_一级)
def kpi_a_level_kpi_5_3(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_level partition(mon_p='${mon_p}',kpi='kpi_5_3')
  select primary_classification,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1='事故'
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.3(单车年事故产值_二级)
def kpi_b_level_kpi_5_3(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_level partition(mon_p='${mon_p}',kpi='kpi_5_3')
  select primary_classification,
  second_level_classification,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1='事故'
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification,second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.3(单车年事故产值_车龄)
def kpi_agetype_kpi_5_3(begin_day, end_day):
    line = """  insert overwrite table  kpi_agetype partition(mon_p='${mon_p}',kpi='kpi_5_3')
  select case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end as age_type,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1='事故'
   group by substr(t.mon_label,1,7),s.asc_code,case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.3(单车年事故产值_保内外)
def kpi_bnbw_kpi_5_3(begin_day, end_day):
    line = """  insert overwrite table  kpi_bnbw partition(mon_p='${mon_p}',kpi='kpi_5_3')
  select case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end as bn,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1='事故'
   group by substr(t.mon_label,1,7),s.asc_code,case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.3(单车年事故产值_一级_车龄)
def kpi_a_agetype_kpi_5_3(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_agetype partition(mon_p='${mon_p}',kpi='kpi_5_3')
  select primary_classification,case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end as age_type,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1='事故'
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification,
   case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.3(单车年事故产值_一级_保内外)
def kpi_a_bnbw_kpi_5_3(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_bnbw partition(mon_p='${mon_p}',kpi='kpi_5_3')
  select primary_classification,
  case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end as bn,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1='事故'
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification,
   case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.3(单车年事故产值_二级_车龄)
def kpi_b_agetype_kpi_5_3(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_agetype partition(mon_p='${mon_p}',kpi='kpi_5_3')
  select primary_classification,second_level_classification,
  case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end as age_type,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1='事故'
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification,
   second_level_classification,
   case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.3(单车年事故产值_二级_保内外)
def kpi_b_bnbw_kpi_5_3(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_bnbw partition(mon_p='${mon_p}',kpi='kpi_5_3')
  select primary_classification,second_level_classification,
  case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end as bn,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s 
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1='事故'
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification,
   second_level_classification,
   case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.4(单车年索赔产值_total)
def kpi_total_kpi_5_4(begin_day, end_day):
    line = """  insert overwrite table  kpi_total partition(mon_p='${mon_p}',kpi='kpi_5_4')
  select substr(t.mon_label,1,7) as mon,
  sum(m.claim_accepted_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s ,claim_uniq m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.claim='1' and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.claim_order_number=m.claim_order_number
    and m.claim_result in('审核同意','33')
   group by substr(t.mon_label,1,7),s.asc_code"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.4(单车年索赔产值_一级)
def kpi_a_level_kpi_5_4(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_level partition(mon_p='${mon_p}',kpi='kpi_5_4')
  select primary_classification,
  substr(t.mon_label,1,7) as mon,
  sum(m.claim_accepted_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s ,claim_uniq m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.claim='1' and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.claim_order_number=m.claim_order_number
    and m.claim_result in('审核同意','33')
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.4(单车年索赔产值_二级)
def kpi_b_level_kpi_5_4(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_level partition(mon_p='${mon_p}',kpi='kpi_5_4')
  select primary_classification,
  second_level_classification,
  substr(t.mon_label,1,7) as mon,
  sum(m.claim_accepted_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s ,claim_uniq m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.claim='1' and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.claim_order_number=m.claim_order_number
    and m.claim_result in('审核同意','33')
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification,second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.4(单车年索赔产值_车龄)
def kpi_agetype_kpi_5_4(begin_day, end_day):
    line = """  insert overwrite table  kpi_agetype partition(mon_p='${mon_p}',kpi='kpi_5_4')
  select case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end as age_type,
  substr(t.mon_label,1,7) as mon,
  sum(m.claim_accepted_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s ,claim_uniq m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.claim='1' and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.claim_order_number=m.claim_order_number
    and m.claim_result in('审核同意','33')
   group by substr(t.mon_label,1,7),s.asc_code,case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.4(单车年索赔产值_保内外)
def kpi_bnbw_kpi_5_4(begin_day, end_day):
    line = """  insert overwrite table  kpi_bnbw partition(mon_p='${mon_p}',kpi='kpi_5_4')
  select case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end as bn,
  substr(t.mon_label,1,7) as mon,
  sum(m.claim_accepted_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s ,claim_uniq m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.claim='1' and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.claim_order_number=m.claim_order_number
    and m.claim_result in('审核同意','33')
   group by substr(t.mon_label,1,7),s.asc_code,case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.4(单车年索赔产值_一级_车龄)
def kpi_a_agetype_kpi_5_4(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_agetype partition(mon_p='${mon_p}',kpi='kpi_5_4')
  select primary_classification,case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end as age_type,
  substr(t.mon_label,1,7) as mon,
  sum(m.claim_accepted_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s ,claim_uniq m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.claim='1' and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.claim_order_number=m.claim_order_number
    and m.claim_result in('审核同意','33')
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification,
   case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.4(单车年索赔产值_一级_保内外)
def kpi_a_bnbw_kpi_5_4(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_bnbw partition(mon_p='${mon_p}',kpi='kpi_5_4')
  select primary_classification,
  case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end as bn,
  substr(t.mon_label,1,7) as mon,
  sum(m.claim_accepted_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s ,claim_uniq m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.claim='1' and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.claim_order_number=m.claim_order_number
    and m.claim_result in('审核同意','33')
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification,
   case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.4(单车年索赔产值_二级_车龄)
def kpi_b_agetype_kpi_5_4(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_agetype partition(mon_p='${mon_p}',kpi='kpi_5_4')
  select primary_classification,second_level_classification,
  case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end as age_type,
  substr(t.mon_label,1,7) as mon,
  sum(m.claim_accepted_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s ,claim_uniq m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.claim='1' and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.claim_order_number=m.claim_order_number
    and m.claim_result in('审核同意','33')
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification,
   second_level_classification,
   case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.4(单车年索赔产值_二级_保内外)
def kpi_b_bnbw_kpi_5_4(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_bnbw partition(mon_p='${mon_p}',kpi='kpi_5_4')
  select primary_classification,second_level_classification,
  case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end as bn,
  substr(t.mon_label,1,7) as mon,
  sum(m.claim_accepted_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s ,claim_uniq m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.claim='1' and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.claim_order_number=m.claim_order_number
    and m.claim_result in('审核同意','33')
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification,
   second_level_classification,
   case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.5(单车年养护品工单产值_total)
def kpi_total_kpi_5_5(begin_day, end_day):
    line = """  insert overwrite table  kpi_total partition(mon_p='${mon_p}',kpi='kpi_5_5')
  select substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,maintnance y where x.part_number=y.part_num  ) m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.5(单车年养护品工单产值_一级)
def kpi_a_level_kpi_5_5(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_level partition(mon_p='${mon_p}',kpi='kpi_5_5')
  select primary_classification,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,maintnance y where x.part_number=y.part_num  ) m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.5(单车年养护品工单产值_二级)
def kpi_b_level_kpi_5_5(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_level partition(mon_p='${mon_p}',kpi='kpi_5_5')
  select primary_classification,
  second_level_classification,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,maintnance y where x.part_number=y.part_num  ) m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification,second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.5(单车年养护品工单产值_车龄)
def kpi_agetype_kpi_5_5(begin_day, end_day):
    line = """  insert overwrite table  kpi_agetype partition(mon_p='${mon_p}',kpi='kpi_5_5')
  select case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end as age_type,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,maintnance y where x.part_number=y.part_num  ) m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.5(单车年养护品工单产值_保内外)
def kpi_bnbw_kpi_5_5(begin_day, end_day):
    line = """  insert overwrite table  kpi_bnbw partition(mon_p='${mon_p}',kpi='kpi_5_5')
  select case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end as bn,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,maintnance y where x.part_number=y.part_num  ) m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.5(单车年养护品工单产值_一级_车龄)
def kpi_a_agetype_kpi_5_5(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_agetype partition(mon_p='${mon_p}',kpi='kpi_5_5')
  select primary_classification,case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end as age_type,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,maintnance y where x.part_number=y.part_num  ) m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification,
   case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.5(单车年养护品工单产值_一级_保内外)
def kpi_a_bnbw_kpi_5_5(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_bnbw partition(mon_p='${mon_p}',kpi='kpi_5_5')
  select primary_classification,
  case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end as bn,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,maintnance y where x.part_number=y.part_num  ) m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification,
   case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.5(单车年养护品工单产值_二级_车龄)
def kpi_b_agetype_kpi_5_5(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_agetype partition(mon_p='${mon_p}',kpi='kpi_5_5')
  select primary_classification,second_level_classification,
  case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end as age_type,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,maintnance y where x.part_number=y.part_num  ) m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification,
   second_level_classification,
   case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.5(单车年养护品工单产值_二级_保内外)
def kpi_b_bnbw_kpi_5_5(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_bnbw partition(mon_p='${mon_p}',kpi='kpi_5_5')
  select primary_classification,second_level_classification,
  case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end as bn,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,maintnance y where x.part_number=y.part_num  ) m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification,
   second_level_classification,
   case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.51(养护品产值_润滑养护_total)
def kpi_total_kpi_5_51(begin_day, end_day):
    line = """  insert overwrite table  kpi_total partition(mon_p='${mon_p}',kpi='kpi_5_51')
  select substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,maintnance y where x.part_number=y.part_num and y.type='润滑养护') m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.51(养护品产值_润滑养护_一级)
def kpi_a_level_kpi_5_51(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_level partition(mon_p='${mon_p}',kpi='kpi_5_51')
  select primary_classification,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,maintnance y where x.part_number=y.part_num  and y.type='润滑养护' ) m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.51(养护品产值_润滑养护_二级)
def kpi_b_level_kpi_5_51(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_level partition(mon_p='${mon_p}',kpi='kpi_5_51')
  select primary_classification,
  second_level_classification,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,maintnance y where x.part_number=y.part_num  and y.type='润滑养护'  ) m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification,second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.51(养护品产值_润滑养护_车龄)
def kpi_agetype_kpi_5_51(begin_day, end_day):
    line = """  insert overwrite table  kpi_agetype partition(mon_p='${mon_p}',kpi='kpi_5_51')
  select case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end as age_type,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,maintnance y where x.part_number=y.part_num  and y.type='润滑养护'  ) m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.51(养护品产值_润滑养护_保内外)
def kpi_bnbw_kpi_5_51(begin_day, end_day):
    line = """  insert overwrite table  kpi_bnbw partition(mon_p='${mon_p}',kpi='kpi_5_51')
  select case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end as bn,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,maintnance y where x.part_number=y.part_num  and y.type='润滑养护'  ) m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.51(养护品产值_润滑养护_一级_车龄)
def kpi_a_agetype_kpi_5_51(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_agetype partition(mon_p='${mon_p}',kpi='kpi_5_51')
  select primary_classification,case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end as age_type,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,maintnance y where x.part_number=y.part_num  and y.type='润滑养护'  ) m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification,
   case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.51(养护品产值_润滑养护_一级_保内外)
def kpi_a_bnbw_kpi_5_51(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_bnbw partition(mon_p='${mon_p}',kpi='kpi_5_51')
  select primary_classification,
  case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end as bn,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,maintnance y where x.part_number=y.part_num  and y.type='润滑养护'  ) m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification,
   case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.51(养护品产值_润滑养护_二级_车龄)
def kpi_b_agetype_kpi_5_51(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_agetype partition(mon_p='${mon_p}',kpi='kpi_5_51')
  select primary_classification,second_level_classification,
  case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end as age_type,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,maintnance y where x.part_number=y.part_num  and y.type='润滑养护'  ) m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification,
   second_level_classification,
   case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.51(养护品产值_润滑养护_二级_保内外)
def kpi_b_bnbw_kpi_5_51(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_bnbw partition(mon_p='${mon_p}',kpi='kpi_5_51')
  select primary_classification,second_level_classification,
  case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end as bn,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,maintnance y where x.part_number=y.part_num  and y.type='润滑养护'  ) m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification,
   second_level_classification,
   case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.52(养护品产值_燃油养护_total)
def kpi_total_kpi_5_52(begin_day, end_day):
    line = """  insert overwrite table  kpi_total partition(mon_p='${mon_p}',kpi='kpi_5_52')
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
   group by substr(t.mon_label,1,7),s.asc_code"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.52(养护品产值_燃油养护_一级)
def kpi_a_level_kpi_5_52(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_level partition(mon_p='${mon_p}',kpi='kpi_5_52')
  select primary_classification,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,maintnance y where x.part_number=y.part_num  and y.type='燃油养护' ) m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.52(养护品产值_燃油养护_二级)
def kpi_b_level_kpi_5_52(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_level partition(mon_p='${mon_p}',kpi='kpi_5_52')
  select primary_classification,
  second_level_classification,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,maintnance y where x.part_number=y.part_num  and y.type='燃油养护'  ) m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification,second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.52(养护品产值_燃油养护_车龄)
def kpi_agetype_kpi_5_52(begin_day, end_day):
    line = """  insert overwrite table  kpi_agetype partition(mon_p='${mon_p}',kpi='kpi_5_52')
  select case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end as age_type,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,maintnance y where x.part_number=y.part_num  and y.type='燃油养护'  ) m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.52(养护品产值_燃油养护_保内外)
def kpi_bnbw_kpi_5_52(begin_day, end_day):
    line = """  insert overwrite table  kpi_bnbw partition(mon_p='${mon_p}',kpi='kpi_5_52')
  select case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end as bn,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,maintnance y where x.part_number=y.part_num  and y.type='燃油养护'  ) m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.52(养护品产值_燃油养护_一级_车龄)
def kpi_a_agetype_kpi_5_52(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_agetype partition(mon_p='${mon_p}',kpi='kpi_5_52')
  select primary_classification,case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end as age_type,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,maintnance y where x.part_number=y.part_num  and y.type='燃油养护'  ) m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification,
   case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.52(养护品产值_燃油养护_一级_保内外)
def kpi_a_bnbw_kpi_5_52(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_bnbw partition(mon_p='${mon_p}',kpi='kpi_5_52')
  select primary_classification,
  case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end as bn,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,maintnance y where x.part_number=y.part_num  and y.type='燃油养护'  ) m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification,
   case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.52(养护品产值_燃油养护_二级_车龄)
def kpi_b_agetype_kpi_5_52(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_agetype partition(mon_p='${mon_p}',kpi='kpi_5_52')
  select primary_classification,second_level_classification,
  case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end as age_type,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,maintnance y where x.part_number=y.part_num  and y.type='燃油养护'  ) m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification,
   second_level_classification,
   case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.52(养护品产值_燃油养护_二级_保内外)
def kpi_b_bnbw_kpi_5_52(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_bnbw partition(mon_p='${mon_p}',kpi='kpi_5_52')
  select primary_classification,second_level_classification,
  case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end as bn,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,maintnance y where x.part_number=y.part_num  and y.type='燃油养护'  ) m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification,
   second_level_classification,
   case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.53(养护品产值_节气门养护_total)
def kpi_total_kpi_5_53(begin_day, end_day):
    line = """  insert overwrite table  kpi_total partition(mon_p='${mon_p}',kpi='kpi_5_53')
  select substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,maintnance y where x.part_number=y.part_num and y.type='节气门养护') m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.53(养护品产值_节气门养护_一级)
def kpi_a_level_kpi_5_53(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_level partition(mon_p='${mon_p}',kpi='kpi_5_53')
  select primary_classification,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,maintnance y where x.part_number=y.part_num  and y.type='节气门养护' ) m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.53(养护品产值_节气门养护_二级)
def kpi_b_level_kpi_5_53(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_level partition(mon_p='${mon_p}',kpi='kpi_5_53')
  select primary_classification,
  second_level_classification,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,maintnance y where x.part_number=y.part_num  and y.type='节气门养护'  ) m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification,second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.53(养护品产值_节气门养护_车龄)
def kpi_agetype_kpi_5_53(begin_day, end_day):
    line = """  insert overwrite table  kpi_agetype partition(mon_p='${mon_p}',kpi='kpi_5_53')
  select case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end as age_type,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,maintnance y where x.part_number=y.part_num  and y.type='节气门养护'  ) m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.53(养护品产值_节气门养护_保内外)
def kpi_bnbw_kpi_5_53(begin_day, end_day):
    line = """  insert overwrite table  kpi_bnbw partition(mon_p='${mon_p}',kpi='kpi_5_53')
  select case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end as bn,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,maintnance y where x.part_number=y.part_num  and y.type='节气门养护'  ) m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.53(养护品产值_节气门养护_一级_车龄)
def kpi_a_agetype_kpi_5_53(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_agetype partition(mon_p='${mon_p}',kpi='kpi_5_53')
  select primary_classification,case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end as age_type,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,maintnance y where x.part_number=y.part_num  and y.type='节气门养护'  ) m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification,
   case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.53(养护品产值_节气门养护_一级_保内外)
def kpi_a_bnbw_kpi_5_53(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_bnbw partition(mon_p='${mon_p}',kpi='kpi_5_53')
  select primary_classification,
  case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end as bn,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,maintnance y where x.part_number=y.part_num  and y.type='节气门养护'  ) m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification,
   case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.53(养护品产值_节气门养护_二级_车龄)
def kpi_b_agetype_kpi_5_53(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_agetype partition(mon_p='${mon_p}',kpi='kpi_5_53')
  select primary_classification,second_level_classification,
  case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end as age_type,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,maintnance y where x.part_number=y.part_num  and y.type='节气门养护'  ) m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification,
   second_level_classification,
   case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.53(养护品产值_节气门养护_二级_保内外)
def kpi_b_bnbw_kpi_5_53(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_bnbw partition(mon_p='${mon_p}',kpi='kpi_5_53')
  select primary_classification,second_level_classification,
  case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end as bn,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,maintnance y where x.part_number=y.part_num  and y.type='节气门养护'  ) m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification,
   second_level_classification,
   case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.54(养护品产值_空调养护_total)
def kpi_total_kpi_5_54(begin_day, end_day):
    line = """  insert overwrite table  kpi_total partition(mon_p='${mon_p}',kpi='kpi_5_54')
  select substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,maintnance y where x.part_number=y.part_num and y.type='空调养护') m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.54(养护品产值_空调养护_一级)
def kpi_a_level_kpi_5_54(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_level partition(mon_p='${mon_p}',kpi='kpi_5_54')
  select primary_classification,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,maintnance y where x.part_number=y.part_num  and y.type='空调养护' ) m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.54(养护品产值_空调养护_二级)
def kpi_b_level_kpi_5_54(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_level partition(mon_p='${mon_p}',kpi='kpi_5_54')
  select primary_classification,
  second_level_classification,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,maintnance y where x.part_number=y.part_num  and y.type='空调养护'  ) m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification,second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.54(养护品产值_空调养护_车龄)
def kpi_agetype_kpi_5_54(begin_day, end_day):
    line = """  insert overwrite table  kpi_agetype partition(mon_p='${mon_p}',kpi='kpi_5_54')
  select case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end as age_type,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,maintnance y where x.part_number=y.part_num  and y.type='空调养护'  ) m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.54(养护品产值_空调养护_保内外)
def kpi_bnbw_kpi_5_54(begin_day, end_day):
    line = """  insert overwrite table  kpi_bnbw partition(mon_p='${mon_p}',kpi='kpi_5_54')
  select case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end as bn,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,maintnance y where x.part_number=y.part_num  and y.type='空调养护'  ) m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.54(养护品产值_空调养护_一级_车龄)
def kpi_a_agetype_kpi_5_54(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_agetype partition(mon_p='${mon_p}',kpi='kpi_5_54')
  select primary_classification,case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end as age_type,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,maintnance y where x.part_number=y.part_num  and y.type='空调养护'  ) m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification,
   case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.54(养护品产值_空调养护_一级_保内外)
def kpi_a_bnbw_kpi_5_54(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_bnbw partition(mon_p='${mon_p}',kpi='kpi_5_54')
  select primary_classification,
  case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end as bn,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,maintnance y where x.part_number=y.part_num  and y.type='空调养护'  ) m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification,
   case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.54(养护品产值_空调养护_二级_车龄)
def kpi_b_agetype_kpi_5_54(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_agetype partition(mon_p='${mon_p}',kpi='kpi_5_54')
  select primary_classification,second_level_classification,
  case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end as age_type,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,maintnance y where x.part_number=y.part_num  and y.type='空调养护'  ) m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification,
   second_level_classification,
   case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.54(养护品产值_空调养护_二级_保内外)
def kpi_b_bnbw_kpi_5_54(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_bnbw partition(mon_p='${mon_p}',kpi='kpi_5_54')
  select primary_classification,second_level_classification,
  case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end as bn,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,maintnance y where x.part_number=y.part_num  and y.type='空调养护'  ) m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification,
   second_level_classification,
   case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.6(单车高流失件工单产值_total)
def kpi_total_kpi_5_6(begin_day, end_day):
    line = """  insert overwrite table  kpi_total partition(mon_p='${mon_p}',kpi='kpi_5_6')
  select substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,high_flow_parts y where x.part_number=y.part_num and 1=1) m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.6(单车高流失件工单产值_一级)
def kpi_a_level_kpi_5_6(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_level partition(mon_p='${mon_p}',kpi='kpi_5_6')
  select primary_classification,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,high_flow_parts y where x.part_number=y.part_num  and 1=1 ) m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.6(单车高流失件工单产值_二级)
def kpi_b_level_kpi_5_6(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_level partition(mon_p='${mon_p}',kpi='kpi_5_6')
  select primary_classification,
  second_level_classification,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,high_flow_parts y where x.part_number=y.part_num  and 1=1  ) m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification,second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.6(单车高流失件工单产值_车龄)
def kpi_agetype_kpi_5_6(begin_day, end_day):
    line = """  insert overwrite table  kpi_agetype partition(mon_p='${mon_p}',kpi='kpi_5_6')
  select case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end as age_type,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,high_flow_parts y where x.part_number=y.part_num  and 1=1  ) m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.6(单车高流失件工单产值_保内外)
def kpi_bnbw_kpi_5_6(begin_day, end_day):
    line = """  insert overwrite table  kpi_bnbw partition(mon_p='${mon_p}',kpi='kpi_5_6')
  select case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end as bn,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,high_flow_parts y where x.part_number=y.part_num  and 1=1  ) m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.6(单车高流失件工单产值_一级_车龄)
def kpi_a_agetype_kpi_5_6(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_agetype partition(mon_p='${mon_p}',kpi='kpi_5_6')
  select primary_classification,case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end as age_type,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,high_flow_parts y where x.part_number=y.part_num  and 1=1  ) m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification,
   case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.6(单车高流失件工单产值_一级_保内外)
def kpi_a_bnbw_kpi_5_6(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_bnbw partition(mon_p='${mon_p}',kpi='kpi_5_6')
  select primary_classification,
  case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end as bn,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,high_flow_parts y where x.part_number=y.part_num  and 1=1  ) m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification,
   case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.6(单车高流失件工单产值_二级_车龄)
def kpi_b_agetype_kpi_5_6(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_agetype partition(mon_p='${mon_p}',kpi='kpi_5_6')
  select primary_classification,second_level_classification,
  case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end as age_type,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,high_flow_parts y where x.part_number=y.part_num  and 1=1  ) m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification,
   second_level_classification,
   case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.6(单车高流失件工单产值_二级_保内外)
def kpi_b_bnbw_kpi_5_6(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_bnbw partition(mon_p='${mon_p}',kpi='kpi_5_6')
  select primary_classification,second_level_classification,
  case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end as bn,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,high_flow_parts y where x.part_number=y.part_num  and 1=1  ) m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification,
   second_level_classification,
   case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.61(单车高流失件产值_轮胎_total)
def kpi_total_kpi_5_61(begin_day, end_day):
    line = """  insert overwrite table  kpi_total partition(mon_p='${mon_p}',kpi='kpi_5_61')
  select substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,high_flow_parts y where x.part_number=y.part_num and y.type='LunT') m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.61(单车高流失件产值_轮胎_一级)
def kpi_a_level_kpi_5_61(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_level partition(mon_p='${mon_p}',kpi='kpi_5_61')
  select primary_classification,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,high_flow_parts y where x.part_number=y.part_num  and y.type='LunT' ) m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.61(单车高流失件产值_轮胎_二级)
def kpi_b_level_kpi_5_61(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_level partition(mon_p='${mon_p}',kpi='kpi_5_61')
  select primary_classification,
  second_level_classification,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,high_flow_parts y where x.part_number=y.part_num  and y.type='LunT'  ) m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification,second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.61(单车高流失件产值_轮胎_车龄)
def kpi_agetype_kpi_5_61(begin_day, end_day):
    line = """  insert overwrite table  kpi_agetype partition(mon_p='${mon_p}',kpi='kpi_5_61')
  select case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end as age_type,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,high_flow_parts y where x.part_number=y.part_num  and y.type='LunT'  ) m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.61(单车高流失件产值_轮胎_保内外)
def kpi_bnbw_kpi_5_61(begin_day, end_day):
    line = """  insert overwrite table  kpi_bnbw partition(mon_p='${mon_p}',kpi='kpi_5_61')
  select case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end as bn,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,high_flow_parts y where x.part_number=y.part_num  and y.type='LunT'  ) m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.61(单车高流失件产值_轮胎_一级_车龄)
def kpi_a_agetype_kpi_5_61(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_agetype partition(mon_p='${mon_p}',kpi='kpi_5_61')
  select primary_classification,case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end as age_type,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,high_flow_parts y where x.part_number=y.part_num  and y.type='LunT'  ) m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification,
   case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.61(单车高流失件产值_轮胎_一级_保内外)
def kpi_a_bnbw_kpi_5_61(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_bnbw partition(mon_p='${mon_p}',kpi='kpi_5_61')
  select primary_classification,
  case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end as bn,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,high_flow_parts y where x.part_number=y.part_num  and y.type='LunT'  ) m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification,
   case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.61(单车高流失件产值_轮胎_二级_车龄)
def kpi_b_agetype_kpi_5_61(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_agetype partition(mon_p='${mon_p}',kpi='kpi_5_61')
  select primary_classification,second_level_classification,
  case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end as age_type,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,high_flow_parts y where x.part_number=y.part_num  and y.type='LunT'  ) m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification,
   second_level_classification,
   case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.61(单车高流失件产值_轮胎_二级_保内外)
def kpi_b_bnbw_kpi_5_61(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_bnbw partition(mon_p='${mon_p}',kpi='kpi_5_61')
  select primary_classification,second_level_classification,
  case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end as bn,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,high_flow_parts y where x.part_number=y.part_num  and y.type='LunT'  ) m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification,
   second_level_classification,
   case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.62(单车高流失件产值_蓄电池_total)
def kpi_total_kpi_5_62(begin_day, end_day):
    line = """  insert overwrite table  kpi_total partition(mon_p='${mon_p}',kpi='kpi_5_62')
  select substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,high_flow_parts y where x.part_number=y.part_num and y.type='DianC') m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.62(单车高流失件产值_蓄电池_一级)
def kpi_a_level_kpi_5_62(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_level partition(mon_p='${mon_p}',kpi='kpi_5_62')
  select primary_classification,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,high_flow_parts y where x.part_number=y.part_num  and y.type='DianC' ) m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.62(单车高流失件产值_蓄电池_二级)
def kpi_b_level_kpi_5_62(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_level partition(mon_p='${mon_p}',kpi='kpi_5_62')
  select primary_classification,
  second_level_classification,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,high_flow_parts y where x.part_number=y.part_num  and y.type='DianC'  ) m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification,second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.62(单车高流失件产值_蓄电池_车龄)
def kpi_agetype_kpi_5_62(begin_day, end_day):
    line = """  insert overwrite table  kpi_agetype partition(mon_p='${mon_p}',kpi='kpi_5_62')
  select case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end as age_type,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,high_flow_parts y where x.part_number=y.part_num  and y.type='DianC'  ) m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.62(单车高流失件产值_蓄电池_保内外)
def kpi_bnbw_kpi_5_62(begin_day, end_day):
    line = """  insert overwrite table  kpi_bnbw partition(mon_p='${mon_p}',kpi='kpi_5_62')
  select case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end as bn,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,high_flow_parts y where x.part_number=y.part_num  and y.type='DianC'  ) m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.62(单车高流失件产值_蓄电池_一级_车龄)
def kpi_a_agetype_kpi_5_62(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_agetype partition(mon_p='${mon_p}',kpi='kpi_5_62')
  select primary_classification,case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end as age_type,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,high_flow_parts y where x.part_number=y.part_num  and y.type='DianC'  ) m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification,
   case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.62(单车高流失件产值_蓄电池_一级_保内外)
def kpi_a_bnbw_kpi_5_62(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_bnbw partition(mon_p='${mon_p}',kpi='kpi_5_62')
  select primary_classification,
  case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end as bn,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,high_flow_parts y where x.part_number=y.part_num  and y.type='DianC'  ) m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification,
   case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.62(单车高流失件产值_蓄电池_二级_车龄)
def kpi_b_agetype_kpi_5_62(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_agetype partition(mon_p='${mon_p}',kpi='kpi_5_62')
  select primary_classification,second_level_classification,
  case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end as age_type,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,high_flow_parts y where x.part_number=y.part_num  and y.type='DianC'  ) m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification,
   second_level_classification,
   case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.62(单车高流失件产值_蓄电池_二级_保内外)
def kpi_b_bnbw_kpi_5_62(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_bnbw partition(mon_p='${mon_p}',kpi='kpi_5_62')
  select primary_classification,second_level_classification,
  case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end as bn,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,high_flow_parts y where x.part_number=y.part_num  and y.type='DianC'  ) m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification,
   second_level_classification,
   case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.63(单车高流失件产值_轮胎&蓄电池_total)
def kpi_total_kpi_5_63(begin_day, end_day):
    line = """  insert overwrite table  kpi_total partition(mon_p='${mon_p}',kpi='kpi_5_63')
  select substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,high_flow_parts y where x.part_number=y.part_num and y.type in('DianC','LunT')) m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.63(单车高流失件产值_轮胎&蓄电池_一级)
def kpi_a_level_kpi_5_63(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_level partition(mon_p='${mon_p}',kpi='kpi_5_63')
  select primary_classification,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,high_flow_parts y where x.part_number=y.part_num  and y.type in('DianC','LunT') ) m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.63(单车高流失件产值_轮胎&蓄电池_二级)
def kpi_b_level_kpi_5_63(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_level partition(mon_p='${mon_p}',kpi='kpi_5_63')
  select primary_classification,
  second_level_classification,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,high_flow_parts y where x.part_number=y.part_num  and y.type in('DianC','LunT')) m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification,second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.63(单车高流失件产值_轮胎&蓄电池_车龄)
def kpi_agetype_kpi_5_63(begin_day, end_day):
    line = """  insert overwrite table  kpi_agetype partition(mon_p='${mon_p}',kpi='kpi_5_63')
  select case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end as age_type,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,high_flow_parts y where x.part_number=y.part_num  and y.type in('DianC','LunT')  ) m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.63(单车高流失件产值_轮胎&蓄电池_保内外)
def kpi_bnbw_kpi_5_63(begin_day, end_day):
    line = """  insert overwrite table  kpi_bnbw partition(mon_p='${mon_p}',kpi='kpi_5_63')
  select case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end as bn,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,high_flow_parts y where x.part_number=y.part_num  and y.type in('DianC','LunT')  ) m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.63(单车高流失件产值_轮胎&蓄电池_一级_车龄)
def kpi_a_agetype_kpi_5_63(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_agetype partition(mon_p='${mon_p}',kpi='kpi_5_63')
  select primary_classification,case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end as age_type,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,high_flow_parts y where x.part_number=y.part_num  and y.type in('DianC','LunT')  ) m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification,
   case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.63(单车高流失件产值_轮胎&蓄电池_一级_保内外)
def kpi_a_bnbw_kpi_5_63(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_bnbw partition(mon_p='${mon_p}',kpi='kpi_5_63')
  select primary_classification,
  case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end as bn,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,high_flow_parts y where x.part_number=y.part_num  and y.type in('DianC','LunT')  ) m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification,
   case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.63(单车高流失件产值_轮胎&蓄电池_二级_车龄)
def kpi_b_agetype_kpi_5_63(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_agetype partition(mon_p='${mon_p}',kpi='kpi_5_63')
  select primary_classification,second_level_classification,
  case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end as age_type,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,high_flow_parts y where x.part_number=y.part_num  and y.type in('DianC','LunT')  ) m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification,
   second_level_classification,
   case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.63(单车高流失件产值_轮胎&蓄电池_二级_保内外)
def kpi_b_bnbw_kpi_5_63(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_bnbw partition(mon_p='${mon_p}',kpi='kpi_5_63')
  select primary_classification,second_level_classification,
  case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end as bn,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,high_flow_parts y where x.part_number=y.part_num  and y.type in('DianC','LunT')  ) m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification,
   second_level_classification,
   case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.64(单车高流失件产值_其他_total)
def kpi_total_kpi_5_64(begin_day, end_day):
    line = """  insert overwrite table  kpi_total partition(mon_p='${mon_p}',kpi='kpi_5_64')
  select substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,high_flow_parts y where x.part_number=y.part_num and y.type ='QiT') m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.64(单车高流失件产值_其他_一级)
def kpi_a_level_kpi_5_64(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_level partition(mon_p='${mon_p}',kpi='kpi_5_64')
  select primary_classification,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,high_flow_parts y where x.part_number=y.part_num  and y.type ='QiT' ) m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.64(单车高流失件产值_其他_二级)
def kpi_b_level_kpi_5_64(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_level partition(mon_p='${mon_p}',kpi='kpi_5_64')
  select primary_classification,
  second_level_classification,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,high_flow_parts y where x.part_number=y.part_num  and y.type ='QiT') m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification,second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.64(单车高流失件产值_其他_车龄)
def kpi_agetype_kpi_5_64(begin_day, end_day):
    line = """  insert overwrite table  kpi_agetype partition(mon_p='${mon_p}',kpi='kpi_5_64')
  select case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end as age_type,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,high_flow_parts y where x.part_number=y.part_num  and y.type ='QiT'  ) m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.64(单车高流失件产值_其他_保内外)
def kpi_bnbw_kpi_5_64(begin_day, end_day):
    line = """  insert overwrite table  kpi_bnbw partition(mon_p='${mon_p}',kpi='kpi_5_64')
  select case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end as bn,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,high_flow_parts y where x.part_number=y.part_num  and y.type ='QiT'  ) m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.64(单车高流失件产值_其他_一级_车龄)
def kpi_a_agetype_kpi_5_64(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_agetype partition(mon_p='${mon_p}',kpi='kpi_5_64')
  select primary_classification,case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end as age_type,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,high_flow_parts y where x.part_number=y.part_num  and y.type ='QiT'  ) m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification,
   case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.64(单车高流失件产值_其他_一级_保内外)
def kpi_a_bnbw_kpi_5_64(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_bnbw partition(mon_p='${mon_p}',kpi='kpi_5_64')
  select primary_classification,
  case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end as bn,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,high_flow_parts y where x.part_number=y.part_num  and y.type ='QiT'  ) m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification,
   case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.64(单车高流失件产值_其他_二级_车龄)
def kpi_b_agetype_kpi_5_64(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_agetype partition(mon_p='${mon_p}',kpi='kpi_5_64')
  select primary_classification,second_level_classification,
  case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end as age_type,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,high_flow_parts y where x.part_number=y.part_num  and y.type ='QiT'  ) m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification,
   second_level_classification,
   case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.64(单车高流失件产值_其他_二级_保内外)
def kpi_b_bnbw_kpi_5_64(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_bnbw partition(mon_p='${mon_p}',kpi='kpi_5_64')
  select primary_classification,second_level_classification,
  case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end as bn,
  substr(t.mon_label,1,7) as mon,
  sum(s.order_balance_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select distinct x.asc_code,x.order_number
   from part x,high_flow_parts y where x.part_number=y.part_num  and y.type ='QiT'  ) m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification,
   second_level_classification,
   case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.7(单车年精品附件产值_total)
def kpi_total_kpi_5_7(begin_day, end_day):
    line = """  insert overwrite table  kpi_total partition(mon_p='${mon_p}',kpi='kpi_5_7')
  select substr(t.mon_label,1,7) as mon,
  sum(m.sales_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select x.asc_code,x.order_number,x.sales_amount
   from part x,enclosure y where x.part_number=y.part_num ) m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.7(单车年精品附件产值_一级)
def kpi_a_level_kpi_5_7(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_level partition(mon_p='${mon_p}',kpi='kpi_5_7')
  select primary_classification,
  substr(t.mon_label,1,7) as mon,
  sum(m.sales_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select x.asc_code,x.order_number,x.sales_amount
   from part x,enclosure y where x.part_number=y.part_num   ) m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.7(单车年精品附件产值_二级)
def kpi_b_level_kpi_5_7(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_level partition(mon_p='${mon_p}',kpi='kpi_5_7')
  select primary_classification,
  second_level_classification,
  substr(t.mon_label,1,7) as mon,
  sum(m.sales_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select x.asc_code,x.order_number,x.sales_amount
   from part x,enclosure y where x.part_number=y.part_num  ) m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification,second_level_classification"""
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.7(单车年精品附件产值_车龄)
def kpi_agetype_kpi_5_7(begin_day, end_day):
    line = """  insert overwrite table  kpi_agetype partition(mon_p='${mon_p}',kpi='kpi_5_7')
  select case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end as age_type,
  substr(t.mon_label,1,7) as mon,
  sum(m.sales_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select x.asc_code,x.order_number,x.sales_amount
   from part x,enclosure y where x.part_number=y.part_num    ) m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.7(单车年精品附件产值_保内外)
def kpi_bnbw_kpi_5_7(begin_day, end_day):
    line = """  insert overwrite table  kpi_bnbw partition(mon_p='${mon_p}',kpi='kpi_5_7')
  select case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end as bn,
  substr(t.mon_label,1,7) as mon,
  sum(m.sales_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select x.asc_code,x.order_number,x.sales_amount
   from part x,enclosure y where x.part_number=y.part_num    ) m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.7(单车年精品附件产值_一级_车龄)
def kpi_a_agetype_kpi_5_7(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_agetype partition(mon_p='${mon_p}',kpi='kpi_5_7')
  select primary_classification,case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end as age_type,
  substr(t.mon_label,1,7) as mon,
  sum(m.sales_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select x.asc_code,x.order_number,x.sales_amount
   from part x,enclosure y where x.part_number=y.part_num    ) m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification,
   case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.7(单车年精品附件产值_一级_保内外)
def kpi_a_bnbw_kpi_5_7(begin_day, end_day):
    line = """  insert overwrite table  kpi_a_bnbw partition(mon_p='${mon_p}',kpi='kpi_5_7')
  select primary_classification,
  case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end as bn,
  substr(t.mon_label,1,7) as mon,
  sum(m.sales_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select x.asc_code,x.order_number,x.sales_amount
   from part x,enclosure y where x.part_number=y.part_num    ) m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification,
   case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.7(单车年精品附件产值_二级_车龄)
def kpi_b_agetype_kpi_5_7(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_agetype partition(mon_p='${mon_p}',kpi='kpi_5_7')
  select primary_classification,second_level_classification,
  case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end as age_type,
  substr(t.mon_label,1,7) as mon,
  sum(m.sales_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select x.asc_code,x.order_number,x.sales_amount
   from part x,enclosure y where x.part_number=y.part_num    ) m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification,
   second_level_classification,
   case when s.outdate='未知' then '未知'
      when mon_diff(t.mon_label,s.outdate)<=11 then '0-1年'
      when mon_diff(t.mon_label,s.outdate)>=12
      and mon_diff(t.mon_label,s.outdate)<=23 then '1-2年'
      when mon_diff(t.mon_label,s.outdate)>=24
      and mon_diff(t.mon_label,s.outdate)<=35 then '2-3年'
      when mon_diff(t.mon_label,s.outdate)>=36
      and mon_diff(t.mon_label,s.outdate)<=47 then '3-4年'
      when mon_diff(t.mon_label,s.outdate)>=48
      and mon_diff(t.mon_label,s.outdate)<=59 then '4-5年'
      else '5+年' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


# ----kpi_5.7(单车年精品附件产值_二级_保内外)
def kpi_b_bnbw_kpi_5_7(begin_day, end_day):
    line = """  insert overwrite table  kpi_b_bnbw partition(mon_p='${mon_p}',kpi='kpi_5_7')
  select primary_classification,second_level_classification,
  case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end as bn,
  substr(t.mon_label,1,7) as mon,
  sum(m.sales_amount)/count(distinct s.vin) as num,
  s.asc_code
  from date_label t,label_order s,
  (select x.asc_code,x.order_number,x.sales_amount
   from part x,enclosure y where x.part_number=y.part_num    ) m
  where substr(t.mon_label,1,7)>='${begin_date}'
    and substr(t.mon_label,1,7)<='${end_date}'
    and substr(tran_date(s.order_balance_date),1,7)>=substr(add_months(t.mon_label,-11),1,7)
    and substr(tran_date(s.order_balance_date),1,7)<=substr(t.mon_label,1,7)
    and s.MAINT_TYPE1<>'删除'
    and s.asc_code=m.asc_code
    and s.order_number=m.order_number
   group by substr(t.mon_label,1,7),s.asc_code,primary_classification,
   second_level_classification,
   case when s.outdate='未知' then '未知'
      when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=23 then '保内'
  	when s.outdate<'2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>23 then '保外'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)<=35 then '保内'
  	when s.outdate>='2013-10-01' 
  	and mon_diff(t.mon_label,s.outdate)>35 then '保外'
  	else '' end """
    mon_p = begin_day + '_' + end_day
    line = line.replace('${begin_date}', begin_day)
    line = line.replace('${end_date}', end_day)
    line = line.replace('${mon_p}', mon_p)
    try:
        spark.sql(line)
    except:
        traceback.print_exc()


if __name__ == "__main__":
    if sys.argv.__len__() != 3:
        print("需要两个参数 <开始时间> <结束时间>")
        sys.exit(-1)
    begin_day = sys.argv[1]
    end_day = sys.argv[2]
    kpi_total_kpi_1(begin_day, end_day)
    print "--------kpi_total_kpi_1"
    kpi_a_level_kpi_1(begin_day, end_day)
    print "--------kpi_a_level_kpi_1"
    kpi_b_level_kpi_1(begin_day, end_day)
    print "--------kpi_b_level_kpi_1"
    kpi_agetype_kpi_1(begin_day, end_day)
    print "--------kpi_agetype_kpi_1"
    kpi_bnbw_kpi_1(begin_day, end_day)
    print "--------kpi_bnbw_kpi_1"
    kpi_a_agetype_kpi_1(begin_day, end_day)
    print "--------kpi_a_agetype_kpi_1"
    kpi_a_bnbw_kpi_1(begin_day, end_day)
    print "--------kpi_a_bnbw_kpi_1"
    kpi_b_agetype_kpi_1(begin_day, end_day)
    print "--------kpi_b_agetype_kpi_1"
    kpi_b_bnbw_kpi_1(begin_day, end_day)
    print "--------kpi_b_bnbw_kpi_1"
    kpi_total_kpi_1_1(begin_day, end_day)
    print "--------kpi_total_kpi_1_1"
    kpi_a_level_kpi_1_1(begin_day, end_day)
    print "--------kpi_a_level_kpi_1_1"
    kpi_b_level_kpi_1_1(begin_day, end_day)
    print "--------kpi_b_level_kpi_1_1"
    kpi_agetype_kpi_1_1(begin_day, end_day)
    print "--------kpi_agetype_kpi_1_1"
    kpi_bnbw_kpi_1_1(begin_day, end_day)
    print "--------kpi_bnbw_kpi_1_1"
    kpi_a_agetype_kpi_1_1(begin_day, end_day)
    print "--------kpi_a_agetype_kpi_1_1"
    kpi_a_bnbw_kpi_1_1(begin_day, end_day)
    print "--------kpi_a_bnbw_kpi_1_1"
    kpi_b_agetype_kpi_1_1(begin_day, end_day)
    print "--------kpi_b_agetype_kpi_1_1"
    kpi_b_bnbw_kpi_1_1(begin_day, end_day)
    print "--------kpi_b_bnbw_kpi_1_1"
    kpi_total_kpi_1_11(begin_day, end_day)
    print "--------kpi_total_kpi_1_11"
    kpi_a_level_kpi_1_11(begin_day, end_day)
    print "--------kpi_a_level_kpi_1_11"
    kpi_b_level_kpi_1_11(begin_day, end_day)
    print "--------kpi_b_level_kpi_1_11"
    kpi_agetype_kpi_1_11(begin_day, end_day)
    print "--------kpi_agetype_kpi_1_11"
    kpi_bnbw_kpi_1_11(begin_day, end_day)
    print "--------kpi_bnbw_kpi_1_11"
    kpi_a_agetype_kpi_1_11(begin_day, end_day)
    print "--------kpi_a_agetype_kpi_1_11"
    kpi_a_bnbw_kpi_1_11(begin_day, end_day)
    print "--------kpi_a_bnbw_kpi_1_11"
    kpi_b_agetype_kpi_1_11(begin_day, end_day)
    print "--------kpi_b_agetype_kpi_1_11"
    kpi_b_bnbw_kpi_1_11(begin_day, end_day)
    print "--------kpi_b_bnbw_kpi_1_11"
    kpi_total_kpi_1_12(begin_day, end_day)
    print "--------kpi_total_kpi_1_12"
    kpi_a_level_kpi_1_12(begin_day, end_day)
    print "--------kpi_a_level_kpi_1_12"
    kpi_b_level_kpi_1_12(begin_day, end_day)
    print "--------kpi_b_level_kpi_1_12"
    kpi_agetype_kpi_1_12(begin_day, end_day)
    print "--------kpi_agetype_kpi_1_12"
    kpi_bnbw_kpi_1_12(begin_day, end_day)
    print "--------kpi_bnbw_kpi_1_12"
    kpi_a_agetype_kpi_1_12(begin_day, end_day)
    print "--------kpi_a_agetype_kpi_1_12"
    kpi_a_bnbw_kpi_1_12(begin_day, end_day)
    print "--------kpi_a_bnbw_kpi_1_12"
    kpi_b_agetype_kpi_1_12(begin_day, end_day)
    print "--------kpi_b_agetype_kpi_1_12"
    kpi_b_bnbw_kpi_1_12(begin_day, end_day)
    print "--------kpi_b_bnbw_kpi_1_12"
    kpi_total_kpi_1_13(begin_day, end_day)
    print "--------kpi_total_kpi_1_13"
    kpi_a_level_kpi_1_13(begin_day, end_day)
    print "--------kpi_a_level_kpi_1_13"
    kpi_b_level_kpi_1_13(begin_day, end_day)
    print "--------kpi_b_level_kpi_1_13"
    kpi_agetype_kpi_1_13(begin_day, end_day)
    print "--------kpi_agetype_kpi_1_13"
    kpi_bnbw_kpi_1_13(begin_day, end_day)
    print "--------kpi_bnbw_kpi_1_13"
    kpi_a_agetype_kpi_1_13(begin_day, end_day)
    print "--------kpi_a_agetype_kpi_1_13"
    kpi_a_bnbw_kpi_1_13(begin_day, end_day)
    print "--------kpi_a_bnbw_kpi_1_13"
    kpi_b_agetype_kpi_1_13(begin_day, end_day)
    print "--------kpi_b_agetype_kpi_1_13"
    kpi_b_bnbw_kpi_1_13(begin_day, end_day)
    print "--------kpi_b_bnbw_kpi_1_13"
    kpi_total_kpi_1_14(begin_day, end_day)
    print "--------kpi_total_kpi_1_14"
    kpi_a_level_kpi_1_14(begin_day, end_day)
    print "--------kpi_a_level_kpi_1_14"
    kpi_b_level_kpi_1_14(begin_day, end_day)
    print "--------kpi_b_level_kpi_1_14"
    kpi_agetype_kpi_1_14(begin_day, end_day)
    print "--------kpi_agetype_kpi_1_14"
    kpi_bnbw_kpi_1_14(begin_day, end_day)
    print "--------kpi_bnbw_kpi_1_14"
    kpi_a_agetype_kpi_1_14(begin_day, end_day)
    print "--------kpi_a_agetype_kpi_1_14"
    kpi_a_bnbw_kpi_1_14(begin_day, end_day)
    print "--------kpi_a_bnbw_kpi_1_14"
    kpi_b_agetype_kpi_1_14(begin_day, end_day)
    print "--------kpi_b_agetype_kpi_1_14"
    kpi_b_bnbw_kpi_1_14(begin_day, end_day)
    print "--------kpi_b_bnbw_kpi_1_14"
    kpi_total_kpi_1_2(begin_day, end_day)
    print "--------kpi_total_kpi_1_2"
    kpi_a_level_kpi_1_2(begin_day, end_day)
    print "--------kpi_a_level_kpi_1_2"
    kpi_b_level_kpi_1_2(begin_day, end_day)
    print "--------kpi_b_level_kpi_1_2"
    kpi_agetype_kpi_1_2(begin_day, end_day)
    print "--------kpi_agetype_kpi_1_2"
    kpi_bnbw_kpi_1_2(begin_day, end_day)
    print "--------kpi_bnbw_kpi_1_2"
    kpi_a_agetype_kpi_1_2(begin_day, end_day)
    print "--------kpi_a_agetype_kpi_1_2"
    kpi_a_bnbw_kpi_1_2(begin_day, end_day)
    print "--------kpi_a_bnbw_kpi_1_2"
    kpi_b_agetype_kpi_1_2(begin_day, end_day)
    print "--------kpi_b_agetype_kpi_1_2"
    kpi_b_bnbw_kpi_1_2(begin_day, end_day)
    print "--------kpi_b_bnbw_kpi_1_2"
    kpi_total_kpi_1_21(begin_day, end_day)
    print "--------kpi_total_kpi_1_21"
    kpi_a_level_kpi_1_21(begin_day, end_day)
    print "--------kpi_a_level_kpi_1_21"
    kpi_b_level_kpi_1_21(begin_day, end_day)
    print "--------kpi_b_level_kpi_1_21"
    kpi_agetype_kpi_1_21(begin_day, end_day)
    print "--------kpi_agetype_kpi_1_21"
    kpi_bnbw_kpi_1_21(begin_day, end_day)
    print "--------kpi_bnbw_kpi_1_21"
    kpi_a_agetype_kpi_1_21(begin_day, end_day)
    print "--------kpi_a_agetype_kpi_1_21"
    kpi_a_bnbw_kpi_1_21(begin_day, end_day)
    print "--------kpi_a_bnbw_kpi_1_21"
    kpi_b_agetype_kpi_1_21(begin_day, end_day)
    print "--------kpi_b_agetype_kpi_1_21"
    kpi_b_bnbw_kpi_1_21(begin_day, end_day)
    print "--------kpi_b_bnbw_kpi_1_21"
    kpi_total_kpi_1_3(begin_day, end_day)
    print "--------kpi_total_kpi_1_3"
    kpi_a_level_kpi_1_3(begin_day, end_day)
    print "--------kpi_a_level_kpi_1_3"
    kpi_b_level_kpi_1_3(begin_day, end_day)
    print "--------kpi_b_level_kpi_1_3"
    kpi_agetype_kpi_1_3(begin_day, end_day)
    print "--------kpi_agetype_kpi_1_3"
    kpi_bnbw_kpi_1_3(begin_day, end_day)
    print "--------kpi_bnbw_kpi_1_3"
    kpi_a_agetype_kpi_1_3(begin_day, end_day)
    print "--------kpi_a_agetype_kpi_1_3"
    kpi_a_bnbw_kpi_1_3(begin_day, end_day)
    print "--------kpi_a_bnbw_kpi_1_3"
    kpi_b_agetype_kpi_1_3(begin_day, end_day)
    print "--------kpi_b_agetype_kpi_1_3"
    kpi_b_bnbw_kpi_1_3(begin_day, end_day)
    print "--------kpi_b_bnbw_kpi_1_3"
    kpi_total_kpi_1_31(begin_day, end_day)
    print "--------kpi_total_kpi_1_31"
    kpi_a_level_kpi_1_31(begin_day, end_day)
    print "--------kpi_a_level_kpi_1_31"
    kpi_b_level_kpi_1_31(begin_day, end_day)
    print "--------kpi_b_level_kpi_1_31"
    kpi_agetype_kpi_1_31(begin_day, end_day)
    print "--------kpi_agetype_kpi_1_31"
    kpi_bnbw_kpi_1_31(begin_day, end_day)
    print "--------kpi_bnbw_kpi_1_31"
    kpi_a_agetype_kpi_1_31(begin_day, end_day)
    print "--------kpi_a_agetype_kpi_1_31"
    kpi_a_bnbw_kpi_1_31(begin_day, end_day)
    print "--------kpi_a_bnbw_kpi_1_31"
    kpi_b_agetype_kpi_1_31(begin_day, end_day)
    print "--------kpi_b_agetype_kpi_1_31"
    kpi_b_bnbw_kpi_1_31(begin_day, end_day)
    print "--------kpi_b_bnbw_kpi_1_31"
    kpi_total_kpi_1_32(begin_day, end_day)
    print "--------kpi_total_kpi_1_32"
    kpi_a_level_kpi_1_32(begin_day, end_day)
    print "--------kpi_a_level_kpi_1_32"
    kpi_b_level_kpi_1_32(begin_day, end_day)
    print "--------kpi_b_level_kpi_1_32"
    kpi_agetype_kpi_1_32(begin_day, end_day)
    print "--------kpi_agetype_kpi_1_32"
    kpi_bnbw_kpi_1_32(begin_day, end_day)
    print "--------kpi_bnbw_kpi_1_32"
    kpi_a_agetype_kpi_1_32(begin_day, end_day)
    print "--------kpi_a_agetype_kpi_1_32"
    kpi_a_bnbw_kpi_1_32(begin_day, end_day)
    print "--------kpi_a_bnbw_kpi_1_32"
    kpi_b_agetype_kpi_1_32(begin_day, end_day)
    print "--------kpi_b_agetype_kpi_1_32"
    kpi_b_bnbw_kpi_1_32(begin_day, end_day)
    print "--------kpi_b_bnbw_kpi_1_32"
    kpi_total_kpi_1_33(begin_day, end_day)
    print "--------kpi_total_kpi_1_33"
    kpi_a_level_kpi_1_33(begin_day, end_day)
    print "--------kpi_a_level_kpi_1_33"
    kpi_b_level_kpi_1_33(begin_day, end_day)
    print "--------kpi_b_level_kpi_1_33"
    kpi_agetype_kpi_1_33(begin_day, end_day)
    print "--------kpi_agetype_kpi_1_33"
    kpi_bnbw_kpi_1_33(begin_day, end_day)
    print "--------kpi_bnbw_kpi_1_33"
    kpi_a_agetype_kpi_1_33(begin_day, end_day)
    print "--------kpi_a_agetype_kpi_1_33"
    kpi_a_bnbw_kpi_1_33(begin_day, end_day)
    print "--------kpi_a_bnbw_kpi_1_33"
    kpi_b_agetype_kpi_1_33(begin_day, end_day)
    print "--------kpi_b_agetype_kpi_1_33"
    kpi_b_bnbw_kpi_1_33(begin_day, end_day)
    print "--------kpi_b_bnbw_kpi_1_33"
    kpi_total_kpi_1_34(begin_day, end_day)
    print "--------kpi_total_kpi_1_34"
    kpi_a_level_kpi_1_34(begin_day, end_day)
    print "--------kpi_a_level_kpi_1_34"
    kpi_b_level_kpi_1_34(begin_day, end_day)
    print "--------kpi_b_level_kpi_1_34"
    kpi_agetype_kpi_1_34(begin_day, end_day)
    print "--------kpi_agetype_kpi_1_34"
    kpi_bnbw_kpi_1_34(begin_day, end_day)
    print "--------kpi_bnbw_kpi_1_34"
    kpi_a_agetype_kpi_1_34(begin_day, end_day)
    print "--------kpi_a_agetype_kpi_1_34"
    kpi_a_bnbw_kpi_1_34(begin_day, end_day)
    print "--------kpi_a_bnbw_kpi_1_34"
    kpi_b_agetype_kpi_1_34(begin_day, end_day)
    print "--------kpi_b_agetype_kpi_1_34"
    kpi_b_bnbw_kpi_1_34(begin_day, end_day)
    print "--------kpi_b_bnbw_kpi_1_34"
    kpi_total_kpi_1_35(begin_day, end_day)
    print "--------kpi_total_kpi_1_35"
    kpi_a_level_kpi_1_35(begin_day, end_day)
    print "--------kpi_a_level_kpi_1_35"
    kpi_b_level_kpi_1_35(begin_day, end_day)
    print "--------kpi_b_level_kpi_1_35"
    kpi_agetype_kpi_1_35(begin_day, end_day)
    print "--------kpi_agetype_kpi_1_35"
    kpi_bnbw_kpi_1_35(begin_day, end_day)
    print "--------kpi_bnbw_kpi_1_35"
    kpi_a_agetype_kpi_1_35(begin_day, end_day)
    print "--------kpi_a_agetype_kpi_1_35"
    kpi_a_bnbw_kpi_1_35(begin_day, end_day)
    print "--------kpi_a_bnbw_kpi_1_35"
    kpi_b_agetype_kpi_1_35(begin_day, end_day)
    print "--------kpi_b_agetype_kpi_1_35"
    kpi_b_bnbw_kpi_1_35(begin_day, end_day)
    print "--------kpi_b_bnbw_kpi_1_35"
    kpi_total_kpi_1_36(begin_day, end_day)
    print "--------kpi_total_kpi_1_36"
    kpi_a_level_kpi_1_36(begin_day, end_day)
    print "--------kpi_a_level_kpi_1_36"
    kpi_b_level_kpi_1_36(begin_day, end_day)
    print "--------kpi_b_level_kpi_1_36"
    kpi_agetype_kpi_1_36(begin_day, end_day)
    print "--------kpi_agetype_kpi_1_36"
    kpi_bnbw_kpi_1_36(begin_day, end_day)
    print "--------kpi_bnbw_kpi_1_36"
    kpi_a_agetype_kpi_1_36(begin_day, end_day)
    print "--------kpi_a_agetype_kpi_1_36"
    kpi_a_bnbw_kpi_1_36(begin_day, end_day)
    print "--------kpi_a_bnbw_kpi_1_36"
    kpi_b_agetype_kpi_1_36(begin_day, end_day)
    print "--------kpi_b_agetype_kpi_1_36"
    kpi_b_bnbw_kpi_1_36(begin_day, end_day)
    print "--------kpi_b_bnbw_kpi_1_36"
    kpi_total_kpi_1_4(begin_day, end_day)
    print "--------kpi_total_kpi_1_4"
    kpi_a_level_kpi_1_4(begin_day, end_day)
    print "--------kpi_a_level_kpi_1_4"
    kpi_b_level_kpi_1_4(begin_day, end_day)
    print "--------kpi_b_level_kpi_1_4"
    kpi_agetype_kpi_1_4(begin_day, end_day)
    print "--------kpi_agetype_kpi_1_4"
    kpi_bnbw_kpi_1_4(begin_day, end_day)
    print "--------kpi_bnbw_kpi_1_4"
    kpi_a_agetype_kpi_1_4(begin_day, end_day)
    print "--------kpi_a_agetype_kpi_1_4"
    kpi_a_bnbw_kpi_1_4(begin_day, end_day)
    print "--------kpi_a_bnbw_kpi_1_4"
    kpi_b_agetype_kpi_1_4(begin_day, end_day)
    print "--------kpi_b_agetype_kpi_1_4"
    kpi_b_bnbw_kpi_1_4(begin_day, end_day)
    print "--------kpi_b_bnbw_kpi_1_4"
    kpi_total_kpi_1_41(begin_day, end_day)
    print "--------kpi_total_kpi_1_41"
    kpi_a_level_kpi_1_41(begin_day, end_day)
    print "--------kpi_a_level_kpi_1_41"
    kpi_b_level_kpi_1_41(begin_day, end_day)
    print "--------kpi_b_level_kpi_1_41"
    kpi_agetype_kpi_1_41(begin_day, end_day)
    print "--------kpi_agetype_kpi_1_41"
    kpi_bnbw_kpi_1_41(begin_day, end_day)
    print "--------kpi_bnbw_kpi_1_41"
    kpi_a_agetype_kpi_1_41(begin_day, end_day)
    print "--------kpi_a_agetype_kpi_1_41"
    kpi_a_bnbw_kpi_1_41(begin_day, end_day)
    print "--------kpi_a_bnbw_kpi_1_41"
    kpi_b_agetype_kpi_1_41(begin_day, end_day)
    print "--------kpi_b_agetype_kpi_1_41"
    kpi_b_bnbw_kpi_1_41(begin_day, end_day)
    print "--------kpi_b_bnbw_kpi_1_41"
    kpi_total_kpi_2(begin_day, end_day)
    print "--------kpi_total_kpi_2"
    kpi_a_level_kpi_2(begin_day, end_day)
    print "--------kpi_a_level_kpi_2"
    kpi_b_level_kpi_2(begin_day, end_day)
    print "--------kpi_b_level_kpi_2"
    kpi_agetype_kpi_2(begin_day, end_day)
    print "--------kpi_agetype_kpi_2"
    kpi_bnbw_kpi_2(begin_day, end_day)
    print "--------kpi_bnbw_kpi_2"
    kpi_a_agetype_kpi_2(begin_day, end_day)
    print "--------kpi_a_agetype_kpi_2"
    kpi_a_bnbw_kpi_2(begin_day, end_day)
    print "--------kpi_a_bnbw_kpi_2"
    kpi_b_agetype_kpi_2(begin_day, end_day)
    print "--------kpi_b_agetype_kpi_2"
    kpi_b_bnbw_kpi_2(begin_day, end_day)
    print "--------kpi_b_bnbw_kpi_2"
    kpi_total_kpi_2_1(begin_day, end_day)
    print "--------kpi_total_kpi_2_1"
    kpi_a_level_kpi_2_1(begin_day, end_day)
    print "--------kpi_a_level_kpi_2_1"
    kpi_b_level_kpi_2_1(begin_day, end_day)
    print "--------kpi_b_level_kpi_2_1"
    kpi_agetype_kpi_2_1(begin_day, end_day)
    print "--------kpi_agetype_kpi_2_1"
    kpi_bnbw_kpi_2_1(begin_day, end_day)
    print "--------kpi_bnbw_kpi_2_1"
    kpi_a_agetype_kpi_2_1(begin_day, end_day)
    print "--------kpi_a_agetype_kpi_2_1"
    kpi_a_bnbw_kpi_2_1(begin_day, end_day)
    print "--------kpi_a_bnbw_kpi_2_1"
    kpi_b_agetype_kpi_2_1(begin_day, end_day)
    print "--------kpi_b_agetype_kpi_2_1"
    kpi_b_bnbw_kpi_2_1(begin_day, end_day)
    print "--------kpi_b_bnbw_kpi_2_1"
    kpi_total_kpi_2_2(begin_day, end_day)
    print "--------kpi_total_kpi_2_2"
    kpi_a_level_kpi_2_2(begin_day, end_day)
    print "--------kpi_a_level_kpi_2_2"
    kpi_b_level_kpi_2_2(begin_day, end_day)
    print "--------kpi_b_level_kpi_2_2"
    kpi_agetype_kpi_2_2(begin_day, end_day)
    print "--------kpi_agetype_kpi_2_2"
    kpi_bnbw_kpi_2_2(begin_day, end_day)
    print "--------kpi_bnbw_kpi_2_2"
    kpi_a_agetype_kpi_2_2(begin_day, end_day)
    print "--------kpi_a_agetype_kpi_2_2"
    kpi_a_bnbw_kpi_2_2(begin_day, end_day)
    print "--------kpi_a_bnbw_kpi_2_2"
    kpi_b_agetype_kpi_2_2(begin_day, end_day)
    print "--------kpi_b_agetype_kpi_2_2"
    kpi_b_bnbw_kpi_2_2(begin_day, end_day)
    print "--------kpi_b_bnbw_kpi_2_2"
    kpi_total_kpi_2_3(begin_day, end_day)
    print "--------kpi_total_kpi_2_3"
    kpi_a_level_kpi_2_3(begin_day, end_day)
    print "--------kpi_a_level_kpi_2_3"
    kpi_b_level_kpi_2_3(begin_day, end_day)
    print "--------kpi_b_level_kpi_2_3"
    kpi_agetype_kpi_2_3(begin_day, end_day)
    print "--------kpi_agetype_kpi_2_3"
    kpi_bnbw_kpi_2_3(begin_day, end_day)
    print "--------kpi_bnbw_kpi_2_3"
    kpi_a_agetype_kpi_2_3(begin_day, end_day)
    print "--------kpi_a_agetype_kpi_2_3"
    kpi_a_bnbw_kpi_2_3(begin_day, end_day)
    print "--------kpi_a_bnbw_kpi_2_3"
    kpi_b_agetype_kpi_2_3(begin_day, end_day)
    print "--------kpi_b_agetype_kpi_2_3"
    kpi_b_bnbw_kpi_2_3(begin_day, end_day)
    print "--------kpi_b_bnbw_kpi_2_3"
    kpi_total_kpi_2_4(begin_day, end_day)
    print "--------kpi_total_kpi_2_4"
    kpi_a_level_kpi_2_4(begin_day, end_day)
    print "--------kpi_a_level_kpi_2_4"
    kpi_b_level_kpi_2_4(begin_day, end_day)
    print "--------kpi_b_level_kpi_2_4"
    kpi_agetype_kpi_2_4(begin_day, end_day)
    print "--------kpi_agetype_kpi_2_4"
    kpi_bnbw_kpi_2_4(begin_day, end_day)
    print "--------kpi_bnbw_kpi_2_4"
    kpi_a_agetype_kpi_2_4(begin_day, end_day)
    print "--------kpi_a_agetype_kpi_2_4"
    kpi_a_bnbw_kpi_2_4(begin_day, end_day)
    print "--------kpi_a_bnbw_kpi_2_4"
    kpi_b_agetype_kpi_2_4(begin_day, end_day)
    print "--------kpi_b_agetype_kpi_2_4"
    kpi_b_bnbw_kpi_2_4(begin_day, end_day)
    print "--------kpi_b_bnbw_kpi_2_4"
    kpi_total_kpi_2_41(begin_day, end_day)
    print "--------kpi_total_kpi_2_41"
    kpi_a_level_kpi_2_41(begin_day, end_day)
    print "--------kpi_a_level_kpi_2_41"
    kpi_b_level_kpi_2_41(begin_day, end_day)
    print "--------kpi_b_level_kpi_2_41"
    kpi_agetype_kpi_2_41(begin_day, end_day)
    print "--------kpi_agetype_kpi_2_41"
    kpi_bnbw_kpi_2_41(begin_day, end_day)
    print "--------kpi_bnbw_kpi_2_41"
    kpi_a_agetype_kpi_2_41(begin_day, end_day)
    print "--------kpi_a_agetype_kpi_2_41"
    kpi_a_bnbw_kpi_2_41(begin_day, end_day)
    print "--------kpi_a_bnbw_kpi_2_41"
    kpi_b_agetype_kpi_2_41(begin_day, end_day)
    print "--------kpi_b_agetype_kpi_2_41"
    kpi_b_bnbw_kpi_2_41(begin_day, end_day)
    print "--------kpi_b_bnbw_kpi_2_41"
    kpi_total_kpi_2_42(begin_day, end_day)
    print "--------kpi_total_kpi_2_42"
    kpi_a_level_kpi_2_42(begin_day, end_day)
    print "--------kpi_a_level_kpi_2_42"
    kpi_b_level_kpi_2_42(begin_day, end_day)
    print "--------kpi_b_level_kpi_2_42"
    kpi_agetype_kpi_2_42(begin_day, end_day)
    print "--------kpi_agetype_kpi_2_42"
    kpi_bnbw_kpi_2_42(begin_day, end_day)
    print "--------kpi_bnbw_kpi_2_42"
    kpi_a_agetype_kpi_2_42(begin_day, end_day)
    print "--------kpi_a_agetype_kpi_2_42"
    kpi_a_bnbw_kpi_2_42(begin_day, end_day)
    print "--------kpi_a_bnbw_kpi_2_42"
    kpi_b_agetype_kpi_2_42(begin_day, end_day)
    print "--------kpi_b_agetype_kpi_2_42"
    kpi_b_bnbw_kpi_2_42(begin_day, end_day)
    print "--------kpi_b_bnbw_kpi_2_42"
    kpi_total_kpi_3(begin_day, end_day)
    print "--------kpi_total_kpi_3"
    kpi_a_level_kpi_3(begin_day, end_day)
    print "--------kpi_a_level_kpi_3"
    kpi_b_level_kpi_3(begin_day, end_day)
    print "--------kpi_b_level_kpi_3"
    kpi_agetype_kpi_3(begin_day, end_day)
    print "--------kpi_agetype_kpi_3"
    kpi_bnbw_kpi_3(begin_day, end_day)
    print "--------kpi_bnbw_kpi_3"
    kpi_a_agetype_kpi_3(begin_day, end_day)
    print "--------kpi_a_agetype_kpi_3"
    kpi_a_bnbw_kpi_3(begin_day, end_day)
    print "--------kpi_a_bnbw_kpi_3"
    kpi_b_agetype_kpi_3(begin_day, end_day)
    print "--------kpi_b_agetype_kpi_3"
    kpi_b_bnbw_kpi_3(begin_day, end_day)
    print "--------kpi_b_bnbw_kpi_3"
    kpi_total_kpi_3_1(begin_day, end_day)
    print "--------kpi_total_kpi_3_1"
    kpi_a_level_kpi_3_1(begin_day, end_day)
    print "--------kpi_a_level_kpi_3_1"
    kpi_b_level_kpi_3_1(begin_day, end_day)
    print "--------kpi_b_level_kpi_3_1"
    kpi_agetype_kpi_3_1(begin_day, end_day)
    print "--------kpi_agetype_kpi_3_1"
    kpi_bnbw_kpi_3_1(begin_day, end_day)
    print "--------kpi_bnbw_kpi_3_1"
    kpi_a_agetype_kpi_3_1(begin_day, end_day)
    print "--------kpi_a_agetype_kpi_3_1"
    kpi_a_bnbw_kpi_3_1(begin_day, end_day)
    print "--------kpi_a_bnbw_kpi_3_1"
    kpi_b_agetype_kpi_3_1(begin_day, end_day)
    print "--------kpi_b_agetype_kpi_3_1"
    kpi_b_bnbw_kpi_3_1(begin_day, end_day)
    print "--------kpi_b_bnbw_kpi_3_1"
    kpi_total_kpi_3_12(begin_day, end_day)
    print "--------kpi_total_kpi_3_12"
    kpi_a_level_kpi_3_12(begin_day, end_day)
    print "--------kpi_a_level_kpi_3_12"
    kpi_b_level_kpi_3_12(begin_day, end_day)
    print "--------kpi_b_level_kpi_3_12"
    kpi_agetype_kpi_3_12(begin_day, end_day)
    print "--------kpi_agetype_kpi_3_12"
    kpi_bnbw_kpi_3_12(begin_day, end_day)
    print "--------kpi_bnbw_kpi_3_12"
    kpi_a_agetype_kpi_3_12(begin_day, end_day)
    print "--------kpi_a_agetype_kpi_3_12"
    kpi_a_bnbw_kpi_3_12(begin_day, end_day)
    print "--------kpi_a_bnbw_kpi_3_12"
    kpi_b_agetype_kpi_3_12(begin_day, end_day)
    print "--------kpi_b_agetype_kpi_3_12"
    kpi_b_bnbw_kpi_3_12(begin_day, end_day)
    print "--------kpi_b_bnbw_kpi_3_12"
    kpi_total_kpi_3_2(begin_day, end_day)
    print "--------kpi_total_kpi_3_2"
    kpi_a_level_kpi_3_2(begin_day, end_day)
    print "--------kpi_a_level_kpi_3_2"
    kpi_b_level_kpi_3_2(begin_day, end_day)
    print "--------kpi_b_level_kpi_3_2"
    kpi_agetype_kpi_3_2(begin_day, end_day)
    print "--------kpi_agetype_kpi_3_2"
    kpi_bnbw_kpi_3_2(begin_day, end_day)
    print "--------kpi_bnbw_kpi_3_2"
    kpi_a_agetype_kpi_3_2(begin_day, end_day)
    print "--------kpi_a_agetype_kpi_3_2"
    kpi_a_bnbw_kpi_3_2(begin_day, end_day)
    print "--------kpi_a_bnbw_kpi_3_2"
    kpi_b_agetype_kpi_3_2(begin_day, end_day)
    print "--------kpi_b_agetype_kpi_3_2"
    kpi_b_bnbw_kpi_3_2(begin_day, end_day)
    print "--------kpi_b_bnbw_kpi_3_2"
    kpi_total_kpi_3_4(begin_day, end_day)
    print "--------kpi_total_kpi_3_4"
    kpi_a_level_kpi_3_4(begin_day, end_day)
    print "--------kpi_a_level_kpi_3_4"
    kpi_b_level_kpi_3_4(begin_day, end_day)
    print "--------kpi_b_level_kpi_3_4"
    kpi_agetype_kpi_3_4(begin_day, end_day)
    print "--------kpi_agetype_kpi_3_4"
    kpi_bnbw_kpi_3_4(begin_day, end_day)
    print "--------kpi_bnbw_kpi_3_4"
    kpi_a_agetype_kpi_3_4(begin_day, end_day)
    print "--------kpi_a_agetype_kpi_3_4"
    kpi_a_bnbw_kpi_3_4(begin_day, end_day)
    print "--------kpi_a_bnbw_kpi_3_4"
    kpi_b_agetype_kpi_3_4(begin_day, end_day)
    print "--------kpi_b_agetype_kpi_3_4"
    kpi_b_bnbw_kpi_3_4(begin_day, end_day)
    print "--------kpi_b_bnbw_kpi_3_4"
    kpi_total_kpi_3_5(begin_day, end_day)
    print "--------kpi_total_kpi_3_5"
    kpi_a_level_kpi_3_5(begin_day, end_day)
    print "--------kpi_a_level_kpi_3_5"
    kpi_b_level_kpi_3_5(begin_day, end_day)
    print "--------kpi_b_level_kpi_3_5"
    kpi_agetype_kpi_3_5(begin_day, end_day)
    print "--------kpi_agetype_kpi_3_5"
    kpi_bnbw_kpi_3_5(begin_day, end_day)
    print "--------kpi_bnbw_kpi_3_5"
    kpi_a_agetype_kpi_3_5(begin_day, end_day)
    print "--------kpi_a_agetype_kpi_3_5"
    kpi_a_bnbw_kpi_3_5(begin_day, end_day)
    print "--------kpi_a_bnbw_kpi_3_5"
    kpi_b_agetype_kpi_3_5(begin_day, end_day)
    print "--------kpi_b_agetype_kpi_3_5"
    kpi_b_bnbw_kpi_3_5(begin_day, end_day)
    print "--------kpi_b_bnbw_kpi_3_5"
    kpi_total_kpi_3_51(begin_day, end_day)
    print "--------kpi_total_kpi_3_51"
    kpi_a_level_kpi_3_51(begin_day, end_day)
    print "--------kpi_a_level_kpi_3_51"
    kpi_b_level_kpi_3_51(begin_day, end_day)
    print "--------kpi_b_level_kpi_3_51"
    kpi_agetype_kpi_3_51(begin_day, end_day)
    print "--------kpi_agetype_kpi_3_51"
    kpi_bnbw_kpi_3_51(begin_day, end_day)
    print "--------kpi_bnbw_kpi_3_51"
    kpi_a_agetype_kpi_3_51(begin_day, end_day)
    print "--------kpi_a_agetype_kpi_3_51"
    kpi_a_bnbw_kpi_3_51(begin_day, end_day)
    print "--------kpi_a_bnbw_kpi_3_51"
    kpi_b_agetype_kpi_3_51(begin_day, end_day)
    print "--------kpi_b_agetype_kpi_3_51"
    kpi_b_bnbw_kpi_3_51(begin_day, end_day)
    print "--------kpi_b_bnbw_kpi_3_51"
    kpi_total_kpi_3_52(begin_day, end_day)
    print "--------kpi_total_kpi_3_52"
    kpi_a_level_kpi_3_52(begin_day, end_day)
    print "--------kpi_a_level_kpi_3_52"
    kpi_b_level_kpi_3_52(begin_day, end_day)
    print "--------kpi_b_level_kpi_3_52"
    kpi_agetype_kpi_3_52(begin_day, end_day)
    print "--------kpi_agetype_kpi_3_52"
    kpi_bnbw_kpi_3_52(begin_day, end_day)
    print "--------kpi_bnbw_kpi_3_52"
    kpi_a_agetype_kpi_3_52(begin_day, end_day)
    print "--------kpi_a_agetype_kpi_3_52"
    kpi_a_bnbw_kpi_3_52(begin_day, end_day)
    print "--------kpi_a_bnbw_kpi_3_52"
    kpi_b_agetype_kpi_3_52(begin_day, end_day)
    print "--------kpi_b_agetype_kpi_3_52"
    kpi_b_bnbw_kpi_3_52(begin_day, end_day)
    print "--------kpi_b_bnbw_kpi_3_52"
    kpi_total_kpi_3_53(begin_day, end_day)
    print "--------kpi_total_kpi_3_53"
    kpi_a_level_kpi_3_53(begin_day, end_day)
    print "--------kpi_a_level_kpi_3_53"
    kpi_b_level_kpi_3_53(begin_day, end_day)
    print "--------kpi_b_level_kpi_3_53"
    kpi_agetype_kpi_3_53(begin_day, end_day)
    print "--------kpi_agetype_kpi_3_53"
    kpi_bnbw_kpi_3_53(begin_day, end_day)
    print "--------kpi_bnbw_kpi_3_53"
    kpi_a_agetype_kpi_3_53(begin_day, end_day)
    print "--------kpi_a_agetype_kpi_3_53"
    kpi_a_bnbw_kpi_3_53(begin_day, end_day)
    print "--------kpi_a_bnbw_kpi_3_53"
    kpi_b_agetype_kpi_3_53(begin_day, end_day)
    print "--------kpi_b_agetype_kpi_3_53"
    kpi_b_bnbw_kpi_3_53(begin_day, end_day)
    print "--------kpi_b_bnbw_kpi_3_53"
    kpi_total_kpi_3_54(begin_day, end_day)
    print "--------kpi_total_kpi_3_54"
    kpi_a_level_kpi_3_54(begin_day, end_day)
    print "--------kpi_a_level_kpi_3_54"
    kpi_b_level_kpi_3_54(begin_day, end_day)
    print "--------kpi_b_level_kpi_3_54"
    kpi_agetype_kpi_3_54(begin_day, end_day)
    print "--------kpi_agetype_kpi_3_54"
    kpi_bnbw_kpi_3_54(begin_day, end_day)
    print "--------kpi_bnbw_kpi_3_54"
    kpi_a_agetype_kpi_3_54(begin_day, end_day)
    print "--------kpi_a_agetype_kpi_3_54"
    kpi_a_bnbw_kpi_3_54(begin_day, end_day)
    print "--------kpi_a_bnbw_kpi_3_54"
    kpi_b_agetype_kpi_3_54(begin_day, end_day)
    print "--------kpi_b_agetype_kpi_3_54"
    kpi_b_bnbw_kpi_3_54(begin_day, end_day)
    print "--------kpi_b_bnbw_kpi_3_54"
    kpi_total_kpi_3_6(begin_day, end_day)
    print "--------kpi_total_kpi_3_6"
    kpi_a_level_kpi_3_6(begin_day, end_day)
    print "--------kpi_a_level_kpi_3_6"
    kpi_b_level_kpi_3_6(begin_day, end_day)
    print "--------kpi_b_level_kpi_3_6"
    kpi_agetype_kpi_3_6(begin_day, end_day)
    print "--------kpi_agetype_kpi_3_6"
    kpi_bnbw_kpi_3_6(begin_day, end_day)
    print "--------kpi_bnbw_kpi_3_6"
    kpi_a_agetype_kpi_3_6(begin_day, end_day)
    print "--------kpi_a_agetype_kpi_3_6"
    kpi_a_bnbw_kpi_3_6(begin_day, end_day)
    print "--------kpi_a_bnbw_kpi_3_6"
    kpi_b_agetype_kpi_3_6(begin_day, end_day)
    print "--------kpi_b_agetype_kpi_3_6"
    kpi_b_bnbw_kpi_3_6(begin_day, end_day)
    print "--------kpi_b_bnbw_kpi_3_6"
    kpi_total_kpi_3_61(begin_day, end_day)
    print "--------kpi_total_kpi_3_61"
    kpi_a_level_kpi_3_61(begin_day, end_day)
    print "--------kpi_a_level_kpi_3_61"
    kpi_b_level_kpi_3_61(begin_day, end_day)
    print "--------kpi_b_level_kpi_3_61"
    kpi_agetype_kpi_3_61(begin_day, end_day)
    print "--------kpi_agetype_kpi_3_61"
    kpi_bnbw_kpi_3_61(begin_day, end_day)
    print "--------kpi_bnbw_kpi_3_61"
    kpi_a_agetype_kpi_3_61(begin_day, end_day)
    print "--------kpi_a_agetype_kpi_3_61"
    kpi_a_bnbw_kpi_3_61(begin_day, end_day)
    print "--------kpi_a_bnbw_kpi_3_61"
    kpi_b_agetype_kpi_3_61(begin_day, end_day)
    print "--------kpi_b_agetype_kpi_3_61"
    kpi_b_bnbw_kpi_3_61(begin_day, end_day)
    print "--------kpi_b_bnbw_kpi_3_61"
    kpi_total_kpi_3_62(begin_day, end_day)
    print "--------kpi_total_kpi_3_62"
    kpi_a_level_kpi_3_62(begin_day, end_day)
    print "--------kpi_a_level_kpi_3_62"
    kpi_b_level_kpi_3_62(begin_day, end_day)
    print "--------kpi_b_level_kpi_3_62"
    kpi_agetype_kpi_3_62(begin_day, end_day)
    print "--------kpi_agetype_kpi_3_62"
    kpi_bnbw_kpi_3_62(begin_day, end_day)
    print "--------kpi_bnbw_kpi_3_62"
    kpi_a_agetype_kpi_3_62(begin_day, end_day)
    print "--------kpi_a_agetype_kpi_3_62"
    kpi_a_bnbw_kpi_3_62(begin_day, end_day)
    print "--------kpi_a_bnbw_kpi_3_62"
    kpi_b_agetype_kpi_3_62(begin_day, end_day)
    print "--------kpi_b_agetype_kpi_3_62"
    kpi_b_bnbw_kpi_3_62(begin_day, end_day)
    print "--------kpi_b_bnbw_kpi_3_62"
    kpi_total_kpi_0(begin_day, end_day)
    print "--------kpi_total_kpi_0"
    kpi_a_level_kpi_0(begin_day, end_day)
    print "--------kpi_a_level_kpi_0"
    kpi_b_level_kpi_0(begin_day, end_day)
    print "--------kpi_b_level_kpi_0"
    kpi_agetype_kpi_0(begin_day, end_day)
    print "--------kpi_agetype_kpi_0"
    kpi_bnbw_kpi_0(begin_day, end_day)
    print "--------kpi_bnbw_kpi_0"
    kpi_a_agetype_kpi_0(begin_day, end_day)
    print "--------kpi_a_agetype_kpi_0"
    kpi_a_bnbw_kpi_0(begin_day, end_day)
    print "--------kpi_a_bnbw_kpi_0"
    kpi_b_agetype_kpi_0(begin_day, end_day)
    print "--------kpi_b_agetype_kpi_0"
    kpi_b_bnbw_kpi_0(begin_day, end_day)
    print "--------kpi_b_bnbw_kpi_0"
    kpi_total_kpi_0_1(begin_day, end_day)
    print "--------kpi_total_kpi_0_1"
    kpi_a_level_kpi_0_1(begin_day, end_day)
    print "--------kpi_a_level_kpi_0_1"
    kpi_b_level_kpi_0_1(begin_day, end_day)
    print "--------kpi_b_level_kpi_0_1"
    kpi_agetype_kpi_0_1(begin_day, end_day)
    print "--------kpi_agetype_kpi_0_1"
    kpi_bnbw_kpi_0_1(begin_day, end_day)
    print "--------kpi_bnbw_kpi_0_1"
    kpi_a_agetype_kpi_0_1(begin_day, end_day)
    print "--------kpi_a_agetype_kpi_0_1"
    kpi_a_bnbw_kpi_0_1(begin_day, end_day)
    print "--------kpi_a_bnbw_kpi_0_1"
    kpi_b_agetype_kpi_0_1(begin_day, end_day)
    print "--------kpi_b_agetype_kpi_0_1"
    kpi_b_bnbw_kpi_0_1(begin_day, end_day)
    print "--------kpi_b_bnbw_kpi_0_1"
    kpi_total_kpi_0_2(begin_day, end_day)
    print "--------kpi_total_kpi_0_2"
    kpi_a_level_kpi_0_2(begin_day, end_day)
    print "--------kpi_a_level_kpi_0_2"
    kpi_b_level_kpi_0_2(begin_day, end_day)
    print "--------kpi_b_level_kpi_0_2"
    kpi_agetype_kpi_0_2(begin_day, end_day)
    print "--------kpi_agetype_kpi_0_2"
    kpi_bnbw_kpi_0_2(begin_day, end_day)
    print "--------kpi_bnbw_kpi_0_2"
    kpi_a_agetype_kpi_0_2(begin_day, end_day)
    print "--------kpi_a_agetype_kpi_0_2"
    kpi_a_bnbw_kpi_0_2(begin_day, end_day)
    print "--------kpi_a_bnbw_kpi_0_2"
    kpi_b_agetype_kpi_0_2(begin_day, end_day)
    print "--------kpi_b_agetype_kpi_0_2"
    kpi_b_bnbw_kpi_0_2(begin_day, end_day)
    print "--------kpi_b_bnbw_kpi_0_2"
    kpi_total_kpi_0_3(begin_day, end_day)
    print "--------kpi_total_kpi_0_3"
    kpi_a_level_kpi_0_3(begin_day, end_day)
    print "--------kpi_a_level_kpi_0_3"
    kpi_b_level_kpi_0_3(begin_day, end_day)
    print "--------kpi_b_level_kpi_0_3"
    kpi_agetype_kpi_0_3(begin_day, end_day)
    print "--------kpi_agetype_kpi_0_3"
    kpi_bnbw_kpi_0_3(begin_day, end_day)
    print "--------kpi_bnbw_kpi_0_3"
    kpi_a_agetype_kpi_0_3(begin_day, end_day)
    print "--------kpi_a_agetype_kpi_0_3"
    kpi_a_bnbw_kpi_0_3(begin_day, end_day)
    print "--------kpi_a_bnbw_kpi_0_3"
    kpi_b_agetype_kpi_0_3(begin_day, end_day)
    print "--------kpi_b_agetype_kpi_0_3"
    kpi_b_bnbw_kpi_0_3(begin_day, end_day)
    print "--------kpi_b_bnbw_kpi_0_3"
    kpi_total_kpi_0_4(begin_day, end_day)
    print "--------kpi_total_kpi_0_4"
    kpi_a_level_kpi_0_4(begin_day, end_day)
    print "--------kpi_a_level_kpi_0_4"
    kpi_b_level_kpi_0_4(begin_day, end_day)
    print "--------kpi_b_level_kpi_0_4"
    kpi_agetype_kpi_0_4(begin_day, end_day)
    print "--------kpi_agetype_kpi_0_4"
    kpi_bnbw_kpi_0_4(begin_day, end_day)
    print "--------kpi_bnbw_kpi_0_4"
    kpi_a_agetype_kpi_0_4(begin_day, end_day)
    print "--------kpi_a_agetype_kpi_0_4"
    kpi_a_bnbw_kpi_0_4(begin_day, end_day)
    print "--------kpi_a_bnbw_kpi_0_4"
    kpi_b_agetype_kpi_0_4(begin_day, end_day)
    print "--------kpi_b_agetype_kpi_0_4"
    kpi_b_bnbw_kpi_0_4(begin_day, end_day)
    print "--------kpi_b_bnbw_kpi_0_4"
    kpi_total_kpi_0_5(begin_day, end_day)
    print "--------kpi_total_kpi_0_5"
    kpi_a_level_kpi_0_5(begin_day, end_day)
    print "--------kpi_a_level_kpi_0_5"
    kpi_b_level_kpi_0_5(begin_day, end_day)
    print "--------kpi_b_level_kpi_0_5"
    kpi_agetype_kpi_0_5(begin_day, end_day)
    print "--------kpi_agetype_kpi_0_5"
    kpi_bnbw_kpi_0_5(begin_day, end_day)
    print "--------kpi_bnbw_kpi_0_5"
    kpi_a_agetype_kpi_0_5(begin_day, end_day)
    print "--------kpi_a_agetype_kpi_0_5"
    kpi_a_bnbw_kpi_0_5(begin_day, end_day)
    print "--------kpi_a_bnbw_kpi_0_5"
    kpi_b_agetype_kpi_0_5(begin_day, end_day)
    print "--------kpi_b_agetype_kpi_0_5"
    kpi_b_bnbw_kpi_0_5(begin_day, end_day)
    print "--------kpi_b_bnbw_kpi_0_5"
    kpi_total_kpi_4(begin_day, end_day)
    print "--------kpi_total_kpi_4"
    kpi_a_level_kpi_4(begin_day, end_day)
    print "--------kpi_a_level_kpi_4"
    kpi_b_level_kpi_4(begin_day, end_day)
    print "--------kpi_b_level_kpi_4"
    kpi_agetype_kpi_4(begin_day, end_day)
    print "--------kpi_agetype_kpi_4"
    kpi_bnbw_kpi_4(begin_day, end_day)
    print "--------kpi_bnbw_kpi_4"
    kpi_a_agetype_kpi_4(begin_day, end_day)
    print "--------kpi_a_agetype_kpi_4"
    kpi_a_bnbw_kpi_4(begin_day, end_day)
    print "--------kpi_a_bnbw_kpi_4"
    kpi_b_agetype_kpi_4(begin_day, end_day)
    print "--------kpi_b_agetype_kpi_4"
    kpi_b_bnbw_kpi_4(begin_day, end_day)
    print "--------kpi_b_bnbw_kpi_4"
    kpi_total_kpi_4_1(begin_day, end_day)
    print "--------kpi_total_kpi_4_1"
    kpi_a_level_kpi_4_1(begin_day, end_day)
    print "--------kpi_a_level_kpi_4_1"
    kpi_b_level_kpi_4_1(begin_day, end_day)
    print "--------kpi_b_level_kpi_4_1"
    kpi_agetype_kpi_4_1(begin_day, end_day)
    print "--------kpi_agetype_kpi_4_1"
    kpi_bnbw_kpi_4_1(begin_day, end_day)
    print "--------kpi_bnbw_kpi_4_1"
    kpi_a_agetype_kpi_4_1(begin_day, end_day)
    print "--------kpi_a_agetype_kpi_4_1"
    kpi_a_bnbw_kpi_4_1(begin_day, end_day)
    print "--------kpi_a_bnbw_kpi_4_1"
    kpi_b_agetype_kpi_4_1(begin_day, end_day)
    print "--------kpi_b_agetype_kpi_4_1"
    kpi_b_bnbw_kpi_4_1(begin_day, end_day)
    print "--------kpi_b_bnbw_kpi_4_1"
    kpi_total_kpi_4_11(begin_day, end_day)
    print "--------kpi_total_kpi_4_11"
    kpi_a_level_kpi_4_11(begin_day, end_day)
    print "--------kpi_a_level_kpi_4_11"
    kpi_b_level_kpi_4_11(begin_day, end_day)
    print "--------kpi_b_level_kpi_4_11"
    kpi_agetype_kpi_4_11(begin_day, end_day)
    print "--------kpi_agetype_kpi_4_11"
    kpi_bnbw_kpi_4_11(begin_day, end_day)
    print "--------kpi_bnbw_kpi_4_11"
    kpi_a_agetype_kpi_4_11(begin_day, end_day)
    print "--------kpi_a_agetype_kpi_4_11"
    kpi_a_bnbw_kpi_4_11(begin_day, end_day)
    print "--------kpi_a_bnbw_kpi_4_11"
    kpi_b_agetype_kpi_4_11(begin_day, end_day)
    print "--------kpi_b_agetype_kpi_4_11"
    kpi_b_bnbw_kpi_4_11(begin_day, end_day)
    print "--------kpi_b_bnbw_kpi_4_11"
    kpi_total_kpi_4_12(begin_day, end_day)
    print "--------kpi_total_kpi_4_12"
    kpi_a_level_kpi_4_12(begin_day, end_day)
    print "--------kpi_a_level_kpi_4_12"
    kpi_b_level_kpi_4_12(begin_day, end_day)
    print "--------kpi_b_level_kpi_4_12"
    kpi_agetype_kpi_4_12(begin_day, end_day)
    print "--------kpi_agetype_kpi_4_12"
    kpi_bnbw_kpi_4_12(begin_day, end_day)
    print "--------kpi_bnbw_kpi_4_12"
    kpi_a_agetype_kpi_4_12(begin_day, end_day)
    print "--------kpi_a_agetype_kpi_4_12"
    kpi_a_bnbw_kpi_4_12(begin_day, end_day)
    print "--------kpi_a_bnbw_kpi_4_12"
    kpi_b_agetype_kpi_4_12(begin_day, end_day)
    print "--------kpi_b_agetype_kpi_4_12"
    kpi_b_bnbw_kpi_4_12(begin_day, end_day)
    print "--------kpi_b_bnbw_kpi_4_12"
    kpi_total_kpi_4_13(begin_day, end_day)
    print "--------kpi_total_kpi_4_13"
    kpi_a_level_kpi_4_13(begin_day, end_day)
    print "--------kpi_a_level_kpi_4_13"
    kpi_b_level_kpi_4_13(begin_day, end_day)
    print "--------kpi_b_level_kpi_4_13"
    kpi_agetype_kpi_4_13(begin_day, end_day)
    print "--------kpi_agetype_kpi_4_13"
    kpi_bnbw_kpi_4_13(begin_day, end_day)
    print "--------kpi_bnbw_kpi_4_13"
    kpi_a_agetype_kpi_4_13(begin_day, end_day)
    print "--------kpi_a_agetype_kpi_4_13"
    kpi_a_bnbw_kpi_4_13(begin_day, end_day)
    print "--------kpi_a_bnbw_kpi_4_13"
    kpi_b_agetype_kpi_4_13(begin_day, end_day)
    print "--------kpi_b_agetype_kpi_4_13"
    kpi_b_bnbw_kpi_4_13(begin_day, end_day)
    print "--------kpi_b_bnbw_kpi_4_13"
    kpi_total_kpi_4_2(begin_day, end_day)
    print "--------kpi_total_kpi_4_2"
    kpi_a_level_kpi_4_2(begin_day, end_day)
    print "--------kpi_a_level_kpi_4_2"
    kpi_b_level_kpi_4_2(begin_day, end_day)
    print "--------kpi_b_level_kpi_4_2"
    kpi_agetype_kpi_4_2(begin_day, end_day)
    print "--------kpi_agetype_kpi_4_2"
    kpi_bnbw_kpi_4_2(begin_day, end_day)
    print "--------kpi_bnbw_kpi_4_2"
    kpi_a_agetype_kpi_4_2(begin_day, end_day)
    print "--------kpi_a_agetype_kpi_4_2"
    kpi_a_bnbw_kpi_4_2(begin_day, end_day)
    print "--------kpi_a_bnbw_kpi_4_2"
    kpi_b_agetype_kpi_4_2(begin_day, end_day)
    print "--------kpi_b_agetype_kpi_4_2"
    kpi_b_bnbw_kpi_4_2(begin_day, end_day)
    print "--------kpi_b_bnbw_kpi_4_2"
    kpi_total_kpi_4_3(begin_day, end_day)
    print "--------kpi_total_kpi_4_3"
    kpi_a_level_kpi_4_3(begin_day, end_day)
    print "--------kpi_a_level_kpi_4_3"
    kpi_b_level_kpi_4_3(begin_day, end_day)
    print "--------kpi_b_level_kpi_4_3"
    kpi_agetype_kpi_4_3(begin_day, end_day)
    print "--------kpi_agetype_kpi_4_3"
    kpi_bnbw_kpi_4_3(begin_day, end_day)
    print "--------kpi_bnbw_kpi_4_3"
    kpi_a_agetype_kpi_4_3(begin_day, end_day)
    print "--------kpi_a_agetype_kpi_4_3"
    kpi_a_bnbw_kpi_4_3(begin_day, end_day)
    print "--------kpi_a_bnbw_kpi_4_3"
    kpi_b_agetype_kpi_4_3(begin_day, end_day)
    print "--------kpi_b_agetype_kpi_4_3"
    kpi_b_bnbw_kpi_4_3(begin_day, end_day)
    print "--------kpi_b_bnbw_kpi_4_3"
    kpi_total_kpi_4_31(begin_day, end_day)
    print "--------kpi_total_kpi_4_31"
    kpi_a_level_kpi_4_31(begin_day, end_day)
    print "--------kpi_a_level_kpi_4_31"
    kpi_b_level_kpi_4_31(begin_day, end_day)
    print "--------kpi_b_level_kpi_4_31"
    kpi_agetype_kpi_4_31(begin_day, end_day)
    print "--------kpi_agetype_kpi_4_31"
    kpi_bnbw_kpi_4_31(begin_day, end_day)
    print "--------kpi_bnbw_kpi_4_31"
    kpi_a_agetype_kpi_4_31(begin_day, end_day)
    print "--------kpi_a_agetype_kpi_4_31"
    kpi_a_bnbw_kpi_4_31(begin_day, end_day)
    print "--------kpi_a_bnbw_kpi_4_31"
    kpi_b_agetype_kpi_4_31(begin_day, end_day)
    print "--------kpi_b_agetype_kpi_4_31"
    kpi_b_bnbw_kpi_4_31(begin_day, end_day)
    print "--------kpi_b_bnbw_kpi_4_31"
    kpi_total_kpi_4_32(begin_day, end_day)
    print "--------kpi_total_kpi_4_32"
    kpi_a_level_kpi_4_32(begin_day, end_day)
    print "--------kpi_a_level_kpi_4_32"
    kpi_b_level_kpi_4_32(begin_day, end_day)
    print "--------kpi_b_level_kpi_4_32"
    kpi_agetype_kpi_4_32(begin_day, end_day)
    print "--------kpi_agetype_kpi_4_32"
    kpi_bnbw_kpi_4_32(begin_day, end_day)
    print "--------kpi_bnbw_kpi_4_32"
    kpi_a_agetype_kpi_4_32(begin_day, end_day)
    print "--------kpi_a_agetype_kpi_4_32"
    kpi_a_bnbw_kpi_4_32(begin_day, end_day)
    print "--------kpi_a_bnbw_kpi_4_32"
    kpi_b_agetype_kpi_4_32(begin_day, end_day)
    print "--------kpi_b_agetype_kpi_4_32"
    kpi_b_bnbw_kpi_4_32(begin_day, end_day)
    print "--------kpi_b_bnbw_kpi_4_32"
    kpi_total_kpi_4_33(begin_day, end_day)
    print "--------kpi_total_kpi_4_33"
    kpi_a_level_kpi_4_33(begin_day, end_day)
    print "--------kpi_a_level_kpi_4_33"
    kpi_b_level_kpi_4_33(begin_day, end_day)
    print "--------kpi_b_level_kpi_4_33"
    kpi_agetype_kpi_4_33(begin_day, end_day)
    print "--------kpi_agetype_kpi_4_33"
    kpi_bnbw_kpi_4_33(begin_day, end_day)
    print "--------kpi_bnbw_kpi_4_33"
    kpi_a_agetype_kpi_4_33(begin_day, end_day)
    print "--------kpi_a_agetype_kpi_4_33"
    kpi_a_bnbw_kpi_4_33(begin_day, end_day)
    print "--------kpi_a_bnbw_kpi_4_33"
    kpi_b_agetype_kpi_4_33(begin_day, end_day)
    print "--------kpi_b_agetype_kpi_4_33"
    kpi_b_bnbw_kpi_4_33(begin_day, end_day)
    print "--------kpi_b_bnbw_kpi_4_33"
    kpi_total_kpi_4_34(begin_day, end_day)
    print "--------kpi_total_kpi_4_34"
    kpi_a_level_kpi_4_34(begin_day, end_day)
    print "--------kpi_a_level_kpi_4_34"
    kpi_b_level_kpi_4_34(begin_day, end_day)
    print "--------kpi_b_level_kpi_4_34"
    kpi_agetype_kpi_4_34(begin_day, end_day)
    print "--------kpi_agetype_kpi_4_34"
    kpi_bnbw_kpi_4_34(begin_day, end_day)
    print "--------kpi_bnbw_kpi_4_34"
    kpi_a_agetype_kpi_4_34(begin_day, end_day)
    print "--------kpi_a_agetype_kpi_4_34"
    kpi_a_bnbw_kpi_4_34(begin_day, end_day)
    print "--------kpi_a_bnbw_kpi_4_34"
    kpi_b_agetype_kpi_4_34(begin_day, end_day)
    print "--------kpi_b_agetype_kpi_4_34"
    kpi_b_bnbw_kpi_4_34(begin_day, end_day)
    print "--------kpi_b_bnbw_kpi_4_34"
    kpi_total_kpi_4_35(begin_day, end_day)
    print "--------kpi_total_kpi_4_35"
    kpi_a_level_kpi_4_35(begin_day, end_day)
    print "--------kpi_a_level_kpi_4_35"
    kpi_b_level_kpi_4_35(begin_day, end_day)
    print "--------kpi_b_level_kpi_4_35"
    kpi_agetype_kpi_4_35(begin_day, end_day)
    print "--------kpi_agetype_kpi_4_35"
    kpi_bnbw_kpi_4_35(begin_day, end_day)
    print "--------kpi_bnbw_kpi_4_35"
    kpi_a_agetype_kpi_4_35(begin_day, end_day)
    print "--------kpi_a_agetype_kpi_4_35"
    kpi_a_bnbw_kpi_4_35(begin_day, end_day)
    print "--------kpi_a_bnbw_kpi_4_35"
    kpi_b_agetype_kpi_4_35(begin_day, end_day)
    print "--------kpi_b_agetype_kpi_4_35"
    kpi_b_bnbw_kpi_4_35(begin_day, end_day)
    print "--------kpi_b_bnbw_kpi_4_35"
    kpi_total_kpi_4_4(begin_day, end_day)
    print "--------kpi_total_kpi_4_4"
    kpi_a_level_kpi_4_4(begin_day, end_day)
    print "--------kpi_a_level_kpi_4_4"
    kpi_b_level_kpi_4_4(begin_day, end_day)
    print "--------kpi_b_level_kpi_4_4"
    kpi_agetype_kpi_4_4(begin_day, end_day)
    print "--------kpi_agetype_kpi_4_4"
    kpi_bnbw_kpi_4_4(begin_day, end_day)
    print "--------kpi_bnbw_kpi_4_4"
    kpi_a_agetype_kpi_4_4(begin_day, end_day)
    print "--------kpi_a_agetype_kpi_4_4"
    kpi_a_bnbw_kpi_4_4(begin_day, end_day)
    print "--------kpi_a_bnbw_kpi_4_4"
    kpi_b_agetype_kpi_4_4(begin_day, end_day)
    print "--------kpi_b_agetype_kpi_4_4"
    kpi_b_bnbw_kpi_4_4(begin_day, end_day)
    print "--------kpi_b_bnbw_kpi_4_4"
    kpi_total_kpi_5(begin_day, end_day)
    print "--------kpi_total_kpi_5"
    kpi_a_level_kpi_5(begin_day, end_day)
    print "--------kpi_a_level_kpi_5"
    kpi_b_level_kpi_5(begin_day, end_day)
    print "--------kpi_b_level_kpi_5"
    kpi_agetype_kpi_5(begin_day, end_day)
    print "--------kpi_agetype_kpi_5"
    kpi_bnbw_kpi_5(begin_day, end_day)
    print "--------kpi_bnbw_kpi_5"
    kpi_a_agetype_kpi_5(begin_day, end_day)
    print "--------kpi_a_agetype_kpi_5"
    kpi_a_bnbw_kpi_5(begin_day, end_day)
    print "--------kpi_a_bnbw_kpi_5"
    kpi_b_agetype_kpi_5(begin_day, end_day)
    print "--------kpi_b_agetype_kpi_5"
    kpi_b_bnbw_kpi_5(begin_day, end_day)
    print "--------kpi_b_bnbw_kpi_5"
    kpi_total_kpi_5_1(begin_day, end_day)
    print "--------kpi_total_kpi_5_1"
    kpi_a_level_kpi_5_1(begin_day, end_day)
    print "--------kpi_a_level_kpi_5_1"
    kpi_b_level_kpi_5_1(begin_day, end_day)
    print "--------kpi_b_level_kpi_5_1"
    kpi_agetype_kpi_5_1(begin_day, end_day)
    print "--------kpi_agetype_kpi_5_1"
    kpi_bnbw_kpi_5_1(begin_day, end_day)
    print "--------kpi_bnbw_kpi_5_1"
    kpi_a_agetype_kpi_5_1(begin_day, end_day)
    print "--------kpi_a_agetype_kpi_5_1"
    kpi_a_bnbw_kpi_5_1(begin_day, end_day)
    print "--------kpi_a_bnbw_kpi_5_1"
    kpi_b_agetype_kpi_5_1(begin_day, end_day)
    print "--------kpi_b_agetype_kpi_5_1"
    kpi_b_bnbw_kpi_5_1(begin_day, end_day)
    print "--------kpi_b_bnbw_kpi_5_1"
    kpi_total_kpi_5_11(begin_day, end_day)
    print "--------kpi_total_kpi_5_11"
    kpi_a_level_kpi_5_11(begin_day, end_day)
    print "--------kpi_a_level_kpi_5_11"
    kpi_b_level_kpi_5_11(begin_day, end_day)
    print "--------kpi_b_level_kpi_5_11"
    kpi_agetype_kpi_5_11(begin_day, end_day)
    print "--------kpi_agetype_kpi_5_11"
    kpi_bnbw_kpi_5_11(begin_day, end_day)
    print "--------kpi_bnbw_kpi_5_11"
    kpi_a_agetype_kpi_5_11(begin_day, end_day)
    print "--------kpi_a_agetype_kpi_5_11"
    kpi_a_bnbw_kpi_5_11(begin_day, end_day)
    print "--------kpi_a_bnbw_kpi_5_11"
    kpi_b_agetype_kpi_5_11(begin_day, end_day)
    print "--------kpi_b_agetype_kpi_5_11"
    kpi_b_bnbw_kpi_5_11(begin_day, end_day)
    print "--------kpi_b_bnbw_kpi_5_11"
    kpi_total_kpi_5_12(begin_day, end_day)
    print "--------kpi_total_kpi_5_12"
    kpi_a_level_kpi_5_12(begin_day, end_day)
    print "--------kpi_a_level_kpi_5_12"
    kpi_b_level_kpi_5_12(begin_day, end_day)
    print "--------kpi_b_level_kpi_5_12"
    kpi_agetype_kpi_5_12(begin_day, end_day)
    print "--------kpi_agetype_kpi_5_12"
    kpi_bnbw_kpi_5_12(begin_day, end_day)
    print "--------kpi_bnbw_kpi_5_12"
    kpi_a_agetype_kpi_5_12(begin_day, end_day)
    print "--------kpi_a_agetype_kpi_5_12"
    kpi_a_bnbw_kpi_5_12(begin_day, end_day)
    print "--------kpi_a_bnbw_kpi_5_12"
    kpi_b_agetype_kpi_5_12(begin_day, end_day)
    print "--------kpi_b_agetype_kpi_5_12"
    kpi_b_bnbw_kpi_5_12(begin_day, end_day)
    print "--------kpi_b_bnbw_kpi_5_12"
    kpi_total_kpi_5_13(begin_day, end_day)
    print "--------kpi_total_kpi_5_13"
    kpi_a_level_kpi_5_13(begin_day, end_day)
    print "--------kpi_a_level_kpi_5_13"
    kpi_b_level_kpi_5_13(begin_day, end_day)
    print "--------kpi_b_level_kpi_5_13"
    kpi_agetype_kpi_5_13(begin_day, end_day)
    print "--------kpi_agetype_kpi_5_13"
    kpi_bnbw_kpi_5_13(begin_day, end_day)
    print "--------kpi_bnbw_kpi_5_13"
    kpi_a_agetype_kpi_5_13(begin_day, end_day)
    print "--------kpi_a_agetype_kpi_5_13"
    kpi_a_bnbw_kpi_5_13(begin_day, end_day)
    print "--------kpi_a_bnbw_kpi_5_13"
    kpi_b_agetype_kpi_5_13(begin_day, end_day)
    print "--------kpi_b_agetype_kpi_5_13"
    kpi_b_bnbw_kpi_5_13(begin_day, end_day)
    print "--------kpi_b_bnbw_kpi_5_13"
    kpi_total_kpi_5_2(begin_day, end_day)
    print "--------kpi_total_kpi_5_2"
    kpi_a_level_kpi_5_2(begin_day, end_day)
    print "--------kpi_a_level_kpi_5_2"
    kpi_b_level_kpi_5_2(begin_day, end_day)
    print "--------kpi_b_level_kpi_5_2"
    kpi_agetype_kpi_5_2(begin_day, end_day)
    print "--------kpi_agetype_kpi_5_2"
    kpi_bnbw_kpi_5_2(begin_day, end_day)
    print "--------kpi_bnbw_kpi_5_2"
    kpi_a_agetype_kpi_5_2(begin_day, end_day)
    print "--------kpi_a_agetype_kpi_5_2"
    kpi_a_bnbw_kpi_5_2(begin_day, end_day)
    print "--------kpi_a_bnbw_kpi_5_2"
    kpi_b_agetype_kpi_5_2(begin_day, end_day)
    print "--------kpi_b_agetype_kpi_5_2"
    kpi_b_bnbw_kpi_5_2(begin_day, end_day)
    print "--------kpi_b_bnbw_kpi_5_2"
    kpi_total_kpi_5_3(begin_day, end_day)
    print "--------kpi_total_kpi_5_3"
    kpi_a_level_kpi_5_3(begin_day, end_day)
    print "--------kpi_a_level_kpi_5_3"
    kpi_b_level_kpi_5_3(begin_day, end_day)
    print "--------kpi_b_level_kpi_5_3"
    kpi_agetype_kpi_5_3(begin_day, end_day)
    print "--------kpi_agetype_kpi_5_3"
    kpi_bnbw_kpi_5_3(begin_day, end_day)
    print "--------kpi_bnbw_kpi_5_3"
    kpi_a_agetype_kpi_5_3(begin_day, end_day)
    print "--------kpi_a_agetype_kpi_5_3"
    kpi_a_bnbw_kpi_5_3(begin_day, end_day)
    print "--------kpi_a_bnbw_kpi_5_3"
    kpi_b_agetype_kpi_5_3(begin_day, end_day)
    print "--------kpi_b_agetype_kpi_5_3"
    kpi_b_bnbw_kpi_5_3(begin_day, end_day)
    print "--------kpi_b_bnbw_kpi_5_3"
    kpi_total_kpi_5_4(begin_day, end_day)
    print "--------kpi_total_kpi_5_4"
    kpi_a_level_kpi_5_4(begin_day, end_day)
    print "--------kpi_a_level_kpi_5_4"
    kpi_b_level_kpi_5_4(begin_day, end_day)
    print "--------kpi_b_level_kpi_5_4"
    kpi_agetype_kpi_5_4(begin_day, end_day)
    print "--------kpi_agetype_kpi_5_4"
    kpi_bnbw_kpi_5_4(begin_day, end_day)
    print "--------kpi_bnbw_kpi_5_4"
    kpi_a_agetype_kpi_5_4(begin_day, end_day)
    print "--------kpi_a_agetype_kpi_5_4"
    kpi_a_bnbw_kpi_5_4(begin_day, end_day)
    print "--------kpi_a_bnbw_kpi_5_4"
    kpi_b_agetype_kpi_5_4(begin_day, end_day)
    print "--------kpi_b_agetype_kpi_5_4"
    kpi_b_bnbw_kpi_5_4(begin_day, end_day)
    print "--------kpi_b_bnbw_kpi_5_4"
    kpi_total_kpi_5_5(begin_day, end_day)
    print "--------kpi_total_kpi_5_5"
    kpi_a_level_kpi_5_5(begin_day, end_day)
    print "--------kpi_a_level_kpi_5_5"
    kpi_b_level_kpi_5_5(begin_day, end_day)
    print "--------kpi_b_level_kpi_5_5"
    kpi_agetype_kpi_5_5(begin_day, end_day)
    print "--------kpi_agetype_kpi_5_5"
    kpi_bnbw_kpi_5_5(begin_day, end_day)
    print "--------kpi_bnbw_kpi_5_5"
    kpi_a_agetype_kpi_5_5(begin_day, end_day)
    print "--------kpi_a_agetype_kpi_5_5"
    kpi_a_bnbw_kpi_5_5(begin_day, end_day)
    print "--------kpi_a_bnbw_kpi_5_5"
    kpi_b_agetype_kpi_5_5(begin_day, end_day)
    print "--------kpi_b_agetype_kpi_5_5"
    kpi_b_bnbw_kpi_5_5(begin_day, end_day)
    print "--------kpi_b_bnbw_kpi_5_5"
    kpi_total_kpi_5_51(begin_day, end_day)
    print "--------kpi_total_kpi_5_51"
    kpi_a_level_kpi_5_51(begin_day, end_day)
    print "--------kpi_a_level_kpi_5_51"
    kpi_b_level_kpi_5_51(begin_day, end_day)
    print "--------kpi_b_level_kpi_5_51"
    kpi_agetype_kpi_5_51(begin_day, end_day)
    print "--------kpi_agetype_kpi_5_51"
    kpi_bnbw_kpi_5_51(begin_day, end_day)
    print "--------kpi_bnbw_kpi_5_51"
    kpi_a_agetype_kpi_5_51(begin_day, end_day)
    print "--------kpi_a_agetype_kpi_5_51"
    kpi_a_bnbw_kpi_5_51(begin_day, end_day)
    print "--------kpi_a_bnbw_kpi_5_51"
    kpi_b_agetype_kpi_5_51(begin_day, end_day)
    print "--------kpi_b_agetype_kpi_5_51"
    kpi_b_bnbw_kpi_5_51(begin_day, end_day)
    print "--------kpi_b_bnbw_kpi_5_51"
    kpi_total_kpi_5_52(begin_day, end_day)
    print "--------kpi_total_kpi_5_52"
    kpi_a_level_kpi_5_52(begin_day, end_day)
    print "--------kpi_a_level_kpi_5_52"
    kpi_b_level_kpi_5_52(begin_day, end_day)
    print "--------kpi_b_level_kpi_5_52"
    kpi_agetype_kpi_5_52(begin_day, end_day)
    print "--------kpi_agetype_kpi_5_52"
    kpi_bnbw_kpi_5_52(begin_day, end_day)
    print "--------kpi_bnbw_kpi_5_52"
    kpi_a_agetype_kpi_5_52(begin_day, end_day)
    print "--------kpi_a_agetype_kpi_5_52"
    kpi_a_bnbw_kpi_5_52(begin_day, end_day)
    print "--------kpi_a_bnbw_kpi_5_52"
    kpi_b_agetype_kpi_5_52(begin_day, end_day)
    print "--------kpi_b_agetype_kpi_5_52"
    kpi_b_bnbw_kpi_5_52(begin_day, end_day)
    print "--------kpi_b_bnbw_kpi_5_52"
    kpi_total_kpi_5_53(begin_day, end_day)
    print "--------kpi_total_kpi_5_53"
    kpi_a_level_kpi_5_53(begin_day, end_day)
    print "--------kpi_a_level_kpi_5_53"
    kpi_b_level_kpi_5_53(begin_day, end_day)
    print "--------kpi_b_level_kpi_5_53"
    kpi_agetype_kpi_5_53(begin_day, end_day)
    print "--------kpi_agetype_kpi_5_53"
    kpi_bnbw_kpi_5_53(begin_day, end_day)
    print "--------kpi_bnbw_kpi_5_53"
    kpi_a_agetype_kpi_5_53(begin_day, end_day)
    print "--------kpi_a_agetype_kpi_5_53"
    kpi_a_bnbw_kpi_5_53(begin_day, end_day)
    print "--------kpi_a_bnbw_kpi_5_53"
    kpi_b_agetype_kpi_5_53(begin_day, end_day)
    print "--------kpi_b_agetype_kpi_5_53"
    kpi_b_bnbw_kpi_5_53(begin_day, end_day)
    print "--------kpi_b_bnbw_kpi_5_53"
    kpi_total_kpi_5_54(begin_day, end_day)
    print "--------kpi_total_kpi_5_54"
    kpi_a_level_kpi_5_54(begin_day, end_day)
    print "--------kpi_a_level_kpi_5_54"
    kpi_b_level_kpi_5_54(begin_day, end_day)
    print "--------kpi_b_level_kpi_5_54"
    kpi_agetype_kpi_5_54(begin_day, end_day)
    print "--------kpi_agetype_kpi_5_54"
    kpi_bnbw_kpi_5_54(begin_day, end_day)
    print "--------kpi_bnbw_kpi_5_54"
    kpi_a_agetype_kpi_5_54(begin_day, end_day)
    print "--------kpi_a_agetype_kpi_5_54"
    kpi_a_bnbw_kpi_5_54(begin_day, end_day)
    print "--------kpi_a_bnbw_kpi_5_54"
    kpi_b_agetype_kpi_5_54(begin_day, end_day)
    print "--------kpi_b_agetype_kpi_5_54"
    kpi_b_bnbw_kpi_5_54(begin_day, end_day)
    print "--------kpi_b_bnbw_kpi_5_54"
    kpi_total_kpi_5_6(begin_day, end_day)
    print "--------kpi_total_kpi_5_6"
    kpi_a_level_kpi_5_6(begin_day, end_day)
    print "--------kpi_a_level_kpi_5_6"
    kpi_b_level_kpi_5_6(begin_day, end_day)
    print "--------kpi_b_level_kpi_5_6"
    kpi_agetype_kpi_5_6(begin_day, end_day)
    print "--------kpi_agetype_kpi_5_6"
    kpi_bnbw_kpi_5_6(begin_day, end_day)
    print "--------kpi_bnbw_kpi_5_6"
    kpi_a_agetype_kpi_5_6(begin_day, end_day)
    print "--------kpi_a_agetype_kpi_5_6"
    kpi_a_bnbw_kpi_5_6(begin_day, end_day)
    print "--------kpi_a_bnbw_kpi_5_6"
    kpi_b_agetype_kpi_5_6(begin_day, end_day)
    print "--------kpi_b_agetype_kpi_5_6"
    kpi_b_bnbw_kpi_5_6(begin_day, end_day)
    print "--------kpi_b_bnbw_kpi_5_6"
    kpi_total_kpi_5_61(begin_day, end_day)
    print "--------kpi_total_kpi_5_61"
    kpi_a_level_kpi_5_61(begin_day, end_day)
    print "--------kpi_a_level_kpi_5_61"
    kpi_b_level_kpi_5_61(begin_day, end_day)
    print "--------kpi_b_level_kpi_5_61"
    kpi_agetype_kpi_5_61(begin_day, end_day)
    print "--------kpi_agetype_kpi_5_61"
    kpi_bnbw_kpi_5_61(begin_day, end_day)
    print "--------kpi_bnbw_kpi_5_61"
    kpi_a_agetype_kpi_5_61(begin_day, end_day)
    print "--------kpi_a_agetype_kpi_5_61"
    kpi_a_bnbw_kpi_5_61(begin_day, end_day)
    print "--------kpi_a_bnbw_kpi_5_61"
    kpi_b_agetype_kpi_5_61(begin_day, end_day)
    print "--------kpi_b_agetype_kpi_5_61"
    kpi_b_bnbw_kpi_5_61(begin_day, end_day)
    print "--------kpi_b_bnbw_kpi_5_61"
    kpi_total_kpi_5_62(begin_day, end_day)
    print "--------kpi_total_kpi_5_62"
    kpi_a_level_kpi_5_62(begin_day, end_day)
    print "--------kpi_a_level_kpi_5_62"
    kpi_b_level_kpi_5_62(begin_day, end_day)
    print "--------kpi_b_level_kpi_5_62"
    kpi_agetype_kpi_5_62(begin_day, end_day)
    print "--------kpi_agetype_kpi_5_62"
    kpi_bnbw_kpi_5_62(begin_day, end_day)
    print "--------kpi_bnbw_kpi_5_62"
    kpi_a_agetype_kpi_5_62(begin_day, end_day)
    print "--------kpi_a_agetype_kpi_5_62"
    kpi_a_bnbw_kpi_5_62(begin_day, end_day)
    print "--------kpi_a_bnbw_kpi_5_62"
    kpi_b_agetype_kpi_5_62(begin_day, end_day)
    print "--------kpi_b_agetype_kpi_5_62"
    kpi_b_bnbw_kpi_5_62(begin_day, end_day)
    print "--------kpi_b_bnbw_kpi_5_62"
    kpi_total_kpi_5_63(begin_day, end_day)
    print "--------kpi_total_kpi_5_63"
    kpi_a_level_kpi_5_63(begin_day, end_day)
    print "--------kpi_a_level_kpi_5_63"
    kpi_b_level_kpi_5_63(begin_day, end_day)
    print "--------kpi_b_level_kpi_5_63"
    kpi_agetype_kpi_5_63(begin_day, end_day)
    print "--------kpi_agetype_kpi_5_63"
    kpi_bnbw_kpi_5_63(begin_day, end_day)
    print "--------kpi_bnbw_kpi_5_63"
    kpi_a_agetype_kpi_5_63(begin_day, end_day)
    print "--------kpi_a_agetype_kpi_5_63"
    kpi_a_bnbw_kpi_5_63(begin_day, end_day)
    print "--------kpi_a_bnbw_kpi_5_63"
    kpi_b_agetype_kpi_5_63(begin_day, end_day)
    print "--------kpi_b_agetype_kpi_5_63"
    kpi_b_bnbw_kpi_5_63(begin_day, end_day)
    print "--------kpi_b_bnbw_kpi_5_63"
    kpi_total_kpi_5_64(begin_day, end_day)
    print "--------kpi_total_kpi_5_64"
    kpi_a_level_kpi_5_64(begin_day, end_day)
    print "--------kpi_a_level_kpi_5_64"
    kpi_b_level_kpi_5_64(begin_day, end_day)
    print "--------kpi_b_level_kpi_5_64"
    kpi_agetype_kpi_5_64(begin_day, end_day)
    print "--------kpi_agetype_kpi_5_64"
    kpi_bnbw_kpi_5_64(begin_day, end_day)
    print "--------kpi_bnbw_kpi_5_64"
    kpi_a_agetype_kpi_5_64(begin_day, end_day)
    print "--------kpi_a_agetype_kpi_5_64"
    kpi_a_bnbw_kpi_5_64(begin_day, end_day)
    print "--------kpi_a_bnbw_kpi_5_64"
    kpi_b_agetype_kpi_5_64(begin_day, end_day)
    print "--------kpi_b_agetype_kpi_5_64"
    kpi_b_bnbw_kpi_5_64(begin_day, end_day)
    print "--------kpi_b_bnbw_kpi_5_64"
    kpi_total_kpi_5_7(begin_day, end_day)
    print "--------kpi_total_kpi_5_7"
    kpi_a_level_kpi_5_7(begin_day, end_day)
    print "--------kpi_a_level_kpi_5_7"
    kpi_b_level_kpi_5_7(begin_day, end_day)
    print "--------kpi_b_level_kpi_5_7"
    kpi_agetype_kpi_5_7(begin_day, end_day)
    print "--------kpi_agetype_kpi_5_7"
    kpi_bnbw_kpi_5_7(begin_day, end_day)
    print "--------kpi_bnbw_kpi_5_7"
    kpi_a_agetype_kpi_5_7(begin_day, end_day)
    print "--------kpi_a_agetype_kpi_5_7"
    kpi_a_bnbw_kpi_5_7(begin_day, end_day)
    print "--------kpi_a_bnbw_kpi_5_7"
    kpi_b_agetype_kpi_5_7(begin_day, end_day)
    print "--------kpi_b_agetype_kpi_5_7"
    kpi_b_bnbw_kpi_5_7(begin_day, end_day)
    print "--------kpi_b_bnbw_kpi_5_7"(begin_day, end_day)
