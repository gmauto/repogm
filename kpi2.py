# coding=utf-8
import sys
import traceback
import datetime

reload(sys)
sys.setdefaultencoding('utf8')
from pyspark import SparkConf, SparkContext
from pyspark.sql import *

sconf = SparkConf().setAppName("ipsos-kpi")
sc = SparkContext(conf=sconf)
sc.setLogLevel("WARN")
spark = HiveContext(sc)
basepath = '/home/ipsos_test1/general'
spark.sql("add jar " + basepath + "/bin/transform-date.jar")
spark.sql("CREATE TEMPORARY FUNCTION mon_diff AS 'com.gm.transformDate.MonDiff'")
spark.sql("CREATE TEMPORARY FUNCTION tran_date AS 'com.gm.transformDate.TransformDate'")
spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
spark.sql("use ipsos_test1")


# kpi_1(占比类_total)
def kpi_1(占比类_total)(ver, pub_db)

:
line = """insert overwrite table  kpi_total_8areas partition(region,mon_p,kpi)
  select mon,sum(num)*1.0/sum(den), am.group_name,'${region}',mon_p,kpi
  from kpi_total_tmp_8areas s join ${pub_db}.asc_mapping am
  on s.asc_code=am.asc_code
  where am.version='${region}'
  group by kpi,mon,am.group_name,mon_p,'${region}'"""
line = line.replace('${ver}', ver)
line = line.replace('${pub_db}', 'pub_db')
try:
    spark.sql(line)
except:
    traceback.print_exc()  # kpi_1(占比类_一级)
def kpi_1(占比类_一级)(ver, pub_db)

:
line = """insert overwrite table  kpi_a_level_8areas partition(region,mon_p,kpi)
  select primary_classification,mon,sum(num)*1.0/sum(den), am.group_name ,'${region}',mon_p,kpi
  from kpi_a_level_tmp_8areas s join ${pub_db}.asc_mapping am
  on s.asc_code=am.asc_code
  where am.version='${region}'
  group by kpi,primary_classification,mon,am.group_name,mon_p,'${region}'"""
line = line.replace('${ver}', ver)
line = line.replace('${pub_db}', 'pub_db')
try:
    spark.sql(line)
except:
    traceback.print_exc()  # kpi_1(占比类_二级)
def kpi_1(占比类_二级)(ver, pub_db)

:
line = """insert overwrite table  kpi_b_level_8areas partition(region,mon_p,kpi)
  select primary_classification,second_level_classification,mon,sum(num)*1.0/sum(den), am.group_name ,'${region}',mon_p,kpi
  from kpi_b_level_tmp_8areas s join ${pub_db}.asc_mapping am
  on s.asc_code=am.asc_code
  where am.version='${region}'
  group by kpi,primary_classification,second_level_classification,mon,am.group_name,mon_p,'${region}'"""
line = line.replace('${ver}', ver)
line = line.replace('${pub_db}', 'pub_db')
try:
    spark.sql(line)
except:
    traceback.print_exc()  # kpi_1(占比类_车龄)
def kpi_1(占比类_车龄)(ver, pub_db)

:
line = """insert overwrite table  kpi_agetype_8areas partition(region,mon_p,kpi)
  select age_type,mon,sum(num)*1.0/sum(den), am.group_name,'${region}',mon_p,kpi
  from kpi_agetype_tmp_8areas s join ${pub_db}.asc_mapping am
  on s.asc_code=am.asc_code
  where am.version='${region}'
  group by kpi,age_type,mon,am.group_name,mon_p,'${region}'"""
line = line.replace('${ver}', ver)
line = line.replace('${pub_db}', 'pub_db')
try:
    spark.sql(line)
except:
    traceback.print_exc()  # kpi_1(占比类_保内外)
def kpi_1(占比类_保内外)(ver, pub_db)

:
line = """insert overwrite table  kpi_bnbw_8areas partition(region,mon_p,kpi)
  select bn,mon,sum(num)*1.0/sum(den), am.group_name,'${region}',mon_p,kpi
  from kpi_bnbw_tmp_8areas s join ${pub_db}.asc_mapping am
  on s.asc_code=am.asc_code
  where am.version='${region}'
  group by kpi,bn,mon,am.group_name,mon_p,'${region}'"""
line = line.replace('${ver}', ver)
line = line.replace('${pub_db}', 'pub_db')
try:
    spark.sql(line)
except:
    traceback.print_exc()  # kpi_1(占比类_一级车龄)
def kpi_1(占比类_一级车龄)(ver, pub_db)

:
line = """insert overwrite table  kpi_a_agetype_8areas partition(region,mon_p,kpi)
  select primary_classification,age_type,mon,sum(num)*1.0/sum(den), am.group_name,'${region}',mon_p,kpi
  from kpi_a_agetype_tmp_8areas s join ${pub_db}.asc_mapping am
  on s.asc_code=am.asc_code
  where am.version='${region}'
  group by kpi,primary_classification,age_type,mon,am.group_name,mon_p,'${region}'"""
line = line.replace('${ver}', ver)
line = line.replace('${pub_db}', 'pub_db')
try:
    spark.sql(line)
except:
    traceback.print_exc()  # kpi_1(占比类_一级保内外)
def kpi_1(占比类_一级保内外)(ver, pub_db)

:
line = """insert overwrite table  kpi_a_bnbw_8areas partition(region,mon_p,kpi)
  select primary_classification,bn,mon,sum(num)*1.0/sum(den), am.group_name,'${region}',mon_p,kpi
  from kpi_a_bnbw_tmp_8areas s join ${pub_db}.asc_mapping am
  on s.asc_code=am.asc_code
  where am.version='${region}'
  group by kpi,primary_classification,bn,mon,am.group_name,mon_p,'${region}'"""
line = line.replace('${ver}', ver)
line = line.replace('${pub_db}', 'pub_db')
try:
    spark.sql(line)
except:
    traceback.print_exc()  # kpi_1(占比类_二级车龄)
def kpi_1(占比类_二级车龄)(ver, pub_db)

:
line = """insert overwrite table  kpi_b_agetype_8areas partition(region,mon_p,kpi)
  select primary_classification,second_level_classification,age_type,mon,sum(num)*1.0/sum(den), am.group_name ,'${region}',mon_p,kpi
  from kpi_b_agetype_tmp_8areas s join ${pub_db}.asc_mapping am
  on s.asc_code=am.asc_code
  where am.version='${region}'
  group by kpi,primary_classification,second_level_classification,age_type,mon,am.group_name,mon_p,'${region}'"""
line = line.replace('${ver}', ver)
line = line.replace('${pub_db}', 'pub_db')
try:
    spark.sql(line)
except:
    traceback.print_exc()  # kpi_1(占比类_二级保内外)
def kpi_1(占比类_二级保内外)(ver, pub_db)

:
line = """insert overwrite table  kpi_b_bnbw_8areas partition(region,mon_p,kpi)
  select primary_classification,second_level_classification,bn,mon,sum(num)*1.0/sum(den), am.group_name ,'${region}',mon_p,kpi
  from kpi_b_bnbw_tmp_8areas s join ${pub_db}.asc_mapping am
  on s.asc_code=am.asc_code
  where am.version='${region}'
  group by kpi,primary_classification,second_level_classification,bn,mon,am.group_name,mon_p,'${region}'"""
line = line.replace('${ver}', ver)
line = line.replace('${pub_db}', 'pub_db')
try:
    spark.sql(line)
except:
    traceback.print_exc()  # kpi_2(直接相加类_total)
def kpi_2(直接相加类_total)(ver, pub_db)

:
line = """insert overwrite table  kpi_total_8areas partition(region,mon_p,kpi)
  select mon,sum(num)*1.0/avg(den), am.group_name,'${region}',mon_p,kpi
  from kpi_total_tmp_8areas_k1 s join ${pub_db}.asc_mapping am
  on s.asc_code=am.asc_code
  where am.version='${region}'
  group by kpi,mon,am.group_name,mon_p,'${region}'"""
line = line.replace('${ver}', ver)
line = line.replace('${pub_db}', 'pub_db')
try:
    spark.sql(line)
except:
    traceback.print_exc()  # kpi_2(直接相加类_一级)
def kpi_2(直接相加类_一级)(ver, pub_db)

:
line = """insert overwrite table  kpi_a_level_8areas partition(region,mon_p,kpi)
  select primary_classification,mon,sum(num)*1.0/avg(den), am.group_name ,'${region}',mon_p,kpi
  from kpi_a_level_tmp_8areas_k1 s join ${pub_db}.asc_mapping am
  on s.asc_code=am.asc_code
  where am.version='${region}'
  group by kpi,primary_classification,mon,am.group_name,mon_p,'${region}'"""
line = line.replace('${ver}', ver)
line = line.replace('${pub_db}', 'pub_db')
try:
    spark.sql(line)
except:
    traceback.print_exc()  # kpi_2(直接相加类_二级)
def kpi_2(直接相加类_二级)(ver, pub_db)

:
line = """insert overwrite table  kpi_b_level_8areas partition(region,mon_p,kpi)
  select primary_classification,second_level_classification,mon,sum(num)*1.0/avg(den), am.group_name ,'${region}',mon_p,kpi
  from kpi_b_level_tmp_8areas_k1 s join ${pub_db}.asc_mapping am
  on s.asc_code=am.asc_code
  where am.version='${region}'
  group by kpi,primary_classification,second_level_classification,mon,am.group_name,mon_p,'${region}'"""
line = line.replace('${ver}', ver)
line = line.replace('${pub_db}', 'pub_db')
try:
    spark.sql(line)
except:
    traceback.print_exc()  # kpi_2(直接相加类_车龄)
def kpi_2(直接相加类_车龄)(ver, pub_db)

:
line = """insert overwrite table  kpi_agetype_8areas partition(region,mon_p,kpi)
  select age_type,mon,sum(num)*1.0/avg(den), am.group_name,'${region}',mon_p,kpi
  from kpi_agetype_tmp_8areas_k1 s join ${pub_db}.asc_mapping am
  on s.asc_code=am.asc_code
  where am.version='${region}'
  group by kpi,age_type,mon,am.group_name,mon_p,'${region}'"""
line = line.replace('${ver}', ver)
line = line.replace('${pub_db}', 'pub_db')
try:
    spark.sql(line)
except:
    traceback.print_exc()  # kpi_2(直接相加类_保内外)
def kpi_2(直接相加类_保内外)(ver, pub_db)

:
line = """insert overwrite table  kpi_bnbw_8areas partition(region,mon_p,kpi)
  select bn,mon,sum(num)*1.0/avg(den), am.group_name,'${region}',mon_p,kpi
  from kpi_bnbw_tmp_8areas_k1 s join ${pub_db}.asc_mapping am
  on s.asc_code=am.asc_code
  where am.version='${region}'
  group by kpi,bn,mon,am.group_name,mon_p,'${region}'"""
line = line.replace('${ver}', ver)
line = line.replace('${pub_db}', 'pub_db')
try:
    spark.sql(line)
except:
    traceback.print_exc()  # kpi_2(直接相加类_以及车龄)
def kpi_2(直接相加类_以及车龄)(ver, pub_db)

:
line = """insert overwrite table  kpi_a_agetype_8areas partition(region,mon_p,kpi)
  select primary_classification,age_type,mon,sum(num)*1.0/avg(den), am.group_name,'${region}',mon_p,kpi
  from kpi_a_agetype_tmp_8areas_k1 s join ${pub_db}.asc_mapping am
  on s.asc_code=am.asc_code
  where am.version='${region}'
  group by kpi,primary_classification,age_type,mon,am.group_name,mon_p,'${region}'"""
line = line.replace('${ver}', ver)
line = line.replace('${pub_db}', 'pub_db')
try:
    spark.sql(line)
except:
    traceback.print_exc()  # kpi_2(直接相加类_一级保内外)
def kpi_2(直接相加类_一级保内外)(ver, pub_db)

:
line = """insert overwrite table  kpi_a_bnbw_8areas partition(region,mon_p,kpi)
  select primary_classification,bn,mon,sum(num)*1.0/avg(den), am.group_name,'${region}',mon_p,kpi
  from kpi_a_bnbw_tmp_8areas_k1 s join ${pub_db}.asc_mapping am
  on s.asc_code=am.asc_code
  where am.version='${region}'
  group by kpi,primary_classification,bn,mon,am.group_name,mon_p,'${region}'"""
line = line.replace('${ver}', ver)
line = line.replace('${pub_db}', 'pub_db')
try:
    spark.sql(line)
except:
    traceback.print_exc()  # kpi_2(直接相加类_二级车龄)
def kpi_2(直接相加类_二级车龄)(ver, pub_db)

:
line = """insert overwrite table  kpi_b_agetype_8areas partition(region,mon_p,kpi)
  select primary_classification,second_level_classification,age_type,mon,sum(num)*1.0/avg(den), am.group_name ,'${region}',mon_p,kpi
  from kpi_b_agetype_tmp_8areas_k1 s join ${pub_db}.asc_mapping am
  on s.asc_code=am.asc_code
  where am.version='${region}'
  group by kpi,primary_classification,second_level_classification,age_type,mon,am.group_name,mon_p,'${region}'"""
line = line.replace('${ver}', ver)
line = line.replace('${pub_db}', 'pub_db')
try:
    spark.sql(line)
except:
    traceback.print_exc()  # kpi_2(直接相加类_二级保内外)
def kpi_2(直接相加类_二级保内外)(ver, pub_db)

:
line = """insert overwrite table  kpi_b_bnbw_8areas partition(region,mon_p,kpi)
  select primary_classification,second_level_classification,bn,mon,sum(num)*1.0/avg(den), am.group_name ,'${region}',mon_p,kpi
  from kpi_b_bnbw_tmp_8areas_k1 s join ${pub_db}.asc_mapping am
  on s.asc_code=am.asc_code
  where am.version='${region}'
  group by kpi,primary_classification,second_level_classification,bn,mon,am.group_name,mon_p,'${region}'"""
line = line.replace('${ver}', ver)
line = line.replace('${pub_db}', 'pub_db')
try:
    spark.sql(line)
except:
    traceback.print_exc()

if __name__ == "__main__":
    if sys.argv.__len__() != 3:
        print("需要两个参数 <开始时间> <结束时间>")
        sys.exit(-1)
    ver = sys.argv[1]
    pub_db = sys.argv[2]
    time_record = open(basepath + "/log/time_record", "w")
    startime = datetime.datetime.now()
    kpi_1(占比类_total)(ver, pub_db)
    endtime = datetime.datetime.now()
    time_record.write("kpi_1(占比类_total)\t" + startime.strftime("%Y-%m-%d %H:%M:%S") + "\t" + endtime.strftime(
        "%Y-%m-%d %H:%M:%S") + "\t" + str((endtime - startime).seconds) + "s\n")
    startime = datetime.datetime.now()
    kpi_1(占比类_一级)(ver, pub_db)
    endtime = datetime.datetime.now()
    time_record.write("kpi_1(占比类_一级)\t" + startime.strftime("%Y-%m-%d %H:%M:%S") + "\t" + endtime.strftime(
        "%Y-%m-%d %H:%M:%S") + "\t" + str((endtime - startime).seconds) + "s\n")
    startime = datetime.datetime.now()
    kpi_1(占比类_二级)(ver, pub_db)
    endtime = datetime.datetime.now()
    time_record.write("kpi_1(占比类_二级)\t" + startime.strftime("%Y-%m-%d %H:%M:%S") + "\t" + endtime.strftime(
        "%Y-%m-%d %H:%M:%S") + "\t" + str((endtime - startime).seconds) + "s\n")
    startime = datetime.datetime.now()
    kpi_1(占比类_车龄)(ver, pub_db)
    endtime = datetime.datetime.now()
    time_record.write("kpi_1(占比类_车龄)\t" + startime.strftime("%Y-%m-%d %H:%M:%S") + "\t" + endtime.strftime(
        "%Y-%m-%d %H:%M:%S") + "\t" + str((endtime - startime).seconds) + "s\n")
    startime = datetime.datetime.now()
    kpi_1(占比类_保内外)(ver, pub_db)
    endtime = datetime.datetime.now()
    time_record.write("kpi_1(占比类_保内外)\t" + startime.strftime("%Y-%m-%d %H:%M:%S") + "\t" + endtime.strftime(
        "%Y-%m-%d %H:%M:%S") + "\t" + str((endtime - startime).seconds) + "s\n")
    startime = datetime.datetime.now()
    kpi_1(占比类_一级车龄)(ver, pub_db)
    endtime = datetime.datetime.now()
    time_record.write("kpi_1(占比类_一级车龄)\t" + startime.strftime("%Y-%m-%d %H:%M:%S") + "\t" + endtime.strftime(
        "%Y-%m-%d %H:%M:%S") + "\t" + str((endtime - startime).seconds) + "s\n")
    startime = datetime.datetime.now()
    kpi_1(占比类_一级保内外)(ver, pub_db)
    endtime = datetime.datetime.now()
    time_record.write("kpi_1(占比类_一级保内外)\t" + startime.strftime("%Y-%m-%d %H:%M:%S") + "\t" + endtime.strftime(
        "%Y-%m-%d %H:%M:%S") + "\t" + str((endtime - startime).seconds) + "s\n")
    startime = datetime.datetime.now()
    kpi_1(占比类_二级车龄)(ver, pub_db)
    endtime = datetime.datetime.now()
    time_record.write("kpi_1(占比类_二级车龄)\t" + startime.strftime("%Y-%m-%d %H:%M:%S") + "\t" + endtime.strftime(
        "%Y-%m-%d %H:%M:%S") + "\t" + str((endtime - startime).seconds) + "s\n")
    startime = datetime.datetime.now()
    kpi_1(占比类_二级保内外)(ver, pub_db)
    endtime = datetime.datetime.now()
    time_record.write("kpi_1(占比类_二级保内外)\t" + startime.strftime("%Y-%m-%d %H:%M:%S") + "\t" + endtime.strftime(
        "%Y-%m-%d %H:%M:%S") + "\t" + str((endtime - startime).seconds) + "s\n")
    startime = datetime.datetime.now()
    kpi_2(直接相加类_total)(ver, pub_db)
    endtime = datetime.datetime.now()
    time_record.write("kpi_2(直接相加类_total)\t" + startime.strftime("%Y-%m-%d %H:%M:%S") + "\t" + endtime.strftime(
        "%Y-%m-%d %H:%M:%S") + "\t" + str((endtime - startime).seconds) + "s\n")
    startime = datetime.datetime.now()
    kpi_2(直接相加类_一级)(ver, pub_db)
    endtime = datetime.datetime.now()
    time_record.write("kpi_2(直接相加类_一级)\t" + startime.strftime("%Y-%m-%d %H:%M:%S") + "\t" + endtime.strftime(
        "%Y-%m-%d %H:%M:%S") + "\t" + str((endtime - startime).seconds) + "s\n")
    startime = datetime.datetime.now()
    kpi_2(直接相加类_二级)(ver, pub_db)
    endtime = datetime.datetime.now()
    time_record.write("kpi_2(直接相加类_二级)\t" + startime.strftime("%Y-%m-%d %H:%M:%S") + "\t" + endtime.strftime(
        "%Y-%m-%d %H:%M:%S") + "\t" + str((endtime - startime).seconds) + "s\n")
    startime = datetime.datetime.now()
    kpi_2(直接相加类_车龄)(ver, pub_db)
    endtime = datetime.datetime.now()
    time_record.write("kpi_2(直接相加类_车龄)\t" + startime.strftime("%Y-%m-%d %H:%M:%S") + "\t" + endtime.strftime(
        "%Y-%m-%d %H:%M:%S") + "\t" + str((endtime - startime).seconds) + "s\n")
    startime = datetime.datetime.now()
    kpi_2(直接相加类_保内外)(ver, pub_db)
    endtime = datetime.datetime.now()
    time_record.write("kpi_2(直接相加类_保内外)\t" + startime.strftime("%Y-%m-%d %H:%M:%S") + "\t" + endtime.strftime(
        "%Y-%m-%d %H:%M:%S") + "\t" + str((endtime - startime).seconds) + "s\n")
    startime = datetime.datetime.now()
    kpi_2(直接相加类_以及车龄)(ver, pub_db)
    endtime = datetime.datetime.now()
    time_record.write("kpi_2(直接相加类_以及车龄)\t" + startime.strftime("%Y-%m-%d %H:%M:%S") + "\t" + endtime.strftime(
        "%Y-%m-%d %H:%M:%S") + "\t" + str((endtime - startime).seconds) + "s\n")
    startime = datetime.datetime.now()
    kpi_2(直接相加类_一级保内外)(ver, pub_db)
    endtime = datetime.datetime.now()
    time_record.write("kpi_2(直接相加类_一级保内外)\t" + startime.strftime("%Y-%m-%d %H:%M:%S") + "\t" + endtime.strftime(
        "%Y-%m-%d %H:%M:%S") + "\t" + str((endtime - startime).seconds) + "s\n")
    startime = datetime.datetime.now()
    kpi_2(直接相加类_二级车龄)(ver, pub_db)
    endtime = datetime.datetime.now()
    time_record.write("kpi_2(直接相加类_二级车龄)\t" + startime.strftime("%Y-%m-%d %H:%M:%S") + "\t" + endtime.strftime(
        "%Y-%m-%d %H:%M:%S") + "\t" + str((endtime - startime).seconds) + "s\n")
    startime = datetime.datetime.now()
    kpi_2(直接相加类_二级保内外)(ver, pub_db)
    endtime = datetime.datetime.now()
    time_record.write("kpi_2(直接相加类_二级保内外)\t" + startime.strftime("%Y-%m-%d %H:%M:%S") + "\t" + endtime.strftime(
        "%Y-%m-%d %H:%M:%S") + "\t" + str((endtime - startime).seconds) + "s\n")
    time_record.close()
