# coding=utf-8
import sys
import traceback
import datetime

reload(sys)
sys.setdefaultencoding('utf8')
from pyspark import SparkConf, SparkContext
from pyspark.sql import *

warehouseLocationHDFS = "hdfs://nameservice1/user/hive/warehouse_ext/DL/ipsos/raw5/tmp"  # need to change it to a hdfs dir
dstPath = "hdfs://nameservice1/user/hive/warehouse_ext/DL/ipsos/raw5/doss_table"  # the dir that you want to save your result
doss_tablename = 'doss_check'
start_date = '2000-01-01'
end_date = '2018-01-01'

dh_dm2h = 'dh_dm2h.'
date='201004_201707'

sconf = SparkConf().setAppName("doss")
sc = SparkContext(conf=sconf)
sc.setLogLevel("WARN")
spark = HiveContext(sc)
'''
spark = SparkSession \
    .builder \
    .appName("part") \
    .config("spark.sql.warehouse.dir", warehouseLocationHDFS) \
    .getOrCreate()
'''

spark.sql('use dl_ipsos')  # need to change the name of database
spark.sql('set spark.sql.crossJoin.enabled=true')

# chev_be_validated1
chev_be_validated1 = 'select t1.* from ' + dh_dm2h + 'ti_cem_be_validated t1 ' \
                                                     'join dealer_info t2 ' \
                                                     'on t1.dealer_code=t2.ch_code ' \
                                                     'where t1.chk_type=\'车主资料核实\''
chev_be_validated1_sql = spark.sql(chev_be_validated1)
chev_be_validated1_sql.registerTempTable('chev_be_validated1')

# chev_be_validated2 & chev_be_validated3
chev_be_validated3 = 'select NAME_BAK as OWNER_NAME,MARKET_TYPE_CD as CUSTOMER_SALES_TYPE,sex as SEXUAL,birth_dt as BIRTHDATE,' \
                     'state as PROVINCE,city as CITY,addr as ADD,serial_num as VIN,pkg as CONF1,color_out as COLOR,' \
                     'pricing_dt as INVOICE_DATE,upload_dt as REPORT_DATE,dealer_code as DEALER_NUMBER,accnt_type_cd as CUSTOMER_TYPE ' \
                     'from ' \
                     '(select t1.*,Row_Number() OVER (partition by serial_num ORDER BY last_upd desc) as rank ' \
                     'from chev_be_validated1 t1)t1 ' \
                     'where rank=1 and substr(pricing_dt,1,10) between \'' + start_date + '\' and \'' + end_date + '\''
chev_be_validated3_sql = spark.sql(chev_be_validated3)
chev_be_validated3_sql.registerTempTable('chev_be_validated3')

# chev_cem_vehicle
chev_cem_vehicle = 'select t1.* from ' + dh_dm2h + 'ti_cem_vehicle_info t1 join dealer_info t2 on t1.dealer_code=t2.ch_code'
chev_cem_vehicle_sql = spark.sql(chev_cem_vehicle)
chev_cem_vehicle_sql.registerTempTable('chev_cem_vehicle')

# chev_be_validated4
chev_be_validated4 = 'select t1.*,t2.series as MAKES,t2.model as SERIES ' \
                     'from chev_be_validated3 t1 ' \
                     'left join chev_cem_vehicle t2 on t1.vin=t2.vin'
chev_be_validated4_sql = spark.sql(chev_be_validated4)
chev_be_validated4_sql.registerTempTable('chev_be_validated4')

# doss
doss = 'select t1.*,t2.asc as DEALER ' \
       'from chev_be_validated4 t1 ' \
       'join dealer_info t2 on t1.DEALER_NUMBER=t2.ch_code'
doss_sql = spark.sql(doss)
doss_sql.registerTempTable('dossaa')

# insert into a table
spark.sql('insert into table ' + doss_tablename + ' partition(mon='+date+') select * from dossaa')

# save a file
# doss_sql.write.save(dstPath)
