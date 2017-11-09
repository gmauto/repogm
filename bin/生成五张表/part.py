# coding=utf-8
import sys
import traceback
import datetime

reload(sys)
sys.setdefaultencoding('utf8')
from pyspark import SparkConf, SparkContext
from pyspark.sql import *

warehouseLocationHDFS = "hdfs://nameservice1/user/hive/warehouse_ext/DL/ipsos/raw5/tmp"  # need to change it to a hdfs dir
dstPath = "hdfs://nameservice1/user/hive/warehouse_ext/DL/ipsos/raw5/part_table"  # the dir that you want to save your result
part_tablename = 'part'
order_tablename = 'order_check'
start_date = '2000-01-01'
end_date = '2018-08-01'
date='201004_201707'
edw = 'edw.'
sconf = SparkConf().setAppName("part")

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

# chev_repair_part_1
chev_repair_part_1 = 'select regexp_replace(t1.vin,\' \',\'\') as vin,t1.repair_part_id,t1.asc_code,t1.storage_position,t1.part_no,' \
                     't1.part_name,t1.part_cost_price,t1.part_cost_amount,t1.part_sale_price,t1.part_sale_amount,t1.sender,' \
                     't1.receiver,t1.charge_mode,t1.ro_no,t1.balance_no,t1.last_balance_no,t1.sgm_vin_tag,t2.ch_code,t2.asc ' \
                     'from ' + edw + 'tt_asc_bo_repair_part t1 join dealer_info t2 ' \
                                     'on t1.asc_code=t2.asc_code and length(t1.balance_no)=11 ' \
                                     'where length(t1.balance_no)=11  and t1.SGM_VIN_TAG=\'Y\''
chev_repair_part_1_sql = spark.sql(chev_repair_part_1)
chev_repair_part_1_sql.registerTempTable('chev_repair_part_1')

# chev_dms_repair_part_result
chev_dms_repair_part_result = 'select t4.asc_code,t4.storage_position,t4.part_no,t4.part_name,t4.part_cost_price,' \
                              't4.part_cost_amount,t4.part_sale_price,t4.sender,t4.receiver,t4.charge_mode,' \
                              't4.repair_part_id,t4.last_balance_no,t5.order_bill_date,t5.order_balance_date,' \
                              't5.order_clear_date,t4.ro_no,t4.balance_no,t5.receptionist,t5.order_desc,t5.maint_type,' \
                              't5.maint_desc,t5.lnumber,t4.vin,t5.makes,t5.series,t5.model,t4.part_sale_amount ' \
                              'from chev_repair_part_1 t4 join ' + order_tablename + ' t5 ' \
                                                                                     'on t4.asc_code=t5.asc_code and t4.ro_no=t5.ORDER_NUMBER and t4.balance_no=t5.BALANCE_NO ' \
                                                                                     'where LENGTH(t4.vin)>5 and t4.vin not in(\'00000000\',\'11111111\',\'22222222\')'
chev_dms_repair_part_result_sql = spark.sql(chev_dms_repair_part_result).cache()
chev_dms_repair_part_result_sql.registerTempTable('chev_dms_repair_part_result')

# part_tmp1
part_tmp1 = 'select asc_code,vin,balance_no,part_no,sum(part_sale_amount) as SALES_AMOUNT ' \
            'from chev_dms_repair_part_result ' \
            'group by asc_code,vin,balance_no,part_no'
part_tmp1_sql = spark.sql(part_tmp1)
part_tmp1_sql.registerTempTable('part_tmp1')

# part_tmp2
part_tmp2 = 'select asc_code,storage_position,part_no,part_name,part_cost_price,part_cost_amount,part_sale_price,sender,' \
            'receiver,charge_mode,repair_part_id,last_balance_no,order_bill_date,order_balance_date,order_clear_date,' \
            'ro_no,balance_no,receptionist,order_desc,maint_type,maint_desc,lnumber,vin,makes,series,model,part_sale_amount ' \
            'from (select *,Row_Number() over(partition by asc_code,vin,balance_no,part_no order by order_balance_date desc) as rank ' \
            'from chev_dms_repair_part_result) tt ' \
            'where tt.rank=1'
part_tmp2_sql = spark.sql(part_tmp2)
part_tmp2_sql.registerTempTable('part_tmp2')

# part
part = 'select t62.asc_code,t62.storage_position,t62.part_no,t62.part_name,t62.part_cost_price,t62.part_cost_amount,' \
       't62.part_sale_price,t62.sender,t62.receiver,t62.charge_mode,t62.repair_part_id,t62.last_balance_no,' \
       't62.order_bill_date,t62.order_balance_date,t62.order_clear_date,t62.ro_no,t62.balance_no,t62.receptionist,' \
       't62.order_desc,t62.maint_type,t62.maint_desc,t62.lnumber,t62.vin,t62.makes,t62.series,t62.model,t61.SALES_AMOUNT ' \
       'from part_tmp1 t61 join part_tmp2 t62 ' \
       'on t61.asc_code=t62.asc_code and t61.vin=t62.vin and t61.balance_no=t62.balance_no and t61.part_no=t62.part_no'
part_sql = spark.sql(part)
part_sql.registerTempTable('partaa')

# insert into a table
spark.sql("""insert into table """ + part_tablename + """ partition(mon='+date+') select * from partaa""")

# save a file
# part_sql.write.save(dstPath)
