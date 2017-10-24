# coding=utf-8
import sys
import traceback
import datetime

reload(sys)
sys.setdefaultencoding('utf8')
from pyspark import SparkConf, SparkContext
from pyspark.sql import *

warehouseLocationHDFS = "hdfs://nameservice1/user/hive/warehouse_ext/DL/ipsos/raw5/tmp"  # need to change it to a hdfs dir
dstPath = "hdfs://nameservice1/user/hive/warehouse_ext/DL/ipsos/raw5/claim_table"  # the dir that you want to save your result
claim_tablename = 'claim_table'
start_date = '2001-01-01'
end_date = '2018-01-01'
edw = 'edw.'

sconf = SparkConf().setAppName("claim")
sc = SparkContext(conf=sconf)
sc.setLogLevel("WARN")
spark = HiveContext(sc)
'''
spark = SparkSession \

    .builder \
    .appName("claim") \
    .config("spark.sql.warehouse.dir", warehouseLocationHDFS) \
    .getOrCreate()
'''

spark.sql('use dl_ipsos')  # need to change the name of database
spark.sql('set spark.sql.crossJoin.enabled=true')

claim = 'select t1.additiona:q!l_credit as ADDITIONAL_LABLE,t1.apply_date as DEAL_DATE,t1.claim_no as CLAIM_ORDER_NUMBER,' \
        't1.claim_no_id as RANK_NUMBER,t1.claim_status as CLAIM_RESULT,t1.claim_type as CLAIM_TYPE,' \
        't1.debit_ind as DEDUCTION,t1.gross_credit as CLAIM_ACCEPTED_AMOUNT,t1.gwm_claim_no as GWM_CLAIM_NUMBER,' \
        't1.gwm_claim_version_no as GWM_VERSION,t1.labour_amount as HOURLY_AMOUNT,' \
        't1.labour_operation_code as OPERATION_CODE,t1.line_no as ROWNUMBER,t1.net_item_amount as OTHER_CHARGE,' \
        't1.parts_amount as PART_AMOUNT,t1.repair_total as CLAIM_AMOUNT,t1.tax_amount as CLAIM_ACCEPTED_AMOUNT_TAX,' \
        't2.ch_code,t2.asc,t2.asc_code ' \
        'from ' + edw + 'tt_asc_claim_order t1 ' \
                        'join dealer_info t2 ' \
                        'on t1.asc_code=t2.asc_code'
claim_sql = spark.sql(claim)
# claim_sql.registerTempTable('claim')
claim_sql.registerTempTable('claimaa')

# insert into a table
spark.sql('insert into table ' + claim_tablename + ' partition(mon=\'201705\') select * from claimaa')

# save a file
# claim_sql.write.save(dstPath)
