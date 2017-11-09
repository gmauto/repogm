# coding=utf-8
import sys
import traceback
import datetime

reload(sys)
sys.setdefaultencoding('utf8')
from pyspark import SparkConf, SparkContext
from pyspark.sql import *

warehouseLocationHDFS = "hdfs://nameservice1/user/hive/warehouse_ext/DL/ipsos/raw5/tmp"  # need to change it to a hdfs dir
dstPath_order = "hdfs://nameservice1/user/hive/warehouse_ext/DL/ipsos/raw5/order_table"  # the dir that you want to save your result
dstPath_cust = "hdfs://nameservice1/user/hive/warehouse_ext/DL/ipsos/raw5/cust_table"  # the dir that you want to save your result
order_tablename = 'order_check'
cust_tablename = 'customer_check'
start_date = '2010-04-01'
end_date = '2018-01-01'

dh_dm2h = 'dh_dm2h.'
siebel = 'siebel.'
edw = 'edw.'
date='201004_201707'

sconf = SparkConf().setAppName("cust-order")
sc = SparkContext(conf=sconf)
sc.setLogLevel("WARN")
spark = HiveContext(sc)
'''
spark = SparkSession \
    .builder \
    .appName("cust_order") \
    .config("spark.sql.warehouse.dir", warehouseLocationHDFS) \
    .getOrCreate()
'''
spark.sql('use dl_ipsos')  # need to change the name of database
spark.sql('set spark.sql.crossJoin.enabled=true')

'''
for cust table
'''
# chev_vehicle_info
chev_vehicle_info = 'select t1.vin as VIN,t1.license as LNUMBER,t1.brand as MAKES,' \
                    't1.series as SERIES,t1.model as MODEL,t1.package as CCONF,t1.color as COLOR,' \
                    't1.purchasing_date as PURCHASE_DATE,t1.mileage as MILEAGE,t1.customer_id,' \
                    't1.contact_id,t1.customer_type,t2.ch_code,t2.asc,t2.asc_code ' \
                    'from ' + dh_dm2h + 'ti_cem_vehicle_info t1 ' \
                                        'join dealer_info t2 ' \
                                        'on t1.dealer_code=t2.ch_code ' \
                                        'and substr(purchasing_date,1,10)>=\'' + start_date + '\' ' \
                                                                                              'and substr(purchasing_date,1,10)<=\'' + end_date + '\''
chev_vehicle_info_sql = spark.sql(chev_vehicle_info)
# chev_vehicle_info_sql.registerTempTable('chev_vehicle_info')
print("-----chev_vehicle_info_sql")
# chev_vehicle_info_sql.limit(100).show()
chev_vehicle_info_sql.registerTempTable('chev_vehicle_info')

# chev_dms_individual_info
chev_dms_individual_info = 'select t1.*,t2.customer_name as OWNER_NAME,' \
                           't2.id_number as OWNER_ID,t2.sex as SEXUAL,t2.industry as INDUSTRY,' \
                           't2.province as PROVINCE,t2.city as CITY,t2.address as ADD,t2.postal_code as ZIP,' \
                           't2.home_phone as PHONENUMBER,t2.cell_phone as MOBILE,t2.birth_date as BIRTHDATE,' \
                           't2.email as E_MAIL,t2.marital_status as MARRIAGE,t2.education as EDUCATION,' \
                           'null as ENTERPRISE_CODE,null as ENTERPRISE_TYPE ' \
                           'from chev_vehicle_info t1 ' \
                           'join ' + dh_dm2h + 'ti_cem_customer_individual t2 ' \
                                               'on t1.customer_id=t2.customer_id ' \
                                               'and t1.customer_type=\'个人客户\''
chev_dms_individual_info_sql = spark.sql(chev_dms_individual_info)
# chev_dms_individual_info_sql.registerTempTable('chev_dms_individual_info')
chev_dms_individual_info_sql.registerTempTable('chev_dms_individual_info')

# chev_dms_company_info
chev_dms_company_info = 'select t1.*,t2.customer_name as OWNER_NAME,' \
                        'null as OWNER_ID,null as SEXUAL,null as INDUSTRY,' \
                        't2.province as PROVINCE,t2.city as CITY,t2.address as ADD,t2.postal_code as ZIP,' \
                        't2.main_phone_number as PHONENUMBER,null as MOBILE,null as BIRTHDATE,' \
                        'null as E_MAIL,null as MARRIAGE,null as EDUCATION,' \
                        'null as ENTERPRISE_CODE,null as ENTERPRISE_TYPE ' \
                        'from chev_vehicle_info t1 ' \
                        'join ' + dh_dm2h + 'ti_cem_customer_company t2 ' \
                                            'on t1.customer_id=t2.customer_id ' \
                                            'and t1.customer_type=\'机构客户\''
chev_dms_company_info_sql = spark.sql(chev_dms_company_info)
# chev_dms_company_info_sql.registerTempTable('chev_dms_company_info')
chev_dms_company_info_sql.registerTempTable('chev_dms_company_info')

# chev_dms_customer_info
chev_dms_customer_info = 'select * from chev_dms_individual_info ' \
                         'union all ' \
                         'select * from chev_dms_company_info'
chev_dms_customer_info_sql = spark.sql(chev_dms_customer_info)
chev_dms_customer_info_sql.registerTempTable('chev_dms_customer_info')

# chev_dms_contact_info
chev_dms_contact_info = 'select t1.*,t2.sex as CONTACTOR_SEXUAL,t2.name as CONTACTOR_NAME,' \
                        't2.home_phone_number as CONTACTOR_PHONE,t2.cell_phone_number as CONTACTOR_MOBILE,' \
                        't2.province as CONTACTOR_PROVINCE,t2.city as CONTACTOR_CITY,t2.address as CONTACTOR_ADD,' \
                        't2.postal_code as CONTACTOR_ZIP ' \
                        'from chev_dms_customer_info t1 ' \
                        'left join ' + dh_dm2h + 'ti_cem_contact t2 ' \
                                                 'on t1.contact_id=t2.contact_id'
chev_dms_contact_info_sql = spark.sql(chev_dms_contact_info)
chev_dms_contact_info_sql.registerTempTable('chev_dms_contact_info')

# chev_cx_be_validated
chev_cx_be_validated = 'select t1.last_name as CONSULTANT,t1.insure_comp_code as INSURANCE_CODE,' \
                       't1.insure_comp as INSURANCE_NAME,t1.insure_dt as INSURANCE_BEGIN_DATE,t1.created,' \
                       't1.delivery_dt,t1.serial_num,t2.ch_code,t2.asc,t2.asc_code ' \
                       'from ' + siebel + 'cx_be_validated t1  ' \
                                          'join dealer_info t2 ' \
                                          'on t1.dealer_code=t2.ch_code ' \
                                          'and t1.chk_type=\'车主资料核实\''
chev_cx_be_validated_sql = spark.sql(chev_cx_be_validated)
chev_cx_be_validated_sql.registerTempTable('chev_cx_be_validated')

# chev_cx_be_validated_1
chev_cx_be_validated_1 = """select CONSULTANT,INSURANCE_CODE,INSURANCE_NAME,INSURANCE_BEGIN_DATE,created,
delivery_dt,serial_num,ch_code,asc,asc_code
from
(select t1.*, Row_Number() OVER (partition by serial_num ORDER BY delivery_dt,created desc) as rank
from chev_cx_be_validated t1) tt
where tt.rank=1"""
chev_cx_be_validated_1_sql = spark.sql(chev_cx_be_validated_1)
chev_cx_be_validated_1_sql.registerTempTable('chev_cx_be_validated_1')

# chev_dms_cx_info
chev_dms_cx_info = 'select t1.*,t2.CONSULTANT,t2.INSURANCE_CODE,t2.INSURANCE_NAME,t2.INSURANCE_BEGIN_DATE ' \
                   'from chev_dms_contact_info t1 ' \
                   'left join chev_cx_be_validated_1 t2 ' \
                   'on t1.vin=t2.serial_num and t1.asc_code=t2.asc_code'
chev_dms_cx_info_sql = spark.sql(chev_dms_cx_info)
chev_dms_cx_info_sql.registerTempTable('chev_dms_cx_info')

'''
for order table
'''
# chev_repair_order  pass
chev_repair_order = """select t1.asc_code,t1.balance_no,t1.balance_time,t1.brand,t1.claim_no,t1.complete_time,t1.deliverer,
t1.deliverer_ddd_code,t1.deliverer_gender,t1.deliverer_mobile,t1.deliverer_phone,
substr(t1.delivery_date,1,10) as end_dt,t1.end_time_supposed,t1.engine_no,
t1.finish_user,t1.is_pre_sale,t1.is_red,t1.last_balance_no,t1.license,t1.model,
t1.out_mileage,t1.owner_name,t1.owner_no,t1.owner_property,t1.receive_amount,
t1.repair_amount,t1.repair_type,t1.ro_id,t1.ro_no,t1.ro_type,
t1.series,t1.service_advisor,t1.sgm_vin_tag,
substr(t1.start_time,1,10) as start_dt,t1.test_driver,t1.total_amount,
t1.vin,t2.ch_code,t2.asc
from """ + edw + """tt_asc_repair_order t1
join dealer_info t2
on t1.asc_code=t2.asc_code and length(t1.balance_no)=11
and substr(t1.start_time,1,10)>='""" + start_date + """' and substr(t1.start_time,1,10)<='""" + end_date + """"'"""
chev_repair_order_sql = spark.sql(chev_repair_order)
chev_repair_order_sql.registerTempTable('chev_repair_order')

# repair_order_info
repair_order_info = 'select asc_code,balance_no,balance_time,brand,claim_no,complete_time,' \
                    'deliverer,deliverer_ddd_code,deliverer_gender,deliverer_mobile,deliverer_phone,' \
                    'case when end_dt is null then start_dt ELSE end_dt end as end_dt,' \
                    'end_time_supposed,engine_no,finish_user,is_pre_sale,is_red,last_balance_no,' \
                    'license,model,out_mileage,owner_name,owner_no,owner_property,receive_amount,' \
                    'repair_amount,repair_type,ro_id,ro_no,ro_type,' \
                    'series,service_advisor,sgm_vin_tag,' \
                    'case when start_dt is null then end_dt else start_dt end as start_dt,' \
                    'test_driver,total_amount,vin,ch_code,asc ' \
                    'from (select * from chev_repair_order ' \
                    'where (start_dt is not null or end_dt is not null) and (length(balance_no)=11 and is_pre_sale <>1) ) t31'
repair_order_info_sql = spark.sql(repair_order_info)
repair_order_info_sql.registerTempTable('repair_order_info')

# repair_order_info2
repair_order_info2 = 'select asc_code,balance_no,balance_time,brand,claim_no,complete_time,deliverer,' \
                     'deliverer_ddd_code,deliverer_gender,deliverer_mobile,deliverer_phone,end_dt,' \
                     'end_time_supposed,engine_no,finish_user,is_pre_sale,is_red,last_balance_no,' \
                     'license,model,out_mileage,owner_name,owner_no,owner_property,receive_amount,' \
                     'repair_amount,repair_type,ro_id,ro_no,ro_type,' \
                     'series,service_advisor,sgm_vin_tag,start_dt,test_driver,total_amount,' \
                     'vin,ch_code,asc ' \
                     'from ' \
                     '(select *,Row_Number() over(partition by asc_code,balance_no order by deliverer_gender desc) as rank ' \
                     'from repair_order_info) t51 ' \
                     'where t51.rank=1'
repair_order_info2_sql = spark.sql(repair_order_info2)
repair_order_info2_sql.registerTempTable('repair_order_info2')

# chev_balance_order
chev_balance_order = 'select t1.ro_no,t1.vin,t1.asc_code,t1.balance_no,t1.balance_time,t1.square_date,t1.total_amount,t1.receive_amount,t1.repair_type_desc,' \
                     't1.ro_type_desc,t1.ro_type,t1.repair_type,t1.SGM_VIN_TAG,t1.is_red,t1.last_balance_no ' \
                     'from ' + edw + 'tt_asc_balance_order t1 ' \
                                     'join dealer_info t2 ' \
                                     'on t1.asc_code=t2.asc_code  ' \
                                     'and substr(t1.balance_time,1,10)>=\'' + start_date + '\' ' \
                                                                                           'and substr(t1.balance_time,1,10)<=\'' + end_date + '\' ' \
                                                                                                                                               'and length(t1.balance_no)=11'
chev_balance_order_sql = spark.sql(chev_balance_order)
chev_balance_order_sql.registerTempTable('chev_balance_order')

# repair_balance_info
repair_balance_info = 'select t5.ro_id,t5.start_dt,t5.ro_no,t5.balance_no,t5.service_advisor,' \
                      't5.license,t5.brand,t5.series,t5.model,t5.repair_amount,t5.finish_user,' \
                      't5.out_mileage,t5.owner_name,t5.deliverer,t5.deliverer_gender,t5.deliverer_ddd_code,' \
                      't5.deliverer_phone,t5.deliverer_mobile,t5.end_time_supposed,t5.complete_time,' \
                      't5.end_dt,t5.owner_no,t5.owner_property,t5.claim_no,t5.test_driver,t5.vin,t5.asc_code,' \
                      't3.balance_time,t3.square_date,t3.total_amount,t3.receive_amount,t3.repair_type_desc,' \
                      't3.ro_type_desc,t3.ro_type,t3.repair_type,t3.SGM_VIN_TAG,t3.is_red,t3.last_balance_no ' \
                      'from repair_order_info2 t5 ' \
                      'join chev_balance_order t3 ' \
                      'on t5.asc_code=t3.asc_code ' \
                      'and t5.balance_no=t3.balance_no ' \
                      'and t5.vin=t3.vin ' \
                      'and t5.ro_no=t3.ro_no ' \
                      'and t5.start_dt<=substr(t3.balance_time,1,10) '

repair_balance_info_sql = spark.sql(repair_balance_info)
repair_balance_info_sql.registerTempTable('repair_balance_info')

# chev_repair_balance
chev_repair_balance = """select *,
case when is_red=1 or (case when last_balance_no='null' then false else regexp_replace(last_balance_no,' ' ,'')<>'' end) then 0 else 1 end as if_acc
from repair_balance_info
WHERE  length(balance_no)=11  and SGM_VIN_TAG='Y'"""
chev_repair_balance_sql = spark.sql(chev_repair_balance)
chev_repair_balance_sql.registerTempTable('chev_repair_balance')

# chev_ti_bo_balsucc
chev_ti_bo_balsucc = """select ro_id,'',start_dt,ro_no,balance_no,service_advisor,license,brand,series,
model,repair_amount,finish_user,out_mileage,owner_name,deliverer,deliverer_gender,
deliverer_ddd_code,deliverer_phone,deliverer_mobile,end_time_supposed,complete_time,
end_dt,owner_no,owner_property,claim_no,test_driver,vin,asc_code,balance_time,square_date,
total_amount,receive_amount,repair_type_desc,ro_type_desc,ro_type,repair_type,balance_no
from (select *,Row_Number() over(partition by asc_code,vin,ro_no order by balance_time desc) as rank
from chev_repair_balance
where if_acc=1) tt
where tt.rank=1"""
chev_ti_bo_balsucc_sql = spark.sql(chev_ti_bo_balsucc)
chev_ti_bo_balsucc_sql.registerTempTable('chev_ti_bo_balsucc')

# insert into a table
spark.sql('insert into table ' + order_tablename + ' partition(mon='+date+') select * from chev_ti_bo_balsucc')

# save a file
# chev_ti_bo_balsucc_sql.write.save(dstPath_order)

'''
get the cust table result
'''
# chev_dms_customer_result
# need repair_order_info2 of order table
chev_dms_customer_result = """select t1.*,t2.balance_time as INSURANCE_ORDER_DATE,t2.deliverer as RETURNER_NAME,
t2.deliverer_gender as RETURNER_SEXUAL,t2.deliverer_ddd_code as RETURNER_PHONE_AREACODE,
t2.deliverer_phone as RETURNER_PHONE,t2.deliverer_mobile as RETURNER_MOBILE,
t2.owner_no as OWNER_CODE,t2.owner_property as OWNER_TYPE
from chev_dms_cx_info t1 left join repair_order_info2 t2
on t1.vin=t2.vin"""
chev_dms_customer_result_sql = spark.sql(chev_dms_customer_result)
chev_dms_customer_result_sql.registerTempTable('chev_dms_customer_result')

# customer
customer = 'select VIN,LNUMBER,MAKES,SERIES,MODEL,CCONF,COLOR,PURCHASE_DATE,MILEAGE,CUSTOMER_TYPE,' \
           'CH_CODE,ASC,OWNER_NAME,OWNER_ID,SEXUAL,INDUSTRY,PROVINCE,CITY,ADD,ZIP,PHONENUMBER,' \
           'MOBILE,BIRTHDATE,E_MAIL,MARRIAGE,EDUCATION,ENTERPRISE_CODE,ENTERPRISE_TYPE,CONTACTOR_SEXUAL,' \
           'CONTACTOR_NAME,CONTACTOR_PHONE,CONTACTOR_MOBILE,CONTACTOR_PROVINCE,CONTACTOR_CITY,' \
           'CONTACTOR_ADD,CONTACTOR_ZIP,CONSULTANT,INSURANCE_CODE,INSURANCE_NAME,INSURANCE_BEGIN_DATE,' \
           'INSURANCE_ORDER_DATE,RETURNER_NAME,RETURNER_SEXUAL,RETURNER_PHONE_AREACODE,RETURNER_PHONE,' \
           'RETURNER_MOBILE,OWNER_CODE,OWNER_TYPE ' \
           'from ' \
           '(select t1.*,Row_Number() OVER (partition by VIN ORDER BY INSURANCE_ORDER_DATE desc) as rank ' \
           'from chev_dms_customer_result t1)t1 ' \
           'where rank=1 ' \
           'and (OWNER_NAME="缺失" or CONTACTOR_NAME="缺失" or RETURNER_NAME is null)'
customer_sql = spark.sql(customer)
customer_sql.registerTempTable('customeraa')

# insert into a table
spark.sql('insert into table ' + cust_tablename + ' partition(mon='+date+')  select * from customeraa')

# save a file
# customer_sql.write.save(dstPath_cust)
