#!/bin/bash

date=201704_201707
#主要用来给kpi建表
database=ipsos_test4
path=hdfs://ns1/user/${database}
#public目录下要求目录深度为1
secondpath=fx
#存放源数据更换分隔符之后的路径
TB_FACT=${path}/${secondpath}
PATH_CLAIM=${TB_FACT}/auto/raw/claim
PATH_CUSTOMER=${TB_FACT}/auto/raw/customer
PATH_DOSS=${TB_FACT}/auto/raw/doss
PATH_ORDER=${TB_FACT}/auto/raw/order
PATH_PART=${TB_FACT_PART}
#存放事实表的路径
#tb_fact=${path}/public/auto/tb4kpi/fact
tb_fact=${TB_FACT}
tb_fact_tmp=${TB_FACT}/fact_tmp
#存放维度表的路径
#tb_dim=hdfs://ns1/user/ori/public/auto/tb4kpi/dim
tb_dim=${TB_FACT}
#存放kpi计算结果的路径 计划传参进来 比如 kpi  badaqukpi
tb_kpi=${TB_FACT}/auto/tbkpkires
#存放flow相关的表的路径
tb_flow_dim=${path}/fx
#存放flow表所需要的文件
#mkdir /home/${database}/general/data/flow
local_dim_files=/home/${database}/general/data/dim
#flow结果表存放目录
#hdfs dfs -ls mkdir -R ${path}/fx/fact_tmp
tb_flow=${TB_FACT}/auto/flow
#fact 表

function mkdifile(){
hdfs dfs -mkdir ${path}/fx/fact_tmp
hdfs dfs -mkdir -R  ${path}/fx/auto/tbkpkires
hdfs dfs -mkdir -R ${path}/fx/fact_tmp
mkdir /home/${database}/general/data/flow
}


function cre_tb_fact_tmp(){
hive -e "
use ${database};

CREATE EXTERNAL TABLE order_tmp(
  rank_number string,
  order_status string,
  order_bill_date string,
  order_number string,
  balance_no string,
  receptionist string,
  lnumber string,
  makes string,
  series string,
  model string,
  maint_amount string,
  acceptance string,
  mileage string,
  owner_name string,
  returner_name string,
  returner_sexual string,
  returner_phone_areacode string,
  returner_phone string,
  returner_mobile string,
  predicted_delivery_date string,
  complete_date string,
  delivery_date string,
  owner_code string,
  owner_type string,
  claim_order_number string,
  trialer string,
  vin string,
  asc_code string,
  order_balance_date string,
  order_clear_date string,
  order_balance_amount string,
  paid_amount string,
  maint_desc string,
  order_desc string,
  order_type string,
  maint_type string,
  balance_no1 string)
PARTITIONED BY (
  mon string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\u0001'
  LINES TERMINATED BY '\n'

LOCATION
  '${tb_fact_tmp}/order';

CREATE EXTERNAL TABLE part_tmp(
  asc_code string,
  stock string,
  part_number string,
  part_desc string,
  cost_price string,
  cost_amount string,
  sales_price string,
  part_dis string,
  part_rec string,
  value_type string,
  rank_number string,
  last_balance_no string,
  order_bill_date string,
  order_balance_date string,
  order_clear_date string,
  order_number string,
  order_balance_number string,
  receptionist string,
  order_desc string,
  maint_type string,
  maint_desc string,
  lnumber string,
  vin string,
  makes string,
  series string,
  model string,
  sales_amount string)
PARTITIONED BY (
  mon string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\u0001'
  LINES TERMINATED BY '\n'

LOCATION
  '${tb_fact_tmp}/part';

CREATE EXTERNAL TABLE claim_uniq_tmp(
  additional_lable string,
  deal_date string,
  claim_order_number string,
  rank_number string,
  claim_result string,
  claim_type string,
  deduction string,
  claim_accepted_amount string,
  gwm_claim_number string,
  gwm_version string,
  hourly_amount string,
  operation_code string,
  rownumber string,
  other_charge string,
  part_amount string,
  claim_amount string,
  claim_accepted_amount_tax string,
  ch_code string,
  asc string,
  asc_code string)
PARTITIONED BY (
  mon string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\u0001'
  LINES TERMINATED BY '\n'

LOCATION
  '${tb_fact_tmp}/claim_uniq';

CREATE EXTERNAL TABLE customer_uniq_tmp(
  vin string,
  lnumber string,
  makes string,
  series string,
  model string,
  conf1 string,
  color string,
  purchase_date string,
  mileage string,
  customer_type string,
  ch_code string,
  asc string,
  owner_name string,
  owner_id string,
  sexual string,
  industry string,
  province string,
  city string,
  add string,
  zip string,
  phonenumber string,
  mobile string,
  birthdate string,
  email string,
  marriage string,
  education string,
  enterprise_code string,
  enterprise_type string,
  contactor_sexual string,
  contactor_name string,
  contactor_phone string,
  contactor_mobile string,
  contactor_province string,
  contactor_city string,
  contactor_add string,
  contactor_zip string,
  consultant string,
  insurance_code string,
  insurance_name string,
  insurance_begin_date string,
  insurance_order_date string,
  returner_name string,
  returner_sexual string,
  returner_phone_areacode string,
  returner_phone string,
  returner_mobile string,
  owner_code string,
  owner_type string)
PARTITIONED BY (
  mon string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\u0001'
  LINES TERMINATED BY '\n'

LOCATION
  '${tb_fact_tmp}/customer_uniq';


  CREATE EXTERNAL TABLE doss_tmp(
  owner_name string,
  customer_sales_type string,
  sexual string,
  birthdate string,
  province string,
  city string,
  add string,
  vin string,
  conf1 string,
  color string,
  invoice_date string,
  report_date string,
  dealer_number string,
  customer_type string,
  makes string,
  series string,
  dealer string)
PARTITIONED BY (
  mon string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\u0001'
  LINES TERMINATED BY '\n'

LOCATION
  '${tb_fact_tmp}/doss';


CREATE EXTERNAL TABLE label_doss_tmp(
  owner_name string,
  customer_sales_type string,
  sexual string,
  birthdate string,
  province string,
  city string,
  add string,
  vin string,
  conf1 string,
  color string,
  invoice_date string,
  report_date string,
  dealer_number string,
  customer_type string,
  makes string,
  series string,
  dealer string,
  asc_code string,
  primary_classification string,
  second_level_classification string)
PARTITIONED BY (
  mon string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\u0001'
  LINES TERMINATED BY '\n'

LOCATION
  '${tb_fact_tmp}/label_doss';

CREATE EXTERNAL TABLE label_order_tmp(
  rank_number string,
  order_status string,
  order_bill_date string,
  order_number string,
  balance_no string,
  receptionist string,
  lnumber string,
  makes string,
  series string,
  model string,
  maint_amount string,
  acceptance string,
  mileage string,
  owner_name string,
  returner_name string,
  returner_sexual string,
  returner_phone_areacode string,
  returner_phone string,
  returner_mobile string,
  predicted_delivery_date string,
  complete_date string,
  delivery_date string,
  owner_code string,
  owner_type string,
  claim_order_number string,
  trialer string,
  vin string,
  asc_code string,
  order_balance_date string,
  order_clear_date string,
  order_balance_amount string,
  paid_amount string,
  maint_desc string,
  order_desc string,
  order_type string,
  maint_type string,
  balance_no_1 string,
  maint_type1 string,
  first_maintnance string,
  claim string,
  outdate string,
  number_of_month string,
  age_type string,
  age_type2 string,
  bn string,
  primary_classification string,
  second_level_classification string)
PARTITIONED BY (
  mon string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\u0001'
  LINES TERMINATED BY '\n'

LOCATION
  '${tb_fact_tmp}/label_order';

"
}

function cre_tb_fact(){
hive -e "
use ${database};

CREATE EXTERNAL TABLE order(
  rank_number string,
  order_status string,
  order_bill_date string,
  order_number string,
  balance_no string,
  receptionist string,
  lnumber string,
  makes string,
  series string,
  model string,
  maint_amount string,
  acceptance string,
  mileage string,
  owner_name string,
  returner_name string,
  returner_sexual string,
  returner_phone_areacode string,
  returner_phone string,
  returner_mobile string,
  predicted_delivery_date string,
  complete_date string,
  delivery_date string,
  owner_code string,
  owner_type string,
  claim_order_number string,
  trialer string,
  vin string,
  asc_code string,
  order_balance_date string,
  order_clear_date string,
  order_balance_amount string,
  paid_amount string,
  maint_desc string,
  order_desc string,
  order_type string,
  maint_type string,
  balance_no1 string)
PARTITIONED BY (
  mon string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\u0001'
  LINES TERMINATED BY '\n'
LOCATION
  '${tb_fact}/order';

CREATE EXTERNAL TABLE part(
  asc_code string,
  stock string,
  part_number string,
  part_desc string,
  cost_price string,
  cost_amount string,
  sales_price string,
  part_dis string,
  part_rec string,
  value_type string,
  rank_number string,
  last_balance_no string,
  order_bill_date string,
  order_balance_date string,
  order_clear_date string,
  order_number string,
  order_balance_number string,
  receptionist string,
  order_desc string,
  maint_type string,
  maint_desc string,
  lnumber string,
  vin string,
  makes string,
  series string,
  model string,
  sales_amount string)
PARTITIONED BY (
  mon string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\u0001'
  LINES TERMINATED BY '\n'
LOCATION
  '${tb_fact}/part';

CREATE EXTERNAL TABLE claim_uniq(
  additional_lable string,
  deal_date string,
  claim_order_number string,
  rank_number string,
  claim_result string,
  claim_type string,
  deduction string,
  claim_accepted_amount string,
  gwm_claim_number string,
  gwm_version string,
  hourly_amount string,
  operation_code string,
  rownumber string,
  other_charge string,
  part_amount string,
  claim_amount string,
  claim_accepted_amount_tax string,
  ch_code string,
  asc string,
  asc_code string)
PARTITIONED BY (
  mon string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\u0001'
  LINES TERMINATED BY '\n'
LOCATION
  '${tb_fact}/claim_uniq';

CREATE EXTERNAL TABLE customer_uniq(
  vin string,
  lnumber string,
  makes string,
  series string,
  model string,
  conf1 string,
  color string,
  purchase_date string,
  mileage string,
  customer_type string,
  ch_code string,
  asc string,
  owner_name string,
  owner_id string,
  sexual string,
  industry string,
  province string,
  city string,
  add string,
  zip string,
  phonenumber string,
  mobile string,
  birthdate string,
  email string,
  marriage string,
  education string,
  enterprise_code string,
  enterprise_type string,
  contactor_sexual string,
  contactor_name string,
  contactor_phone string,
  contactor_mobile string,
  contactor_province string,
  contactor_city string,
  contactor_add string,
  contactor_zip string,
  consultant string,
  insurance_code string,
  insurance_name string,
  insurance_begin_date string,
  insurance_order_date string,
  returner_name string,
  returner_sexual string,
  returner_phone_areacode string,
  returner_phone string,
  returner_mobile string,
  owner_code string,
  owner_type string)
PARTITIONED BY (
  mon string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\u0001'
  LINES TERMINATED BY '\n'
LOCATION
  '${tb_fact}/customer_uniq';

  
  CREATE EXTERNAL TABLE doss(
  owner_name string,
  customer_sales_type string,
  sexual string,
  birthdate string,
  province string,
  city string,
  add string,
  vin string,
  conf1 string,
  color string,
  invoice_date string,
  report_date string,
  dealer_number string,
  customer_type string,
  makes string,
  series string,
  dealer string)
PARTITIONED BY (
  mon string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\u0001'
  LINES TERMINATED BY '\n'
LOCATION
  '${tb_fact}/doss';

  
CREATE EXTERNAL TABLE label_doss(
  owner_name string,
  customer_sales_type string,
  sexual string,
  birthdate string,
  province string,
  city string,
  add string,
  vin string,
  conf1 string,
  color string,
  invoice_date string,
  report_date string,
  dealer_number string,
  customer_type string,
  makes string,
  series string,
  dealer string,
  asc_code string,
  primary_classification string,
  second_level_classification string)
PARTITIONED BY (
  mon string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\u0001'
  LINES TERMINATED BY '\n'
LOCATION
  '${tb_fact}/label_doss';

CREATE EXTERNAL TABLE label_order(
  rank_number string,
  order_status string,
  order_bill_date string,
  order_number string,
  balance_no string,
  receptionist string,
  lnumber string,
  makes string,
  series string,
  model string,
  maint_amount string,
  acceptance string,
  mileage string,
  owner_name string,
  returner_name string,
  returner_sexual string,
  returner_phone_areacode string,
  returner_phone string,
  returner_mobile string,
  predicted_delivery_date string,
  complete_date string,
  delivery_date string,
  owner_code string,
  owner_type string,
  claim_order_number string,
  trialer string,
  vin string,
  asc_code string,
  order_balance_date string,
  order_clear_date string,
  order_balance_amount string,
  paid_amount string,
  maint_desc string,
  order_desc string,
  order_type string,
  maint_type string,
  balance_no_1 string,
  maint_type1 string,
  first_maintnance string,
  claim string,
  outdate string,
  number_of_month string,
  age_type string,
  age_type2 string,
  bn string,
  primary_classification string,
  second_level_classification string)
PARTITIONED BY (
  mon string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\u0001'
  LINES TERMINATED BY '\n'
LOCATION
  '${tb_fact}/label_order';

"
}

#dim表

function cre_tb_dim(){
hive -e "
use ${database};
CREATE EXTERNAL TABLE date_label(
  mon_label string,
  quarter_label string,
  year_label string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t'
  LINES TERMINATED BY '\n'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  '${tb_dim}/date_label';

CREATE EXTERNAL TABLE distributor(
   asccode string,
  chcode string,
  asc string)
PARTITIONED BY (
  version string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t'
  LINES TERMINATED BY '\n'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  '${tb_dim}/distributor';

  
CREATE EXTERNAL TABLE doss_asc(
  dealer_number string,
  asc_code string)
PARTITIONED BY (
  version string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t'
  LINES TERMINATED BY '\n'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  '${tb_dim}/doss_asc';

CREATE EXTERNAL TABLE enclosure(
  part_num string,
  info string,
  classify string,
  name_chinese string,
  brand string,
  type string)
PARTITIONED BY (
  version string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t'
  LINES TERMINATED BY '\n'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  '${tb_dim}/enclosure';

CREATE EXTERNAL TABLE engine_oil(
  part_num string,
  part_name string,
  brand string,
  type string)
PARTITIONED BY (
  version string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t'
  LINES TERMINATED BY '\n'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  '${tb_dim}/engine_oil';

CREATE EXTERNAL TABLE filter(
  part_num string,
  name_en string,
  name_chinese string)
PARTITIONED BY (
  version string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t'
  LINES TERMINATED BY '\n'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  '${tb_dim}/filter';

CREATE EXTERNAL TABLE high_flow_parts(
  part_num string,
  type string,
  type1 string)
PARTITIONED BY (
  version string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t'
  LINES TERMINATED BY '\n'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  '${tb_dim}/high_flow_parts';


  
CREATE EXTERNAL TABLE maintnance(
  part_num string,
  part_name string,
  type string)
PARTITIONED BY (
  version string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t'
  LINES TERMINATED BY '\n'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  '${tb_dim}/maintnance';

CREATE EXTERNAL TABLE mapping(
  fact_table string,
  dim_table string,
  month string,
  version string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t'
  LINES TERMINATED BY '\n'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  '${tb_dim}/mapping';

  
  CREATE EXTERNAL TABLE asc_mapping(
  asc_code string,
  group_name string,
  version string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t'
  LINES TERMINATED BY '\n'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  '${tb_dim}/asc_mapping';

   CREATE EXTERNAL TABLE mon_mapping(
  mon string,
  time string,
  version string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t'
  LINES TERMINATED BY '\n'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  '${tb_dim}/mon_mapping';
  
  
CREATE EXTERNAL TABLE dealer_info(
  asc_code string, 
 ch_code string, 
  asc string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '\t' 
  LINES TERMINATED BY '\n' 
LOCATION
  '${tb_dim}/dealer_info';
  
"
}

function cre_check_table(){
hive -e "
use ${database};

CREATE EXTERNAL TABLE order_check(
  rank_number string,
  order_status string,
  order_bill_date string,
  order_number string,
  balance_no string,
  receptionist string,
  lnumber string,
  makes string,
  series string,
  model string,
  maint_amount string,
  acceptance string,
  mileage string,
  owner_name string,
  returner_name string,
  returner_sexual string,
  returner_phone_areacode string,
  returner_phone string,
  returner_mobile string,
  predicted_delivery_date string,
  complete_date string,
  delivery_date string,
  owner_code string,
  owner_type string,
  claim_order_number string,
  trialer string,
  vin string,
  asc_code string,
  order_balance_date string,
  order_clear_date string,
  order_balance_amount string,
  paid_amount string,
  maint_desc string,
  order_desc string,
  order_type string,
  maint_type string,
  balance_no1 string)
PARTITIONED BY (
  mon string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\u0001'
  LINES TERMINATED BY '\n'
LOCATION
  '${PATH_ORDER}';

CREATE EXTERNAL TABLE claim_check(
  additional_lable string,
  deal_date string,
  claim_order_number string,
  rank_number string,
  claim_result string,
  claim_type string,
  deduction string,
  claim_accepted_amount string,
  gwm_claim_number string,
  gwm_version string,
  hourly_amount string,
  operation_code string,
  rownumber string,
  other_charge string,
  part_amount string,
  claim_amount string,
  claim_accepted_amount_tax string,
  ch_code string,
  asc string,
  asc_code string)
PARTITIONED BY (
  mon string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\u0001'
  LINES TERMINATED BY '\n'
LOCATION
  '${PATH_CLAIM}';

CREATE EXTERNAL TABLE customer_check(
  vin string,
  lnumber string,
  makes string,
  series string,
  model string,
  conf1 string,
  color string,
  purchase_date string,
  mileage string,
  customer_type string,
  ch_code string,
  asc string,
  owner_name string,
  owner_id string,
  sexual string,
  industry string,
  province string,
  city string,
  add string,
  zip string,
  phonenumber string,
  mobile string,
  birthdate string,
  email string,
  marriage string,
  education string,
  enterprise_code string,
  enterprise_type string,
  contactor_sexual string,
  contactor_name string,
  contactor_phone string,
  contactor_mobile string,
  contactor_province string,
  contactor_city string,
  contactor_add string,
  contactor_zip string,
  consultant string,
  insurance_code string,
  insurance_name string,
  insurance_begin_date string,
  insurance_order_date string,
  returner_name string,
  returner_sexual string,
  returner_phone_areacode string,
  returner_phone string,
  returner_mobile string,
  owner_code string,
  owner_type string)
PARTITIONED BY (
  mon string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\u0001'
  LINES TERMINATED BY '\n'
LOCATION
  '${PATH_CUSTOMER}';

  
  CREATE EXTERNAL TABLE doss_check(
  owner_name string,
  customer_sales_type string,
  sexual string,
  birthdate string,
  province string,
  city string,
  add string,
  vin string,
  conf1 string,
  color string,
  invoice_date string,
  report_date string,
  dealer_number string,
  customer_type string,
  makes string,
  series string,
  dealer string)
PARTITIONED BY (
  mon string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\u0001'
  LINES TERMINATED BY '\n'
LOCATION
  '${PATH_DOSS}';
"
}


#kpi表
function cre_tb_kpi(){
if [ $# -lt 1 ]
then
 echo "需要一个参数，是kpi，八大区，全国……"
 exit -1
else
 kpi_path=${tb_kpi}/$1
fi

hive -e "
use ${database};
 CREATE EXTERNAL TABLE kpi_a_agetype(
  a_level string,
  age_type string,
  mon string,
  num string,
  asc_code string)
PARTITIONED BY (
  classify string,
  mon_p string,
  kpi string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\u0001'
  LINES TERMINATED BY '\n'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  '${kpi_path}/kpi_a_agetype';
----
CREATE EXTERNAL TABLE kpi_a_bnbw(
  a_level string,
  bnbw string,
  mon string,
  num string,
  asc_code string)
PARTITIONED BY (
classify string,
  mon_p string,
  kpi string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\u0001'
  LINES TERMINATED BY '\n'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  '${kpi_path}/kpi_a_bnbw';
----
CREATE EXTERNAL TABLE kpi_a_level(
  a_level string,
  mon string,
  num string,
  asc_code string)
PARTITIONED BY (
classify string,
  mon_p string,
  kpi string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\u0001'
  LINES TERMINATED BY '\n'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  '${kpi_path}/kpi_a_level';
----
CREATE EXTERNAL TABLE kpi_agetype(
  age_type string,
  mon string,
  num string,
  asc_code string)
PARTITIONED BY (
classify string,
  mon_p string,
  kpi string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\u0001'
  LINES TERMINATED BY '\n'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  '${kpi_path}/kpi_agetype';
----
CREATE EXTERNAL TABLE kpi_b_agetype(
  a_level string,
  b_level string,
  age_type string,
  mon string,
  num string,
  asc_code string)
PARTITIONED BY (
classify string,
  mon_p string,
  kpi string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\u0001'
  LINES TERMINATED BY '\n'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  '${kpi_path}/kpi_b_agetype';
----
CREATE EXTERNAL TABLE kpi_b_bnbw(
  a_level string,
  b_level string,
  bnbw string,
  mon string,
  num string,
  asc_code string)
PARTITIONED BY (
classify string,
  mon_p string,
  kpi string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\u0001'
  LINES TERMINATED BY '\n'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  '${kpi_path}/kpi_b_bnbw';
----
CREATE EXTERNAL TABLE kpi_b_level(
  a_level string,
  b_level string,
  mon string,
  num string,
  asc_code string)
PARTITIONED BY (
classify string,
  mon_p string,
  kpi string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\u0001'
  LINES TERMINATED BY '\n'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  '${kpi_path}/kpi_b_level';
----
CREATE EXTERNAL TABLE kpi_bnbw(
  bnbw string,
  mon string,
  num string,
  asc_code string)
PARTITIONED BY (
classify string,
  mon_p string,
  kpi string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\u0001'
  LINES TERMINATED BY '\n'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  '${kpi_path}/kpi_bnbw';
----
CREATE EXTERNAL TABLE kpi_total(
  mon string,
  num string,
  asc_code string)
PARTITIONED BY (
classify string,
  mon_p string,
  kpi string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\u0001'
  LINES TERMINATED BY '\n'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  '${kpi_path}/kpi_total';
"
}


#八大区kpi计算表的tmp表
function cre_tb_kpi_tmp(){
if [ $# -lt 1 ]
then
 echo "需要制定一个名字，比如8areas"
 exit -1
fi
name=$1
hive -e "
use ${database};
create external table kpi_total_tmp_${name}(

mon  string,
num  string,
den  string,
asc_code  string
)partitioned by(
mon_p string,
kpi string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
Location '${tb_kpi}/${name}/kpi_total_tmp_${name}';


create external table kpi_a_level_tmp_${name}(

primary_classification string,
mon  string,
num  string,
den  string,
asc_code  string
)partitioned by(
mon_p string,
kpi string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
Location '${tb_kpi}/${name}/kpi_a_level_tmp_${name}';



create external table kpi_b_level_tmp_${name}(

primary_classification string,
second_level_classification  string,
mon  string,
num  string,
den  string,
asc_code  string
)partitioned by(
mon_p string,
kpi string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
Location '${tb_kpi}/${name}/kpi_b_level_tmp_${name}';


create external table kpi_agetype_tmp_${name}(

age_type  string,
mon  string,
num  string,
den  string,
asc_code  string
)partitioned by(
mon_p string,
kpi string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
Location '${tb_kpi}/${name}/kpi_agetype_tmp_${name}';



create external table kpi_bnbw_tmp_${name}(
bn  string,
mon  string,
num  string,
den  string,
asc_code  string
)partitioned by(
mon_p string,
kpi string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
Location '${tb_kpi}/${name}/kpi_bnbw_tmp_${name}';


create external table kpi_a_agetype_tmp_${name}(
primary_classification  string,
age_type  string,
mon  string,
num  string,
den  string,
asc_code  string
)partitioned by(
mon_p string,
kpi string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
Location '${tb_kpi}/${name}/kpi_a_agetype_tmp_${name}';


create external table kpi_a_bnbw_tmp_${name}(
primary_classification  string,
bn  string,
mon  string,
num  string,
den  string,
asc_code  string
)partitioned by(
mon_p string,
kpi string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
Location '${tb_kpi}/${name}/kpi_a_bnbw_tmp_${name}';


create external table kpi_b_agetype_tmp_${name}(
primary_classification  string,
second_level_classification  string,
age_type  string,
mon  string,
num  string,
den  string,
asc_code  string
)partitioned by(
mon_p string,
kpi string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
Location '${tb_kpi}/${name}/kpi_b_agetype_tmp_${name}';


create external table kpi_b_bnbw_tmp_${name}(
primary_classification  string,
second_level_classification  string,
bn  string,
mon  string,
num  string,
den  string,
asc_code  string
)partitioned by(
mon_p string,
kpi string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
Location '${tb_kpi}/${name}/kpi_b_bnbw_tmp_${name}';
"
}

#kpi表 八大区 全国
function cre_tb_kpi_other(){
if [ $# -lt 1 ]
then
 echo "需要一个参数，是kpi，八大区，全国……"
 exit -1
else
 kpi_path=${tb_kpi}/$1
fi
name=$1

hive -e "
use ${database};
 CREATE EXTERNAL TABLE kpi_a_agetype_${name}(
  a_level string,
  age_type string,
  mon string,
  num string,
  asc_code string)
PARTITIONED BY (
region string,
  mon_p string,
  kpi string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\u0001'
  LINES TERMINATED BY '\n'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  '${kpi_path}/kpi_a_agetype_${name}';

CREATE EXTERNAL TABLE kpi_a_bnbw_${name}(
  a_level string,
  bnbw string,
  mon string,
  num string,
  asc_code string)
PARTITIONED BY (
region string,
  mon_p string,
  kpi string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\u0001'
  LINES TERMINATED BY '\n'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  '${kpi_path}/kpi_a_bnbw_${name}';

CREATE EXTERNAL TABLE kpi_a_level_${name}(
  a_level string,
  mon string,
  num string,
  asc_code string)
PARTITIONED BY (
region string,
  mon_p string,
  kpi string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\u0001'
  LINES TERMINATED BY '\n'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  '${kpi_path}/kpi_a_level_${name}';

CREATE EXTERNAL TABLE kpi_agetype_${name}(
  age_type string,
  mon string,
  num string,
  asc_code string)
PARTITIONED BY (
region string,
  mon_p string,
  kpi string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\u0001'
  LINES TERMINATED BY '\n'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  '${kpi_path}/kpi_agetype_${name}';

CREATE EXTERNAL TABLE kpi_b_agetype_${name}(
  a_level string,
  b_level string,
  age_type string,
  mon string,
  num string,
  asc_code string)
PARTITIONED BY (
region string,
  mon_p string,
  kpi string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\u0001'
  LINES TERMINATED BY '\n'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  '${kpi_path}/kpi_b_agetype_${name}';

CREATE EXTERNAL TABLE kpi_b_bnbw_${name}(
  a_level string,
  b_level string,
  bnbw string,
  mon string,
  num string,
  asc_code string)
PARTITIONED BY (
region string,
  mon_p string,
  kpi string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\u0001'
  LINES TERMINATED BY '\n'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  '${kpi_path}/kpi_b_bnbw_${name}';

CREATE EXTERNAL TABLE kpi_b_level_${name}(
  a_level string,
  b_level string,
  mon string,
  num string,
  asc_code string)
PARTITIONED BY (
region string,
  mon_p string,
  kpi string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\u0001'
  LINES TERMINATED BY '\n'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  '${kpi_path}/kpi_b_level_${name}';

CREATE EXTERNAL TABLE kpi_bnbw_${name}(
  bnbw string,
  mon string,
  num string,
  asc_code string)
PARTITIONED BY (
region string,
  mon_p string,
  kpi string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\u0001'
  LINES TERMINATED BY '\n'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  '${kpi_path}/kpi_bnbw_${name}';

CREATE EXTERNAL TABLE kpi_total_${name}(
  mon string,
  num string,
  asc_code string)
PARTITIONED BY (
  region string,
  mon_p string,
  kpi string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\u0001'
  LINES TERMINATED BY '\n'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  '${kpi_path}/kpi_total_${name}';
"
}

#创建flow表相关的表
function cre_flow_dim(){
hive -e "
use ${database};
----sexual
create external table sexual(
sexual  string,
id  string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
Location '${tb_flow_dim}/sexual';

----province
create external table province(
province  string,
id  string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
Location '${tb_flow_dim}/province';

----city
create external table city(
city  string,
id  string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
Location '${tb_flow_dim}/city';

----name
create external table name(
name  string,
id  string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
Location '${tb_flow_dim}/name';

----primary_classification
create external table primary_classification(
primary_classification  string,
id  string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
Location '${tb_flow_dim}/primary_classification';

----second_level_classification
create external table second_level_classification(
second_level_classification  string,
id  string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
Location '${tb_flow_dim}/second_level_classification';
"
}

function cre_flow(){
hive -e "
use ${database};
----flow_tmp
create external table flow_tmp(
order_number string,
vin  string,
number  string,
owner_code  string,
birthdate  string,
sexual  string,
province  string,
city  string,
asc_code  string,
asc  string,
outdate  string,
deal_year  string,
deal_date  string,
part_number  string,
name  string,
quantity  string,
sales_amount  string,
age  string,
bnjk  string,
bwjk  string,
mileage  string,
maint_type  string,
primary_classification  string,
second_level_classification  string,
type2 string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
Location '${tb_flow}/flow_tmp';

----flow_0628_pre
create external table flow_pre(
order_number string,
vin  string,
number  string,
owner_code  string,
birthdate  string,
sexual  string,
province  string,
city  string,
asc_code  string,
asc  string,
outdate  string,
deal_year  string,
deal_date  string,
part_number  string,
name  string,
quantity  string,
sales_amount  string,
age  string,
bnjk  string,
bwjk  string,
mileage  string,
primary_classification  string,
second_level_classification  string,
maint_type1 string,              	                    
type2 string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
Location '${tb_flow}/flow_pre';

----flow_0628
create external table flow(
order_number string,
vin  string,
number  string,
owner_code  string,
birthdate  string,
sexual  string,
province  string,
city  string,
asc_code  string,
asc  string,
outdate  string,
deal_year  string,
deal_date  string,
part_number  string,
name  string,
quantity  string,
sales_amount  string,
age  string,
bnjk  string,
bwjk  string,
mileage  string,
primary_classification  string,
second_level_classification  string,
maint_type1 string,              	                    
type2 string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
Location '${tb_flow}/flow';


----flow_le
create external table flow_le(
order_number string,
vin  string,
number  string,
owner_code  string,
birthdate  string,
sexual  string,
province  string,
city  string,
asc_code  string,
asc  string,
outdate  string,
deal_year  string,
deal_date  string,
part_number  string,
name  string,
quantity  string,
sales_amount  string,
age  string,
bnjk  string,
bwjk  string,
mileage  string,
primary_classification  string,
second_level_classification  string,
maint_type1 string,              	                    
type2 string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
Location '${tb_flow}/flow_le';
"
}

#加载数据到flow的
function load_data_dim(){
if [ $# -lt 1 ]
then
 echo "需要一个参数，version"
 exit -1
else
 version=$1
fi
hive -e "
use ${database};
load data local inpath '${local_dim_files}/maintnance/1/*' overwrite into table maintnance partition(version=${version});
load data local inpath '${local_dim_files}/enclosure/1/*' overwrite into table enclosure partition(version=${version});
load data local inpath '${local_dim_files}/engine_oil/1/*' overwrite into table engine_oil partition(version=${version});
load data local inpath '${local_dim_files}/distributor/1/*' overwrite into table distributor partition(version=${version});
load data local inpath '${local_dim_files}/filter/1/*' overwrite into table filter partition(version=${version});
load data local inpath '${local_dim_files}/doss_asc/1/*' overwrite into table doss_asc partition(version=${version});
load data local inpath '${local_dim_files}/sexual/*' overwrite into table sexual;
load data local inpath '${local_dim_files}/city/*' overwrite into table city;
load data local inpath '${local_dim_files}/asc_mapping/*' overwrite into table asc_mapping;
load data local inpath '${local_dim_files}/name/*' overwrite into table name;
load data local inpath '${local_dim_files}/date_label/*' overwrite into table date_label;
load data local inpath '${local_dim_files}/primary_classification/*' overwrite into table primary_classification;
load data local inpath '${local_dim_files}/second_level_classification/*' overwrite into table second_level_classification;
load data local inpath '${local_dim_files}/province/*' overwrite into table province;
load data local inpath '${local_dim_files}/mapping/*' overwrite into table mapping;
"
}




function add_par(){

if [ $# -lt 1 ]
then
 echo "需要一个参数，version"
 exit -1
else
 version=$1
fi
hive -e "
use ${database};
alter table high_flow_parts add partition(version='${version}') location '${version}';
alter table maintnance      add partition(version='${version}') location '${version}';
alter table enclosure       add partition(version='${version}') location '${version}';
alter table engine_oil      add partition(version='${version}') location '${version}';
alter table distributor     add partition(version='${version}') location '${version}';
alter table filter          add partition(version='${version}') location '${version}';
alter table doss_asc        add partition(version='${version}') location '${version}';
 "
}

function add_par_check(){

if [ $# -lt 1 ]
then
 echo "需要一个参数，version"
 exit -1
else
 version=$1
fi
hive -e "
use ${database};
alter table doss_check add partition(mon='${version}') location '${version}';
alter table claim_check      add partition(mon='${version}') location '${version}';
alter table customer_check       add partition(mon='${version}') location '${version}';
alter table order_check      add partition(mon='${version}') location '${version}';
alter table part      add partition(mon='${version}') location '${version}';
 "
}
$1 $2
