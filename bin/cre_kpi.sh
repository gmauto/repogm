#!/bin/bash

 hive -e "
  use ipsos;
  drop table if exists customer_uniq_2200621;
  create external table customer_uniq_2200621(
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
  owner_type string
  )row format delimited
  fields terminated by '\t'
  lines terminated by '\n'
  location 'hdfs://ns1/user/ipsos/private/auto/uniq/customer_uniq_2200621/';
 



 
  
  drop table if exists claim_uniq_2200621;
  create external table claim_uniq_2200621(
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
  asc_code string
  )row format delimited
  fields terminated by '\t'
  lines terminated by '\n'
  location 'hdfs://ns1/user/ipsos/private/auto/uniq/claim_uniq_2200621/';
 



 
  
  drop table if exists label_doss_2200621;
  create external table label_doss_2200621(
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
  second_level_classification string
  )row format delimited
  fields terminated by '\t'
  lines terminated by '\n'
  location 'hdfs://ns1/user/ipsos/private/auto/label/label_doss_2200621/';
 





  
  drop table if exists label_order_2200621;
  create external table label_order_2200621(
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
  second_level_classification string
  )row format delimited
  fields terminated by '\t'
  lines terminated by '\n'
  location 'hdfs://ns1/user/ipsos/private/auto/label/order/label_order_2200621/';
 




 
  
  drop table if exists kpi_total_2200621;
  create external table kpi_total_2200621(
  mon string,
  num string,
  asc_code string
  )partitioned by(
   mon_p string,
   kpi string
  )row format delimited
  fields terminated by '\t'
  lines terminated by '\n'
  location 'hdfs://ns1/user/ipsos/private/auto/kpi/kpi_total_2200621/';
 



 
  
  drop table if exists kpi_a_level_2200621;
  create external table kpi_a_level_2200621(
  a_level string,
  mon string,
  num string,
  asc_code string
  )partitioned by(
   mon_p string,
   kpi string
  )row format delimited
  fields terminated by '\t'
  lines terminated by '\n'
  location 'hdfs://ns1/user/ipsos/private/auto/kpi/kpi_a_level_2200621/';
 



 
  
  drop table if exists kpi_b_level_2200621;
  create external table kpi_b_level_2200621(
  a_level string,
  b_level string,
  mon string,
  num string,
  asc_code string
  )partitioned by(
   mon_p string,
   kpi string
  )row format delimited
  fields terminated by '\t'
  lines terminated by '\n'
  location 'hdfs://ns1/user/ipsos/private/auto/kpi/kpi_b_level_2200621/';
 



 
  
  drop table if exists kpi_agetype_2200621;
  create external table kpi_agetype_2200621(
  age_type string,
  mon string,
  num string,
  asc_code string
  )partitioned by(
   mon_p string,
   kpi string
  )row format delimited
  fields terminated by '\t'
  lines terminated by '\n'
  location 'hdfs://ns1/user/ipsos/private/auto/kpi/kpi_agetype_2200621/';
 



 
  
  drop table if exists kpi_bnbw_2200621;
  create external table kpi_bnbw_2200621(
  bnbw string,
  mon string,
  num string,
  asc_code string
  )partitioned by(
   mon_p string,
   kpi string
  )row format delimited
  fields terminated by '\t'
  lines terminated by '\n'
  location 'hdfs://ns1/user/ipsos/private/auto/kpi/kpi_bnbw_2200621/';
 



 
  
  drop table if exists kpi_a_agetype_2200621;
  create external table kpi_a_agetype_2200621(
  a_level string,
  age_type string,
  mon string,
  num string,
  asc_code string
  )partitioned by(
   mon_p string,
   kpi string
  )row format delimited
  fields terminated by '\t'
  lines terminated by '\n'
  location 'hdfs://ns1/user/ipsos/private/auto/kpi/kpi_a_agetype_2200621/';
 



 
  
  drop table if exists kpi_b_agetype_2200621;
  create external table kpi_b_agetype_2200621(
  a_level string,
  b_level string,
  age_type string,
  mon string,
  num string,
  asc_code string
  )partitioned by(
   mon_p string,
   kpi string
  )row format delimited
  fields terminated by '\t'
  lines terminated by '\n'
  location 'hdfs://ns1/user/ipsos/private/auto/kpi/kpi_b_agetype_2200621/';
 



 
  
  drop table if exists kpi_a_bnbw_2200621;
  create external table kpi_a_bnbw_2200621(
  a_level string,
  bnbw string,
  mon string,
  num string,
  asc_code string
  )partitioned by(
   mon_p string,
   kpi string
  )row format delimited
  fields terminated by '\t'
  lines terminated by '\n'
  location 'hdfs://ns1/user/ipsos/private/auto/kpi/kpi_a_bnbw_2200621/';
 



 
  
  drop table if exists kpi_b_bnbw_2200621;
  create external table kpi_b_bnbw_2200621(
  a_level string,
  b_level string,
  bnbw string,
  mon string,
  num string,
  asc_code string
  )partitioned by(
   mon_p string,
   kpi string
  )row format delimited
  fields terminated by '\t'
  lines terminated by '\n'
  location 'hdfs://ns1/user/ipsos/private/auto/kpi/kpi_b_bnbw_2200621/';
 
"
