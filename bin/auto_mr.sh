#!/bin/bash
source /etc/profile

#用户名
user_name=ori
#设置reduce的个数
reduce_num=10

#日志路径
log_path=/home/${user_name}/general/log

#替换数据中的分隔符 将 “，” 替换为 \001\
PORMAT_JAR=/home/${user_name}/general/bin/format.jar
FORMAT_MAINCLASS=com.gm.main.AutoRun

#mapping表路径
#mapping=/user/ori/public/auto/tb4kpi/dim/mapping
mapping=/user/ori/public/mapping

#ori public目录 公共访问路径 存放kpi计算需要的表
#TB_FACT=hdfs://ns1/user/ori/public/auto/tb4kpi/fact
#TB_FACT=hdfs://ns1/user/ori/public
TB_FACT=hdfs://ns1/user/ori/private/fact_tmp
TB_FACT_ORDER=${TB_FACT}/order_uniq
TB_FACT_PART=${TB_FACT}/part
TB_FACT_CLAIM_UNIQ=${TB_FACT}/claim_uniq
TB_FACT_CUSTOMER_UNIQ=${TB_FACT}/customer_uniq
TB_FACT_DOSS=${TB_FACT}/doss_uniq
TB_FACT_LABEL_DOSS=${TB_FACT}/label_doss
TB_FACT_LABEL_ORDER=${TB_FACT}/label_order

#源数据本地存放目录
LOCAL_PATH_PART=/home/${user_name}/general/data/part
LOCAL_PATH_CLAIM=/home/${user_name}/general/data/claim
LOCAL_PATH_CUSTOMER=/home/${user_name}/general/data/customer
LOCAL_PATH_DOSS=/home/${user_name}/general/data/doss
LOCAL_PATH_ORDER=/home/${user_name}/general/data/order

#源数据集群存放目录(源数据存放在私有目录下)
PATH_PART_R=private/auto/raw_csv/part
PATH_CLAIM_R=private/auto/raw_csv/claim
PATH_CUSTOMER_R=private/auto/raw_csv/customer
PATH_DOSS_R=private/auto/raw_csv/doss
PATH_ORDER_R=private/auto/raw_csv/order

#存放源数据更换分隔符之后的路径
PATH_CLAIM=private/auto/raw/claim
PATH_CUSTOMER=private/auto/raw/customer
PATH_DOSS=private/auto/raw/doss
PATH_ORDER=private/auto/raw/order
#PATH_PART=private/auto/raw/part
PATH_PART=${TB_FACT_PART}

#uniq
UNIQ_JAR=/home/${user_name}/general/bin/uniq.jar
MAIN_CLASS_UNIQ=com.gm.uniq.main.Main

#PATH_UNIQ_OUT_DOSS=private/auto/uniq/doss/
PATH_UNIQ_OUT_DOSS=${TB_FACT_DOSS}
#PATH_UNIQ_OUT_CLAIM=private/auto/uniq/claim/
PATH_UNIQ_OUT_CLAIM=${TB_FACT_CLAIM_UNIQ}
#PATH_UNIQ_OUT_CUSTOMER=private/auto/uniq/customer/
PATH_UNIQ_OUT_CUSTOMER=${TB_FACT_CUSTOMER_UNIQ}
#PATH_UNIQ_OUT_ORDER=private/auto/uniq/order/
PATH_UNIQ_OUT_ORDER=${TB_FACT_ORDER}
PATH_UNIQ_OUT_part=private/auto/uniq/order/

#打删除标签
DELETE_JAR=/home/${user_name}/general/bin/delete-useless.jar
DELETE_MAINCLASS=com.gm.delete.main.Main
PATH_LABEL_DELETE_ORDER=private/auto/label/order/delete




#label
LABEL_JAR=label.jar
#是否首保 是否索赔
FM_MAINCLASS=com.gm.label.claimAndFirstMaintenance.main.Main
#维修类别1
FIX_MAINCLASS=com.gm.label.maintType.main.Main
#出库日期	车龄月数	车龄分类	一级分类	二级分类 	保内 保外	车龄分类1(others)
OTHER_MAINCLASS=com.gm.label.others.main.Main
#给doss 打asc_code 一级分类 二级分类
DOSS_MAINCLASS=com.gm.label.doss.main.Main

PATH_LABEL_FM=private/auto/label/order/first_maintnance
PATH_LABEL_FIX=private/auto/label/order/fix
#PATH_LABEL_OTHER=private/auto/label/order/other
PATH_LABEL_OTHER=${TB_FACT_LABEL_ORDER}
#PATH_LABEL_DOSS=private/auto/label/doss
PATH_LABEL_DOSS=${TB_FACT_LABEL_DOSS}


#在集群创建存放数据的文件夹
function mkdir_local(){
 if [ $# -lt 1 ]
 then
  echo "我们需要一个日期  比如：201701"
  exit -1
 fi
 date=$1
 mkdir ${LOCAL_PATH_PART}/${date}
 mkdir ${LOCAL_PATH_CLAIM}/${date}
 mkdir ${LOCAL_PATH_CUSTOMER}/${date}
 mkdir ${LOCAL_PATH_DOSS}/${date}
 mkdir ${LOCAL_PATH_ORDER}/${date}
}

#向集群上传源数据
function put_raw() {
 if [ $# -lt 1 ]
 then
  echo "我们需要一个日期  比如：201701"
  exit -1
 fi
 date=$1
 hadoop fs -rm -r ${PATH_PART_R}/${date}
 hadoop fs -rm -r ${PATH_CLAIM_R}/${date}
 hadoop fs -rm -r ${PATH_CUSTOMER_R}/${date}
 hadoop fs -rm -r ${PATH_DOSS_R}/${date}
 hadoop fs -rm -r ${PATH_ORDER_R}/${date}

 hadoop fs -mkdir -p ${PATH_ORDER_R}/${date}
 hadoop fs -mkdir -p ${PATH_CLAIM_R}/${date}
 hadoop fs -mkdir -p ${PATH_CUSTOMER_R}/${date}
 hadoop fs -mkdir -p ${PATH_DOSS_R}/${date}
 hadoop fs -mkdir -p ${PATH_PART_R}/${date}

 hadoop fs -put ${LOCAL_PATH_PART}/${date}/* ${PATH_PART_R}/${date}
 hadoop fs -put ${LOCAL_PATH_CLAIM}/${date}/* ${PATH_CLAIM_R}/${date}
 hadoop fs -put ${LOCAL_PATH_CUSTOMER}/${date}/* ${PATH_CUSTOMER_R}/${date}
 hadoop fs -put ${LOCAL_PATH_DOSS}/${date}/* ${PATH_DOSS_R}/${date}
 hadoop fs -put ${LOCAL_PATH_ORDER}/${date}/* ${PATH_ORDER_R}/${date}
}

#分隔符转换
function format_delimiter_claim() {
 if [ $# -lt 1 ]
 then
  echo "我们需要一个日期  比如：201701"
  exit -1
 fi
 date=$1
 in_path=${PATH_CLAIM_R}/${date}
 out_path=${PATH_CLAIM}/${date}
 echo ${in_path}
 echo ${out_path}
 hadoop fs -rm -r ${out_path}
 hadoop jar ${PORMAT_JAR} ${FORMAT_MAINCLASS} 20 ${in_path} ${out_path}
}

#分隔符转换
function format_delimiter_order() {
 if [ $# -lt 1 ]
 then
  echo "我们需要一个日期  比如：201701"
  exit -1
 fi
 date=$1
 in_path=${PATH_ORDER_R}/${date}
 out_path=${PATH_ORDER}/${date}
 echo ${in_path}
 echo ${out_path}
 hadoop fs -rm -r ${out_path}
 hadoop jar ${PORMAT_JAR} ${FORMAT_MAINCLASS} 37 ${in_path} ${out_path}
}

#分隔符转换
function format_delimiter_doss() {
 if [ $# -lt 1 ]
 then
  echo "我们需要一个日期  比如：201701"
  exit -1
 fi
 date=$1
 in_path=${PATH_DOSS_R}/${date}
 out_path=${PATH_DOSS}/${date}
 echo ${in_path}
 echo ${out_path}
 hadoop fs -rm -r ${out_path}
 hadoop jar ${PORMAT_JAR} ${FORMAT_MAINCLASS} 17 ${in_path} ${out_path}
}

#分隔符转换
function format_delimiter_customer() {
 if [ $# -lt 1 ]
 then
  echo "我们需要一个日期  比如：201701"
  exit -1
 fi
 date=$1
 in_path=${PATH_CUSTOMER_R}/${date}
 out_path=${PATH_CUSTOMER}/${date}
 echo ${in_path}
 echo ${out_path}
 hadoop fs -rm -r ${out_path}
 hadoop jar ${PORMAT_JAR} ${FORMAT_MAINCLASS} 48 ${in_path} ${out_path}
}

#分隔符转换
#part 转化分隔符后就是事实表了，路径指向ori公有露露
function format_delimiter_part(){
 if [ $# -lt 1 ]
 then
  echo "我们需要一个日期  比如：201701"
  exit -1
 fi
 date=$1
 in_path=${PATH_PART_R}/${date}
 #out_path=${PATH_PART}/${date}
 out_path=${PATH_PART}/${date}
 echo ${in_path}
 echo ${out_path}
 hadoop fs -rm -r ${out_path}
 hadoop jar ${PORMAT_JAR} ${FORMAT_MAINCLASS} 27,yes ${in_path} ${out_path}
}

#分隔符转换
#数据标准化转 “，” 分隔符为 \001 需要在set.xml 中配置原始分隔符， 并传参告诉程序是否需要去重
function format_delimiter(){
 if [ $# -lt 1 ]
 then
  echo "我们需要一个日期  比如：201701"
  exit -1
 fi
 date=$1
 format_delimiter_claim ${date} >${log_path}/format_delimiter_claim_${date}.log 2>&1
 format_delimiter_order ${date} >${log_path}/format_delimiter_order_${date}.log 2>&1
 format_delimiter_doss ${date} >${log_path}/format_delimiter_doss_${date}.log 2>&1
 format_delimiter_customer ${date} >${log_path}/format_delimiter_customer_${date}.log 2>&1
 format_delimiter_part ${date} >${log_path}/format_delimiter_part_${date}.log 2>&1
}



#uniq 0,1,10,46 private/auto/raw/customer/chev700/ private/auto/uniq/customer/chev700 > ../log/uniq_cus.log 2>&1
#uniq 7,12 private/auto/raw/doss/chev700/ private/auto/uniq/doss/chev700 > ../log/uniq_doss.log 2>&1
#uniq 3,27 private/auto/raw/order/chev700/ private/auto/uniq/order/chev700 > ../log/uniq_order.log 2>&1
#uniq claim所有字段 private/auto/raw/order/chev700/ private/auto/uniq/order/chev700 > ../log/uniq_order.log 2>&1
#去重需要传入三个参数，第一个去重基准下标，比如2,3,4 ，第二个输入路径，第三个输出路径
function uniq_data() {
 hadoop jar ${UNIQ_JAR} ${MAIN_CLASS_UNIQ}  -Dmapreduce.job.reduces=${reduce_num} $1 $2 $3
}

function uniq_cus(){
date=$1
#in="private/auto/uniq/customer/201701_201703 private/auto/uniq/customer/chev700 private/auto/uniq/customer/supplement"
#out="private/auto/uniq/customer/combine"
in=${PATH_CUSTOMER}/${date}
out=${PATH_UNIQ_OUT_CUSTOMER}/${date}
col="0,1,10,46"
hadoop fs -rm -r ${out}
echo ${in}
echo ${out}
hadoop jar ${UNIQ_JAR} ${MAIN_CLASS_UNIQ}  -Dmapreduce.job.reduces=${reduce_num} ${col} ${in} ${out}
}

#doss表字段进行了调整，去重需要校对
function uniq_doss(){
date=$1
#in="private/auto/uniq/doss/chev700 private/auto/raw/doss/201701_201703 private/auto/raw/doss/supplement"
#out="private/auto/raw/doss/combine"
in=${PATH_DOSS}/${date}
out=${PATH_UNIQ_OUT_DOSS}/${date}
col="7,12"
echo ${in}
echo ${out}
hadoop fs -rm -r ${out}
 hadoop jar ${UNIQ_JAR} ${MAIN_CLASS_UNIQ}  -Dmapreduce.job.reduces=${reduce_num} ${col} ${in} ${out}
}

function uniq_order(){
date=$1
#in="private/auto/uniq/order/chev700 private/auto/raw/order/201701_201703 private/auto/raw/order/supplement"
#out="private/auto/raw/order/combine"
in=${PATH_ORDER}/${date}
out=${PATH_UNIQ_OUT_ORDER}/${date}

col="3,27"
echo ${in}
echo ${out}
hadoop fs -rm -r ${out}
 hadoop jar ${UNIQ_JAR} ${MAIN_CLASS_UNIQ}  -Dmapreduce.job.reduces=${reduce_num} ${col} ${in} ${out}
}


function uniq_claim(){
date=$1
#in="private/auto/uniq/claim/201701_201703 private/auto/uniq/claim/chev700 private/auto/uniq/claim/supplement"
#out="private/auto/uniq/claim/combine"
in=${PATH_CLAIM}/${date}
out=${PATH_UNIQ_OUT_CLAIM}/${date}
col="0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19"
echo ${in}
echo ${out}
hadoop fs -rm -r ${out}
 hadoop jar ${UNIQ_JAR} ${MAIN_CLASS_UNIQ}  -Dmapreduce.job.reduces=${reduce_num} ${col} ${in} ${out}
}


#打删除标签
function delete_useless(){
 if [ $# -lt 1 ]
 then
  echo "我们需要一个日期  比如：201701"
  exit -1
 fi
 date=$1
 in_path=${PATH_UNIQ_OUT_ORDER}/${date}
 out_path=${PATH_LABEL_DELETE_ORDER}/${date}
 echo ${in_path}
 echo ${out_path}
 hadoop fs -rm -r ${out_path}
 hadoop jar ${DELETE_JAR} ${DELETE_MAINCLASS} -files conf/CHE -Dmapreduce.job.reduces=${reduce_num} ${mapping} ${in_path} ${out_path}
}

#打是否首保，索赔标签  xx
function label_fm(){
 if [ $# -lt 1 ]
 then
  echo "我们需要一个日期  比如：201701"
  exit -1
 fi
 date=$1
 in_path="${PATH_LABEL_DELETE_ORDER}/${date} ${PATH_UNIQ_OUT_CLAIM}/*"
 out_path=${PATH_LABEL_FM}/${date}
 echo ${in_path}
 echo ${out_path}
 hadoop fs -rm -r ${out_path}
 hadoop jar ${LABEL_JAR} ${FM_MAINCLASS} -Dmapreduce.job.reduces=${reduce_num} ${in_path} ${out_path}
}

#打维修标签  xx
function label_fix(){
 if [ $# -lt 1 ]
 then
  echo "我们需要一个日期  比如：201701"
  exit -1
 fi
 date=$1
 in_path="${PATH_LABEL_FM}/${date} ${PATH_PART}/*"
 out_path=${PATH_LABEL_FIX}/${date}
 echo ${in_path}
 echo ${out_path}
 hadoop fs -rm -r ${out_path}
 hadoop jar ${LABEL_JAR} ${FIX_MAINCLASS} -files conf/filter,conf/engineoil -Dmapreduce.job.reduces=${reduce_num} -Dmapreduce.reduce.memory.mb=8192 -Dmapreduce.reduce.java.opts=-Xmx6964m ${mapping} ${in_path} ${out_path}
}

#打剩余的出库日期等时间相关标签，以及一级二级车型 xx
function label_other(){
 if [ $# -lt 1 ]
 then
  echo "我们需要一个日期  比如：201701"
  exit -1
 fi
 date=$1
 in_path="${PATH_LABEL_FIX}/${date} ${PATH_UNIQ_OUT_DOSS}/* ${PATH_UNIQ_OUT_CUSTOMER}/*"
 out_path=${PATH_LABEL_OTHER}/${date}
 echo ${in_path}
 echo ${out_path}
 hadoop fs -rm -r ${out_path}
 hadoop jar ${LABEL_JAR} ${OTHER_MAINCLASS} -files conf/mark -Dmapreduce.job.reduces=${reduce_num} ${mapping} ${in_path} ${out_path}
}

#给doss表打一二级车型标签以及asc_code  xx
function label_doss(){
 if [ $# -lt 1 ]
 then
  echo "我们需要一个日期  比如：201701"
  exit -1
 fi
 date=$1
 in_path=${PATH_UNIQ_OUT_DOSS}/${date}
 out_path=${PATH_LABEL_DOSS}/${date}
 echo ${in_path}
 echo ${out_path}
 hadoop fs -rm -r ${out_path}
 hadoop jar ${LABEL_JAR} ${DOSS_MAINCLASS} -files conf/mark_doss,conf/doss_asc -Dmapreduce.job.reduces=0 ${mapping} ${in_path} ${out_path}
}

#串联打标签过程
function label_all() {
 if [ $# -lt 1 ]
 then
  echo "我们需要一个日期  比如：201701"
  exit -1
 fi
 date=$1
 delete_useless ${date} >${log_path}/delete_useless_${date}.log 2>&1
 label_fm ${date} >${log_path}/label_fm_${date}.log 2>&1
 label_fix ${date} >${log_path}/label_fix_${date}.log 2>&1
 label_other ${date} >${log_path}/label_other_${date}.log 2>&1
 label_doss ${date} >${log_path}/label_doss_${date}.log 2>&1
}


#给打完标签的数据添加分区
function add_partition() {
 if [ $# -lt 1 ]
 then
  echo "我们需要一个日期  比如：201701"
  exit -1
 fi
 date=$1
 hive -e "
  use ${data_base};
  alter table claim_uniq add partition(mon='${date}') location '${date}';
  alter table customer_uniq add partition(mon='${date}') location '${date}';
  alter table label_doss add partition(mon='${date}') location '${date}';
  alter table label_order add partition(mon='${date}') location '${date}';
  alter table part add partition(mon='${date}') location '${date}';
 " >${log_path}/add_partition_${date}.log 2>&1
}

#用于检测新数据中是否有新的车型品牌
function check_new_mark() {
 if [ $# -lt 1 ]
 then
  echo "我们需要一个日期  比如：201701"
  exit -1
 fi
 date=$1
 hive -e "
  use ${data_base};
  select distinct a.makes, a.series, a.model
  from label_order a
  left outer join mark_order b
  on (b.marks = a.makes and b.series=a.series and b.models=a.model)
  where a.mon='${date}';
 " >/home/${user_name}/general/compare/order_compare 2>${log_path}/compare_order

 hive -e "
  use ${data_base};
  select distinct a.makes, a.series
  from label_doss a
  left outer join mark_doss b
  on a.makes=b.marks and a.series=b.series
  where a.mon='${date}' ;
  " >/home/${user_name}/general/compare/doss_compare 2>${log_path}/compare_doss

}


function cre_asc_baseline(){
 hive -e "
  use ${user_name};
  drop table if exists asc_baseline;
  create external table asc_baseline(
  kpi string,
  a_level string,
  b_level string,
  age_type string,
  mon string,
  own_num string,
  base_num string
  )row format delimited
  fields terminated by '\t'
  lines terminated by '\n'
  location 'hdfs://ns1/user/${user_name}/private/auto/kpi/asc_baseline/'
 "
}

function cre_base_kpi(){
 hive -e "
  use ${user_name};
  drop table if exists base_kpi;
  create external table base_kpi(
  kpi_kpi string,
  KPI string,
  KPI_Name string
  )row format delimited
  fields terminated by '\t'
  lines terminated by '\n'
  location 'hdfs://ns1/user/${user_name}/private/auto/kpi/base_kpi/'
 "
}
#创建customer表
function cre_customer() {
 hive -e "
  use ${data_base};  drop table if exists customer;
  create external table customer(
  VIN string,
  LNUMBER string,
  MAKES string,
  SERIES string,
  MODEL string,
  CONF1 string,
  COLOR string,
  PURCHASE_DATE string,
  MILEAGE string,
  CUSTOMER_TYPE string,
  CH_CODE string,
  ASC string,
  OWNER_NAME string,
  OWNER_ID string,
  SEXUAL string,
  INDUSTRY string,
  PROVINCE string,
  CITY string,
  ADD string,
  ZIP string,
  PHONENUMBER string,
  MOBILE string,
  BIRTHDATE string,
  EMAIL string,
  MARRIAGE string,
  EDUCATION string,
  ENTERPRISE_CODE string,
  ENTERPRISE_TYPE string,
  CONTACTOR_SEXUAL string,
  CONTACTOR_NAME string,
  CONTACTOR_PHONE string,
  CONTACTOR_MOBILE string,
  CONTACTOR_PROVINCE string,
  CONTACTOR_CITY string,
  CONTACTOR_ADD string,
  CONTACTOR_ZIP string,
  CONSULTANT string,
  INSURANCE_CODE string,
  INSURANCE_NAME string,
  INSURANCE_BEGIN_DATE string,
  INSURANCE_ORDER_DATE string,
  RETURNER_NAME string,
  RETURNER_SEXUAL string,
  RETURNER_PHONE_AREACODE string,
  RETURNER_PHONE string,
  RETURNER_MOBILE string,
  OWNER_CODE string,
  OWNER_TYPE string
  )partitioned by(
   mon string
  )row format delimited
  fields terminated by '\001'
  lines terminated by '\n'
  location 'hdfs://ns1/user/${user_name}/private/auto/raw/customer/'
 "
}

function cre_distributor(){
 hive -e "
  use ${data_base};
  drop table if exists distributor;
  create external table distributor(
  chcode string,
  asccode string,
  asc string
  )row format delimited
  fields terminated by '\t'
  lines terminated by '\n'
  location 'hdfs://ns1/user/${user_name}/private/auto/assistant/distributor/'
 "
}

function cre_flow(){
 hive -e "
  use ${data_base};
  drop table if exists flow;
  create external table flow(
  order_number string,
  vin string,
  number string,
  owner_code string,
  birthdate string,
  sexual string,
  province string,
  city string,
  asc_code string,
  ASC string,
  outdate string,
  deal_year string,
  deal_date string,
  part_number string,
  NAME string,
  quantity string,
  SALES_AMOUNT string,
  age string,
  BNJK string,
  BWJK string,
  mileage string,
  MAINT_TYPE string,
  primary_classification string
  )row format delimited
  fields terminated by '\t'
  lines terminated by '\n'
  location 'hdfs://ns1/user/${user_name}/private/auto/kpi/flow/'
 "
}

#创建claim表
function cre_claim() {
 hive -e "
  use ${data_base};
  drop table if exists claim;
  create external table claim(
  ADDITIONAL_LABLE string,
  DEAL_DATE string,
  CLAIM_ORDER_NUMBER string,
  RANK_NUMBER string,
  CLAIM_RESULT string,
  CLAIM_TYPE string,
  DEDUCTION string,
  CLAIM_ACCEPTED_AMOUNT string,
  GWM_CLAIM_NUMBER string,
  GWM_VERSION string,
  HOURLY_AMOUNT string,
  OPERATION_CODE string,
  ROWNUMBER string,
  OTHER_CHARGE string,
  PART_AMOUNT string,
  CLAIM_AMOUNT string,
  CLAIM_ACCEPTED_AMOUNT_TAX string,
  CH_CODE string,
  ASC string,
  ASC_CODE string
  )partitioned by(
   mon string
  )row format delimited
  fields terminated by '\001'
  lines terminated by '\n'
  location 'hdfs://ns1/user/${user_name}/private/auto/raw/claim/'
 "
}

#创建doss表
function cre_doss() {
 hive -e "
  use ${data_base};
  drop table if exists doss;
  create external table doss(
  OWNER_NAME string,
  CUSTOMER_SALES_TYPE string,
  SEXUAL string,
  BIRTHDATE string,
  PROVINCE string,
  CITY string,
  ADD string,
  VIN string,
  CONF1 string,
  COLOR string,
  INVOICE_DATE string,
  REPORT_DATE string,
  DEALER_NUMBER string,
  CUSTOMER_TYPE string,
  MAKES string,
  SERIES string,
  DEALER string
  )partitioned by(
   mon string
  )row format delimited
  fields terminated by '\001'
  lines terminated by '\n'
  location 'hdfs://ns1/user/${user_name}/private/auto/raw/doss/'
 "
}

#创建order表
function cre_order() {
 hive -e "
  use ${data_base};
  drop table if exists order;
  create external table order(
  RANK_NUMBER string,
  ORDER_STATUS string,
  ORDER_BILL_DATE string,
  ORDER_NUMBER string,
  BALANCE_NO string,
  RECEPTIONIST string,
  LNUMBER string,
  MAKES string,
  SERIES string,
  MODEL string,
  MAINT_AMOUNT string,
  ACCEPTANCE string,
  MILEAGE string,
  OWNER_NAME string,
  RETURNER_NAME string,
  RETURNER_SEXUAL string,
  RETURNER_PHONE_AREACODE string,
  RETURNER_PHONE string,
  RETURNER_MOBILE string,
  PREDICTED_DELIVERY_DATE string,
  COMPLETE_DATE string,
  DELIVERY_DATE string,
  OWNER_CODE string,
  OWNER_TYPE string,
  CLAIM_ORDER_NUMBER string,
  TRIALER string,
  VIN string,
  ASC_CODE string,
  ORDER_BALANCE_DATE string,
  ORDER_CLEAR_DATE string,
  ORDER_BALANCE_AMOUNT string,
  PAID_AMOUNT string,
  MAINT_DESC string,
  ORDER_DESC string,
  ORDER_TYPE string,
  MAINT_TYPE string,
  BALANCE_NO_1 string
  )partitioned by(
   mon string
  )row format delimited
  fields terminated by '\001'
  lines terminated by '\n'
  location 'hdfs://ns1/user/${user_name}/private/auto/raw/order/'
 "
}

#创建part表
function cre_part() {
 hive -e "
  use ${data_base};
  drop table if exists part;
  create external table part(
  ASC_CODE string,
  STOCK string,
  PART_NUMBER string,
  PART_DESC string,
  COST_PRICE string,
  COST_AMOUNT string,
  SALES_PRICE string,
  PART_DIS string,
  PART_REC string,
  VALUE_TYPE string,
  RANK_NUMBER string,
  LAST_BALANCE_NO string,
  ORDER_BILL_DATE string,
  ORDER_BALANCE_DATE string,
  ORDER_CLEAR_DATE string,
  ORDER_NUMBER string,
  ORDER_BALANCE_NUMBER string,
  RECEPTIONIST string,
  ORDER_DESC string,
  MAINT_TYPE string,
  MAINT_DESC string,
  LNUMBER string,
  VIN string,
  MAKES string,
  SERIES string,
  MODEL string,
  SALES_AMOUNT string
  )partitioned by(
   mon string
  )row format delimited
  fields terminated by '\001'
  lines terminated by '\n'
  location 'hdfs://ns1/user/${user_name}/private/auto/raw/part/'
 "
}

#创建customer_uniq表
function cre_customer_uniq() {
 hive -e "
  use ${data_base};  drop table if exists customer_uniq;
  create external table customer_uniq(
  VIN string,
  LNUMBER string,
  MAKES string,
  SERIES string,
  MODEL string,
  CONF1 string,
  COLOR string,
  PURCHASE_DATE string,
  MILEAGE string,
  customer_TYPE string,
  CH_CODE string,
  ASC string,
  OWNER_NAME string,
  OWNER_ID string,
  SEXUAL string,
  INDUSTRY string,
  PROVINCE string,
  CITY string,
  ADD string,
  ZIP string,
  PHONENUMBER string,
  MOBILE string,
  BIRTHDATE string,
  EMAIL string,
  MARRIAGE string,
  EDUCATION string,
  ENTERPRISE_CODE string,
  ENTERPRISE_TYPE string,
  CONTACTOR_SEXUAL string,
  CONTACTOR_NAME string,
  CONTACTOR_PHONE string,
  CONTACTOR_MOBILE string,
  CONTACTOR_PROVINCE string,
  CONTACTOR_CITY string,
  CONTACTOR_ADD string,
  CONTACTOR_ZIP string,
  CONSULTANT string,
  INSURANCE_CODE string,
  INSURANCE_NAME string,
  INSURANCE_BEGIN_DATE string,
  INSURANCE_ORDER_DATE string,
  RETURNER_NAME string,
  RETURNER_SEXUAL string,
  RETURNER_PHONE_AREACODE string,
  RETURNER_PHONE string,
  RETURNER_MOBILE string,
  OWNER_CODE string,
  OWNER_TYPE string
  )partitioned by(
   mon string
  )row format delimited
  fields terminated by '\001'
  lines terminated by '\n'
  location 'hdfs://ns1/user/${user_name}/private/auto/uniq/customer/'
 "
}


function cre_customer_uniq_2200621() {
 hive -e "
  use ${data_base};  drop table if exists customer_uniq_2200621;
  create external table customer_uniq_2200621(
  VIN string,
  LNUMBER string,
  MAKES string,
  SERIES string,
  MODEL string,
  CONF1 string,
  COLOR string,
  PURCHASE_DATE string,
  MILEAGE string,
  customer_TYPE string,
  CH_CODE string,
  ASC string,
  OWNER_NAME string,
  OWNER_ID string,
  SEXUAL string,
  INDUSTRY string,
  PROVINCE string,
  CITY string,
  ADD string,
  ZIP string,
  PHONENUMBER string,
  MOBILE string,
  BIRTHDATE string,
  EMAIL string,
  MARRIAGE string,
  EDUCATION string,
  ENTERPRISE_CODE string,
  ENTERPRISE_TYPE string,
  CONTACTOR_SEXUAL string,
  CONTACTOR_NAME string,
  CONTACTOR_PHONE string,
  CONTACTOR_MOBILE string,
  CONTACTOR_PROVINCE string,
  CONTACTOR_CITY string,
  CONTACTOR_ADD string,
  CONTACTOR_ZIP string,
  CONSULTANT string,
  INSURANCE_CODE string,
  INSURANCE_NAME string,
  INSURANCE_BEGIN_DATE string,
  INSURANCE_ORDER_DATE string,
  RETURNER_NAME string,
  RETURNER_SEXUAL string,
  RETURNER_PHONE_AREACODE string,
  RETURNER_PHONE string,
  RETURNER_MOBILE string,
  OWNER_CODE string,
  OWNER_TYPE string
  )partitioned by(
   mon string
  )row format delimited
  fields terminated by '\001'
  lines terminated by '\n'
  location 'hdfs://ns1/user/${user_name}/private/auto/uniq/customer_uniq_2200621/'
 "
}


#创建claim_uniq表
function cre_claim_uniq() {
 hive -e "
  use ${data_base};  drop table if exists claim_uniq;
  create external table claim_uniq(
  ADDITIONAL_LABLE string,
  DEAL_DATE string,
  CLAIM_ORDER_NUMBER string,
  RANK_NUMBER string,
  CLAIM_RESULT string,
  CLAIM_TYPE string,
  DEDUCTION string,
  CLAIM_ACCEPTED_AMOUNT string,
  GWM_CLAIM_NUMBER string,
  GWM_VERSION string,
  HOURLY_AMOUNT string,
  OPERATION_CODE string,
  ROWNUMBER string,
  OTHER_CHARGE string,
  PART_AMOUNT string,
  CLAIM_AMOUNT string,
  CLAIM_ACCEPTED_AMOUNT_TAX string,
  CH_CODE string,
  ASC string,
  ASC_CODE string
  )partitioned by(
   mon string
  )row format delimited
  fields terminated by '\001'
  lines terminated by '\n'
  location 'hdfs://ns1/user/${user_name}/private/auto/uniq/claim/'
 "
}

function cre_claim_uniq_2200621() {
 hive -e "
  use ${data_base};
  drop table if exists claim_uniq_2200621;
  create external table claim_uniq_2200621(
  ADDITIONAL_LABLE string,
  DEAL_DATE string,
  CLAIM_ORDER_NUMBER string,
  RANK_NUMBER string,
  CLAIM_RESULT string,
  CLAIM_TYPE string,
  DEDUCTION string,
  CLAIM_ACCEPTED_AMOUNT string,
  GWM_CLAIM_NUMBER string,
  GWM_VERSION string,
  HOURLY_AMOUNT string,
  OPERATION_CODE string,
  ROWNUMBER string,
  OTHER_CHARGE string,
  PART_AMOUNT string,
  CLAIM_AMOUNT string,
  CLAIM_ACCEPTED_AMOUNT_TAX string,
  CH_CODE string,
  ASC string,
  ASC_CODE string
  )partitioned by(
   mon string
  )row format delimited
  fields terminated by '\001'
  lines terminated by '\n'
  location 'hdfs://ns1/user/${user_name}/private/auto/uniq/claim_uniq_2200621/'
 "
}

#创建doss_uniq表
function cre_doss_uniq() {
 hive -e "
  use ${data_base};
  drop table if exists doss_uniq;
  create external table doss_uniq(
  OWNER_NAME string,
  customer_uniq_SALES_TYPE string,
  SEXUAL string,
  BIRTHDATE string,
  PROVINCE string,
  CITY string,
  ADD string,
  VIN string,
  CONF1 string,
  COLOR string,
  INVOICE_DATE string,
  REPORT_DATE string,
  DEALER_NUMBER string,
  customer_TYPE string,
  MAKES string,
  SERIES string,
  DEALER string
  )partitioned by(
   mon string
  )row format delimited
  fields terminated by '\001'
  lines terminated by '\n'
  location 'hdfs://ns1/user/${user_name}/private/auto/uniq/doss/'
 "
}

#创建order_uniq表
function cre_order_uniq() {
 hive -e "
  use ${data_base};
  drop table if exists order_uniq;
  create external table order_uniq(
  RANK_NUMBER string,
  ORDER_STATUS string,
  ORDER_BILL_DATE string,
  ORDER_NUMBER string,
  BALANCE_NO string,
  RECEPTIONIST string,
  LNUMBER string,
  MAKES string,
  SERIES string,
  MODEL string,
  MAINT_AMOUNT string,
  ACCEPTANCE string,
  MILEAGE string,
  OWNER_NAME string,
  RETURNER_NAME string,
  RETURNER_SEXUAL string,
  RETURNER_PHONE_AREACODE string,
  RETURNER_PHONE string,
  RETURNER_MOBILE string,
  PREDICTED_DELIVERY_DATE string,
  COMPLETE_DATE string,
  DELIVERY_DATE string,
  OWNER_CODE string,
  OWNER_TYPE string,
  CLAIM_ORDER_NUMBER string,
  TRIALER string,
  VIN string,
  ASC_CODE string,
  ORDER_BALANCE_DATE string,
  ORDER_CLEAR_DATE string,
  ORDER_BALANCE_AMOUNT string,
  PAID_AMOUNT string,
  MAINT_DESC string,
  ORDER_DESC string,
  ORDER_TYPE string,
  MAINT_TYPE string,
  BALANCE_NO_1 string
  )partitioned by(
   mon string
  )row format delimited
  fields terminated by '\001'
  lines terminated by '\n'
  location 'hdfs://ns1/user/${user_name}/private/auto/uniq/order/'
 "
}

#创建part_uniq表
function cre_part_uniq() {
 hive -e "
  use ${data_base};
  drop table if exists part_uniq;
  create external table part_uniq(
  ASC_CODE string,
  STOCK string,
  PART_NUMBER string,
  PART_DESC string,
  COST_PRICE string,
  COST_AMOUNT string,
  SALES_PRICE string,
  PART_DIS string,
  PART_REC string,
  VALUE_TYPE string,
  RANK_NUMBER string,
  LAST_BALANCE_NO string,
  ORDER_BILL_DATE string,
  ORDER_BALANCE_DATE string,
  ORDER_CLEAR_DATE string,
  ORDER_NUMBER string,
  ORDER_BALANCE_NUMBER string,
  RECEPTIONIST string,
  ORDER_DESC string,
  MAINT_TYPE string,
  MAINT_DESC string,
  LNUMBER string,
  VIN string,
  MAKES string,
  SERIES string,
  MODEL string,
  SALES_AMOUNT string
  )partitioned by(
   mon string
  )row format delimited
  fields terminated by '\001'
  lines terminated by '\n'
  location 'hdfs://ns1/user/${user_name}/private/auto/uniq/part/'
 "
}


#cre_customer
#cre_claim
#cre_doss
#cre_order
#cre_part

#cre_customer_uniq
#cre_claim_uniq
#cre_doss_uniq
#cre_order_uniq
#cre_part

function add_part() {
 hive -e "
  use ${data_base};
  alter table customer_uniq  add partition (mon = 'chev700') location 'chev700';
  alter table claim_uniq  add partition (mon = 'chev700') location 'chev700';
  alter table doss_uniq  add partition (mon = 'chev700') location 'chev700';
  alter table order_uniq  add partition (mon = 'chev700') location 'chev700';
 "
}

function cre_label_order(){
hive -e "
  use ${data_base};
  drop table if exists label_order;
  create external table label_order(
  RANK_NUMBER string,
  ORDER_STATUS string,
  ORDER_BILL_DATE string,
  ORDER_NUMBER string,
  BALANCE_NO string,
  RECEPTIONIST string,
  LNUMBER string,
  MAKES string,
  SERIES string,
  MODEL string,
  MAINT_AMOUNT string,
  ACCEPTANCE string,
  MILEAGE string,
  OWNER_NAME string,
  RETURNER_NAME string,
  RETURNER_SEXUAL string,
  RETURNER_PHONE_AREACODE string,
  RETURNER_PHONE string,
  RETURNER_MOBILE string,
  PREDICTED_DELIVERY_DATE string,
  COMPLETE_DATE string,
  DELIVERY_DATE string,
  OWNER_CODE string,
  OWNER_TYPE string,
  CLAIM_ORDER_NUMBER string,
  TRIALER string,
  VIN string,
  ASC_CODE string,
  ORDER_BALANCE_DATE string,
  ORDER_CLEAR_DATE string,
  ORDER_BALANCE_AMOUNT string,
  PAID_AMOUNT string,
  MAINT_DESC string,
  ORDER_DESC string,
  ORDER_TYPE string,
  MAINT_TYPE string,
  BALANCE_NO_1 string,
  MAINT_TYPE1 string,
  first_maintnance string,
  claim string,
  outdate string,
  number_of_month string,
  age_type string,
  age_type2 string,
  bn string,
  primary_classification string,
  second_level_classification string
  )partitioned by(
   mon string
  )row format delimited
  fields terminated by '\001'
  lines terminated by '\n'
  location 'hdfs://ns1/user/${user_name}/private/auto/label/order/other/'
 "
}


function cre_label_order_bnwjk(){
hive -e "
  use ${data_base};
  drop table if exists label_order_bnwjk;
  create external table label_order_bnwjk(
  RANK_NUMBER string,
  ORDER_STATUS string,
  ORDER_BILL_DATE string,
  ORDER_NUMBER string,
  BALANCE_NO string,
  RECEPTIONIST string,
  LNUMBER string,
  MAKES string,
  SERIES string,
  MODEL string,
  MAINT_AMOUNT string,
  ACCEPTANCE string,
  MILEAGE string,
  OWNER_NAME string,
  RETURNER_NAME string,
  RETURNER_SEXUAL string,
  RETURNER_PHONE_AREACODE string,
  RETURNER_PHONE string,
  RETURNER_MOBILE string,
  PREDICTED_DELIVERY_DATE string,
  COMPLETE_DATE string,
  DELIVERY_DATE string,
  OWNER_CODE string,
  OWNER_TYPE string,
  CLAIM_ORDER_NUMBER string,
  TRIALER string,
  VIN string,
  ASC_CODE string,
  ORDER_BALANCE_DATE string,
  ORDER_CLEAR_DATE string,
  ORDER_BALANCE_AMOUNT string,
  PAID_AMOUNT string,
  MAINT_DESC string,
  ORDER_DESC string,
  ORDER_TYPE string,
  MAINT_TYPE string,
  BALANCE_NO_1 string,
  MAINT_TYPE1 string,
  first_maintnance string,
  claim string,
  outdate string,
  number_of_month string,
  age_type string,
  age_type2 string,
  bn string,
  primary_classification string,
  second_level_classification string,
  bnjk string,
  bwjk string
  )row format delimited
  fields terminated by '\001'
  lines terminated by '\n'
  location 'hdfs://ns1/user/${user_name}/private/auto/label/order/bnwjk/'
 "
}


function cre_enclosure(){
hive -e "
  use ${data_base};
  drop table if exists enclosure;
  create external table enclosure(
  Part_Num string,
  info string,
  classify string,
  Name_Chinese string,
  brand string,
  type string
  )row format delimited
  fields terminated by '\t'
  lines terminated by '\n'
  location 'hdfs://ns1/user/${user_name}/private/auto/assistant/enclosure/'
 "
}

function cre_high_flow_parts(){
 hive -e "
  use ${data_base};
  drop table if exists high_flow_parts;
  create external table high_flow_parts(
  part_num string,
  type string,
  type1 string
  )row format delimited
  fields terminated by '\t'
  lines terminated by '\n'
  location 'hdfs://ns1/user/${user_name}/private/auto/assistant/high_flow_parts/'
 "
}

function cre_customer_mark(){
hive -e "
  use ${user_name};
  drop table if exists customer_mark;
  create external table customer_mark(
  marks string,
  series string,
  model string,
  yjcx string
  )row format delimited
  fields terminated by '\t'
  lines terminated by '\n'
  location 'hdfs://ns1/user/${user_name}/private/auto/assistant/customer_mark/'
 "
}

function cre_db_base(){
hive -e "
  use ${user_name};
  drop table if exists db_base;
  create external table db_base(
   ch_code string,
   asc string,
   vin string,
   yjcx string,
   invoicedate string,
   bnjk string,
   bwjk string
  )row format delimited
  fields terminated by '\t'
  lines terminated by '\n'
  location 'hdfs://ns1/user/${user_name}/private/auto/assistant/db_base/'
 "
}
function cre_filter(){
 hive -e "
  use ${data_base};
  drop table if exists filter;
  create external table filter(
  part_num string,
  name_en string,
  name_Chinese string
  )row format delimited
  fields terminated by '\t'
  lines terminated by '\n'
  location 'hdfs://ns1/user/${user_name}/private/auto/assistant/filter/'
 "
}

function cre_engine_oil(){
hive -e "
  use ${data_base};
  drop table if exists engine_oil;
  create external table engine_oil(
  part_num string,
  part_name string,
  brand string,
  type string
  )row format delimited
  fields terminated by '\t'
  lines terminated by '\n'
  location 'hdfs://ns1/user/${user_name}/private/auto/assistant/engine_oil/'
 "
}

function cre_maintnance(){
 hive -e "
  use ${data_base};
  drop table if exists maintnance;
  create external table maintnance(
  part_num string,
  part_name string,
  type string
  )row format delimited
  fields terminated by '\t'
  lines terminated by '\n'
  location 'hdfs://ns1/user/${user_name}/private/auto/assistant/maintnance/'
 "
}

function cre_kpi_total(){
 hive -e "
  use ${data_base};
  drop table if exists kpi_total;
  create external table kpi_total(
  mon string,
  num string,
  asc_code string
  )partitioned by(
   mon_p string,
   kpi string
  )row format delimited
  fields terminated by '\t'
  lines terminated by '\n'
  location 'hdfs://ns1/user/${user_name}/private/auto/kpi/kpi_total/'
 "
}

function cre_kpi_a_level(){
 hive -e "
  use ${data_base};
  drop table if exists kpi_a_level;
  create external table kpi_a_level(
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
  location 'hdfs://ns1/user/${user_name}/private/auto/kpi/kpi_a_level/'
 "
}

function cre_kpi_b_level(){
 hive -e "
  use ${data_base};
  drop table if exists kpi_b_level;
  create external table kpi_b_level(
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
  location 'hdfs://ns1/user/${user_name}/private/auto/kpi/kpi_b_level/'
 "
}

function cre_kpi_agetype(){
 hive -e "
  use ${data_base};
  drop table if exists kpi_agetype;
  create external table kpi_agetype(
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
  location 'hdfs://ns1/user/${user_name}/private/auto/kpi/kpi_agetype/'
 "
}

function cre_kpi_bnbw(){
 hive -e "
  use ${data_base};
  drop table if exists kpi_bnbw;
  create external table kpi_bnbw(
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
  location 'hdfs://ns1/user/${user_name}/private/auto/kpi/kpi_bnbw/'
 "
}

function cre_kpi_a_agetype(){
 hive -e "
  use ${data_base};
  drop table if exists kpi_a_agetype;
  create external table kpi_a_agetype(
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
  location 'hdfs://ns1/user/${user_name}/private/auto/kpi/kpi_a_agetype/'
 "
}

function cre_kpi_b_agetype(){
 hive -e "
  use ${data_base};
  drop table if exists kpi_b_agetype;
  create external table kpi_b_agetype(
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
  location 'hdfs://ns1/user/${user_name}/private/auto/kpi/kpi_b_agetype/'
 "
}

function cre_kpi_a_bnbw(){
 hive -e "
  use ${data_base};
  drop table if exists kpi_a_bnbw;
  create external table kpi_a_bnbw(
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
  location 'hdfs://ns1/user/${user_name}/private/auto/kpi/kpi_a_bnbw/'
 "
}

function cre_kpi_b_bnbw(){
 hive -e "
  use ${data_base};
  drop table if exists kpi_b_bnbw;
  create external table kpi_b_bnbw(
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
  location 'hdfs://ns1/user/${user_name}/private/auto/kpi/kpi_b_bnbw/'
 "
}

function cre_kpi_total_chev(){
 hive -e "
  use ${data_base};
  drop table if exists kpi_total_chev;
  create external table kpi_total_chev(
  mon string,
  num string,
  asc_code string
  )partitioned by(
   mon_p string,
   kpi string
  )row format delimited
  fields terminated by '\t'
  lines terminated by '\n'
  location 'hdfs://ns1/user/${user_name}/private/auto/kpi/kpi_total_chev/'
 "
}

function cre_kpi_a_level_chev(){
 hive -e "
  use ${data_base};
  drop table if exists kpi_a_level_chev;
  create external table kpi_a_level_chev(
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
  location 'hdfs://ns1/user/${user_name}/private/auto/kpi/kpi_a_level_chev/'
 "
}

function cre_kpi_b_level_chev(){
 hive -e "
  use ${data_base};
  drop table if exists kpi_b_level_chev;
  create external table kpi_b_level_chev(
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
  location 'hdfs://ns1/user/${user_name}/private/auto/kpi/kpi_b_level_chev/'
 "
}

function cre_kpi_agetype_chev(){
 hive -e "
  use ${data_base};
  drop table if exists kpi_agetype_chev;
  create external table kpi_agetype_chev(
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
  location 'hdfs://ns1/user/${user_name}/private/auto/kpi/kpi_agetype_chev/'
 "
}

function cre_kpi_bnbw_chev(){
 hive -e "
  use ${data_base};
  drop table if exists kpi_bnbw_chev;
  create external table kpi_bnbw_chev(
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
  location 'hdfs://ns1/user/${user_name}/private/auto/kpi/kpi_bnbw_chev/'
 "
}

function cre_kpi_a_agetype_chev(){
 hive -e "
  use ${data_base};
  drop table if exists kpi_a_agetype_chev;
  create external table kpi_a_agetype_chev(
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
  location 'hdfs://ns1/user/${user_name}/private/auto/kpi/kpi_a_agetype_chev/'
 "
}

function cre_kpi_b_agetype_chev(){
 hive -e "
  use ${data_base};
  drop table if exists kpi_b_agetype_chev;
  create external table kpi_b_agetype_chev(
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
  location 'hdfs://ns1/user/${user_name}/private/auto/kpi/kpi_b_agetype_chev/'
 "
}

function cre_kpi_a_bnbw_chev(){
 hive -e "
  use ${data_base};
  drop table if exists kpi_a_bnbw_chev;
  create external table kpi_a_bnbw_chev(
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
  location 'hdfs://ns1/user/${user_name}/private/auto/kpi/kpi_a_bnbw_chev/'
 "
}

function cre_kpi_b_bnbw_chev(){
 hive -e "
  use ${data_base};
  drop table if exists kpi_b_bnbw_chev;
  create external table kpi_b_bnbw_chev(
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
  location 'hdfs://ns1/user/${user_name}/private/auto/kpi/kpi_b_bnbw_chev/'
 "
}


function cre_label_doss(){
 hive -e "
  use ${data_base};
  drop table if exists label_doss;
  create external table label_doss(
  OWNER_NAME string,
  CUSTOMER_SALES_TYPE string,
  SEXUAL string,
  BIRTHDATE string,
  PROVINCE string,
  CITY string,
  ADD string,
  VIN string,
  CONF1 string,
  COLOR string,
  INVOICE_DATE string,
  REPORT_DATE string,
  DEALER_NUMBER string,
  CUSTOMER_TYPE string,
  MAKES string,
  SERIES string,
  DEALER string,
  primary_classification string,
  second_level_classification string
  )partitioned by(
   mon string
  )row format delimited
  fields terminated by '\001'
  lines terminated by '\n'
  location 'hdfs://ns1/user/${user_name}/private/auto/label/doss/';
 "
}

#创建mark_order
function cre_mark_order(){
 hive -e "
  use ${data_base};
  drop table if exists mark_order;
  create external table mark_order(
  marks string,
  series string,
  models string,
  fc string,
  sc string
  )row format delimited
  fields terminated by '\t'
  lines terminated by '\n'
  location 'hdfs://ns1/user/${user_name}/private/auto/assistant/mark_order/'
 "
}

#创建mark_doss表
function cre_mark_doss(){
 hive -e "
  use ${data_base};
  drop table if exists mark_doss;
  create external table mark_doss(
  marks string,
  series string,
  fc string,
  sc string
  )row format delimited
  fields terminated by '\t'
  lines terminated by '\n'
  location 'hdfs://ns1/user/${user_name}/private/auto/assistant/mark_doss/'
 "
}

function cre_jipanshu(){
 hive -e "
  use ${user_name};
  drop table if exists jipanshu;
  create external table jipanshu(
  asc_code string,
  primary_classification, string,
  age_type, string,
  baoyang, string,
  weixiu, string,
  ranyouyanghu, string,
  runhuayanghu, string,
  kongtiaoyanghu, string,
  jieqimenyanghu, string,
  diduanjiyou, string,
  zhongduanjiyou, string,
  gaoduanjiyou, string,
  jilv, string,
  luntai, string,
  xudianchi, string,
  shachepian, string,
  huohuasai, string,
  shachepan string
  )partitioned by(
   mon string,
   kpi string
  )row format delimited
  fields terminated by '\t'
  lines terminated by '\n'
  location 'hdfs://ns1/user/${user_name}/private/auto/kpi/jipanshu/'
 "
 }
#cre_kpi_total
#cre_kpi_a_level
#cre_kpi_b_level
#cre_kpi_agetype
#cre_kpi_bnbw
#cre_kpi_a_agetype
#cre_kpi_b_agetype
#cre_kpi_a_bnbw
#cre_kpi_b_bnbw

#add jar /home/${user_name}/general/bin/transform-date.jar;
#CREATE TEMPORARY FUNCTION tran_date AS 'com.gm.transformDate.TransformDate';

#uniq 0,1,10,46 private/auto/raw/customer/chev700/ private/auto/uniq/customer/chev700 > ../log/uniq_cus.log 2>&1
#uniq 7,12 private/auto/raw/doss/chev700/ private/auto/uniq/doss/chev700 > ../log/uniq_doss.log 2>&1
#uniq 3,27 private/auto/raw/order/chev700/ private/auto/uniq/order/chev700 > ../log/uniq_order.log 2>&1

#cre_label_order
#cre_enclosure
#cre_high_flow_parts
#cre_filter
#cre_engine_oil
#cre_maintnance


#delete_useless ${PATH_UNIQ_OUT_ORDER} ${PATH_LABEL_DELETE}
#label_fix ${PATH_LABEL_FM} ${PATH_PART} ${PATH_LABEL_FIX}
#label_doss ${PATH_UNIQ_OUT_DOSS} ${PATH_LABEL_DOSS}
#label_other ${PATH_UNIQ_OUT_DOSS} ${PATH_UNIQ_OUT_CUSTOMER} ${PATH_LABEL_FIX} ${PATH_LABEL_OTHER}
#label_fm ${PATH_LABEL_DELETE} ${PATH_UNIQ_OUT_CLAIM} ${PATH_LABEL_FM}

if [ $# -lt 1 ]
then
 echo "sh $0 <function_name> [date]"
 exit 1
fi
$1 $2
