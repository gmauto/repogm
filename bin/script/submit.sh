#!/bin/bash

#用户名
user_name=ipsos_test4

#日志路径
log_path=/home/${user_name}/general/log

date=201704_201707


#sh auto_cretb.sh mkdifile
#sh auto_cretb.sh cre_tb_fact
#sh auto_cretb.sh cre_tb_dim
#sh auto_cretb.sh cre_tb_kpi_tmp 8areas
#sh auto_cretb.sh cre_tb_kpi_tmp 8areas_k1
#sh auto_cretb.sh  cre_tb_kpi_other 8areas
#sh auto_cretb.sh  cre_flow_dim
#sh auto_cretb.sh  cre_flow
#sh auto_cretb.sh load_data_dim 1
#sh auto_cretb.sh add_par 1
#sh auto_cretb.sh cre_check_table 
#sh auto_cretb.sh add_par_check 201704_201707
#sh auto_mr.sh  mkdir_local 201704_201707
#sh auto_mr.sh put_raw 201704_201707

#format_delimiter
#sh auto_check.sh fun_all 
#sh load_data.sh load_data_ff 3
#sh load_data.sh load_localfile 3
#sh auto_check.sh fun_alli
#sh auto_mr.sh format_delimiter ${date} >${log_path}/format_delimiter_part_${date}.log 2>&1
#sh auto_mr.sh uniq_cus ${date} >${log_path}/format_delimiter_cus_${date}.log 2>&1
#sh auto_mr.sh uniq_doss  ${date} >${log_path}/format_delimiter_dosses_${date}.log 2>&1
#sh auto_mr.sh uniq_order ${date} >${log_path}/format_delimiter_ord_${date}.log 2>&1
#sh auto_mr.sh uniq_claim ${date} >${log_path}/format_delimiter_claims_${date}.log 2>&1
#sh auto_mr.sh delete_useless ${date} >${log_path}/delete_useless_${date}.log 2>&1
#sh auto_mr.sh label_fm ${date} >${log_path}/label_fm_${date}.log 2>&1

#sh auto_mr.sh label_fix ${date} >${log_path}/label_fix_${date}_2017-10-25_1.log 2>&1
#sh auto_mr.sh label_other ${date} >${log_path}/label_other_${date}.log 2>&1
#sh auto_mr.sh label_doss ${date} >${log_path}/label_doss_${date}.log 2>&1
#sh auto_mr.sh add_partition 201704_201707

source /etc/profile
startTime=`date +"%s.%N"`
#spark-submit --master yarn --driver-memory 3g --executor-memory 5g --num-executors 30 --executor-cores 1 /home/ipsos_test4/general/bin/zjxj.py 2017-04 2017-07 ipsos_test4 time_record_zjxj >../log/zjxj_test 2>&1 
spark-submit --master yarn --driver-memory 3g --executor-memory 5g --num-executors 30 --executor-cores 1 /home/ipsos_test4/general/bin/zhanbi.py 2017-04 2017-07 ipsos_test4 time_record_zhanbi > ../log/zhanbi_test 2>&1 
#全国月度
spark-submit --master yarn --driver-memory 3g --executor-memory 5g --num-executors 30 --executor-cores 1 /home/ipsos_test4/general/bin/fjp.py 2010-04 2017-07 ipsos_test4 whole_country everymonth >../log/fjp_test 2>&1 
spark-submit --master yarn --driver-memory 3g --executor-memory 5g --num-executors 30 --executor-cores 1 /home/ipsos_test4/general/bin/jpl.py 2010-04 2017-07 ipsos_test4 whole_country everymonth >../log/jpl_test 2>&1 
#单店月度

endTime=`date +"%s.%N"`
echo `awk -v x1="$(echo $endTime | cut -d '.' -f 1)" -v x2="$(echo $startTime | cut -d '.' -f 1)" -v y1="$[$(echo $endTime | cut -d '.' -f 2) / 1000]" -v y2="$[$(echo $startTime | cut -d '.' -f 2) /1000]" 'BEGIN{printf "RunTime:%.1f s",(x1-x2)+(y1-y2)/1000000}'` >> ../log/time_log
