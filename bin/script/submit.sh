#!/bin/bash

#用户名
user_name=ipsos_test4

#日志路径
log_path=/home/${user_name}/general/log

date=201704_201707

#创建集群文件路径和本地文件路径
#sh auto_cretb.sh mkdifile
#创建kpi计算之前的五张数据存储表
#sh auto_cretb.sh cre_tb_fact
#创建kpi计算的辅助表
#sh auto_cretb.sh cre_tb_dim
#创建kpi计算的表
#sh auto_cretb.sh cre_tb_kpi_tmp 8areas
#sh auto_cretb.sh cre_tb_kpi_tmp 8areas_k1
#sh auto_cretb.sh  cre_tb_kpi_other 8areas
#创建kpi计算的相关辅助表
#sh auto_cretb.sh  cre_flow_dim
#创建流水表
#sh auto_cretb.sh  cre_flow
#创建数据检查表，在通用的即为生成原始数据的五张表
#sh auto_cretb.sh cre_check_table 
#导入辅助表数据，参数是当前的版本号
#sh load_data.sh load_data_ff 3
#更新或者加载本地conf目录下的文件
#sh load_data.sh load_localfile 3
#给辅助表添加分区，分区为版本号
#sh auto_cretb.sh add_par 1
#为数据检测表添加分区
#sh auto_cretb.sh add_par_check ${date}
#存放原始数据的本地路径，通用不需要
#sh auto_mr.sh  mkdir_local ${date}
#向集群上传原始数据，通用不需要
#sh auto_mr.sh put_raw ${date}

#format_delimiter
#导入辅助表数据，参数是当前的版本号
#sh load_data.sh load_data_ff 3
#更新或者加载本地conf目录下的文件
#sh load_data.sh load_localfile 3
#分隔符转换
#sh auto_mr.sh format_delimiter ${date} >${log_path}/format_delimiter_part_${date}.log 2>&1
#用于检测新增零件
#sh auto_check.sh part_check 
#数据去重
#sh auto_mr.sh uniq_cus ${date} >${log_path}/format_delimiter_cus_${date}.log 2>&1
#sh auto_mr.sh uniq_doss  ${date} >${log_path}/format_delimiter_dosses_${date}.log 2>&1
#sh auto_mr.sh uniq_order ${date} >${log_path}/format_delimiter_ord_${date}.log 2>&1
#sh auto_mr.sh uniq_claim ${date} >${log_path}/format_delimiter_claims_${date}.log 2>&1
#打标签
#sh auto_mr.sh delete_useless ${date} >${log_path}/delete_useless_${date}.log 2>&1
#sh auto_mr.sh label_fm ${date} >${log_path}/label_fm_${date}.log 2>&1
#sh auto_mr.sh label_fix ${date} >${log_path}/label_fix_${date}.log 2>&1
#sh auto_mr.sh label_other ${date} >${log_path}/label_other_${date}.log 2>&1
#sh auto_mr.sh label_doss ${date} >${log_path}/label_doss_${date}.log 2>&1
#sh auto_mr.sh add_partition ${date}
#用于检测新增品牌车型车系
#sh auto_check.sh part_check 
#计算流水表
#sh flow.sh
#sexual,province,city,name,primary_classification,second_level_classification,distributor
#sh auto_check.sh flow_dim

#source /etc/profile
#startTime=`date +"%s.%N"`
#spark-submit --master yarn --driver-memory 3g --executor-memory 5g --num-executors 30 --executor-cores 1 /home/${user_name}/general/bin/zjxj.py 2017-04 2017-07 ${user_name} time_record_zjxj >../log/zjxj_test 2>&1
#spark-submit --master yarn --driver-memory 3g --executor-memory 5g --num-executors 30 --executor-cores 1 /home/${user_name}/general/bin/zhanbi.py 2017-04 2017-07 ${user_name} time_record_zhanbi > ../log/zhanbi_test 2>&1
#全国月度
#spark-submit --master yarn --driver-memory 3g --executor-memory 5g --num-executors 30 --executor-cores 1 /home/${user_name}/general/bin/fjp.py 2010-04 2017-07 ${user_name} whole_country everymonth >../log/fjp_test 2>&1
#spark-submit --master yarn --driver-memory 3g --executor-memory 5g --num-executors 30 --executor-cores 1 /home/${user_name}/general/bin/jpl.py 2010-04 2017-07 ${user_name} whole_country everymonth >../log/jpl_test 2>&1
#单店月度

#endTime=`date +"%s.%N"`
#echo `awk -v x1="$(echo $endTime | cut -d '.' -f 1)" -v x2="$(echo $startTime | cut -d '.' -f 1)" -v y1="$[$(echo $endTime | cut -d '.' -f 2) / 1000]" -v y2="$[$(echo $startTime | cut -d '.' -f 2) /1000]" 'BEGIN{printf "RunTime:%.1f s",(x1-x2)+(y1-y2)/1000000}'` >> ../log/time_log
