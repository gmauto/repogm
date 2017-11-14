#!/bin/bash

PATH_UNIQ_OUT_ORDER=hdfs://ns1/user/ori/public/label_order/
DELETE_JAR=/home/ori/general/bin/delete.jar
DELETE_MAINCLASS=com.chance.del.DelMain
reduce_num=10
PATH_LABEL_DELETE_ORDER=hdfs://ns1/user/ori/public/delete_counter
mapping=/user/ori/public/mapping
log_path=/home/ori/general/log
#打删除标签
function delete_useless(){
 in_path=${PATH_UNIQ_OUT_ORDER}/*/*
 out_path=${PATH_LABEL_DELETE_ORDER}
 echo ${in_path}
 echo ${out_path}
 hadoop fs -rm -r ${out_path}
 hadoop jar ${DELETE_JAR} ${DELETE_MAINCLASS} -files conf/CHE -Dmapreduce.job.reduces=${reduce_num} ${mapping} ${in_path} ${out_path}
}


#串联打标签过程
function label_all() {

 delete_useless >${log_path}/delete_20171114.log 2>&1
 
}
label_all

