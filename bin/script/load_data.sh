#!/bin/bash

database=ipsos_test4
username=ipsos_test4
local_dim_files=/home/${username}/general/data/dim
local_file=/home/${username}/general/bin/conf
function load_data_ff(){
if [ $# -lt 1 ]
then
 echo "需要一个参数，version"
 exit -1
else
 version=$1
fi
hive -e "
use ${database};
load data local inpath '${local_dim_files}/${version}/maintnance/*' overwrite into table maintnance partition(version=${version});
load data local inpath '${local_dim_files}/${version}/enclosure/*' overwrite into table enclosure partition(version=${version});
load data local inpath '${local_dim_files}/${version}/engine_oil/*' overwrite into table engine_oil partition(version=${version});
load data local inpath '${local_dim_files}/${version}/distributor/*' overwrite into table distributor partition(version=${version});
load data local inpath '${local_dim_files}/${version}/filter/*' overwrite into table filter partition(version=${version});
load data local inpath '${local_dim_files}/${version}/doss_asc/*' overwrite into table doss_asc partition(version=${version});
load data local inpath '${local_dim_files}/${version}/high_flow_parts/*' overwrite into table high_flow_parts partition(version=${version});
load data local inpath '${local_dim_files}/${version}/sexual/*' overwrite into table sexual;
load data local inpath '${local_dim_files}/${version}/city/*' overwrite into table city;
load data local inpath '${local_dim_files}/${version}/asc_mapping/*' overwrite into table asc_mapping;
load data local inpath '${local_dim_files}/${version}/name/*' overwrite into table name;
load data local inpath '${local_dim_files}/${version}/primary_classification/*' overwrite into table primary_classification;
load data local inpath '${local_dim_files}/${version}/second_level_classification/*' overwrite into table second_level_classification;
load data local inpath '${local_dim_files}/${version}/province/*' overwrite into table province;
load data local inpath '${local_dim_files}/${version}/mapping/*' overwrite into table mapping;
load data local inpath '${local_dim_files}/mon_mapping/*' overwrite into table mon_mapping;
load data local inpath '${local_dim_files}/${version}/maint_type1/*' overwrite into table maint_type1;
load data local inpath '${local_dim_files}/${version}/type2/*' overwrite into table type2;
load data local inpath '${local_dim_files}/${version}/distributor/*' overwrite into table dealer_info;
"
}

function load_localfile(){
version=$1
cat ${local_dim_files}/${version}/CHE/* >> ${local_file}/CHE
cat ${local_dim_files}/${version}/doss_asc/* >> ${local_file}/doss_asc
cat ${local_dim_files}/${version}/local_filter/* >> ${local_file}/engineoil
cat ${local_dim_files}/${version}/local_engineoil/* >> ${local_file}/filter
cat ${local_dim_files}/${version}/local_mark/* >> ${local_file}/mark
cat ${local_dim_files}/${version}/local_mark_doss/* >> ${local_file}/mark_doss
}

$1 $2
