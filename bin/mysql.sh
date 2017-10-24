#!/bin/bash
recordFile=../log/time_record
function region_8(){
#开始时间
 date1=$(date  +"%Y-%m-%d %H:%M:%S")
 time1=$(date +"%s")
mysql -uipsos -pIpsos@123 -e "
use ipsos;
ALTER TABLE region_8 DELAY_KEY_WRITE=1;
load data local infile '/home/ipsos/general/data/133/baqu/baqu' into table region_8 character set utf8 fields terminated by '\t'  lines terminated by '\n' (a_level,b_level,age_type,kpi,kpi_name,mon,asc_code,own_num,quar_own_num,year_own_num,base_num,quar_base_num,year_base_num);
quit"> ../log/$FUNCNAME 2>&1
  echo "$FUNCNAME  $?" >> ../log/res_test
#结束时间
time2=$(date +"%s")
date2=$(date +"%Y-%m-%d %H:%M:%S")

#计算时间
let time=time2-time1
let min_time=time/3600
echo -e "$FUNCNAME\t${date1}\t${date2}\t${time}s" >>${recordFile}
echo "----------------------------" >>${recordFile}
}

function other_region_2(){
#开始时间
 date1=$(date  +"%Y-%m-%d %H:%M:%S")
 time1=$(date +"%s")
mysql -uipsos -pIpsos@123 -e "
use ipsos;
ALTER TABLE other_region_2 DELAY_KEY_WRITE=1;
load data local infile '/home/ipsos/general/data/133/erquqita/erquqita' into table other_region_2 character set utf8 fields terminated by '\t'  lines terminated by '\n' (a_level,b_level,age_type,kpi,kpi_name,mon,asc_code,own_num,quar_own_num,year_own_num,base_num,quar_base_num,year_base_num);
quit"> ../log/$FUNCNAME 2>&1
  echo "$FUNCNAME  $?" >> ../log/res_test
#结束时间
time2=$(date +"%s")
date2=$(date +"%Y-%m-%d %H:%M:%S")

#计算时间
let time=time2-time1
let min_time=time/3600
echo -e "$FUNCNAME\t${date1}\t${date2}\t${time}s" >>${recordFile}
echo "----------------------------" >>${recordFile}
}

function other_region_6(){
#开始时间
 date1=$(date  +"%Y-%m-%d %H:%M:%S")
 time1=$(date +"%s")
mysql -uipsos -pIpsos@123 -e "
use ipsos;

ALTER TABLE other_region_6 DELAY_KEY_WRITE=1;
load data local infile '/home/ipsos/general/data/133/liuquqita/liuquqita' into table other_region_6 character set utf8 fields terminated by '\t'  lines terminated by '\n' (a_level,b_level,age_type,kpi,kpi_name,mon,asc_code,own_num,quar_own_num,year_own_num,base_num,quar_base_num,year_base_num);
quit"> ../log/$FUNCNAME 2>&1
  echo "$FUNCNAME  $?" >> ../log/res_test
#结束时间
time2=$(date +"%s")
date2=$(date +"%Y-%m-%d %H:%M:%S")

#计算时间
let time=time2-time1
let min_time=time/3600
echo -e "$FUNCNAME\t${date1}\t${date2}\t${time}s" >>${recordFile}
echo "----------------------------" >>${recordFile}
}

function shanghai_region_7(){
#开始时间
 date1=$(date  +"%Y-%m-%d %H:%M:%S")
 time1=$(date +"%s")
mysql -uipsos -pIpsos@123 -e "
use ipsos;
ALTER TABLE shanghai_region_7 DELAY_KEY_WRITE=1;
load data local infile '/home/ipsos/general/data/133/qiqushanghai/qiqushanghai' into table shanghai_region_7 character set utf8 fields terminated by '\t'  lines terminated by '\n' (a_level,b_level,age_type,kpi,kpi_name,mon,asc_code,own_num,quar_own_num,year_own_num,base_num,quar_base_num,year_base_num);

quit"> ../log/$FUNCNAME 2>&1
  echo "$FUNCNAME  $?" >> ../log/res_test
#结束时间
time2=$(date +"%s")
date2=$(date +"%Y-%m-%d %H:%M:%S")

#计算时间
let time=time2-time1
let min_time=time/3600
echo -e "$FUNCNAME\t${date1}\t${date2}\t${time}s" >>${recordFile}
echo "----------------------------" >>${recordFile}
}

function guangshen_region_4(){
#开始时间
 date1=$(date  +"%Y-%m-%d %H:%M:%S")
 time1=$(date +"%s")
mysql -uipsos -pIpsos@123 -e "
use ipsos;

ALTER TABLE guangshen_region_4 DELAY_KEY_WRITE=1;
load data local infile '/home/ipsos/general/data/133/siquguangzhoushengzhen/siquguangzhoushengzhen' into table guangshen_region_4 character set utf8 fields terminated by '\t'  lines terminated by '\n' (a_level,b_level,age_type,kpi,kpi_name,mon,asc_code,own_num,quar_own_num,year_own_num,base_num,quar_base_num,year_base_num);
quit"> ../log/$FUNCNAME 2>&1
  echo "$FUNCNAME  $?" >> ../log/res_test
#结束时间
time2=$(date +"%s")
date2=$(date +"%Y-%m-%d %H:%M:%S")

#计算时间
let time=time2-time1
let min_time=time/3600
echo -e "$FUNCNAME\t${date1}\t${date2}\t${time}s" >>${recordFile}
echo "----------------------------" >>${recordFile}
}

function region_5(){
#开始时间
 date1=$(date  +"%Y-%m-%d %H:%M:%S")
 time1=$(date +"%s")
mysql -uipsos -pIpsos@123 -e "
use ipsos;
ALTER TABLE region_5 DELAY_KEY_WRITE=1;
load data local infile '/home/ipsos/general/data/133/wuqu/wuqu' into table region_5 character set utf8 fields terminated by '\t'  lines terminated by '\n' (a_level,b_level,age_type,kpi,kpi_name,mon,asc_code,own_num,quar_own_num,year_own_num,base_num,quar_base_num,year_base_num);

quit"> ../log/$FUNCNAME 2>&1
  echo "$FUNCNAME  $?" >> ../log/res_test
#结束时间
time2=$(date +"%s")
date2=$(date +"%Y-%m-%d %H:%M:%S")

#计算时间
let time=time2-time1
let min_time=time/3600
echo -e "$FUNCNAME\t${date1}\t${date2}\t${time}s" >>${recordFile}
echo "----------------------------" >>${recordFile}
}

function shandong_region_1(){
#开始时间
 date1=$(date  +"%Y-%m-%d %H:%M:%S")
 time1=$(date +"%s")
mysql -uipsos -pIpsos@123 -e "
use ipsos;
ALTER TABLE shandong_region_1 DELAY_KEY_WRITE=1;
load data local infile '/home/ipsos/general/data/133/yiqushandong/yiqushandong' into table shandong_region_1 character set utf8 fields terminated by '\t'  lines terminated by '\n' (a_level,b_level,age_type,kpi,kpi_name,mon,asc_code,own_num,quar_own_num,year_own_num,base_num,quar_base_num,year_base_num);

quit"> ../log/$FUNCNAME 2>&1
  echo "$FUNCNAME  $?" >> ../log/res_test
#结束时间
time2=$(date +"%s")
date2=$(date +"%Y-%m-%d %H:%M:%S")

#计算时间
let time=time2-time1
let min_time=time/3600
echo -e "$FUNCNAME\t${date1}\t${date2}\t${time}s" >>${recordFile}
echo "----------------------------" >>${recordFile}
}

function chengchong_region_6(){
#开始时间
 date1=$(date  +"%Y-%m-%d %H:%M:%S")
 time1=$(date +"%s")
mysql -uipsos -pIpsos@123 -e "
use ipsos;
ALTER TABLE chengchong_region_6 DELAY_KEY_WRITE=1;
load data local infile '/home/ipsos/general/data/133/liuquchengduchongqing/liuquchengduchongqing' into table chengchong_region_6 character set utf8 fields terminated by '\t'  lines terminated by '\n' (a_level,b_level,age_type,kpi,kpi_name,mon,asc_code,own_num,quar_own_num,year_own_num,base_num,quar_base_num,year_base_num);

quit"> ../log/$FUNCNAME 2>&1
  echo "$FUNCNAME  $?" >> ../log/res_test
#结束时间
time2=$(date +"%s")
date2=$(date +"%Y-%m-%d %H:%M:%S")

#计算时间
let time=time2-time1
let min_time=time/3600
echo -e "$FUNCNAME\t${date1}\t${date2}\t${time}s" >>${recordFile}
echo "----------------------------" >>${recordFile}
}

function other_region_7(){
#开始时间
 date1=$(date  +"%Y-%m-%d %H:%M:%S")
 time1=$(date +"%s")
mysql -uipsos -pIpsos@123 -e "
use ipsos;
ALTER TABLE other_region_7 DELAY_KEY_WRITE=1;
load data local infile '/home/ipsos/general/data/133/qiquqita/qiquqita' into table other_region_7 character set utf8 fields terminated by '\t'  lines terminated by '\n' (a_level,b_level,age_type,kpi,kpi_name,mon,asc_code,own_num,quar_own_num,year_own_num,base_num,quar_base_num,year_base_num);

quit"> ../log/$FUNCNAME 2>&1
  echo "$FUNCNAME  $?" >> ../log/res_test
#结束时间
time2=$(date +"%s")
date2=$(date +"%Y-%m-%d %H:%M:%S")

#计算时间
let time=time2-time1
let min_time=time/3600
echo -e "$FUNCNAME\t${date1}\t${date2}\t${time}s" >>${recordFile}
echo "----------------------------" >>${recordFile}
}

function other_region_4(){
#开始时间
 date1=$(date  +"%Y-%m-%d %H:%M:%S")
 time1=$(date +"%s")
mysql -uipsos -pIpsos@123 -e "
use ipsos;
ALTER TABLE other_region_4 DELAY_KEY_WRITE=1;
load data local infile '/home/ipsos/general/data/133/siquqita/siquqita' into table other_region_4 character set utf8 fields terminated by '\t'  lines terminated by '\n' (a_level,b_level,age_type,kpi,kpi_name,mon,asc_code,own_num,quar_own_num,year_own_num,base_num,quar_base_num,year_base_num);

quit"> ../log/$FUNCNAME 2>&1
  echo "$FUNCNAME  $?" >> ../log/res_test
#结束时间
time2=$(date +"%s")
date2=$(date +"%Y-%m-%d %H:%M:%S")

#计算时间
let time=time2-time1
let min_time=time/3600
echo -e "$FUNCNAME\t${date1}\t${date2}\t${time}s" >>${recordFile}
echo "----------------------------" >>${recordFile}
}


function dongbei_region_1(){
#开始时间
 date1=$(date  +"%Y-%m-%d %H:%M:%S")
 time1=$(date +"%s")
mysql -uipsos -pIpsos@123 -e "
use ipsos;
ALTER TABLE dongbei_region_1 DELAY_KEY_WRITE=1;
load data local infile '/home/ipsos/general/data/133/yiqudongbei/yiqudongbei' into table dongbei_region_1 character set utf8 fields terminated by '\t'  lines terminated by '\n' (a_level,b_level,age_type,kpi,kpi_name,mon,asc_code,own_num,quar_own_num,year_own_num,base_num,quar_base_num,year_base_num);

quit"> ../log/$FUNCNAME 2>&1
  echo "$FUNCNAME  $?" >> ../log/res_test
#结束时间
time2=$(date +"%s")
date2=$(date +"%Y-%m-%d %H:%M:%S")

#计算时间
let time=time2-time1
let min_time=time/3600
echo -e "$FUNCNAME\t${date1}\t${date2}\t${time}s" >>${recordFile}
echo "----------------------------" >>${recordFile}
}

function region_3(){
#开始时间
 date1=$(date  +"%Y-%m-%d %H:%M:%S")
 time1=$(date +"%s")
mysql -uipsos -pIpsos@123 -e "
use ipsos;

ALTER TABLE region_3 DELAY_KEY_WRITE=1;
load data local infile '/home/ipsos/general/data/133/sanqu/sanqu' into table region_3 character set utf8 fields terminated by '\t'  lines terminated by '\n' (a_level,b_level,age_type,kpi,kpi_name,mon,asc_code,own_num,quar_own_num,year_own_num,base_num,quar_base_num,year_base_num);
quit"> ../log/$FUNCNAME 2>&1
  echo "$FUNCNAME  $?" >> ../log/res_test
#结束时间
time2=$(date +"%s")
date2=$(date +"%Y-%m-%d %H:%M:%S")

#计算时间
let time=time2-time1
let min_time=time/3600
echo -e "$FUNCNAME\t${date1}\t${date2}\t${time}s" >>${recordFile}
echo "----------------------------" >>${recordFile}
}

function beijing_region_2(){
#开始时间
 date1=$(date  +"%Y-%m-%d %H:%M:%S")
 time1=$(date +"%s")
mysql -uipsos -pIpsos@123 -e "
use ipsos;

ALTER TABLE beijing_region_2 DELAY_KEY_WRITE=1;
load data local infile '/home/ipsos/general/data/133/erqubeijing/erqubeijing' into table beijing_region_2 character set utf8 fields terminated by '\t'  lines terminated by '\n' (a_level,b_level,age_type,kpi,kpi_name,mon,asc_code,own_num,quar_own_num,year_own_num,base_num,quar_base_num,year_base_num);
quit"> ../log/$FUNCNAME 2>&1
  echo "$FUNCNAME  $?" >> ../log/res_test
#结束时间
time2=$(date +"%s")
date2=$(date +"%Y-%m-%d %H:%M:%S")

#计算时间
let time=time2-time1
let min_time=time/3600
echo -e "$FUNCNAME\t${date1}\t${date2}\t${time}s" >>${recordFile}
echo "----------------------------" >>${recordFile}
}






function bat1(){
region_8
other_region_2
other_region_6
}

function bat2(){
shanghai_region_7
guangshen_region_4
region_5
}

function bat3(){
shandong_region_1
chengchong_region_6
other_region_7
}

function bat4(){
other_region_4
dongbei_region_1
region_3
beijing_region_2
}

bat1 &
bat2 &
bat3 &
bat4 &