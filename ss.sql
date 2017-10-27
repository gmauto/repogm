select t1.*,t2.part_number
from (select t1.part_number,t1.order_number
from ori.part t1 join ori.filter t2
on t1.part_number=t2.part_num) t2 join
(select *
from ori.label_order ) t1
on t1.order_number=t1.order_number
where maint_type1<>'删除' and substr(tran_date(order_balance_date),1,7)>='2017-04'
and substr(tran_date(order_balance_date),1,7)<='2017-07';

select t1.* ,t3.part_num,t3.name_chinese
from ori.label_order t1 join  ori.part t2 join ori.filter t3
on t1.order_number=t2.order_number and t2.part_number=t3.part_num
where t1.maint_type1<>'删除' and t1.substr(tran_date(order_balance_date),1,7)>='2017-04'
and t1.substr(tran_date(order_balance_date),1,7)<='2017-07'


nohup spark-sql -e "
use ori;
add jar /home/ipsos_test1/general/bin/transform-date.jar;
CREATE TEMPORARY FUNCTION mon_diff AS 'com.gm.transformDate.MonDiff';
CREATE TEMPORARY FUNCTION tran_date AS 'com.gm.transformDate.TransformDate';
set hive.exec.dynamic.partition.mode=nonstrict;
set spark.sql.crossJoin.enabled=true;
select distinct t2.part_number,t2.part_desc
from (select *
from ori.label_order
where maint_type1<>'删除' and substr(tran_date(order_balance_date),1,7)>='2017-04' and substr(tran_date(order_balance_date),1,7)<='2017-07') t1 join part t2
on t1.order_number=t2.order_number;
" > part 2>logg &




select t1.* from
   ori.part t1 join (select distinct part_num from ori.filter where version=2) t2 on t1.part_number=t2.part_num
