select
   substr(tran_date(s.order_balance_date),1,7) as mon,
   count(distinct case when tasc_code is not null then s.vin else null end)/count(distinct s.vin) as num,
   am.group_name
from
   (select s.*,t.asc_code as tasccode
 from ori.label_order s
   left join (
    select distinct t1.asc_code,t1.order_number,m1.month from ori.part t1 join ori.high_flow_parts x
    join (select month,version from ori.mapping where fact_table='label_order' and dim_table='high_flow_parts') m1
    on t1.part_number=x.part_num and x.version=m1.version
    and substr(tran_date(t1.order_balance_date),1,7)=m1.month
    ) t
    on  s.order_number=t.order_number
    and s.asc_code=t.asc_code
    where substr(tran_date(s.order_balance_date),1,7)>='a'
    and substr(tran_date(s.order_balance_date),1,7)<='b'
    and s.MAINT_TYPE1<>'删除' )s
    join ori.asc_mapping am
    on s.asc_code=am.asc_code
    where  am.version='8areas'
    group by substr(tran_date(s.order_balance_date),1,7),am.group_name;