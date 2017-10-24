----表 t4 生成逻辑
(select regexp_replace(t1.vin,' ','') as vin,
t1.repair_part_id,
t1.asc_code,
t1.storage_position,
t1.part_no,
t1.part_name,
t1.part_cost_price,
t1.part_cost_amount,
t1.part_sale_price,
t1.part_sale_amount,
t1.sender,
t1.receiver,
t1.charge_mode,
t1.ro_no,
t1.balance_no,
t1.last_balance_no,
t1.sgm_vin_tag,
t1.valid,
t2.ch_code,
t2.asc
from ori.tt_asc_bo_repair_part t1 join ori.dealer_info t2
on t1.asc_code=t2.asc_code and length(t1.balance_no)=11
where length(t1.balance_no)=11 and t1.VALID=1 and t1.SGM_VIN_TAG='Y') t4



---- t6
select
t4.asc_code,
t4.storage_position,
t4.part_no,
t4.part_name,
t4.part_cost_price,
t4.part_cost_amount,
t4.part_sale_price,
t4.sender,
t4.receiver,
t4.charge_mode,
t4.repair_part_id,
t4.last_balance_no,
t5.order_bill_date,
t5.order_balance_date,
t5.order_clear_date,
t4.ro_no,
t4.balance_no,
t5.receptionist,
t5.order_desc,
t5.maint_type,
t5.maint_desc,
t5.lnumber,
t4.vin,
t5.makes,
t5.series,
t5.model,
t4.part_sale_amount
from t4 join raw_order t5
on t4.asc_code=t5.asc_code and t4.ro_no=t5.ORDER_NUMBER and t4.balance_no=t5.ORDER_BALANCE_NUMBER
where LENGTH(vin)>5 and vin not in('00000000','11111111','22222222');

----根据 asc_code vin 结算单号 配件代码 结算日期去重，保留计算日期最大一个记录，销售金额求和，改名为销售总金额
----销售金额求和  t61
select asc_code,vin,balance_no,part_no,sum(part_sale_amount) as SALES_AMOUNT
from t6
group by asc_code,vin,balance_no,part_no;


----結算日期最大的一条记录 t62
select
asc_code,
storage_position,
part_no,
part_name,
part_cost_price,
part_cost_amount,
part_sale_price,
sender,
receiver,
charge_mode,
repair_part_id,
last_balance_no,
order_bill_date,
order_balance_date,
order_clear_date,
ro_no,
balance_no,
receptionist,
order_desc,
maint_type,
maint_desc,
lnumber,
vin,
makes,
series,
model,
part_sale_amount
from (select *,rank() over(partition by asc_code,vin,balance_no,part_no order by order_balance_date desc) as rank
from t6) tt
where tt.rank=1;


----t7 生成邏輯 asc_code,vin,balance_no,part_no
select
t62.asc_code,
t62.storage_position,
t62.part_no,
t62.part_name,
t62.part_cost_price,
t62.part_cost_amount,
t62.part_sale_price,
t62.sender,
t62.receiver,
t62.charge_mode,
t62.repair_part_id,
t62.last_balance_no,
t62.order_bill_date,
t62.order_balance_date,
t62.order_clear_date,
t62.ro_no,
t62.balance_no,
t62.receptionist,
t62.order_desc,
t62.maint_type,
t62.maint_desc,
t62.lnumber,
t62.vin,
t62.makes,
t62.series,
t62.model,
t61.SALES_AMOUNT
from t61 join t62
on t61.asc_code=t62.asc_code and t61.vin=t62.vin and t61.balance_no=t62.balance_no and t61.part_no=t62.part_no;