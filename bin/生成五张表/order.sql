-----生成 t3
select
t1.asc_code,
t1.balance_no,
t1.balance_time,
t1.brand,
t1.claim_no,
t1.complete_time,
t1.deliverer,
t1.deliverer_ddd_code,
t1.deliverer_gender,
t1.deliverer_mobile,
t1.deliverer_phone,
substr(t1.delivery_date,1,10) as end_dt,
t1.end_time_supposed,
t1.engine_no,
t1.finish_user,
t1.is_pre_sale,
t1.is_red,
t1.last_balance_no,
t1.license,
t1.model,
t1.out_mileage,
t1.owner_name,
t1.owner_no,
t1.owner_property,
t1.receive_amount,
t1.repair_amount,
t1.repair_type,
t1.repair_type_desc,
t1.ro_id,
t1.ro_no,
t1.ro_type,
t1.ro_type_desc,
t1.series,
t1.service_advisor,
t1.sgm_vin_tag,
t1.square_date,
substr(t1.start_time,1,10) as start_dt,
t1.test_driver,
t1.total_amount,
t1.valid,
t1.vin,
t2.ch_code,
t2.asc
from tt_asc_repair_order t1 join dealer_info t2
on t1.asc_code=t2.asc_code and length(t1.balance_no)=11 and ${start_date}<=substr(t1.start_time,1,10) and substr(t1.start_time,1,10)<=${end_date};


-----t4 表生成邏輯

select
asc_code,
balance_no,
balance_time,
brand,
claim_no,
complete_time,
deliverer,
deliverer_ddd_code,
deliverer_gender,
deliverer_mobile,
deliverer_phone,
case when end_dt is null then start_dt ELSE end_dt end as end_dt,
end_time_supposed,
engine_no,
finish_user,
is_pre_sale,
is_red,
last_balance_no,
license,
model,
out_mileage,
owner_name,
owner_no,
owner_property,
receive_amount,
repair_amount,
repair_type,
repair_type_desc,
ro_id,
ro_no,
ro_type,
ro_type_desc,
series,
service_advisor,
sgm_vin_tag,
square_date,
case when start_dt is null then end_dt else start_dt end as start_dt,
test_driver,
total_amount,
valid,
vin,
ch_code,
asc
from (select * from t3
where (start_dt is not null or end_dt is not null) and (length(balance_no)=11 and is_pre_sale <>1) ) t31

-----t5生成邏輯
根据asc_code balance_no deliverer_gender去重,相同的asc_code balance_no取最deliverer_gender最大的一个
select
asc_code,
balance_no,
balance_time,
brand,
claim_no,
complete_time,
deliverer,
deliverer_ddd_code,
deliverer_gender,
deliverer_mobile,
deliverer_phone,
end_dt,
end_time_supposed,
engine_no,
finish_user,
is_pre_sale,
is_red,
last_balance_no,
license,
model,
out_mileage,
owner_name,
owner_no,
owner_property,
receive_amount,
repair_amount,
repair_type,
repair_type_desc,
ro_id,
ro_no,
ro_type,
ro_type_desc,
series,
service_advisor,
sgm_vin_tag,
square_date,
start_dt,
test_driver,
total_amount,
valid,
vin,
ch_code,
asc
from
(select *,rank() over(partition by asc_code,balance_no order by deliverer_gender desc) as rank
from t4) t51
where t51.rank=1;


-----t6
select
t5.ro_id,
t5.start_dt,
t5.ro_no,
t5.balance_no,
t5.service_advisor,
t5.license,
t5.brand,
t5.series,
t5.model,
t5.repair_amount,
t5.finish_user,
t5.out_mileage,
t5.owner_name,
t5.deliverer,
t5.deliverer_gender,
t5.deliverer_ddd_code,
t5.deliverer_phone,
t5.deliverer_mobile,
t5.end_time_supposed,
t5.complete_time,
t5.end_dt,
t5.owner_no,
t5.owner_property,
t5.claim_no,
t5.test_driver,
t5.vin,
t5.asc_code,
t3.balance_time,
t3.square_date,
t3.total_amount,
t3.receive_amount,
t3.repair_type_desc,
t3.ro_type_desc,
t3.ro_type,
t3.repair_type,
t3.VALID,
t3.SGM_VIN_TAG,
t3.is_red,
t3.last_balance_no
from t5 join t3
on t5.asc_code=t3.asc_code
and t5.balance_no=t3.balance_no
and t5.vin=t3.vin
and t5.ro_no=t3.ro_no
and t5.start_dt<=substr(t3.balance_time,1,10)
and t5.VALID =1;


-----t7
select *,case when is_red = 1 or regexp_replace(last_balance_no,' ','') then 0 else 1 as if_acc
from t6
WHERE  length(balance_no)=11 and VALID=1 and SGM_VIN_TAG='Y';

---t8
判断是否为事故车
if (is_red = 1) or (compress()^='') then if_acc = 0;
 else if_acc = 1;
按照vin asc_code ro_no balance_time去重，相同的vin asc_code ro_no取balance_time最大的一个记录，并且if_acc=1，

select
ro_id,
valid,
start_dt,
ro_no,
balance_no,
service_advisor,
license,
brand,
series,
model,
repair_amount,
finish_user,
out_mileage,
owner_name,
deliverer,
deliverer_gender,
deliverer_ddd_code,
deliverer_phone,
deliverer_mobile,
end_time_supposed,
complete_time,
end_dt,
owner_no,
owner_property,
claim_no,
test_driver,
vin,
asc_code,
balance_time,
square_date,
total_amount,
receive_amount,
repair_type_desc,
ro_type_desc,
ro_type,
repair_type
from
(select *,rank() over(partition by asc_code,vin,ro_no, order by balance_time desc) as rank
from t7
where if_acc=1) t81
where rank=1;