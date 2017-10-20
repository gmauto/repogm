insert overwrite table hebingbiao partition(region)
select h1.a_level,h1.b_level,h1.age_type,h1.kpi,h1.kpi_name,h1.mon,
h1.own_num,
h2.quar_own_num,
h3.year_own_num,
h4.base_num,
h5.quar_base_own_num,
h6.year_base_num,
h1.asc_code,
h1.tier,
h1.region
from own_month h1

------------------------------------------季度
 join own_quarter h2 on h1.kpi=h2.kpi
and h1.a_level=h2.a_level
and h1.b_level=h2.b_level
and h1.age_type=h2.age_type
and h1.mon=h2.mon
and h1.asc_code=h2.asc_code
and h1.region=h2.region
and h1.tier=h2.tier

---------------------------年度
join own_year h3 on h1.kpi=h2.kpi
and h1.a_level=h3.a_level
and h1.b_level=h3.b_level
and h1.age_type=h3.age_type
and h1.mon=h3.mon
and h1.asc_code=h3.asc_code
and h1.region=h3.region
and h1.tier=h3.tier


-------------标准值月
 join base_month h4
on h1.kpi=h4.kpi
and h1.a_level=h4.a_level
and h1.b_level=h4.b_level
and h1.age_type=h4.age_type
and h1.mon=h4.mon
and h1.region=h4.region
and h1.tier=h4.tier

------------------------------------------标准值季度
join base_quarter h5 on h1.kpi=h5.kpi
and h1.a_level=h5.a_level
and h1.b_level=h5.b_level
and h1.age_type=h5.age_type
and h1.mon=h5.mon
and h1.region=h5.region
and h1.tier=h5.tier

---------------------------标准值年度
join base_year h6 on h1.kpi=h6.kpi
and h1.a_level=h6.a_level
and h1.b_level=h6.b_level
and h1.age_type=h6.age_type
and h1.mon=h6.mon
and h1.region=h6.region
and h1.tier=h6.tier
where h1.tier is not null
