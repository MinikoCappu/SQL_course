select
    case when grouping(m.mine_name)=1 then '== ИТОГО ==' else m.mine_name end as mine_name,
    case when grouping(s.shift_name)=1 then '== ПОДИТОГ ==' else s.shift_name end as shift_name,
    sum(fp.tons_mined) as total_tons,
    count(distinct fp.equipment_id) as equipment_cnt
from fact_production fp
join dim_mine m on fp.mine_id = m.mine_id
join dim_shift s on fp.shift_id = s.shift_id
where fp.date_id = 20240115
group by rollup(m.mine_name, s.shift_name)
order by grouping(m.mine_name), m.mine_name, grouping(s.shift_name), s.shift_name;

select
    case when grouping(m.mine_name)=1 then 'ВСЕ ШАХТЫ' else m.mine_name end as mine,
    case when grouping(et.type_name)=1 then 'ВСЕ ТИПЫ' else et.type_name end as type,
    sum(fp.tons_mined) as total_tons,
    avg(fp.tons_mined) as avg_tons,
    grouping(m.mine_name, et.type_name) as grouping_level
from fact_production fp
join dim_mine m on fp.mine_id = m.mine_id
join dim_equipment e on fp.equipment_id = e.equipment_id
join dim_equipment_type et on e.equipment_type_id = et.equipment_type_id
where fp.date_id between 20240101 and 20240331
group by cube(m.mine_name, et.type_name)
order by grouping_level, m.mine_name, et.type_name;

select
    case
        when grouping(m.mine_name)=0 then 'Шахта'
        when grouping(s.shift_name)=0 then 'Смена'
        when grouping(et.type_name)=0 then 'Тип оборудования'
        else 'ИТОГО'
    end as dimension,
    coalesce(m.mine_name, s.shift_name, et.type_name, 'Все') as dimension_value,
    sum(fp.tons_mined) as total_tons,
    sum(fp.trips_count) as total_trips,
    avg(fp.tons_mined/nullif(fp.trips_count,0)) as avg_tons_per_trip
from fact_production fp
join dim_mine m on fp.mine_id = m.mine_id
join dim_shift s on fp.shift_id = s.shift_id
join dim_equipment e on fp.equipment_id = e.equipment_id
join dim_equipment_type et on e.equipment_type_id = et.equipment_type_id
where fp.date_id between 20240101 and 20240131
group by grouping sets (
    (m.mine_name),
    (s.shift_name),
    (et.type_name),
    ()
);

select
    m.mine_name,
    round(avg(case when d.month=1 then f.fe_content end),2) as jan,
    round(avg(case when d.month=2 then f.fe_content end),2) as feb,
    round(avg(case when d.month=3 then f.fe_content end),2) as mar,
    round(avg(case when d.month=4 then f.fe_content end),2) as apr,
    round(avg(case when d.month=5 then f.fe_content end),2) as may,
    round(avg(case when d.month=6 then f.fe_content end),2) as jun,
    round(avg(f.fe_content),2) as avg_all
from fact_ore_quality f
join dim_mine m on f.mine_id = m.mine_id
join dim_date d on f.date_id = d.date_id
where d.year=2024 and d.month<=6
group by m.mine_name

union all

select
    'ИТОГО',
    round(avg(case when d.month=1 then f.fe_content end),2),
    round(avg(case when d.month=2 then f.fe_content end),2),
    round(avg(case when d.month=3 then f.fe_content end),2),
    round(avg(case when d.month=4 then f.fe_content end),2),
    round(avg(case when d.month=5 then f.fe_content end),2),
    round(avg(case when d.month=6 then f.fe_content end),2),
    round(avg(f.fe_content),2)
from fact_ore_quality f
join dim_date d on f.date_id = d.date_id
where d.year=2024 and d.month<=6;

create extension if not exists tablefunc;

select *
from crosstab(
$$
select
    e.equipment_name,
    r.reason_name,
    round(sum(fd.duration_min)/60.0,1)
from fact_equipment_downtime fd
join dim_equipment e on fd.equipment_id=e.equipment_id
join dim_downtime_reason r on fd.reason_id=r.reason_id
where fd.date_id between 20240101 and 20240331
group by e.equipment_name, r.reason_name
order by 1,2
$$,
$$
select reason_name
from dim_downtime_reason dr
join fact_equipment_downtime fd on dr.reason_id=fd.reason_id
where fd.date_id between 20240101 and 20240331
group by reason_name
order by sum(fd.duration_min) desc
limit 5
$$
) as ct(
equipment_name text,
r1 numeric,
r2 numeric,
r3 numeric,
r4 numeric,
r5 numeric
);

select
    coalesce(m.mine_name,'== ИТОГО ==') as mine,
    'Добыча (тонн)' as metric,
    sum(case when d.month=1 then fp.tons_mined end) as jan,
    sum(case when d.month=2 then fp.tons_mined end) as feb,
    sum(case when d.month=3 then fp.tons_mined end) as mar,
    sum(fp.tons_mined) as q1_total,
    (sum(case when d.month=2 then fp.tons_mined end) -
     sum(case when d.month=1 then fp.tons_mined end))*100.0 /
     nullif(sum(case when d.month=1 then fp.tons_mined end),0) as feb_vs_jan,
    (sum(case when d.month=3 then fp.tons_mined end) -
     sum(case when d.month=2 then fp.tons_mined end))*100.0 /
     nullif(sum(case when d.month=2 then fp.tons_mined end),0) as mar_vs_feb,
    case
        when abs(
            (sum(case when d.month=3 then fp.tons_mined end) -
             sum(case when d.month=2 then fp.tons_mined end))*100.0 /
             nullif(sum(case when d.month=2 then fp.tons_mined end),0)
        ) < 5 then 'стабильно'
        when sum(case when d.month=3 then fp.tons_mined end) >
             sum(case when d.month=2 then fp.tons_mined end) then 'рост'
        else 'снижение'
    end as trend
from fact_production fp
join dim_mine m on fp.mine_id=m.mine_id
join dim_date d on fp.date_id=d.date_id
where d.year=2024 and d.quarter=1
group by rollup(m.mine_name)

union all

select
    coalesce(m.mine_name,'== ИТОГО =='),
    'Простои (часы)',
    sum(case when d.month=1 then fd.duration_min end)/60.0,
    sum(case when d.month=2 then fd.duration_min end)/60.0,
    sum(case when d.month=3 then fd.duration_min end)/60.0,
    sum(fd.duration_min)/60.0,
    null,
    null,
    null
from fact_equipment_downtime fd
join dim_equipment e on fd.equipment_id=e.equipment_id
join dim_mine m on e.mine_id=m.mine_id
join dim_date d on fd.date_id=d.date_id
where d.year=2024 and d.quarter=1
group by rollup(m.mine_name)
order by mine;