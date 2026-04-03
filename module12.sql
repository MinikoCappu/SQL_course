select
    'Добыча' as event_type,
    e.equipment_name,
    fp.tons_mined as value,
    'тонн' as unit
from fact_production fp
join dim_equipment e on fp.equipment_id = e.equipment_id
where fp.date_id = 20240315

union all

select
    'Простой' as event_type,
    e.equipment_name,
    fd.duration_min as value,
    'мин.' as unit
from fact_equipment_downtime fd
join dim_equipment e on fd.equipment_id = e.equipment_id
where fd.date_id = 20240315

order by equipment_name, event_type;

select mine_name
from (
    select m.mine_name
    from fact_production fp
    join dim_mine m on fp.mine_id = m.mine_id
    where fp.date_id between 20240101 and 20240331

    union

    select m.mine_name
    from fact_equipment_downtime fd
    join dim_equipment e on fd.equipment_id = e.equipment_id
    join dim_mine m on e.mine_id = m.mine_id
    where fd.date_id between 20240101 and 20240331
) t;

select e.equipment_name, et.type_name
from dim_equipment e
join dim_equipment_type et on e.equipment_type_id = et.equipment_type_id
where e.equipment_id in (

    select equipment_id
    from fact_production
    where date_id between 20240101 and 20240331

    except

    select distinct fp.equipment_id
    from fact_production fp
    join fact_ore_quality foq
        on fp.date_id = foq.date_id
       and fp.mine_id = foq.mine_id
       and fp.shaft_id = foq.shaft_id
);
/*
select e.equipment_name, et.type_name
from dim_equipment e
join dim_equipment_type et on e.equipment_type_id = et.equipment_type_id
where exists (
    select 1
    from fact_production fp
    where fp.equipment_id = e.equipment_id
    and fp.date_id between 20240101 and 20240331
)
and not exists (
    select 1
    from fact_production fp
    join fact_ore_quality foq
        on fp.date_id = foq.date_id
       and fp.mine_id = foq.mine_id
       and fp.shaft_id = foq.shaft_id
    where fp.equipment_id = e.equipment_id
);*/

select
    o.last_name,
    o.first_name,
    o.position,
    o.qualification
from dim_operator o
where o.operator_id in (

    select fp.operator_id
    from fact_production fp
    join dim_equipment e on fp.equipment_id = e.equipment_id
    join dim_equipment_type et on e.equipment_type_id = et.equipment_type_id
    where et.type_code = 'LHD'

    intersect

    select fp.operator_id
    from fact_production fp
    join dim_equipment e on fp.equipment_id = e.equipment_id
    join dim_equipment_type et on e.equipment_type_id = et.equipment_type_id
    where et.type_code = 'TRUCK'
);

WITH lhd_operators AS (
    SELECT DISTINCT fp.operator_id
    FROM fact_production fp
    JOIN dim_equipment e ON fp.equipment_id = e.equipment_id
    JOIN dim_equipment_type et ON e.equipment_type_id = et.equipment_type_id
    WHERE et.type_code = 'LHD'
),
truck_operators AS (
    SELECT DISTINCT fp.operator_id
    FROM fact_production fp
    JOIN dim_equipment e ON fp.equipment_id = e.equipment_id
    JOIN dim_equipment_type et ON e.equipment_type_id = et.equipment_type_id
    WHERE et.type_code = 'TRUCK'
),
both_types AS (
    SELECT operator_id FROM lhd_operators
    INTERSECT
    SELECT operator_id FROM truck_operators
),
only_lhd AS (
    SELECT operator_id FROM lhd_operators
    EXCEPT
    SELECT operator_id FROM truck_operators
),
only_truck AS (
    SELECT operator_id FROM truck_operators
    EXCEPT
    SELECT operator_id FROM lhd_operators
),
total AS (
    SELECT COUNT(DISTINCT operator_id) AS cnt FROM fact_production
)
SELECT 'Оба типа' AS category,
       (SELECT COUNT(*) FROM both_types) AS count,
       ROUND((SELECT COUNT(*) FROM both_types)::numeric / (SELECT cnt FROM total) * 100, 1) AS pct
UNION ALL
SELECT 'Только ПДМ',
       (SELECT COUNT(*) FROM only_lhd),
       ROUND((SELECT COUNT(*) FROM only_lhd)::numeric / (SELECT cnt FROM total) * 100, 1)
UNION ALL
SELECT 'Только самосвал',
       (SELECT COUNT(*) FROM only_truck),
       ROUND((SELECT COUNT(*) FROM only_truck)::numeric / (SELECT cnt FROM total) * 100, 1);

select m.mine_name, top5.*
from dim_mine m
cross join lateral (
    select
        d.full_date,
        e.equipment_name,
        r.reason_name,
        fd.duration_min,
        round(fd.duration_min/60.0,1) as hours
    from fact_equipment_downtime fd
    join dim_equipment e on fd.equipment_id = e.equipment_id
    join dim_downtime_reason r on fd.reason_id = r.reason_id
    join dim_date d on fd.date_id = d.date_id
    where e.mine_id = m.mine_id
    and fd.is_planned = false
    and fd.date_id between 20240101 and 20240331
    order by fd.duration_min desc
    limit 5
) top5
where m.status = 'active'
order by m.mine_name, top5.duration_min desc;

select
    s.sensor_code,
    st.type_name,
    e.equipment_name,
    t.date_id,
    t.time_id,
    t.sensor_value,
    t.is_alarm
from dim_sensor s
join dim_sensor_type st on s.sensor_type_id = st.sensor_type_id
join dim_equipment e on s.equipment_id = e.equipment_id

left join lateral (
    select
        d.full_date || ' ' || tm.full_time as last_time,
        ft.sensor_value,
        ft.is_alarm,
        ft.date_id,
        ft.time_id
    from fact_equipment_telemetry ft
    join dim_date d on ft.date_id = d.date_id
    join dim_time tm on ft.time_id = tm.time_id
    where ft.sensor_id = s.sensor_id
    order by ft.date_id desc, ft.time_id desc
    limit 1
) t on true

order by t.last_time asc;

with data as (select m.mine_name, 'Добыча (тонн)' as kpi_name, sum(fp.tons_mined) as kpi_value
from fact_production fp
join dim_mine m on fp.mine_id = m.mine_id
where fp.date_id between 20240301 and 20240331
group by m.mine_name

union all

select m.mine_name, 'Простои (часы)', sum(fd.duration_min)/60.0
from fact_equipment_downtime fd
join dim_equipment e on fd.equipment_id = e.equipment_id
join dim_mine m on e.mine_id = m.mine_id
where fd.date_id between 20240301 and 20240331
group by m.mine_name

union all

select m.mine_name, 'Fe (%)', avg(foq.fe_content)
from fact_ore_quality foq
join dim_mine m on foq.mine_id = m.mine_id
where foq.date_id between 20240301 and 20240331
group by m.mine_name

union all

select m.mine_name, 'Тревоги', count(*)
from fact_equipment_telemetry ft
join dim_equipment e on ft.equipment_id = e.equipment_id
join dim_mine m on e.mine_id = m.mine_id
where ft.date_id between 20240301 and 20240331
and ft.is_alarm = true
group by m.mine_name)
select mine_name, kpi_name, kpi_value
from data
order by mine_name;



with data as (select m.mine_name, 'Добыча (тонн)' as kpi_name, sum(fp.tons_mined) as kpi_value
from fact_production fp
join dim_mine m on fp.mine_id = m.mine_id
where fp.date_id between 20240301 and 20240331
group by m.mine_name

union all

select m.mine_name, 'Простои (часы)', sum(fd.duration_min)/60.0
from fact_equipment_downtime fd
join dim_equipment e on fd.equipment_id = e.equipment_id
join dim_mine m on e.mine_id = m.mine_id
where fd.date_id between 20240301 and 20240331
group by m.mine_name

union all

select m.mine_name, 'Fe (%)', avg(foq.fe_content)
from fact_ore_quality foq
join dim_mine m on foq.mine_id = m.mine_id
where foq.date_id between 20240301 and 20240331
group by m.mine_name

union all

select m.mine_name, 'Тревоги', count(*)
from fact_equipment_telemetry ft
join dim_equipment e on ft.equipment_id = e.equipment_id
join dim_mine m on e.mine_id = m.mine_id
where ft.date_id between 20240301 and 20240331
and ft.is_alarm = true
group by m.mine_name)
select
    mine_name,
    max(case when kpi_name = 'Добыча (тонн)' then kpi_value end) as production,
    max(case when kpi_name = 'Простои (часы)' then kpi_value end) as downtime,
    max(case when kpi_name = 'Fe (%)' then kpi_value end) as fe,
    max(case when kpi_name = 'Тревоги' then kpi_value end) as alarms
from data
group by mine_name;