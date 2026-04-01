-- Задание 1 — скалярный подзапрос (фильтрация)
select
    o.last_name || ' ' || left(o.first_name, 1) || '.' as operator_name,
    sum(fp.tons_mined) as total_tons,
    (
        select avg(sub.total_tons)
        from (select sum(tons_mined) as total_tons 
        from fact_production fp2
        where fp2.date_id between 20240301 and 20240331
        group by operator_id) sub
    ) as avg_tons
from fact_production fp
join dim_operator o on fp.operator_id = o.operator_id
where fp.date_id between 20240301 and 20240331
group by o.operator_id, o.last_name, o.first_name
having sum(fp.tons_mined) >
(
    select avg(total_tons)
    from (
        select sum(fp2.tons_mined) as total_tons
        from fact_production fp2
        where fp2.date_id between 20240301 and 20240331
        group by fp2.operator_id
    ) sub
)
order by total_tons desc;

-- Задание 2 — подзапрос с IN
select
    s.sensor_code,
    st.type_name,
    e.equipment_name,
    e.status
from dim_sensor s
join dim_sensor_type st on s.sensor_type_id = st.sensor_type_id
join dim_equipment e on s.equipment_id = e.equipment_id
where s.equipment_id in (
    select distinct equipment_id
    from fact_production
    where date_id between 20240101 and 20240331
)
order by e.equipment_name, s.sensor_code;

-- Задание 3 — NOT IN
select
    e.equipment_name,
    et.type_name,
    m.mine_name,
    e.status
from dim_equipment e
join dim_equipment_type et on e.equipment_type_id = et.equipment_type_id
join dim_mine m on e.mine_id = m.mine_id
where e.equipment_id not in (
    select equipment_id
    from fact_production
    where equipment_id is not null
) order by e.equipment_name;

-- Задание 4 — коррелированный подзапрос
select
    m.mine_name,
    d.full_date,
    e.equipment_name,
    fp.tons_mined,

    round((
        select avg(fp2.tons_mined)
        from fact_production fp2
        where fp2.mine_id = fp.mine_id
        and fp2.date_id between 20240101 and 20240331
    ))::numeric as avg_mine

from fact_production fp
join dim_mine m on fp.mine_id = m.mine_id
join dim_date d on fp.date_id = d.date_id
join dim_equipment e on fp.equipment_id = e.equipment_id

where fp.date_id between 20240101 and 20240331
and fp.tons_mined <
(
    select avg(fp2.tons_mined)
    from fact_production fp2
    where fp2.mine_id = fp.mine_id
)
order by (fp.tons_mined - (
	select avg(fp2.tons_mined)
	from fact_production fp2
	where fp2.mine_id = fp.mine_id
	and fp2.date_id between 20240101 and 20240331)) asc
limit 15;


-- Задание 5 — EXISTS (тревожные показатели)
select
    e.equipment_name,
    et.type_name,
    m.mine_name,

    (
        select count(*)
        from fact_equipment_telemetry ft
        where ft.equipment_id = e.equipment_id
        and ft.is_alarm = true
        and ft.date_id between 20240301 and 20240331
    ) as alarm_count

from dim_equipment e
join dim_equipment_type et on e.equipment_type_id = et.equipment_type_id
join dim_mine m on e.mine_id = m.mine_id

where exists (
    select 1
    from fact_equipment_telemetry ft
    where ft.equipment_id = e.equipment_id
    and ft.is_alarm = true
    and ft.date_id between 20240301 and 20240331
)
order by alarm_count desc;

-- Задание 6 — NOT EXISTS (дни без работы оборудования)
select
    d.full_date,
    d.day_of_week_name,
    d.is_weekend
from dim_date d
where d.date_id between 20240301 and 20240331
and not exists (
    select 1
    from fact_production fp
    where fp.date_id = d.date_id
    and fp.equipment_id = 1
)
order by d.full_date;

-- Задание 7 — ALL
select
    e.equipment_name,
    et.type_name,
    d.full_date,
    fp.shift_id,
    fp.tons_mined
from fact_production fp
join dim_equipment e on fp.equipment_id = e.equipment_id
join dim_equipment_type et on e.equipment_type_id = et.equipment_type_id
join dim_date d on fp.date_id = d.date_id

where fp.tons_mined > all (
    select fp2.tons_mined
    from fact_production fp2
    join dim_equipment e2 on fp2.equipment_id = e2.equipment_id
    join dim_equipment_type et2 on e2.equipment_type_id = et2.equipment_type_id
    where et2.type_code = 'TRUCK'
);