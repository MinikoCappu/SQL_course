set search_path to public;

create or replace view v_daily_production_summary as
select
    d.full_date,
    m.mine_name,
    s.shift_name,
    count(*) as records_count,
    sum(fp.tons_mined) as total_tons,
    sum(fp.fuel_consumed_l) as total_fuel,
    avg(fp.trips_count) as avg_trips
from fact_production fp
join dim_date d on fp.date_id = d.date_id
join dim_mine m on fp.mine_id = m.mine_id
join dim_shift s on fp.shift_id = s.shift_id
group by d.full_date, m.mine_name, s.shift_name;

SELECT d.full_date,
       m.mine_name,
       sh.shift_name,
       COUNT(*) AS record_count,
       SUM(p.tons_mined) AS total_tons,
       SUM(p.fuel_consumed_l) AS total_fuel,
       ROUND(AVG(p.trips_count)::numeric, 1) AS avg_trips
FROM fact_production p
JOIN dim_date d ON p.date_id = d.date_id
JOIN dim_mine m ON p.mine_id = m.mine_id
JOIN dim_shift sh ON p.shift_id = sh.shift_id
WHERE p.date_id BETWEEN 20240301 AND 20240331
  AND m.mine_name LIKE '%Северная%'
GROUP BY d.full_date, m.mine_name, sh.shift_name
HAVING COUNT(*) > 0
ORDER BY d.full_date, sh.shift_name;

create or replace view v_unplanned_downtime as
select *
from fact_equipment_downtime
where is_planned = false
with check option;

SELECT COUNT(*) AS total_downtime,
       SUM(CASE WHEN is_planned = FALSE THEN 1 ELSE 0 END) AS unplanned_count,
       SUM(CASE WHEN is_planned = TRUE THEN 1 ELSE 0 END) AS planned_count
FROM fact_equipment_downtime;

create materialized view mv_monthly_ore_quality as
select
    m.mine_name,
    d.year_month,
    count(*) as samples_count,
    round(avg(f.fe_content), 2) as avg_fe,
    min(f.fe_content) as min_fe,
    max(f.fe_content) as max_fe,
    avg(f.sio2_content) as avg_sio2,
    avg(f.moisture) as avg_moisture
from fact_ore_quality f
join dim_mine m on f.mine_id = m.mine_id
join dim_date d on f.date_id = d.date_id
group by m.mine_name, d.year_month;

SELECT m.mine_name,
       TO_CHAR(d.full_date, 'YYYY-MM') AS year_month,
       COUNT(*) AS sample_count,
       ROUND(AVG(q.fe_content)::numeric, 2) AS avg_fe,
       ROUND(MIN(q.fe_content)::numeric, 2) AS min_fe,
       ROUND(MAX(q.fe_content)::numeric, 2) AS max_fe,
       ROUND(AVG(q.sio2_content)::numeric, 2) AS avg_sio2
FROM fact_ore_quality q
JOIN dim_mine m ON q.mine_id = m.mine_id
JOIN dim_date d ON q.date_id = d.date_id
GROUP BY m.mine_name, TO_CHAR(d.full_date, 'YYYY-MM')
ORDER BY m.mine_name, year_month;

select *
from (
    select
        s.shift_name,
        o.last_name || ' ' || left(o.first_name,1) || '.' as operator_name,
        sum(fp.tons_mined) as total_tons,
        row_number() over (
            partition by fp.shift_id
            order by sum(fp.tons_mined) desc
        ) as rn
    from fact_production fp
    join dim_operator o on fp.operator_id = o.operator_id
    join dim_shift s on fp.shift_id = s.shift_id
    where fp.date_id between 20240101 and 20240331
    group by fp.shift_id, s.shift_name, o.operator_id, o.last_name, o.first_name
) t
where rn = 1
order by shift_name;

with production_cte as (
    select
        mine_id,
        sum(operating_hours) as work_hours,
        sum(tons_mined) as total_tons
    from fact_production
    where date_id between 20240101 and 20240331
    group by mine_id
),
downtime_cte as (
    select
        e.mine_id,
        sum(fd.duration_min)/60.0 as downtime_hours
    from fact_equipment_downtime fd
    join dim_equipment e on fd.equipment_id = e.equipment_id
    where fd.date_id between 20240101 and 20240331
    group by e.mine_id
)
select
    m.mine_name,
    p.work_hours,
    d.downtime_hours,
    p.total_tons,
    round(
        p.work_hours / nullif(p.work_hours + d.downtime_hours,0) * 100, 1
    ) as availability_pct
from production_cte p
join downtime_cte d on p.mine_id = d.mine_id
join dim_mine m on p.mine_id = m.mine_id
order by availability_pct asc;

create or replace function fn_equipment_downtime_report(
    p_equipment_id int,
    p_date_from int,
    p_date_to int
)
returns table (
    full_date date,
    reason_name text,
    category text,
    duration_min int,
    duration_hours numeric,
    is_planned boolean,
    comment text
)
language sql
as $$
select
    d.full_date,
    r.reason_name,
    r.category,
    fd.duration_min,
    round(fd.duration_min/60.0,1),
    fd.is_planned,
    fd.comment
from fact_equipment_downtime fd
join dim_date d on fd.date_id = d.date_id
join dim_downtime_reason r on fd.reason_id = r.reason_id
where fd.equipment_id = p_equipment_id
and fd.date_id between p_date_from and p_date_to;
$$;

SELECT d.full_date,
       r.reason_name,
       r.category,
       fd.duration_min,
       ROUND(fd.duration_min / 60.0, 1) AS duration_hours,
       fd.is_planned,
       fd.comment
FROM fact_equipment_downtime fd
JOIN dim_date d ON fd.date_id = d.date_id
JOIN dim_downtime_reason r ON fd.reason_id = r.reason_id
WHERE fd.equipment_id = 1
  AND fd.date_id BETWEEN 20240101 AND 20240131
ORDER BY d.full_date;

with recursive loc_tree as (
    select
        location_id,
        parent_id,
        location_name,
        location_type,
        location_name::text as path,
        1 as level
    from dim_location_hierarchy
    where parent_id is null

    union all

    select
        l.location_id,
        l.parent_id,
        l.location_name,
        l.location_type,
        lt.path || ' → ' || l.location_name,
        lt.level + 1
    from dim_location_hierarchy l
    join loc_tree lt on l.parent_id = lt.location_id
)
select
    repeat(' ', level*2) || location_name as tree,
    location_type,
    path,
    level
from loc_tree
order by path;

with recursive dates as (
    select 20240201 as date_id
    union all
    select date_id + 1
    from dates
    where date_id < 20240229
)
select
    d.full_date,
    d.day_of_week_name,
    case when d.is_weekend then 'Выходной' else 'Рабочий' end as day_type
from dates dt
join dim_date d on dt.date_id = d.date_id
left join fact_production fp
    on fp.date_id = d.date_id and fp.mine_id = 1
where fp.date_id is null
and d.is_weekend = false;

with daily as (
    select
        date_id,
        sum(tons_mined) as daily_tons
    from fact_production
    where mine_id = 1
    and date_id between 20240101 and 20240331
    group by date_id
)
select
    dd.full_date,
    daily_tons,
    avg(daily_tons) over (
        order by d.date_id
        rows between 6 preceding and current row
    ) as ma7,
    max(daily_tons) over (
        order by d.date_id
        rows between 6 preceding and current row
    ) as max7,
    round(
        (daily_tons - avg(daily_tons) over (ORDER BY d.date_id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)) /
        nullif(avg(daily_tons) over (ORDER BY d.date_id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),0) * 100, 1
    ) as deviation_pct
from daily d
join dim_date dd on d.date_id = dd.date_id;

create or replace view v_ore_quality_detail as
select
    f.*,
    m.mine_name,
    s.shift_name,
    g.grade_name,
    case
        when fe_content >= 65 then 'Богатая'
        when fe_content >= 55 then 'Средняя'
        else 'Бедная'
    end as quality_category
from fact_ore_quality f
join dim_mine m on f.mine_id = m.mine_id
join dim_shift s on f.shift_id = s.shift_id
join dim_ore_grade g on f.ore_grade_id = g.ore_grade_id;

create or replace function fn_ore_quality_stats(
    p_mine_id int,
    p_year int,
    p_month int
)
returns table (
    samples int,
    avg_fe numeric,
    stddev_fe numeric,
    good_pct numeric
)
language sql
as $$
select
    count(*),
    avg(fe_content),
    stddev(fe_content),
    sum(case when fe_content >= 55 then 1 else 0 end)*100.0/count(*)
from fact_ore_quality f
join dim_date d on f.date_id = d.date_id
where f.mine_id = p_mine_id
and d.year = p_year
and d.month = p_month;
$$;

SELECT m.mine_name, stats.*
FROM dim_mine m
CROSS JOIN LATERAL fn_ore_quality_stats(m.mine_id, 2024, 3) stats
WHERE m.status = 'active';
