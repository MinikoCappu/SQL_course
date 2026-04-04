select
    e.equipment_name,
    fp.tons_mined,
    sum(fp.tons_mined) over () as total_tons,
    round(fp.tons_mined * 100.0 / sum(fp.tons_mined) over (), 1) as pct
from fact_production fp
join dim_equipment e on fp.equipment_id = e.equipment_id
where fp.date_id = 20240115
  and fp.shift_id = 1
order by fp.tons_mined desc;

with daily as (
    select
        fp.mine_id,
        d.full_date,
        sum(fp.tons_mined) as daily_tons
    from fact_production fp
    join dim_date d on fp.date_id = d.date_id
    where d.year = 2024 and d.month = 1
    group by fp.mine_id, d.full_date
)
select
    mine_id,
    full_date,
    daily_tons,
    sum(daily_tons) over (partition by mine_id order by full_date) as cumulative_tons
from daily
order by mine_id, full_date;

with daily as (
    select
        d.full_date,
        sum(fp.fuel_consumed_l) as fuel
    from fact_production fp
    join dim_date d on fp.date_id = d.date_id
    where fp.mine_id = 1
      and fp.date_id between 20240101 and 20240331
    group by d.full_date
)
select
    full_date,
    fuel,
    round(avg(fuel) over (order by full_date rows between 6 preceding and current row), 2) as ma7,
    round(avg(fuel) over (order by full_date rows between 13 preceding and current row), 2) as ma14
from daily;

with agg as (
    select
        o.operator_id,
        o.last_name || ' ' || left(o.first_name,1) || '.' as operator_name,
        et.type_name,
        sum(fp.tons_mined) as total_tons
    from fact_production fp
    join dim_operator o on fp.operator_id = o.operator_id
    join dim_equipment e on fp.equipment_id = e.equipment_id
    join dim_equipment_type et on e.equipment_type_id = et.equipment_type_id
    where fp.date_id between 20240101 and 20240630
    group by o.operator_id, operator_name, et.type_name
)
select *
from (
    select
        operator_name,
        type_name,
        total_tons,
        rank() over (partition by type_name order by total_tons desc) as rnk,
        dense_rank() over (partition by type_name order by total_tons desc) as drnk,
        ntile(4) over (partition by type_name order by total_tons desc) as quartile
    from agg
) t
where rnk <= 5
order by type_name, rnk;

with daily as (
    select
        d.full_date,
        fp.shift_id,
        sum(fp.tons_mined) as tons
    from fact_production fp
    join dim_date d on fp.date_id = d.date_id
    where fp.mine_id = 1
      and d.year = 2024 and d.month = 1
    group by d.full_date, fp.shift_id
)
select
    full_date,
    shift_id,
    tons,
    lag(tons) over w_seq as prev_shift,
    tons * 100.0 / sum(tons) over (partition by full_date) as pct_day,
    avg(tons) over w7 as ma7
from daily
window
    w_seq as (partition by shift_id order by full_date),
    w7 as (partition by shift_id order by full_date rows between 6 preceding and current row)
order by full_date, shift_id;

select
    e.equipment_name,
    d.full_date,
    r.reason_name,
    fd.duration_min,
    lag(d.full_date) over (partition by fd.equipment_id order by d.full_date) as prev_date,
    d.full_date - lag(d.full_date) over (partition by fd.equipment_id order by d.full_date) as days_between,
    lead(d.full_date) over (partition by fd.equipment_id order by d.full_date) as next_date,
    avg(d.full_date - lag(d.full_date) over (partition by fd.equipment_id order by d.full_date))
        over (partition by fd.equipment_id) as avg_days
from fact_equipment_downtime fd
join dim_equipment e on fd.equipment_id = e.equipment_id
join dim_date d on fd.date_id = d.date_id
join dim_downtime_reason r on fd.reason_id = r.reason_id
where fd.is_planned = false
order by e.equipment_name, d.full_date;

with stats as (
    select
        mine_id,
        percentile_cont(0.25) within group (order by fe_content) as q1,
        percentile_cont(0.75) within group (order by fe_content) as q3
    from fact_ore_quality
    where date_id between 20240101 and 20240630
    group by mine_id
)
select
    m.mine_name,
    d.full_date,
    f.sample_number,
    f.fe_content,
    case
        when f.fe_content < q1 - 1.5*(q3-q1) then 'Выброс (низ)'
        when f.fe_content > q3 + 1.5*(q3-q1) then 'Выброс (верх)'
    end as status
from fact_ore_quality f
join stats s on f.mine_id = s.mine_id
join dim_mine m on f.mine_id = m.mine_id
join dim_date d on f.date_id = d.date_id
where f.date_id between 20240101 and 20240630
and (
    f.fe_content < q1 - 1.5*(q3-q1)
    or f.fe_content > q3 + 1.5*(q3-q1)
);

with daily as (
    select
        fp.equipment_id,
        d.full_date,
        sum(fp.tons_mined) as tons
    from fact_production fp
    join dim_date d on fp.date_id = d.date_id
    where d.year = 2024
    group by fp.equipment_id, d.full_date
)
select *
from (
    select
        e.equipment_name,
        et.type_name,
        full_date,
        tons,
        row_number() over (partition by equipment_id order by tons desc) as rn,
        max(tons) over (partition by equipment_id) - tons as diff
    from daily d
    join dim_equipment e on d.equipment_id = e.equipment_id
    join dim_equipment_type et on e.equipment_type_id = et.equipment_type_id
) t
where rn <= 3;

with agg as (
    select
        r.reason_name,
        sum(fd.duration_min)/60.0 as hours
    from fact_equipment_downtime fd
    join dim_downtime_reason r on fd.reason_id = r.reason_id
    where fd.date_id between 20240101 and 20240630
    group by r.reason_name
)
select
    reason_name,
    hours,
    hours * 100.0 / sum(hours) over () as pct,
    sum(hours) over (order by hours desc) * 100.0 / sum(hours) over () as cum_pct,
    case
        when sum(hours) over (order by hours desc) * 100.0 / sum(hours) over () <= 80 then 'A'
        when sum(hours) over (order by hours desc) * 100.0 / sum(hours) over () <= 95 then 'B'
        else 'C'
    end as pareto
from agg
order by hours desc;

with ranked as (
    select *,
        row_number() over (
            partition by sensor_id, date_id, time_id
            order by telemetry_id desc
        ) as rn
    from fact_equipment_telemetry
),
stats as (
    select
        count(*) as total_rows,
        count(*) filter (where rn = 1) as after_rows
    from ranked
)
select
    total_rows,
    after_rows,
    total_rows - after_rows as removed,
    (total_rows - after_rows) * 100.0 / total_rows as pct
from stats;

with base as (
    select
        ft.sensor_id,
        ft.date_id,
        ft.time_id,
        ft.sensor_value,
        lag(ft.sensor_value) over w_seq as prev_val,
        avg(ft.sensor_value) over w8 as ma,
        stddev(ft.sensor_value) over w8 as sd,
        percent_rank() over w_seq as pct
    from fact_equipment_telemetry ft
    where ft.equipment_id = 1
      and ft.date_id between 20240101 and 20240107
    window
        w8 as (partition by ft.sensor_id order by ft.date_id, ft.time_id rows between 7 preceding and current row),
        w_seq as (partition by ft.sensor_id order by ft.date_id, ft.time_id)
)
select *,
    case
        when pct > 0.95 then 'ОПАСНОСТЬ'
        when pct > 0.85 then 'ВНИМАНИЕ'
        else 'Норма'
    end as risk
from base
where pct > 0.85;

with daily as (
    select
        d.full_date,
        sum(fp.tons_mined) as tons
    from fact_production fp
    join dim_date d on d.date_id = fp.date_id
    where fp.mine_id = 1 and d.year = 2024 and d.month = 1
    group by d.full_date
)
select
    full_date,
    tons,
    lag(tons) over w_seq as prev_day,
    (tons - lag(tons) over w_seq) * 100.0 / lag(tons) over w_seq as pct_change,
    avg(tons) over w7 as ma7,
    sum(tons) over w_seq as cumulative,
    rank() over w_seq as rnk,
    ntile(3) over w_seq as bucket,
    percentile_cont(0.5) within group (order by tons) over () as median,
    (tons - percentile_cont(0.5) within group (order by tons) over ()) * 100.0 /
        percentile_cont(0.5) within group (order by tons) over () as dev_pct,
    case
        when abs((tons - lag(tons) over w_seq) * 100.0 / lag(tons) over w_seq) < 5 then 'стабильно'
        when tons > lag(tons) over w_seq then 'рост'
        else 'снижение'
    end as trend
from daily
window
    w_seq as (order by full_date),
    w7 as (order by full_date rows between 6 preceding and current row)
order by full_date;