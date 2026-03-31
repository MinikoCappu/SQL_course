set search_path to public;

-- Задание 1 — округление результатов анализов
select
    sample_number,
    round(fe_content, 1) as fe_rounded,
    ceil(sio2_content) as sio2_ceil,
    floor(al2o3_content) as al2o3_floor
from fact_ore_quality
where date_id = 20240315
order by fe_rounded desc;

-- Задание 2 — отклонение от целевого содержания Fe
select
    sample_number,
    fe_content,
    fe_content - 60 as deviation,
    abs(fe_content - 60) as abs_deviation,
    case
        when sign(fe_content - 60) = 1 then 'Выше нормы'
        when sign(fe_content - 60) = 0 then 'В норме'
        else 'Ниже нормы'
    end as direction,
    power(fe_content - 60, 2) as squared_dev
from fact_ore_quality
where date_id between 20240301 and 20240331
order by abs_deviation desc
limit 10;

-- Задание 3 — статистика добычи по сменам
select
    shift_id,
    case shift_id
        when 1 then 'Утренняя'
        when 2 then 'Дневная'
        when 3 then 'Ночная'
    end as shift_name,
    count(*) as records_count,
    sum(tons_mined) as total_tons,
    round(avg(tons_mined), 2) as avg_tons,
    count(distinct operator_id) as unique_operators
from fact_production
where date_id between 20240301 and 20240331
group by shift_id
order by shift_id;

-- Задание 4 — причины простоев по оборудованию
select
    e.equipment_name,
    string_agg(distinct dr.reason_name, '; ' order by dr.reason_name) as reasons,
    sum(fd.duration_min) as total_min,
    count(*) as incidents
from fact_equipment_downtime fd
join dim_equipment e on fd.equipment_id = e.equipment_id
join dim_downtime_reason dr on fd.reason_id = dr.reason_id
where fd.date_id between 20240301 and 20240331
group by e.equipment_name
order by total_min desc;

-- Задание 5 — преобразование date_id и форматирование
select
    date_id,
    to_char(to_date(date_id::text, 'YYYYMMDD'), 'DD.MM.YYYY') as formatted_date,
    sum(tons_mined) as total_tons,
    to_char(sum(tons_mined), 'FM999G999G999D00') as formatted_tons
from fact_production
where date_id between 20240301 and 20240307
group by date_id
order by date_id;

-- Задание 6 — классификация проб и процент качества
select
    d.full_date,
    sum(case when foq.fe_content >= 65 then 1 else 0 end) as rich_ore,
    sum(case when foq.fe_content between 55 and 64.999 then 1 else 0 end) as medium_ore,
    sum(case when foq.fe_content < 55 then 1 else 0 end) as poor_ore,
    count(*) as total,
    round(
        100.0 * sum(case when foq.fe_content >= 60 then 1 else 0 end)
        / nullif(count(*), 0), 1
    ) as good_pct
from fact_ore_quality foq
join dim_date d on foq.date_id = d.date_id
where foq.date_id between 20240301 and 20240331
group by d.full_date
order by d.full_date;

-- Задание 7 — KPI операторов
select
    o.last_name ||' '|| o.first_name,
    sum(fp.tons_mined) as total_tons,
    coalesce(sum(fp.fuel_consumed_l), 0) as total_fuel,
    round(sum(fp.tons_mined) / nullif(sum(fp.trips_count), 0), 2) as tons_per_trip,
    round(
        coalesce(sum(fp.fuel_consumed_l), 0) / nullif(sum(fp.tons_mined), 0),
        3
    ) as fuel_per_ton
from fact_production fp
join dim_operator o on fp.operator_id = o.operator_id
where fp.date_id between 20240301 and 20240331
group by o.last_name, o.first_name
order by tons_per_trip desc;

-- Задание 8 — анализ пропусков данных
select
    count(*) as total_rows,

    count(sio2_content) as sio2_filled,
    count(*) - count(sio2_content) as sio2_null,
    round(100.0 * count(sio2_content) / count(*), 1) as sio2_pct,

    count(al2o3_content) as al2o3_filled,
    count(*) - count(al2o3_content) as al2o3_null,
    round(100.0 * count(al2o3_content) / count(*), 1) as al2o3_pct,

    count(moisture) as moisture_filled,
    count(*) - count(moisture) as moisture_null,
    round(100.0 * count(moisture) / count(*), 1) as moisture_pct,

    count(density) as density_filled,
    count(*) - count(density) as density_null,
    round(100.0 * count(density) / count(*), 1) as density_pct,

    count(sample_weight_kg) as weight_filled,
    count(*) - count(sample_weight_kg) as weight_null,
    round(100.0 * count(sample_weight_kg) / count(*), 1) as weight_pct

from fact_ore_quality
where date_id between 20240301 and 20240331;

-- Задание 9 — KPI оборудования
select
    e.equipment_name,
    et.type_name,

    count(*) as shifts_count,
    round(sum(fp.tons_mined), 1) as total_tons,
    round(sum(fp.operating_hours), 1) as total_hours,

    round(
        sum(fp.tons_mined) / nullif(sum(fp.operating_hours), 0),
        2
    ) as productivity,

    round(
        sum(fp.operating_hours) / nullif(count(*) * 8, 0) * 100,
        1
    ) as utilization_pct,

    round(
        coalesce(sum(fp.fuel_consumed_l), 0) /
        nullif(sum(fp.tons_mined), 0),
        3
    ) as fuel_per_ton,

    case
        when sum(fp.tons_mined) / nullif(sum(fp.operating_hours), 0) > 20 then 'Высокая'
        when sum(fp.tons_mined) / nullif(sum(fp.operating_hours), 0) > 12 then 'Средняя'
        else 'Низкая'
    end as efficiency_category

from fact_production fp
join dim_equipment e on fp.equipment_id = e.equipment_id
join dim_equipment_type et on e.equipment_type_id = et.equipment_type_id
where fp.date_id between 20240301 and 20240331
group by e.equipment_name, et.type_name
order by productivity desc;

-- Задание 10 — анализ и категоризация простоев
with categorized as (
    select
        e.equipment_name,
        dr.reason_name,
        coalesce(fd.duration_min, 0) as duration_safe,
        round(coalesce(fd.duration_min, 0) / 60.0, 1) as duration_hours,
        case
            when coalesce(fd.duration_min, 0) > 480 then 'Критический'
            when coalesce(fd.duration_min, 0) > 120 then 'Длительный'
            when coalesce(fd.duration_min, 0) > 30 then 'Средний'
            else 'Короткий'
        end as category,
        case
            when fd.is_planned then 'Плановый'
            else 'Внеплановый'
        end as plan_status,
        case
            when fd.end_time is null then 'В процессе'
            else 'Завершён'
        end as completion_status
    from fact_equipment_downtime fd
    join dim_equipment e on fd.equipment_id = e.equipment_id
    join dim_downtime_reason dr on fd.reason_id = dr.reason_id
    where fd.date_id between 20240301 and 20240331
)
select
    category,
    count(*) as incidents,
    round(sum(duration_safe) / 60.0, 1) as total_hours,
    round(
        100.0 * sum(duration_safe) /
        nullif(sum(sum(duration_safe)) over (), 0),
        1
    ) as pct
from categorized
group by category
order by total_hours desc;

