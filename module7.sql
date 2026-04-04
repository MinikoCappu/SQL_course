select
    tablename,
    indexname,
    indexdef
from pg_indexes
where tablename in (
    'fact_production',
    'fact_equipment_telemetry',
    'fact_equipment_downtime',
    'fact_ore_quality'
)
order by tablename, indexname;

select
    tablename,
    indexname,
    indexdef
from pg_indexes
where tablename in (
    'fact_production',
    'fact_equipment_telemetry',
    'fact_equipment_downtime',
    'fact_ore_quality'
)
order by tablename, indexname;

select
    relname,
    pg_size_pretty(pg_relation_size(relid)) as table_size,
    pg_size_pretty(pg_indexes_size(relid)) as indexes_size
from pg_catalog.pg_statio_user_tables
where relname in (
    'fact_production',
    'fact_equipment_telemetry',
    'fact_equipment_downtime',
    'fact_ore_quality'
);

EXPLAIN
SELECT e.equipment_name,
       SUM(p.tons_mined) AS total_tons,
       SUM(p.fuel_consumed_l) AS total_fuel,
       SUM(p.operating_hours) AS total_hours
FROM fact_production p
JOIN dim_equipment e ON p.equipment_id = e.equipment_id
WHERE p.date_id BETWEEN 20240301 AND 20240331
GROUP BY e.equipment_name
ORDER BY total_tons DESC;

EXPLAIN ANALYZE
SELECT e.equipment_name,
       SUM(p.tons_mined) AS total_tons,
       SUM(p.fuel_consumed_l) AS total_fuel,
       SUM(p.operating_hours) AS total_hours
FROM fact_production p
JOIN dim_equipment e ON p.equipment_id = e.equipment_id
WHERE p.date_id BETWEEN 20240301 AND 20240331
GROUP BY e.equipment_name
ORDER BY total_tons DESC;

EXPLAIN (ANALYZE, BUFFERS)
SELECT e.equipment_name,
       SUM(p.tons_mined) AS total_tons,
       SUM(p.fuel_consumed_l) AS total_fuel,
       SUM(p.operating_hours) AS total_hours
FROM fact_production p
JOIN dim_equipment e ON p.equipment_id = e.equipment_id
WHERE p.date_id BETWEEN 20240301 AND 20240331
GROUP BY e.equipment_name
ORDER BY total_tons DESC;

EXPLAIN (ANALYZE, BUFFERS)
SELECT e.equipment_name,
       SUM(p.tons_mined) AS total_tons,
       SUM(p.fuel_consumed_l) AS total_fuel,
       SUM(p.operating_hours) AS total_hours
FROM fact_production p
JOIN dim_equipment e ON p.equipment_id = e.equipment_id
WHERE p.date_id BETWEEN 20240301 AND 20240331
GROUP BY e.equipment_name
ORDER BY total_tons DESC;

create index idx_prod_fuel
on fact_production(fuel_consumed_l);

create index idx_telemetry_alarm_partial
on fact_equipment_telemetry(date_id)
where is_alarm = true;

create index idx_telemetry_alarm_partial
on fact_equipment_telemetry(date_id)
where is_alarm = true;

create index idx_telemetry_alarm_full
on fact_equipment_telemetry(date_id, is_alarm);

create index idx_prod_equip_date
on fact_production(equipment_id, date_id);

create index idx_prod_date_equip
on fact_production(date_id, equipment_id);

create index idx_operator_lower_lastname
on dim_operator(lower(last_name));
vacuum fact_production;

create index idx_telemetry_date_brin
on fact_equipment_telemetry using brin(date_id)
with (pages_per_range = 128);

select count(*)
from pg_indexes
where tablename = 'fact_production';

create index idx_test_1 on fact_production(tons_mined);
create index idx_test_2 on fact_production(fuel_consumed_l, operating_hours);
create index idx_test_3 on fact_production(date_id, shift_id, mine_id);

create index idx_prod_date_mine
on fact_production(date_id, mine_id);

create index idx_quality_date
on fact_ore_quality(date_id, ore_grade_id);

create index idx_downtime_unplanned
on fact_equipment_downtime(date_id)
where is_planned = false;

create index idx_telemetry_equip_alarm
on fact_equipment_telemetry(equipment_id, date_id desc)
where is_alarm = true;

create index idx_prod_operator_date
on fact_production(operator_id, date_id);


-- ============================================================
-- Удаление индексов, созданных в ходе лабораторной работы
-- ============================================================

-- Задание 3
DROP INDEX IF EXISTS idx_prod_fuel;

-- Задание 4
DROP INDEX IF EXISTS idx_telemetry_alarm_partial;
DROP INDEX IF EXISTS idx_telemetry_alarm_full;

-- Задание 5
DROP INDEX IF EXISTS idx_prod_equip_date;
DROP INDEX IF EXISTS idx_prod_date_equip;

-- Задание 6
DROP INDEX IF EXISTS idx_operator_lower_lastname;

-- Задание 7
DROP INDEX IF EXISTS idx_prod_date_cover;
DROP INDEX IF EXISTS idx_prod_date_cover_ext;

-- Задание 8
DROP INDEX IF EXISTS idx_telemetry_date_brin;

-- Задание 9
DROP INDEX IF EXISTS idx_test_1;
DROP INDEX IF EXISTS idx_test_2;
DROP INDEX IF EXISTS idx_test_3;

-- Задание 10
DROP INDEX IF EXISTS idx_prod_date_mine;
DROP INDEX IF EXISTS idx_quality_date;
DROP INDEX IF EXISTS idx_downtime_unplanned;
DROP INDEX IF EXISTS idx_telemetry_equip_alarm;
DROP INDEX IF EXISTS idx_prod_operator_date;

-- ============================================================
-- Удаление тестовых строк
-- ============================================================
DELETE FROM fact_production
WHERE date_id = 20240401;
