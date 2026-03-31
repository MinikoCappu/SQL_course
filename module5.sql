set search_path to bairbiliktuev, public;
-- ============================================================
-- задание 1. добавление нового оборудования
-- ============================================================

begin;

select *
from bairbiliktuev.practice_dim_equipment
where equipment_id = 200
   or inventory_number = 'INV-TRK-200';

insert into bairbiliktuev.practice_dim_equipment (
    equipment_id,
    equipment_type_id,
    mine_id,
    equipment_name,
    inventory_number,
    manufacturer,
    model,
    year_manufactured,
    commissioning_date,
    status,
    has_video_recorder,
    has_navigation
)
values (
    200,
    2,
    2,
    'Самосвал МоАЗ-7529',
    'INV-TRK-200',
    'МоАЗ',
    '7529',
    2025,
    '2025-03-15',
    'active',
    true,
    true
);

select *
from bairbiliktuev.practice_dim_equipment
where equipment_id = 200;

rollback;



-- ============================================================
-- задание 2. массовая вставка операторов
-- ============================================================

begin;

select *
from bairbiliktuev.practice_dim_operator
where operator_id >= 200
   or tab_number in ('TAB-200', 'TAB-201', 'TAB-202');

insert into bairbiliktuev.practice_dim_operator (
    operator_id,
    tab_number,
    last_name,
    first_name,
    middle_name,
    position,
    qualification,
    hire_date,
    mine_id
)
values
    (200, 'TAB-200', 'Сидоров', 'Михаил', 'Иванович', 'Машинист ПДМ', '4 разряд', '2025-03-01', 1),
    (201, 'TAB-201', 'Петрова', 'Елена', 'Сергеевна', 'Оператор скипа', '3 разряд', '2025-03-01', 2),
    (202, 'TAB-202', 'Волков', 'Дмитрий', 'Алексеевич', 'Водитель самосвала', '5 разряд', '2025-03-10', 2);

select *
from bairbiliktuev.practice_dim_operator
where operator_id >= 200
order by operator_id;

rollback;



-- ============================================================
-- задание 3. загрузка из staging
-- ============================================================

begin;

select count(*) as before_count
from bairbiliktuev.practice_fact_production;

select *
from bairbiliktuev.staging_production
where is_validated = true
order by staging_id;

insert into bairbiliktuev.practice_fact_production (
    production_id,
    date_id,
    shift_id,
    mine_id,
    shaft_id,
    equipment_id,
    operator_id,
    location_id,
    ore_grade_id,
    tons_mined,
    tons_transported,
    trips_count,
    distance_km,
    fuel_consumed_l,
    operating_hours,
    loaded_at
)
select
    3000 + s.staging_id as production_id,
    s.date_id,
    s.shift_id,
    s.mine_id,
    s.shaft_id,
    s.equipment_id,
    s.operator_id,
    s.location_id,
    s.ore_grade_id,
    s.tons_mined,
    s.tons_transported,
    s.trips_count,
    s.distance_km,
    s.fuel_consumed_l,
    s.operating_hours,
    s.loaded_at
from bairbiliktuev.staging_production s
where s.is_validated = true
  and not exists (
      select 1
      from bairbiliktuev.practice_fact_production p
      where p.date_id = s.date_id
        and p.shift_id = s.shift_id
        and p.equipment_id = s.equipment_id
        and p.operator_id = s.operator_id
  );

select count(*) as after_count
from bairbiliktuev.practice_fact_production;

select *
from bairbiliktuev.practice_fact_production
where production_id >= 3001
order by production_id;

rollback;



-- ============================================================
-- задание 4. insert ... returning с логированием
-- ============================================================

begin;

select *
from bairbiliktuev.practice_dim_ore_grade
where ore_grade_id = 300
   or grade_code = 'EXPORT';

with inserted_grade as (
    insert into bairbiliktuev.practice_dim_ore_grade (
        ore_grade_id,
        grade_name,
        grade_code,
        fe_content_min,
        fe_content_max,
        description
    )
    values (
        300,
        'Экспортный',
        'EXPORT',
        63.00,
        68.00,
        'Руда для экспортных поставок'
    )
    returning ore_grade_id, grade_name, grade_code
)
insert into bairbiliktuev.practice_equipment_log (
    equipment_id,
    action,
    details
)
select
    0,
    'INSERT',
    'Добавлен сорт руды: ' || grade_name || ' (' || grade_code || ')'
from inserted_grade;

select *
from bairbiliktuev.practice_dim_ore_grade
where ore_grade_id = 300;

select *
from bairbiliktuev.practice_equipment_log
where equipment_id = 0
  and action = 'INSERT'
order by log_id desc;

rollback;



-- ============================================================
-- задание 5. обновление статуса оборудования
-- ============================================================

begin;

select
    equipment_id,
    equipment_name,
    year_manufactured,
    status
from bairbiliktuev.practice_dim_equipment
where mine_id = 1
  and year_manufactured <= 2018
order by equipment_id;

update bairbiliktuev.practice_dim_equipment
set status = 'maintenance'
where mine_id = 1
  and year_manufactured <= 2018
returning equipment_id, equipment_name, year_manufactured, status;

select
    equipment_id,
    equipment_name,
    year_manufactured,
    status
from bairbiliktuev.practice_dim_equipment
where status = 'maintenance'
order by equipment_id;

rollback;



-- ============================================================
-- задание 6. update с подзапросом
-- ============================================================

begin;

select
    e.equipment_id,
    e.equipment_name,
    e.has_navigation
from bairbiliktuev.practice_dim_equipment e
where e.has_navigation = false
order by e.equipment_id;

update bairbiliktuev.practice_dim_equipment e
set has_navigation = true
where e.has_navigation = false
  and e.equipment_id in (
      select distinct s.equipment_id
      from public.dim_sensor s
      join public.dim_sensor_type st
        on s.sensor_type_id = st.sensor_type_id
      where st.type_code = 'NAV'
        and lower(s.status) = 'active'
  )
returning e.equipment_id, e.equipment_name, e.has_navigation;

select
    e.equipment_id,
    e.equipment_name,
    e.has_navigation
from bairbiliktuev.practice_dim_equipment e
where e.has_navigation = true
order by e.equipment_id;

rollback;

-- ============================================================
-- задание 7. delete с условием и архивированием
-- ============================================================

begin;

select *
from bairbiliktuev.practice_fact_telemetry
where date_id = 20240315
  and is_alarm = true
order by telemetry_id;

select *
from bairbiliktuev.practice_archive_telemetry
order by archived_at desc, telemetry_id desc;

with deleted_rows as (
    delete from bairbiliktuev.practice_fact_telemetry
    where date_id = 20240315
      and is_alarm = true
    returning
        telemetry_id,
        date_id,
        time_id,
        equipment_id,
        sensor_id,
        location_id,
        sensor_value,
        is_alarm,
        quality_flag,
        loaded_at
)
insert into bairbiliktuev.practice_archive_telemetry (
    telemetry_id,
    date_id,
    time_id,
    equipment_id,
    sensor_id,
    location_id,
    sensor_value,
    is_alarm,
    quality_flag,
    loaded_at
)
select
    telemetry_id,
    date_id,
    time_id,
    equipment_id,
    sensor_id,
    location_id,
    sensor_value,
    is_alarm,
    quality_flag,
    loaded_at
from deleted_rows;

select *
from bairbiliktuev.practice_fact_telemetry
where date_id = 20240315
  and is_alarm = true;

select *
from bairbiliktuev.practice_archive_telemetry
order by archived_at desc, telemetry_id desc;

rollback;



-- ============================================================
-- задание 8. merge — синхронизация справочника
-- ============================================================

begin;

select *
from bairbiliktuev.practice_dim_downtime_reason
order by reason_code;

select *
from bairbiliktuev.staging_downtime_reasons
order by reason_code;

with max_id as (
    select coalesce(max(reason_id), 0) as max_reason_id
    from bairbiliktuev.practice_dim_downtime_reason
),
src as (
    select
        s.reason_name,
        s.reason_code,
        s.category,
        s.description,
        m.max_reason_id
        + row_number() over (
            order by s.reason_code
          ) as new_reason_id
    from bairbiliktuev.staging_downtime_reasons s
    cross join max_id m
)
merge into bairbiliktuev.practice_dim_downtime_reason as t
using src
on t.reason_code = src.reason_code
when matched then
    update set
        reason_name = src.reason_name,
        category = src.category,
        description = src.description
when not matched then
    insert (
        reason_id,
        reason_name,
        reason_code,
        category,
        description
    )
    values (
        src.new_reason_id,
        src.reason_name,
        src.reason_code,
        src.category,
        src.description
    );

select *
from bairbiliktuev.practice_dim_downtime_reason
order by reason_code;

select
    reason_code,
    count(*) as cnt
from bairbiliktuev.practice_dim_downtime_reason
group by reason_code
having count(*) > 1;

rollback;



-- ============================================================
-- задание 9. upsert — идемпотентная загрузка
-- ============================================================

begin;

select *
from bairbiliktuev.practice_dim_operator
where tab_number in ('TAB-200', 'TAB-201', 'TAB-NEW')
order by tab_number;

insert into bairbiliktuev.practice_dim_operator (
    operator_id,
    tab_number,
    last_name,
    first_name,
    middle_name,
    position,
    qualification,
    hire_date,
    mine_id
)
values
    (200, 'TAB-200', 'Сидоров', 'Михаил', 'Иванович', 'Старший машинист ПДМ', '5 разряд', '2025-03-01', 1),
    (201, 'TAB-201', 'Петрова', 'Елена', 'Сергеевна', 'Старший оператор скипа', '4 разряд', '2025-03-01', 2),
    (203, 'TAB-NEW', 'Орлов', 'Андрей', 'Павлович', 'Оператор буровой установки', '4 разряд', '2025-03-12', 1)
on conflict (tab_number)
do update set
    position = excluded.position,
    qualification = excluded.qualification;

select *
from bairbiliktuev.practice_dim_operator
where tab_number in ('TAB-200', 'TAB-201', 'TAB-NEW')
order by tab_number;

rollback;