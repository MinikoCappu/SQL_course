select equipment_name,
Length(equipment_name) as name_len,
length(inventory_number) as inv_len,
length(model) as model_len,
length(manufacturer) as manufacturer_len,
length(manufacturer) + length(model) + length(inventory_number) as total_length
from dim_equipment
order by total_length desc;

select equipment_name, 
inventory_number,
split_part(inventory_number, '-', 1) as prefix,
split_part(inventory_number, '-', 2) as type_code,
split_part(inventory_number, '-', 3)::int as serial_number,
case split_part(inventory_number, '-', 2) 
 when 'LHD' then 'Погрузочно-доставочная машина'
 when 'TRK' then 'Шахтный самосвал'
 when 'CRT' then 'Вагонетка'
 when 'SKP' then 'Скиповой подъёмник'
end as off_cypher
from  dim_equipment
order by type_code, serial_number;

select first_name, last_name, middle_name,
last_name ||' '|| substring (first_name, 1, 1) ||'.'|| coalesce(substring(middle_name, 1, 1) ||'.', '') as short_name,
substring (first_name, 1, 1) ||'.'|| coalesce(substring(middle_name, 1, 1) ||'.', '') ||' '|| last_name as initials ,
upper(last_name),
lower(position) from dim_operator
order by last_name;

select equipment_name from dim_equipment where equipment_name like '%ПДМ%';
select equipment_name, manufacturer from dim_equipment where manufacturer ilike 's%';
select mine_name from dim_mine where mine_name like '%"%';
select inventory_number from dim_equipment where inventory_number ~ '-(00[1-9]|010)$';

select mine_name, 
count(equipment_id) as equipment_count,
string_agg(equipment_name, ', ' order by equipment_name) as equipment_list,
string_agg(distinct manufacturer, ', ' order by manufacturer) as manufacturers_list
from dim_mine
left join dim_equipment
on dim_mine.mine_id = dim_equipment.mine_id
group by dim_mine.mine_id, dim_mine.mine_name
order by dim_mine.mine_name;

select
equipment_name,
commissioning_date,
age(current_date, commissioning_date) as age_interval,
extract(year from age(current_date, commissioning_date)) as age_years,
(current_date - commissioning_date) as days_in_use,
case when extract(year from age(current_date, commissioning_date)) < 2 then 'новое'
when extract(year from age(current_date, commissioning_date)) between 2 and 4 then 'рабочее'
else 'требует внимания'
end as status_category
from dim_equipment
order by age_years desc;

select
equipment_name,
commissioning_date,
to_char(commissioning_date, 'dd.mm.yyyy') as ru_date,
to_char(commissioning_date, 'yyyy-mm-dd') as iso_date,
to_char(commissioning_date, 'yyyy-"q"q') as year_quarter,
to_char(commissioning_date, 'yyyy-mm') as year_month
from dim_equipment;

select
    extract(isodow from start_time) as day_of_week_num,
    to_char(start_time, 'tmday') as day_of_week_name,
    count(*) as downtime_count,
    round(avg(duration_min), 2) as avg_duration_min
from fact_equipment_downtime
group by
    extract(isodow from start_time),
    to_char(start_time, 'tmday')
order by day_of_week_num;

select
    date_trunc('hour', start_time) as hour_start,
    count(*) as downtime_count
from fact_equipment_downtime
group by date_trunc('hour', start_time)
order by hour_start;

select
    extract(hour from start_time) as hour_num,
    count(*) as downtime_count
from fact_equipment_downtime
group by extract(hour from start_time)
order by hour_num;

select
    extract(hour from start_time) as hour_num,
    count(*) as downtime_count
from fact_equipment_downtime
group by extract(hour from start_time)
order by downtime_count desc, hour_num
limit 1;

select
    s.sensor_code,
    st.type_name,
    e.equipment_name,
    s.calibration_date,
    (current_date - s.calibration_date) as days_since,
    (s.calibration_date + interval '180 days')::date as next_calibration,
    case
        when (current_date - s.calibration_date) > 180 then 'просрочена'
        when (current_date - s.calibration_date) between 150 and 180 then 'скоро'
        else 'в норме'
    end as cal_status
from dim_sensor s
left join dim_equipment e
    on s.equipment_id = e.equipment_id
left join dim_sensor_type st
    on s.sensor_type_id = st.sensor_type_id
order by
    case
        when (current_date - s.calibration_date) > 180 then 1
        when (current_date - s.calibration_date) between 150 and 180 then 2
        else 3
    end,
    s.calibration_date;

select
    '[' || et.type_name || '] ' ||
    e.equipment_name || ' (' || e.manufacturer || ' ' || e.model || ')' ||
    ' | шахта: ' || m.mine_name ||
    ' | введён: ' || to_char(e.commissioning_date, 'dd.mm.yyyy') ||
    ' | возраст: ' || extract(year from age(current_date, e.commissioning_date)) || ' лет' ||
    ' | статус: ' ||
        case e.status
            when 'active' then 'АКТИВЕН'
            when 'maintenance' then 'НА ТО'
            when 'decommissioned' then 'СПИСАН'
            else upper(e.status)
        end ||
    ' | видеорег.: ' ||
        case when e.has_video_recorder then 'ДА' else 'НЕТ' end ||
    ' | навигация: ' ||
        case when e.has_navigation then 'ДА' else 'НЕТ' end
    as equipment_description
from dim_equipment e
left join dim_equipment_type et
    on e.equipment_type_id = et.equipment_type_id
left join dim_mine m
    on e.mine_id = m.mine_id;