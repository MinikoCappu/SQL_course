create or replace function calc_oee(
    p_operating_hours numeric,
    p_planned_hours numeric,
    p_actual_tons numeric,
    p_target_tons numeric
)
returns numeric
language sql
immutable
as $$
select
    case
        when p_planned_hours = 0 or p_target_tons = 0 then null
        else round(
            (p_operating_hours / p_planned_hours) *
            (p_actual_tons / p_target_tons) * 100, 1
        )
    end;
$$;

select calc_oee(10,12,80,100);
select calc_oee(12,12,100,100);
select calc_oee(8,12,0,100);

select
    equipment_id,
    calc_oee(sum(operating_hours), 12, sum(tons_mined), 100)
from fact_production
group by equipment_id;

create or replace function classify_downtime(p_duration_min int)
returns varchar
language plpgsql
as $$
begin
    if p_duration_min < 15 then return 'Микропростой';
    elsif p_duration_min <= 60 then return 'Краткий простой';
    elsif p_duration_min <= 240 then return 'Средний простой';
    elsif p_duration_min <= 480 then return 'Длительный простой';
    else return 'Критический простой';
    end if;
end;
$$;

select
    classify_downtime(duration_min),
    count(*),
    avg(duration_min),
    count(*) * 100.0 / sum(count(*)) over ()
from fact_equipment_downtime
where date_id between 20240101 and 20240131
group by 1;

create or replace function get_equipment_summary(
    p_equipment_id int,
    p_date_from int,
    p_date_to int
)
returns table(
    report_date date,
    tons_mined numeric,
    trips int,
    operating_hours numeric,
    fuel_liters numeric,
    tons_per_hour numeric
)
language sql
stable
as $$
select
    d.full_date,
    sum(fp.tons_mined),
    sum(fp.trips_count),
    sum(fp.operating_hours),
    sum(fp.fuel_consumed_l),
    sum(fp.tons_mined)/nullif(sum(fp.operating_hours),0)
from fact_production fp
join dim_date d on fp.date_id = d.date_id
where fp.equipment_id = p_equipment_id
and fp.date_id between p_date_from and p_date_to
group by d.full_date;
$$;

select * from get_equipment_summary(1,20240101,20240131);

select e.equipment_name, s.*
from dim_equipment e
cross join lateral get_equipment_summary(e.equipment_id,20240101,20240131) s
where e.mine_id=1;

create or replace function get_production_filtered(
    p_date_from int,
    p_date_to int,
    p_mine_id int default null,
    p_shift_id int default null,
    p_equipment_type_id int default null
)
returns table(
    mine_name text,
    shift_name text,
    equipment_type text,
    total_tons numeric,
    total_trips numeric,
    equip_count int
)
language sql
as $$
select
    m.mine_name,
    s.shift_name,
    et.type_name,
    sum(fp.tons_mined),
    sum(fp.trips_count),
    count(distinct fp.equipment_id)
from fact_production fp
join dim_mine m on fp.mine_id=m.mine_id
join dim_shift s on fp.shift_id=s.shift_id
join dim_equipment e on fp.equipment_id=e.equipment_id
join dim_equipment_type et on e.equipment_type_id=et.equipment_type_id
where fp.date_id between p_date_from and p_date_to
and (p_mine_id is null or fp.mine_id=p_mine_id)
and (p_shift_id is null or fp.shift_id=p_shift_id)
and (p_equipment_type_id is null or et.equipment_type_id=p_equipment_type_id)
group by m.mine_name,s.shift_name,et.type_name;
$$;

create table archive_telemetry (like fact_equipment_telemetry including all);

create or replace procedure archive_old_telemetry(
    p_before_date_id int,
    out p_archived int,
    out p_deleted int
)
language plpgsql
as $$
begin
    insert into archive_telemetry
    select * from fact_equipment_telemetry
    where date_id < p_before_date_id;
    get diagnostics p_archived = row_count;
    commit;

    delete from fact_equipment_telemetry
    where date_id < p_before_date_id;
    get diagnostics p_deleted = row_count;
    commit;
end;
$$;

create or replace function count_fact_records(
    p_table_name text,
    p_date_from int,
    p_date_to int
)
returns bigint
language plpgsql
as $$
declare v_sql text;
v_cnt bigint;
begin
    if p_table_name not like 'fact_%' then
        raise exception 'invalid table';
    end if;

    v_sql := format(
        'select count(*) from %I where date_id between $1 and $2',
        p_table_name
    );

    execute v_sql into v_cnt using p_date_from, p_date_to;
    return v_cnt;
end;
$$;

create or replace function build_production_report(
    p_group_by text,
    p_date_from int,
    p_date_to int,
    p_order_by text default 'total_tons desc'
)
returns table(
    dimension_name varchar,
    total_tons numeric,
    total_trips bigint,
    avg_productivity numeric
)
language plpgsql
as $$
declare v_join text;
v_field text;
v_sql text;
begin
    case p_group_by
        when 'mine' then
            v_join := 'join dim_mine d on fp.mine_id=d.mine_id';
            v_field := 'd.mine_name';
        when 'shift' then
            v_join := 'join dim_shift d on fp.shift_id=d.shift_id';
            v_field := 'd.shift_name';
        when 'operator' then
            v_join := 'join dim_operator d on fp.operator_id=d.operator_id';
            v_field := 'd.last_name';
        when 'equipment' then
            v_join := 'join dim_equipment d on fp.equipment_id=d.equipment_id';
            v_field := 'd.equipment_name';
        when 'equipment_type' then
            v_join := 'join dim_equipment e on fp.equipment_id=e.equipment_id join dim_equipment_type d on e.equipment_type_id=d.equipment_type_id';
            v_field := 'd.type_name';
        else
            raise exception 'invalid group';
    end case;

    v_sql := format(
        'select %s::varchar,
                sum(fp.tons_mined),
                sum(fp.trips_count),
                sum(fp.tons_mined)/nullif(sum(fp.operating_hours),0)
         from fact_production fp %s
         where fp.date_id between $1 and $2
         group by 1
         order by %s',
        v_field, v_join, p_order_by
    );

    return query execute v_sql using p_date_from, p_date_to;
end;
$$;

create table staging_daily_production(
    date_id int,
    equipment_id int,
    shift_id int,
    operator_id int,
    tons_mined numeric,
    trips_count int,
    operating_hours numeric,
    fuel_consumed_l numeric,
    loaded_at timestamp default now()
);

create table staging_rejected(
    date_id int,
    equipment_id int,
    shift_id int,
    operator_id int,
    tons_mined numeric,
    trips_count int,
    operating_hours numeric,
    fuel_consumed_l numeric,
    reject_reason text
);

create or replace procedure process_daily_production(
    p_date_id int,
    out p_validated int,
    out p_rejected int,
    out p_loaded int
)
language plpgsql
as $$
begin
    if not exists(select 1 from staging_daily_production where date_id=p_date_id) then
        raise exception 'no data';
    end if;

    insert into staging_rejected
    select s.*, 'invalid'
    from staging_daily_production s
    where s.date_id=p_date_id
    and (s.tons_mined<0
         or not exists(select 1 from dim_equipment e where e.equipment_id=s.equipment_id)
         or not exists(select 1 from dim_operator o where o.operator_id=s.operator_id));
    get diagnostics p_rejected = row_count;
    commit;

    delete from fact_production where date_id=p_date_id;

    insert into fact_production
    select *
    from staging_daily_production s
    where s.date_id=p_date_id
    and not exists(
        select 1 from staging_rejected r
        where r.date_id=s.date_id
        and r.equipment_id=s.equipment_id
    );
    get diagnostics p_loaded = row_count;
    p_validated := p_loaded;
    commit;
end;
$$;