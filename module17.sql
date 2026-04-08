create or replace function safe_production_rate(p_tons numeric, p_hours numeric)
returns numeric
language plpgsql
as $$
begin
    if p_tons is null or p_hours is null then
        return null;
    end if;

    return p_tons / p_hours;

exception
    when division_by_zero then
        raise warning 'division by zero';
        return 0;
end;
$$;

select safe_production_rate(150,8);
select safe_production_rate(150,0);
select safe_production_rate(null,8);

select equipment_id, tons_mined, operating_hours,
       safe_production_rate(tons_mined, operating_hours) as rate
from fact_production
where date_id = 20250115
order by rate desc
limit 10;

create or replace function validate_sensor_reading(p_sensor_type varchar, p_value numeric)
returns varchar
language plpgsql
as $$
begin
    if p_sensor_type = 'Температура' then
        if p_value < -40 or p_value > 200 then
            raise exception 'out of range' using errcode='S0002', hint='-40..200';
        end if;
    elsif p_sensor_type = 'Давление' then
        if p_value < 0 or p_value > 500 then
            raise exception 'out of range' using errcode='S0002', hint='0..500';
        end if;
    elsif p_sensor_type = 'Вибрация' then
        if p_value < 0 or p_value > 100 then
            raise exception 'out of range' using errcode='S0002', hint='0..100';
        end if;
    elsif p_sensor_type = 'Скорость' then
        if p_value < 0 or p_value > 50 then
            raise exception 'out of range' using errcode='S0002', hint='0..50';
        end if;
    else
        raise exception 'unknown type' using errcode='S0001';
    end if;

    return 'OK';
end;
$$;

do $$
declare
    i int;
    v_ok int:=0;
    v_err int:=0;
begin
    for i in 1..10 loop
        begin
            insert into fact_equipment_downtime(
                equipment_id, date_id, duration_min
            )
            values(
                case when i=3 then 999999 else 1 end,
                20250101,
                case when i=4 then null else 10 end
            );

            v_ok := v_ok + 1;

        exception
            when others then
                perform log_error('insert_downtime', sqlerrm);
                raise warning 'row % error: %', i, sqlerrm;
                v_err := v_err + 1;
        end;
    end loop;

    raise notice 'ok=% error=%', v_ok, v_err;
end $$;

create or replace function test_error_diagnostics(p_error_type int)
returns table(field_name varchar, field_value text)
language plpgsql
as $$
declare
    v_msg text;
    v_detail text;
    v_hint text;
    v_code text;
begin
    begin
        if p_error_type=1 then perform 1/0;
        elsif p_error_type=2 then insert into dim_mine(mine_id) values(1);
        elsif p_error_type=3 then insert into fact_production(mine_id) values(9999);
        elsif p_error_type=4 then perform 'abc'::int;
        else raise exception 'custom';
        end if;

    exception
        when others then
            get stacked diagnostics
                v_msg = message_text,
                v_detail = pg_exception_detail,
                v_hint = pg_exception_hint,
                v_code = returned_sqlstate;

            return query select 'message', v_msg;
            return query select 'detail', v_detail;
            return query select 'hint', v_hint;
            return query select 'code', v_code;
    end;
end;
$$;

create table staging_lab_results (
    row_id serial,
    mine_name text,
    sample_date text,
    fe_content text,
    moisture text,
    status varchar(20) default 'NEW',
    error_msg text
);

create or replace function process_lab_import()
returns table(total int, valid int, errors int)
language plpgsql
as $$
declare
    r record;
    v_total int:=0;
    v_valid int:=0;
    v_err int:=0;
    v_date date;
    v_fe numeric;
begin
    for r in select * from staging_lab_results where status='NEW' loop
        v_total := v_total + 1;
        begin
            v_date := to_date(r.sample_date,'DD-MM-YYYY');
            v_fe := r.fe_content::numeric;

            if v_fe < 0 or v_fe > 100 then
                raise exception 'fe range';
            end if;

            if not exists(select 1 from dim_mine where mine_name=r.mine_name) then
                raise exception 'mine not found';
            end if;

            update staging_lab_results set status='VALID' where row_id=r.row_id;
            v_valid := v_valid + 1;

        exception
            when others then
                update staging_lab_results set status='ERROR', error_msg=sqlerrm where row_id=r.row_id;
                perform log_error('process_lab_import', sqlerrm);
                v_err := v_err + 1;
        end;
    end loop;

    return query select v_total, v_valid, v_err;
end;
$$;

create table daily_kpi (
    kpi_id serial primary key,
    mine_id int,
    date_id int,
    tons_mined numeric,
    oee_percent numeric,
    downtime_hours numeric,
    quality_score numeric,
    status varchar(20),
    error_detail text,
    calculated_at timestamp default now(),
    unique (mine_id, date_id)
);

create or replace function recalculate_daily_kpi(p_date_id int)
returns table(mines_processed int, mines_ok int, mines_error int)
language plpgsql
as $$
declare
    r record;
    v_proc int:=0;
    v_ok int:=0;
    v_err int:=0;
    v_tons numeric;
    v_hours numeric;
    v_dt numeric;
    v_fe numeric;
begin
    for r in select mine_id from dim_mine loop
        v_proc := v_proc + 1;
        begin
            select sum(tons_mined), sum(operating_hours)
            into v_tons, v_hours
            from fact_production
            where mine_id=r.mine_id and date_id=p_date_id;

            select sum(duration_min)/60.0 into v_dt
            from fact_equipment_downtime
            where date_id=p_date_id;

            select avg(fe_content) into v_fe
            from fact_ore_quality
            where mine_id=r.mine_id and date_id=p_date_id;

            insert into daily_kpi(mine_id,date_id,tons_mined,oee_percent,downtime_hours,quality_score,status)
            values(r.mine_id,p_date_id,v_tons,(v_hours/8)*100,v_dt,v_fe,'OK')
            on conflict (mine_id,date_id) do update
            set tons_mined=excluded.tons_mined;

            v_ok := v_ok + 1;

        exception
            when others then
                insert into daily_kpi(mine_id,date_id,status,error_detail)
                values(r.mine_id,p_date_id,'ERROR',sqlerrm)
                on conflict (mine_id,date_id) do update
                set status='ERROR', error_detail=excluded.error_detail;

                perform log_error('recalculate_daily_kpi', sqlerrm);
                v_err := v_err + 1;
        end;
    end loop;

    return query select v_proc, v_ok, v_err;
end;
$$;