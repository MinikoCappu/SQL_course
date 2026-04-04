do $$
declare
    v_mines int;
    v_tons numeric;
    v_fe numeric;
    v_downtime int;
begin
    select count(*) into v_mines from dim_mine;
    select sum(tons_mined) into v_tons from fact_production where date_id between 20250101 and 20250131;
    select avg(fe_content) into v_fe from fact_ore_quality where date_id between 20250101 and 20250131;
    select count(*) into v_downtime from fact_equipment_downtime where date_id between 20250101 and 20250131;

    raise notice '===== Сводка по предприятию «Руда+» =====';
    raise notice 'Количество шахт: %', v_mines;
    raise notice 'Добыча за январь 2025: % т', coalesce(v_tons,0);
    raise notice 'Среднее содержание Fe: % %%', round(coalesce(v_fe,0),1);
    raise notice 'Количество простоев: %', v_downtime;
    raise notice '==========================================';
end $$;

do $$
declare
    r record;
    v_age int;
    v_cat text;
    c_new int:=0;
    c_work int:=0;
    c_warn int:=0;
    c_replace int:=0;
begin
    for r in select e.equipment_name, et.type_name,
        coalesce(e.commissioning_date, current_date - (random()*4000)::int) as dt
        from dim_equipment e
        join dim_equipment_type et on e.equipment_type_id=et.equipment_type_id
    loop
        v_age := extract(year from age(current_date, r.dt));
        if v_age < 2 then v_cat:='Новое'; c_new:=c_new+1;
        elsif v_age <=5 then v_cat:='Рабочее'; c_work:=c_work+1;
        elsif v_age <=10 then v_cat:='Требует внимания'; c_warn:=c_warn+1;
        else v_cat:='На замену'; c_replace:=c_replace+1;
        end if;

        raise notice '% | % | % лет | %', r.equipment_name, r.type_name, v_age, v_cat;
    end loop;

    raise notice 'Итого: Новое=% Рабочее=% Внимание=% Замена=%', c_new,c_work,c_warn,c_replace;
end $$;

do $$
declare
    i int;
    v_date int;
    v_tons numeric;
    v_total numeric:=0;
    v_avg numeric;
    v_cnt int:=0;
    v_best numeric:=0;
    v_best_day int:=0;
begin
    for i in 1..14 loop
        v_date := 20250100 + i;
        select coalesce(sum(tons_mined),0) into v_tons from fact_production where date_id=v_date;

        v_total := v_total + v_tons;
        v_cnt := v_cnt + 1;
        v_avg := v_total / v_cnt;

        if v_tons > v_best then
            v_best := v_tons;
            v_best_day := i;
        end if;

        if v_tons > v_avg then
            raise notice 'День %: % т | Нарастающий: % т | РЕКОРД', lpad(i::text,2,'0'), v_tons, v_total;
        else
            raise notice 'День %: % т | Нарастающий: % т', lpad(i::text,2,'0'), v_tons, v_total;
        end if;
    end loop;

    raise notice 'Итого: %, Среднее: %, Лучший день: %', v_total, v_avg, v_best_day;
end $$;

do $$
declare
    v_date int:=20250101;
    v_sum numeric:=0;
    v_day numeric;
    v_threshold numeric:=500;
begin
    while v_date <= 20250131 loop
        select coalesce(sum(duration_min)/60.0,0) into v_day from fact_equipment_downtime where date_id=v_date;
        v_sum := v_sum + v_day;

        if v_sum >= v_threshold then
            raise notice 'Порог достигнут: %', v_date;
            exit;
        end if;

        v_date := v_date + 1;
        continue;
    end loop;

    if v_sum < v_threshold then
        raise notice 'Порог не достигнут';
    end if;
end $$;

do $$
declare
    arr int[];
    v_type int;
    v_cnt int;
    v_meas bigint;
    v_status text;
begin
    select array_agg(sensor_type_id) into arr from dim_sensor_type;

    foreach v_type in array arr loop
        select count(*) into v_cnt from dim_sensor where sensor_type_id=v_type;
        select count(*) into v_meas from fact_equipment_telemetry t
        join dim_sensor s on t.sensor_id=s.sensor_id
        where s.sensor_type_id=v_type and t.date_id between 20250101 and 20250131;

        case
            when v_cnt=0 or v_meas=0 then v_status:='Нет данных';
            when v_meas/v_cnt > 1000 then v_status:='Активно работает';
            when v_meas/v_cnt >=100 then v_status:='Нормальная работа';
            else v_status:='Редкие показания';
        end case;

        raise notice 'Тип % | Датчиков % | Показаний % | %', v_type, v_cnt, v_meas, v_status;
    end loop;
end $$;

create table report_shift_summary (
    report_date date,
    shift_name varchar(50),
    mine_name varchar(100),
    total_tons numeric(12,2),
    equipment_used int,
    efficiency numeric(5,1),
    created_at timestamp default now()
);

do $$
declare
    r record;
    v_rows int;
begin
    for r in select full_date, date_id from dim_date where date_id between 20250101 and 20250115 loop
        insert into report_shift_summary
        select
            r.full_date,
            s.shift_name,
            m.mine_name,
            sum(fp.tons_mined),
            count(distinct fp.equipment_id),
            sum(fp.operating_hours)/(count(distinct fp.equipment_id)*8)*100
        from fact_production fp
        join dim_shift s on fp.shift_id=s.shift_id
        join dim_mine m on fp.mine_id=m.mine_id
        where fp.date_id=r.date_id
        group by s.shift_name, m.mine_name;

        get diagnostics v_rows = row_count;
        raise notice 'Дата % вставлено % строк', r.full_date, v_rows;
    end loop;
end $$;

create or replace function get_quality_trend(p_year int, p_mine_id int default null)
returns table(
    month_num int,
    month_name varchar,
    samples_count bigint,
    avg_fe numeric,
    min_fe numeric,
    max_fe numeric,
    running_avg_fe numeric,
    trend varchar
)
language plpgsql
as $$
declare
    i int;
    v_prev numeric:=null;
    v_run numeric:=0;
    v_cnt int:=0;
    v_cur numeric;
begin
    for i in 1..12 loop
        select count(*), avg(fe_content), min(fe_content), max(fe_content)
        into samples_count, avg_fe, min_fe, max_fe
        from fact_ore_quality oq
        join dim_date d on oq.date_id=d.date_id
        where d.year=p_year and d.month=i
        and (p_mine_id is null or oq.mine_id=p_mine_id);

        v_cnt := v_cnt + 1;
        v_run := v_run + coalesce(avg_fe,0);
        running_avg_fe := v_run / v_cnt;
        v_cur := avg_fe;

        if v_prev is null then trend:='Стабильно';
        elsif v_cur > v_prev then trend:='Улучшение';
        elsif v_cur < v_prev then trend:='Ухудшение';
        else trend:='Стабильно';
        end if;

        v_prev := v_cur;
        month_num := i;
        month_name := to_char(make_date(p_year,i,1),'Month');

        return next;
    end loop;
end;
$$;

create or replace function validate_mes_data(p_date_from int, p_date_to int)
returns table(
    check_id int,
    check_name varchar,
    severity varchar,
    affected_rows bigint,
    details text,
    recommendation text
)
language plpgsql
as $$
begin
    return query
    select 1,'Отрицательная добыча','ОШИБКА',count(*),'<0','Проверить ввод'
    from fact_production where tons_mined<0 and date_id between p_date_from and p_date_to;

    return query
    select 2,'Слишком большая добыча','ПРЕДУПРЕЖДЕНИЕ',count(*),'>500','Проверить аномалии'
    from fact_production where tons_mined>500 and date_id between p_date_from and p_date_to;

    return query
    select 3,'Нулевые часы','ОШИБКА',count(*),'0 часов','Проверить часы'
    from fact_production where operating_hours=0 and tons_mined>0;

    return query
    select 4,'Нет добычи','ИНФО',count(*),'дни без записей','Проверить загрузку'
    from dim_date d
    where d.date_id between p_date_from and p_date_to
    and not exists(select 1 from fact_production fp where fp.date_id=d.date_id);

    return query
    select 5,'Fe вне диапазона','ОШИБКА',count(*),'0-100','Проверить лабораторию'
    from fact_ore_quality where fe_content not between 0 and 100;

    return query
    select 6,'Долгие простои','ПРЕДУПРЕЖДЕНИЕ',count(*),'>1440','Проверить корректность'
    from fact_equipment_downtime where duration_min>1440;

    return query
    select 7,'Нет телеметрии','ИНФО',count(*),'нет данных','Проверить датчики'
    from dim_equipment e
    where not exists(
        select 1 from fact_equipment_telemetry t
        where t.equipment_id=e.equipment_id
        and t.date_id between p_date_from and p_date_to
    );

    return query
    select 8,'Дубли','ОШИБКА',count(*),'повторы','Удалить дубликаты'
    from (
        select equipment_id,shift_id,date_id,count(*)
        from fact_production
        group by 1,2,3
        having count(*)>1
    ) t;
end;
$$;