analyze fact_production;

select
    attname,
    n_distinct,
    correlation,
    null_frac,
    most_common_vals[1:5]
from pg_stats
where tablename = 'fact_production'
order by attname;

create index idx_prod_date_ff100 on fact_production(date_id) with (fillfactor = 100);
create index idx_prod_date_ff90  on fact_production(date_id) with (fillfactor = 90);
create index idx_prod_date_ff70  on fact_production(date_id) with (fillfactor = 70);
create index idx_prod_date_ff50  on fact_production(date_id) with (fillfactor = 50);

select
    indexname,
    pg_size_pretty(pg_relation_size(indexname::regclass)),
    pg_relation_size(indexname::regclass)
from pg_indexes
where indexname like 'idx_prod_date_ff%'
order by pg_relation_size(indexname::regclass);

drop index if exists idx_prod_date_ff100;
drop index if exists idx_prod_date_ff90;
drop index if exists idx_prod_date_ff70;
drop index if exists idx_prod_date_ff50;

select
    attname,
    attstattarget
from pg_attribute
where attrelid = 'fact_production'::regclass
  and attnum > 0
  and not attisdropped
order by attnum;

explain analyze
select *
from fact_production
where mine_id = 1
  and shaft_id = 1
  and date_id between 20240101 and 20240131;

alter table fact_production alter column mine_id set statistics 1000;
alter table fact_production alter column shaft_id set statistics 1000;
alter table fact_production alter column date_id set statistics 1000;

analyze fact_production;

create statistics stat_prod_mine_shaft (dependencies, ndistinct)
    on mine_id, shaft_id from fact_production;

analyze fact_production;

explain analyze
select *
from fact_production
where mine_id = 1
  and shaft_id = 1
  and date_id between 20240101 and 20240131;

select
    stxname,
    stxkeys,
    stxkind
from pg_statistic_ext
where stxname = 'stat_prod_mine_shaft';

create index idx_prod_equip_date_v1 on fact_production(equipment_id, date_id);
create index idx_prod_equip_date_v2 on fact_production(equipment_id, date_id);
create index idx_prod_equip_only on fact_production(equipment_id);

select
    a.indexrelid::regclass,
    b.indexrelid::regclass
from pg_index a
join pg_index b
  on a.indrelid = b.indrelid
 and a.indexrelid < b.indexrelid
 and a.indkey = b.indkey;

select
    pg_size_pretty(sum(pg_relation_size(b.indexrelid)))
from pg_index a
join pg_index b
  on a.indrelid = b.indrelid
 and a.indexrelid < b.indexrelid
 and a.indkey = b.indkey;

drop index if exists idx_prod_equip_date_v1;
drop index if exists idx_prod_equip_date_v2;
drop index if exists idx_prod_equip_only;

select
    schemaname || '.' || relname,
    indexrelname,
    idx_scan,
    idx_tup_read,
    pg_size_pretty(pg_relation_size(indexrelid)),
    pg_relation_size(indexrelid)
from pg_stat_user_indexes
where idx_scan = 0
  and schemaname = 'public'
order by pg_relation_size(indexrelid) desc;

select
    pg_size_pretty(sum(pg_relation_size(indexrelid))),
    count(*)
from pg_stat_user_indexes
where idx_scan = 0
  and schemaname = 'public';

select
    stats_reset
from pg_stat_bgwriter;

create index idx_prod_bloat_test on fact_production(equipment_id, date_id);

select pg_size_pretty(pg_relation_size('idx_prod_bloat_test'));

update fact_production
set equipment_id = equipment_id
where date_id between 20240101 and 20240115;

update fact_production
set equipment_id = equipment_id
where date_id between 20240116 and 20240131;

select pg_size_pretty(pg_relation_size('idx_prod_bloat_test'));

reindex index idx_prod_bloat_test;

reindex index concurrently idx_prod_bloat_test;

drop index if exists idx_prod_bloat_test;

explain analyze
select date_id, equipment_id, tons_mined
from fact_production
where date_id = 20240315;

create index idx_prod_equip_date_covering
on fact_production(equipment_id, date_id)
include (tons_mined, trips_count, operating_hours);

vacuum fact_production;

explain analyze
select date_id,
       sum(tons_mined),
       sum(trips_count),
       sum(operating_hours)
from fact_production
where equipment_id = 5
  and date_id between 20240101 and 20240331
group by date_id;

drop index if exists idx_prod_equip_date_covering;

create index idx_oee_prod
on fact_production(date_id, equipment_id);

create index idx_oee_downtime
on fact_equipment_downtime(date_id, equipment_id);

create index idx_equip_status
on dim_equipment(status);

vacuum fact_production;
vacuum fact_equipment_downtime;
vacuum dim_equipment;

drop index if exists idx_oee_prod;
drop index if exists idx_oee_downtime;
drop index if exists idx_equip_status;

create index idx_q1_prod_mine_date
on fact_production(mine_id, date_id);

create index idx_q2_downtime_equip
on fact_equipment_downtime(equipment_id, date_id);

create index idx_q5_downtime_unplanned
on fact_equipment_downtime(date_id)
where is_planned = false;

create index idx_q3_telemetry_alarm
on fact_equipment_telemetry(date_id)
where is_alarm = true;

create index idx_q4_ore_mine_date
on fact_ore_quality(mine_id, date_id);

vacuum fact_production;
vacuum fact_equipment_downtime;
vacuum fact_equipment_telemetry;
vacuum fact_ore_quality;

drop index if exists idx_q1_prod_mine_date;
drop index if exists idx_q2_downtime_equip;
drop index if exists idx_q5_downtime_unplanned;
drop index if exists idx_q3_telemetry_alarm;
drop index if exists idx_q4_ore_mine_date;

select
    relname,
    pg_size_pretty(pg_relation_size(relid)),
    pg_size_pretty(pg_total_relation_size(relid) - pg_relation_size(relid)),
    round(
        (pg_total_relation_size(relid) - pg_relation_size(relid))::numeric /
        nullif(pg_relation_size(relid), 0) * 100, 1
    )
from pg_catalog.pg_statio_user_tables
where schemaname = 'public'
  and relname like 'fact_%'
order by pg_relation_size(relid) desc;