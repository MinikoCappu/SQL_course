begin;

insert into fact_production (
    date_id, shift_id, mine_id, equipment_id,
    operator_id, location_id, ore_grade_id,
    tons_mined, tons_transported, trips_count,
    distance_km, fuel_consumed_l, operating_hours
)
values
(20250310,1,1,1,1,1,1,100,95,5,10,20,6),
(20250310,1,1,2,1,1,1,110,100,6,11,22,7),
(20250310,1,1,3,1,1,1,120,110,7,12,24,8),
(20250310,1,1,4,1,1,1,130,120,8,13,26,9),
(20250310,1,1,5,1,1,1,140,130,9,14,28,10);

select * from fact_production where date_id=20250310 and shift_id=1;

commit;

select * from fact_production where date_id=20250310 and shift_id=1;

begin;

insert into fact_production (
    date_id, shift_id, mine_id, equipment_id,
    operator_id, location_id, ore_grade_id,
    tons_mined, tons_transported, trips_count,
    distance_km, fuel_consumed_l, operating_hours
)
values
(20250310,2,1,1,1,1,1,100,95,5,10,20,6),
(20250310,2,1,2,1,1,1,110,100,6,11,22,7),
(20250310,2,1,3,1,1,1,120,110,7,12,24,8),
(20250310,2,1,4,1,1,1,130,120,8,13,26,9),
(20250310,2,1,5,1,1,1,140,130,9,14,28,10);

rollback;

select * from fact_production where date_id=20250310 and shift_id=2;

begin;

insert into fact_production (
    date_id, shift_id, mine_id, equipment_id,
    operator_id, location_id, ore_grade_id,
    tons_mined, tons_transported, trips_count,
    distance_km, fuel_consumed_l, operating_hours
)
values (20250311,1,1,1,1,1,1,100,95,5,10,20,6);

savepoint sp_after_production;

insert into fact_ore_quality (
    date_id, mine_id, shaft_id, sample_number, fe_content
)
values (20250311,1,1,'S1',60);

savepoint sp_after_quality;

insert into fact_equipment_telemetry (
    equipment_id, sensor_id, date_id, time_id, sensor_value
)
values (1,999999,20250311,1,10);

rollback to sp_after_quality;

commit;

select * from fact_production where date_id=20250311;
select * from fact_ore_quality where date_id=20250311;
select * from fact_equipment_telemetry where date_id=20250311;

create table equipment_balance (
    equipment_id int primary key,
    balance_tons numeric default 0,
    check (balance_tons >= 0)
);

insert into equipment_balance values (1,1000),(2,500);

begin;

update equipment_balance
set balance_tons = balance_tons - 200
where equipment_id = 1;

update equipment_balance
set balance_tons = balance_tons + 200
where equipment_id = 2;

commit;

select * from equipment_balance;

begin;

update equipment_balance
set balance_tons = balance_tons - 1500
where equipment_id = 2;

update equipment_balance
set balance_tons = balance_tons + 1500
where equipment_id = 1;

commit;

select * from equipment_balance;