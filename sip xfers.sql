/*******************
Date: 2022-03-14
Author: Tony
Data Request: 
o	Passenger Trip transfers, by Metrorail line, Metrobus route
DONE    o	Share (%) of total rider boarding with a transfer - dm transfers
     ***peak/off peak is probably a good idea, since it can be asymmetrical.***
        the transfer datamarts (not Trace, the ones that are dm_xfer�) to get the number of transfers by line. 
        You might also need to pull the total ridership by line to get the percentages.

Time Range - Oct 2019 / Sep-Oct 2021 including new service changes (9/5), excluding 7k suspension (8/17)
excluding:
  sep 1 - 5
  oct 18 - 31
*******************/


with rail_week_per as ( 
    select yearmo 
         , case 
                when period='AM Peak' then 'Peak' 
                when period='PM Peak' then 'Peak' 
                else 'Off Peak' 
                end as peak
         , b.id as station_route--MSTN ID (STATION ID)
         , sum(entry_cnt + transfer_cnt)/count(distinct dateday) as avg_boardings
    from planapi.dm_rail_ridership_v a
    left join planapi.rail_net_station_xy_v b on b.name_od = a.station
    where servicetype = 'Weekday' and Holiday = 'No' and yearmo in (201910, 202109, 202110)
    and dateday != to_date('2010-10-01', 'YYYY-MM-DD') 
    and dateday not between to_date('2021-09-01', 'YYYY-MM-DD') and to_date('2021-09-05', 'YYYY-MM-DD')
    and dateday not between to_date('2021-10-18', 'YYYY-MM-DD') and to_date('2021-10-31', 'YYYY-MM-DD')
    
    group by yearmo, servicetype, case 
                when period='AM Peak' then 'Peak' 
                when period='PM Peak' then 'Peak' 
                else 'Off Peak' end 
                , station
                , b.id
    order by yearmo, station
) 

, bus_prep as( 
    select h.date_year_month
        , h.svc_date
        , h.date_holiday
        , h.date_service_type
        , tp.period
        , h.route
        , sum(ons)/count(distinct svc_date) as avg_boardings
    from (
        select svc_date
             , date_year_month 
             , date_holiday
             , date_service_type
             , to_char(I_event_time, 'HH24') as hh24
             , place_id as reg_id
             , route_id
             , route
             , I_stop_sequence
             , place_name
             , ons
             , offs
        from planapi.trace_bus_movement_v -- TRACE BOARDING COUNTS / SERVICE DATE / YEARMO
        where date_service_type = 'Weekday' and date_holiday = 'No'  
          and svc_date between to_date('2019-10-02', 'YYYY-MM-DD') and to_date('2019-10-31', 'YYYY-MM-DD') -- routes are null on 10/1/2019
          or (svc_date between to_date('2021-09-06', 'YYYY-MM-DD') and to_date('2021-09-30', 'YYYY-MM-DD'))--NEED TO OMIT DATES BEFORE SEP SERVICE CHANGES
          or (svc_date between to_date('2021-10-01', 'YYYY-MM-DD') and to_date('2021-10-17', 'YYYY-MM-DD'))--NEED TO OMIT DATES AFTER 7K SUSPENSION
        ) h 
   
    left join  
        (select period_hour
              , period_desc as period
         from planapi.d_timeperiod_bus_hr_v) tp --SERVICE PERIODS
     on h.hh24 = tp.period_hour
     
     group by h.date_year_month
            , h.svc_date
            , h.date_holiday
            , h.date_service_type
            , tp.period
            , h.reg_id
            , h.route_id
            , h.route
            , h.I_stop_sequence
            , h.place_name
     order by date_year_month, svc_date, route_id, route, I_stop_sequence, period
) --select * from bus_prep; --141 sec

, bus_week_per as (
select date_year_month as yearmo
     , case 
            when period='AM Peak' then 'Peak' 
            when period='PM Peak' then 'Peak' 
            else 'Off Peak' 
            end as peak
     , route as station_route
     , avg_boardings 
from bus_prep
) --select * from bus_week_per;

, mode_combined as (
select * from rail_week_per
union
select * from bus_week_per
) --select * from mode_combined; --144 seconds

--------XFR'S B2B / B2R / R2B ----------
--select * from planapi.dm_xfers_b2r_v where yearmo = 201910;
--select * from planapi.dm_xfers_r2b_v where yearmo = 201910;
--select * from planapi.dm_xfers_b2b_v where yearmo = 201910;

--B2R xfers
--with b2r as (
 , b2r as (
select tfr_type 
--     , to_rail_station as tfr_to_name
     , to_char(to_mstn_id) as tfr_to
     , yearmo
     , case 
            when period_desc='AM Peak' then 'Peak' 
            when period_desc='PM Peak' then 'Peak' 
            else 'Off Peak' 
            end as peak
     , sum(trans_count)/count(distinct dateday) as avg_transfers
from planapi.dm_xfers_b2r_v
where date_day_type = 'Weekday' and date_holiday = 'No' and yearmo in (201910, 202109, 202110)
and dateday != to_date('2010-10-01', 'YYYY-MM-DD') 
and dateday not between to_date('2021-09-01', 'YYYY-MM-DD') and to_date('2021-09-05', 'YYYY-MM-DD')
and dateday not between to_date('2021-10-18', 'YYYY-MM-DD') and to_date('2021-10-31', 'YYYY-MM-DD')
group by tfr_type
     , to_rail_station
     , to_mstn_id
     , yearmo
     , case 
            when period_desc='AM Peak' then 'Peak' 
            when period_desc='PM Peak' then 'Peak' 
            else 'Off Peak' end
order by yearmo, to_mstn_id, peak desc
) --select * from b2r;--21 seconds 


--R2B xfers
-- with r2b as (
, r2b as (
select tfr_type
--     , to_bus_route_desc as tfr_to_name
     , to_char(to_route_alpha) as tfr_to
     , yearmo
     , case 
            when period_desc='AM Peak' then 'Peak' 
            when period_desc='PM Peak' then 'Peak' 
            else 'Off Peak' 
            end as peak
     , sum(trans_count)/count(distinct dateday) as avg_transfers
from planapi.dm_xfers_r2b_v
where date_day_type = 'Weekday' and date_holiday = 'No' and yearmo in (201910, 202109, 202110)
and dateday != to_date('2010-10-01', 'YYYY-MM-DD') and to_route_alpha is not null
and dateday not between to_date('2021-09-01', 'YYYY-MM-DD') and to_date('2021-09-05', 'YYYY-MM-DD')
and dateday not between to_date('2021-10-18', 'YYYY-MM-DD') and to_date('2021-10-31', 'YYYY-MM-DD')
group by tfr_type
     , to_bus_route_desc
     , to_route_alpha
     , yearmo
     , case 
            when period_desc='AM Peak' then 'Peak' 
            when period_desc='PM Peak' then 'Peak' 
            else 'Off Peak' end
order by yearmo, to_route_alpha, peak desc
) --select * from r2b;--1.4 seconds 

-- select count(distinct tfr_to) from r2b;
--alpha not null 1506
--alpha null 1700
--distinct alpha 271
--distinct bus route number 369

--B2B xfers
-- with b2b as (
, b2b as (
select tfr_type
--     , to_bus_route_desc as tfr_to_name
     , to_route_alpha as tfr_to
     , yearmo
     , case 
            when period_desc='AM Peak' then 'Peak' 
            when period_desc='PM Peak' then 'Peak' 
            else 'Off Peak' 
            end as peak
     , sum(trans_count)/count(distinct dateday) as avg_transfers
from planapi.dm_xfers_b2b_v
where date_day_type = 'Weekday' and date_holiday = 'No' and yearmo in (201910, 202109, 202110)
and to_route_alpha is not null
and dateday != to_date('2010-10-01', 'YYYY-MM-DD') 
and dateday not between to_date('2021-09-01', 'YYYY-MM-DD') and to_date('2021-09-05', 'YYYY-MM-DD')
and dateday not between to_date('2021-10-18', 'YYYY-MM-DD') and to_date('2021-10-31', 'YYYY-MM-DD')
group by tfr_type
     , to_bus_route_desc
     , to_route_alpha
     , yearmo
     , case 
            when period_desc='AM Peak' then 'Peak' 
            when period_desc='PM Peak' then 'Peak' 
            else 'Off Peak' end
order by yearmo, to_route_alpha, peak desc
) --select * from b2b;
--select count(*) from b2b where txf_to is null;
--select * from b2b; --2.4 seconds 
--count of nulls (to_route_alpha)(routeID) 1846 - 0 - 1583
--count of not nulls (to_route_alpha)(routeID) 1814 // 3660
--401 distinct route id's 
, xfer_combined as (
select * from r2b
union
select * from b2r
union
select * from b2b
) --select * from xfer_combined;

--test final combine
 select a.*
      , b.avg_boardings
      , case 
          when avg_boardings = 0 then NULL
          else round(a.avg_transfers/b.avg_boardings, 2) 
          end as tfr_share 
 from xfer_combined a
 left join mode_combined b
 on a.yearmo = b.yearmo and a.peak = b.peak and a.tfr_to = b.station_route; --483 SEC


--------------------------------------------------------------- 

/* --OLD CODE 
--select * from planapi.dm_xfers_b2r_v where yearmo = 201910;
--select * from planapi.dm_xfers_r2b_v where yearmo = 201910;
--select * from planapi.dm_xfers_b2b_v where yearmo = 201910;

--XFR'S B2B / B2R / R2B 
--B2R xfers

with b2r as (
select tfr_type 
     , to_rail_station as tfr_to_name
     , to_char(to_mstn_id) as tfr_to
     , yearmo
     , case 
            when period_desc='AM Peak' then 'Peak' 
            when period_desc='PM Peak' then 'Peak' 
            else 'Off Peak' 
            end as peak
     , sum(trans_count) as transfers
from planapi.dm_xfers_b2r_v
where date_day_type = 'Weekday' and date_holiday = 'No' and yearmo in (201910, 202109, 202110)
    and dateday not between to_date('2021-09-01', 'YYYY-MM-DD') and to_date('2021-09-05', 'YYYY-MM-DD')
    and dateday not between to_date('2021-10-18', 'YYYY-MM-DD') and to_date('2021-10-31', 'YYYY-MM-DD')
group by tfr_type
     , to_rail_station
     , to_mstn_id
     , yearmo
     , case 
            when period_desc='AM Peak' then 'Peak' 
            when period_desc='PM Peak' then 'Peak' 
            else 'Off Peak' end
order by yearmo, to_mstn_id, peak desc
) --21 seconds 

--R2B xfers
, r2b as (
select tfr_type
     , to_bus_route_desc as tfr_to_name
     , to_char(to_bus_route_number) as tfr_to
     , yearmo
     , case 
            when period_desc='AM Peak' then 'Peak' 
            when period_desc='PM Peak' then 'Peak' 
            else 'Off Peak' 
            end as peak
     , sum(trans_count) as transfers
from planapi.dm_xfers_r2b_v
where date_day_type = 'Weekday' and date_holiday = 'No' and yearmo in (201910, 202109, 202110)
    and dateday not between to_date('2021-09-01', 'YYYY-MM-DD') and to_date('2021-09-05', 'YYYY-MM-DD')
    and dateday not between to_date('2021-10-18', 'YYYY-MM-DD') and to_date('2021-10-31', 'YYYY-MM-DD')
group by tfr_type
     , to_bus_route_desc
     , to_bus_route_number
     , yearmo
     , case 
            when period_desc='AM Peak' then 'Peak' 
            when period_desc='PM Peak' then 'Peak' 
            else 'Off Peak' end
order by yearmo, to_bus_route_number, peak desc
) --1.4 seconds

--B2B xfers
--; with b2b as (
, b2b as (
select tfr_type
     , to_bus_route_desc as tfr_to_name
--     , to_bus_route_number as txf_to_route_id
     , case
            when to_route_alpha is null and to_bus_route_desc like '#%' 
            then SUBSTR(to_bus_route_desc,2,INSTR(to_bus_route_desc, '-', 1)-2)
            when to_route_alpha is null and to_bus_route_desc like 'Route #%' 
            then SUBSTR(to_bus_route_desc,8, INSTR(to_bus_route_desc, '-', 1)-8)
            when to_route_alpha is null and to_bus_route_desc like 'Route %' 
            then SUBSTR(to_bus_route_desc,INSTR(to_bus_route_desc, ' ', 1),(LENGTH(to_bus_route_desc) - INSTR(to_bus_route_desc, ' ', 1)+1))
            when to_route_alpha is null and to_bus_route_desc like '%:%' 
            then SUBSTR(to_bus_route_desc,1,INSTR(to_bus_route_desc, ':', 1)-1)
            when to_route_alpha is null and to_bus_route_desc like '%-%' 
            then SUBSTR(to_bus_route_desc,1,INSTR(to_bus_route_desc, '-', 1)-1)
            else to_route_alpha
            end as tfr_to
     , yearmo
     , case 
            when period_desc='AM Peak' then 'Peak' 
            when period_desc='PM Peak' then 'Peak' 
            else 'Off Peak' 
            end as peak
     , sum(trans_count) as transfers
from planapi.dm_xfers_b2b_v
where date_day_type = 'Weekday' and date_holiday = 'No' and yearmo in (201910, 202109, 202110)
    and dateday not between to_date('2021-09-01', 'YYYY-MM-DD') and to_date('2021-09-05', 'YYYY-MM-DD')
    and dateday not between to_date('2021-10-18', 'YYYY-MM-DD') and to_date('2021-10-31', 'YYYY-MM-DD')
group by tfr_type
     , to_bus_route_desc
--     , to_bus_route_number
     , case --over half of route alpha names are NULL in xfer mart - case conditions take from bus route description based on conditions below
            when to_route_alpha is null and to_bus_route_desc like '#%' --take name from to_bus_route_desc when starting with #
            then SUBSTR(to_bus_route_desc,2,INSTR(to_bus_route_desc, '-', 1)-2)
            when to_route_alpha is null and to_bus_route_desc like 'Route #%'  --take name from to_bus_route_desc when starting with Route #
            then SUBSTR(to_bus_route_desc,8, INSTR(to_bus_route_desc, '-', 1)-8)
            when to_route_alpha is null and to_bus_route_desc like 'Route %'  --take name from to_bus_route_desc when starting with Route 
            then SUBSTR(to_bus_route_desc,INSTR(to_bus_route_desc, ' ', 1),(LENGTH(to_bus_route_desc) - INSTR(to_bus_route_desc, ' ', 1)+1))
            when to_route_alpha is null and to_bus_route_desc like '%:%'  --take name from to_bus_route_desc when containing ':'
            then SUBSTR(to_bus_route_desc,1,INSTR(to_bus_route_desc, ':', 1)-1)
            when to_route_alpha is null and to_bus_route_desc like '%-%'  --take name from to_bus_route_desc when containing '-'
            then SUBSTR(to_bus_route_desc,1,INSTR(to_bus_route_desc, '-', 1)-1)
            else to_route_alpha end --take name from route alpha otherwise
     , yearmo
     , case 
            when period_desc='AM Peak' then 'Peak' 
            when period_desc='PM Peak' then 'Peak' 
            else 'Off Peak' end
order by yearmo, to_bus_route_desc, peak desc
) --select * from b2b;
--select count(*) from b2b where txf_to is null;
--select * from b2b; --2.4 seconds 
--count of nulls (to_route_alpha)(routeID) 1846 - 0 - 1583
--count of not nulls (to_route_alpha)(routeID) 1814 // 3660
--401 distinct route id's 
, combined as (
select * from r2b
union
select * from b2r
union
select * from b2b
) select * from combined;

--select tfr_type, count(*) from combined where txf_to is not null
--group by tfr_type;--25 seconds */