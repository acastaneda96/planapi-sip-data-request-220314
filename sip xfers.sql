/*******************
Date: 2022-03-14
Author: Tony
Data Request: 
�	Passenger Trip transfers, by Metrorail line, Metrobus route
IN PROGRESS    o	Share (%) of total rider boarding with a transfer - dm transfers
        peak/off peak is probably a good idea, since it can be asymmetrical.
        the transfer datamarts (not Trace, the ones that are dm_xfer�) to get the number of transfers by line. 
        You might also need to pull the total ridership by line to get the percentages.

Time Range - Oct 2019 / Sep-Oct 2021 including new service changes (9/5), excluding 7k suspension (8/17)
excluding:
  sep 1 - 5
  oct 18 - 31
*******************/

with rail_week_per as ( 
    select yearmo 
         , servicetype
         , case 
                when period='AM Peak' then 'Peak' 
                when period='PM Peak' then 'Peak' 
                else 'Off Peak' 
                end as peak
         , station
         , b.id --MSTN ID (STATION ID)
         , sum(entry_cnt + transfer_cnt) as boardings
         , sum(exit_cnt) as alightings
    from planapi.dm_rail_ridership_v a
    left join planapi.rail_net_station_xy_v b on b.name_od = a.station
    where servicetype = 'Weekday' and yearmo in (201910, 202109, 202110)
    and dateday not between to_date('2021-09-01', 'YYYY-MM-DD') and to_date('2021-09-05', 'YYYY-MM-DD')
    and dateday not between to_date('2021-10-18', 'YYYY-MM-DD') and to_date('2021-10-31', 'YYYY-MM-DD')
    and Holiday = 'No'
    group by yearmo, servicetype, case 
                when period='AM Peak' then 'Peak' 
                when period='PM Peak' then 'Peak' 
                else 'Off Peak' end , station, b.id
    order by yearmo, station
) 


with bus_week_per as( 
--, bus_prep as (
    select c.yearmo
        , c.date_day_type
        , c.date_holiday
        , tp.period
        , g.lat
        , g.lon
        , h.svc_date
        , h.route_id
        , h.dest_sign_route_text
        , h.stop_sequence
--        , h.bus_id
        , sum(stop_front_door_entry + stop_back_door_entry) as boardings
        , sum(stop_front_door_exit + stop_back_door_exit) as alightings
--        , count(distinct(svc_date)) as day_count
    from (
    select svc_date
         , event_dtm
         , to_char(event_dtm, 'HH24') as hh24
         , bus_id
         , route_id
         , dest_sign_route_text
         , stop_sequence
         , stop_front_door_entry
         , stop_back_door_entry
         , stop_front_door_exit
         , stop_back_door_exit
    from planapi.bus_state_pax_load_hmbrw_l -- for boarding counts / service date / yearmo
    where svc_date between to_date('2019-10-01', 'YYYY-MM-DD') and to_date('2019-10-31', 'YYYY-MM-DD')
      or (svc_date between to_date('2021-09-01', 'YYYY-MM-DD') and to_date('2021-09-30', 'YYYY-MM-DD'))
      or (svc_date between to_date('2021-10-01', 'YYYY-MM-DD') and to_date('2021-10-31', 'YYYY-MM-DD'))
    ) h 
    
    left join
    (select yearmo
        , dateday
        , date_day_type
        , date_holiday
    from planapi.d_date_bus_v -- for date period / service type
    where yearmo in (201910, 202109, 202110)
    ) c on c.dateday = to_date(h.svc_date)
   
    left join  
    (select period_hour
          , period_desc as period
     from planapi.d_timeperiod_bus_hr_v) tp -- for service periods
     on h.hh24 = tp.period_hour
     
     left join 
     (select route_id
           , stop_sequence
           , lat
           , lon
      from planapi.bus_state_stop_sequence_v) g
      on h.route_id = g.route_id and h.stop_sequence = g.stop_sequence
    
     group by c.yearmo
            , c.date_day_type
            , c.date_holiday
            , tp.period
            , g.lat
            , g.lon
            , h.svc_date
            , h.route_id
            , h.dest_sign_route_text
            , h.stop_sequence
    order by yearmo, svc_date, route_id, period
) 

, bus_week_per as (
select yearmo
     , date_day_type
     , case 
            when period='AM Peak' then 'Peak' 
            when period='PM Peak' then 'Peak' 
            else 'Off Peak' 
            end as peak
     , route_id
     , dest_sign_route_text
     , stop_sequence
     , lat 
     , lon
     , boardings
     , alightings 
from bus_prep
where date_holiday = 'No' and date_day_type = 'Weekday' 
and svc_date not between to_date('2021-09-01', 'YYYY-MM-DD') and to_date('2021-09-05', 'YYYY-MM-DD')
and svc_date not between to_date('2021-10-18', 'YYYY-MM-DD') and to_date('2021-10-31', 'YYYY-MM-DD')
) -- IT WORKED! 249 SECONDS -- well it worked before i added in the stop_sequence join, we'll see how it goes now


--select * from planapi.dm_xfers_b2r_v where yearmo = 201910;
--select * from planapi.dm_xfers_r2b_v where yearmo = 201910;
--select * from planapi.dm_xfers_b2b_v where yearmo = 201910;

--XFR'S B2B / B2R / R2B 
--B2R xfers
--with b2r as (
, b2r as (
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

----select * from b2r;
--select a.*
--     , b.boardings
--     , round(sum(a.transfers/b.boardings),8) as transfer_share
--from b2r a
--left join rail_week_per b on a.yearmo = b.yearmo 
--                       and a.tfr_to = b.id
--                       and a.peak = b.peak
--group by a.tfr_type 
--     , a.tfr_to_name
--     , a.tfr_to
--     , a.yearmo 
--     , a.peak
--     , a.transfers
--     , b.boardings
--;--45 sec
;
select * from planapi.dm_xfers_r2b_v
where date_day_type = 'Weekday' and date_holiday = 'No' and yearmo in (201910) ;

--R2B xfers
with r2b as (
--, r2b as (
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

select count(distinct tfr_to) from r2b;
--alpha not null 1506
--alpha null 1700
--distinct alpha 271
--distinct bus route number 369

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
--group by tfr_type;--25 seconds
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