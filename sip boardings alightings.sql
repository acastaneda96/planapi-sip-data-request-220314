/*******************
Date: 2022-03-14
Author: Tony
Data Request: 

•	To determine Jobs accessible via 30 minutes on transit, the following to identify appropriate origin nodes - dm for rail, hmbrw for bus
DONE    o	Metrorail passenger boardings, weekday AM peak, weekday PM peak, by station
IN PROGRESS    o	Metrobus passenger boardings, weekday AM peak, weekday PM peak, by stop
DONE    o	GIS references (such as lat/long) for the above Metrorail station and Metrobus stops - ?

Time Range - Oct 2019 / Sep-Oct 2021 including new service changes (9/5), excluding 7k suspension (8/17)
excluding:
  sep 1 - 5
  oct 18 - 31
*******************/
--XY COORDINATES / IDS AND NAMES FOR BUS AND RAIL
select * from planapi.gis_bus_stop_v; 

select * from planapi.gis_rail_station_v;


with rail_week_per as ( 
    select yearmo 
         , servicetype
         , period
         , station
         , b.id --MSTN ID (STATION ID)
         , sum(entry_cnt + transfer_cnt) as boardings
         , sum(exit_cnt) as alightings
    from planapi.dm_rail_ridership_v a
    left join planapi.rail_net_station_xy_v b on b.name_od = a.station
    where servicetype = 'Weekday' and yearmo in (201910, 202109, 202110)
    and dateday not between to_date('2021-09-01', 'YYYY-MM-DD') and to_date('2021-09-05', 'YYYY-MM-DD') --OMITTING DATES BEFORE SEP SERVICE CHANGES
    and dateday not between to_date('2021-10-18', 'YYYY-MM-DD') and to_date('2021-10-31', 'YYYY-MM-DD') --OMITTING DATES AFTER 7K SUSPENSION
    and period in ('AM Peak', 'PM Peak') and Holiday = 'No'
    group by yearmo, servicetype, period, station, b.id
    order by yearmo, period
) select * from rail_week_per;


--select * from planapi.bus_state_stop_sequence_v;
select * from planapi.bus_state_pax_load_hmbrw_l 
where svc_date = to_date('2021-09-06', 'YYYY-MM-DD');
select * from planapi.d_timeperiod_bus_hr_v;

--    HMBRW VS SCHED_STOP_SEQUENCE  --
--    route_id = pattern_id
--    route_id_substr = route
--    stop_sequence = stop_sequence

with bus_week_per as( 
    select c.yearmo
        , c.date_day_type
        , c.date_holiday
        , tp.period
        , g.reg_id
        , g.geostopid 
        , h.svc_date
        , h.route_id
        , h.route_id_substr
        , h.stop_sequence
        , sum(stop_front_door_entry + stop_back_door_entry) as boardings
        , sum(stop_front_door_exit + stop_back_door_exit) as alightings
    from (
        select svc_date
             , event_dtm
             , to_char(event_dtm, 'HH24') as hh24
             , bus_id
             , route_id
             , route_id_substr
             , stop_sequence
             , stop_front_door_entry
             , stop_back_door_entry
             , stop_front_door_exit
             , stop_back_door_exit
        from planapi.bus_state_pax_load_hmbrw_l -- HMBRW boarding counts / service date / yearmo
        where svc_date between to_date('2019-10-01', 'YYYY-MM-DD') and to_date('2019-10-31', 'YYYY-MM-DD')
          or (svc_date between to_date('2021-09-01', 'YYYY-MM-DD') and to_date('2021-09-30', 'YYYY-MM-DD'))
          or (svc_date between to_date('2021-10-01', 'YYYY-MM-DD') and to_date('2021-10-31', 'YYYY-MM-DD'))
        ) h 
    
    left join
        (select yearmo
            , dateday
            , date_day_type
            , date_holiday
         from planapi.d_date_bus_v -- BUS DAY TYPE / HOLIDAYS
         where yearmo in (201910, 202109, 202110)
         ) c on c.dateday = to_date(h.svc_date)
   
    left join  
        (select period_hour
              , period_desc as period
         from planapi.d_timeperiod_bus_hr_v) tp --SERVICE PERIODS
     on h.hh24 = tp.period_hour
     
    left join 
        (select pattern_id
               , route
               , stop_sequence
               , reg_id
               , geostopid
         from planapi.bus_sched_stop_sequence_v) g --IDS FOR BUS
     on h.route_id = g.pattern_id and h.route_id_substr = g.route and h.stop_sequence = g.stop_sequence
     
     group by c.yearmo
            , c.date_day_type
            , c.date_holiday
            , tp.period
            , g.reg_id
            , g.geostopid 
            , h.svc_date
            , h.route_id
            , h.route_id_substr
            , h.stop_sequence
     order by yearmo, svc_date, route_id, route_id_substr, stop_sequence, period
) 
;

--Check for the correct version type for 201910 / 2021 09 / 10
--Time Range - Oct 2019 / Sep-Oct 2021 including new service changes (9/5), excluding 7k suspension (8/17)
--excluding:
--  sep 1 - 5
--  oct 18 - 31
select * from planapi.bus_sched_stop_sequence_v where versionid in (69,70,85) order by versionid desc ; --86 is the newest version
select versionid, versionname, version_start_date, version_end_date from planapi.bus_sched_version_v 
where versionid in (69,70,85);
--Version 69 = 201910  10/01-10/19
--Version 70 = 201910 10/20-10/31
--Version 85 = 202109 and 202110!!!!!!!!!




select yearmo
     , date_day_type
     , period
     , reg_id 
     , geostopid
     , route_id
     , route_id_substr
     , stop_sequence
     , boardings
     , alightings 
from bus_week_per
where date_holiday = 'No' and date_day_type = 'Weekday' and period in ('AM Peak', 'PM Peak')
and svc_date not between to_date('2021-09-01', 'YYYY-MM-DD') and to_date('2021-09-05', 'YYYY-MM-DD')
and svc_date not between to_date('2021-10-18', 'YYYY-MM-DD') and to_date('2021-10-31', 'YYYY-MM-DD')
; --IT WORKED! 249 SECONDS -- well it worked before i added in the stop_sequence join, we'll see how it goes now
-- NEW QUERY 1955 SEC - 32 MIN




