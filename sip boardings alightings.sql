/*******************
Date: 2022-03-14
Author: Tony
Data Request: 

To determine Jobs accessible via 30 minutes on transit, the following to identify appropriate origin nodes - dm for rail, hmbrw for bus
DONE    o	Metrorail passenger boardings, weekday AM peak, weekday PM peak, by station (avg weekday)
DONE    o	Metrobus passenger boardings, weekday AM peak, weekday PM peak, by stop (avg weekday)
DONE    o	GIS references (such as lat/long) for the above Metrorail station and Metrobus stops - gis%

Time Range - Oct 2019 / Sep-Oct 2021 including new service changes (9/5), excluding 7k suspension (8/17)
excluding:
  sep 1 - 5
  oct 18 - 31
*******************/
--XY COORDINATES / IDS AND NAMES FOR BUS AND RAIL
-- select * from planapi.gis_bus_stop_v; 
-- select * from planapi.gis_rail_station_v;


with rail_week_per as ( 
    select yearmo 
         , servicetype
         , period
         , station
         , b.id --MSTN ID (STATION ID)
         , round(sum(entry_cnt + transfer_cnt)/count(distinct dateday),2) as avg_boardings
         , round(sum(exit_cnt)/count(distinct dateday),2) as avg_alightings
--         , count(distinct dateday) as day_count
    from planapi.dm_rail_ridership_v a
    left join planapi.rail_net_station_xy_v b on b.name_od = a.station
    where servicetype = 'Weekday' and period in ('AM Peak', 'PM Peak') and Holiday = 'No' and yearmo in (201910, 202109, 202110)
    and dateday != to_date('2019-10-01', 'YYYY-MM-DD') --OMITTING 10/1 to compare with bus, bus has null values on this day
    and dateday not between to_date('2021-09-01', 'YYYY-MM-DD') and to_date('2021-09-05', 'YYYY-MM-DD') --OMITTING DATES BEFORE SEP SERVICE CHANGES
    and dateday not between to_date('2021-10-18', 'YYYY-MM-DD') and to_date('2021-10-31', 'YYYY-MM-DD') --OMITTING DATES AFTER 7K SUSPENSION
    group by yearmo, servicetype, period, station, b.id
    order by yearmo, period
) select * from rail_week_per; --12 sec


--select * from planprod.hmbrw_pax_load
--where svc_date = to_date('2021-09-06', 'YYYY-MM-DD');
--select * from planapi.d_timeperiod_bus_hr_v;
--select * from planprod.hmbrw_bus_sched_stop_times;

--    HMBRW VS SCHED_STOP_SEQUENCE  --
--    route_id = pattern_id
--    route_id_substr = route
--    stop_sequence = stop_sequence

with bus_week_per as( 
    select h.date_year_month
        , h.svc_date
        , h.date_holiday
        , h.date_service_type
        , tp.period
        , h.reg_id
        , h.route_id
        , h.route
        , h.I_stop_sequence
        , h.place_name
        , sum(ons)/count(distinct svc_date) as avg_boardings
        , sum(offs)/count(distinct svc_date) as avg_alightings
        , count(distinct svc_date) as day_count
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
) --select * from bus_week_per; --141 sec

select date_year_month
     , date_service_type
     , period
     , reg_id 
     , route_id
     , route
     , I_stop_sequence
     , place_name
     , avg_boardings 
     , avg_alightings
from bus_week_per where period in ('AM Peak', 'PM Peak') ; -- 82 seconds / previous hmbrw queries 249 SECONDS no reg_id / 1955 SEC - 32 MIN w/ reg_id




