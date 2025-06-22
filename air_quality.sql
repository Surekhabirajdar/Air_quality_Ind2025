select * from air_quality_2025;
select distinct pollutant_id  as Pollutants from air_quality_2025;
desc air_quality_2025;

select pollutant_id , avg(pollutant_avg) as Pollutant_Average from air_quality_2025 group by pollutant_id order by Pollutant_Average ;

select a.state,a.pollutant_id,a.pollutant_avg, 'max' as type
from air_quality_2025 a JOIN (select state, max(pollutant_avg) as max_val from air_quality_2025 group by state)
b on a.state=b.state and a.pollutant_avg=b.max_val
union 
select a.state,a.pollutant_id,a.pollutant_avg, 'min' as type
from air_quality_2025 a JOIN (select state, min(pollutant_avg) as min_val from air_quality_2025 group by state)
b on a.state=b.state and a.pollutant_avg=b.min_val
order by state,type;

select state ,pollutant_id,pollutant_avg,station from air_quality_2025 where state='Andhra_Pradesh' and pollutant_id='NH3';

select state,avg(pollutant_avg) as avg_pollution from air_quality_2025 group by state
having round(avg(pollutant_avg),2)=(select max(state_avg) from (select round(avg(pollutant_avg),2) as state_avg from air_quality_2025 group by state)
as max_table)
or round(avg(pollutant_avg),2)=(select min(state_avg) from(select round(avg(pollutant_avg),2) as state_avg from air_quality_2025 group by state)
as min_table);

select  max(state_avg),min(state_avg) from (select Round(Avg(pollutant_avg),2) as state_avg from air_quality_2025 group by state) as test_tabel;

select state,avg(pollutant_avg) from air_quality_2025 group by state;

select state,avg(pollutant_avg) as avg_pollution from air_quality_2025 group by state
having avg(pollutant_avg)=(select max(state_avg) from (select avg(pollutant_avg) as state_avg from air_quality_2025 group by state)
as max_table)
or avg(pollutant_avg)=(select min(state_avg) from(select avg(pollutant_avg) as state_avg from air_quality_2025 group by state)
as min_table);
/*this only returns only one value because min or max doesnt recognize float value as we didnt round it.
it will first take avg value and then max of it , it isnt same as floating value so it doesnt return anything..2.908765 not eq to=2.9*/

select state, count(distinct(station)) from air_quality_2025 group by state;

select state ,count(station) as station_no from air_quality_2025 group by state 
having count(station)=(select max(station_count) from( select count(station) as station_count from air_quality_2025 group by state) as Station_table);

select state,city, pollutant_id,count(*) as occurence from air_quality_2025 group by state,city,pollutant_id
having count(*)=(select max(pollutant_count) from (select state s, city c , count(pollutant_id) as pollutant_count from air_quality_2025
group by state,city,pollutant_id)as sub
where sub.c=air_quality_2025.city and sub.s=air_quality_2025.state);

with Rankedpollutants as (select state,city,pollutant_id,count(*) as occurence,
row_number() over (partition by state,city order by count(*) desc) as Ranks
from air_quality_2025 group by state,city,pollutant_id)
select state,city,pollutant_id from Rankedpollutants where Ranks=1;

select pollutant_id,pollutant_avg from air_quality_2025 order by pollutant_avg desc limit 1;

select pollutant_id,avg(pollutant_avg) as average from air_quality_2025  group by pollutant_id order by average desc limit 1; 

select city,pollutant_id,avg(pollutant_avg) as average_val from air_quality_2025 group by city,pollutant_id order by average_val desc ;

with pollutantaverages as (select city,pollutant_id,avg(pollutant_avg) as pol_avg from air_quality_2025 
where pollutant_id in ('PM10','PM2.5','NO2','OZONE','CO','NH3','SO2') group by city,pollutant_id),
ranked as (select * ,row_number() over (partition by pollutant_id order by pol_avg desc) as rnk
from pollutantaverages)
select city,pollutant_id,pol_avg from ranked where rnk=1;

select pollutant_id,state,min(pollutant_avg),max(pollutant_avg),avg(pollutant_avg) from air_quality_2025 group by pollutant_id,state
order by pollutant_id,state;

select state,city,pollutant_id ,count(*) as exceedance from air_quality_2025 where 
(pollutant_id='PM2.5' AND pollutant_avg > 60) OR
    (pollutant_id = 'PM10' AND pollutant_avg > 100) OR
    (pollutant_id = 'NO2' AND pollutant_avg > 80) OR
    (pollutant_id = 'SO2' AND pollutant_avg > 50)
group by state,city,pollutant_id order by pollutant_id;

select station,pollutant_id,avg(pollutant_avg) as avg_count from air_quality_2025 group by station,pollutant_id order by avg_count desc;

alter table air_quality_2025 add column region varchar(50);
SET SQL_SAFE_UPDATES = 0;
UPDATE air_quality_2025
SET region = CASE
    WHEN state IN ('Delhi', 'Punjab', 'Haryana', 'Uttar_Pradesh', 'Rajasthan','Uttarakhand','Chandigarh', 'Jammu_and_Kashmir','Himachal Pradesh') THEN 'North'
    WHEN state IN ('TamilNadu', 'Kerala', 'Karnataka', 'Telangana', 'Andhra_Pradesh','Puducherry') THEN 'South'
    WHEN state IN ('West_Bengal', 'Odisha', 'Bihar', 'Jharkhand','Sikkim') THEN 'East'
    WHEN state IN ('Maharashtra', 'Gujarat', 'Goa') THEN 'West'
    WHEN state IN ('Madhya Pradesh', 'Chhattisgarh') THEN 'Central'
    WHEN state IN ('Assam', 'Manipur', 'Meghalaya', 'Nagaland','Tripura','Mizoram') THEN 'Northeast'
    WHEN state IN ('Andaman and Nicobar') THEN 'Islands'
    ELSE 'Unknown'
END;
SET SQL_SAFE_UPDATES = 1;

select * from air_quality_2025;
select distinct state from air_quality_2025;
alter table air_quality_2025 drop column region;

select region,max(pollutant_avg) as avg_value from air_quality_2025 group by region order by avg_value desc;

select state,city,avg(pollutant_avg) as avg_pollution from air_quality_2025 group by state,city order by avg_pollution asc limit 10;

select station,max(pollutant_avg) as max_pol from air_quality_2025 group by station order by max_pol desc limit 1;

select station,max(pollutant_avg),min(pollutant_avg),max(pollutant_avg)-min(pollutant_avg) as Rang from air_quality_2025 group by station order by Rang desc;

select station,avg(pollutant_avg) as low_pol, stddev(pollutant_avg) as std_dev from air_quality_2025 group by station order by std_dev asc;

select distinct last_update from air_quality_2025;

select state,city,avg(pollutant_avg) as avg_val from air_quality_2025 group by state,city order by avg_val desc limit 10;