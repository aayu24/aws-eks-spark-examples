-- First Step is to Get Day Wise Record Count for Each Device
-- Then apply a ranking function assigning rank on basis of the count
-- Finally select the top 5 for each day

with day_device_cnt as (
    select date,device,count(1) as hits 
    from demo.logs
    group by date,device
),
ranked_day_device_cnt as (
    select date,device,hits,
    row_number() over (partition by date order by hits desc) as rn
    from day_device_cnt
)
select date,device,hits from ranked_day_device_cnt
where rn<=5
order by date desc,hits desc;