-- First Step is to Get Day Wise Record Count for Each IP
-- Then apply a ranking function assigning rank on basis of the count
-- Finally select the top 5 for each day using rank

with day_ip_cnt as (
    select date,ip,count(1) as hits
    from demo.logs
    group by date,ip
),
ranked_day_ip_cnt as (
    select date,ip,hits,
    row_number() over (partition by date order by hits desc) as rn
    from day_ip_cnt
)
select date,ip,hits from ranked_day_ip_cnt
where rn<=5
order by date desc,hits desc;