-- First Step is to Get Week Wise Record Count for Each IP
-- Then apply a ranking function assigning rank on basis of the count
-- Finally select the top 5 for each week using rank

with week_ip_cnt as (
    select DATE_SUB(DATE(timestamp), (DAYOFWEEK(timestamp) - 1)) as week_date,ip,count(1) as hits
    from demo.logs
    group by DATE_SUB(DATE(timestamp), (DAYOFWEEK(timestamp) - 1)),ip
),
ranked_week_ip_cnt as (
    select week_date,ip,hits,
    row_number() over (partition by week_date order by hits desc) as rn
    from week_ip_cnt
)
select week_date,ip,hits from ranked_week_ip_cnt
where rn<=5
order by week_date desc,hits desc;