create database news_db;
use news_db;
select * from dim_ad_category;
select * from fact_print_sales;
select * from dim_city;
set sql_safe_updates=0;
alter table dim_city rename column ï»¿city_id to City_ID;
alter table fact_print_sales rename column `Copies Sold` to copies_sold;
alter table fact_print_sales rename column ï»¿edition_ID to edition_id;

/*Business Request 1: Monthly Circulation Drop Check
You need month-over-month difference in net_circulation*/

with cte as(select city_id,month,net_circulation,
lag(net_circulation,1) over (partition by city_id order by month asc) as prevmonth_net,
lead(net_circulation,1) over(partition by city_id order by month desc) as nextmonth_net
from fact_print_sales)
select a.net_circulation,(a.net_circulation-a.prevmonth_net) as momdiff,b.city as city,a.month as month
from cte as a join dim_city as b on a.city_id=b.city_id
where a.prevmonth_net is not null
order by momdiff asc
limit 3;



select * from dim_ad_category;
select * from fact_ad_revenue;
set sql_safe_updates=0;
ALTER TABLE fact_ad_revenue
ADD COLUMN year_col INT,
ADD COLUMN quarter_number INT;
update fact_ad_revenue
set year_col=regexp_substr(quarter,'[0-9]{4}'),
quarter_number= case 
when quarter regexp "(Q1)|(1st Qtr)" then 1
when quarter regexp "(Q2)|(2nd Qtr)" then 2
when quarter regexp "(Q3)|(3rd Qtr)" then 3
when quarter regexp "(Q4)|(4th Qtr)" then 4
end;
/*Business Request 2: Yearly Revenue Concentration by Category 
•	Think: Which categories form >50% of yearly ad revenue?*/ 

with revpercat as(select round(sum(a.ad_revenue),0) as cat_rev,b.category_group as category_group,a.year_col as year_col
from fact_ad_revenue as a join  dim_ad_category as b on a.ad_category=b.ad_category
group by b.category_group,a.year_col),
yearlyrev as(select round(sum(ad_revenue),0) as year_rev,year_col
from fact_ad_revenue
group by year_col)
select round(a.cat_rev/b.year_rev *100,2) as percent,a.category_group as category,b.year_col as year,a.cat_rev as category_revenue,b.year_rev as yearly_revenue
from revpercat as a join yearlyrev as b on a.year_col=b.year_col
where a.cat_rev > 0.3 * b.year_rev
order by year,percent desc;

/*Business Request 3: 2024 Print Efficiency Leaderboard
Think: Efficiency = net_circulation / copies_printed.*/
select * from fact_print_sales;

ALTER TABLE fact_print_sales
ADD COLUMN year_col INT,
ADD COLUMN copies_printed INT;
update fact_print_sales
set year_col=case 
when right(Month,length(month)-instr(month,"-"))="19" then 2019
when right(Month,length(month)-instr(month,"-"))="20" then 2020
when right(Month,length(month)-instr(month,"-"))="21" then 2021
when right(Month,length(month)-instr(month,"-"))="22" then 2022
when right(Month,length(month)-instr(month,"-"))="23" then 2023
when right(Month,length(month)-instr(month,"-"))="24" then 2024
end,
copies_printed= (copies_sold + copies_returned) ;

with efficiency as (select sum(net_circulation)*1.0/sum(copies_printed) as efficiency,city_id
from fact_print_sales
where year_col=2024
group by city_id),
ranked as(select city_id, efficiency,rank()over(order by efficiency  desc) as rnk
from efficiency)
select a.city_id,a.rnk,b.city
from ranked as a inner join dim_city as b on a.City_ID=b.City_ID
where rnk<=5;
/*Business Request 4: Internet Readiness Growth (2021)
Think: Compare Q1 (Jan–Mar) vs. Q4 (Oct–Dec).*/
select* from dim_city;
select* from ranked;
select * from fact_city_readiness;
select regexp_substr(quarter,'[0-9]{4}') as year
from fact_city_readiness;
 select case 
when quarter regexp "(Q1)" then "Q1"
when quarter regexp "(Q2)" then "Q2"
when quarter regexp "(Q3)" then "Q3"
when quarter regexp "(Q4)" then "Q4"
end as quarter_no
from fact_city_readiness;
set sql_safe_updates=0;
ALTER TABLE fact_city_readiness
ADD COLUMN year_col INT,
ADD COLUMN quarter_number INT;
update fact_city_readiness
set year_col=regexp_substr(quarter,'[0-9]{4}'),
quarter_number= case 
when quarter regexp "(Q1)" then 1
when quarter regexp "(Q2)" then 2
when quarter regexp "(Q3)" then 3
when quarter regexp "(Q4)" then 4
end;

select round(avg(case when a.quarter_number=1 then a.internet_penetration end)-
avg (case when a.quarter_number=4 then a.internet_penetration end),2)as diff,b.city as cityname
from fact_city_readiness as a join dim_city as b on a.city_id=b.city_id
where a.year_col=2021
group by b.city
order by diff asc;

/*Business Request 5: Consistent Multi-Year Decline (2019–2024)
Think: Strictly decreasing sequence of both net_circulation and ad_revenue.*/
select * from fact_ad_revenue;
select * from fact_print_sales;

with yearly as (
    select 
        fs.city_id,
        fs.year_col,
        sum(fs.net_circulation) as yearly_net_circulation,
        sum(ar.ad_revenue) as yearly_ad_revenue
    from fact_print_sales as fs
    join fact_ad_revenue as ar 
         on fs.edition_id=ar.edition_id and fs.year_col = ar.year_col
    where fs.year_col between 2019 and 2024
    group by fs.city_id, fs.year_col
),
lagging as(select y.*,lag(y.yearly_net_circulation) over( partition by y.city_id order by y.year_col) as prev_circ ,
lag(y.yearly_ad_revenue) over( partition by y.city_id order by y.year_col) as prev_rev
from yearly as y)

select a.city_id, b.city,min(a.yearly_net_circulation) as yearly_net_circulation ,min(a.yearly_ad_revenue) as yearly_ad_revenue, 
case when sum(case when a.yearly_net_circulation < a.prev_circ then 1 else 0 end)=count(*)-1 
then 'yes' else 'No' end as is_declining_print,
case when sum(case when a.yearly_ad_revenue < a.prev_rev then 1 else 0 end)=count(*)-1 
then 'yes' else 'No' end as is_declining_rev,
 case when (sum(case when a.yearly_net_circulation < a.prev_circ then 1 else 0 end)=count(*)-1) 
and (sum(case when a.yearly_ad_revenue < a.prev_rev then 1 else 0 end)=count(*)-1 )
then 'yes' else 'No' end as is_declining_both
from lagging as a join dim_city as b on a.city_id=b.city_id
group by a.city_id,b.city;

/*Business Request 6: Readiness vs. Pilot Engagement Outlier
Think: Highest readiness score but among bottom 3 engagement.*/
select * from fact_city_readiness;
select * from fact_digital_pilot;
set sql_safe_updates=0;
ALTER TABLE fact_digital_pilot
ADD COLUMN year_col INT;
update fact_digital_pilot
set year_col=regexp_substr(launch_month,'[0-9]{4}');
with readiness as (select round(avg(literacy_rate+smartphone_penetration+internet_penetration)/3,2) as readiness_score,city_id
from fact_city_readiness
where year_col=2021
group by city_id),
engagement as( select sum(a.downloads_or_accesses) as engage_metric,b.city as city_name,a.city_id
from fact_digital_pilot as a join dim_city as b on a.city_id=b.city_id
where a.year_col=2021
group by b.city,a.city_id)
select c.city as city_name,r.readiness_score,e.engage_metric,
rank() over ( order by r.readiness_score desc) as rnk_readiness,
rank()over (order by e.engage_metric asc) as rnk_engage,
case when rank() over ( order by r.readiness_score desc)=1 and rank()over (order by e.engage_metric asc)<=3
then "yes" else "no"
end as outlier
from readiness r 
join engagement e on r.city_id=e.city_id
join dim_city c on e.city_id=c.city_id;


    



