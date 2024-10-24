-- total 4 stocks:TSM, TSLA, NVDA, MSFT

-- 1.time series analysis
-- daily return by using windows function： LAG（Lead）
SELECT
    name,
    date,
    closing_price,
    LAG(closing_price, 1) OVER (PARTITION BY name ORDER BY date) AS prev_closing_price,
    round((closing_price-(LAG(closing_price, 1) OVER (PARTITION BY name ORDER BY date)))/(LAG(closing_price, 1) OVER (PARTITION BY name ORDER BY date)) * 100,4) as daily_return
FROM
    stock_data
order by name,date

-- 2.calculating moving average + sell-hold-buy advice

select 
    name,
    date,
    closing_price,
    avg(closing_price) over (partition by name order by date rows 9 preceding) as moving_10_avg,
    avg(closing_price) over (partition by name order by date rows 49 preceding ) as moving_50_avg
from
    stock_data
order by name, date;


SELECT 
    name,
    date,
    opening_price,
    closing_price,
    AVG(closing_price) OVER (PARTITION BY name ORDER BY date ROWS 9 PRECEDING) as moving_10_avg,
    CASE 
        WHEN closing_price > AVG(closing_price) OVER (PARTITION BY name ORDER BY date ROWS 9 PRECEDING) THEN 'Buy'
        WHEN closing_price < AVG(closing_price) OVER (PARTITION BY name ORDER BY date ROWS 9 PRECEDING) THEN 'Sell'
        ELSE 'Hold'
    END AS advice
FROM stock_data;


-- 3. high and low using join
SELECT 
    a.name,
    a.date,
    CASE 
        WHEN a.closing_price = b.oneyear_high THEN b.oneyear_high
        ELSE NULL
    END AS highest_price,
    CASE 
        WHEN a.closing_price = b.oneyear_low THEN b.oneyear_low
        ELSE NULL
    END AS lowest_price
FROM stock_data AS a
JOIN 
(
    SELECT
       name,
       MAX(closing_price) AS oneyear_high,
       MIN(closing_price) AS oneyear_low
    FROM
        stock_data
    GROUP BY
        name
) AS b
ON a.name = b.name 
   AND (a.closing_price = b.oneyear_high OR a.closing_price = b.oneyear_low);
    
    
    
-- 4.Determine if the threshold has been exceeded， like 'circuit breaker'

with tmp1 as
(
SELECT
    name,
    date,
    LAG(closing_price, 1) OVER (PARTITION BY name ORDER BY date) AS prev_closing_price,
    highest_price,
    lowest_price
FROM
    stock_data
),
tmp2 as
(
select 
	name,
	date,
	round((highest_price-prev_closing_price)/prev_closing_price * 100,2) as Maximum_increase,
	round((lowest_price-prev_closing_price)/prev_closing_price * 100,2) as Maximum_decrease
from tmp1
order by name,date
)
select 
		*,
		case when Maximum_decrease < -5 then 'Red Flag_Warining_decrease' 
			when Maximum_increase >5 then 'Red Flag_Warining_increase' 
            else 'NA' 
            end as Note
from tmp2
order by Note desc,name,date;



-- 5.looking at the volumn in general 
with tmp1 as
(
SELECT 
    name,
    date,
    CASE 
        WHEN volume LIKE '%M' THEN CAST(LEFT(volume, LENGTH(volume) - 1) AS DECIMAL) * 1000000
        WHEN volume LIKE '%B' THEN CAST(LEFT(volume, LENGTH(volume) - 1) AS DECIMAL) * 1000000000
        ELSE CAST(volume AS DECIMAL)
    END AS volume_in_numeric
FROM stock_data
order by name,date
)
select 
		name,
		max(volume_in_numeric) as max_volume,
		min(volume_in_numeric) as min_volume,
        max(volume_in_numeric)/min(volume_in_numeric) as multiples
from tmp1
group by name


-- 6.looking ate the volumn by month
with tmp1 as 
(
SELECT 
    name,
    date,
    concat(year(date), '-',month(date)) as dates,
    CASE 
        WHEN volume LIKE '%M' THEN CAST(LEFT(volume, LENGTH(volume) - 1) AS DECIMAL) * 1000000
        WHEN volume LIKE '%B' THEN CAST(LEFT(volume, LENGTH(volume) - 1) AS DECIMAL) * 1000000000
        ELSE CAST(volume AS DECIMAL)
    END AS volume_in_numeric
FROM stock_data
order by name,dates
)
select 
dates,name,min(volume_in_numeric)as min_volume,max(volume_in_numeric)as max_volume,max(volume_in_numeric)/min(volume_in_numeric) as multiples
from tmp1
group by name,dates
order by name,dates

-- 7.trend analysis
-- mark 'incresing' or 'decreasing'
select
		name,
		date,
        opening_price,
        closing_price,
		case when closing_price > opening_price then '1'
			 when closing_price < opening_price then '-1'
			 else '0'
		end as signal_status
from stock_data
order by name,date



-- 通过这种方式，new_group_mark 会为每个不同的连续信号区间打上不同的标记,标记信号变化为 1（即开始一个新的连续区间）
-- Find the longest consecutive days for each stock in its three states

with tmp1 as
(
select
		name,
		date,
        opening_price,
        closing_price,
		case when closing_price > opening_price then '1'
			 when closing_price < opening_price then '-1'
			 else '0'
		end as signal_status
from stock_data
order by name,date
),
tmp2 as
(
select 
		*, 
		LAG(signal_status) OVER (PARTITION BY name ORDER BY date) AS prev_signal
from
tmp1
),
tmp3 as
(
select  *,  
      CASE WHEN signal_status <> prev_signal THEN 1 ELSE 0 END as if_signal_change,
      sum(CASE WHEN signal_status <> prev_signal THEN 1 ELSE 0 END ) over(partition by name order by date) as new_group_mark
from tmp2
),
tmp4 as
(
select name,signal_status,new_group_mark,min(date) as mindate,max(date)as maxdate,count(*) as counts
from tmp3
group by name,signal_status,new_group_mark
)
select 
	 name,signal_status,max(counts)
from tmp4
group by name,signal_status

-- above,just find the max counts in each state, and if this stock has several date ranges with same day counts, no display 



-- below, also find the start+end date (full infomation)
-- with dense_rank, the result will show different date ranges fully even it has same day counts


with tmp1 as
(
select
		name,
		date,
        opening_price,
        closing_price,
		case when closing_price > opening_price then '1'
			 when closing_price < opening_price then '-1'
			 else '0'
		end as signal_status
from stock_data
order by name,date
),
tmp2 as
(
select 
		*, 
		 LAG(signal_status) OVER (PARTITION BY name ORDER BY date) AS prev_signal
from
tmp1
),
tmp3 as
(
select  *,  
      CASE WHEN signal_status <> prev_signal THEN 1 ELSE 0 END as if_signal_change,
      sum(CASE WHEN signal_status <> prev_signal THEN 1 ELSE 0 END ) over(partition by name order by date) as new_group_mark
from tmp2
),
tmp4 as
(
select name,signal_status,new_group_mark,min(date) as mindate,max(date)as maxdate,count(*) as day_counts        -- after new_group_mark,then find the start and end date
from tmp3                                                                                                   -- 找到起始日期也很重要，再加上统计数counts 
group by name,signal_status,new_group_mark
),
tmp5 as
(
select *,dense_rank ()over(partition by name,signal_status order by day_counts desc) as ranking               -- do not use row_numer fuction
from tmp4
)
select name,signal_status,mindate,maxdate,day_counts
from tmp5
where ranking =1



-- 8. volatility
SELECT 
    name,
    STDDEV(closing_price) AS volatility
FROM stock_data
GROUP BY name

-- 9.rsi ：Relative Strength Index

WITH returns AS (
    SELECT 
        name,
	    closing_price - LAG(closing_price) OVER (PARTITION BY name ORDER BY date) AS daily_return
    FROM stock_data
),
RSI as
(
SELECT 
    name,
    100 - (100 / (1 + AVG(CASE WHEN daily_return > 0 THEN daily_return ELSE 0 END) / 
                      AVG(CASE WHEN daily_return < 0 THEN -daily_return ELSE 0 END))) AS rsi
FROM returns
GROUP BY name
)
select
*, case when rsi between 30 and 70 then ' neutral' else null end as mark
from RSI
