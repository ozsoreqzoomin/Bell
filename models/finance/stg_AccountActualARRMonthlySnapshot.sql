with Opportunity as (
    select * from {{ source('SALESFORCE2', 'Opportunity') }}
),

Dates as (
    select * from {{ source('SALESFORCE2', 'Dim_Date') }}
),

Accounts as (
    select * from {{ source('SALESFORCE2', 'Account') }}
),

Account_ARR_MonthlySnapshots as (        
 
select Date, account_id, Sum(Total_ARR_C) Current_ARR,
Case when LEAD(Sum(Total_ARR_C)) over (Partition by account_id order by Date desc) is NULL then Sum(Total_ARR_C) else Sum(Total_ARR_C)-LEAD(Sum(Total_ARR_C)) over (Partition by account_id order by Date desc) end as ARR_Change,
  RANK() OVER (PARTITION BY account_id ORDER BY Date DESC) AS DESCRank

from
 (select D.Date,account_id,SUBSCRIPTION_STATUS_C  AccountStatus,SUBSCRIPTION_END_DATE_C  End_Date, SUBSCRIPTION_START_DATE_C  Start_Date, Close_Date, Stage_Name, Total_ARR_C,type 
  from Opportunity o 
  left join
  (select distinct DATE from Dates where DAY(DATE) = 1
    and DATE > '2015-01-01' AND DATE < current_date()) D
    on D.Date < o.SUBSCRIPTION_END_DATE_C AND D.DATE > o.SUBSCRIPTION_START_DATE_C
  where 
  --account_id in ('0012p0000357h44AAA') and 
  stage_name in ('Closed Won')) a
  where Date is not NULL
  group by 1,2 order by Date desc
  )
  
  select ams.Date, a.ID, a.Name ,
  CASE WHEN ams.DESCRANK = 1 and a.TYPE = 'Former Customer' then 0 else CURRENT_ARR end as CURRENT_ARR,
  CASE WHEN ams.DESCRANK = 1 and a.TYPE = 'Former Customer' then -(CURRENT_ARR) else ARR_CHANGE end as ARRCHANGE,
  CASE WHEN ARRCHANGE = CURRENT_ARR AND ARRCHANGE > 0 then 'New'
  WHEN -ARRCHANGE = CURRENT_ARR AND ARRCHANGE < 0 then 'Churn'
  WHEN ARRCHANGE > 0 then 'Expansion'
  WHEN ARRCHANGE < 0 then 'Downgrade'
  ELSE 'No change' END AS ARR_Change_Type, 
 a.Type, ams.DESCRANK
  from Account_ARR_MonthlySnapshots ams
  join Accounts a on a.ID = ams.Account_ID
  where Date IS NOT null