 
with MaxPendingOpp as (
  
  select D.Date,account_id,o.ID,SUBSCRIPTION_STATUS_C as SUBSCRIPTION_STATUS,SUBSCRIPTION_END_DATE_C  End_Date, SUBSCRIPTION_START_DATE_C  Start_Date, Close_Date, Stage_Name, Total_ARR_C,type, RANK() OVER (PARTITION BY account_id ORDER BY Date DESC) AS DESCRank 
  from {{ ref('base_Opportunity')}} o 
  left join
  (select distinct DATE from {{ ref('base_Dates')}} where DAY(DATE) = 1
    and DATE > '2015-01-01' AND DATE < current_date()) D
    on D.Date < o.SUBSCRIPTION_END_DATE_C AND D.DATE > o.SUBSCRIPTION_START_DATE_C
  where 
  --account_id in ('001D000001jVeZ9IAK') and 
  stage_name in ('Closed Won') and SUBSCRIPTION_STATUS_C = 'Expired - Pending Renewal'

),
  
d as (
  select Date from {{ ref('base_Dates')}} where Date < current_date() and Day(Date) = 1
),
  
final as (
select d.date, MaxPendingOpp.Account_ID, SUBSCRIPTION_STATUS, Total_ARR_C as Pending_ARR, Type, Stage_Name  from d
join
(select * from MaxPendingOpp where DESCRANK = 1) MaxPendingOpp
on
MaxPendingOpp.Date < d.Date
)

select date, Account_ID, sum(Pending_ARR) as Pending_ARR from final
group by 1,2
