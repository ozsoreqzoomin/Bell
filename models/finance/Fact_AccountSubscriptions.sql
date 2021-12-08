with ActualARRMonthly as (
  
select *, Active_ARR + Pending_ARR as Actual_ARR,
CASE WHEN LEAD(Actual_ARR) over (Partition by Account_ID order by Date desc) is NULL then 0 else LEAD(Actual_ARR) over (Partition by Account_ID order by Date desc) end as Previous_ARR,
CASE WHEN Previous_ARR = 0 then Actual_ARR else Actual_ARR - Previous_ARR end as ARR_Change,
RANK() OVER (PARTITION BY account_id ORDER BY Date DESC) AS DESCRank from 
  
  (
select coalesce(aam.date, pam.date) Date, coalesce(aam.account_id, pam.account_id) Account_ID,
case when Active_ARR is NULL then 0 else Active_ARR end as Active_ARR,
case when Pending_ARR is NULL then 0 else Pending_ARR end as Pending_ARR
from {{ ref('stg_ActiveARRMonthly')}} aam full outer join {{ ref('stg_PendingARRMonthly')}} pam
on aam.date = pam.date AND aam.account_id = pam.account_id 
  )
  
)

select arm.Date, arm.Account_ID, 
CASE WHEN arm.DESCRANK = 1 and a.TYPE = 'Former Customer' then 0 else Active_ARR end as Active_ARR,
arm.Pending_ARR, arm.Previous_ARR, a.Name, a.Type,
CASE WHEN arm.DESCRANK = 1 and a.TYPE = 'Former Customer' then 0 else Actual_ARR end as Actual_ARR,
  CASE WHEN arm.DESCRANK = 1 and a.TYPE = 'Former Customer' then -(Actual_ARR) else ARR_CHANGE end as ARR_CHANGE,
  CASE WHEN ARR_CHANGE = Actual_ARR AND ARR_CHANGE > 0 then 'New'
  WHEN arm.DESCRANK = 1 and a.TYPE = 'Former Customer' then 'Churn'
  WHEN ARR_CHANGE > 0 then 'Expansion'
  WHEN ARR_CHANGE < 0 then 'Downgrade'
  ELSE 'No change' END AS ARR_Change_Type,
  arm.DESCRANK
  from ActualARRMonthly arm join {{ ref('base_Accounts')}} a
on arm.account_id = a.ID
--where Name like '%Master%'
