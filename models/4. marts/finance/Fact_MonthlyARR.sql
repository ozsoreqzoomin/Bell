with ARRMonthlyChangesFlat as (
  
select *, Active_ARR + Pending_ARR as Actual_ARR,
CASE WHEN LEAD(Actual_ARR) over (Partition by Account_ID order by Date desc) is NULL then 0 else LEAD(Actual_ARR) over (Partition by Account_ID order by Date desc) end as Previous_ARR,
CASE WHEN Previous_ARR = 0 then Actual_ARR else Actual_ARR - Previous_ARR end as ARR_Change
  from 
  
  (
select coalesce(aam.date, pam.date) Date, coalesce(aam.account_id, pam.account_id) Account_ID,
case when Active_ARR is NULL then 0 else Active_ARR end as Active_ARR,
case when Pending_ARR is NULL then 0 else Pending_ARR end as Pending_ARR
from {{ ref('stg_ActiveARRMonthly')}}  aam full outer join {{ ref('stg_PendingARRMonthly')}} pam
on aam.date = pam.date AND aam.account_id = pam.account_id 
  )
)

select a.Name, a.Type as Customer_Type, ARRMonthlyChangesSteps.Date, ARRMonthlyChangesSteps.Account_ID, ARRMonthlyChangesSteps.ARR,
CASE WHEN Customer_Type = 'Former Customer' AND RANK() OVER (PARTITION BY account_id ORDER BY Date DESC) = 1 then 'Churn'
WHEN RANK() OVER (PARTITION BY account_id ORDER BY Date ASC) = 1 then 'New'
WHEN ARR < 0 then 'Downgrade'
WHEN ARR > 0 AND ARRMonthlyChangesSteps.Type = 'Not mapped' then 'Expansion' 
ELSE ARRMonthlyChangesSteps.Type end as Type
from 
(
  select Date, Account_ID, Actual_ARR - ARR_Change - Pending_ARR as ARR, 'Flat ARR' as Type from ARRMonthlyChangesFlat
  union all
  select Date, Account_ID, Pending_ARR, 'Pending' as Type from ARRMonthlyChangesFlat
  union all
  select Date, Account_ID, ARR_Change, 'Not mapped' as Type from ARRMonthlyChangesFlat 
) ARRMonthlyChangesSteps 
join
{{ref('base_Accounts')}} a
on ARRMonthlyChangesSteps.account_id = a.ID
 where ARR != 0 --AND Name LIKE '%Service%'
 
