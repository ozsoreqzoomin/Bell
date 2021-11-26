with ActualARR as (
 select * from {{ ref('stg_AccountActualARRMonthlySnapshot')}} where DESCRANK = 1 and Type = 'Customer'
),
  
Date as (
  select Date from SALESFORCE2.Dim_Date where Date < current_date() and Day(Date) = 1
),

final as (
select d.Date, a.ID, a.Name, a.CURRENT_ARR, a.ARRCHANGE, 'Pending' as ARR_CHANGE_TYPE, a.Type, a.DESCRANK from Date d join
ActualARR a
on d.Date > a.Date
)
  select * from final