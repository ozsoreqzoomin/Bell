with ActualARRMonthly as (
    select DATE, ID, NAME, CURRENT_ARR, ARRCHANGE, ARR_CHANGE_TYPE, TYPE from {{ ref('stg_AccountActualARRMonthlySnapshot')}}
),
  
PendingARRMonthly as (
    select DATE, ID, NAME, CURRENT_ARR, ARRCHANGE, ARR_CHANGE_TYPE, TYPE from {{ ref('stg_AccountPendingARRMonthlySnapshot')}}
),
  
final as (
    Select *, RANK() OVER (PARTITION BY ID ORDER BY Date DESC) AS DESCRank from
  (select * from ActualARRMonthly union select * from PendingARRMonthly)
)

Select * from final 