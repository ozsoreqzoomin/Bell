select a.*, arr.Date as FirstSubscriptionDate from {{ ref('base_Accounts')}} a
  join
 {{ ref('stg_MonthlyARR')}} arr
  on
  a.ID = arr.account_id AND arr.ARR_Change_Type = 'New'