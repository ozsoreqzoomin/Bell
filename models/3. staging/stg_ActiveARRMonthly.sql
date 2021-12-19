select Date, account_id, Sum(Total_ARR_C) Active_ARR

from
 (select D.Date,account_id,o.ID,SUBSCRIPTION_STATUS_C  AccountStatus,SUBSCRIPTION_END_DATE_C  End_Date, SUBSCRIPTION_START_DATE_C  Start_Date, Close_Date, Stage_Name, Total_ARR_C,type 
  from {{ ref('base_Opportunity')}} o 
  left join
  (select distinct DATE from {{ ref('base_Dates')}} where DAY(DATE) = 1
    and DATE > '2015-01-01' AND DATE < current_date()) D
    on D.Date <= o.SUBSCRIPTION_END_DATE_C AND D.DATE >= o.SUBSCRIPTION_START_DATE_C
  where 
  --account_id in ('0012p0000357h44AAA') and 
  stage_name in ('Closed Won')) a
  where Date is not NULL
  group by 1,2 order by Date desc