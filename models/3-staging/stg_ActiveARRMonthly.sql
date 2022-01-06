SELECT
    Date, 
    Account_ID,
    SUM(Total_ARR) as Active_ARR
FROM
 (
   SELECT
   D.Date,
   Account_ID,
   o.Opportunity_ID,
   Subscription_Status,
   Subscription_End_Date, 
   Subscription_Start_Date,
   Close_Date,
   Stage_Name,
   Total_ARR,
   Type 
FROM 
  {{ref('base_Opportunity')}} o 
LEFT JOIN  
   (
    SELECT
    DISTINCT DATE 
    FROM 
    {{ref('base_Dates')}}
    WHERE DAY(DATE) = 1
   ) D
ON 
  D.Date <= o.Subscription_End_Date AND D.DATE >= o.Subscription_Start_Date
  WHERE 
  --account_id in ('0012p0000357h44AAA') and 
  Stage_Name IN ('Closed Won')) a
  WHERE
  Date IS NOT NULL
  GROUP BY 1,2 ORDER BY Date DESC