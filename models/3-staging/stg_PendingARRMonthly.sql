WITH MaxPendingOpp AS ( 
  SELECT
  D.Date,
  O.Account_ID,
  O.Opportunity_ID,
  O.Subscription_Status,
  O.Subscription_End_Date,
  O.Subscription_Start_Date,
  O.Close_Date,
  O.Stage_Name,
  O.Total_ARR,
  O.Type,
  RANK() OVER (PARTITION BY Account_ID ORDER BY Date DESC) AS DESCRank 
  FROM
  {{ref('base_Opportunity')}} O
  LEFT JOIN
  (
    SELECT
    DISTINCT DATE
    FROM
    {{ref('base_Dates')}} 
    WHERE
    DAY(DATE) = 1
    ) D
    ON
    D.Date < O.Subscription_End_Date AND D.Date > O.Subscription_Start_Date
  WHERE 
  --account_id in ('001D000001jVeZ9IAK') and 
  Stage_Name in ('Closed Won') and Subscription_Status = 'Expired - Pending Renewal'
),
  
D AS (
  SELECT
  Date 
  FROM
  {{ref('base_Dates')}} 
  WHERE
  Date < CURRENT_DATE() AND Day(Date) = 1
),
  
final AS (
SELECT
D.Date,
MaxPendingOpp.Account_ID,
MaxPendingOpp.Subscription_Status, 
MaxPendingOpp.Total_ARR as Pending_ARR,
MaxPendingOpp.Type,
MaxPendingOpp.Stage_Name 
FROM D
JOIN
(
  SELECT 
  Date,
  Account_ID,
  Opportunity_ID,
  Subscription_Status,
  Subscription_End_Date,
  Subscription_Start_Date,
  Close_Date,
  Stage_Name,
  Total_ARR,
  Type,
  DESCRank 
  FROM
  MaxPendingOpp 
  WHERE 
  DESCRANK = 1
) MaxPendingOpp
ON
MaxPendingOpp.Date < D.Date
)

SELECT
Date,
Account_ID,
SUM(Pending_ARR) as Pending_ARR
FROM
final
GROUP BY 1,2
