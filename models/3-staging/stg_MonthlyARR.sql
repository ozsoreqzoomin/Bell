WITH ARRMonthlyChangesFlat AS (
SELECT
  Date,
  Account_ID,
  Active_ARR,
  Pending_ARR,
  Active_ARR + Pending_ARR as Actual_ARR,
  CASE WHEN LEAD(Actual_ARR) over (Partition by Account_ID order by Date desc) is NULL then 0 else LEAD(Actual_ARR) over (Partition by Account_ID order by Date desc) end as Previous_ARR,
  CASE WHEN Previous_ARR = 0 then Actual_ARR else Actual_ARR - Previous_ARR end as ARR_Change
  FROM 
    (
      SELECT
      COALESCE(aam.Date, pam.Date) Date,
      COALESCE(aam.Account_ID, pam.Account_ID) Account_ID,
      CASE WHEN Active_ARR IS NULL THEN 0 ELSE Active_ARR END AS Active_ARR,
      CASE WHEN Pending_ARR IS NULL THEN 0 ELSE Pending_ARR END AS Pending_ARR
      FROM
      {{ref('stg_ActiveARRMonthly')}} aam
      FULL OUTER JOIN
      {{ref('stg_PendingARRMonthly')}} pam
      ON
      aam.Date = pam.Date AND aam.Account_ID = pam.Account_ID 
    )
),

Churns as (
SELECT
  Account_Name, Account_Type, Date, Account_ID, ARR, ARR_Change_Type from
  (
      SELECT
      a.Account_Name,
      a.Account_Type,
      ARRMonthlyChangesFlat.Date,
      ARRMonthlyChangesFlat.Account_ID,
      -(Actual_ARR + Pending_ARR) ARR,
      'Churn' AS ARR_Change_Type,
      RANK() OVER (PARTITION BY ARRMonthlyChangesFlat.Account_ID ORDER BY Date DESC) as DescRank
      FROM
      ARRMonthlyChangesFlat
      JOIN
      {{ref('base_Accounts')}} a
      ON
      ARRMonthlyChangesFlat.Account_ID = a.Account_ID
      WHERE
      Account_Type = 'Former Customer'
    )
  WHERE DescRank = 1
)

SELECT 
a.Account_Name,
a.Account_Type,
ARRMonthlyChangesSteps.Date Snapshot_Date,
ARRMonthlyChangesSteps.Account_ID,
ARRMonthlyChangesSteps.ARR,
CASE WHEN RANK() OVER (PARTITION BY ARRMonthlyChangesSteps.Account_ID ORDER BY Date ASC) = 1 THEN 'New'
     WHEN ARR < 0 then 'Downgrade'
     WHEN ARR > 0 AND ARRMonthlyChangesSteps.Type = 'Not mapped' THEN 'Expansion' 
     ELSE ARRMonthlyChangesSteps.Type END AS ARR_Change_Type
FROM 
(
  SELECT Date, Account_ID, Actual_ARR - ARR_Change - Pending_ARR AS ARR, 'Flat ARR' AS Type FROM ARRMonthlyChangesFlat
  UNION ALL
  SELECT Date, Account_ID, Pending_ARR, 'Pending' AS Type FROM ARRMonthlyChangesFlat
  UNION ALL
  SELECT Date, Account_ID, ARR_Change, 'Not mapped' AS Type FROM ARRMonthlyChangesFlat 
) ARRMonthlyChangesSteps 
JOIN
{{ref('base_Accounts')}} a
ON
ARRMonthlyChangesSteps.Account_ID = a.Account_ID
WHERE
ARR != 0 
UNION ALL
SELECT
      Account_Name,
      Account_Type,
      Date,
      Account_ID,
      ARR,
      ARR_Change_Type
FROM
Churns

 -- where Customer_Type = 'Former Customer'
 
 
