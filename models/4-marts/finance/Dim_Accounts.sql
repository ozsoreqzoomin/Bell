SELECT
    a.Account_ID,
    a.Account_Name,
    a.Account_Type,
    a.Industry,
    a.Account_Creation_Date,
    a.Account_Timezone,
    a.Account_Territory,
    a.Active_ARR,
    a.Actual_ICU,
    a.Churn_Risk,
    a.Account_CSM,
    a.Account_Manager,
    arr.SNAPSHOT_DATE AS FirstSubscriptionDate
 FROM
    {{ref('base_Accounts')}} a
 LEFT JOIN
 {{ref('stg_MonthlyARR')}} arr
 ON
  a.Account_ID = arr.Account_ID AND arr.ARR_Change_Type = 'New'