SELECT a.Account_Name,
    Account_Type,
    Snapshot_Date,
    Account_ID,
    ARR,
    ARR_Change_Type
FROM 
    {{ref('stg_MonthlyARR')}}