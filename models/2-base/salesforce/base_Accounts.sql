SELECT 
    ID as Account_ID,
    NAME as Account_Name,
    TYPE as Account_Type,
    INDUSTRY as Industry,
    CREATEDDATE as Account_Creation_Date,
    TIMEZONE__C as Account_Timezone,
    TERRITORY__C as Account_Territory,
    ACTIVE_ARR__C as Active_ARR,
    ACTUAL_ICU__C as Actual_ICU,
    CHURN_RISK__C as Churn_Risk,
    ACCOUNT_CSM__C as Account_CSM,
    ACCOUNT_MANAGER__C as Account_Manager
FROM
    {{source('SALESFORCE', 'src_Account')}}
