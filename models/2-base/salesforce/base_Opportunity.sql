SELECT 
    ID as Opportunity_ID,
    Name as Opportunity_Name,
    Type,
    ACCOUNTID as Account_ID,
    CLOSEDATE as Close_Date,
    STAGENAME as Stage_Name,
    CREATEDDATE as Create_Date,
    Probability,
    TOTAL_ARR__C as Total_ARR,
    KICKOFF_STATUS__C as Kickoff_Status,
    SUBSCRIPTION_STATUS__C as Subscription_Status,
    SUBSCRIPTION_START_DATE__C as Subscription_Start_Date,
    SUBSCRIPTION_END_DATE__C as Subscription_End_Date
FROM
    {{source('SALESFORCE', 'src_Opportunity')}}