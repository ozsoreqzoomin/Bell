SELECT
  DateNum as Numeric_Date,
  Date
FROM
    {{source('STATIC_FILES', 'src_Dates')}}
WHERE
  DATE >= '2013-01-01' and DATE <= CURRENT_DATE()