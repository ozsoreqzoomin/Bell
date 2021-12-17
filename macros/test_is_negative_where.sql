{% macro test_is_negative_where(model, column_name, condition) %}

with validation_errors as (

    select {{column_name}}
    from {{model}}
    where {{column_name}} > 0 AND {{condition}}

)

select *
from validation_errors

{% endmacro %}