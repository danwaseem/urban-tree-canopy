-- Custom generic test: assert all non-null values are in [min_value, max_value].
-- Usage in schema.yml:
--   tests:
--     - value_between:
--         min_value: -1
--         max_value: 1
{% test value_between(model, column_name, min_value, max_value) %}
select *
from {{ model }}
where {{ column_name }} is not null
  and (
    {{ column_name }} < {{ min_value }}
    or {{ column_name }} > {{ max_value }}
  )
{% endtest %}


-- Custom generic test: assert all values are strictly greater than zero.
-- Usage in schema.yml:
--   tests:
--     - positive_value
{% test positive_value(model, column_name) %}
select *
from {{ model }}
where {{ column_name }} is not null
  and {{ column_name }} <= 0
{% endtest %}
