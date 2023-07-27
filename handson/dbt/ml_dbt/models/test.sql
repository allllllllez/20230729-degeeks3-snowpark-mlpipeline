select
    *
from
    {{ ref('feature') }}
where
    order_date >= '1998-01-01'::date
    and order_date <= '1998-03-31'::date
