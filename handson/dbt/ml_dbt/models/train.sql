select
    *
from
    {{ ref('feature') }}
where
    order_date <= '1997-12-31'::date
