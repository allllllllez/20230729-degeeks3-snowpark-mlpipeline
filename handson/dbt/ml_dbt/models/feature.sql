with orders as(
    select
        *
    from
        {{ source('tpc_h', 'orders') }}
), lineitem as (
    select
        *
    from
        {{ source('tpc_h', 'lineitem') }}
)
select
    orders.o_orderdate as order_date,
    sum(l_extendedprice) as sum_base_price,  -- 合計価格
    avg(sum_base_price) over (
        order by order_date asc 
        rows between 2 preceding and current row) as moving_3day_sum_base_price_avg, -- 合計金額の3日間移動平均
    avg(sum_base_price) over (
        order by order_date asc 
        rows between 6 preceding and current row) as moving_week_sum_base_price_avg, -- 合計金額の7日間移動平均
    sum(l_extendedprice * (1-l_discount)) as sum_disc_price,  -- 割引合計価格
    sum(l_extendedprice * (1-l_discount) * (1+l_tax)) as sum_charge, -- 割引合計価格+税金
    sum(l_quantity) as sum_qty, -- 合計数量
    avg(sum_qty) over (
        order by order_date asc 
        rows between 2 preceding and current row) as moving_3day_sum_qty_avg, -- 合計数量の3日間移動平均
    avg(sum_qty) over (
        order by order_date asc 
        rows between 6 preceding and current row) as moving_week_sum_qty_avg, -- 合計数量の7日間移動平均
    avg(l_quantity) as avg_qty,  -- 平均数量
    avg(l_extendedprice) as avg_price,  -- 平均合計価格
    avg(l_discount) as avg_disc,  -- 平均割引の合計
    count(distinct l_orderkey) as count_order  -- 注文件数
from
    lineitem
inner join 
    orders 
    on orders.o_orderkey = lineitem.l_orderkey
group by order_date
order by order_date asc
