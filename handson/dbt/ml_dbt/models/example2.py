from snowflake.snowpark.functions import sum, avg, count, col, dateadd, lit, to_date


def model(dbt, session):
    dbt.config(materialized="table")
    item = dbt.source(
        'tpc_h', 'lineitem'
    ).filter(
        col("l_shipdate") <= dateadd("day", lit(-90), to_date(lit("1998-12-01")))
    ).group_by([
        "l_returnflag",
        "l_linestatus",
    ]).agg(
        sum("l_quantity").name("sum_qty"),
        sum("l_extendedprice").name("sum_base_price"),
        sum(col("l_extendedprice") * (1 - col("l_discount"))).name("sum_disc_price"),
        sum(col("l_extendedprice") * (1 - col("l_discount")) * (1 + col("l_tax"))).name("sum_charge"),
        avg("l_quantity").name("avg_qty"),
        avg("l_extendedprice").name("avg_price"),
        avg("l_discount").name("avg_disc"),
        count("*").name("count_order"),
    )

    return item
