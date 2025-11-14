import snowflake.snowpark.functions as F


def model(dbt, session):

    dbt.config(materialized="incremental",unique_key = 'OrderID')

    orders_df = dbt.source("QWT_RAW", "raw_orders")

    if dbt.is_incremental:

        max_order_date = f"select max(orderdate) from {dbt.this}"

        orders_df = orders_df.filter(
            orders_df.orderdate > session.sql(max_order_date).collect()[0][0]
        )
    return orders_df
