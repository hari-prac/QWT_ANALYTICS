
def model(dbt,session):
    customer_df = dbt.source("QWT_RAW","raw_customers")
    return customer_df
