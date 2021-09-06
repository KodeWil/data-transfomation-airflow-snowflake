from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow import DAG
from datetime import datetime
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
import os
SNOWFLAKE_CONN_ID = 'snowflake_conn'


sql_query = """
    Create or replace table avg_last_fees_venue_events as 
    SELECT "city", venues."venue_name", "avg_fee_last_events"
    FROM venues
    JOIN 
        (Select "venue_id", "avg_fee_last_events", max("event_time")
                from (select "venue_id","event_time", avg("fee.amount") over (
                partition by "venue_id"
                ORDER BY "event_time"
                ROWS BETWEEN  3 PRECEDING AND CURRENT ROW 
                ) "avg_fee_last_events"
        from events_table
        order by "venue_id", "event_time")
        group by "venue_id", "avg_fee_last_events") avg_fee_last_events_by_venue
    ON venues."venue_id" = avg_fee_last_events_by_venue."venue_id"
    having "avg_fee_last_events" > 0
    ORDER BY "city", "avg_fee_last_events" DESC; 
    """


default_args = {
    'start_date': datetime(2020, 1, 1)
}

with DAG('avg_last_fees_venue_events', schedule_interval='@daily', default_args=default_args, catchup=False) as dag:
    
    snowflake_op_sql_list = SnowflakeOperator(
        task_id='snowflake_op_sql', dag=dag, snowflake_conn_id=SNOWFLAKE_CONN_ID, sql=sql_query
    )

    slack = SlackWebhookOperator(
    task_id="update_notification",
    dag=dag,
    http_conn_id  ="slack_conn",
    webhook_token=os.getenv('slack_webhook_url'),
    message ="last_fees_city_events has been updated!",
    channel="#random"
    )

    snowflake_op_sql_list >> slack