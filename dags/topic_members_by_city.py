from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow import DAG
from datetime import datetime
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
import os
from datetime import datetime

SNOWFLAKE_CONN_ID = 'snowflake_conn'


sql_query = """
    CREATE OR REPLACE TABLE topic_members_by_city as
    SELECT a."city", MEMBERS_TOPICS."topic_key", sum(1) as total_members
    FROM MEMBERS_TOPICS
    JOIN
        (SELECT DISTINCT MEMBERS."member_id", "city"
        FROM MEMBERS
        JOIN MEMBERS_TOPICS 
        ON MEMBERS."member_id" = MEMBERS_TOPICS."member_id") as a
    ON MEMBERS_TOPICS."member_id" = a."member_id"
    GROUP BY "city", MEMBERS_TOPICS."topic_key"
    ORDER BY "city", total_members DESC;
    """

sql_add_key = """
                ALTER TABLE topic_members_by_city 
                ADD PRIMARY KEY ("city", "topic_key");"""

default_args = {
    'start_date': datetime(2020, 1, 1)
}

with DAG('topic_members_by_city_dag', schedule_interval='@daily', default_args=default_args, catchup=False) as dag:
    topic_members_by_city = SnowflakeOperator(
        task_id='topic_members_by_city', dag=dag, snowflake_conn_id=SNOWFLAKE_CONN_ID, sql=sql_query
    )

    snowflake_add_key = SnowflakeOperator(
        task_id='snowflake_add_key', dag=dag, snowflake_conn_id=SNOWFLAKE_CONN_ID, sql=sql_add_key
    )

    slack = SlackWebhookOperator(
    task_id="update_notification",
    dag=dag,
    http_conn_id  ="slack_conn",
    webhook_token=os.getenv('slack_webhook_url'),
    message ="topic_members_by_city has been updated!",
    channel="#random"
    )

    topic_members_by_city >> snowflake_add_key >> slack