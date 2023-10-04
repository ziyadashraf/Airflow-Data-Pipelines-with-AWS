from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
import os
from airflow.operators import (
    StageToRedshiftOperator,
    LoadFactOperator,
    LoadDimensionOperator,
    DataQualityOperator,
)
from plugins.helpers import SqlQueries


default_args = {
    "owner": "udacity",
    "start_date": datetime(2023, 1, 12),
    "retries": 3,
    "depends_on_past": False,
    "retry_delay": timedelta(minutes=5),
    "email_on_retry": False,
}


dag = DAG(
    "dag_finale",
    default_args=default_args,
    description="Load and transform data in Redshift with Airflow",
    schedule_interval="0 * * * *",
    catchup=False,
)

start_operator = DummyOperator(task_id="Exec_start", dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id="Stage_events",
    s3_bucket="udacity-dend",
    s3_prefix="log_data",
    table="staging_events",
    copy_options="JSON 's3://udacity-dend/log_json_path.json'",
)


stage_songs_to_redshift = StageToRedshiftOperator(
    task_id="Stage_songs",
    s3_bucket="udacity-dend",
    s3_prefix="song_data",
    table="staging_songs",
    copy_options="FORMAT AS JSON 'auto'",
)


load_songplays_table = LoadFactOperator(
    task_id="Load_songplays_fact_table",
    dag=dag,
    redshift_conn_id="redshift",
    table="songplays",
    sql_stmt=SqlQueries.songplay_table_insert,
)


load_user_dimension_table = LoadDimensionOperator(
    task_id="Load_user_dimension_table",
    query=SqlQueries.user_table_insert,
    conn_id="redshift",
    table_name="users",
    clear_existing_data=True,
)


load_song_dimension_table = LoadDimensionOperator(
    task_id="Load_song_dimension_table",
    query=SqlQueries.song_table_insert,
    conn_id="redshift",
    table_name="songs",
    clear_existing_data=True,
)


load_artist_dimension_table = LoadDimensionOperator(
    task_id="load_artist_dimension_table",
    query=SqlQueries.artist_table_insert,
    conn_id="redshift",
    table_name="artists",
    clear_existing_data=True,
)


load_time_dimension_table = LoadDimensionOperator(
    task_id="load_time_dimension_table",
    query=SqlQueries.time_table_insert,
    conn_id="redshift",
    table_name="time",
    clear_existing_data=True,
)


run_quality_checks = DataQualityOperator(
    task_id="Run_data_quality_checks",
    dag=dag,
    redshift_conn_id="redshift",
    tables=["songplays", "users", "songs", "artists", "time"],
)


end_operator = DummyOperator(task_id="Exec_stop", dag=dag)


start_operator >> stage_songs_to_redshift
start_operator >> stage_events_to_redshift

stage_songs_to_redshift >> load_songplays_table
stage_events_to_redshift >> load_songplays_table

load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator
