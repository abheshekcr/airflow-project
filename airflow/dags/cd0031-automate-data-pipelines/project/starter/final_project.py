from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator
from udacity.common import final_project_sql_statements
from airflow.operators.postgres_operator import PostgresOperator


default_args = {
    'owner': 'Abheshek',
    'start_date': pendulum.now(),
    'depends_on_past':False,
    'retries':3,
    'retries_delay':timedelta(minutes=5),
    'catchup':False,
    'email_on_retry':False
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *',
)
def final_project():

    start_operator = DummyOperator(task_id='Begin_execution')


    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        table="staging_events",
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        s3_bucket="airflow-bucket123",
        s3_key="log-data/2018/11/",
        json="s3://airflow-bucket123/log_json_path.json"
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        table="staging_songs",
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        s3_bucket="airflow-bucket123",
        s3_key="song-data/A/A/A/",
        json="auto"
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        origin_table1="staging_events",
        origin_table2="staging_songs",
        destination_table="songplay_fact_table",
        redshift_conn_id="redshift"
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        sql_query=final_project_sql_statements.SqlQueries.user_table_insert,
        table="user_dim_table",
        redshift_conn_id="redshift",
        truncate=True
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        sql_query=final_project_sql_statements.SqlQueries.song_table_insert,
        table="song_dim_table",
        redshift_conn_id="redshift",
        truncate=False
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        sql_query=final_project_sql_statements.SqlQueries.artist_table_insert,
        table="artist_dim_table",
        redshift_conn_id="redshift",
        truncate=False
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        sql_query=final_project_sql_statements.SqlQueries.time_table_insert,
        table="time_dim_table",
        redshift_conn_id="redshift",
        truncate=False
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id="redshift",
        table=["songplay_fact_table","user_dim_table","song_dim_table","artist_dim_table","time_dim_table"]
    )


    end_operator = DummyOperator(task_id='end_execution')


    start_operator >> [stage_events_to_redshift,stage_songs_to_redshift] >> load_songplays_table
    load_songplays_table >> [load_user_dimension_table,load_song_dimension_table,load_artist_dimension_table,load_time_dimension_table] >> run_quality_checks 
    run_quality_checks >> end_operator
   
final_project_dag = final_project()