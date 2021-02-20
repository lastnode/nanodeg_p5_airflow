from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import PrepareRedshiftOperator
from helpers import SqlQueries

default_args = {
    'owner': 'mahangu',
    'start_date': datetime(2020, 2, 20),
    'depends_on_past': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=2),
    'catchup': False,
    'email_on_retry': False
}

dag = DAG('ready_redshift',
          default_args=default_args,
          description='Drop old tables and and create new tables for our ETL dag',
          schedule_interval='@once'
)

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)


drop_staging_events_table = PrepareRedshiftOperator(
    task_id='Drop_staging_events_table',
    dag=dag,
    provide_context=True,
    table="staging_events",
    redshift_conn_id="redshift",    
    sql_query=SqlQueries.staging_events_table_drop
)

drop_staging_songs_table = PrepareRedshiftOperator(
    task_id='Drop_staging_songs_table',
    dag=dag,
    provide_context=True,
    table="staging_songs",
    redshift_conn_id="redshift",    
    sql_query=SqlQueries.staging_songs_table_drop
)

drop_songplays_table = PrepareRedshiftOperator(
    task_id='Drop_songplays_table',
    dag=dag,
    provide_context=True,
    table="songplays",
    redshift_conn_id="redshift",    
    sql_query=SqlQueries.songplay_table_drop
)
    
drop_user_table = PrepareRedshiftOperator(
    task_id='Drop_user_table',
    dag=dag,
    provide_context=True,
    table="user",
    redshift_conn_id="redshift",    
    sql_query=SqlQueries.user_table_drop
)

drop_song_table = PrepareRedshiftOperator(
    task_id='Drop_song_table',
    dag=dag,
    provide_context=True,
    table="song",
    redshift_conn_id="redshift",    
    sql_query=SqlQueries.song_table_drop
)

drop_artist_table = PrepareRedshiftOperator(
    task_id='Drop_artist_table',
    dag=dag,
    provide_context=True,
    table="artist",
    redshift_conn_id="redshift",    
    sql_query=SqlQueries.artist_table_drop
)

drop_time_table = PrepareRedshiftOperator(
    task_id='Drop_time_table',
    dag=dag,
    provide_context=True,
    table="time",
    redshift_conn_id="redshift",    
    sql_query=SqlQueries.time_table_drop
)

create_staging_events_table = PrepareRedshiftOperator(
    task_id='Create_staging_events_table',
    dag=dag,
    provide_context=True,
    table="staging_events",
    redshift_conn_id="redshift",    
    sql_query=SqlQueries.staging_events_table_create
)

create_staging_songs_table = PrepareRedshiftOperator(
    task_id='Create_staging_songs_table',
    dag=dag,
    provide_context=True,
    table="staging_songs",
    redshift_conn_id="redshift",    
    sql_query=SqlQueries.staging_songs_table_create
)

create_songplays_table = PrepareRedshiftOperator(
    task_id='Create_songplays_table',
    dag=dag,
    provide_context=True,
    table="songplays",
    redshift_conn_id="redshift",    
    sql_query=SqlQueries.songplay_table_create
)

create_user_table = PrepareRedshiftOperator(
    task_id='Create_user_table',
    dag=dag,
    provide_context=True,
    table="users",
    redshift_conn_id="redshift",    
    sql_query=SqlQueries.user_table_create
)

create_song_table = PrepareRedshiftOperator(
    task_id='Create_song_table',
    dag=dag,
    provide_context=True,
    table="songs",
    redshift_conn_id="redshift",    
    sql_query=SqlQueries.song_table_create
)

create_artist_table = PrepareRedshiftOperator(
    task_id='Create_artist_table',
    dag=dag,
    provide_context=True,
    table="artists",
    redshift_conn_id="redshift",    
    sql_query=SqlQueries.artist_table_create
)

create_time_table = PrepareRedshiftOperator(
    task_id='Create_time_table',
    dag=dag,
    provide_context=True,
    table="time",
    redshift_conn_id="redshift",    
    sql_query=SqlQueries.time_table_create
)
    
end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> drop_staging_events_table
start_operator >> drop_staging_songs_table
start_operator >> drop_songplays_table
start_operator >> drop_user_table
start_operator >> drop_song_table
start_operator >> drop_artist_table
start_operator >> drop_time_table


drop_staging_events_table >> create_staging_events_table
drop_staging_songs_table >> create_staging_songs_table
drop_songplays_table >> create_songplays_table
drop_user_table >> create_user_table
drop_song_table >> create_song_table
drop_artist_table >> create_artist_table
drop_time_table >> create_time_table

create_staging_events_table >> end_operator
create_staging_songs_table >> end_operator
create_songplays_table >> end_operator
create_user_table >> end_operator
create_song_table >> end_operator
create_artist_table >> end_operator
create_time_table >> end_operator

