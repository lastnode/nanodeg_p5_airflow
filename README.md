# Introduction

A part of the [Udacity Data Engineering Nanodegree](https://www.udacity.com/course/data-engineer-nanodegree--nd027), this [ETL](https://en.wikipedia.org/wiki/Extract,_transform,_load) project looks to collect and present user activity information for a fictional music streaming service called Sparkify. To do this, data is gathered from song information and application `.json` log files (which were generated from the [Million Song Dataset](http://millionsongdataset.com/) and from [eventsim](https://github.com/Interana/eventsim) respectively and given to us).

Part of this ETL process is from [an earlier iteraetion of this project](https://github.com/lastnode/nanodeg_p3_redshift), where we loaded these `.json` files into an [Amazon Redshift](https://aws.amazon.com/redshift/) cluster, using [Redshift's COPY function](https://docs.aws.amazon.com/redshift/latest/dg/r_COPY.html) to do the data extraction for us. This was particularly useful because it took full advantage of Redshift's MPP (massively parallel processing) architecture to perform paralell ETL on the JSON files that had to be processed.

The difference in this new project is that we use [Apache Airflow](https://airflow.apache.org/) for workflow management, allowing us to automate this ETL process, so that it can be run daily and also automatically log all tasks and retry them when they fail. These tasks are maanged via the Airflow Scheduler, which makes use of [Directed Acyclic Graphs (DAGs)](https://airflow.apache.org/docs/apache-airflow/stable/tutorial.html) for this purpose. These DAGs in turn uitilise several [custom Airflow Operators](https://airflow.apache.org/docs/apache-airflow/stable/howto/custom-operator.html) to perform these tasks. These operators allow for a level of modularity in our project that is very useful.

To run this project, you will need a working installation of Apache Airflow, and a connection to [Amazon S3](https://aws.amazon.com/s3/) and [Amazon Redshift](https://aws.amazon.com/redshift/). The basic two-step ETL process is as follows:

1. Use the Redshift COPY function to extract `.json` files from s3 to staging tables in Redshift
2. Transform the data in Redshift and create fact and dimension tables

# Files
```
- dags/ready_redshift.py - The preparatory DAG that drops any existing tables from Redshift and creates new ones for us, with the correct schema.
- dags/sparkify_etl.py - The main ETL tag that extracts the `.json` data from s3, copies it to Redshift and transforms it
- plugins/operators/prepare_redshift.py - The PrepareRedshiftOperator that drops old tables and creates new ones. Used by the ready_redshift.py DAG.
- plugins/operators/stage_redshift.py - The StageToRedshiftOperator that we use to copy the `.json` data from s3 to Redshift
- plugins/operators/load_fact.py - The LoadFactOperator, used to generate the `songplays` fact table
- plugins/operators/load_dimension.py - The LoadDimensionOperator, used to generate the `songs`, `users`, `artists`, and `time` dimension tables
- plugins/operators/data_quality.py - The DataQualityOperator, currently used to check if any of the tables are empty after ETL. 
- plugins/helpers/sql_queries.py - Contains all the SQL queries used by all the other operators.

````

# ETL Process

## Setup

To get going, you will need to create two new connections within Airflow:

### Amazong S3
First, you will need to [create an IAM Role](https://docs.aws.amazon.com/redshift/latest/dg/c-getting-started-using-spectrum-create-role.html) that [allows Redshift to access S3](https://docs.aws.amazon.com/redshift/latest/mgmt/authorizing-redshift-service.html) for you. Once you have created the role, grab the `Access key ID` and the `Secret access key` and add them to Airflow under `Admin > Connections > Create`. 

- Conn ID: `aws_credentials`
- Login: _the Access key ID for your Redshift > S3 IAM Role_
- Password: _Secret access key for this Access key ID_

This is the connection that allows Airflow to access S3 and copy the `.json` files from the Sparkify S3 bucket, so unless you have configured this correctly, the `sparkify_etl.py` DAG will not be able to execute the `StageToRedshiftOperator` correctly and it will fail.


### Amazon Redshift
Next, you will need to add your Amazon Redshift connection details to a new Airflow connection. Note that for Airflow to access your Redshift cluster, you will need to [Authorize access to the cluster](https://docs.aws.amazon.com/redshift/latest/gsg/rs-gsg-authorize-cluster-access.html). This may require [some troubleshooting](https://aws.amazon.com/premiumsupport/knowledge-center/cannot-connect-redshift-cluster/) the first time you do it.

- Conn ID: `redshift`
- Host: _your Amazon Redshift cluster's endpoint (usually something like: `your_cluster_name.abcdefgh.us-west-2.redshift.amazonaws.com`)
- Schema: _the name of the Redshift db_
- Login: _your Redshift username_
- Password: _your Redshift password_
- Port: _the port you have opened to allow connections on (`5439` by default)_

## Ready Redshift

Once your connections are set up correctly in Airflow, go ahead and run the `ready_redshift.py` dag. This **will drop any existing tables** with the following names from your database: `staging_songs`, `staging_events`, `songplays`, `songs`, `users`,`artists`, `time` and recreate the same tables with the following schemas:

````
staging_events_table_create = ("""
    
    CREATE TABLE public.staging_events (
        artist varchar(256),
        auth varchar(256),
        firstname varchar(256),
        gender varchar(256),
        iteminsession int4,
        lastname varchar(256),
        length numeric(18,0),
        "level" varchar(256),
        location varchar(256),
        "method" varchar(256),
        page varchar(256),
        registration numeric(18,0),
        sessionid int4,
        song varchar(256),
        status int4,
        ts int8,
        useragent varchar(256),
        userid int4
    );
    
    """)
    
    staging_songs_table_create = ("""
    
    CREATE TABLE public.staging_songs (
        num_songs int4,
        artist_id varchar(256),
        artist_name varchar(256),
        artist_latitude numeric(18,0),
        artist_longitude numeric(18,0),
        artist_location varchar(256),
        song_id varchar(256),
        title varchar(256),
        duration numeric(18,0),
        "year" int4
    );
    
    """)
    
       
    songplay_table_create = ("""
    
    CREATE TABLE public.songplays (
	songplay_id varchar(32) NOT NULL,
	start_time timestamp NOT NULL,
	userid int4 NOT NULL,
	"level" varchar(256),
	song_id varchar(256),
	artist_id varchar(256),
	session_id int4,
	location varchar(256),
	user_agent varchar(256),
	CONSTRAINT songplays_pkey PRIMARY KEY (songplay_id)
    );
    
    """)
    
    
    user_table_create = ("""
    
    CREATE TABLE public.users (
	userid int4 NOT NULL,
	first_name varchar(256),
	last_name varchar(256),
	gender varchar(256),
	"level" varchar(256),
	CONSTRAINT users_pkey PRIMARY KEY (userid)
    );
    
    """)
    
    song_table_create = ("""
    
    CREATE TABLE public.songs (
	songid varchar(256) NOT NULL,
	title varchar(256),
	artistid varchar(256),
	"year" int4,
	duration numeric(18,0),
	CONSTRAINT songs_pkey PRIMARY KEY (songid)
    );
    
    """)
    
    artist_table_create = ("""
    
    CREATE TABLE public.artists (
	artistid varchar(256) NOT NULL,
	name varchar(256),
	location varchar(256),
	latitude numeric(18,0),
	longitude numeric(18,0)
    );

    
    """)
    
    time_table_create = ("""
    
    CREATE TABLE public."time" (
	start_time timestamp NOT NULL,
	"hour" int4,
	"day" int4,
	week int4,
	"month" varchar(256),
	"year" int4,
	dayofweek varchar(256),
	CONSTRAINT time_pkey PRIMARY KEY (start_time)
    );

    
    """)

````

## Run Sparkify ETL

Once the `ready_redshift.py` DAG has been run, you can toggle it off and toggle on the `sparkify_etl.py` DAG. Before doing so, you may want to configure its `start_date` and `end_date` values in the `default_args` dict, as the DAG is scheduled to run once an hour, and having it scheduled to run acros many days could result in a lot of backfill tasks being scheduled.

Here are some other options that can be configured in this DAG.

- In the `stage_events_to_redshift` task, `partition_data` can be set to either `True` or `False`. If set to `True`, the StageToRedshiftOperator will only process files from the S3 `log_data` folder based on the `execution_date` of the DAG. This can be a useful way to only process data for a certain timeframe.

- In all the `load_songplays_table` and `load_*_dimension_table` tasks, the `truncate_table` attribute can be set to either `True` or `False`. If set to `True`, the `LoadFactOperator` / `LoadDimensionOperator` will [TRUNCATE](https://docs.aws.amazon.com/redshift/latest/dg/r_TRUNCATE.html) the tables before running the new queries on them. This allows you to switch between append-only and insert-delete functionality for these tasks.

## Queries

Once the `sparkify_etl.py` DAG has run succesfully, you will be able to query the `songplays` fact table and the `songs`, `users`, `artists` and `time` dimension tables in Redshift. Here are some sample queries that may be useful to start with. You can run these in the [Redshift Query Editor](https://docs.aws.amazon.com/redshift/latest/mgmt/query-editor.html):


#### How many songs were started by free vs paid users?

`select count(songplay_id), users.level from songplays inner join users on songplays.user_id = users.user_id group by 2;`

#### Which hour of the day are users starting to play the most songs?

`select hour, count(songplay_id) from songplays inner join time on songplays.start_time = time.start_time group by hour order by 1 desc;`

#### Which 30 users have started listening to the most songs?

`select user_id, count(songplay_id) from songplays group by user_id order by 2 desc limit 30;`


#### What are the most popular operating systems among users who have played songs?
```
select 
  sum(case when user_agent like '%Windows%' then 1 else 0 end) as windows_sum, 
  sum(case when user_agent like '%Linux%' then 1 else 0 end) as linux_sum, 
  sum(case when user_agent like '%Mac%' then 1 else 0 end) as mac_sum, 
  sum(case when user_agent like '%iPhone%' then 1 else 0 end) as iphone_sum, 
  sum(case when user_agent like '%Android%' then 1 else 0 end) as anrdoid_sum 
from songplays;
```

#### What are the most popular 90s songs among users?

```
select
    count(songs.song_id) as count,
    songs.title, 
    songs.year
from songplays

join songs on songplays.song_id = songs.song_id

where songs.year BETWEEN 1990 and 1999

group by songs.song_id, songs.title,  songs.year 
order by count(songs.song_id) desc;
```


