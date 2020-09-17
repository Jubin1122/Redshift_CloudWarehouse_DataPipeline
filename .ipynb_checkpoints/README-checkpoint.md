# Project Introduction

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

Here, we tasked with building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to. 

# Project Datasets

In this project we are working with two datasets that reside in S3. Here are the S3 links for each:

- Song Data
> s3://udacity-dend/song_data

- Log Data
> s3://udacity-dend/log_data

# Getting Started

### Infrastructure Process:

1. First, we have to run the ___create_cluster.py___. After it runs, it will create the cluster also, it will attach respective iam roles.

2. To successfully create the cluster and to run other files, we need to parse the ___dwh.cfg___ file, with the necessary parameters defined.




root@415e21c4c001:/home/workspace# python analyze_tables.py
 Analytical Table: staging_events..
    8056
  [Finished]  
 Analytical Table: staging_songs..
    14896
  [Finished]  
 Analytical Table: artists..
    10025
  [Finished]  
 Analytical Table: songs..
    14896
  [Finished]  
 Analytical Table: time..
    104
  [Finished]  
 Analytical Table: users..
    1144
  [Finished]  





3. To check whether the cluster is created or not, we will run ___cluster_details.py___ . 

4. Once we receive the cluster information, we will get the cluster endpoint, rolearn, and we will open TCP port to access the cluster endpoint. 
    Moreover, we will write a function to write back the **dwh_endpoint** and **dwh_role_arn** back to the configration file.
    This will be done by running ___cluster_endpoint.py___.
    
5. Finally, we will delete the cluster by running ___delete_cluster.py___.

### ETL Process

- In the ETL process, there are mainly two steps:
  
  > First we will run ___create_tables.py___. Primarily, it will drop the tables if it exists before and creates the new ones.
  
  > Then we will run ___etl.py___ which first copy load data from S3 to staging tables on Redshift and then load data from
   staging tables to analytical tables using insert and select.
   
# Analyzing Results

- After running the _etl.py_ we will run ___analyze_tables.py___, this will simply return the count of records in each analytical tables.

##### Output of analyze_tables.py




Analytical Table: staging_events..
    8056
  [DONE]  
 Analytical Table: staging_songs..
    14896
  [DONE]  
 Analytical Table: artists..
    10025
  [DONE]  
 Analytical Table: songs..
    14896
  [DONE]  
 Analytical Table: time..
    104
  [DONE]  
 Analytical Table: users..
    1144
  [DONE]  





# Database/Schema Structure:

Here, we have used _star schema_ datastructure for designing the database.  

#### Staging Tables:

- staging_events
- staging_songs

#### Fact Table:

- songplays: In this all the event data are assosiated with the records with page = NextSong and song = title and contains
    _songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent_.

### Dimension Tables:

- There are four dimension tables, namely:

    1. time: It consists of _start_time, hour, day, week, month, year, weekday_.
    
    2. users: It consists of _user_id, first_name, last_name, gender, level_.
    
    3. artists: It consists of _artist_id, name, location, latitude, longitude_.
    
    4. songs: It consists of _song_id, title, artist_id, year, duration.

### Note:
Here, all the records are unique without any duplicate values.
    