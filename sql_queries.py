import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

log_data = config.get('S3', 'log_data')
log_jsonpath = config.get('S3', 'log_jsonpath')
song_data = config.get('S3', 'song_data')
dwh_iam_role_arn = config.get("IAM_ROLE", "dwh_role_arn") 

#.0 DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

# CREATE TABLES

staging_events_create_table= ("""
    CREATE TABLE staging_events (
        artist varchar(200),
        auth varchar(50),
        firstName varchar(500),
        gender char(1),
        itemInSession integer,
        lastName varchar(300),
        length decimal(13, 5),
        level varchar(50),
        location varchar(600),
        method varchar(50),
        page varchar(600),
        registration float,
        sessionId integer,
        song varchar(600),
        status integer,
        ts varchar(60),
        userAgent varchar(600),
        userId integer
    );
""")

staging_songs_create_table = ("""
    CREATE TABLE staging_songs (
        num_songs integer,
        artist_id varchar(50),
        artist_latitude decimal(13, 5),
        artist_longitude decimal(13, 5),
        artist_location varchar(600),
        artist_name varchar(600),
        song_id varchar(50),
        title varchar(600),
        duration decimal(16, 5),
        year integer
    );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id integer identity(0,1) sortkey,
        start_time timestamp not null,
        user_id integer not null references users (user_id),
        level varchar(50),
        song_id varchar(50) references songs (song_id),
        artist_id varchar(50) references artists (artist_id),
        session_id integer not null,
        location varchar(500),
        user_agent varchar
    )
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id integer primary key,
        first_name varchar(600) not null,
        last_name varchar(600) not null,
        gender char(1),
        level varchar(50) not null
    )
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id varchar(50) primary key,
        title varchar(600) not null sortkey,
        artist_id varchar not null distkey references artists (artist_id),
        year integer not null,
        duration decimal(16, 6) not null
    )
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id varchar(50) primary key,
        name varchar(600) not null sortkey,
        location varchar(600),
        latitude decimal(15,6),
        longitude decimal(15,6)
    )
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time timestamp not null primary key sortkey,
        hour numeric not null,
        day numeric not null,
        week numeric not null,
        month numeric not null,
        year numeric not null,
        weekday numeric not null
    )
""")

# STAGING TABLES

staging_events_copy = ("""
copy staging_events from {} region 'us-west-2' 
iam_role '{}' 
format as json {}
timeformat as 'epochmillisecs'
""").format(log_data, dwh_iam_role_arn, log_jsonpath)

staging_songs_copy = ("""
copy staging_songs from {} 
region 'us-west-2' 
iam_role '{}' 
format as json 'auto'
""").format(song_data, dwh_iam_role_arn)

# FINAL TABLES

user_table_insert = ("""
    insert into users (user_id, first_name, last_name, gender, level)
    select distinct
        userId AS user_id,
        firstName AS first_name,
        lastName AS last_name,
        gender,
        level
    FROM
        staging_events
    WHERE
        page = 'NextSong' AND
        user_id not in (select distinct user_id FROM users)
""")

artist_table_insert = ("""
    insert into artists (artist_id, name, location, latitude, longitude)
    select distinct
        artist_id,
        artist_name AS name,
        artist_location AS location,
        artist_latitude AS latitude,
        artist_longitude AS longitude
    FROM
        staging_songs
    WHERE 
        artist_id not in (select distinct artist_id FROM artists)
""")

time_table_insert = ("""
    insert into time (start_time, hour, day, week, month, year, weekday)
    SELECT
        ts AS start_time,
        EXTRACT(hour from ts) AS hour,
        EXTRACT(day from ts) AS day,
        EXTRACT(week from ts) AS week,
        EXTRACT(month from ts) AS month,
        EXTRACT(year from ts) AS year, 
        EXTRACT(weekday from ts) AS weekday
    FROM (
        SELECT distinct timestamp 'epoch' + ts/1000 *INTERVAL '1 second' as ts 
            FROM staging_events s     
    )
    WHERE 
        start_time not in (select distinct start_time FROM time)
""")

song_table_insert = ("""
    insert into songs (song_id, title, artist_id, year, duration)
    select distinct
        song_id,
        title,
        artist_id,
        year,
        duration
    from
        staging_songs
    where 
        song_id not in (select distinct song_id FROM songs)
""")

songplay_table_insert = ("""
    insert into songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    select distinct
        timestamp 'epoch' + ts/1000 * INTERVAL '1 second' AS start_time,
        se.userId as user_id,
        se.level,
        ss.song_id as song_id,
        ss.artist_id as artist_id,
        se.sessionId as session_id,
        se.location as location,
        se.userAgent as user_agent
    from
        staging_events se, staging_songs ss
    where 
        se.page = 'NextSong' AND 
        se.song = ss.title AND 
        se.userId not in (
            select distinct 
                t1.user_id 
            FROM 
                songplays t1 
            WHERE 
                t1.user_id = se.userId AND  
                t1.session_id = se.sessionId
        ) 
""")

# Count of rows for each table

staging_events_count = ("""
select count(*) from staging_events
""")

staging_songs_count = ("""
select count(*) from staging_songs
""")

artists_count = ("""
select count(*) from artists
""")

songs_count = ("""
select count(*) from songs
""")

time_count = ("""
select count(*) from time
""")

users_count = ("""
select count(*) from users
""")

songplays_count = ("""
select count(*) from songplays
""")


# QUERY LISTS

create_table_queries = [staging_events_create_table,staging_songs_create_table,time_table_create,user_table_create,artist_table_create,song_table_create,
    songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_order = ['staging_events', 'staging_songs']
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_order = ['artists', 'songs', 'time', 'users', 'songplays']
insert_table_queries = [artist_table_insert, song_table_insert, time_table_insert, user_table_insert, songplay_table_insert]
count_table_order = ['staging_events', 'staging_songs', 'artists', 'songs', 'time', 'users', 'songplays']
count_table_queries = [staging_events_count, staging_songs_count, artists_count, songs_count, users_count, songplays_count]