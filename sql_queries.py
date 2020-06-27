import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')



# DROP TABLES

staging_events_table_drop = "Drop table if exists StagingEvents;"
staging_songs_table_drop = "Drop table if exists StagingSongs ;"
songplay_table_drop = "Drop table if exists songplay ;"
user_table_drop = "Drop table if exists users ;"
song_table_drop = "drop table if exists songs"
artist_table_drop = "drop table if exists artists ;"
time_table_drop = "drop table if exists time ;"

# CREATE TABLES  Done!!


    

staging_events_table_create= ("""
Create table if not exists StagingEvents (
artist varchar,
auth varchar,
firstname varchar,
gender varchar,
itemInSession int,
lastName varchar,
length numeric,
level varchar,
location varchar,
method varchar,
page varchar,
registration numeric,
sessionId int,
song varchar,
status int,
ts numeric,
userAgent varchar,
userId int)
""")

staging_songs_table_create = ("""
Create table if not exists StagingSongs (
num_songs int,
artist_id Varchar,
artist_latitude Varchar,
artist_longitude Varchar,
artist_location Varchar,
artist_name Varchar,
song_id Varchar,
title Varchar,
duration numeric,
year int
)

""")




songplay_table_create = ("""
create table if not exists songplay(
songplay_id int IDENTITY(0,1) primary key, 
start_time timestamp, 
user_id int not null, 
level Varchar , 
song_id Varchar not null, 
artist_id Varchar not null, 
session_id int not null, 
location varchar, 
user_agent varchar)




""")

user_table_create = ("""
create table if not exists users(
userid int Primary key,
firstname Varchar,
lastname Varchar,
gender Varchar,
level Varchar)

""")

song_table_create = ("""
create table if not exists songs(
song_id Varchar Primary key,
title Varchar,
artist_id Varchar,
duration numeric,
year int)

""")

artist_table_create = ("""
create table if not exists artists(
artist_id Varchar Primary key,
artist_name Varchar,
artist_location Varchar,
artist_latitude numeric,
artist_longitude numeric
)
""")

time_table_create = ("""
create table if not exists time(
start_time timestamp Primary key,
hour int,
day int,
week int,
month int,
year int,
weekday int
)
""")

# STAGING TABLES                                                !Done

staging_events_copy = (f"""
     copy StagingEvents from {config.get("S3","LOG_DATA")} 
    credentials 'aws_iam_role={config.get("IAM_ROLE","ARN")}' 
    format as JSON {config.get("S3","LOG_JSONPATH")};
""")

staging_songs_copy = (f"""
copy StagingSongs from {config.get("S3","SONG_DATA")} 
    credentials 'aws_iam_role={config.get("IAM_ROLE","ARN")}' 
    json 'auto';
""")

# FINAL TABLES

songplay_table_insert = ("""

insert into songplay(
start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
select timestamp 'epoch' + ev.ts/1000 *INTERVAL '1 second' as start_time, 
ev.userid, 
ev.level, 
s.song_id, 
s.artist_id, 
ev.sessionid, 
ev.location, 
ev.useragent
from StagingEvents ev left join StagingSongs s
on ev.song = s.title and ev.artist = s.artist_name
where ev.page = 'NextSong' and song_id is not null
""")


user_table_insert = ("""
insert into users(
userid, firstname, lastname, gender, level)
select s.userid, s.firstname, s.lastname, s.gender, s.level
from  StagingEvents s join (
select userid, max(ts) as ts from StagingEvents
group by 1) lastSession
on s.userid = lastSession.userid and s.ts = lastSession.ts and s.userid is not null
""")

song_table_insert = ("""
insert into songs (
song_id, title, artist_id, year, duration)
Select distinct song_id, title, artist_id, year, duration
from StagingSongs
where song_id is not null ;
""")



artist_table_insert = ("""
insert into artists(
artist_id, artist_name, artist_location, artist_latitude, artist_longitude)
select artist_id, artist_name, artist_location, artist_latitude, artist_longitude
from StagingSongs
where artist_id is not null
""")

time_table_insert = ("""

insert into time(start_time, hour, day, week, month, year, weekday)
select 
tt.t_s, 
extract (hour from tt.t_s ) as hour , 
extract (day from tt.t_s ) as  day, 
extract (week from tt.t_s ) as week , 
extract (month from tt.t_s ) as month , 
extract (year from tt.t_s ) as year , 
extract (weekday from tt.t_s ) as weekday 
from (
Select distinct timestamp 'epoch' + ts/1000 *INTERVAL '1 second' as t_s 
from 
StagingEvents
where page = 'NextSong'
) tt

""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
