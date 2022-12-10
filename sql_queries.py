# DROP TABLES

songplay_table_drop = "drop table songplays"
user_table_drop = "drop table users"
song_table_drop = "drop table song_table"
artist_table_drop = "drop table artist_table"
time_table_drop = "drop table time_table"

# CREATE TABLES

songplay_table_create = ("""create table songplays(songPlayID serial primary key,userId int,ts bigint,songID varchar,artistID varchar,level varchar,itemInSession int,page varchar, userAgent varchar,length float
,foreign key(userID) references users(userID)
,foreign key(ts) references time(ts)
,foreign key(artistID) references artists(artistID)
,foreign key(songID) references songs(songID))
""")

user_table_create = ("""create table users(userID int,firstName varchar,
lastName varchar,gender varchar,location text,primary key(userID))
""")

song_table_create = ("""create table songs(songID varchar,title text,artistID varchar,duration float,year int,primary key(songID))
""")

artist_table_create = ("""create table artists(artistID varchar,artist_latitude decimal,
artist_longitude decimal,artist_location varchar,artist_name varchar,primary key(artistID))
""")

time_table_create = ("""create table time(sessionID int,ts bigint,hour int,minutes int,seconds int,startDate date,
primary key (ts))
""")

# INSERT RECORDS

songplay_table_insert = ("""insert into songplays(userId ,ts ,songID ,artistID ,level ,itemInSession ,page , userAgent ,length) values(%s,%s,%s,%s,%s,%s,%s,%s,%s)
on conflict do nothing;
""")

user_table_insert = ("""insert into users(userID ,firstName ,lastName ,gender ,location)
values(%s,%s,%s,%s,%s)
on conflict do nothing;
""")

song_table_insert = ("""insert into songs(songID ,title ,artistID ,duration ,year)
values(%s,%s,%s,%s,%s)
on conflict do nothing;
""")

artist_table_insert = ("""insert into artists(artistID ,artist_latitude ,
artist_longitude ,artist_location ,artist_name)
values(%s,%s,%s,%s,%s)
on conflict do nothing;
""")


time_table_insert = ("""insert into time(sessionID ,ts ,hour ,minutes ,seconds ,startDate)
values(%s,%s,%s,%s,%s,%s)
on conflict do nothing;
""")

# FIND SONGS

song_select = ("""select s.songID,a.artistID from songs s inner join artists a on a.artistID=s.artistID
where s.title=%s and a.artist_name=%s and s.duration=%s
""")

time_select = ("""select select ts from time where ts=%s 
""")



# QUERY LISTS

create_table_queries = [ user_table_create,artist_table_create ,song_table_create, time_table_create,songplay_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop,song_table_drop,artist_table_drop, time_table_drop]
