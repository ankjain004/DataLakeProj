### Discuss the purpose of this database in context of the startup, Sparkify, and their analytical goals.

"Sparkify" being A music streaming startup company with increasing customer base and significant increase in data volume, one of the objective is to manage huge amount of data in normalized way so further analytics can be performed and data managebility will be sustainable irrespective of growth. 

During this project, we received 2 major datasets about SONG and USER LOG. Later we prepared some dimentional tables which has key column and their values and prepared one Fact table using those dimentional tables.

We have used AWS S3 storage to store our tables, which can be made accessible to limited set of analytics team using AWS IAM option and roles/privileges can be defined as per their responsibility.

Now Analytics team can dig out various insights from these dimentional & fact tables:

1) Most popular artists, whose songs are listened most, so Sparify can keep track on upcoming songs/albums from those artists and ensure song's availability to customer.
2) Customers who spent significant amount of time on application, who speical offers can be proposed to selected customer.
3) Time based analysis to understand pattern of various users.
4) Location based analysis to provide special services.
.. Many more as per business demand.

### State and justify your database schema design and ETL pipeline.

Given dataset about SONGS and LOGS have borken down to below Fact & Dimentional tables. 
All these tables are in Normalized form with useful columns and data and meet relational databases rules. 

# Fact Table
songplays - records in log data associated with song plays i.e. records with page NextSong
songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

# Dimension Tables
users - users in the app
user_id, first_name, last_name, gender, level

# songs - songs in music database
song_id, title, artist_id, year, duration

# artists - artists in music database
artist_id, name, location, lattitude, longitude

# time - timestamps of records in songplays broken down into specific units
start_time, hour, day, week, month, year, weekday

Here we have used AWS S3 storage where access is controlled by IAM defined users/policies.

Raw dataset is also available in S3 storage, which we read using Spark functions (Extract) and prepared dimentonal tables with relevant columns which are used to prepare FACT tables (Transform).
We have used Spark function to write transformed data into S3 storage (Load)so later it can be made accessible to Analytics team via IAM roles/group.

so we have used ETL pipeline along with AWS S3/IAM feature to control data accessibility and availability.






