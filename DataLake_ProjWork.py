#!/usr/bin/env python
# coding: utf-8

# In[4]:


import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql import types


# In[5]:


config = configparser.ConfigParser()
config.read_file(open('dl.cfg'))

os.environ["AWS_ACCESS_KEY_ID"]= config['AWS']['AWS_ACCESS_KEY_ID']
os.environ["AWS_SECRET_ACCESS_KEY"]= config['AWS']['AWS_SECRET_ACCESS_KEY']


# In[1]:


#print(os.environ['AWS_ACCESS_KEY_ID'])
#print(os.environ['AWS_SECRET_ACCESS_KEY'])


# In[6]:


spark = SparkSession         .builder         .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0")         .getOrCreate()


# In[ ]:


# get filepath to song data file
song_data = "s3a://udacity-dend/song_data/A/A/A/*.json"
output_data = "s3a://ank04bucket/DL_Proj_" 

 # read song data file
song_df = spark.read.json(song_data)
#song_temp = song_df.select("title")
#song_temp.show()

 # extract columns to create songs table
songs_table = song_df.select("song_id","title","artist_id","year","duration").dropDuplicates(["song_id"])
#songs_table.show(5)


 # write songs table to parquet files partitioned by year and artist

songs_table_path = output_data + "songs_table.parquet"
songs_table.write.mode('overwrite').partitionBy("year","artist_id").parquet(songs_table_path)
 # extract columns to create artists table

artists_table = song_df.select("artist_id","artist_name",col("artist_location").alias("Location"),col("artist_latitude").alias("latitude"),col("artist_longitude").alias("longitude")).dropDuplicates(["artist_id"])
#artists_table.show(5)    
 # write artists table to parquet files

artists_table_path = output_data + "artists_table.parquet"
artists_table.write.mode('overwrite').parquet(artists_table_path)


# In[ ]:



    # get filepath to log data file
    
log_data ="s3a://udacity-dend/log_data/2018/11/*.json"
output_data = "s3a://ank04bucket/DL_Proj_"
    
    # read log data file

log_df = spark.read.json(log_data)

    # filter by actions for song plays

log_df = log_df.where(log_df.page=="NextSong")

    # extract columns for users table    
users_table = log_df.select(col("userId").alias("user_id"),col("firstName").alias("first_name"),col("lastName").alias("last_name"),"gender","level").dropDuplicates(["user_id"])
    
    # write users table to parquet files
users_table_path = output_data + "users_table.parquet"
users_table.write.mode('overwrite').parquet(users_table_path)

    # create timestamp column from original timestamp column
#get_timestamp = udf(lambda x: datetime.fromtimestamp(int(x)/1000.0),TimestampType())
#df = df.withColumn("timestamp",get_timestamp(df.ts)) 
    
    # create datetime column from original timestamp column
    #get_datetime = udf()
#df = df.withColumn("datetime",get_timestamp(df.ts)) 
#df.printSchema()
    # extract columns to create time table
    #time_table = 
    
    # write time table to parquet files partitioned by year and month
    #time_table

    # read in song data to use for songplays table
    #song_df = 

    # extract columns from joined song and log datasets to create songplays table 
    #songplays_table = 

    # write songplays table to parquet files partitioned by year and month
    #songplays_table


# In[16]:


# create timestamp column from original timestamp column

get_timestamp = udf(lambda x: datetime.fromtimestamp(int(x)/1000.0),types.TimestampType())
log_df = log_df.withColumn("timestamp",get_timestamp(log_df.ts)) 

# create datetime column from original timestamp column
#get_datetime = udf()

log_df = log_df.withColumn("datetime",get_timestamp(log_df.ts)) 
#log_df.printSchema()
#log_df.show(5)


# In[17]:


time_table_path = output_data + "time_table.parquet"
# extract columns to create time table

time_table = log_df.select(col("datetime").alias("start_Time"),
                        hour(log_df.datetime).alias("hour"),
                        dayofmonth(log_df.datetime).alias("day"),
                        weekofyear(log_df.datetime).alias("week"),
                        month(log_df.datetime).alias("month"),
                        year(log_df.datetime).alias("year"),
                        date_format(log_df.datetime,'E').alias("weekday")
                       ).dropDuplicates(["start_Time"])
#time_table.printSchema()
#time_table.show(5)
    # write time table to parquet files partitioned by year and month
    
time_table = time_table.write.mode('overwrite').partitionBy("year","month").parquet(time_table_path)


# In[23]:


# read in song data to use for songplays table
songs_df = spark.read.json(song_data)
#songs_df.printSchema()

song_log_join = log_df.join(songs_df, (log_df.artist == songs_df.artist_name) & (log_df.song == songs_df.title), how="left")
#song_log_join.printSchema()
song_log_join.show(1)
    # extract columns from joined song and log datasets to create songplays table 
#songplays_table = log_df.select("song",col("datetime").alias("start_Time"),
 #                            col("userId").alias("user_Id"),
  #                           "level",col("sessionId").alias("session_Id"),
   #                          "location",col("userAgent").alias("user_agent")
    #                        )
#songplays_table_join.printSchema()
    # write songplays table to parquet files partitioned by year and month
    #songplays_table


# In[ ]:




