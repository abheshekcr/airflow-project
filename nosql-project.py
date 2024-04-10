#!/usr/bin/env python
# coding: utf-8

# # Part I. ETL Pipeline for Pre-Processing the Files

# ## PLEASE RUN THE FOLLOWING CODE FOR PRE-PROCESSING THE FILES

# #### Import Python packages 

# In[8]:


# Import Python packages 
import pandas as pd
import cassandra
import re
import os
import glob
import numpy as np
import json
import csv


# #### Creating list of filepaths to process original event csv data files

# In[9]:


# checking your current working directory
print(os.getcwd())

# Get your current folder and subfolder event data
filepath = os.getcwd() + '/event_data'

# Create a for loop to create a list of files and collect each filepath
for root, dirs, files in os.walk(filepath):
    
# join the file path and roots with the subdirectories using glob
    file_path_list = glob.glob(os.path.join(root,'*'))
    #print(file_path_list)


# #### Processing the files to create the data file csv that will be used for Apache Casssandra tables

# In[10]:


# initiating an empty list of rows that will be generated from each file
full_data_rows_list = [] 
    
# for every filepath in the file path list 
for f in file_path_list:

# reading csv file 
    with open(f, 'r', encoding = 'utf8', newline='') as csvfile: 
        # creating a csv reader object 
        csvreader = csv.reader(csvfile) 
        next(csvreader)
        
 # extracting each data row one by one and append it        
        for line in csvreader:
            #print(line)
            full_data_rows_list.append(line) 
            

# creating a smaller event data csv file called event_datafile_full csv that will be used to insert data into the \
# Apache Cassandra tables
csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)

with open('event_datafile_new.csv', 'w', encoding = 'utf8', newline='') as f:
    writer = csv.writer(f, dialect='myDialect')
    writer.writerow(['artist','firstName','gender','itemInSession','lastName','length',                'level','location','sessionId','song','userId'])
    for row in full_data_rows_list:
        if (row[0] == ''):
            continue
        writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[12], row[13], row[16]))


# In[18]:


# check the number of rows in your csv file
with open('event_datafile_new.csv', 'r', encoding = 'utf8') as f:
    print(sum(1 for line in f))


# # Part II. Complete the Apache Cassandra coding portion of your project. 
# 
# ## Now you are ready to work with the CSV file titled <font color=red>event_datafile_new.csv</font>, located within the Workspace directory.  The event_datafile_new.csv contains the following columns: 
# - artist 
# - firstName of user
# - gender of user
# - item number in session
# - last name of user
# - length of the song
# - level (paid or free song)
# - location of the user
# - sessionId
# - song title
# - userId
# 
# The image below is a screenshot of what the denormalized data should appear like in the <font color=red>**event_datafile_new.csv**</font> after the code above is run:<br>
# 
# <img src="images/image_event_datafile_new.jpg">

# #### Creating a Cluster

# In[42]:


from cassandra.cluster import Cluster
try:
    cluster = Cluster(['127.0.0.1'])
    session=cluster.connect()
except Exception as e:
    print(e)


# #### Create Keyspace

# In[43]:


try:
    session.execute("""
    CREATE KEYSPACE IF NOT EXISTS music
    WITH REPLICATION = 
    { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"""
)

except Exception as e:
    print(e)


# #### Set Keyspace

# In[44]:


try:
    session.set_keyspace('music')
except Exception as e:
    print(e)


# ## Create queries to ask the following three questions of the data
# 
# ### 1. Give me the artist, song title and song's length in the music app history that was heard during  sessionId = 338, and itemInSession  = 4
# 
# 
# ### 2. Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182
#     
# 
# ### 3. Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'
# 
# 
# 
#using session_id and item_in_session makes the primary key unique 
using session_id serves as a really good one for partitoning as we can group data into individual session_id
earlier i used session_id and item_in_session as partitoning key ..performance of query was very slow
then i changed it to only session_id as partitioning key and item_in_session as clustering key ..and query performance was fast
# In[ ]:


query = "CREATE TABLE IF NOT EXISTS detail_by_sessionid_and_itemsInSession"

query = query + "(session_id int,item_in_session int,artist text,song_title text,song_length float,              PRIMARY KEY ((session_id),item_in_session))"
try:
    session.execute(query)
except Exception as e:
    print(e)                  


# In[ ]:


file = 'event_datafile_new.csv'

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader)
    for line in csvreader:
        query = "insert into detail_by_sessionid_and_itemsInSession(session_id,item_in_session,artist,song_title,song_length)"
        query = query + "values(%s,%s,%s,%s,%s)"
        session.execute(query, (int(line[8]),int(line[3]),line[0], line[9],float(line[5])))


# #### Do a SELECT to verify that the data have been inserted into each table

# In[94]:


query = "SELECT artist,song_title,song_length             FROM detail_by_sessionid_and_itemsInSession WHERE session_id=338 AND item_in_session=4"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)
    
for row in rows:
    print (row.artist, row.song_title, row.song_length)


# #using user_id and session_id was really good for partition key as it helps in clustering data in different nodes 
# adding items_in_session to the partition key makes the primary key unique 

# In[88]:


query = "CREATE TABLE IF NOT EXISTS detail_by_userid_and_session"

query = query + "(user_id int,session_id int,items_in_session int,artist_name text,song text,first_name text,last_name text,             PRIMARY KEY ((user_id,session_id),items_in_session))"
try:
    session.execute(query)
except Exception as e:
    print(e)              


# In[89]:


file = 'event_datafile_new.csv'

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader)
    for line in csvreader:
        query = "INSERT INTO detail_by_userid_and_session(user_id,session_id,items_in_session,artist_name,song,first_name,last_name)"
        query = query + "values(%s,%s,%s,%s,%s,%s,%s)"
        session.execute(query, (int(line[10]),int(line[8]),int(line[3]),line[0],line[9],line[1],line[4]))


# In[90]:


query = "SELECT artist_name,song,first_name,last_name,         user_id,session_id,items_in_session from detail_by_userid_and_session WHERE user_id=10 AND session_id=182"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)
    
for row in rows:
    print (row.artist_name,row.song,row.first_name,row.last_name,row.user_id,row.session_id,row.items_in_session)


# #here the use of user_id along with songs was a good idea because only using songs results in 
# very scattered data and hence while reading it would have taken more time 
# grouping it with user_id helps in better partitioning and hence faster reads 

# In[85]:


query = "CREATE TABLE IF NOT EXISTS details_by_songs"
query = query + "(songs text,user_id int,first_name text,last_name text, PRIMARY KEY (songs,user_id))"
try:
    session.execute(query)
except Exception as e:
    print(e)   

                    


# In[86]:


file = 'event_datafile_new.csv'

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader)
    for line in csvreader:
        query = "INSERT INTO details_by_songs(songs,user_id,first_name,last_name)"
        query = query + "values(%s,%s,%s,%s)"
        session.execute(query, (line[9],int(line[10]),line[1],line[4]))


# In[87]:


query = "SELECT first_name,last_name FROM details_by_songs where songs='All Hands Against His Own'"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)
    
for row in rows:
    print (row.first_name,row.last_name)


# ### Drop the tables before closing out the sessions

# In[31]:


query = "drop table detail_by_sessionid_and_itemsInSession"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)


# In[32]:


query = "drop table detail_by_userid_and_session"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)


# In[33]:


query = "drop table details_by_song"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)


# ### Close the session and cluster connection¶

# In[34]:


session.shutdown()
cluster.shutdown()


# In[ ]:





# In[ ]:




