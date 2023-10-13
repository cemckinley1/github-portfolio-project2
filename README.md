## Colin McKinley Portfolio Project 2
# Project: Data Modeling with Apache Cassandra

---

## Project Overview
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analysis team is particularly interested in understanding what songs users are listening to. Currently, there is no easy way to query the data to generate the results, since the data reside in a directory of CSV files on user activity on the app.

In this project, I completed an ETL pipeline using Python. I modeled the data by creating tables in Apache Cassandra to run queries provided. 

---

## Part I. ETL Pipeline for Pre-Processing the Files

#### Import Python packages
```py
import pandas as pd
import cassandra
import re
import os
import glob
import numpy as np
import json
import csv
```

#### Creating list of filepaths to process original event csv data files
```py
# checking your current working directory
print(os.getcwd())

# Get your current folder and subfolder event data
filepath = os.getcwd() + '/event_data'

# Create a for loop to create a list of files and collect each filepath
for root, dirs, files in os.walk(filepath):
    
# join the file path and roots with the subdirectories using glob
    file_path_list = glob.glob(os.path.join(root,'*'))
    #print(file_path_list)
```
CWD: /workspace/home



#### Processing the files to create the data file csv that will be used for Apache Casssandra tables
```py
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
            
# uncomment the code below if you would like to get total number of rows 
#print(len(full_data_rows_list))
# uncomment the code below if you would like to check to see what the list of event data rows will look like
#print(full_data_rows_list)

# creating a smaller event data csv file called event_datafile_full csv that will be used to insert data into the \
# Apache Cassandra tables
csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)

with open('event_datafile_new.csv', 'w', encoding = 'utf8', newline='') as f:
    writer = csv.writer(f, dialect='myDialect')
    writer.writerow(['artist','firstName','gender','itemInSession','lastName','length',\
                'level','location','sessionId','song','userId'])
    for row in full_data_rows_list:
        if (row[0] == ''):
            continue
        writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[12], row[13], row[16]))
```
```py
# check the number of rows in your csv file
with open('event_datafile_new.csv', 'r', encoding = 'utf8') as f:
    print(sum(1 for line in f))
```
Number of rows in event_datafile_new.csv: 6821


---


## Part II. Complete the Apache Cassandra coding portion of your project.

#### The event_datafile_new.csv contains the following columns:

![](/assets/img/Screenshot1.png)
![](/assets/img/Screenshot2.png)

---

#### Creating a Cluster

```py
# This should make a connection to a Cassandra instance your local machine 
# (127.0.0.1)

from cassandra.cluster import Cluster
cluster = Cluster()

# To establish connection and begin executing queries, need a session
session = cluster.connect()
```

#### Create Keyspace

```py
# TO-DO: Create a Keyspace 
try:
    session.execute("""
    CREATE KEYSPACE IF NOT EXISTS project1b
    WITH REPLICATION =
    { 'class'  :  'SimpleStrategy', 'replication_factor' : 1 }"""
)
    
except Exception as e:
    print(e)
```

#### Set Keyspace

```py
# TO-DO: Set KEYSPACE to the keyspace specified above
try:
    session.set_keyspace('project1b')
except Exception as e:
    print(e)
```

#### Now we need to create tables to run the following queries. Remember, with Apache Cassandra you model the database tables on the queries you want to run.

### Create queries to ask the following three questions of the data

1. Give me the artist, song title and song's length in the music app history that was heard during sessionId = 338, and itemInSession = 4
2. Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182
3. Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'


## Query 1
Give me the artist, song title and song's length in the music app history that was heard during sessionId = 338, and itemInSession = 4

```py
## TO-DO: Query 1:  Give me the artist, song title and song's length in the music app history that was heard during \
## sessionId = 338, and itemInSession = 4

table_query = """
    CREATE TABLE IF NOT EXISTS listening_library (
        session_id int, 
        item_in_session int, 
        artist text, 
        length float, 
        song text, 
        PRIMARY KEY (session_id, item_in_session)
    );"""
try:
    session.execute(table_query)
except Exception as e:
    print(e)
