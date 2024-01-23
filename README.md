# Project_1
Youtube Harvesting and Warehousing
from googleapiclient.discovery import build
import pymongo
import pymysql
import pandas as pd
import streamlit as st
import time
from streamlit import session_state

def api_connect():
    api_key = 'Your api_key'
    
    api_service_name = "youtube"
    api_version = "v3"
    youtube =  build(api_service_name,api_version,developerKey = api_key)
    return youtube

youtube = api_connect()

First i have created a youtube api_key which is responsible for connecting to the YouTube API using the provided API key and returning a YouTube service object. It uses the build function from the googleapiclient.discovery module.

After that to get a channel_information from youtube created a function called get_channel_info. This function is responsible for making a request to the YouTube API to retrieve information about a specific channel using its channel_id. It extracts relevant data from the API response and returns it as a dictionary.

def get_channel_info(channel_id):
    request = youtube.channels().list(
            part="snippet,contentDetails,statistics",
            id=channel_id
        )
    response_channel = request.execute()

    for i in response_channel['items']:
        channel_data = dict(Channel_name = i['snippet']['title'],
                            Channel_id = i['id'],
                            Channel_views = i['statistics']['viewCount'],
                            Channel_description = i['snippet']['description'],
                            Channel_type = i['kind'],
                            Playlist_Id = i['contentDetails']['relatedPlaylists']['uploads'],
                            Subscriber_counts = i['statistics']['subscriberCount'],
                            Video_counts = i['statistics']['videoCount']                      
                           )
        return channel_data

def get_video_ids(channel_id):
    video_ids = []
    response = youtube.channels().list(id=channel_id,
                                               part="contentDetails").execute()
    Playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    nextpage_token = None

    while True:

        video_id_response = youtube.playlistItems().list(part = "snippet,contentDetails",
                                                         maxResults = 50,
                                                         playlistId = Playlist_id,
                                                         pageToken = nextpage_token).execute()
        for i in range(len(video_id_response['items'])):
            video_ids.append(video_id_response['items'][i]['snippet']['resourceId']['videoId'])
        nextpage_token = video_id_response.get('nextPageToken')
        if nextpage_token is None:
            break
    return video_ids
    
The get_video_ids function is designed to retrieve a list of video IDs associated with a given YouTube channel ID. It does this by first obtaining the channel's uploads playlist ID and then retrieving the video IDs from that playlist.
1.It sends a request to the channels.list endpoint with the specified channel_id to obtain information about the channel, specifically its uploads playlist ID.
2.Using the obtained uploads playlist ID, it sends requests to the playlistItems.list endpoint to retrieve the video IDs associated with that playlist.
3.The function retrieves video IDs in batches of 50 (specified by maxResults), and it continues making requests using the nextPageToken until there are no more pages of results.

def get_video_info(video_ids):
    Video_datas = []
    for video_id in video_ids:
        video_info_request = youtube.videos().list(part="snippet,contentDetails,statistics",
                                                   id=video_id).execute()


        for i in video_info_request['items']:
            Video_data = dict(Channel_name = i['snippet']['channelTitle'],
                              Channel_id = i['snippet']['channelId'],
                              Video_id = i['id'],
                              Video_name = i['snippet']['description'],
                              Video_description = i['snippet']['description'],
                              published_date = i['snippet']['publishedAt'],
                              View_count = i['statistics']['viewCount'],
                              like_count = i['statistics'].get('likeCount'),
                              favorite_count = i['statistics']['favoriteCount'],
                              comment_count = i['statistics'].get('commentCount'),
                              thumbnail = i['snippet']['thumbnails']['default']['url'] ,
                              Caption_status = i['contentDetails']['caption'],
                              Duration = i['contentDetails']['duration']
            )
            Video_datas.append(Video_data)
    return Video_datas

The get_video_info function is designed to retrieve detailed information about a list of YouTube videos given their video IDs. It uses the YouTube Data API to get information such as the video's title, description, statistics (e.g., view count, like count), and more.

def get_comment_details(video_ids):
    Comment_datas = []

    for video_id in video_ids:
        try:
            comment_request = youtube.commentThreads().list(part="snippet,replies",
                                                            videoId=video_id,
                                                            maxResults=50).execute()
            for i in comment_request['items']:
                comment_data = dict(Comment_id=i['id'],
                                    Video_id=i['snippet']['topLevelComment']['snippet']['videoId'],
                                    Comment_text=i['snippet']['topLevelComment']['snippet']['textDisplay'],
                                    Comment_author=i['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                                    Comment_published_date=i['snippet']['topLevelComment']['snippet']['publishedAt'])
                Comment_datas.append(comment_data)
        except Exception as e:
            print(f"An error occurred: {e}")
           
    return Comment_datas

The get_comment_details function is designed to retrieve details about comments on a list of YouTube videos, given their video IDs. This function uses the YouTube Data API to get information such as comment ID, video ID, comment text, comment author, and comment published date. 

**MongoDB Connection:**
client = pymongo.MongoClient("mongodb://localhost:27017")
mydb = client["Youtube"]

def channel_details(channel_id):
    ch_details = get_channel_info(channel_id)
    vi_ids = get_video_ids(channel_id)
    vi_info = get_video_info(vi_ids)
    com_det = get_comment_details(vi_ids)
    
    youtube = mydb["channel_details"]
    youtube.insert_one({"Channel_Details":ch_details,"Video_Ids":vi_ids,
                        "Video_Details":vi_info,"Comment_Details":com_det})
    
    return "successful"

The channel_details function is designed to gather information about a YouTube channel, its videos, and associated comments, and then store this information in a MongoDB collection named "channel_details."    

**Input:** channel_id - The ID of the YouTube channel for which you want to collect and store data.
**Processing:**
  It calls the get_channel_info function to obtain details about the specified channel.
  It calls the get_video_ids function to retrieve the video IDs associated with the channel.
  It calls the get_video_info function to gather details about the videos using the obtained video IDs.
  It calls the get_comment_details function to collect information about comments on the videos.
Storing Data:
  It creates a dictionary ({"Channel_Details": ch_details, "Video_Ids": vi_ids, "Video_Details": vi_info, "Comment_Details": com_det}) containing channel details, video IDs, video details, and comment details.
  It inserts this dictionary as a document into the "channel_details" collection of the MongoDB database.
**Output:** It returns the string "successful" to indicate that the data collection and storage were successful.


**MYSQL Database:**

Create a table in a MySQL database to store information about YouTube channels. It extracts channel details from the "channel_details" collection in the MongoDB database and creates a corresponding table in MySQL.

**To Create a Channel Table:**

def channel_table():
    myconnect = pymysql.connect(host = '****', user = '***', passwd = '***',database = 'your database name')
    cur = myconnect.cursor()

    channel_data = []
    mycol = mydb["channel_details"]

    for i in mycol.find({},{"_id":0,"Channel_Details":1}):
        channel_data.append(i["Channel_Details"])
    df = pd.DataFrame(channel_data)

    columns = ", ".join(
        f"{column_name} {dtype}"
        for column_name, dtype in zip(df.columns, df.dtypes)
    )

    drop_channel = '''drop table if exists channels'''
    cur.execute(drop_channel)
    myconnect.commit()

    sql_create_table = f"CREATE TABLE IF NOT EXISTS channels ({columns});"

    channel_data = sql_create_table.replace("object","text")

    cur.execute(channel_data)

    sql = "insert into channels values (%s,%s,%s,%s,%s,%s,%s,%s)"

    for i in range(0,len(df)):
        cur.execute(sql,tuple(df.iloc[i]))
        myconnect.commit()

**MySQL Connection:** It establishes a connection to the MySQL database using the provided credentials.

**Retrieve Channel Data from MongoDB:**

  It retrieves channel details from the "channel_details" collection in MongoDB and stores them in a Pandas DataFrame.
  
**Table Creation and Data Insertion:**

  It drops the existing "channels" table if it exists to ensure a clean slate.
  It dynamically generates the SQL statement for creating the table based on the DataFrame columns and their data types.
  It executes the CREATE TABLE statement to create the "channels" table in MySQL.
  It inserts data into the "channels" table by iterating over the DataFrame rows and executing SQL INSERT statements.     

**To Create a Video Table:**

  def video_table():
    myconnect = pymysql.connect(host = '****', user = '***', passwd = '***',database = 'your database name')
    cur = myconnect.cursor()

    video_data = []
    mycol = mydb["channel_details"]

    for entry in mycol.find({}, {"_id": 0, "Video_Details": 1}):
        video_data.extend(entry.get("Video_Details", []))
    df_video = pd.DataFrame(video_data)

    columns = ", ".join(
        f"{column_name} {dtype}"
        for column_name, dtype in zip(df_video.columns, df_video.dtypes)
    )

    drop_video = '''drop table if exists video'''
    cur.execute(drop_video)
    myconnect.commit()

    sql_create_table = f"CREATE TABLE IF NOT EXISTS video ({columns});"

    video_data = sql_create_table.replace("object","text")

    cur.execute(video_data)

    insert_video = "insert into video values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

    for i in range(0,len(df_video)):
        cur.execute(insert_video,tuple(df_video.iloc[i]))
        myconnect.commit()

The video_table function is responsible for creating a table in a MySQL database to store information about YouTube videos. It extracts video details from the "channel_details" collection in the MongoDB database and creates a corresponding table in MySQL.

**MySQL Connection:** It establishes a connection to the MySQL database using the provided credentials.

**Retrieve Video Data from MongoDB:**

  It retrieves video details from the "channel_details" collection in MongoDB and stores them in a Pandas DataFrame.
**Table Creation and Data Insertion:**

  It drops the existing "video" table if it exists to ensure a clean slate.
  It dynamically generates the SQL statement for creating the table based on the DataFrame columns and their data types.
  It executes the CREATE TABLE statement to create the "video" table in MySQL.
  It inserts data into the "video" table by iterating over the DataFrame rows and executing SQL INSERT statements.

**To Create a Comment Table:**

  def comment_table():
    myconnect = pymysql.connect(host = '****', user = '***', passwd = '***',database = 'your database name')
    cur = myconnect.cursor()
    comment_data = []
    mycol = mydb["channel_details"]

    for entry in mycol.find({}, {"_id": 0, "Comment_Details": 1}):
        comment_data.extend(entry.get("Comment_Details", []))
    df_comment = pd.DataFrame(comment_data)

    columns = ", ".join(
        f"{column_name} {dtype}"
        for column_name, dtype in zip(df_comment.columns, df_comment.dtypes)
    )

    drop_comment = '''drop table if exists comment'''
    cur.execute(drop_comment)
    myconnect.commit()

    sql_create_table = f"CREATE TABLE IF NOT EXISTS comment ({columns});"

    comment_data = sql_create_table.replace("object","text")

    cur.execute(comment_data)

    insert_comment = "insert into comment values (%s,%s,%s,%s,%s)"

    for i in range(0,len(df_comment)):
        cur.execute(insert_comment,tuple(df_comment.iloc[i]))
        myconnect.commit()       
        
The comment_table function is responsible for creating a table in a MySQL database to store information about YouTube comments. It extracts comment details from the "channel_details" collection in the MongoDB database and creates a corresponding table in MySQL.


**MySQL Connection:** It establishes a connection to the MySQL database using the provided credentials.

**Retrieve Comment Data from MongoDB:**

  It retrieves comment details from the "channel_details" collection in MongoDB and stores them in a Pandas DataFrame.
  
**Table Creation and Data Insertion:**

  It drops the existing "comment" table if it exists to ensure a clean slate.
  It dynamically generates the SQL statement for creating the table based on the DataFrame columns and their data types.
  It executes the CREATE TABLE statement to create the "comment" table in MySQL.
  It inserts data into the "comment" table by iterating over the DataFrame rows and executing SQL INSERT statements.

  Then create a Streamlit web application that allows users to log in, collect and store data from YouTube channels, and perform SQL queries on the stored data.
