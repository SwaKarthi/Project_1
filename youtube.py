from googleapiclient.discovery import build
import pymongo
import pymysql
import pandas as pd
import streamlit as st
import time
from streamlit import session_state



client = pymongo.MongoClient("mongodb://localhost:27017")
mydb = client["Youtube"]

myconnect = pymysql.connect(host = '127.0.0.1', user = 'root', passwd = 'Sw@30',database = 'youtube')
cur = myconnect.cursor()


def api_connect():
    api_key = 'AIzaSyAcuhECMkyCKNO99-1xv0i03uHd-v2l1o0'
    
    api_service_name = "youtube"
    api_version = "v3"
    youtube =  build(api_service_name,api_version,developerKey = api_key)
    return youtube

youtube = api_connect()

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


#Mongodb Connection
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

#SQL Connect

def channel_table():
    myconnect = pymysql.connect(host = '127.0.0.1', user = 'root', passwd = 'Sw@30',database = 'youtube')
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

def video_table():
    myconnect = pymysql.connect(host = '127.0.0.1', user = 'root', passwd = 'Sw@30',database = 'youtube')
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


def comment_table():
    myconnect = pymysql.connect(host = '127.0.0.1', user = 'root', passwd = 'Sw@30',database = 'youtube')
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

def sql_tables():
    channel_table()
    video_table()
    comment_table()
    return "Tables Created Successfully"

#streamlit 
def show_channel_table():
    channel_data = []
    mydb = client["Youtube"]
    mycol = mydb["channel_details"]
    for i in mycol.find({},{"_id":0,"Channel_Details":1}):
        channel_data.append(i["Channel_Details"])
    df = st.dataframe(channel_data)

    return df

def show_video_table():
    video_data = []
    mydb = client["Youtube"]
    mycol = mydb["channel_details"]

    for entry in mycol.find({}, {"_id": 0, "Video_Details": 1}):
        video_data.extend(entry.get("Video_Details", []))
    df_video = st.dataframe(video_data)

    return df_video

def show_comment_table():
    comment_data = []
    mydb = client["Youtube"]
    mycol = mydb["channel_details"]

    for entry in mycol.find({}, {"_id": 0, "Comment_Details": 1}):
        comment_data.extend(entry.get("Comment_Details", []))
    df_comment = st.dataframe(comment_data)
   
    return df_comment



def check_login(username, password, stored_user):
    return username == stored_user["username"] and password == stored_user["password"]

def Display_after_login(stored_user):
    st.write(f"Welcome,{stored_user['username']}!")
        
    Channel_id = st.text_input("Enter the Channel ID")
   
    insert = None
    
    if st.button("Collect and Store Data"):
        with st.spinner("In Progress"):
            time.sleep(2)
        
        channel_ids = []
        mydb = client["Youtube"]
        coll1 = mydb["channel_details"]
        
        for i in coll1.find({},{"_id":0,"Channel_Details":1}):
            channel_ids.append(i["Channel_Details"]["Channel_id"])
            
        if Channel_id in channel_ids:
            st.success("Given Channel Id already exists") 
        else:
            insert = channel_details(Channel_id)
            st.success(insert)
    if insert:
        st.success("Channel details successfully inserted")

    
    
    
    st.subheader(":red[SQL Tables]",divider="rainbow")


    if st.button("Migrate to SQL"):
        Table=sql_tables()
        st.success(Table)

    SQL = st.selectbox("Select the table to view",("Channel_table",
                                                    "Video Table",
                                                    "Comment Table"))
            
    if SQL == "Channel_table":
        channel_list = show_channel_table()
    elif SQL == "Video Table":
        video_list = show_video_table()
    elif SQL == "Comment Table":
        comment_list = show_comment_table()

    st.subheader(":red[Question and Answers]",divider="rainbow")
    st.markdown(":blue[**Question No:1 What are the names of all the videos and their corresponding channels?**]")
    st.markdown(":green[**Answer:**]")
    ans1 = '''select video_name,channel_name from video'''
    cur.execute(ans1)
    myconnect.commit()
    t1 = cur.fetchall()
    df_ans1 = pd.DataFrame(t1,columns = ["Video Title","Channel_name"])
    st.write(df_ans1)

    st.markdown(":blue[**Question No:2 Which channels have the most number of videos, and how many videos do they have?**]")
    st.markdown(":green[**Answer:**]")
    ans2 = '''select video_counts,channel_name from channels order by cast(video_counts as signed) desc'''
    cur.execute(ans2)
    myconnect.commit()
    t2 = cur.fetchall()
    df_ans2 = pd.DataFrame(t2,columns = ["Channel_name","No of Videos"])
    st.write(df_ans2)

    st.markdown(":blue[**Question No:3 What are the top 10 most viewed videos and their respective channels?**]")
    st.markdown(":green[**Answer:**]")
    ans3 = '''select view_count,channel_name,video_name from video order by cast(view_count as signed) desc limit 10'''
    cur.execute(ans3)
    myconnect.commit()
    t3 = cur.fetchall()
    df_qus3 = pd.DataFrame(t3,columns = ["Views","Channel_name","Video Title"])
    st.write(df_qus3)

    st.markdown(":blue[**Question No:4 How many comments were made on each video, and what are their corresponding video names?**]")
    st.markdown(":green[**Answer:**]")
    ans4 = '''select comment_count,video_name from video where cast(comment_count as signed) is not null'''
    cur.execute(ans4)
    myconnect.commit()
    t4 = cur.fetchall()
    df_ans4 = pd.DataFrame(t4,columns = ["No of comments","Video Title"])
    st.write(df_ans4)

    st.markdown(":blue[**Question No:5 Which videos have the highest number of likes, and what are their corresponding channel names?**]")
    st.markdown(":green[**Answer:**]")
    ans5 = '''select like_count,channel_name from video order by CAST(like_count AS signed) desc limit 10'''
    cur.execute(ans5)
    myconnect.commit()
    t5 = cur.fetchall()
    df_ans5 = pd.DataFrame(t5,columns = ["No of likes","Channel_name"])
    st.write(df_ans5)

    st.markdown(":blue[**Question No:6 What is the total number of likes and dislikes for each video, and what are their corresponding video names?**]")
    st.markdown(":green[**Answer:**]")
    ans6 = '''select like_count,channel_name,video_name from video order by cast(like_count as signed) desc'''
    cur.execute(ans6)
    myconnect.commit()
    t6 = cur.fetchall()
    df_ans6 = pd.DataFrame(t6,columns = ["No of likes","Channel_name","Video Title"])
    st.write(df_ans6)

    st.markdown(":blue[**Question No:7 What is the total number of views for each channel, and what are their corresponding channel names?**]")
    st.markdown(":green[**Answer:**]")
    ans7 = '''select channel_views,channel_name from channels'''
    cur.execute(ans7)
    myconnect.commit()
    t7 = cur.fetchall()
    df_ans7 = pd.DataFrame(t7,columns = ["No of views","Channel_name"])
    st.write(df_ans7)

    st.markdown(":blue[**Question No:8 What are the names of all the channels that have published videos in the year 2022?**]")
    st.markdown(":green[**Answer:**]")
    ans8 = '''select video_name,published_date, channel_name from video where year(published_date) = 2022'''
    cur.execute(ans8)
    myconnect.commit()
    t8 = cur.fetchall()
    df_ans8 = pd.DataFrame(t8,columns = ["Video Title", "published_date","Channel_name"])
    st.write(df_ans8)

    st.markdown(":blue[**Question No:9 What is the average duration of all videos in each channel, and what are their corresponding channel names?**]")
    st.markdown(":green[**Answer:**]")
    ans9 = '''SELECT channel_name, SEC_TO_TIME(AVG((SUBSTR(REPLACE(duration, "PT", ""), 1, INSTR(REPLACE(duration, "PT", ""), "M") - 1) * 60 +
            REPLACE(SUBSTR(REPLACE(duration, "PT", ""), INSTR(REPLACE(duration, "PT", ""), "M") + 1,INSTR(REPLACE(duration, "PT", ""), "S")),"S","")))) AS Average_Duration
            FROM video GROUP BY channel_name;'''
    cur.execute(ans9)
    myconnect.commit()
    t9 = cur.fetchall()
    df_ans9 = pd.DataFrame(t9,columns = ["Channel_name","Average Duration"])
    st.write(df_ans9)

    st.markdown(":blue[**Question No:10 Which videos have the highest number of comments, and what are their corresponding channel names?**]")
    st.markdown(":green[**Answer:**]")
    ans10 = '''select channel_name,video_name,comment_count from video where comment_count is not null order by cast(comment_count as signed) desc'''
    cur.execute(ans10)
    myconnect.commit()
    t10 = cur.fetchall()
    df_ans10 = pd.DataFrame(t10,columns = ["Channel_name","Video Title","Comment count"])
    st.write(df_ans10)




def main():
    if 'logged_in' not in st.session_state:
        st.session_state.logged_in = False

    if 'stored_user' not in st.session_state:
        st.session_state.stored_user = {"username": None, "password": None}

    if 'page' not in st.session_state:
        st.session_state.page = "login_page"    

    username = None
    password = None
    stored_user = st.session_state.stored_user  

    
    if not st.session_state.logged_in:
        st.title(":red[Youtube Data Harvesting and Warehousing]")
        
       
        st.header("Login")
        st.markdown("Please enter your credentials to log in:")
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")
        login_button = st.button("Login")

        if login_button:
            st.session_state.stored_user = {"username": username, "password": password}
            if check_login(username, password, st.session_state.stored_user):
                st.session_state.logged_in = True
                st.session_state.page = "display_page"
                st.success("Login successful!")
            else:
                st.error("Invalid username or password.")
    else:
        st.write(f"Logged in as {stored_user['username']}")
        logout_button = st.button("Logout")

        if logout_button:
            st.session_state.logged_in = False
            st.session_state.page = "login_page"
            st.success("Logout successful!")
        
    if st.session_state.page == "display_page":
        Display_after_login(st.session_state.stored_user)
    elif st.session_state.page == "login_page":
        
        st.markdown(":green[Welcome to the YouTube Harvesting and Warehousing App!]")
        st.markdown("Please log in to access the features.")

if __name__ == "__main__":
    main()