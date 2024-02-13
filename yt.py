import pymongo
import mysql.connector
import pandas as pd
import streamlit as st
from pymongo import MongoClient
from googleapiclient.discovery import build

def Api_connect():
    Api_Id="AIzaSyBLH6-Uupg82OO8xpNDH_A4EwzgaDzyaxU"

    api_service_name = "youtube"
    api_version = "v3"
    youtube = build(api_service_name,api_version,developerKey=Api_Id)
    return youtube

youtube=Api_connect()

def get_channel_info(channel_id):
    
    request = youtube.channels().list(
                part = "snippet,contentDetails,Statistics",
                id = channel_id)
            
    response1=request.execute()

    for i in range(0,len(response1["items"])):
        data = dict(Channel_Name = response1["items"][i]["snippet"]["title"],
                    Channel_Id = response1["items"][i]["id"],
                    Subscription_Count= response1["items"][i]["statistics"]["subscriberCount"],
                    Views = response1["items"][i]["statistics"]["viewCount"],
                    Total_Videos = response1["items"][i]["statistics"]["videoCount"],
                    Channel_Description = response1["items"][i]["snippet"]["description"],
                    Playlist_Id = response1["items"][i]["contentDetails"]["relatedPlaylists"]["uploads"])
        return data

def get_playlist_info(channel_id):
    All_data = []
    next_page_token = None
    next_page = True
    while next_page:

        request = youtube.playlists().list(
            part="snippet,contentDetails",
            channelId=channel_id,
            maxResults=100,
            pageToken=next_page_token
            )
        response = request.execute()

        for item in response['items']: 
            data={'PlaylistId':item['id'],
                    'Title':item['snippet']['title'],
                    'ChannelId':item['snippet']['channelId'],
                    'ChannelName':item['snippet']['channelTitle'],
                    'PublishedAt':item['snippet']['publishedAt'],
                    'VideoCount':item['contentDetails']['itemCount']}
            All_data.append(data)
        next_page_token = response.get('nextPageToken')
        if next_page_token is None:
            next_page=False
    return All_data
    
def get_channel_videos(channel_id):
    video_ids = []
    res = youtube.channels().list(id=channel_id, 
                                  part='contentDetails').execute()
    playlist_id = res['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None
    
    while True:
        res = youtube.playlistItems().list( 
                                           part = 'snippet',
                                           playlistId = playlist_id, 
                                           maxResults = 50,
                                           pageToken = next_page_token).execute()
        
        for i in range(len(res['items'])):
            video_ids.append(res['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = res.get('nextPageToken')
        
        if next_page_token is None:
            break
    return video_ids

def get_video_info(video_ids):

    video_data = []

    for video_id in video_ids:
        request = youtube.videos().list(
                    part="snippet,contentDetails,statistics",
                    id= video_id)
        response = request.execute()

        for item in response["items"]:
            data = dict(Channel_Name = item['snippet']['channelTitle'],
                        Channel_Id = item['snippet']['channelId'],
                        Video_Id = item['id'],
                        Title = item['snippet']['title'],
                        Tags = item['snippet'].get('tags'),
                        Thumbnail = item['snippet']['thumbnails']['default']['url'],
                        Description = item['snippet']['description'],
                        Published_Date = item['snippet']['publishedAt'],
                        Duration = item['contentDetails']['duration'],
                        Views = item['statistics']['viewCount'],
                        Likes = item['statistics'].get('likeCount'),
                        Comments = item['statistics'].get('commentCount'),
                        Favorite_Count = item['statistics']['favoriteCount'],
                        Definition = item['contentDetails']['definition'],
                        Caption_Status = item['contentDetails']['caption']
                        )
            video_data.append(data)
    return video_data

def get_comment_info(video_ids):
        Comment_Information = []
        try:
                for video_id in video_ids:

                        request = youtube.commentThreads().list(
                                part = "snippet",
                                videoId = video_id,
                                maxResults = 50
                                )
                        response5 = request.execute()
                        
                        for item in response5["items"]:
                                comment_information = dict(
                                        Comment_Id = item["snippet"]["topLevelComment"]["id"],
                                        Video_Id = item["snippet"]["videoId"],
                                        Comment_Text = item["snippet"]["topLevelComment"]["snippet"]["textOriginal"],
                                        Comment_Author = item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                                        Comment_Published = item["snippet"]["topLevelComment"]["snippet"]["publishedAt"])

                                Comment_Information.append(comment_information)
        except:
                pass
                
        return Comment_Information
        
client = MongoClient("localhost", 27017)
db = client["youtube_data"]
collection = db["channel_detail1"]


def channel_details(channel_id):
    ch_details = get_channel_info(channel_id)
    pl_details = get_playlist_info(channel_id)
    vi_ids = get_channel_videos(channel_id)
    vi_details = get_video_info(vi_ids)
    com_details = get_comment_info(vi_ids)

    coll1 = db["channel_detail1"]
    coll1.insert_one({"channel_information":ch_details,"playlist_information":pl_details,"video_information":vi_details,
                     "comment_information":com_details})
    
    return "upload completed successfully"

def channels_table():
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="Vishnu_14",
        database="youtube_data",
        port="3306"
    )
    cursor = mydb.cursor()

    drop_query = "DROP TABLE IF EXISTS channels"
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_query = '''CREATE TABLE IF NOT EXISTS channels (
            Channel_Name VARCHAR(100),
            Channel_Id VARCHAR(80) PRIMARY KEY,
            Subscription_Count BIGINT,
            Views BIGINT,
            Total_Videos INT,
            Channel_Description TEXT,
            Playlist_Id VARCHAR(50)
        )'''
        cursor.execute(create_query)
        mydb.commit()
    except:
        st.write("Channels Table already created")
 
    ch_list = []
    db = client["youtube_data"]
    coll1 = db["channel_detail1"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df = pd.DataFrame(ch_list)
    
    for index,row in df.iterrows():
        insert_query = '''INSERT into channels(Channel_Name,
                                                    Channel_Id,
                                                    Subscription_Count,
                                                    Views,
                                                    Total_Videos,
                                                    Channel_Description,
                                                    Playlist_Id)
                                        VALUES(%s,%s,%s,%s,%s,%s,%s)'''
            

        values =(
                row['Channel_Name'],
                row['Channel_Id'],
                row['Subscription_Count'],
                row['Views'],
                row['Total_Videos'],
                row['Channel_Description'],
                row['Playlist_Id'])
        try:                     
            cursor.execute(insert_query,values)
            mydb.commit()    
        except:
            st.write("Channels values are already inserted")

            
def playlists_table():
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="Vishnu_14",
        database="youtube_data",
        port="3306"
    )
    cursor = mydb.cursor()

    drop_query = "drop table if exists playlists"
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_query = '''create table if not exists playlists(PlaylistId varchar(100) primary key,
                        Title varchar(80), 
                        ChannelId varchar(100), 
                        ChannelName varchar(100),
                        PublishedAt timestamp,
                        VideoCount int
                        )'''
        cursor.execute(create_query)
        mydb.commit()
    except:
        st.write("Playlists Table alredy created")    

    pl_list = []
    db = client["youtube_data"]
    coll1 =db["channel_detail1"]
    for pl_data in coll1.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(pl_data["playlist_information"])):
                pl_list.append(pl_data["playlist_information"][i])
    df = pd.DataFrame(pl_list)
    
    for index,row in df.iterrows():
        insert_query = '''INSERT into playlists(PlaylistId,
                                                    Title,
                                                    ChannelId,
                                                    ChannelName,
                                                    PublishedAt,
                                                    VideoCount)
                                        VALUES(%s,%s,%s,%s,%s,%s)'''            
        values =(
                row['PlaylistId'],
                row['Title'],
                row['ChannelId'],
                row['ChannelName'],
                row['PublishedAt'],
                row['VideoCount'])
                
        try:                     
            cursor.execute(insert_query,values)
            mydb.commit()    
        except:
            st.write("Playlists values are already inserted")

def videos_table():

    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="Vishnu_14",
        database="youtube_data",
        port="3306"
    )
    cursor = mydb.cursor()

    try:
        create_query = '''create table if not exists videos(
                        Channel_Name varchar(150),
                        Channel_Id varchar(100),
                        Video_Id varchar(50) primary key, 
                        Title varchar(150), 
                        Tags text,
                        Thumbnail varchar(225),
                        Description text, 
                        Published_Date timestamp,
                        Duration varchar(20), 
                        Views bigint, 
                        Likes bigint,
                        Dislikes bigint,
                        Comments int,
                        Favorite_Count int, 
                        Definition varchar(10), 
                        Caption_Status varchar(50) 
                        )''' 
                        
        cursor.execute(create_query)             
        mydb.commit()
    except:
        st.write("Videos Table already created")

    vi_list = []
    db = client["youtube_data"]
    coll1 = db["channel_detail1"]
    for vi_data in coll1.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    df2 = pd.DataFrame(vi_list)
        
    
    for index, row in df2.iterrows():
        insert_query = '''
                    INSERT INTO videos (Channel_Name,
                        Channel_Id,
                        Video_Id, 
                        Title, 
                        Tags,
                        Thumbnail,
                        Description, 
                        Published_Date,
                        Duration, 
                        Views, 
                        Likes,
                        Dislikes,
                        Comments,
                        Favorite_Count, 
                        Definition, 
                        Caption_Status 
                        )VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''    
                        
        values = (
                    row['Channel_Name'],
                    row['Channel_Id'],
                    row['Video_Id'],
                    row['Title'],
                    row['Tags'],
                    row['Thumbnail'],
                    row['Description'],
                    row['Published_Date'],
                    row['Duration'],
                    row['Views'],
                    row['Likes'],
                    row['Comments'],
                    row['Favorite_Count'],
                    row['Definition'],
                    row['Caption_Status'])                 
        try:    
            cursor.execute(insert_query,values)
            mydb.commit()
        except:
            st.write("videos values already inserted in the table")
        

def comments_table():
    
    mydb = mysql.connector.connect(
            host="localhost",
            user="root",
            password="Vishnu_14",
            database="youtube_data",
            port="3306"
        )
    cursor = mydb.cursor()

    drop_query = "drop table if exists comments"
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_query = '''CREATE TABLE if not exists comments(Comment_Id varchar(100) primary key,
                       Video_Id varchar(80),
                       Comment_Text text, 
                       Comment_Author varchar(150),
                       Comment_Published timestamp)'''
        cursor.execute(create_query)
        mydb.commit()
        
    except:
        st.write("Commentsp Table already created")

    com_list = []
    db = client["youtube_data"]
    coll1 = db["channel_detail1"]
    for com_data in coll1.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
    df3 = pd.DataFrame(com_list)


    for index, row in df3.iterrows():
            insert_query = '''
                INSERT INTO comments (Comment_Id,
                                      Video_Id ,
                                      Comment_Text,
                                      Comment_Author,
                                      Comment_Published)
                VALUES (%s, %s, %s, %s, %s)

            '''
            values = (
                row['Comment_Id'],
                row['Video_Id'],
                row['Comment_Text'],
                row['Comment_Author'],
                row['Comment_Published']
            )
            try:
                cursor.execute(insert_query,values)
                mydb.commit()
            except:
               st.write("This comments are already exist in comments table")

def tables():
    channels_table()
    playlists_table()
    videos_table()
    comments_table()
    return "Tables Created successfully"
    
def show_channels_table():
    ch_list = []
    db = client["youtube_data"]
    coll1 = db["channel_detail1"] 
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    channels_table = st.dataframe(ch_list)
    return channels_table

def show_playlists_table():
    db = client["youtube_data"]
    coll1 =db["channel_detail1"]
    pl_list = []
    for pl_data in coll1.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(pl_data["playlist_information"])):
                pl_list.append(pl_data["playlist_information"][i])
    playlists_table = st.dataframe(pl_list)
    return playlists_table

def show_videos_table():
    vi_list = []
    db = client["youtube_data"]
    coll2 = db["channel_detail1"]
    for vi_data in coll2.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    videos_table = st.dataframe(vi_list)
    return videos_table

def show_comments_table():
    com_list = []
    db = client["youtube_data"]
    coll3 = db["channel_detail1"]
    for com_data in coll3.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
    comments_table = st.dataframe(com_list)
    return comments_table

st.title(":red[YouTube Data Harvesting and Warehousing]")
channel_id = st.text_input("Enter the Channel id")
channels = channel_id.split(',')
channels = [ch.strip() for ch in channels if ch]

if st.button("Upload to MongoDB"):
    for channel in channels:
        ch_ids = []
        db = client["youtube_data"]
        coll1 = db["channel_detail1"]
        for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
            ch_ids.append(ch_data["channel_information"]["Channel_Id"])
        if channel in ch_ids:
            st.success("Channel details of the given channel id: " + channel + " already exists")
        else:
            output = channel_details(channel)
            st.success(output)
            
client = pymongo.MongoClient("localhost", 27017)
db = client["youtube_data"]
collection = db["channel_detail1"]

mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Vishnu_14",
    database="youtube_data",
    port="3306"
)
cursor = mydb.cursor()

def list_channel_details():
    channel_details = []
    for doc in collection.find({}, {"_id": 0, "channel_information": 1}):
        channel_details.append(doc["channel_information"])
    return channel_details

def upload_to_mysql(channel_id):
    channel_data = collection.find_one({"channel_information.Channel_Id": channel_id}, {"_id": 0})

    if channel_data is not None:
        channel_info = channel_data.get("channel_information")  
        playlist_info = channel_data.get("playlist_information")
        video_info = channel_data.get("video_information")
        comment_info = channel_data.get("comment_information")

    else:
        print("Channel data not found for channel id:", channel_id)
        
def main():
    
    channel_details = list_channel_details()
    
    st.write("## Channel Details from MongoDB")
    channel_df = pd.DataFrame(channel_details)
    st.write(channel_df)
    
    st.write("## Select a Channel to Upload to MySQL")
    selected_channel = st.selectbox("Select Channel", channel_df["Channel_Name"])
    
    if st.button("Upload to MySQL"):
        upload_to_mysql(selected_channel)
        st.success("Channel data uploaded to MySQL successfully!")
        
if __name__ == "__main__":
    main()
   
mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="Vishnu_14",
        database="youtube_data",
        port="3306"
    )
cursor = mydb.cursor()
    
question = st.selectbox(
    'Please Select Your Question',
    ('1. What are the names of all the videos and their corresponding channels?',
     '2. Which channels have the most number of videos, and how many videos do they have?',
     '3. What are the top 10 most viewed videos and their respective channels?',
     '4. How many comments were made on each video, and what are their corresponding video names?',
     '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
     '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
     '7. What is the total number of views for each channel, and what are their corresponding channel names?',
     '8. What are the names of all the channels that have published videos in the year 2022?',
     '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
     '10. Which videos have the highest number of comments, and what are their corresponding channel names?'))

     
if question == '1. What are the names of all the videos and their corresponding channels?':
    query1 = '''SELECT Title, Channel_Name FROM videos;'''
    cursor.execute(query1)
    t1 = cursor.fetchall()
    st.write(pd.DataFrame(t1, columns=["Video Title", "Channel Name"]))

elif question == '2. Which channels have the most number of videos, and how many videos do they have?':
    query2 = "SELECT Channel_Name, COUNT(*) AS Video_Count FROM channels GROUP BY Channel_Name ORDER BY Video_Count DESC LIMIT 1;"
    cursor.execute(query2)
    t2 = cursor.fetchall()
    st.write(pd.DataFrame(t2, columns=["Channel Name", "No Of Videos"]))

elif question == '3. What are the top 10 most viewed videos and their respective channels?':
    query3 = "SELECT Title, Channel_Name, Views FROM videos ORDER BY Views DESC LIMIT 10;"
    cursor.execute(query3)
    t3 = cursor.fetchall()
    st.write(pd.DataFrame(t3, columns=["Video Title", "Channel Name", "Views"]))

elif question == '4. How many comments were made on each video, and what are their corresponding video names?':
    query4 = "SELECT Title, Comments FROM videos;"
    cursor.execute(query4)
    t4 = cursor.fetchall()
    st.write(pd.DataFrame(t4, columns=["Video Title", "No Of Comments"]))

elif question == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
    query5 = "SELECT Title, Channel_Name, Likes FROM videos ORDER BY Likes DESC LIMIT 1;"
    cursor.execute(query5)
    t5 = cursor.fetchall()
    st.write(pd.DataFrame(t5, columns=["Video Title", "Channel Name", "Like Count"]))

elif question == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
    query6 = "SELECT Title, SUM(Likes) AS Total_Likes, SUM(Dislikes) AS Total_Dislikes FROM videos GROUP BY Title;"
    cursor.execute(query6)
    t6 = cursor.fetchall()
    st.write(pd.DataFrame(t6, columns=["Video Title", "Total Likes", "Total Dislikes"]))

elif question == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
    query7 = "SELECT Channel_Name, SUM(Views) AS Total_Views FROM channels GROUP BY Channel_Name;"
    cursor.execute(query7)
    t7 = cursor.fetchall()
    st.write(pd.DataFrame(t7, columns=["Channel Name", "Total Views"]))

elif question == '8. What are the names of all the channels that have published videos in the year 2022?':
    query8 = "SELECT DISTINCT Channel_Name FROM videos WHERE EXTRACT(YEAR FROM Published_Date) = 2022;"
    cursor.execute(query8)
    t8 = cursor.fetchall()
    st.write(pd.DataFrame(t8, columns=["Channel Name"]))

elif question == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
    query9 =  "SELECT Channel_Name, AVG(Duration) AS Average_Duration FROM videos GROUP BY Channel_Name;"
    cursor.execute(query9)
    t9 = cursor.fetchall()
    avg_durations = [{"Channel Name": row[0], "Average Duration": str(row[1]) + " seconds"} for row in t9]
    st.write(pd.DataFrame(t9,columns=["Channel Name","Average_Duration"]))

elif question == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
    query10 = "SELECT Title, Channel_Name, Comments FROM videos ORDER BY Comments DESC LIMIT 1;"
    cursor.execute(query10)
    t10 = cursor.fetchall()
    st.write(pd.DataFrame(t10, columns=["Video Title", "Channel Name", "No Of Comments"]))
