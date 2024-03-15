#import packages
import streamlit as st
import googleapiclient.discovery
import pymongo
import mysql.connector as sql
import pandas as pd
import isodate
from datetime import datetime

#youtube
api_key = 'AIzaSyDYqmIc6uGky8SBSX2cK6QQ2LrXTWoBJnY'
api_service_name = "youtube"
api_version = "v3"
youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=api_key)

# sql connection
mydb=sql.connect(host="localhost",user="root",password="root",database= "youtube_data",port = "3306")
cursor=mydb.cursor(buffered=True)
#mongodb connection
mongo_url=pymongo.MongoClient( "mongodb://raj:guvi2024@ac-nr8vpka-shard-00-00.vrmcz0d.mongodb.net:27017,ac-nr8vpka-shard-00-01.vrmcz0d.mongodb.net:27017,ac-nr8vpka-shard-00-02.vrmcz0d.mongodb.net:27017/?ssl=true&replicaSet=atlas-oe657d-shard-0&authSource=admin&retryWrites=true&w=majority&appName=Cluster1")

#streamlit page
st.set_page_config(
    page_title="Streamlit App",
    page_icon="▶️",
    layout="wide",
    initial_sidebar_state="auto")


st.title("YouTube Data Harvesting and Warehousing")
user_input = st.text_input("Enter Channel Id:")

#function channel_details
def channel_details(channel_id):
  request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id
    )
  response = request.execute()
  datetime_str = response['items'][0]['snippet']['publishedAt']
  try:
    datetime_obj = datetime.strptime(datetime_str, '%Y-%m-%dT%H:%M:%S.%fZ')
  except ValueError:
    datetime_obj = datetime.strptime(datetime_str, '%Y-%m-%dT%H:%M:%SZ')
  data = {"Channel_Id" : response['items'][0]['id'],
          "Channel_name" : response['items'][0]['snippet']['title'],          
          "Description":response['items'][0]['snippet'].get('description'),
          "Published_at" : datetime_obj,
          "Playlist_id" : response['items'][0]['contentDetails']['relatedPlaylists']['uploads'],
          "Subscriber_count" : response['items'][0]['statistics']['subscriberCount'],
          "video_count" : response['items'][0]['statistics']['videoCount'],
          "View_count" : response['items'][0]['statistics']['viewCount']}
  return data

#function video_ids
def get_videos_ids(channel_id):
  video_ids=[]
  resp=youtube.channels().list(id=channel_id,part='contentDetails').execute()
  Playlist_id=resp['items'][0]['contentDetails']['relatedPlaylists']['uploads']
  next_page_token=None
  while True:
    response1=youtube.playlistItems().list(part='snippet',
                                           playlistId=Playlist_id,
                                           maxResults=50,
                                           pageToken=next_page_token).execute()

    for i in range(len(response1['items'])):
        video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
    next_page_token=response1.get("nextPageToken")
    if next_page_token is None:
       break
  return video_ids

#function video_info
def get_video_info(VideoIds):
  video_datas=[]
  for video_id in VideoIds:
      request=youtube.videos().list(part="snippet, ContentDetails, statistics",id=video_id)
      response=request.execute()
      tag=response['items'][0]['snippet'].get("tags",[])
      t=",".join(tag)
      datetime_str = response['items'][0]['snippet']['publishedAt']
      try:
        datetime_obj = datetime.strptime(datetime_str, '%Y-%m-%dT%H:%M:%SZ')
      except ValueError:
        datetime_obj = datetime.strptime(datetime_str, '%Y-%m-%dT%H:%M:%S.%fZ')

      Duration=response['items'][0]['contentDetails']['duration']
      duration = isodate.parse_duration(Duration)
      user = str(duration).replace('0:0:0', '')
      for item in response["items"]:
        data=dict(Channel_Name=item['snippet']['channelTitle'], Channel_Id=item['snippet']['channelId'],
                  Video_Id=item['id'], Title=item['snippet']['title'],Tags=t,Thumbnail=item['snippet']['thumbnails']['default']['url'],
                  Description=item['snippet'].get('description'), Published_Date=datetime_obj, Duration=user,
                  Views=item['statistics']['viewCount'], likes=item['statistics'].get('likeCount'),Comments=item['statistics'].get('commentCount'),Favorite_Count=item['statistics'].get('favoriteCount'),
                  Caption_Status=item['contentDetails']['caption'])
        video_datas.append(data)
  return video_datas

#function comments
def get_comment_info(VideoIds):
  Comment_data=[] 
  try:
    for video_id in VideoIds:
    #   next_page_token=None
    #   while True:
        request=youtube.commentThreads().list(part="snippet",videoId=video_id,maxResults=50) #,pageToken=next_page_token)
        response=request.execute()
        datetime_str = response['items'][0]['snippet']['topLevelComment']['snippet']['publishedAt']
        try:
            datetime_obj = datetime.strptime(datetime_str, '%Y-%m-%dT%H:%M:%SZ')
        except ValueError:
            datetime_obj = datetime.strptime(datetime_str, '%Y-%m-%dT%H:%M:%S.%fZ')
        for item in response['items']:
            data=dict(Comment_Id=item['snippet']['topLevelComment']['id'],Video_Id=item['snippet']['topLevelComment']['snippet']['videoId'], 
                    Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    Comment_Published=datetime_obj)
            Comment_data.append(data)
         
        # if 'nextPageToken' in response:
        #             page_token = response['nextPageToken']  # Set page token for next request
        # else:
        #     break        
  except:
    pass
  return Comment_data

#function transfer channel
def transfer_channel():    
    db=mongo_url['YouTube_Data']
    col=db['channel']
    bj=channel_details(user_input)
    col.insert_one(bj)

#function transfer videos
def transfer_videos():
    db=mongo_url['YouTube_Data']
    col1=db['video']
    v_id=get_videos_ids(user_input)
    video_details=get_video_info(v_id)
    col1.insert_many(video_details)

#function transfer comments
def transfer_comments():    
    db=mongo_url['YouTube_Data']
    col2=db['comments']
    v_id=get_videos_ids(user_input)
    comment_details=get_comment_info(v_id)
    col2.insert_many(comment_details)

#function st.session_state
def is_transfer_videos_done():
    if 'transfer_videos_done' not in st.session_state:
        st.session_state.transfer_videos_done = False
    return st.session_state.transfer_videos_done

def is_transfer_comments_done():
    if 'transfer_comments_done' not in st.session_state:
        st.session_state.transfer_comments_done = False
    return st.session_state.transfer_comments_done
  
#function migrate channel
def migrate_channel():    
    db=mongo_url['YouTube_Data']
    col=db['channel']
    cursor.execute("DROP TABLE IF EXISTS Channel")
    cursor.execute("""
        CREATE TABLE Channel (
            Channel_Id VARCHAR(255),Channel_name VARCHAR(255),Description TEXT,Published_at DATETIME,
                   Playlist_id VARCHAR(255),Subscriber_count INT,video_count INT,View_count INT,PRIMARY KEY(Channel_Id))""")
    for document in col.find({}, {"_id": 0}):
        cursor.execute("""
            INSERT INTO Channel (
                Channel_Id,Channel_name,Description,Published_at,Playlist_id,Subscriber_count, video_count,
                       View_count) VALUES 
                       ( %s, %s, %s, %s, %s, %s, %s,%s)""",                 
            (document["Channel_Id"],document["Channel_name"],document["Description"], document["Published_at"], 
              document["Playlist_id"], document["Subscriber_count"],
            document["video_count"], document["View_count"] ))
        mydb.commit()

#function migrate video
def migrate_video():
    db=mongo_url['YouTube_Data']
    col=db['video']
    cursor.execute("DROP TABLE IF EXISTS video")
    cursor.execute("""
        CREATE TABLE video (
            Channel_Name VARCHAR(255),Channel_Id VARCHAR(255),Video_Id VARCHAR(255),Title TEXT,Tags TEXT,Thumbnail VARCHAR(255),
            Description TEXT,Published_Date DATETIME,Duration TIME,Views INT,likes INT,Comments INT,Favorite_Count INT,
            Caption_Status VARCHAR(255))""")
    for document in col.find({}, {"_id": 0}):
        cursor.execute("""
            INSERT INTO video (
                Channel_Name, Channel_Id, Video_Id, Title, Tags, Thumbnail,Description, Published_Date, Duration, 
                       Views, likes, Comments,Favorite_Count, Caption_Status) VALUES 
                       ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",                 
            (document["Channel_Name"], document["Channel_Id"], document["Video_Id"], document["Title"],
            document["Tags"], document["Thumbnail"], document["Description"], document["Published_Date"],
            document["Duration"], document["Views"], document["likes"], document["Comments"],
            document["Favorite_Count"], document["Caption_Status"]
        ))
        mydb.commit()

#function migrate comments
def migrate_comments():
    db=mongo_url['YouTube_Data']  
    col=db['comments']
    cursor.execute("DROP TABLE IF EXISTS comments")
    cursor.execute("""
        CREATE TABLE comments (
            Comment_Id VARCHAR(255),Video_Id VARCHAR(255),Comment_Text TEXT,Comment_Author VARCHAR(255), 
                 Comment_Published DATETIME)""")
    for document in col.find({}, {"_id": 0}):
        cursor.execute("""
            INSERT INTO comments (
                Comment_Id,Video_Id,Comment_Text,Comment_Author,Comment_Published) VALUES 
                       ( %s, %s, %s, %s, %s)""",                 
            (document["Comment_Id"],document["Video_Id"],document["Comment_Text"],document["Comment_Author"], 
              document["Comment_Published"] ))
        mydb.commit()

#function queries 10
def Query1(): 
      query1=""" SELECT Channel_name, Title FROM video"""
      cursor.execute(query1)
      r1=cursor.fetchall()
      c1=cursor.column_names
      df1=pd.DataFrame(r1,columns=c1)
      st.write(df1)

def Query2():
      query2="""SELECT Channel_Name , count(Channel_Name) as No_of_videos FROM video GROUP BY Channel_Name ORDER BY No_of_videos DESC
"""
      cursor.execute(query2)
      r2=cursor.fetchall()
      c2=cursor.column_names
      df2=pd.DataFrame(r2,columns=c2)
      st.write(df2)

def Query3():
      query3="""SELECT Channel_Name,Title,Views FROM video ORDER BY Views DESC LIMIT 10"""
      cursor.execute(query3)
      r3=cursor.fetchall()
      c3=cursor.column_names
      df3=pd.DataFrame(r3,columns=c3)
      st.write(df3)

def Query4():
      query4="""SELECT Title AS Video_names ,Comments AS comment_count FROM video"""
      cursor.execute(query4)
      r4=cursor.fetchall()
      c4=cursor.column_names
      df4=pd.DataFrame(r4,columns=c4)
      st.write(df4)

def Query5():
      query5="""SELECT Channel_Name,Title AS Video_names ,likes FROM video ORDER BY likes DESC"""
      cursor.execute(query5)
      r5=cursor.fetchall()
      c5=cursor.column_names
      df5=pd.DataFrame(r5,columns=c5)
      st.write(df5)

def Query6():
      st.write("YouTube has hidden the dislike count")
      query6="""SELECT Channel_Name,Title AS Video_names ,likes FROM video"""
      cursor.execute(query6)
      r6=cursor.fetchall()
      c6=cursor.column_names
      df6=pd.DataFrame(r6,columns=c6)
      st.write(df6)

def Query7():
      query7="""SELECT Channel_Name, View_count FROM channel"""
      cursor.execute(query7)
      r7=cursor.fetchall()
      c7=cursor.column_names
      df7=pd.DataFrame(r7,columns=c7)
      st.write(df7)

def Query8():
      query8="""SELECT Channel_Name, count(Channel_Name) AS Total_videos FROM video WHERE YEAR(Published_Date)=2022 GROUP by Channel_Name"""
      cursor.execute(query8)
      r8=cursor.fetchall()
      c8=cursor.column_names
      df8=pd.DataFrame(r8,columns=c8)
      st.write(df8)

def Query9():
      query9="""SELECT Channel_Name,avg(TIME_TO_SEC(Duration)) AS Duration_in_seconds FROM video GROUP BY Channel_Name"""
      cursor.execute(query9)
      r9=cursor.fetchall()
      c9=cursor.column_names
      df9=pd.DataFrame(r9,columns=c9)
      st.write(df9)

def Query10():
      query10="""SELECT Channel_Name, Title AS Video_names ,Comments AS comment_count FROM video ORDER BY Comments DESC"""
      cursor.execute(query10)
      r10=cursor.fetchall()
      c10=cursor.column_names
      df10=pd.DataFrame(r10,columns=c10)
      st.write(df10)

#streamlit tab
tab1, tab2, tab3, tab4 = st.tabs(["$\large📝COLLECT DATA📝 $", "$\large🍃TRANSFER🍃 $", "$\large🐬MIGRATE🐬  $", "$\large📊VIEW📊 $" ])

#tab collect data
with tab1:
    st.header("Collection of data page")
    if st.button("Collect Channel Details"):
        bj=channel_details(user_input)
        st.write(' Extracted data from channel ')
        st.table(bj)

    if st.button("Collect Video Ids"):
        v_id=get_videos_ids(user_input)
        vid_df=pd.DataFrame(v_id)
        st.write(vid_df)

    if st.button("Collect Video Details"):
        v_id=get_videos_ids(user_input)
        video_details=get_video_info(v_id)
        df_v=pd.DataFrame(video_details)
        st.write(' Extracted data from videos ')
        st.write(df_v)

    if st.button("Collect Comment Details"):
        v_id=get_videos_ids(user_input)
        comment_details=get_comment_info(v_id)
        df_c=pd.DataFrame(comment_details)
        st.write(' Extracted data from comments ')
        st.write(df_c)

#tab transfer
with tab2:
    st.header("Transfer datas to mongodb page")
    if st.button("Transfer Channel to mongodb"):
        ids=[]
        db=mongo_url['YouTube_Data']
        col=db['channel']
        for ids_c in col.find({},{"_id":0,"Channel_Id":1}):
            ids.append(ids_c['Channel_Id'])

        if user_input in ids:
           st.error("Channel Id already exists")
        else:
            tc=transfer_channel()
            st.success("Channel details uploaded to mongodb successfully!!!")

    if st.button("Transfer Videos to mongodb"):
        if not is_transfer_videos_done():
            tv=transfer_videos()
            st.session_state.transfer_videos_done = True
            st.success("Video details uploaded to mongodb successfully!!!")
        else:
           st.error("Details already transformed!!!")
       
        
    if st.button("Transfer Comments to mongodb"):
        if not is_transfer_comments_done():
            tco=transfer_comments()
            st.session_state.transfer_comments_done = True
            st.success("Comments detail uploaded to mongodb successfully!!!")
        else:
            st.error("Details already transformed!!!")

#tab migrate
with tab3:
   st.header("Migrate datas to MySQL page")
   if st.button("Migrate Channel"):
      try:
          migrate_channel()
          st.success("Channel details uploaded to mySQL successfully!!!")
      except:
          st.error("Details already transformed!!!")

   if st.button("Migrate Video"):
      try:
          migrate_video()
          st.success("Video details uploaded to MySQL successfully!!!")
      except:
          st.error("Details already transformed!!!")

   if st.button("Migrate comments"):
      try:
          migrate_comments()
          st.success("Comments detail uploaded to MySQL successfully!!!")
      except:
          st.error("Details already transformed!!!")

#tab view
with tab4:    
   st.header("Queries page")
   st.write("Select Queries and view the result")
   sidebar_options = ["1. What are the names of all the videos and their corresponding channels?",
                      "2. Which channels have the most number of videos, and how many videos do they have?",
                      "3. What are the top 10 most viewed videos and their respective channels?",
                      "4. How many comments were made on each video, and what are their corresponding video names?",
                      "5. Which videos have the highest number of likes, and what are their corresponding channel names?",
                      "6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
                      "7. What is the total number of views for each channel, and what are their corresponding channel names?",
                      "8. What are the names of all the channels that have published videos in the year 2022?",                    
                      "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                      "10. Which videos have the highest number of comments, and what are their corresponding channel names?"]
   selection = st.selectbox("QUERIES", sidebar_options)

   if selection == "1. What are the names of all the videos and their corresponding channels?":
        Query1()

   if selection == "2. Which channels have the most number of videos, and how many videos do they have?":
        Query2()

   if selection == "3. What are the top 10 most viewed videos and their respective channels?":
       Query3()

   if selection == "4. How many comments were made on each video, and what are their corresponding video names?":
       Query4()

   if selection == "5. Which videos have the highest number of likes, and what are their corresponding channel names?":
       Query5()

   if selection == "6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
       Query6()

   if selection == "7. What is the total number of views for each channel, and what are their corresponding channel names?":
       Query7()

   if selection == "8. What are the names of all the channels that have published videos in the year 2022?":
       Query8()

   if selection == "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?":
       Query9()

   if selection == "10. Which videos have the highest number of comments, and what are their corresponding channel names?":
       Query10()
    
  


       