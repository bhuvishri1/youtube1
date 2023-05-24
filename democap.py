#%pip install google-api-python-client
from googleapiclient.discovery import build
import pandas as pd
from pymongo import MongoClient
import mysql.connector
import streamlit as st

api_key = 'AIzaSyBuyIUBP6S-Dzj__QDj71tAvt7v98eUKRA'
channel_ids = 'UCZ_mDxNa29sUsH_5lS2h3dw'
youtube = build("youtube", "v3", developerKey=api_key)

# Function to get channel statistics
def get_channel_stats(youtube, channel_ids):
    all_data = []
    request = youtube.channels().list(
                part='id,snippet,contentDetails,statistics',
                id=channel_ids)
    response = request.execute()
    for i in range(len(response["items"])):
        data = dict(Channel_Name=response["items"][i]["snippet"]["title"],
                    Channel_id=response["items"][i]["id"],
                    SubscriberCount=response["items"][i]["statistics"]["subscriberCount"],
                    ChannelViews=response["items"][i]["statistics"]["viewCount"],
                    ChannelDescription=response["items"][i]["snippet"]["description"],
                    Playlist_Id=response["items"][i]["contentDetails"]["relatedPlaylists"]["uploads"],
                    VideoCount=response["items"][i]["statistics"]["videoCount"])
        all_data.append(data)
        
        df = pd.DataFrame(all_data)
        
        Playlist_Id = df["Playlist_Id"][0]
        
    return data,Playlist_Id

c = get_channel_stats(youtube, channel_ids)
#c[1]
Playlist_Id = c[1]

def get_video_stats(youtube,Playlist_Id):    
        request = youtube.playlistItems().list(
              part='contentDetails',
              playlistId=Playlist_Id,
              maxResults=30)
        response = request.execute()
        
        video_ids = []
        
        next_page_token = response.get("nextPageToken")
        more_pages=True
        
        while more_pages:
            if next_page_token is None:
                more_pages= False
            else:
                request = youtube.playlistItems().list(
                                part='contentDetails',
                                playlistId=Playlist_Id,
                                maxResults=30,
                                pageToken=next_page_token)
                response = request.execute()                   
                                       
                for i in range(len(response['items'])):
                    video_ids.append(response["items"][i]["contentDetails"]["videoId"])
                          
                next_page_token = response.get("nextPageToken") 
                
        video_i = video_ids
        return video_i

v= get_video_stats(youtube,Playlist_Id)

def get_video_details(youtube,v):
    all_videoStats = []
    
    for i in range(0,len(v),30):
             request=youtube.videos().list(
                    part='snippet,statistics,contentDetails',
                    id=','.join(v[i:i+30]
                               ))
             response=request.execute()
         
             for video in response['items']:
                video_stats={"snippet":["channelTitle","title","description","tags","publishedAt"],
                            "statistics":["viewCount","likeCount","favouritecount","commentCount"],
                            "contentDetails":["duration","definition","caption"]
                                              }
                                
                video_info={"playlist_id":Playlist_Id}
                video_info['video_id']=video['id']
                
                for k in video_stats.keys():
                    for vi in video_stats[k]:
                        try:
                            
                             video_info[vi] = video[k][vi] 
                                
                            
                               
                        except: 
                        
                               video_info[vi] = None
                     
                     
                        
            
                all_videoStats.append(video_info)
                
    return all_videoStats

vd=get_video_details(youtube, v)

def get_comments_details(youtube,v):
    all_comment_stats=[]
    
    for video_id in v:
        request=youtube.commentThreads().list(
                part="snippet,replies",
                videoId=video_id)
        response = request.execute()
        for item in response['items'][0:2]:
                
                comment = item['snippet']['topLevelComment']['snippet']
                comment_data = {
                    'comment_id': item['id'],
                    'video_id':video_id,
                    'Comment': comment['textDisplay'],
                    'Comment Author': comment['authorDisplayName'],
                    'Comment Published At': comment['publishedAt']
                }
                
               
        
        all_comment_stats.append([comment_data])
        
    return all_comment_stats

cm=get_comments_details(youtube,v)

def main():
    finalData={"channel_data":[c,v,vd,cm]}
    return finalData

fd=main()

def convert_str_to_int(fd):
    for key, value in fd.items():
        if isinstance(value, str):
            try:
                fd[key] = int(value)
            except ValueError:
                pass  # Ignore if the string cannot be converted to an integer
    return fd

def convert_to_mysql_datetime(published_at):
    datetime_obj =  parser.isoparse(published_at)
    mysql_datetime = datetime_obj.strftime("%Y-%m-%dT%H:%M:%SZ")
    datetime1  = datetime_obj.strftime("%Y-%m-%d %H:%M:%S")
    return datetime1

from datetime import timedelta
from datetime import datetime
from dateutil import parser

def convert_duration_to_seconds(duration):
     duration = duration[2:]  # Remove 'PT' from the beginning of the string
     time_delta = timedelta()
     if 'H' in duration:
        hours = int(duration.split('H')[0])         

        time_delta += timedelta(hours=hours)
        duration = duration.split('H')[1]
    
     if 'M' in duration:
        minutes = int(duration.split('M')[0])
        time_delta += timedelta(minutes=minutes)
        duration = duration.split('M')[1]
    
     if 'S' in duration:
        seconds = int(duration.split('S')[0])
        time_delta += timedelta(seconds=seconds)
    
     total_seconds = time_delta.total_seconds()
     return total_seconds



def sql_connect_migrate():
    mydb= mysql.connector.connect ( host="127.0.0.1",user="root",password="BH7@ravi$",)

    mycursor = mydb.cursor()
    mycursor.execute('use yAnalys')

    channel = ''' CREATE TABLE CHANNEL(Channel_Name VARCHAR(30),
                                    Channel_id VARCHAR(255) NOT NULL,
                                    SubscriberCount INT(10),
                                    ChannelViews INT(10),
                                    ChannelDescription TEXT,
                                    PRIMARY KEY(Channel_id));'''
    mycursor.execute(channel)
    mydb.commit()

    channelD = convert_str_to_int(ch)

    mycursor = mydb.cursor()
    chquery = '''
    INSERT INTO CHANNEL(channel_id, channel_name, subscriberCount, channelViews, channelDescription)
    VALUES (%s,%s,%s,%s,%s)
    '''
    chvalues = (channelD["Channel_id"],channelD["Channel_Name"],channelD["SubscriberCount"],channelD["ChannelViews"],channelD["ChannelDescription"])
    mycursor.execute(chquery,chvalues)
    mydb.commit()

    playlist_table = ''' 
    CREATE TABLE IF NOT EXISTS playlist(
        Playlist_Id VARCHAR(255) NOT NULL,
        Channel_id VARCHAR(255),
        Channel_Name VARCHAR(255),
        PRIMARY KEY(Playlist_Id),
        FOREIGN KEY(Channel_id) REFERENCES CHANNEL(Channel_id)

    );'''

    mycursor = mydb.cursor()
    mycursor.execute(playlist_table)
    mydb.commit()

    mycursor = mydb.cursor()

    query = '''
    INSERT INTO playlist (Playlist_Id, Channel_id, Channel_Name)
    VALUES (%s, %s, %s)
    '''
    values = (channelD["Playlist_Id"],channelD["Channel_id"],channelD["Channel_Name"])
    mycursor.execute(query,values)
    mydb.commit()

    vid_det = data_dict[0]["channel_data"][2]

    mycursor.execute('use yAnalys')

    video_table = '''
    CREATE TABLE IF NOT EXISTS video(
        video_ids VARCHAR(255) NOT NULL,
        playlist_id VARCHAR(255),
        title VARCHAR(255),
        description TEXT,
        published_at DATETIME,
        view_count INT(11),
        like_count INT(11),
        favorite_count INT(11),
        comment_count INT(11),
        duration INT(11),
        definition TEXT,
        caption_status VARCHAR(255),
        PRIMARY KEY(video_ids),
        FOREIGN KEY(playlist_id) REFERENCES playlist(Playlist_Id)
    
    );
    '''
    mycursor = mydb.cursor()
    mycursor.execute(video_table)
    mydb.commit() 

    video_details=[]
    video_date=[]
    video_time=[]


    for i in range(len(vid_det)):
        v = vid_det[i]
        v = convert_str_to_int(v)
        video_details.append(v)
        d = vid_det[i]["publishedAt"]
        d = convert_to_mysql_datetime(d)
        video_date.append(d)
        t = vid_det[i]['duration']
        t = convert_duration_to_seconds(t)
        video_time.append(t)

    for i in range(len(vid_det)):
        mycursor = mydb.cursor()
        query=''' 
        INSERT INTO video(video_ids,playlist_id,title,description,published_at,view_count,like_count,favorite_count,comment_count,duration,definition,caption_status)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        '''
        values = (
            vid_det[i]['video_id'],
            video_details[i]['playlist_id'],
            video_details[i]['title'],
            video_details[i]['description'],
            video_date[i],
            video_details[i]['viewCount'],
            video_details[i]['likeCount'],
            None,
            video_details[i]['commentCount'],
            video_time[i],
            video_details[i]['definition'],
            video_details[i]['caption']
            
            
        )
        mycursor.execute(query,values)
        mydb.commit() 

    comment_table = '''
    CREATE TABLE IF NOT EXISTS comments (
        comment_ids VARCHAR(255),
        video_ids VARCHAR(255),
        comment TEXT,
        author VARCHAR(255),
        published_at DATETIME,
        PRIMARY KEY (comment_ids),
        FOREIGN KEY (video_ids) REFERENCES video(video_ids)
    );
    '''
    mycursor = mydb.cursor()
    mycursor.execute(comment_table)
    mydb.commit()

    cmt = data_dict[0]["channel_data"][3]

    Cvideo_id = []
    comment = []
    comment_author = []
    comment_date = []
    comment_id = []
    for i in range(len(cmt)):
        id = cmt[i][0]["video_id"]
        Cvideo_id.append(id)
        comments = cmt[i][0]["Comment"]
        comment.append(comments)
        author = cmt[i][0]["Comment Author"]
        comment_author.append(author)
        date = cmt[i][0]['Comment Published At']
        date = convert_to_mysql_datetime(date)
        comment_date.append(date)
        cmt_id = cmt[i][0]['comment_id']
        comment_id.append(cmt_id) 

    for i in range(len(comment_id)):
        mycursor = mydb.cursor()
        query =''' 
        INSERT INTO comments(comment_ids,video_ids,comment,author,published_at)
        Values(%s,%s,%s,%s,%s)
        '''
        values = (comment_id[i],Cvideo_id[i],comment[i],comment_author[i],comment_date[i])
        
    
    mycursor.execute(query, values)
    mydb.commit()


#USING STREAMLIT CREATING A WEB APP
#creating main page
title = '<p style="font-family:sans-serif; color:red; font-size: 42px;">YOUTUBEDATA</p>'
st.markdown(title, unsafe_allow_html=True)
st.subheader("Scrape YOUTUBE Data Here")

id = st.sidebar.text_input("channel_id")
if id:
    st.write("Channel_Name is {}".format(c[0]['Channel_Name']))

Mongo = st.button(
     label = "Upload to MongoDb"
     )

if Mongo:
    cli = MongoClient("mongodb://127.0.0.1:27017")
    db = cli.get_database("YT")
    col = db["dataFinal"]
    col.insert_one(fd)
    data_dict=list(col.find())
    
    ch = data_dict[0]["channel_data"][0][0]
    vid = data_dict[0]["channel_data"][1]
    vid_det = data_dict[0]["channel_data"][2]
    cmt = data_dict[0]["channel_data"][3]
    channelD = convert_str_to_int(ch)
    st.success("Successfully uploaded to MongoDb ",icon = None)


sql = st.button(
    label="Migrate Mongo Data to Mysql"
)
if sql:
    d = sql_connect_migrate()
    st.success("Successfully Migrated to MongoDb",icon =None)

sql_query = st.button("Query")
if sql_query:
    mydb= mysql.connector.connect ( host="127.0.0.1",user="root",password="BH7@ravi$",database = "yanalys")
    q1 = st.text_area('Query1'," What are the names of all the videos and their corresponding channels?")
    if q1:
        mycursor = mydb.cursor()
        query = '''
            SELECT video.title, playlist.Channel_Name,video.title
            FROM video
            JOIN playlist 
            ON video.playlist_id =  playlist.Playlist_Id
            '''  
        mycursor.execute(query)
    for i in mycursor:
        st.write("Channel_id :",i[0],
            "Channel_Name: ",i[1],
            "video_title :",i[2]) 
    mydb.commit()

    q2 = st.text_area('Query2',".Which channels have the most number of videos, and how many videos do they have?")
    if q2:
        mycursor = mydb.cursor()
        query = '''
                SELECT playlist.Channel_Name,count(video.video_ids)  as videoCount 
                FROM playlist
                JOIN video 
                ON video.playlist_id = playlist.Playlist_Id
                GROUP BY playlist.Channel_Name
                ORDER BY videoCount DESC
            '''
        mycursor.execute(query)
        for i in mycursor:
            st.write("CHANNEL_NAME: ",i[0],
                "VideoCount: ",i[1])
        mydb.commit()

    q3 = st.text_area('Query3',"What are the top 10 most viewed videos and their respective channels?")
    if q3:
        mycursor = mydb.cursor()
        query = '''
        
            SELECT playlist.Channel_Name, video.view_count,video.video_ids
            FROM playlist
            JOIN video
            ON video.playlist_id = playlist.Playlist_Id
            GROUP BY playlist.Channel_Name, video.video_ids
            ORDER BY video.view_count DESC
            LIMIT 10
            '''
        mycursor.execute(query)
        for i in mycursor:
            st.write("Channel_NAME :",i[0],
            "Most_viewed :",i[1],
            "video_id :",i[2])
        mydb.commit()

    q4 = st.text_area('Query4',"How many comments were made on each video, and what are their  corresponding video names?")
    if q4:
        mycursor = mydb.cursor()
        query = '''
            SELECT video.comment_count,video.video_ids,video.title
            FROM video
            GROUP BY video.video_ids
            '''
        mycursor.execute(query)
        for i in mycursor:
            st.write("Comment_COUNT : ",i[0],
                "video_id: ",i[1],
                "Video_title : ",i[2])
        mydb.commit()

    q5 = st.text_area('Query5','.Which videos have the highest number of likes, and what are their corresponding channel names?')
    if q5:
        mycursor = mydb.cursor()
        query = '''

            SELECT video.like_count,video.video_ids,video.title
            FROM video
            GROUP BY video.video_ids
            ORDER BY video.like_count DESC
            LIMIT 10;
            ''' 
        mycursor.execute(query)
        for i in mycursor:
            st.write("NO OF LIKES :",i[0],
                "video_id :",i[1],
                "video_title :",i[2])
        mydb.commit()

    q6 = st.text_area('Query6','What is the total number of likes and dislikes for each video, and what are their corresponding video names?')
    if q6:
        mycursor = mydb.cursor()
        query = '''

            SELECT video.like_count AS totalLIKES,video.title,video.video_ids
            FROM video
            GROUP BY video.title,video.video_ids
            ORDER BY totalLIKES DESC 
            ''' 
        mycursor.execute(query)
        for i in mycursor:
            print("NO OF LIKES :",i[0],
                "video_title :",i[1],
                "video_id :",i[2])
        mydb.commit()

    q7 = st.text_area('Query7','What is the total number of views for each channel, and what are their corresponding channel names?')
    if q7:
        mycursor = mydb.cursor()
        query = '''


            SELECT playlist.Channel_Name,sum(video.view_count)as TOTALVIEWS,video.video_ids
            FROM playlist
            JOIN video
            ON video.playlist_id = playlist.Playlist_Id
            GROUP BY playlist.Channel_Name, video.video_ids
            ORDER BY TOTALVIEWS DESC
            LIMIT 10
            ''' 
        mycursor.execute(query)
        for i in mycursor:
            st.write("Channel_name : ",i[0],
                "no_of_views : ",i[1],
                "video_id : ",i[2])
        mydb.commit()


    q8 = st.text_area('Query8','What are the names of all the channels that have published videos in the year  2022?')
    if q8:
        mycursor = mydb.cursor()
        query8 = '''

            SELECT playlist.Channel_Name,video.published_at
            FROM playlist
            JOIN video
            ON playlist.Playlist_Id = video.playlist_id
            WHERE YEAR(video.published_at) = 2022;
            ''' 

        mycursor = mydb.cursor()
        mycursor.execute(query8)
        #ans= mycursor.fetchall()
        for i in mycursor:
          st.write(i)


    q9 = st.text_area('Query9','What is the average duration of all videos in each channel, and what are their corresponding channel names?')
    if q9:
        mycursor = mydb.cursor()
        query = '''
            SELECT avg(video.duration),playlist.Channel_Name
            FROM video
            JOIN playlist
            on video.playlist_id = playlist.Playlist_Id
            GROUP BY playlist.Channel_Name
            ''' 
        mycursor.execute(query)
        for i in mycursor:
            st.write("AVG DURATION: ",i[0],
                    "Channel_name: ",i[1])
        mydb.commit()


    q10 = st.text_area('Query10','Which videos have the highest number of comments, and what are their corresponding channel names?')
    if q10:
        mycursor = mydb.cursor()
        query = '''

            SELECT max(video.comment_count) AS MAXCMT,playlist.Channel_Name,video.video_ids
            FROM video
            JOIN playlist
            on video.playlist_id = playlist.Playlist_Id
            GROUP BY video.video_ids,playlist.Channel_Name
            ORDER BY MAXCMT
            ''' 
        mycursor.execute(query)
        for i in mycursor:
            st.write("Comment_COUNT : ",i[0],
                "Channel_name : ",i[1],
                "Video_ids : ",i[2])
        mydb.commit()