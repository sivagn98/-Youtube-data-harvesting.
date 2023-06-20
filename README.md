import pymongo
import psycopg2
import streamlit as st
import pandas as pd
from googleapiclient.discovery import build
from streamlit_option_menu import option_menu
import plotly.express as px
import plotly.figure_factory as ff

# Establish connection to MongoDB
client = pymongo.MongoClient("mongodb+srv://siva98:generate@cluster0.v8zufde.mongodb.net/?retryWrites=true&w=majority")
mg_db = client['Channel_Database']

# Establish connection to PostgreSQL
siva = psycopg2.connect(host="localhost", user="postgres", password="4075218.", port=5432, database="youtube")
guvi = siva.cursor()

# YouTube API key
api_key = "AIzaSyChEwCgdtCrshFjNSzAEvbchKjJtTDl3Lw"
youtube = build('youtube', 'v3', developerKey=api_key)

# Create tables in PostgreSQL
def create_table():
    guvi.execute("CREATE TABLE Channel_table("
                 "Channel_ID varchar(50) PRIMARY KEY,"
                 "Channel_Name varchar(50),"
                 "Subscribers int,"
                 "Views int,"
                 "Total_videos int)")
    siva.commit()

    guvi.execute("CREATE TABLE Video_table("
                 "Video_ID varchar(50) PRIMARY KEY,"
                 "Channel_Id varchar(50),"
                 "Title varchar(100),"
                 "Description TEXT,"
                 "Published_Date TIMESTAMP,"
                 "Category_Id int,"
                 "Thumbnails_URL varchar(100),"
                 "Duration Time,"
                 "Video_Quality varchar(20),"
                 "Caption_status varchar(10),"
                 "Licensed varchar(10),"
                 "View_count int,"
                 "Like_count int,"
                 "Fav_count int,"
                 "Tags TEXT,"
                 "Dislike_count int,"
                 "Comment_Count int)")
    siva.commit()

    guvi.execute("CREATE TABLE Comment_table("
                 "Comment_ID VARCHAR(50) PRIMARY KEY,"
                 "Video_ID VARCHAR(50),"
                 "Comment TEXT,"
                 "Comment_author VARCHAR(50),"
                 "Comment_date TIMESTAMP,"
                 "Comment_like INT,"
                 "ReplyCount INT)")
    siva.commit()

# Retrieve channel statistics
def get_channel_stats(youtube, channel_id):
    request = youtube.channels().list(
        part='snippet,contentDetails,statistics',
        id=channel_id
    )
    response = request.execute()
    
    try:
        data = {
            'Channel_Id': channel_id,
            'Channel_name': response['items'][0]['snippet']['title'],
            'Subscribers': response['items'][0]['statistics']['subscriberCount'],
            'Views': response['items'][0]['statistics']['viewCount'],
            'Total_videos': response['items'][0]['statistics']['videoCount'],
            'playlist_id': response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
        }
        return data
    except KeyError:
        return False

# Retrieve video information
def get_video_Info(youtube, video_id):
    request = youtube.videos().list(
        part="snippet,contentDetails,statistics",
        id=video_id
    )
    video_response = request.execute()
    return video_response

# Retrieve video statistics
def get_video_stats(youtube, playlist_id):
    all_video = []
    next_page_token = None
    
    while True:
        request = youtube.playlistItems().list(
            part='contentDetails',
            playlistId=playlist_id,
            maxResults=50,
            pageToken=next_page_token
        )
        response = request.execute()

        for i in range(len(response['items'])):
            video_id = response['items'][i]['contentDetails']['videoId']
            video_info = get_video_Info(youtube, video_id)
            
            try:
                data = {
                    'Video_ID': video_id,
                    'Channel_Id': video_info['items'][0]['snippet']['channelId'],
                    'Title': video_info['items'][0]['snippet']['title'],
                    'Description': video_info['items'][0]['snippet']['description'],
                    'Published_Date': video_info['items'][0]['snippet']['publishedAt'],
                    'Category_Id': video_info['items'][0]['snippet']['categoryId'],
                    'Thumbnails_URL': video_info['items'][0]['snippet']['thumbnails']['default']['url'],
                    'Duration': video_info['items'][0]['contentDetails']['duration'],
                    'Video_Quality': video_info['items'][0]['contentDetails']['definition'],
                    'Caption_status': video_info['items'][0]['contentDetails']['caption'],
                    'Licensed': video_info['items'][0]['contentDetails']['licensedContent'],
                    'View_count': video_info['items'][0]['statistics']['viewCount'],
                    'Like_count': video_info['items'][0]['statistics']['likeCount'],
                    'Fav_count': video_info['items'][0]['statistics']['favoriteCount'],
                    'Tags': video_info['items'][0]['snippet']['tags'],
                    'Dislike_count': video_info['items'][0]['statistics']['dislikeCount'],
                    'Comment_Count': video_info['items'][0]['statistics']['commentCount']
                }
                all_video.append(data)
            except KeyError:
                continue

        next_page_token = response.get('nextPageToken')

        if not next_page_token:
            break

    return all_video

# Retrieve comment information
def get_comment_info(youtube, video_id):
    request = youtube.commentThreads().list(
        part='snippet',
        videoId=video_id,
        maxResults=100
    )
    comment_response = request.execute()
    return comment_response

# Retrieve video comments
def get_video_comments(youtube, video_id):
    all_comments = []
    next_page_token = None
    
    while True:
        request = youtube.commentThreads().list(
            part='snippet',
            videoId=video_id,
            maxResults=100,
            pageToken=next_page_token
        )
        response = request.execute()

        for item in response['items']:
            comment_id = item['snippet']['topLevelComment']['id']
            comment_info = get_comment_info(youtube, video_id)
            
            try:
                data = {
                    'Comment_ID': comment_id,
                    'Video_ID': video_id,
                    'Comment': comment_info['items'][0]['snippet']['topLevelComment']['snippet']['textOriginal'],
                    'Comment_author': comment_info['items'][0]['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    'Comment_date': comment_info['items'][0]['snippet']['topLevelComment']['snippet']['publishedAt'],
                    'Comment_like': comment_info['items'][0]['snippet']['topLevelComment']['snippet']['likeCount'],
                    'ReplyCount': item['snippet']['totalReplyCount']
                }
                all_comments.append(data)
            except KeyError:
                continue

        next_page_token = response.get('nextPageToken')

        if not next_page_token:
            break

    return all_comments

# Insert channel data into PostgreSQL
def insert_channel_data(channel_data):
    guvi.execute("INSERT INTO Channel_table(Channel_ID, Channel_Name, Subscribers, Views, Total_videos) "
                 "VALUES (%(Channel_Id)s, %(Channel_name)s, %(Subscribers)s, %(Views)s, %(Total_videos)s)", channel_data)
    siva.commit()

# Insert video data into PostgreSQL
def insert_video_data(video_data):
    guvi.execute("INSERT INTO Video_table(Video_ID, Channel_Id, Title, Description, Published_Date, Category_Id, "
                 "Thumbnails_URL, Duration, Video_Quality, Caption_status, Licensed, View_count, Like_count, "
                 "Fav_count, Tags, Dislike_count, Comment_Count) "
                 "VALUES (%(Video_ID)s, %(Channel_Id)s, %(Title)s, %(Description)s, %(Published_Date)s, %(Category_Id)s, "
                 "%(Thumbnails_URL)s, %(Duration)s, %(Video_Quality)s, %(Caption_status)s, %(Licensed)s, %(View_count)s, "
                 "%(Like_count)s, %(Fav_count)s, %(Tags)s, %(Dislike_count)s, %(Comment_Count)s)", video_data)
    siva.commit()

# Insert comment data into PostgreSQL
def insert_comment_data(comment_data):
    guvi.execute("INSERT INTO Comment_table(Comment_ID, Video_ID, Comment, Comment_author, Comment_date, Comment_like, "
                 "ReplyCount) "
                 "VALUES (%(Comment_ID)s, %(Video_ID)s, %(Comment)s, %(Comment_author)s, %(Comment_date)s, "
                 "%(Comment_like)s, %(ReplyCount)s)", comment_data)
    siva.commit()

# Main function
def main():
    st.title("YouTube Data Analytics Dashboard")
    menu = ["Create Database", "Retrieve Data"]
    choice = st.sidebar.selectbox("Menu", menu)

    if choice == "Create Database":
        create_table()
        st.success("Database created successfully!")

    elif choice == "Retrieve Data":
        st.subheader("Channel Statistics")

        # Retrieve channel ID from user input
        channel_id = st.text_input("Enter Channel ID")

        if st.button("Get Channel Statistics"):
            # Retrieve channel data from YouTube API
            channel_data = get_channel_stats(youtube, channel_id)

            if channel_data:
                # Insert channel data into PostgreSQL
                insert_channel_data(channel_data)
                st.success("Channel statistics retrieved and inserted into the database successfully!")
            else:
                st.error("Failed to retrieve channel statistics. Please check the channel ID and try again.")

        st.subheader("Video Statistics")

        # Retrieve video ID from user input
        video_id = st.text_input("Enter Video ID")

        if st.button("Get Video Statistics"):
            # Retrieve video data from YouTube API
            video_data = get_video_stats(youtube, video_id)

            if video_data:
                # Insert video data into PostgreSQL
                insert_video_data(video_data)
                st.success("Video statistics retrieved and inserted into the database successfully!")
            else:
                st.error("Failed to retrieve video statistics. Please check the video ID and try again.")

        st.subheader("Comment Statistics")

        # Retrieve video ID from user input
        video_id_comment = st.text_input("Enter Video ID")

        if st.button("Get Video Comments"):
            # Retrieve comment data from YouTube API
            comment_data = get_video_comments(youtube, video_id_comment)

            if comment_data:
                # Insert comment data into PostgreSQL
                insert_comment_data(comment_data)
                st.success("Video comments retrieved and inserted into the database successfully!")
            else:
                st.error("Failed to retrieve video comments. Please check the video ID and try again.")

if __name__ == '__main__':
    main()
