# -Youtube-data-harvesting.

from googleapiclient.discovery import build
import pandas as pd

import pymongo
import psycopg2

import streamlit as st
from streamlit_option_menu import option_menu
import plotly.express as px
import plotly.figure_factory as ff

client = pymongo.MongoClient("mongodb+srv://siva98:generate@cluster0.v8zufde.mongodb.net/?retryWrites=true&w=majority")
mg_db = client['Channel_Database']
siva = psycopg2.connect(host="localhost", user="postgres", password="4075218.", port=5432, database="youtube")
guvi = siva.cursor()

api_key = "AIzaSyChEwCgdtCrshFjNSzAEvbchKjJtTDl3Lw"
youtube = build('youtube', 'v3', developerKey=api_key)
# channel_id = 'UCl23mvQ3321L7zO6JyzhVmg' Mumbai indians
# channel_id = "UC2J_VKrAzOEJuQvFFtj3KUw" Chennai super kings
# channel_id = 'UCCq1xDJMBRF61kiOgU90_kw' royal challengers banglore
# channel_id = 'UCScgEv0U9Wcnk24KfAzGTXg' Sunrisers Hyderabad
# channel_id = "UCp10aBPqcOeBbEg7d_K9SBw" Kolkata Knight Riders
# channel_id = "mi8xUqL43BMlhvJbAf-Ew" Lucknow Super Giants
# channel_id = 'UCCBe9iIoN9Ar-Elluxca-Xw' Gujarat Titans
# channel_id = "UCEzB47eM-HZu04f4mB2nycg" Delhi capitals
# channel_id = 'UCvRa1LWA_-aARq1AQMC4AyA' punjab kings
# channel_id = 'UCkpgyRmcNy-aZFLUkKkWK4w' Rajastan royals



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


def get_channel_stats(youtube, channel_id):
    request = youtube.channels().list(
        part='snippet,contentDetails,statistics',
        id=channel_id)
    response = request.execute()
    #st.write(response)
    try:
        data = dict(Channel_Id=channel_id,
          Channel_name=response['items'][0]['snippet']['title'],
          Subscribers=response['items'][0]['statistics']['subscriberCount'],
          Views=response['items'][0]['statistics']['viewCount'],
          Total_videos=response['items'][0]['statistics']['videoCount'],
          playlist_id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads'])
        return data
    except KeyError:
        return False

def get_video_Info(youtube,video_id): # retrieve complete info of video
    request = youtube.videos().list(
              part="snippet,contentDetails,statistics",
              id=video_id
          )
    video_response = request.execute()
    return video_response


def get_video_stats(youtube, playlist_id):
    all_video = []
    next_page_token = None
    while True:
        request = youtube.playlistItems().list(
                    part='contentDetails',
                    playlistId = playlist_id,
                    maxResults = 50,
                    pageToken = next_page_token)
        response = request.execute()

        for i in range(len(response['items'])):
            video_response = get_video_Info(youtube,response['items'][i]['contentDetails']['videoId'])
            video = dict(video_id = response['items'][i]['contentDetails']['videoId'],
                         Channel_id = video_response['items'][0]['snippet']['channelId'],
                         title = video_response['items'][0]['snippet']['title'],
                         description = video_response['items'][0]['snippet']['description'],
                         published_Date = video_response['items'][0]['snippet']['publishedAt'],
                         category_Id = video_response['items'][0]['snippet']['categoryId'],
                         thumbnails = video_response['items'][0]['snippet']['thumbnails']['default']['url'],
                         duration = video_response['items'][0]['contentDetails']['duration'],
                         Video_Quality = video_response['items'][0]['contentDetails']['definition'],
                         Caption_status = video_response['items'][0]['contentDetails']['caption'],
                         licensed = video_response['items'][0]['contentDetails']['licensedContent'],
                         view_count = video_response['items'][0]['statistics']['viewCount'])
            try:
              video['like_count'] = video_response['items'][0]['statistics']['likeCount']
            except:
              video['like_count'] = 0
            try:
              video['favorite_count'] = video_response['items'][0]['statistics']['favoriteCount']
            except:
              video['favorite_count'] = 0
            try:
              video['tags'] = video_response['items'][0]['snippet']['tags']
            except:
              video['tags'] = 0
            try:
              video['dislike_count'] = video_response['items'][0]['statistics']['dislikeCount']
            except:
              video['dislike_count'] = 0
            try:
              video['comment_Count'] = video_response['items'][0]['statistics']['commentCount']
            except:
              video['comment_Count'] = 0
            all_video.append(video)        
        next_page_token = response.get('nextPageToken')
        if next_page_token is None:
            break
    return all_video


def update_comment_stats(youtube, video_ids, Channel_name):
    channel_records1 = mg_db[channel_name]
    a = channel_records1.find()
    obj_id = a[0]['_id']
    comments = []
    for video_id in video_ids:
        next_page_token = None
        try:
            while True:
                request = youtube.commentThreads().list(part='snippet',
                                                      videoId=video_id,
                                                      maxResults=50,
                                                      pageToken=next_page_token)
                response = request.execute()
                if response['items']:

                    for item in response['items']:
                        comment_info = dict(comment_id = item['id'],
                            video_id = item['snippet']['topLevelComment']['snippet']['videoId'],
                            comment = item['snippet']['topLevelComment']['snippet']['textDisplay'],
                            comment_author = item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            comment_date = item['snippet']['topLevelComment']['snippet']['publishedAt'])
                        try:
                          comment_info['comment_like'] = item['snippet']['topLevelComment']['snippet']['likeCount']
                        except:
                          comment_info['comment_like'] = 0
                        try:
                          comment_info['totalReplyCount'] = item['snippet']['totalReplyCount']
                        except:
                          comment_info['totalReplyCount'] = 0
                        comments.append(comment_info)
                        channel_records1.update_one({'_id': obj_id}, {'$push': {'Comment_info': comment_info}})
                    next_page_token = response.get('nextPageToken')
                if next_page_token is None:
                    break
        except:
          pass
    return comments


def retrieve_channel(youtube, channel_id):
    channel_statistics = get_channel_stats(youtube, channel_id)
    video_statistics = get_video_stats(youtube, channel_statistics['playlist_id'])
    video_df = pd.DataFrame(video_statistics)
    comment_statistics = []
    channel = dict(Channel_info = channel_statistics, Video_info = video_statistics, Comment_info = comment_statistics)
    return channel


def channel_sql(Channel_mong):
    channel_name = Channel_mong['Channel_name']
    channel_id = Channel_mong['Channel_Id']
    Subscribers = int(Channel_mong['Subscribers'])
    Views = int(Channel_mong['Views'])
    Total_videos = int(Channel_mong['Total_videos'])
    guvi.execute(f"select * from channel_table where channel_id = '{channel_id}'")
    retrive = guvi.fetchall()
    if len(retrive) > 0:
        guvi.execute(f"DELETE FROM channel_table WHERE channel_id = '{channel_id}'")
        siva.commit()
    insert_one = "INSERT INTO channel_table VALUES (%s, %s, %s, %s, %s)"
    guvi.execute(insert_one, (channel_id, channel_name, Subscribers, Views, Total_videos))
    siva.commit()


def video_table_clear(vid_id):
    guvi.execute(f"select * from video_table where video_id = '{vid_id}'")
    retrive = guvi.fetchall()
    if len(retrive) > 0:
        guvi.execute(f"DELETE FROM video_table WHERE video_id = '{vid_id}'")
        siva.commit()


def comment_table_clear(com_id):
    guvi.execute(f"select * from comment_table where comment_id = '{com_id}'")
    retrive = guvi.fetchall()
    if len(retrive) > 0:
        guvi.execute(f"DELETE FROM comment_table WHERE comment_id = '{com_id}'")
        siva.commit()


def video_sql(video_mong):
    df = pd.DataFrame(video_mong)
    df = df.fillna(0)
    # Convert duration column to timedelta
    df['duration'] = pd.to_timedelta(df['duration'])
    # Convert timedelta to string format
    df['duration'] = df['duration'].astype(str)
    # Format the string to hh:mm:ss
    df['duration'] = df['duration'].str.extract(r'(\d+:\d+:\d+)')
    # Convert Date column to datetime format
    df['published_Date'] = pd.to_datetime(df['published_Date'])
    # Format datetime without timezone offset
    df['published_Date'] = df['published_Date'].dt.strftime('%Y-%m-%d %H:%M:%S')
    # converting str to int for counts column
    df['view_count'] = df['view_count'].astype(int)
    df['like_count'] = df['like_count'].astype(int)
    df['dislike_count'] = df['dislike_count'].astype(int)
    df['comment_Count'] = df['comment_Count'].astype(int)
    df['favorite_count'] = df['favorite_count'].astype(int)
    # st.dataframe(df)
    for row in df.itertuples(index=False):
        vid_id = row[0]
        video_table_clear(vid_id)
        guvi.execute("INSERT INTO video_table VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", row)
        siva.commit()


def comment_sql(comment_mong):
    df_comment = pd.DataFrame(comment_mong)
    df_comment['comment_date'] = pd.to_datetime(df_comment['comment_date'])
    df_comment['comment_date'] = df_comment['comment_date'].dt.strftime('%Y-%m-%d %H:%M:%S')

    df_comment = df_comment.fillna(0)
    for row in df_comment.itertuples(index=False):
        com_id = row[0]
        comment_table_clear(com_id)
        guvi.execute("INSERT INTO comment_table VALUES (%s,%s,%s,%s,%s,%s,%s)", row)
        siva.commit()

st.set_page_config(page_title="Youtube Data Harvesting", page_icon="",layout="wide", initial_sidebar_state="expanded")
st.markdown(
    """
    <style>
    .main {
        padding: 0rem 0rem;
    }
    .sidebar .sidebar-content {
        width: 300px;
    }
    </style>
    """,
    unsafe_allow_html=True
)
st.title("Youtube Data Harvesting")
with st.sidebar:
    selected = option_menu(
        menu_title="Navigaton",  # required
        options=["Home","---","Harvest", "Migrate", "FAQ", "---", "About"],  # required
        icons=["house","", "","arrows-move", "patch-question", "", "envelope"],  # optional
        menu_icon="youtube",  # optional
        default_index=0,  # optional
        styles={"nav-link": {"--hover-color": "brown"}},
        orientation="vertical",
    )

collection_list = sorted(mg_db.list_collection_names())

if selected == 'Home':

    st.markdown('__<p style="font-family: verdana; text-align:left; font-size: 15px; color: #FAA026">Our web application provides an interactive platform to explore YouTube channels and their content. With just the YouTube channel ID, you can retrieve comprehensive information about any channel and dive into its details.</P>__',
                unsafe_allow_html=True)

    st.markdown("""
    Features:
        * Channel Overview: Get a quick overview of the channel, including its name, description, and subscriber count. This gives you a snapshot of the channel's popularity and focus. Upon overview of channel, we can proceed with harvest option to collect complete information about the channel and store it in our databae
        * No Structured Data Storage: We store the retrieved channel information in our MongoDB database. This ensures that the data is securely stored and readily available for future use.
        * Migration to PostgreSQL: To enhance data analysis and reporting capabilities, we migrate the stored channel information to a structured PostgreSQL database. This allows for efficient querying and data manipulation, enabling us to extract valuable insights from the stored data.
        * Currently, we are in the first stage of our YouTube analytics project which focuses on FAQs for the channels. To make it easier to get answers, we've added a tab entitled "FAQ" that will give responses based on harvested channels.
        * Our web application leverages the power of YouTube's API and combines it with the versatility of MongoDB and PostgreSQL databases. This combination ensures a seamless user experience while providing robust data management and analysis capabilities.
        * Start exploring YouTube channels like never before! Enter the YouTube channel ID, and our web application will provide you with a comprehensive overview of the channel, its latest and popular videos, and much more.
        Enjoy your journey through the world of YouTube channels with our web application!.""")

elif selected == 'Harvest':
    col1, col6, col7 = st.columns([2, 1, 2])
    with col1:
        channel_id = st.text_input("Enter Channel Id:")
    col4, col5, col6, col7 = st.columns([1, 1, 2, 2])
    with col4:
        Search = st.button("Search")
    with col5:
        harvest = st.button("Harvest")
    if Search:
        Channel_mong = get_channel_stats(youtube, channel_id)
        if Channel_mong:
            channel_list = []
            channel_list.append(Channel_mong)
            channel_df = pd.DataFrame(channel_list)
            channel_df = channel_df.T
            channel_df = channel_df.rename(columns={0: 'Details'})
            st.dataframe(channel_df)
        else:
            st.write("")
            st.error('Channel not found, check the channel id and try again', icon="🚨")
    if harvest:
        #st.write(channel_id)
        Channel_mong = get_channel_stats(youtube, channel_id)
        if Channel_mong:
            channel_details = retrieve_channel(youtube, channel_id)
            channel_name = channel_details['Channel_info']['Channel_name']
            channel_records = mg_db[channel_name]
            if channel_name in collection_list:
                channel_records.delete_many({})
            channel_records.insert_one(channel_details) #inserting channel & video Info
            video_ids = [item['video_id'] for item in channel_details['Video_info']]
            #inserting each comment to mongodb directly and also collecting all together and display in streamlit preview
            comment = update_comment_stats(youtube, video_ids,channel_name)
            channel_details['Comment_info'] = comment
            st.write('harvest completed')
            st.write(channel_details)
            st.write('Moved to MongoDB')
        else:
            st.write("")
            st.error('Channel not found, check the channel id and try again', icon="🚨")
elif selected == 'Migrate':
    col1,col2,col3 = st.columns([2,1,2])
    with col1:
        select_channel = st.selectbox('Select Channel to migrate data to SQL:', collection_list)
    col4, col5, col6, col7 = st.columns([1, 1, 2, 2])
    with col4:
        Show = st.button("Show")
    with col5:
        Migrate = st.button("Migrate")
    if Show:
        MongoData = []
        channel_records_pull = mg_db[select_channel]
        c = channel_records_pull.find()
        for i in c:
            MongoData.append(i)
        Channel_mong = MongoData[0]['Channel_info']
        channel_list = []
        channel_list.append(Channel_mong)
        channel_df = pd.DataFrame(channel_list)
        channel_df = channel_df.T
        channel_df = channel_df.rename(columns={0: 'Details'})
        st.dataframe(channel_df)
    elif Migrate:
        dataframe = False
        #st.write(select_channel)
        channel_records_pull = mg_db[select_channel]
        #st.write(channel_records_pull)
        MongoData = []
        c = channel_records_pull.find()
        #st.write(c)
        for i in c:
            MongoData.append(i)
        #st.write(MongoData)
        Channel_mong = MongoData[0]['Channel_info']
        channel_sql(Channel_mong)
        video_mong = MongoData[0]['Video_info']
        video_sql(video_mong)
        comment_mong = MongoData[0]['Comment_info']
        comment_sql(comment_mong)
        st.markdown('__<p style="text-align:center; font-size: 20px; color: #FAA026">Data Migrated to SQL</P>__',
                    unsafe_allow_html=True)
        dataframe = True
        if dataframe:
            tab1, tab2, tab3 = st.tabs(['Channel Info','Video Info','Comment Info'])
            with tab1:
                guvi.execute(f"select * from channel_table where channel_name = '{select_channel}'")
                y = guvi.fetchall()
                channel_df_mig = pd.DataFrame(y,columns=['Channel ID','Channel Name','Subscribers','Total Views','Total Videos'])
                channel_df_mig = channel_df_mig.T
                channel_df_mig = channel_df_mig.rename(columns={0: 'Details'})
                st.dataframe(channel_df_mig)


            with tab2:
                guvi.execute(f"select video_table.title, video_table.published_date, "
                             f"video_table.category_id, video_table.duration, video_table.view_count, "
                             f"video_table.like_count, video_table.comment_count from video_table join channel_table "
                             f"on video_table.channel_id = channel_table.channel_id "
                             f"where channel_table.channel_name = '{select_channel}'")
                y = guvi.fetchall()
                video_df_mig = pd.DataFrame(y, columns=['Video Name', 'Posted On', 'Category',
                                                          'Duration','Views','Likes','Total Comments/replies'])
                video_df_mig = video_df_mig.set_index('Video Name')
                st.dataframe(video_df_mig)

            with tab3:
                guvi.execute(f"select chan_video.title, comment_table.comment, comment_table.comment_author, "
                             f"comment_table.comment_date,comment_table.comment_like,comment_table.replycount  "
                             f"from comment_table join (select video_table.video_id as video_id,video_table.title "
                             f"as title,channel_table.channel_name as name from video_table join channel_table "
                             f"on video_table.channel_id = channel_table.channel_id "
                             f"where channel_table.channel_name = '{select_channel}') as chan_video "
                             f"on comment_table.video_id = chan_video.video_id where chan_video.name = '{select_channel}'")
                y = guvi.fetchall()
                comment_df_mig = pd.DataFrame(y, columns=['Video Name', 'Comment', 'Author',
                                                        'Commented On', 'Likes', 'replies'])
                comment_df_mig = comment_df_mig.set_index('Video Name')
                st.dataframe(comment_df_mig)

elif selected == "FAQ":
    st.markdown('__<p style="text-align:left; font-size: 30px; color: #FAA026">Top 10 FAQs</P>__',
                unsafe_allow_html=True)
    with st.expander("Q1. What are the names of all the videos and their corresponding channels?"):
        query1 = "select channel_table.channel_name, video_table.title from video_table " \
                "join channel_table on video_table.channel_id = channel_table.channel_id " \
                "order by channel_table.channel_name"
        guvi.execute(query1)
        faq1 = guvi.fetchall()
        faq1_df = pd.DataFrame(faq1,columns = ['Channel Name','Video Title'])
        faq1_df = faq1_df.set_index('Channel Name')
        st.write(" ")
        st.write("Here you can find a comprehensive list of channels and the associated videos within them:")
        st.dataframe(faq1_df,width=1000)
    with st.expander("Q2. Which channels have the most number of videos and how many videos do they have?"):
        query2 = 'select channel_name, total_videos as Videos from channel_table order by total_videos desc limit 3'
        guvi.execute(query2)
        faq2 = guvi.fetchall()
        faq2_df = pd.DataFrame(faq2, columns=['Channel Name', 'Total Video'])
        channel_name = faq2_df.iloc[0, 0]
        total_video = faq2_df.iloc[0, 1]
        st.write("'",channel_name, "' channel has the most videos with a total count of", str(total_video),'. below are the top 3 channel in the list')
        faq2_df = faq2_df.set_index('Channel Name')
        c1, c2, c3 = st.columns([1,1,2])
        with c2:
            st.dataframe(faq2_df, width=1000)
    with st.expander("Q3. What are the top 10 most viewed videos and their respective channels?"):
        query3 = "select channel_table.channel_name, video_table.title, video_table.view_count from video_table join channel_table " \
                "on video_table.channel_id = channel_table.channel_id order by video_table.view_count desc limit 10"
        guvi.execute(query3)
        faq3 = guvi.fetchall()
        faq3_df = pd.DataFrame(faq3, columns=['Channel Name', 'Video Title', 'View Count'])
        st.write(faq3_df.iloc[0,0]," channel is on the top of the list for the video ",faq3_df.iloc[0,1]," with the view count of ",str(faq3_df.iloc[0,2]))
        st.write(" ")
        st.write(" ")
        faq3_df = faq3_df.set_index('Channel Name')
        st.dataframe(faq3_df, width=1000)
    with st.expander("Q4. How many comments were made on each video and what are their corresponding video names?"):
        query4 = "select title, comment_count from video_table order by comment_count desc"
        guvi.execute(query4)
        faq4 = guvi.fetchall()
        faq4_df = pd.DataFrame(faq4, columns=['Video Name', 'Total Comment'])
        st.write(" ")
        st.write(faq4_df.iloc[0,0],"received ",str(faq4_df.iloc[0,1])," comments.")
        st.write(" ")
        st.write(" ")
        faq4_df = faq4_df.set_index('Video Name')
        c1, c2, c3 = st.columns([0.25, 2,0.5])
        with c2:
            st.dataframe(faq4_df, width=700)
    with st.expander("Q5. Which videos have the highest number of likes and what are their corresponding channel names?"):
        query5="select video_table.like_count, video_table.title, channel_table.channel_name from video_table " \
               "join channel_table on video_table.channel_id = channel_table.channel_id " \
               "order by video_table.like_count desc limit 10"
        guvi.execute(query5)
        faq5 = guvi.fetchall()
        faq5_df = pd.DataFrame(faq5, columns=['Total Comment', 'Video Name', 'Channel Name'])
        st.write(" ")
        st.write("Below are the top 10 liked videos and their channel name:")
        st.write(" ")
        st.write(" ")
        faq5_df = faq5_df.set_index('Total Comment')
        st.dataframe(faq5_df, width=1000)
    with st.expander("Q6. What is the total number of likes and dislikes for each video and what are their corresponding video names?"):
        query6 = 'select title, like_count, dislike_count from video_table order by like_count desc'
        guvi.execute(query6)
        faq6 = guvi.fetchall()
        faq6_df = pd.DataFrame(faq6, columns=['Video Name', 'Like Count','Dislike Count'])
        st.write("Recently, YouTube has taken away the ability to publicly view the dislike count of a video due to "
                 "an experimental update. Though this information is not visible in the below table, you can still "
                 "find a list of videos along with their like counts.")
        st.write(r"Source link: https://blog.youtube/news-and-events/update-to-youtube/")
        st.write(" ")
        st.write(" ")
        faq6_df = faq6_df.set_index('Video Name')
        st.dataframe(faq6_df, width=1000)

    with st.expander("Q7. What is the total number of views for each channel and what are their corresponding channel names?"):
        query7 = "select channel_table.channel_name, sum(video_table.view_count), " \
                 "round(avg(video_table.view_count),2) as Avg_count from video_table join channel_table " \
                 "on video_table.channel_id = channel_table.channel_id group by channel_table.channel_name " \
                 "order by sum(video_table.view_count) desc"
        guvi.execute(query7)
        faq7 = guvi.fetchall()
        faq7_df = pd.DataFrame(faq7, columns=['Channel Name', 'View Count', 'Avg view/video'])
        faq7_df['Avg view/video'] = faq7_df['Avg view/video'].astype(float)
        st.write(" ")
        st.write(" ")
        faq7_df = faq7_df.set_index('Channel Name')
        c1, c2, c3 = st.columns([0.5, 2, 1.5])
        with c2:
            st.dataframe(faq7_df, width=1000)
    with st.expander("Q8. What are the names of all the channels that have published videos in the year 2022?"):
        query8 = "select channel_table.channel_name, count(video_table.title), " \
                 "sum(video_table.view_count) as total_count from video_table join channel_table " \
                 "on video_table.channel_id = channel_table.channel_id " \
                 "where extract(year from video_table.published_date) = 2022 " \
                 "group by channel_table.channel_name " \
                 "order by count(video_table.title) desc"
        guvi.execute(query8)
        faq8 = guvi.fetchall()
        faq8_df = pd.DataFrame(faq8, columns=['Channel Name', 'Total Videos', 'Total Views'])
        #faq8_df['Avg view/video'] = faq8_df['Avg view/video'].astype(float)
        st.write(" ")
        st.write(" ")
        faq8_df = faq8_df.set_index('Channel Name')
        c1, c2, c3 = st.columns([0.5, 2, 1.5])
        with c2:
            st.dataframe(faq8_df, width=1000)

    with st.expander("Q9. What is the average duration of all videos in each channel and what are their corresponding channel names?"):
        query9 = "select channel_name, " \
                 "EXTRACT(MINUTE FROM duration) || ' mins ' || round(EXTRACT(SECOND FROM duration)) || ' secs' AS new_duration " \
                 "from (select channel_table.channel_name as channel_name, avg(video_table.duration) as duration " \
                 "from video_table join channel_table on video_table.channel_id = channel_table.channel_id " \
                 "group by channel_table.channel_name order by avg(video_table.duration) desc) as subq"
        guvi.execute(query9)
        faq9 = guvi.fetchall()
        faq9_df = pd.DataFrame(faq9, columns=['Channel Name', 'Average Duration'])
        st.write(" ")
        st.write(" ")
        faq9_df = faq9_df.set_index('Channel Name')
        c1, c2, c3 = st.columns([0.5, 2, 1.5])
        with c2:
            st.dataframe(faq9_df, width=1000)

    with st.expander("Q10. Which videos have the highest number of comments and what are their corresponding channel names?"):
        query10 = "select video_table.comment_count, video_table.title, channel_table.channel_name from video_table join channel_table on video_table.channel_id = channel_table.channel_id order by comment_count desc limit 10"
        guvi.execute(query10)
        faq10 = guvi.fetchall()
        faq10_df = pd.DataFrame(faq10, columns=['Total Comment', 'Video Name', 'Channel Name'])
        st.write(" ")
        chan = faq10_df.iloc[0, 2]
        st.write(faq10_df.iloc[0, 1], f"-- video from {chan} channel received ", str(faq10_df.iloc[0, 0]), " comments and holding first position.")
        st.write(" ")
        faq10_df = faq10_df.set_index('Total Comment')
        st.dataframe(faq10_df, width=1000)
    st.write("Note: The above details are solely based on the data that we scraped and should not be taken as a "
             "representation of global channels.")
elif selected == "About":
    st.markdown('__<p style="text-align:left; font-size: 25px; color: #FAA026">Youtube Data Harvesting</P>__',
                unsafe_allow_html=True)
    st.write(
        "This data harvesting project focused on mining data from youtube based on user request and store it in database to do analysis process, in the initial we published with data mining and provide finding for FAQs, in the next phase, data visualization and ML analysis part will be included.")
    st.markdown('__<p style="text-align:left; font-size: 20px; color: #FAA026">Applications and Packages Used:</P>__',
                unsafe_allow_html=True)
    st.write("  * Python")
    st.write("  * PostgresSql")
    st.write("  * MongoDB")
    st.write("  * Streamlit")
    st.write("  * Pandas")
    st.write("  * Github")
    st.write("  * Psycopg2")
    st.write("  * Pymongo")
    st.markdown(
        '__<p style="text-align:left; font-size: 20px; color: #FAA026">For feedback/suggestion, connect with me on</P>__',
        unsafe_allow_html=True)
    st.subheader("LinkedIn")
    st.write("linkedin.com/in/siva-kumar-665457191")
    st.subheader("Email ID")
    st.write("sivaeie98@gmail.com")
    st.subheader("Github")
    st.write("https://github.com/sivagn98")
