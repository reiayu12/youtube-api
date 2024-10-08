import pandas as pd
from apiclient.discovery import build

YOUTUBE_API_KEY_1 = ''

youtube = build('youtube', 'v3', developerKey=YOUTUBE_API_KEY_1)

keyword = ''
# Get data before this date
date_before = '2024-1-2'
# Get data after this date
date_after = '2024-1-1'

# Variables to get video information
part = 'snippet'
q = keyword
order = 'date'
type = 'video'
publishedBefore = date_before + 'T00:00:00Z'
publishedAfter = date_after + 'T00:00:00Z'
nextPageToken = None


all_items = []
data=[]
statistics_data=[]
channel_data = []
video_info = pd.DataFrame(data)

while True:
    # get video information and pagetoken by serchlist method
    search_response = youtube.search().list(part=part, 
                                            q=q, 
                                            order=order, 
                                            type=type, 
                                            publishedBefore=publishedBefore, 
                                            publishedAfter=publishedAfter,
                                            pageToken=nextPageToken).execute()
    
    # add video information (dictionary) to the list
    all_items.extend(search_response['items'])

    # get nextpegetoken
    nextPageToken = search_response.get('nextPageToken')
    if not nextPageToken:
        break

# Convert video information (dictionary) into a DataFrame
df = pd.DataFrame(all_items)

# Extract videoId from the 'id' variable for each row
video_id = df['id'].apply(lambda x: x['videoId'])
channel_id = df['snippet'].apply(lambda x: x['channelId'])
# Retrieve basic information from the 'snippet'
video_info['videoId']  = df['id'].apply(lambda x: x['videoId'])
video_info['title'] = df['snippet'].apply(lambda x: x['title'])
video_info['publishedAt'] = df['snippet'].apply(lambda x: x['publishedAt'])
video_info['channelId'] = df['snippet'].apply(lambda x: x['channelId'])
video_info['description'] = df['snippet'].apply(lambda x: x['description'])

# Retrieve statistics for each video from the video_id list
for video in video_id:
    search_response2 = youtube.videos().list(
        id=video,
        part="statistics"
    ).execute()


    # Add statistics to the list
    stats = search_response2['items'][0]['statistics']
    statistics_data.append({
        'videoId': video,
        'viewCount': stats.get('viewCount', 0),
        'likeCount': stats.get('likeCount', 0),
        'commentCount': stats.get('commentCount', 0)
    })
statistics_df = pd.DataFrame(statistics_data)

# Retrieve channel information for each channelId from the channel_id list
for channel in channel_id:
    search_response3 = youtube.channels().list(
        part="statistics",
        id=channel
    ).execute()

    ch = search_response3['items'][0]['statistics']
    channel_data.append({
        'channelId': channel,
        'subscriberCount': ch.get('subscriberCount', 0),
        'videoCount': ch.get('videoCount', 0)
    })
channel_df = pd.DataFrame(channel_data)

# Inner join the data
re = pd.merge(statistics_df, video_info, on='videoId', how = 'inner')
result = pd.merge(re, channel_df, on='channelId', how = 'inner')
print(result)
