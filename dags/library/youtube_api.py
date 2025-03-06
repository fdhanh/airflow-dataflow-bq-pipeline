from googleapiclient.discovery import build

def open_connection(developer_key):
    client = build("youtube", "v3", developerKey=developer_key)
    return client

def get_video_ids_by_keyword(developer_key, keyword, 
                                       max_result=20):
    client = open_connection(developer_key)
    video_ids = []
    next_page_token = None

    while len(video_ids) < max_result:
        request = client.search().list(
            part='snippet',
            q=keyword,
            type='video',
            pageToken=next_page_token
        )
        response = request.execute()
        video_ids += [x['id']['videoId'] for x in response['items']]
        next_page_token = response['nextPageToken']

    return video_ids[:max_result]

def get_videos_metadata_by_ids(developer_key, video_ids=[]):
    client = open_connection(developer_key)
    request = client.videos().list(
        part='snippet,status',
        id=','.join(video_ids)
    )
    response = request.execute()
    return response