# Code inspiration and snippets from:
# youtube api sample
# https://github.com/youtube/api-samples/blob/master/python/yt_analytics_v2.py

import os

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

class YouTubeHandler:
    def __init__(self):
        # retrieve YouTube Data API v3 from environ variables
        DEVELOPER_KEY = os.environ.get("YT_DATA_API_V3_KEY2")
        API_SERVICE_NAME = 'youtube'
        API_VERSION = 'v3'

        # get youtube service
        self.youtube_service = build(API_SERVICE_NAME, API_VERSION,
                                     developerKey=DEVELOPER_KEY)

    def request_videos(self, regionCode="US", videoCategoryId=0, maxResults=1):
        """
        Retrieve most popular videos

        videoCategoryId -- 0 default, do not filter by category Id
        """
        try:
            # For parameters refer -- https://developers.google.com/youtube/v3/guides/implementation/videos
            request = self.youtube_service.videos().list(part='id,snippet,statistics',
                                                        chart='mostPopular',
                                                        regionCode=regionCode,
                                                        videoCategoryId=videoCategoryId,
                                                        maxResults=maxResults
                                                        )
            response = request.execute()
        
        except HttpError as e:
            print("HTTP error in bridge: ", str(e))
            response = []
        
        return response

    def parse_response(self, response):
        """
        Pick the matched videos i.e items in the response and parse them into a dic
        """

        # Youtube video Resource structure
        # https://developers-dot-devsite-v2-prod.appspot.com/youtube/v3/docs/videos#resource

        parsed_response = []

        for item in response['items']:

            # each item is a list containing one 'video Resource' object
            # item -- contains 'kind', 'etag', 'id', 'snippet', 'statistics'
            # snippet -- contains 'publishedAt', 'channelId', 'title',
            #            'description', 'thumbnails', 'channelTitle', 
            #            'categoryId', 'liveBroadcastContent', 'localized'
            parsed_item = dict(id=item['id'], 
                               title=item['snippet']['title'], 
                               channelId=item['snippet']['channelId'], 
                               categoryId=item['snippet']['categoryId'],
                               channelTitle=item['snippet']['channelTitle'], 
                               publisedAt=item['snippet']['publishedAt']
                               )

            # Statistics contains a dictionary containing
            # contains the count of views, likes, dislikes, favorites, comments
            parsed_item.update(item['statistics'])

            parsed_response.append(parsed_item)
        
        return parsed_response

