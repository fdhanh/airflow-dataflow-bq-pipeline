from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from airflow.providers.google.cloud.operators.dataflow import DataflowTemplatedJobStartOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow import DAG

import library.cloud_storage as gcs
from library import spotify_api, youtube_api

import gspread
from datetime import datetime
import json
import logging
import os

SPREADSHEET_ID=os.getenv("SPREADSHEET_ID")
SPREADSHEET_SHEET_NAME=os.getenv("SPREADSHEET_SHEET_NAME")
SPOTIFY_CLIENT_ID=os.getenv("SPOTIFY_CLIENT_ID")
SPOTIFY_CLIENT_SECRET=os.getenv("SPOTIFY_CLIENT_SECRET")
GOOGLE_API_KEY=os.getenv("GOOGLE_API_KEY")
GCS_BUCKET=os.getenv("GCS_BUCKET")
GCP_PROJECT=os.getenv("GCP_PROJECT")
GCP_REGION=os.getenv("GCP_REGION")

default_args = {
    "start_date": datetime(2024, 3, 4),
    "retries": 1,
}

def get_current_date(context):
    return context["data_interval_end"].strftime("%Y%m%d")

def get_song_list(context):
    """
    Get song list from gspread
    id:1OkDM1miCXh48M23n_C4AOGPl_DW1QtIXFSdHW6Grzxg; 
    sheet: Data
    """
    # get client service using service account
    gcp_hook = GoogleCloudBaseHook(gcp_conn_id="service-acc")
    creds = json.loads(gcp_hook._get_field('keyfile_dict'))
    client = gspread.service_account_from_dict(creds)

    # open spreadsheet
    spreadsheet = client.open_by_key(SPREADSHEET_ID)
    worksheet = spreadsheet.worksheet(SPREADSHEET_SHEET_NAME)
    data = worksheet.get_all_records()[:5]
    return data

def fetch_spotify_api_data(**context):
    """
    Get data from Spotify API 
    Reference: https://developer.spotify.com/
    """
    # get current date
    current_date = get_current_date(context)
    
    # get song list
    song_list = get_song_list(context)  

    # get token
    access_token = spotify_api.get_access_token(
        client_id=SPOTIFY_CLIENT_ID,
        client_secret=SPOTIFY_CLIENT_SECRET
    )
    
    album_ids = []

    # get data for each song using search list
    for row in song_list:
        retry = True
        while retry:
            response = spotify_api.get_track_list_metadata_by_keyword(
                access_token=access_token, 
                keyword=row['SONG TITLE']
                )
            data_json = json.loads(response.text)
            if response.status_code == 200:
                retry = False
            elif response.status_code == 401:
                logging.info(f"{data_json}")
                access_token = spotify_api.get_access_token()
            else:
                retry = False
        
        data = {}
        data['items'] = data_json['tracks']['items']
        data['code'] = row['CODE']
        data['original_artist'] = row['ORIGINAL ARTIST']
        data['song_title'] = row['SONG TITLE']
        

        # get album ids from data
        album_ids+=[sl['album']['id'] for sl in data['items']]

        data=json.dumps(data, indent=4)

        # push into gcs
        gcs.write(data=data,
                  gcs_bucket=GCS_BUCKET,
                  gcs_path=f"spotify/{current_date}/search_list/{row['CODE']}.json")
        
    # get album metadata
    for album_id in album_ids:
        if album_id is not None:
            retry=True
            while retry:
                response = spotify_api.get_album_metadata_by_id(
                    access_token=access_token,
                    album_id=album_id
                )
                data_json = json.loads(response.text)
                if response.status_code == 200:
                    retry = False
                elif response.status_code == 401:
                    logging.info(f"{data_json}")
                    access_token = spotify_api.get_access_token()
                else:
                    retry = False

            data = json.dumps(data_json, indent=4)

            gcs.write(data=data,
                    gcs_bucket=GCS_BUCKET,
                    gcs_path=f"spotify/{current_date}/album_details/{album_id}.json")
        
def fetch_youtube_api_data(**context):
    """
    Get data from Youtube API.
    Reference: https://developers.google.com/youtube/v3
    """
    # get current date
    current_date = get_current_date(context)

    # get song list
    song_list = get_song_list(context)

    # get data for each song title
    for row in song_list:
        video_ids = youtube_api.get_video_ids_by_keyword(developer_key=GOOGLE_API_KEY,
                                                         keyword=row['SONG TITLE'])
        
        logging.info(video_ids)

        # get video details for each video
        data_details = youtube_api.get_videos_metadata_by_ids(developer_key=GOOGLE_API_KEY,
                                                      video_ids=video_ids)
        
        logging.info(data_details)

        data = {}
        data['items'] = data_details['items']
        data['code'] = row['CODE']
        data['original_artist'] = row['ORIGINAL ARTIST']
        data['song_title'] = row['SONG TITLE']
        data=json.dumps(data, indent=4)

        # push into gcs
        gcs.write(data=data,
                  gcs_bucket=GCS_BUCKET,
                  gcs_path=f"youtube/{current_date}/search_list/{row['CODE']}.json")

with DAG(
    "etl_api_stream_platform",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False
) as dag:
    
    # fetch data from API
    extract_api_spotify = PythonOperator(
        task_id="fetch_spotify_api_data",
        python_callable=fetch_spotify_api_data
    )

    extract_api_youtube = PythonOperator(
        task_id="fetch_youtube_api_data",
        python_callable=fetch_youtube_api_data
    )

    # run transformation using dataflow
    transform_search_list_spotify = DataflowTemplatedJobStartOperator(
        task_id="transform_search_list_spotify",
        template=f"gs://{GCS_BUCKET}/transformation_script/search_list",
        parameters={
            "input":"gs://{}/spotify/{{ data_interval_end.strftime('%Y%m%d') }}/search_list/*.json".format(GCS_BUCKET),
            "output":"gs://{}/transformed_data/spotify/{{ data_interval_end.strftime('%Y%m%d') }}/search_list".format(GCS_BUCKET),
        }
    )

    transform_album_list_spotify = DataflowTemplatedJobStartOperator(
        task_id="transform_album_list_spotify",
        template=f"gs://{GCS_BUCKET}/transformation_script/album_list",
        parameters={
            "input":"gs://{}/spotify/{{ data_interval_end.strftime('%Y%m%d') }}/album_list/*.json".format(GCS_BUCKET),
            "output":"gs://{}/transformed_data/spotify/{{ data_interval_end.strftime('%Y%m%d') }}/album_list".format(GCS_BUCKET),
        }
    )

    transform_search_list_youtube = DataflowTemplatedJobStartOperator(
        task_id="transform_search_list_youtube",
        template=f"gs://{GCS_BUCKET}/transformation_script/search_list",
        parameters={
            "input":"gs://{}/youtube/{{ data_interval_end.strftime('%Y%m%d') }}/search_list/*.json".format(GCS_BUCKET),
            "output":"gs://{}/transformed_data/youtube/{{ data_interval_end.strftime('%Y%m%d') }}/search_list".format(GCS_BUCKET),
        }
    )

    # load from cloud storage to bigquery
    load_search_list_spotify = GCSToBigQueryOperator(
        task_id="load_search_list_spotify",
        bucket=GCS_BUCKET,
        source_objects=["transformed_data/spotify/{{ data_interval_end.strftime('%Y%m%d') }}/search_list/*.json".format(GCS_BUCKET)],
        destination_project_dataset_table=f"{GCP_PROJECT}:transformed_table.spotify___search_list",
        source_format='NEWLINE_DELIMITED_JSON',
        write_disposition="WRITE_APPEND"
    )

    load_album_list_spotify = GCSToBigQueryOperator(
        task_id="load_album_list_spotify",
        bucket=GCS_BUCKET,
        source_objects=["transformed_data/spotify/{{ data_interval_end.strftime('%Y%m%d') }}/album_list/*.json".format(GCS_BUCKET)],
        destination_project_dataset_table=f"{GCP_PROJECT}:transformed_table.spotify___album_list",
        source_format='NEWLINE_DELIMITED_JSON',
        write_disposition="WRITE_APPEND"
    )

    load_search_list_youtube = GCSToBigQueryOperator(
        task_id="load_search_list_youtube",
        bucket=GCS_BUCKET,
        source_objects=["transformed_data/youtube/{{ data_interval_end.strftime('%Y%m%d') }}/search_list/*.json".format(GCS_BUCKET)],
        destination_project_dataset_table=f"{GCP_PROJECT}:transformed_table.youtube___search_list",
        source_format='NEWLINE_DELIMITED_JSON',
        write_disposition="WRITE_APPEND"
    )

    extract_api_spotify >> transform_search_list_spotify >> load_search_list_spotify
    extract_api_spotify >> transform_album_list_spotify >> load_album_list_spotify
    extract_api_youtube >> transform_search_list_youtube >> load_search_list_youtube