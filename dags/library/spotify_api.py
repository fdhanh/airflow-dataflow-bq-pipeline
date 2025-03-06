import requests
import base64
import logging

BASE_URL = "https://api.spotify.com/v1"

def get_access_token(client_id, client_secret):
    logging.info("Get new access token")

    # Encode client ID and secret
    auth_string = f"{client_id}:{client_secret}"
    auth_bytes = auth_string.encode("utf-8")
    auth_base64 = base64.b64encode(auth_bytes).decode("utf-8")

    # Set up the request
    url = "https://accounts.spotify.com/api/token"
    headers = {
        "Authorization": f"Basic {auth_base64}",
        "Content-Type": "application/x-www-form-urlencoded",
    }
    data = {
        "grant_type": "client_credentials"
    }

    # Make the POST request
    response = requests.post(url, headers=headers, data=data)
    if response.status_code == 200:
        access_token = response.json().get("access_token")
        return access_token
    else:
        raise "Error: "+ response.status_code + response.text

def get_track_list_metadata_by_keyword(access_token, keyword):
    # get track metadata from spotify by keyword (song title)
    headers = {
        "Authorization": "Bearer "+ access_token
    }
    params = {
        "q": keyword,
        "type": "track"
    }
    
    # make the GET request
    response = requests.get(BASE_URL + '/search', 
                            headers=headers,
                            params=params)
    return response

def get_album_metadata_by_id(access_token, album_id):
    # get album metadata from spotify by id 
    headers = {
        "Authorization": "Bearer "+ access_token
    }
    
    # make the GET request
    response = requests.get(BASE_URL + '/albums/' + str(album_id), 
                            headers=headers)
    return response
