from google.cloud import storage
import logging
import pyarrow as pa
import pyarrow.parquet as pq
import io

def open_connection():
    """Open connection for Google Cloud Storage"""

    client = storage.Client()
    return client

# def write(str_value: str, gcs_url: str, client=None):
#     """Upload file from local path to gcs path"""
#     logging.info(f"Upload string to {gcs_url}.")

#     if client is None:
#         client = open_connection()

#     # gcs_path = parse_gs_url(gcs_url)
#     # bucket = client.bucket(gcs_path['bucket_name'])
#     # blob = bucket.blob(gcs_path['path'])
#     # blob.upload_from_string(str_value, num_retries=3)

#     bucket = client.bucket(bucket_name)

#     return gcs_url

def write(data: str, gcs_bucket:str, 
          gcs_path: str, client=None,
          content_type="application/json"):
    """Upload file from string to gcs path"""
    logging.info(f"Upload string to gs://{gcs_bucket}/{gcs_path}")

    if client is None:
        client = open_connection()

    bucket = client.bucket(gcs_bucket)
    blob = bucket.blob(gcs_path)

    # if content_type == "application/octet-stream":
    #     # convert to PyArrow table
    #     table = pa.Table.from_pydict(data)

    #     # save to buffer parquet
    #     buffer = io.BytesIO()
    #     pq.write_table(table, buffer)
    #     buffer.seek(0)

    #     # upload
    #     blob.upload_from_string(buffer.getvalue(),
    #                             content_type=content_type)
    
    # else:
    blob.upload_from_string(data, 
                            content_type="application/json")

    return f"gs://{gcs_bucket}/{gcs_path}"