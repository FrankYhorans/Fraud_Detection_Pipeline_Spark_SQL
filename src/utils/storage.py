import os

def get_storage_options() -> dict:
    return {
        "key": os.environ.get("MINIO_ROOT_USER"),
        "secret": os.environ.get("MINIO_ROOT_PASSWORD"),
        "client_kwargs": {"endpoint_url": "http://minio:9000"},
        
    }