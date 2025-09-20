# scripts/create_container.py
import os
from azure.storage.blob import BlobServiceClient

conn = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
container = os.environ.get("AZURE_STORAGE_CONTAINER", "media")

svc = BlobServiceClient.from_connection_string(conn)
try:
    svc.create_container(container)
except Exception:
    pass  # 既にあれば無視
