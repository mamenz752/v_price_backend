import os
import logging
import azure.functions as func
from azure.storage.blob import BlobServiceClient

def main(priceTrigger: func.TimerRequest) -> None:
    logging.info("Connection test started")
    try:
        conn_str = os.environ["AzureWebJobsStorage"]
        bsc = BlobServiceClient.from_connection_string(conn_str)
        containers = [container.name for container in bsc.list_containers()]
        logging.info(f"Successfully connected to Blob Storage. Containers: {containers}")
    except Exception as e:
        logging.error(f"Failed to connect to Blob Storage: {str(e)}")