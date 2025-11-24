import os
from azure.storage.blob import BlobServiceClient, ContainerClient
from azure.core.credentials import AzureSasCredential

def get_blob_service_client():
    """
    Azure Blob Service Clientを取得する関数。
    環境変数AZURE_SAS_TOKENが設定されている場合はSASトークンを使用し、
    設定されていない場合は接続文字列を使用して接続します。
    """
    mode = os.getenv('AZURE_BLOB_AUTH_MODE', 'connection_string')
    container_name = os.getenv('AZURE_STORAGE_CONTAINER')

    if mode == "connection_string":
        conn_str = os.getenv("AZURE_CONNECTION_STRING")
        return ContainerClient.from_connection_string(conn_str, container_name)
    elif mode == "sas":
        account_url = os.getenv('AZURE_STORAGE_ACCOUNT_URL')
        sas_token = os.getenv('AZURE_STORAGE_SAS_TOKEN')
        blob_service = BlobServiceClient(
            account_url=account_url,
            credential=AzureSasCredential(sas_token)
        )
        return blob_service.get_container_client(container_name)
    else:
        raise RuntimeError("AZURE_BLOB_AUTH_MODEが不正、または必要な環境変数が設定されていません。")

