# app/management/commands/reset_azurite.py
from django.core.management.base import BaseCommand
from django.conf import settings
from azure.storage.blob import BlobServiceClient

class Command(BaseCommand):
    help = "Delete and recreate Azurite container"

    def handle(self, *args, **options):
        # Azuriteに接続
        bsc = BlobServiceClient.from_connection_string(settings.AZURE_CONNECTION_STRING)
        container_name = settings.AZURE_CONTAINER
        
        try:
            # コンテナが存在する場合は削除
            container_client = bsc.get_container_client(container_name)
            container_client.delete_container()
            self.stdout.write(self.style.SUCCESS(f"Deleted container: {container_name}"))
        except Exception as e:
            self.stdout.write(self.style.WARNING(f"Container may not exist: {e}"))
        
        try:
            # コンテナを再作成
            container_client = bsc.create_container(container_name)
            self.stdout.write(self.style.SUCCESS(f"Created container: {container_name}"))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Failed to create container: {e}"))
            return
            
        self.stdout.write(self.style.SUCCESS(f"Container {container_name} has been reset."))
        self.stdout.write("Now run 'python manage.py seed_blobs' to upload files again.")