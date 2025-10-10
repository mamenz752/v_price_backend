# app/management/commands/init_azurite.py
from django.core.management.base import BaseCommand
from django.conf import settings
from azure.storage.blob import BlobServiceClient

class Command(BaseCommand):
    help = "Create Azurite container if not exists"

    def handle(self, *args, **options):
        bsc = BlobServiceClient.from_connection_string(settings.AZURE_CONNECTION_STRING)
        container = bsc.get_container_client(settings.AZURE_CONTAINER)

        try:
            container.create_container()
            self.stdout.write(self.style.SUCCESS(f"Created container: {settings.AZURE_CONTAINER}"))
        except Exception as e:
            self.stdout.write(self.style.WARNING(f"Container may already exist: {e}"))
