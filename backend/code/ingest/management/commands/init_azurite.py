# app/management/commands/init_azurite.py
from django.core.management.base import BaseCommand
from django.conf import settings
from config.storage.azure_blob import get_blob_service_client

class Command(BaseCommand):
    help = "Create Azurite container if not exists"

    def handle(self, *args, **options):
        container = get_blob_service_client()
        try:
            container.create_container()
            self.stdout.write(self.style.SUCCESS(f"Created container: {settings.AZURE_CONTAINER}"))
        except Exception as e:
            self.stdout.write(self.style.WARNING(f"Container may already exist: {e}"))
