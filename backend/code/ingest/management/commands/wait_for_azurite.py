# app/management/commands/wait_for_azurite.py
import socket
import time
from django.core.management.base import BaseCommand

class Command(BaseCommand):
    help = "Wait until Azurite (Blob service) is available on host:port"

    def handle(self, *args, **options):
        host = "azurite"   # docker-compose のサービス名
        port = 10000       # Blob service のポート
        self.stdout.write(f"Waiting for Azurite at {host}:{port}...")

        while True:
            try:
                with socket.create_connection((host, port), timeout=2):
                    break
            except OSError:
                self.stdout.write("Azurite not ready, waiting 1s...")
                time.sleep(1)

        self.stdout.write(self.style.SUCCESS("Azurite is ready!"))
