# app/management/commands/seed_blobs.py
from pathlib import Path
import os
from django.core.management.base import BaseCommand
from django.conf import settings
from azure.storage.blob import BlobServiceClient


class Command(BaseCommand):
    help = (
        "Upload all files under the seed directory to Azurite container.\n"
        "Priority of seed directory:\n"
        " 1) CLI arg (src_dir)\n"
        " 2) env SEED_BLOBS_DIR\n"
        " 3) '/data' (mounted volume)\n"
        " 4) <BASE_DIR>/../../data  (backend/code から 2 つ上の data)\n"
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "src_dir",
            nargs="?",
            default=None,
            help="Path to seed directory (optional). If omitted, auto-detect.",
        )

    def handle(self, *args, **opts):
        # 1) CLI引数
        src_dir = opts.get("src_dir")

        # 2) 環境変数
        if not src_dir:
            src_dir = os.environ.get("SEED_BLOBS_DIR")

        # 3) /data があればそれを優先（docker で ./data:/data をマウントしている場合）
        if not src_dir and Path("/data").exists():
            src_dir = "/data"

        # 4) backend/code から 2 つ上（= ルート）にある data/
        if not src_dir:
            # settings.BASE_DIR は通常 manage.py のあるディレクトリ（= backend/code）を指す想定
            base_dir = Path(settings.BASE_DIR).resolve()
            candidate = base_dir.parent.parent / "data"   # backend/code -> backend -> <root>/data
            if candidate.exists():
                src_dir = str(candidate)

        if not src_dir:
            self.stderr.write(self.style.ERROR("Seed directory could not be resolved."))
            self.stderr.write(self.style.ERROR(
                "Try: `python manage.py seed_blobs /absolute/path/to/data` "
                "or set SEED_BLOBS_DIR env, or mount ./data to /data."
            ))
            return

        src = Path(src_dir).resolve()
        if not src.exists():
            self.stderr.write(self.style.ERROR(f"Source dir not found: {src}"))
            return
        if not src.is_dir():
            self.stderr.write(self.style.ERROR(f"Source is not a directory: {src}"))
            return

        bsc = BlobServiceClient.from_connection_string(settings.AZURE_CONNECTION_STRING)
        container = bsc.get_container_client(settings.AZURE_CONTAINER)

        count = 0
        for p in src.rglob("*"):
            if p.is_file():
                blob_name = str(p.relative_to(src)).replace("\\", "/")
                with open(p, "rb") as f:
                    container.upload_blob(name=blob_name, data=f, overwrite=True)
                count += 1
                self.stdout.write(self.style.SUCCESS(f"Seeded: {blob_name}"))

        self.stdout.write(self.style.SUCCESS(f"Done. Uploaded {count} files from {src}"))
