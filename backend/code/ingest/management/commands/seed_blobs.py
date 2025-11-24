# app/management/commands/seed_blobs.py
from pathlib import Path
import os
import re
from datetime import datetime
from django.core.management.base import BaseCommand
from django.conf import settings
from config.storage.azure_blob import get_blob_service_client


class Command(BaseCommand):
    help = (
        "Upload all files under the seed directory to Azurite container.\n"
        "Priority of seed directory:\n"
        " 1) CLI arg (src_dir)\n"
        " 2) env SEED_BLOBS_DIR\n"
        " 3) '/data' (mounted volume)\n"
        " 4) <BASE_DIR>/../../data  (backend/code から 2 つ上の data)\n"
        "Files will be uploaded to:\n"
        " - price data: price/YYYY/MM/YYYY-MM-DD.txt\n"
        " - weather data: weather/YYYY/MM/YYYY_MM_mid.csv or YYYY_MM_last.csv\n"
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

        try:
            container = get_blob_service_client()
        except Exception as e:
            self.stderr.write(self.style.ERROR(f"Azurite connection error: {str(e)}"))
            return

        count = 0
        for p in src.rglob("*"):
            if p.is_file():
                # 相対パスを取得
                rel_path = str(p.relative_to(src)).replace("\\", "/")

                # 元のパスがtxt/プレフィックスを持つ場合は除去
                original_path = rel_path
                if rel_path.startswith("txt/"):
                    rel_path = rel_path[4:]

                # ファイルタイプとパターンに基づいて適切なパスを構築
                blob_name = self.determine_blob_path(rel_path)
                if blob_name:
                    self.stdout.write(f"変換: {original_path} -> {blob_name}")
                    try:
                        with open(p, "rb") as f:
                            container.upload_blob(name=blob_name, data=f, overwrite=True)
                        count += 1
                        self.stdout.write(self.style.SUCCESS(f"Seeded: {blob_name}"))
                    except Exception as e:
                        self.stderr.write(self.style.ERROR(f"Upload error {blob_name}: {str(e)}"))
                else:
                    self.stdout.write(self.style.WARNING(f"スキップ: {rel_path} - パターンに一致しません"))

        self.stdout.write(self.style.SUCCESS(f"Done. Uploaded {count} files from {src}"))
        
    def determine_blob_path(self, file_path):
        """
        ファイルのパスに基づいて、適切なBlobのパスを決定する
        
        priceデータ: price/2019/01/2019-01-05.txt
        weatherデータ: weather/2019/01/2019_01_mid.csv
        """
        # 最初に既にprice/やweather/で始まるパスは維持する
        if file_path.startswith('price/') or file_path.startswith('weather/'):
            return file_path
        
        file_name = os.path.basename(file_path)
        
        # priceデータ (2019-01-05.txt パターン)
        price_match = re.match(r'(\d{4})-(\d{2})-(\d{2})\.txt$', file_name)
        if price_match:
            year, month, day = price_match.groups()
            return f"price/{year}/{month}/{file_name}"
        
        # weatherデータ (2019_01_mid.csv または 2019_01_last.csv パターン)
        weather_match = re.match(r'(\d{4})_(\d{2})_(mid|last)\.csv$', file_name)
        if weather_match:
            year, month, period = weather_match.groups()
            return f"weather/{year}/{month}/{file_name}"
        
        # その他の既知のパターン (データ移行中に混在している可能性がある)
        try:
            # 年/月/日付.txtパターンの抽出
            path_parts = file_path.split('/')
            if len(path_parts) >= 3:
                potential_year = path_parts[-3]
                potential_month = path_parts[-2]
                if potential_year.isdigit() and potential_month.isdigit() and file_name.endswith('.txt'):
                    return f"price/{potential_year}/{potential_month}/{file_name}"
                elif potential_year.isdigit() and potential_month.isdigit() and file_name.endswith('.csv'):
                    return f"weather/{potential_year}/{potential_month}/{file_name}"
        except:
            pass

        # どのパターンにも一致しない場合はそのまま返す
        return file_path
