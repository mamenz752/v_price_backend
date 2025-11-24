#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pathlib import Path
from config.storage.azure_blob import get_blob_service_client
from django.conf import settings

# Azurite接続
print("Azurite接続開始")
try:
    container = get_blob_service_client()
    print(f"接続成功: {settings.AZURE_CONTAINER}")
except Exception as e:
    print(f"接続エラー: {str(e)}")
    exit(1)

# 天気データファイルをアップロード
file_path = '/data/weather/2018/01/2018_01_mid.csv'
blob_name = 'weather/2018/01/2018_01_mid.csv'

print(f"ファイル確認: {file_path}")
print(f"存在確認: {Path(file_path).exists()}")

try:
    with open(file_path, 'rb') as f:
        print(f"ファイルを開きました: {file_path}")
        content = f.read()
        print(f"コンテンツ読み取り: {len(content)} バイト")
        container.upload_blob(name=blob_name, data=content, overwrite=True)
        print(f"アップロード成功: {blob_name}")
except Exception as e:
    print(f"アップロードエラー: {str(e)}")