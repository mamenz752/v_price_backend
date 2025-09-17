# functions/Dockerfile
FROM mcr.microsoft.com/azure-functions/python:4-python3.11

# （任意）ビルドに必要なら OS 依存ライブラリを追加
# RUN apt-get update && apt-get install -y build-essential libpq-dev && rm -rf /var/lib/apt/lists/*

# 依存インストール
WORKDIR /home/site/wwwroot
# COPY requirements.txt .
# RUN pip install --no-cache-dir -r requirements.txt

# 関数と設定を配置
COPY host.json .

# 既定のエントリポイントは base イメージ側で func host を起動
# ローカルでは docker-compose で 7071 を開けておけばOK
