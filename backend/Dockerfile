# ベースイメージ
FROM python:3.11

# 作業ディレクトリの作成
WORKDIR /code

# パッケージインストール
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Djangoプロジェクトファイルを全てコピー（必要に応じて調整）
COPY . /code
