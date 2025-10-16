# ベースイメージ
FROM python:3.11

# 日本語環境の設定
ENV LANG ja_JP.UTF-8
ENV LC_ALL ja_JP.UTF-8
ENV LC_CTYPE ja_JP.UTF-8
ENV PYTHONIOENCODING=utf-8
ENV PYTHONUNBUFFERED=1

# 日本語ロケールのインストール
RUN apt-get update && apt-get install -y locales && \
    echo "ja_JP.UTF-8 UTF-8" > /etc/locale.gen && \
    locale-gen

# 作業ディレクトリの作成
WORKDIR /code

# パッケージインストール
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Djangoプロジェクトファイルを全てコピー（必要に応じて調整）
COPY . /code
