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

# Gunicornの本番環境設定
ENV DJANGO_ENV=production

# Gunicorn 起動（DJANGO_ENV で dev/prod 切り替え）
# config.wsgi:application の config はプロジェクト名に変更してください
CMD ["sh", "-c", "\
  if [ \"$DJANGO_ENV\" = \"development\" ]; then \
    echo 'Starting Gunicorn (development)...'; \
    gunicorn config.wsgi:application \
      --bind 0.0.0.0:8000 \
      --workers 1 \
      --reload \
      --log-level debug; \
  else \
    echo 'Starting Gunicorn (production)...'; \
    gunicorn config.wsgi:application \
      --bind 0.0.0.0:8000 \
      --workers 3 \
      --log-level info; \
  fi \
"]