# 野菜価格データ取り込みシステム (v_price_backend)

## 概要

このシステムは野菜の価格データと気象データを取り込み、分析するためのDjangoアプリケーションです。
Azuriteストレージやローカルファイルシステムからデータをインポートし、PostgreSQLデータベースに保存します。

## システム構成

- **バックエンド**: Django (Python)
- **データベース**: PostgreSQL
- **ストレージ**: Azurite (Azure Storage エミュレーター)
- **コンテナ化**: Docker, Docker Compose

## 環境構築手順

1. `docker compose up -d`
2. `docker compose run --rm django-migrate`
3. `docker compose exec web python manage.py createsuperuser`
4. `docker compose exec web python manage.py seed_data`

データベースとボリュームを含む全てのDocker資産をクリアする場合:

```bash
docker system prune -a --volumes
```

## 主要な機能

1. テキストファイルからの野菜価格データのインポート
2. CSVファイルからの気象データのインポート
3. 管理画面からのデータインポート機能
4. データ分析と可視化（今後実装予定）

## ディレクトリ構造

```text
.
├── backend/               # Djangoバックエンド
│   ├── Dockerfile         # Djangoアプリ用Dockerfile
│   ├── requirements.txt   # Pythonパッケージ依存関係
│   └── code/              # Djangoアプリケーションコード
├── data/                  # データファイル
│   ├── price/             # 野菜価格データ
│   └── weather/           # 気象データ
├── db_data/               # PostgreSQLデータ
└── docker-compose.yml     # Docker Compose設定
```

## モデル構造

### 野菜マスター (Vegetable)

- name: 野菜名
- code: 野菜コード

### 地域マスター (Region)

- name: 地域名
- weather_code: 気象データコード

### 市場データ (IngestMarket)

- target_date: 対象日
- high_price: 最高価格
- medium_price: 中央値価格
- low_price: 最低価格
- average_price: 平均価格
- arrival_amount: 入荷量
- weight_per: 単位重量
- trend: 価格傾向 (上昇、下降、安定など)
- vegetable: 野菜 (Vegetableへの外部キー)

### 気象データ (IngestWeather)

- target_date: 対象日
- max_temp: 最高気温
- mean_temp: 平均気温
- min_temp: 最低気温
- sum_precipitation: 総降水量
- sunshine_duration: 日照時間
- ave_humidity: 平均湿度
- region: 地域 (Regionへの外部キー)

## データインポート方法

### 管理画面からのインポート

1. [http://localhost:8000/admin/](http://localhost:8000/admin/) にアクセス
2. スーパーユーザーでログイン
3. 「Ingest」アプリから必要なインポート機能を選択

### コマンドラインからのインポート

全てのデータをインポート:

```bash
docker compose exec web python manage.py import_azurite_data
```

価格データのみインポート:

```bash
docker compose exec web python manage.py import_azurite_data --price-only
```

気象データのみインポート:

```bash
docker compose exec web python manage.py import_azurite_data --weather-only
```

特定ディレクトリからインポート:

```bash
docker compose exec web python manage.py import_azurite_data --price-dir /data/price/2022 --weather-dir /data/weather/2022
```

## ファイル形式

### 価格データ (txt)

ファイル名: `YYYY-MM-DD.txt`

```text
最高値: 123.45
中央値: 100.0
最安値: 80.5
平均: 101.2
入荷量: 500
重量: 100
傾向: 上昇
```

### 気象データ (csv)

ファイル名: `YYYY_MM_mid.csv` または `YYYY_MM_last.csv`

```csv
年,月,日,最高気温,平均気温,最低気温,降水量の合計,日照時間,平均湿度
2022,1,15,10.5,5.2,0.1,10,3.5,65
```

## 開発者向け情報

### コードの主要なクラス

- **DataParser**: データ解析の基本クラス
- **MarketDataParser**: 市場価格データ解析クラス
- **WeatherDataParser**: 気象データ解析クラス
- **DataSaver**: データ保存クラス
- **FileProcessor**: ファイル処理クラス

### 新しいデータ形式のサポート追加方法

1. 適切なParserクラスを拡張
2. 新しいファイル形式に対応するメソッドを実装
3. 必要に応じてDataSaverにメソッドを追加
4. FileProcessorを更新して新しい解析メソッドを呼び出す

## トラブルシューティング

### データベース接続エラー

PostgreSQLコンテナが起動しているか確認:

```bash
docker compose ps
```

### ファイルインポートエラー

ログを確認:

```bash
docker compose logs web
```

### 管理画面アクセスエラー

マイグレーションが実行されているか確認:

```bash
docker compose exec web python manage.py showmigrations
```

### マイグレーションの実行方法

モデルを変更した後のマイグレーション実行（Azurite接続なし）:

```bash
docker compose run --rm django-migrate
```

このコマンドはAzuriteに接続せずにマイグレーションのみを実行します。
