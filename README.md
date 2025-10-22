# 野菜価格データ分析システム (v_price_backend)

## 概要

このシステムは野菜の価格データと気象データを取り込み、分析するためのDjangoアプリケーションです。
Azuriteストレージやローカルファイルシステムからデータをインポートし、PostgreSQLデータベースに保存します。
取り込んだデータを集計・分析し、半月ごとのデータとして提供します。
また、野菜価格の予測機能やレポート生成機能も実装されています。

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

1. テキストファイルからの野菜価格データのインポート (ingestアプリ)
2. CSVファイルからの気象データのインポート (ingestアプリ)
3. 取り込んだデータの半月単位での集計 (computeアプリ)
4. 野菜価格の予測機能 (forecastアプリ)
5. レポート生成と可視化 (reportsアプリ)
6. Webインターフェースによるデータ閲覧
7. 管理画面からのデータインポート・集計・予測機能
8. ユーザー認証・管理 (accountsアプリ)
9. フィードバック収集 (feedbackアプリ)

## 詳細ドキュメント

詳細な説明は以下のドキュメントを参照してください：

- [データモデル概要](docs/data_models.md)
- [インポートシステムの解説](docs/import_system.md)
- [データ集計処理の解説](docs/compute_system.md)
- [価格予測システムの解説](docs/forecast_system.md)
- [システムアーキテクチャ](docs/architecture.md)
- [開発者ガイド](docs/developer_guide.md)
- [アプリケーションルーティングマップ](docs/routing_map.md)

## ディレクトリ構造

```text
.
├── backend/               # Djangoバックエンド
│   ├── Dockerfile         # Djangoアプリ用Dockerfile
│   ├── requirements.txt   # Pythonパッケージ依存関係
│   └── code/              # Djangoアプリケーションコード
│       ├── accounts/      # ユーザー管理アプリ
│       ├── compute/       # データ集計アプリ
│       ├── config/        # Django設定
│       ├── feedback/      # フィードバックアプリ
│       ├── forecast/      # 価格予測アプリ
│       ├── ingest/        # データ取込アプリ
│       └── reports/       # レポート生成アプリ
├── data/                  # データファイル
│   ├── price/             # 野菜価格データ
│   └── weather/           # 気象データ
├── db_data/               # PostgreSQLデータ
├── docs/                  # プロジェクト文書
└── docker-compose.yml     # Docker Compose設定
```

## モデル構造概要

### ingestアプリ - データ取り込みモデル

- **Vegetable**: 野菜マスター（名前、コード）
- **Region**: 地域マスター（名前、産地コード、市場コード、県コード、観測所コード）
- **IngestMarket**: 市場データ（日付、価格情報、数量情報など）
- **IngestWeather**: 気象データ（日付、気温、降水量、日照時間など）

### computeアプリ - データ集計モデル

- **ComputeMarket**: 半月単位の市場データ集計
- **ComputeWeather**: 半月単位の気象データ集計

### forecastアプリ - 予測モデル

- **ForecastModelKind**: 予測モデルの種類（タグ名、対象野菜）
- **ForecastModelVariable**: 予測モデルの変数（名前、前期間）
- **ForecastModelFeatureSet**: 予測モデルの特徴セット（対象月、モデル種類、変数）
- **ForecastModelVersion**: 予測モデルのバージョン（対象月、アクティブ状態、モデル種類）
- **ForecastModelCoef**: 予測モデルの係数（変数、係数、統計値）
- **ForecastModelEvaluation**: 予測モデルの評価指標（相関係数、R二乗、RMSE等）

## データフロー

1. **データソース**: テキストファイル（価格データ）、CSVファイル（気象データ）
2. **データ取り込み**: ingestアプリによる生データの取り込み
3. **データ集計**: computeアプリによる半月単位の集計処理
4. **データ予測**: forecastアプリによる価格予測モデルの適用
5. **データ活用**:
   - reportsアプリによるレポート生成と可視化
   - Webインターフェースによるデータ閲覧
   - APIエンドポイントからのデータ取得（開発中）

## 管理インターフェース

システムの管理画面では以下の操作が可能です：

1. データインポート操作
   - 全データインポート
   - 価格データのみのインポート
   - 気象データのみのインポート

2. データ集計操作
   - 全データの集計（価格＋天気）
   - 価格データのみの集計
   - 天気データのみの集計
   - 集計データのリセット

3. 予測モデル管理
   - 予測モデル種類の管理
   - 予測モデル変数の管理
   - 予測モデル特徴セットの管理
   - 予測モデルバージョンの管理
   - 予測モデル係数の管理
   - 予測モデル評価指標の管理

4. マスターデータ管理
   - 野菜マスター
   - 地域マスター

## 運用とメンテナンス

### データベース操作

マイグレーションの実行（Azurite接続なし）:

```bash
docker compose run --rm django-migrate
```

このコマンドはAzuriteに接続せずにマイグレーションのみを実行します。

### 一般的なトラブルシューティング

#### データベース接続エラー

PostgreSQLコンテナが起動しているか確認:

```bash
docker compose ps
```

#### ファイルインポートエラー

ログを確認:

```bash
docker compose logs web
```

#### 管理画面アクセスエラー

マイグレーションが実行されているか確認:

```bash
docker compose exec web python manage.py showmigrations
```
