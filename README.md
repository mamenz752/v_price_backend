# 野菜価格データ分析システム (v_price_backend)

## 概要

このシステムは野菜の価格データと気象データを取り込み、分析するためのDjangoアプリケーションです。
Azuriteストレージやローカルファイルシステムからデータをインポートし、PostgreSQLデータベースに保存します。
取り込んだデータを集計・分析し、半月ごとのデータとして提供します。

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
4. 管理画面からのデータインポート・集計機能
5. ユーザー認証・管理 (accountsアプリ)
6. レポート作成 (今後拡張予定)

## 詳細ドキュメント

詳細な説明は以下のドキュメントを参照してください：

- [データモデル概要](docs/data_models.md)
- [インポートシステムの解説](docs/import_system.md)
- [データ集計処理の解説](docs/compute_system.md)
- [システムアーキテクチャ](docs/architecture.md)
- [開発者ガイド](docs/developer_guide.md)

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

## モデル構造概要

### ingestアプリ - データ取り込みモデル

- **Vegetable**: 野菜マスター（名前、コード）
- **Region**: 地域マスター（名前、産地コード、市場コード、県コード、観測所コード）
- **IngestMarket**: 市場データ（日付、価格情報、数量情報など）
- **IngestWeather**: 気象データ（日付、気温、降水量、日照時間など）

### computeアプリ - データ集計モデル

- **ComputeMarket**: 半月単位の市場データ集計
- **ComputeWeather**: 半月単位の気象データ集計

## データフロー

1. **データソース**: テキストファイル（価格データ）、CSVファイル（気象データ）
2. **データ取り込み**: ingestアプリによる生データの取り込み
3. **データ集計**: computeアプリによる半月単位の集計処理
4. **データ活用**: 分析・可視化（今後拡張予定）

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

3. マスターデータ管理
   - 野菜マスター
   - 地域マスター

## トラブルシューティング

## 運用とメンテナンス

### データベース操作

マイグレーションの実行（Azurite接続なし）:

```bash
docker compose run --rm django-migrate
```

このコマンドはAzuriteに接続せずにマイグレーションのみを実行します。

### トラブルシューティング

**データベース接続エラー**

PostgreSQLコンテナが起動しているか確認:

```bash
docker compose ps
```

**ファイルインポートエラー**

ログを確認:

```bash
docker compose logs web
```

**管理画面アクセスエラー**

マイグレーションが実行されているか確認:

```bash
docker compose exec web python manage.py showmigrations
```
