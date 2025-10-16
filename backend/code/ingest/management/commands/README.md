# シードデータ登録用コマンド

このコマンドは、システムで使用する野菜と地域のマスターデータを登録するためのものです。

## 使い方

Djangoのマネジメントコマンドとして実行します：

```bash
# Docker環境内で実行
docker compose exec backend python manage.py seed_data

# ローカルで実行する場合
python manage.py seed_data
```

## 登録されるデータ

### 野菜データ
- キャベツ (code: 1)
- レタス (code: 2)
- ハクサイ (code: 3)
- ホウレンソウ (code: 4)
- ニンジン (code: 5)
- ダイコン (code: 6)
- タマネギ (code: 7)
- ジャガイモ (code: 8)
- トマト (code: 9)
- ナス (code: 10)
- キュウリ (code: 11)
- ピーマン (code: 12)

### 地域データ
- 北海道 (price_code: 1, weather_code: 1)
- 東北 (price_code: 2, weather_code: 2)
- 関東 (price_code: 3, weather_code: 3)
- 中部 (price_code: 4, weather_code: 4)
- 近畿 (price_code: 5, weather_code: 5)
- 中国 (price_code: 6, weather_code: 6)
- 四国 (price_code: 7, weather_code: 7)
- 九州 (price_code: 8, weather_code: 8)
- 沖縄 (price_code: 9, weather_code: 9)

## 注意事項

- 既にデータが登録されている場合は、重複登録を避けるためにスキップされます
- エラーが発生した場合は、トランザクションがロールバックされ、データは登録されません