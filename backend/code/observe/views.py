from django.conf import settings
from django.shortcuts import render, redirect
from django.contrib import messages
from django.views.decorators.http import require_POST
from forecast.models import (
    ForecastModelKind, ForecastModelVariable, ForecastModelFeatureSet, ForecastModelVersion
)
from forecast.service.run_ols import (
    ForecastOLSRunner, ForecastOLSConfig
)
from compute.service import aggregate_market_data, aggregate_weather_data
from compute.service import _group_market_records, _group_weather_records, _aggregate_market_group, _aggregate_weather_group
from compute.models import ComputeMarket, ComputeWeather
from ingest.models import IngestMarket, IngestWeather
from django.db import transaction
import logging
import datetime
from typing import Optional, List

from ingest.services import DataIngestor
from ingest.models import Vegetable, Region
import logging
from django.http import JsonResponse, HttpResponseBadRequest, HttpResponseForbidden
from django.views.decorators.csrf import csrf_exempt
import json

logger = logging.getLogger(__name__)

@csrf_exempt
def test_webhook(request):
    """テスト用のWebhookエンドポイント"""
    logger.info(f"TestWebhook called: method={request.method}, path={request.path}")
    logger.info(f"Headers: {dict(request.headers)}")
    if request.body:
        logger.info(f"Body: {request.body.decode('utf-8', errors='replace')}")
    
    return JsonResponse({
        "status": "success",
        "message": "Test webhook working",
        "method": request.method,
        "path": request.path
    })

@csrf_exempt
def DailyPriceWebhook(request):
    # POST 以外は弾く
    if request.method != "POST":
        return HttpResponseBadRequest("POST only")

    # 簡易トークン認証（任意だけど付けておくと安心）
    expected = getattr(settings, "WEBHOOK_TOKEN", None)
    token = request.headers.get("X-Webhook-Token")
    if expected and token != expected:
        logger.warning("Webhook token mismatch: got=%s", token)
        return HttpResponseForbidden("invalid token")
    
    raw = request.body.decode("utf-8", errors="replace")
    logger.info("RAW BODY: %r", raw)

    # JSON パース
    try:
        payload = json.loads(request.body.decode("utf-8"))
    except Exception as e:
        logger.error("Failed to parse webhook JSON: %s", e, exc_info=True)
        return HttpResponseBadRequest("invalid json")

    # createdAtからDateTimeオブジェクトを取得
    try:
        created_at_str = payload.get('createdAt')
        if created_at_str:
            created_at = datetime.datetime.fromisoformat(created_at_str.replace('Z', '+00:00'))
            # JSTに変換
            jst_timezone = datetime.timezone(datetime.timedelta(hours=9))
            created_at_jst = created_at.astimezone(jst_timezone)
            target_date = created_at_jst.date()
        else:
            # createdAtがない場合は今日の日付を使用
            target_date = datetime.date.today()
    except Exception as e:
        logger.warning("Failed to parse createdAt, using today's date: %s", e)
        target_date = datetime.date.today()

    logger.info(
        "[Webhook] Daily price update received: eventType=%s, target_date=%s",
        payload.get("eventType"),
        target_date
    )

    try:
        # DataIngestorを使用してAzure Storageから今日のデータを取得・格納
        ingestor = DataIngestor()
        
        # 既存のAzuriteファイル構造に合わせてファイルパスを生成
        # 例: price/2025/11/2025-11-13.txt
        year = target_date.year
        month = target_date.month
        filename = f"price/{year}/{month:02d}/{target_date.strftime('%Y-%m-%d')}.txt"
        
        logger.info(f"Attempting to ingest price file: {filename}")
        
        # Azure Storageからファイルを取得してIngestMarketに格納
        result = ingestor.ingest_price_file(filename, target_date)
        
        if result['success']:
            logger.info(f"Successfully ingested {result['records_created']} price records for {target_date}")
            return JsonResponse({
                "status": "success",
                "message": f"Ingested {result['records_created']} price records",
                "target_date": str(target_date),
                "filename": filename
            })
        else:
            logger.error(f"Failed to ingest price file: {result['error']}")
            return JsonResponse({
                "status": "error",
                "message": result['error'],
                "target_date": str(target_date),
                "filename": filename
            }, status=500)
            
    except Exception as e:
        logger.error(f"Exception during price data ingestion: {str(e)}", exc_info=True)
        return JsonResponse({
            "status": "error",
            "message": f"Internal error: {str(e)})",
            "target_date": str(target_date)
        }, status=500)

@csrf_exempt
def DeadlineWebhook(request):
    # デバッグ用ログ
    logger.info(f"DeadlineWebhook called: method={request.method}, path={request.path}")
    
    # POST 以外は弾く
    if request.method != "POST":
        logger.warning("DeadlineWebhook: Non-POST request received")
        return HttpResponseBadRequest("POST only")

    # 簡易トークン認証（任意だけど付けておくと安心）
    expected = getattr(settings, "WEBHOOK_TOKEN", None)
    token = request.headers.get("X-Webhook-Token")
    logger.info(f"DeadlineWebhook: expected_token={expected}, received_token={token}")
    
    if expected and token != expected:
        logger.warning("Webhook token mismatch: got=%s", token)
        return HttpResponseForbidden("invalid token")
    
    raw = request.body.decode("utf-8", errors="replace")
    logger.info("DeadlineWebhook RAW BODY: %r", raw)

    # JSON パース
    try:
        payload = json.loads(request.body.decode("utf-8"))
    except Exception as e:
        logger.error("Failed to parse webhook JSON: %s", e, exc_info=True)
        return HttpResponseBadRequest("invalid json")

    # createdAtからDateTimeオブジェクトを取得
    try:
        created_at_str = payload.get('createdAt')
        if created_at_str:
            created_at = datetime.datetime.fromisoformat(created_at_str.replace('Z', '+00:00'))
            # JSTに変換
            jst_timezone = datetime.timezone(datetime.timedelta(hours=9))
            created_at_jst = created_at.astimezone(jst_timezone)
            target_date = created_at_jst.date()
        else:
            # createdAtがない場合は今日の日付を使用
            target_date = datetime.date.today()
    except Exception as e:
        logger.warning("Failed to parse createdAt, using today's date: %s", e)
        target_date = datetime.date.today()

    logger.info(
        "[Webhook] deadline.weather.update received: eventType=%s, target_date=%s",
        payload.get("eventType"),
        target_date
    )

    try:
        # DataIngestorを使用してAzure Storageから気象データを取得・格納
        ingestor = DataIngestor()
        
        year = target_date.year
        month = target_date.month
        day = target_date.day
        
        # 受信日に基づいて処理対象を決定
        if day == 1:
            # 1日受信: 前月のlast.csvを処理（前月16日-月末の15日分）
            if month == 1:
                prev_year = year - 1
                prev_month = 12
            else:
                prev_year = year
                prev_month = month - 1
            
            filename = f"weather/{prev_year}/{prev_month:02d}/{prev_year}_{prev_month:02d}_last.csv"
            half = "後半"
            # 前月の日付を基準として使用
            reference_date = datetime.date(prev_year, prev_month, 16)
            
            logger.info(f"Processing previous month's last half: {prev_year}/{prev_month} (16日-月末)")
            
        elif day == 16:
            # 16日受信: 当月のmid.csvを処理（当月1-15日の15日分）
            filename = f"weather/{year}/{month:02d}/{year}_{month:02d}_mid.csv"
            half = "前半"
            reference_date = datetime.date(year, month, 1)
            
            logger.info(f"Processing current month's first half: {year}/{month} (1-15日)")
            
        else:
            # 1日と16日以外は処理しない
            logger.warning(f"Webhook received on unexpected day: {day}. Expected day 1 or 16.")
            return JsonResponse({
                "status": "warning",
                "message": f"Webhook received on unexpected day: {day}. No processing performed.",
                "target_date": str(target_date)
            })
        
        # ファイル処理
        logger.info(f"Attempting to ingest weather file: {filename}")
        result = ingestor.ingest_weather_file(filename, reference_date, half=half)
        
        if result['success']:
            logger.info(f"Successfully ingested {result['records_created']} weather records from {filename}")
            
            # 気象データ処理完了後、データ整合性確認とモデル実行を開始
            logger.info("Weather data ingestion completed. Starting data validation and model execution...")
            
            try:
                # Step 1: 前日のIngestMarketデータ存在確認
                jst_timezone = datetime.timezone(datetime.timedelta(hours=9))
                yesterday_jst = (datetime.datetime.now(jst_timezone) - datetime.timedelta(days=1)).date()
                
                market_exists = IngestMarket.objects.filter(target_date=yesterday_jst).exists()
                logger.info(f"IngestMarket data exists for {yesterday_jst}: {market_exists}")
                
                if not market_exists:
                    logger.warning(f"IngestMarket data for {yesterday_jst} not found. Proceeding anyway...")
                
                # Step 2: IngestWeatherデータ存在確認（処理した期間）
                weather_start_date = _calculate_period_start_date(reference_date.year, reference_date.month, half)
                weather_end_date = _calculate_period_end_date(reference_date.year, reference_date.month, half)
                
                weather_count = IngestWeather.objects.filter(
                    target_date__gte=weather_start_date,
                    target_date__lte=weather_end_date
                ).count()
                
                logger.info(f"IngestWeather data count for {weather_start_date} to {weather_end_date}: {weather_count}")
                
                if weather_count == 0:
                    logger.warning("No IngestWeather data found for the processed period. Proceeding anyway...")
                
                # Step 3: モデル実行処理を開始
                logger.info("Starting model execution process...")
                model_execution_result = execute_model_processing(target_date, logger)
                
                return JsonResponse({
                    "status": "success",
                    "message": f"Ingested {result['records_created']} weather records and completed model execution",
                    "target_date": str(target_date),
                    "processed_file": filename,
                    "period": half,
                    "records_created": result['records_created'],
                    "market_data_yesterday": market_exists,
                    "weather_data_count": weather_count,
                    "model_execution": model_execution_result
                })
                
            except Exception as model_error:
                logger.error(f"Error during model execution: {str(model_error)}", exc_info=True)
                return JsonResponse({
                    "status": "partial_success",
                    "message": f"Weather data ingested successfully, but model execution failed",
                    "target_date": str(target_date),
                    "processed_file": filename,
                    "records_created": result['records_created'],
                    "model_error": str(model_error)
                })
        else:
            logger.error(f"Failed to ingest weather file {filename}: {result['error']}")
            return JsonResponse({
                "status": "error",
                "message": result['error'],
                "target_date": str(target_date),
                "filename": filename
            }, status=500)
            
    except Exception as e:
        logger.error(f"Exception during weather data ingestion: {str(e)}", exc_info=True)
        return JsonResponse({
            "status": "error",
            "message": f"Internal error: {str(e)}",
            "target_date": str(target_date)
        }, status=500)

@require_POST
def run_model_by_webhook(request):
    """モデル実行ビュー（POSTのみ）
    受信した通知を受けて、ComputeMarketとComputeWeatherを生成してから、
    対象月の既存 FeatureSet を取得し、is_active=True の model_version に紐づく
    出力値（既存レコード）を更新し、併せて ObserveReport の該当レコードを更新する。
    """
    logger = logging.getLogger(__name__)

    try:
        updated_year = int(request.POST.get('year', 0))
        updated_month = int(request.POST.get('month', 0))
        update_day = int(request.POST.get('day', 1))  # デフォルトは1日
    except (ValueError, TypeError):
        messages.error(request, '更新対象の年/月/日が不正です')
        return redirect('feedback:index')

    if not (1 <= updated_month <= 12 and updated_year > 0):
        messages.error(request, '更新対象の年/月が不正です')
        return redirect('feedback:index')
    
    # 日付に基づいて前半/後半を決定
    if update_day <= 15:
        updated_half = "前半"
    else:
        updated_half = "後半"
    
    logger.info(f"Webhook受信: {updated_year}年{updated_month}月{updated_half} (日付: {update_day})")
    
    try:
        # Step 1: 集計期間を計算（受信日の直前の半期のみ）
        # 例: 2025年5月1日受信 → 2025年4月16日〜4月30日を集計対象
        # 例: 2025年5月16日受信 → 2025年5月1日〜5月15日を集計対象
        if updated_half == "前半":
            # 前半（1日）受信時は前月後半を集計
            if updated_month == 1:
                aggregation_start_year = updated_year - 1
                aggregation_start_month = 12
            else:
                aggregation_start_year = updated_year
                aggregation_start_month = updated_month - 1
            aggregation_start_half = "後半"
            aggregation_end_year = aggregation_start_year
            aggregation_end_month = aggregation_start_month
            aggregation_end_half = "後半"
        else:
            # 後半（16日）受信時は当月前半を集計
            aggregation_start_year = updated_year
            aggregation_start_month = updated_month
            aggregation_start_half = "前半"
            aggregation_end_year = updated_year
            aggregation_end_month = updated_month
            aggregation_end_half = "前半"
        
        logger.info(f"集計期間: {aggregation_start_year}年{aggregation_start_month}月{aggregation_start_half} 〜 {aggregation_end_year}年{aggregation_end_month}月{aggregation_end_half}")
        
        # ComputeMarketとComputeWeatherデータを期間限定で集計・生成
        compute_results = compute_data_for_aggregation_period(
            aggregation_start_year, aggregation_start_month, aggregation_start_half,
            aggregation_end_year, aggregation_end_month, aggregation_end_half, logger)
        market_result = compute_results['market']
        weather_result = compute_results['weather']
        
        # Step 2: 予測モデルのrunnerを実行（受信月以降のみを対象）
        # 集計期間内（例: 2025年4月後半まで）の予測は除外し、
        # 受信月以降（例: 2025年5月以降）の予測のみを更新
        logger.info(f"予測モデル更新開始: {updated_year}年{updated_month}月{updated_half}以降を対象")
        runner = ForecastOLSRunner(config=ForecastOLSConfig(region_name='広島'))
        
        # 予測開始期間を受信月・前後半に設定
        # これにより集計期間（〜2025年4月後半）は除外され、
        # 2025年5月以降の予測のみが更新される
        updated_count = runner.update_predictions_for_period(
            updated_year=updated_year,
            updated_month=updated_month,
            updated_half=updated_half,
            variable_ids=None,  # 全変数を対象
            create_if_missing=True,
            look_ahead_years=1,  # 1年先まで予測
            logger=logger
        )
        
        logger.info(f"予測モデル更新完了: {updated_count} 件更新")
        
        # 成功メッセージ（詳細な期間情報を含む）
        compute_summary = f"Market: {market_result.created}作成/{market_result.updated}更新, Weather: {weather_result.created}作成/{weather_result.updated}更新"
        messages.success(request, 
            f'Webhook処理完了: {updated_year}年{updated_month}月{updated_half}\n'
            f'集計期間: {aggregation_start_year}/{aggregation_start_month}{aggregation_start_half} 〜 {aggregation_end_year}/{aggregation_end_month}{aggregation_end_half}\n'
            f'Compute集計: {compute_summary}\n'
            f'予測更新: {updated_count} 件（{updated_year}年{updated_month}月{updated_half}以降）')
        
    except Exception as e:
        logger.error(f"Webhook処理中にエラーが発生: {str(e)}", exc_info=True)
        messages.error(request, f'処理中にエラーが発生しました: {str(e)}')
    
    return redirect('feedback:index')


def execute_model_processing(target_date: datetime.date, logger: logging.Logger) -> dict:
    """
    モデル実行処理を実行する（run_model_by_webhookの処理を流用）
    
    Args:
        target_date: Webhook受信日
        logger: ロガー
        
    Returns:
        dict: 実行結果
    """
    try:
        updated_year = target_date.year
        updated_month = target_date.month
        update_day = target_date.day
        
        # 日付に基づいて前半/後半を決定
        if update_day <= 15:
            updated_half = "前半"
        else:
            updated_half = "後半"
        
        logger.info(f"Model execution: {updated_year}年{updated_month}月{updated_half} (日付: {update_day})")
        
        # Step 1: 集計期間を計算（受信日の直前の半期のみ）
        if updated_half == "前半":
            # 前半（1日）受信時は前月後半を集計
            if updated_month == 1:
                aggregation_start_year = updated_year - 1
                aggregation_start_month = 12
            else:
                aggregation_start_year = updated_year
                aggregation_start_month = updated_month - 1
            aggregation_start_half = "後半"
            aggregation_end_year = aggregation_start_year
            aggregation_end_month = aggregation_start_month
            aggregation_end_half = "後半"
        else:
            # 後半（16日）受信時は当月前半を集計
            aggregation_start_year = updated_year
            aggregation_start_month = updated_month
            aggregation_start_half = "前半"
            aggregation_end_year = updated_year
            aggregation_end_month = updated_month
            aggregation_end_half = "前半"
        
        logger.info(f"集計期間: {aggregation_start_year}年{aggregation_start_month}月{aggregation_start_half} 〜 {aggregation_end_year}年{aggregation_end_month}月{aggregation_end_half}")
        
        # ComputeMarketとComputeWeatherデータを期間限定で集計・生成
        compute_results = compute_data_for_aggregation_period(
            aggregation_start_year, aggregation_start_month, aggregation_start_half,
            aggregation_end_year, aggregation_end_month, aggregation_end_half, logger)
        market_result = compute_results['market']
        weather_result = compute_results['weather']
        
        # Step 2: 予測モデルのrunnerを実行
        logger.info(f"予測モデル更新開始: {updated_year}年{updated_month}月{updated_half}以降を対象")
        runner = ForecastOLSRunner(config=ForecastOLSConfig(region_name='広島'))
        
        updated_count = runner.update_predictions_for_period(
            updated_year=updated_year,
            updated_month=updated_month,
            updated_half=updated_half,
            variable_ids=None,  # 全変数を対象
            create_if_missing=True,
            look_ahead_years=1,  # 1年先まで予測
            logger=logger
        )
        
        logger.info(f"予測モデル更新完了: {updated_count} 件更新")
        
        return {
            "success": True,
            "market_created": market_result.created,
            "market_updated": market_result.updated,
            "weather_created": weather_result.created,
            "weather_updated": weather_result.updated,
            "predictions_updated": updated_count,
            "aggregation_period": f"{aggregation_start_year}/{aggregation_start_month}{aggregation_start_half} 〜 {aggregation_end_year}/{aggregation_end_month}{aggregation_end_half}",
            "prediction_period": f"{updated_year}年{updated_month}月{updated_half}以降"
        }
        
    except Exception as e:
        logger.error(f"モデル実行処理中にエラーが発生: {str(e)}", exc_info=True)
        return {
            "success": False,
            "error": str(e)
        }


def compute_data_for_aggregation_period(
    start_year: int, start_month: int, start_half: str,
    end_year: int, end_month: int, end_half: str,
    logger: Optional[logging.Logger] = None):
    """
    指定された期間範囲のIngestデータを集計してComputeMarket/ComputeWeatherデータを生成する
    
    Args:
        start_year, start_month, start_half: 集計開始期間
        end_year, end_month, end_half: 集計終了期間
        logger: ロガー（オプション）
        
    Returns:
        dict: 集計結果 {'market': AggregationResult, 'weather': AggregationResult}
        
    Example:
        # 2024年4月後半〜2025年4月前半のデータを集計
        compute_data_for_aggregation_period(2024, 4, "後半", 2025, 4, "前半")
    """
    if logger is None:
        logger = logging.getLogger(__name__)
    
    # 集計期間の開始日・終了日を計算
    start_date = _calculate_period_start_date(start_year, start_month, start_half)
    end_date = _calculate_period_end_date(end_year, end_month, end_half)
    
    logger.info(f"期間限定集計: {start_date} 〜 {end_date}")
    
    try:
        # 期間内のIngestMarketデータを取得して集計
        market_queryset = IngestMarket.objects.filter(
            target_date__gte=start_date,
            target_date__lte=end_date
        )
        logger.info(f"IngestMarket対象レコード数: {market_queryset.count()}")
        
        market_result = aggregate_market_data_filtered(market_queryset, logger)
        logger.info(f"Market集計完了: 作成={market_result.created}, 更新={market_result.updated}")
        
    except Exception as e:
        logger.error(f"Market集計エラー: {str(e)}", exc_info=True)
        raise
    
    try:
        # 期間内のIngestWeatherデータを取得して集計
        weather_queryset = IngestWeather.objects.filter(
            target_date__gte=start_date,
            target_date__lte=end_date
        )
        logger.info(f"IngestWeather対象レコード数: {weather_queryset.count()}")
        
        weather_result = aggregate_weather_data_filtered(weather_queryset, logger)
        logger.info(f"Weather集計完了: 作成={weather_result.created}, 更新={weather_result.updated}")
        
    except Exception as e:
        logger.error(f"Weather集計エラー: {str(e)}", exc_info=True)
        raise
    
    return {
        'market': market_result,
        'weather': weather_result
    }


def _calculate_period_start_date(year: int, month: int, half: str) -> datetime.date:
    """期間の開始日を計算"""
    if half == "前半":
        return datetime.date(year, month, 1)
    else:  # "後半"
        return datetime.date(year, month, 16)


def _calculate_period_end_date(year: int, month: int, half: str) -> datetime.date:
    """期間の終了日を計算"""
    if half == "前半":
        return datetime.date(year, month, 15)
    else:  # "後半"
        # 月末日を取得
        if month == 12:
            next_month = datetime.date(year + 1, 1, 1)
        else:
            next_month = datetime.date(year, month + 1, 1)
        return next_month - datetime.timedelta(days=1)


@transaction.atomic
def aggregate_market_data_filtered(queryset, logger: Optional[logging.Logger] = None):
    """期間限定のIngestMarketデータからComputeMarketを集計生成"""
    from compute.service import AggregationResult
    
    if logger is None:
        logger = logging.getLogger(__name__)
    
    # IngestMarketデータをグループ化
    grouped_records = _group_market_records(queryset)
    logger.info(f"Market グループ数: {len(grouped_records)}")
    
    created_count = 0
    updated_count = 0
    
    for group_key, records in grouped_records.items():
        # グループキー: (vegetable_id, region_id, target_half, target_year, target_month)
        # compute.serviceの_group_market_recordsの実装に合わせる
        vegetable_id, region_id, target_half, target_year, target_month = group_key
        
        # グループデータを集計
        aggregated_data = _aggregate_market_group(records)
        
        # ComputeMarketレコードの作成または更新
        compute_market, created = ComputeMarket.objects.update_or_create(
            vegetable_id=vegetable_id,
            region_id=region_id,
            target_year=target_year,
            target_month=target_month,
            target_half=target_half,
            defaults=aggregated_data
        )
        
        if created:
            created_count += 1
        else:
            updated_count += 1
    
    return AggregationResult(created=created_count, updated=updated_count)


@transaction.atomic 
def aggregate_weather_data_filtered(queryset, logger: Optional[logging.Logger] = None):
    """期間限定のIngestWeatherデータからComputeWeatherを集計生成"""
    from compute.service import AggregationResult
    
    if logger is None:
        logger = logging.getLogger(__name__)
    
    # IngestWeatherデータをグループ化
    grouped_records = _group_weather_records(queryset)
    logger.info(f"Weather グループ数: {len(grouped_records)}")
    
    created_count = 0
    updated_count = 0
    
    for group_key, records in grouped_records.items():
        # グループキー: (region_id, target_half, target_year, target_month)
        region_id, target_half, target_year, target_month = group_key
        
        # グループデータを集計
        aggregated_data = _aggregate_weather_group(records)
        
        # ComputeWeatherレコードの作成または更新
        compute_weather, created = ComputeWeather.objects.update_or_create(
            region_id=region_id,
            target_half=target_half, 
            target_year=target_year,
            target_month=target_month,
            defaults=aggregated_data
        )
        
        if created:
            created_count += 1
        else:
            updated_count += 1
    
    return AggregationResult(created=created_count, updated=updated_count)