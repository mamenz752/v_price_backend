from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
import logging
from typing import Dict, Iterable, List, Optional, Tuple

from django.db import transaction

from ingest.models import IngestMarket, IngestWeather
from .models import ComputeMarket, ComputeWeather

# ロガーの設定
logger = logging.getLogger(__name__)

HALF_FIRST = "前半"
HALF_SECOND = "後半"


@dataclass
class AggregationResult:
    created: int = 0
    updated: int = 0


def _target_half(day: int) -> str:
    """Return target half label for a given day-of-month."""
    return HALF_FIRST if day <= 15 else HALF_SECOND


def _mean(values: Iterable[Optional[float]]) -> Optional[float]:
    """Return arithmetic mean ignoring None values."""
    cleaned: List[float] = [float(value) for value in values if value is not None]
    if not cleaned:
        return None
    return sum(cleaned) / len(cleaned)


def _select_trend(values: Iterable[Optional[str]]) -> Optional[str]:
    """
    Pick the most frequent trend label among the records.
    Falls back to the first non-empty label if all frequencies tie.
    """
    cleaned = [value for value in values if value]
    if not cleaned:
        return None
    counter = Counter(cleaned)
    most_common = counter.most_common()
    if not most_common:
        return None
    top_count = most_common[0][1]
    top_candidates = [label for label, count in most_common if count == top_count]
    if len(top_candidates) == 1:
        return top_candidates[0]
    # Preserve original order when there is a tie.
    for label in cleaned:
        if label in top_candidates:
            return label
    return cleaned[0]


def _aggregate_market_group(records: Iterable[IngestMarket]) -> Dict[str, Optional[float]]:
    records = list(records)
    logger.debug(f"市場データグループの集計: レコード数={len(records)}")
    
    average_price = _mean(record.average_price for record in records)
    source_price = _mean(record.source_price for record in records)
    volume = _mean(record.volume for record in records)
    trend = _select_trend(record.trend for record in records)
    
    logger.debug(f"市場データ集計結果: 平均価格={average_price}, 元価格={source_price}, 数量={volume}, トレンド={trend}")
    
    return {
        "average_price": average_price,
        "source_price": source_price,
        "volume": volume,
        "trend": trend,
    }


def _aggregate_weather_group(records: Iterable[IngestWeather]) -> Dict[str, Optional[float]]:
    records = list(records)
    logger.debug(f"気象データグループの集計: レコード数={len(records)}")
    
    max_temp = _mean(record.max_temp for record in records)
    mean_temp = _mean(record.mean_temp for record in records)
    min_temp = _mean(record.min_temp for record in records)
    sum_precipitation = _mean(record.sum_precipitation for record in records)
    sunshine_duration = _mean(record.sunshine_duration for record in records)
    ave_humidity = _mean(record.ave_humidity for record in records)
    
    logger.debug(f"気象データ集計結果: 最高気温={max_temp}, 平均気温={mean_temp}, 最低気温={min_temp}, " 
                f"降水量={sum_precipitation}, 日照時間={sunshine_duration}, 平均湿度={ave_humidity}")
    
    return {
        "max_temp": max_temp,
        "mean_temp": mean_temp,
        "min_temp": min_temp,
        "sum_precipitation": sum_precipitation,
        "sunshine_duration": sunshine_duration,
        "ave_humidity": ave_humidity,
    }


def _group_market_records(
    queryset: Iterable[IngestMarket],
) -> Dict[Tuple[int, int, str, int, Optional[int]], List[IngestMarket]]:
    """
    Group market records by (vegetable_id, region_id, target_half, year, month).
    Region can be null, so we use its id or None as part of the key.
    """
    logger.info("市場価格データのグループ化処理を開始します")
    buckets: Dict[Tuple[int, int, str, int, Optional[int]], List[IngestMarket]] = defaultdict(list)
    record_count = 0
    skipped_count = 0
    
    for record in queryset:
        record_count += 1
        if record.price is None or record.volume is None:
            skipped_count += 1
            logger.debug(f"無効な市場データをスキップ: ID={record.id}, 野菜ID={record.vegetable_id}, 地域ID={record.region_id}")
            continue
            
        target = record.target_date
        key = (
            record.vegetable_id,
            record.region_id,
            _target_half(target.day),
            target.year,
            target.month,
        )
        buckets[key].append(record)
        
    logger.info(f"市場価格データのグループ化完了: 処理レコード数={record_count}, スキップ={skipped_count}, グループ数={len(buckets)}")
    return buckets


def _group_weather_records(
    queryset: Iterable[IngestWeather],
) -> Dict[Tuple[int, str, int, int], List[IngestWeather]]:
    """
    Group weather records by (region_id, target_half, year, month).
    """
    logger.info("気象データのグループ化処理を開始します")
    buckets: Dict[Tuple[int, str, int, int], List[IngestWeather]] = defaultdict(list)
    record_count = 0
    
    for record in queryset:
        record_count += 1
        target = record.target_date
        key = (
            record.region_id,
            _target_half(target.day),
            target.year,
            target.month,
        )
        buckets[key].append(record)
        
    logger.info(f"気象データのグループ化完了: 処理レコード数={record_count}, グループ数={len(buckets)}")
    return buckets


@transaction.atomic
def aggregate_market_data() -> AggregationResult:
    """
    Aggregate ingest market data by half-month and upsert ComputeMarket rows.
    """
    logger.info("市場価格データの集計処理を開始します")
    groups = _group_market_records(IngestMarket.objects.all())
    logger.info(f"市場価格データのグループ数: {len(groups)}")
    result = AggregationResult()

    for (vegetable_id, region_id, target_half, target_year, target_month), records in groups.items():
        logger.info(f"市場価格データ集計: 野菜ID={vegetable_id}, 地域ID={region_id}, 年={target_year}, 月={target_month}, 半期={target_half}, レコード数={len(records)}")
        aggregated = _aggregate_market_group(records)
        defaults = {
            "target_year": target_year,
            "target_month": target_month,
            "target_half": target_half,
            **aggregated,
        }
        _, created = ComputeMarket.objects.update_or_create(
            vegetable_id=vegetable_id,
            region_id=region_id,
            target_year=target_year,
            target_month=target_month,
            target_half=target_half,
            defaults=defaults,
        )
        if created:
            result.created += 1
            logger.info(f"市場価格データ新規作成: 野菜ID={vegetable_id}, 地域ID={region_id}, 年={target_year}, 月={target_month}, 半期={target_half}")
        else:
            result.updated += 1
            logger.info(f"市場価格データ更新: 野菜ID={vegetable_id}, 地域ID={region_id}, 年={target_year}, 月={target_month}, 半期={target_half}")

    logger.info(f"市場価格データの集計処理が完了しました。新規作成: {result.created}, 更新: {result.updated}")
    return result


@transaction.atomic
def aggregate_weather_data() -> AggregationResult:
    """
    Aggregate ingest weather data by half-month and upsert ComputeWeather rows.
    """
    logger.info("気象データの集計処理を開始します")
    groups = _group_weather_records(IngestWeather.objects.all())
    logger.info(f"気象データのグループ数: {len(groups)}")
    result = AggregationResult()

    for (region_id, target_half, target_year, target_month), records in groups.items():
        logger.info(f"気象データ集計: 地域ID={region_id}, 年={target_year}, 月={target_month}, 半期={target_half}, レコード数={len(records)}")
        aggregated = _aggregate_weather_group(records)
        defaults = {
            "target_year": target_year,
            "target_month": target_month,
            "target_half": target_half,
            **aggregated,
        }
        _, created = ComputeWeather.objects.update_or_create(
            region_id=region_id,
            target_year=target_year,
            target_month=target_month,
            target_half=target_half,
            defaults=defaults,
        )
        if created:
            result.created += 1
            logger.info(f"気象データ新規作成: 地域ID={region_id}, 年={target_year}, 月={target_month}, 半期={target_half}")
        else:
            result.updated += 1
            logger.info(f"気象データ更新: 地域ID={region_id}, 年={target_year}, 月={target_month}, 半期={target_half}")

    logger.info(f"気象データの集計処理が完了しました。新規作成: {result.created}, 更新: {result.updated}")
    return result


@transaction.atomic
@transaction.atomic
def aggregate_all_data() -> Dict[str, AggregationResult]:
    """
    Run both market and weather aggregation in a single transaction.
    """
    logger.info("すべてのデータの集計処理を開始します")
    market_result = aggregate_market_data()
    weather_result = aggregate_weather_data()
    
    logger.info(f"すべてのデータの集計処理が完了しました。市場データ(新規:{market_result.created}, 更新:{market_result.updated}), "
                f"気象データ(新規:{weather_result.created}, 更新:{weather_result.updated})")
    
    return {
        "market": market_result,
        "weather": weather_result,
    }


@transaction.atomic
def reset_compute_data() -> Dict[str, int]:
    """
    Remove all rows from ComputeMarket and ComputeWeather.
    """
    logger.info("集計データのリセット処理を開始します")
    market_deleted = ComputeMarket.objects.all().delete()[0]
    logger.info(f"市場価格の集計データを削除しました: {market_deleted}件")
    
    weather_deleted = ComputeWeather.objects.all().delete()[0]
    logger.info(f"気象データの集計データを削除しました: {weather_deleted}件")
    
    logger.info(f"集計データのリセット処理が完了しました。合計削除件数: {market_deleted + weather_deleted}件")
    return {
        "market_deleted": market_deleted,
        "weather_deleted": weather_deleted,
    }
