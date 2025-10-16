from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple

from django.db import transaction

from ingest.models import IngestMarket, IngestWeather
from .models import ComputeMarket, ComputeWeather

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
    return {
        "average_price": _mean(record.average_price for record in records),
        "source_price": _mean(record.source_price for record in records),
        "volume": _mean(record.volume for record in records),
        "trend": _select_trend(record.trend for record in records),
    }


def _aggregate_weather_group(records: Iterable[IngestWeather]) -> Dict[str, Optional[float]]:
    records = list(records)
    return {
        "max_temp": _mean(record.max_temp for record in records),
        "mean_temp": _mean(record.mean_temp for record in records),
        "min_temp": _mean(record.min_temp for record in records),
        "sum_precipitation": _mean(record.sum_precipitation for record in records),
        "sunshine_duration": _mean(record.sunshine_duration for record in records),
        "ave_humidity": _mean(record.ave_humidity for record in records),
    }


def _group_market_records(
    queryset: Iterable[IngestMarket],
) -> Dict[Tuple[int, int, str, int, Optional[int]], List[IngestMarket]]:
    """
    Group market records by (vegetable_id, region_id, target_half, year, month).
    Region can be null, so we use its id or None as part of the key.
    """
    buckets: Dict[Tuple[int, int, str, int, Optional[int]], List[IngestMarket]] = defaultdict(list)
    for record in queryset:
        target = record.target_date
        key = (
            record.vegetable_id,
            record.region_id,
            _target_half(target.day),
            target.year,
            target.month,
        )
        buckets[key].append(record)
    return buckets


def _group_weather_records(
    queryset: Iterable[IngestWeather],
) -> Dict[Tuple[int, str, int, int], List[IngestWeather]]:
    """
    Group weather records by (region_id, target_half, year, month).
    """
    buckets: Dict[Tuple[int, str, int, int], List[IngestWeather]] = defaultdict(list)
    for record in queryset:
        target = record.target_date
        key = (
            record.region_id,
            _target_half(target.day),
            target.year,
            target.month,
        )
        buckets[key].append(record)
    return buckets


@transaction.atomic
def aggregate_market_data() -> AggregationResult:
    """
    Aggregate ingest market data by half-month and upsert ComputeMarket rows.
    """
    groups = _group_market_records(IngestMarket.objects.all())
    result = AggregationResult()

    for (vegetable_id, region_id, target_half, target_year, target_month), records in groups.items():
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
        else:
            result.updated += 1

    return result


@transaction.atomic
def aggregate_weather_data() -> AggregationResult:
    """
    Aggregate ingest weather data by half-month and upsert ComputeWeather rows.
    """
    groups = _group_weather_records(IngestWeather.objects.all())
    result = AggregationResult()

    for (region_id, target_half, target_year, target_month), records in groups.items():
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
        else:
            result.updated += 1

    return result


@transaction.atomic
def aggregate_all_data() -> Dict[str, AggregationResult]:
    """
    Run both market and weather aggregation in a single transaction.
    """
    market_result = aggregate_market_data()
    weather_result = aggregate_weather_data()
    return {
        "market": market_result,
        "weather": weather_result,
    }


@transaction.atomic
def reset_compute_data() -> Dict[str, int]:
    """
    Remove all rows from ComputeMarket and ComputeWeather.
    """
    market_deleted = ComputeMarket.objects.all().delete()[0]
    weather_deleted = ComputeWeather.objects.all().delete()[0]
    return {
        "market_deleted": market_deleted,
        "weather_deleted": weather_deleted,
    }
