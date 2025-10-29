from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, List, Dict
from datetime import datetime
from django.db.models import Q
from django.db import transaction
from forecast.models import (
    ForecastModelVersion,
    ForecastModelCoef,
    ForecastModelVariable,
    ForecastModelKind
)
from compute.models import ComputeWeather, ComputeMarket
from observe.models import ObserveReport
from ingest.models import Region

@dataclass
class ObserveServiceConfig:
    """予測サービスの設定"""
    region_name: str = '広島'
    confidence_interval: float = 0.95  # 信頼区間

class ObserveService:
    """予測を実行し、結果を保存するサービス"""
    def __init__(self, config: Optional[ObserveServiceConfig] = None):
        self.cfg = config or ObserveServiceConfig()
        self._region = Region.objects.get(name=self.cfg.region_name)

    def _get_target_period(self, year: int, month: int, half: str) -> Dict:
        """指定された年月から予測対象期間を計算する"""
        periods = []
        current_year = year
        current_month = month
        current_half = half

        # 3期前までのデータを収集
        for _ in range(4):  # 現在の期も含めて4期分
            periods.append({
                'year': current_year,
                'month': current_month,
                'half': current_half
            })

            # 前の期に移動
            if current_half == '後半':
                current_half = '前半'
            else:
                current_half = '後半'
                current_month -= 1
                if current_month < 1:
                    current_month = 12
                    current_year -= 1

        return periods

    def _get_weather_data(self, periods: List[Dict]) -> Dict:
        """気象データを取得する"""
        weather_data = {}
        for period in periods:
            try:
                weather = ComputeWeather.objects.get(
                    region=self._region,
                    target_year=period['year'],
                    target_month=period['month'],
                    target_half=period['half']
                )
                key = f"{period['year']}_{period['month']}_{period['half']}"
                weather_data[key] = {
                    'max_temp': weather.max_temp,
                    'mean_temp': weather.mean_temp,
                    'min_temp': weather.min_temp,
                    'sum_precipitation': weather.sum_precipitation,
                    'sunshine_duration': weather.sunshine_duration,
                    'ave_humidity': weather.ave_humidity
                }
            except ComputeWeather.DoesNotExist:
                continue
        return weather_data

    def _get_market_data(self, periods: List[Dict], vegetable_id: int) -> Dict:
        """市場データを取得する"""
        market_data = {}
        for period in periods:
            try:
                market = ComputeMarket.objects.get(
                    region=self._region,
                    vegetable_id=vegetable_id,
                    target_year=period['year'],
                    target_month=period['month'],
                    target_half=period['half']
                )
                key = f"{period['year']}_{period['month']}_{period['half']}"
                market_data[key] = {
                    'average_price': market.average_price,
                    'volume': market.volume
                }
            except ComputeMarket.DoesNotExist:
                continue
        return market_data

    def predict_for_model_version(self, model_version: ForecastModelVersion, year: int, month: int, half: str) -> None:
        """特定のモデルバージョンに基づいて予測を実行し、結果を保存する"""
        # モデルの係数を取得
        coefs = ForecastModelCoef.objects.filter(
            variable__forecast_model_coefs__id__in=ForecastModelCoef.objects.filter(
                variable__forecast_model_feature_sets__model_kind=model_version.model_kind,
                variable__forecast_model_feature_sets__target_month=model_version.target_month
            ).values_list('id', flat=True)
        ).select_related('variable')

        # 予測対象期間のデータを取得
        periods = self._get_target_period(year, month, half)
        weather_data = self._get_weather_data(periods)
        market_data = self._get_market_data(periods, model_version.model_kind.vegetable.id)

        # 予測値を計算
        prediction = 0.0
        const_value = 0.0

        for coef in coefs:
            if coef.variable.name == 'const':
                const_value = coef.coef
                continue

            # 変数の時期を特定
            term = coef.variable.previous_term
            if term >= len(periods):
                continue

            target_period = periods[term]
            period_key = f"{target_period['year']}_{target_period['month']}_{target_period['half']}"

            # 変数の値を取得
            var_value = None
            if coef.variable.name in ['max_temp', 'mean_temp', 'min_temp', 'sum_precipitation', 'sunshine_duration', 'ave_humidity']:
                if period_key in weather_data:
                    var_value = weather_data[period_key].get(coef.variable.name)
            elif coef.variable.name in ['average_price', 'volume']:
                if period_key in market_data:
                    var_value = market_data[period_key].get(coef.variable.name)

            if var_value is not None:
                prediction += coef.coef * var_value

        # 定数項を加算
        prediction += const_value

        # 信頼区間を計算（簡易的な実装）
        # FIXME: RMSEを参照
        margin = prediction * (1 - self.cfg.confidence_interval)
        min_price = prediction - margin
        max_price = prediction + margin

        # 予測結果を保存
        with transaction.atomic():
            ObserveReport.objects.create(
                target_year=year,
                target_month=month,
                target_half=half,
                predict_price=prediction,
                min_price=min_price,
                max_price=max_price,
                model_version=model_version
            )

    def observe_latest_model(self, model_kind_id: int, target_year: int, target_month: int, target_half: str) -> Optional[ObserveReport]:
        """最新のモデルバージョンで予測を実行"""
        try:
            latest_version = ForecastModelVersion.objects.filter(
                model_kind_id=model_kind_id,
                target_month=target_month,
                is_active=True
            ).latest('created_at')

            self.predict_for_model_version(latest_version, target_year, target_month, target_half)
            
            return ObserveReport.objects.filter(
                model_version=latest_version,
                target_year=target_year,
                target_month=target_month,
                target_half=target_half
            ).latest('created_at')

        except (ForecastModelVersion.DoesNotExist, ObserveReport.DoesNotExist):
            return None
