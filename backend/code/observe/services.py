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
from forecast.models import ForecastModelFeatureSet
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

    def _get_target_period(self, year: int, month: int, half: str, max_coef_term: int) -> Dict:
        """指定された年月から予測対象期間を計算する"""
        periods = []
        current_year = year
        current_month = month
        current_half = half

        for _ in range(max_coef_term + 1):
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
        # coefs = ForecastModelCoef.objects.filter(
        #     variable__forecast_model_coefs__id__in=ForecastModelCoef.objects.filter(
        #         variable__forecast_model_feature_sets__model_kind=model_version.model_kind,
        #         variable__forecast_model_feature_sets__target_month=model_version.target_month,
        #         model_version=model_version
        #     ).values_list('id', flat=True)
        # ).select_related('variable')

        import logging
        logger = logging.getLogger(__name__)
        logger.info("predict_for_model_version: model_version id=%s, target_month=%s", getattr(model_version, 'id', None), getattr(model_version, 'target_month', None))

        # FIXME:: なぜか予測値が増えて一定の結果が出ないので直す
        feature_sets = ForecastModelFeatureSet.objects.filter(
            model_kind=model_version.model_kind,
            target_month=model_version.target_month
        ).select_related('variable')

        variable_ids = feature_sets.values_list('variable_id', flat=True)

        coefs = ForecastModelCoef.objects.filter(
            model_version=model_version,
            model_version__is_active=True,
            variable_id__in=variable_ids
        ).select_related('variable')

        import logging
        logger = logging.getLogger(__name__)
        if not coefs.exists():
            logger.info("predict_for_model_version: no coefficients found for model_version id=%s, skipping prediction", getattr(model_version, 'id', None))
            return None
        
        coef_dict = {(coef.variable.name, coef.variable.previous_term): coef 
                 for coef in coefs}

        max_coef_term = max(coef.variable.previous_term for coef in coefs)        
        logger(f"使用する最大の係数期間: {max_coef_term}")

        # 予測対象期間のデータを取得
        periods = self._get_target_period(year, month, half, max_coef_term)
        weather_data = self._get_weather_data(periods)
        # market_data = self._get_market_data(periods, model_version.model_kind.vegetable.id)

        logger.info(f"気象データ: {weather_data}")

        # 予測値を計算
        prediction = 0.0
        const_value = 0.0

        # モデルで定義されている (name, previous_term) の集合を先に取得しておく
        feature_vars = set(ForecastModelFeatureSet.objects.filter(
            model_kind=model_version.model_kind,
            target_month=model_version.target_month
        ).values_list('variable__name', 'variable__previous_term'))

        for (var_name, prev_term), coef in coef_dict.items():
            if var_name == 'const':
                const_value = coef.coef
                continue

            if prev_term >= len(periods):
                continue

            target_period = periods[prev_term]
            period_key = f"{target_period['year']}_{target_period['month']}_{target_period['half']}"
            period_data = weather_data.get(period_key, {})

            var_value = period_data.get(var_name)
            if var_value is not None:
                logger.debug(f"変数計算: {var_name} (期間: {prev_term}) 値: {var_value} * 係数: {coef.coef}")
                prediction += coef.coef * var_value

        # for coef in coefs:
        #     if coef.variable.name == 'const':
        #         const_value = coef.coef
        #         continue

        #     # 変数の時期を特定
        #     term = coef.variable.previous_term
        #     if term >= len(periods):
        #         continue

        #     target_period = periods[term]
        #     period_key = f"{target_period['year']}_{target_period['month']}_{target_period['half']}"
        #     print(f"期間キー: {period_key}")
        #     period_data = weather_data.get(period_key, {})
        #     print("期間データ:", period_data)

        #     # 変数の値を取得（モデルで定義された変数かつ previous_term が一致する場合のみ）
        #     var_value = None
        #     var_key = (coef.variable.name, coef.variable.previous_term)
        #     if var_key in feature_vars:
        #         # period_data は該当期間の weather データ辞書なので、variable 名で存在を確認
        #         if coef.variable.name in period_data:
        #             var_value = period_data.get(coef.variable.name)

        #     if var_value is not None:
        #         print(f"変数 {coef.variable.name} (期間: {term}) の値: {var_value} * 係数: {coef.coef}")
        #         prediction += coef.coef * var_value

        # 定数項を加算
        prediction += const_value

        # モデルのRMSEを取得して信頼区間を計算
        try:
            model_evaluation = model_version.forecastmodelevaluation_set.latest('created_at')
            rmse = model_evaluation.rmse
            min_price = prediction - rmse
            max_price = prediction + rmse
        except:
            # RMSEが取得できない場合は、予測値の±5%をデフォルトとして使用
            margin = prediction * 0.05
            min_price = prediction - margin
            max_price = prediction + margin

        # 予測結果を保存
        try:
            with transaction.atomic():
                report = ObserveReport.objects.create(
                    target_year=year,
                    target_month=month,
                    target_half=half,
                    predict_price=prediction,
                    min_price=min_price,
                    max_price=max_price,
                    model_version=model_version
                )
            logger.info(f"予測結果を保存しました: year={year}, month={month}, half={half}, prediction={prediction}")
            return report
        except Exception as e:
            logger.error(f"予測結果の保存に失敗しました: {str(e)}", exc_info=True)
            return None

    def observe_latest_model(self, model_kind_id: int, target_year: int, target_month: int, target_half: str) -> Optional[ObserveReport]:
        """最新のモデルバージョンで予測を実行"""
        import logging
        logger = logging.getLogger(__name__)
        
        try:
            latest_version = ForecastModelVersion.objects.filter(
                model_kind_id=model_kind_id,
                target_month=target_month,
                is_active=True
            ).latest('created_at')

            logger.info(f"最新のモデルバージョンを取得: id={latest_version.id}")

            # predict_for_model_versionの戻り値を直接使用
            report = self.predict_for_model_version(
                latest_version, target_year, target_month, target_half
            )
            
            if report:
                logger.info(f"予測結果を保存しました: report_id={report.id}")
                return report
            else:
                logger.warning("予測結果が生成されませんでした")
                return None

        except ForecastModelVersion.DoesNotExist:
            logger.error(f"アクティブなモデルが見つかりません: model_kind_id={model_kind_id}, target_month={target_month}")
            return None
        except Exception as e:
            logger.error(f"予測実行中にエラーが発生: {str(e)}", exc_info=True)
            return None