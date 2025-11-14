from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, List, Dict
from datetime import datetime, date
from django.db.models import Q
from django.db import transaction
from django.utils import timezone
import logging
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

    def predict_for_model_version(self, model_version: ForecastModelVersion, year: int, month: int, half: str, force_update: bool = False, allow_past_predictions: bool = False) -> Optional[float]:
        """
        特定のモデルバージョンに基づいて予測を実行し、結果を保存する
        🔥 重要: 予測結果は実行時点より未来の日付でのみ保存される
        """
        # モデルの係数を取得
        # coefs = ForecastModelCoef.objects.filter(
        #     variable__forecast_model_coefs__id__in=ForecastModelCoef.objects.filter(
        #         variable__forecast_model_feature_sets__model_kind=model_version.model_kind,
        #         variable__forecast_model_feature_sets__target_month=model_version.target_month,
        #         model_version=model_version
        #     ).values_list('id', flat=True)
        # ).select_related('variable')

        logger = logging.getLogger(__name__)
        logger.info(
            "[PREDICT] START model_version_id=%s, allow_past=%s, target=%s-%s-%s",
            getattr(model_version, "id", None),
            allow_past_predictions,
            year, month, half,
        )

        try:
            feature_sets = ForecastModelFeatureSet.objects.filter(
                model_kind=model_version.model_kind,
                target_month=model_version.target_month
            ).select_related('variable')
            
            logger.info(
                "[PREDICT] feature_sets count=%s for model_version_id=%s",
                feature_sets.count(),
                getattr(model_version, "id", None),
            )

            variable_ids = feature_sets.values_list('variable_id', flat=True)
            logger.debug("[PREDICT] variable_ids=%s", variable_ids)

            coefs = ForecastModelCoef.objects.filter(
                model_version=model_version,
                model_version__is_active=True,
                variable_id__in=variable_ids
            ).select_related('variable')

            if not coefs.exists():
                logger.info("predict_for_model_version: no coefficients found for model_version id=%s, skipping prediction", getattr(model_version, 'id', None))
                return None
            
            coef_dict = {(coef.variable.name, coef.variable.previous_term): coef 
                    for coef in coefs}

            max_coef_term = max(coef.variable.previous_term for coef in coefs)        
            logger.info(f"使用する最大の係数期間: {max_coef_term}")

            # 予測対象期間のデータを取得
            periods = self._get_target_period(year, month, half, max_coef_term)
            weather_data = self._get_weather_data(periods)
            # market_data = self._get_market_data(periods, model_version.model_kind.vegetable.id)

            logger.info(f"気象データ: {weather_data}")
        except Exception as e:
            logger.error(f"予測実行中にエラーが発生しました: {str(e)}", exc_info=True)
            return None

        # 予測値を計算
        prediction = 0.0
        const_value = 0.0
        used_variables_count = 0

        logger.info(f"🔍 予測計算開始: target={year}-{month} {half}, coefficients_count={len(coef_dict)}")

        # モデルで定義されている (name, previous_term) の集合を先に取得しておく
        feature_vars = set(ForecastModelFeatureSet.objects.filter(
            model_kind=model_version.model_kind,
            target_month=model_version.target_month
        ).values_list('variable__name', 'variable__previous_term'))

        for (var_name, prev_term), coef in coef_dict.items():
            if var_name == 'const':
                const_value = coef.coef
                logger.info(f"🔍 定数項: {const_value}")
                continue

            if prev_term >= len(periods):
                logger.warning(f"🔍 期間不足でスキップ: {var_name}_{prev_term}, periods_length={len(periods)}")
                continue

            target_period = periods[prev_term]
            period_key = f"{target_period['year']}_{target_period['month']}_{target_period['half']}"
            period_data = weather_data.get(period_key, {})
            
            logger.info(f"🔍 変数処理: {var_name}_{prev_term}, period_key={period_key}")
            logger.info(f"🔍 期間データ keys: {list(period_data.keys())}")

            var_value = period_data.get(var_name)
            if var_value is not None:
                contribution = coef.coef * var_value
                prediction += contribution
                used_variables_count += 1
                logger.info(f"🔍 変数適用: {var_name}_{prev_term} = {var_value} * {coef.coef} = {contribution}")
            else:
                logger.warning(f"🔍 変数値なし: {var_name}_{prev_term}, period_key={period_key}")
                logger.warning(f"🔍 利用可能なデータ: {period_data}")

        logger.info(f"🔍 使用変数数: {used_variables_count}/{len(coef_dict)-1}")  # constを除く        # for coef in coefs:
        
        # 定数項を加算
        prediction += const_value
        logger.info(f"🔍 最終予測値: 変数の合計={prediction-const_value} + 定数={const_value} = {prediction}")

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

        # 🔧 未来日付チェック（allow_past_predictions=Falseの場合のみ）
        if not allow_past_predictions:
                current_date = date.today()
                prediction_date = self._calculate_prediction_date(year, month, half)
                
                if prediction_date <= current_date:
                    logger.warning(
                        "Skipping non-future prediction (Webhook mode): prediction_date=%s <= current_date=%s", 
                        prediction_date, current_date
                    )
                    return None
        else:
            logger.info(
                "Past prediction allowed (Feedback mode): target=%s-%s-%s", 
                year, month, half
            )
        
        # 予測結果を保存（未来日付のみ、またはforce_update=Trueの場合）
        try:
                with transaction.atomic():
                    if force_update:
                        # force_update=Trueの場合は既存レコードを確認して更新または新規作成
                        existing_report = ObserveReport.objects.filter(
                            model_version=model_version,
                            target_year=year,
                            target_month=month,
                            target_half=half
                        ).first()
                        
                        if existing_report:
                            # 既存レコードを更新
                            existing_report.predict_price = prediction
                            existing_report.min_price = min_price
                            existing_report.max_price = max_price
                            existing_report.updated_at = timezone.now()
                            existing_report.save()
                            logger.info(
                                "予測結果を更新: year=%d, month=%d, half=%s, prediction=%.3f (report_id=%s)", 
                                year, month, half, prediction, existing_report.id
                            )
                        else:
                            # 新規作成
                            report = ObserveReport.objects.create(
                                target_year=year,
                                target_month=month,
                                target_half=half,
                                predict_price=prediction,
                                min_price=min_price,
                                max_price=max_price,
                                model_version=model_version
                            )
                            logger.info(
                                "予測結果を新規作成: year=%d, month=%d, half=%s, prediction=%.3f (report_id=%s)", 
                                year, month, half, prediction, report.id
                            )
                    else:
                        # 従来の処理（新規作成のみ）
                        report = ObserveReport.objects.create(
                            target_year=year,
                            target_month=month,
                            target_half=half,
                            predict_price=prediction,
                            min_price=min_price,
                            max_price=max_price,
                            model_version=model_version
                        )
                        logger.info(
                            "未来予測結果を保存: year=%d, month=%d, half=%s, prediction=%.3f, prediction_date=%s", 
                            year, month, half, prediction, prediction_date
                        )
                    
                # 🔥 重要: ObserveReportインスタンスではなく予測値（float）を返す
                return float(prediction)
        except Exception as e:
            logger.error(f"予測結果の保存に失敗しました: {str(e)}", exc_info=True)
            return None
        except Exception as e:
            logger.error(f"予測実行中にエラーが発生しました: {str(e)}", exc_info=True)
            return None

    def observe_latest_model(self, model_kind_id: int, target_year: int, target_month: int, target_half: str, allow_past_predictions: bool = False, feedback_mode: bool = False) -> Optional[ObserveReport]:
        """
        最新のモデルバージョンで予測を実行
        
        Args:
            allow_past_predictions: Trueの場合、過去予測も許可（/feedback用）
            feedback_mode: Trueの場合、指定されたtarget_monthのみ予測（/feedback用）
        """
        logger = logging.getLogger(__name__)
        
        # 🔧 feedback_mode時は未来日付チェックを無効化
        if not feedback_mode and not allow_past_predictions:
            current_date = date.today()
            prediction_date = self._calculate_prediction_date(target_year, target_month, target_half)
            
            if prediction_date <= current_date:
                logger.warning(
                    "Skipping non-future prediction in observe_latest_model (Webhook mode): prediction_date=%s <= current_date=%s", 
                    prediction_date, current_date
                )
                return None
        
        try:
            latest_version = ForecastModelVersion.objects.filter(
                model_kind_id=model_kind_id,
                target_month=target_month,  # 🔧 指定されたtarget_monthのモデルのみ
                is_active=True
            ).latest('created_at')

            logger.info(
                "最新モデルバージョンで予測実行: model_id=%s, target=%s-%02d %s, feedback_mode=%s", 
                latest_version.id, target_year, target_month, target_half, feedback_mode
            )

            # 🔧 feedback_mode時はallow_past_predictionsをTrueに設定
            prediction_value = self.predict_for_model_version(
                latest_version, target_year, target_month, target_half, 
                force_update=True, 
                allow_past_predictions=allow_past_predictions or feedback_mode
            )
            
            if prediction_value:
                # ObserveReportを取得して返す
                report = ObserveReport.objects.filter(
                    model_version=latest_version,
                    target_year=target_year,
                    target_month=target_month,
                    target_half=target_half
                ).first()
                
                logger.info(
                    "予測結果保存成功 (feedback_mode=%s): report_id=%s, target=%s-%02d %s", 
                    feedback_mode, report.id if report else "None", target_year, target_month, target_half
                )
                return report
            else:
                logger.warning(
                    "予測結果が生成されませんでした: target=%s-%02d %s", 
                    target_year, target_month, target_half
                )
                return None

        except ForecastModelVersion.DoesNotExist:
            logger.error(f"アクティブなモデルが見つかりません: model_kind_id={model_kind_id}, target_month={target_month}")
            return None
        except Exception as e:
            logger.error(f"予測実行中にエラーが発生: {str(e)}", exc_info=True)
            return None
    
    def _calculate_prediction_date(self, year: int, month: int, half: str) -> date:
        """
        予測対象の年月・前後半から代表日付を計算する
        前半: 月の15日、後半: 月の末日を使用
        """
        try:
            if half == "前半":
                return date(year, month, 15)
            else:  # "後半"
                # 月末日を計算
                if month == 12:
                    next_month = date(year + 1, 1, 1)
                else:
                    next_month = date(year, month + 1, 1)
                from datetime import timedelta
                return next_month - timedelta(days=1)
        except ValueError:
            # 無効な日付の場合は現在日を返す（フォールバック）
            return timezone.now().date()


# DataIngestorクラスは ingest.services.DataIngestor に移動しました