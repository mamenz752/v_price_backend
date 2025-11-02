from django.db import transaction
from django.utils import timezone
from typing import List, Optional, Dict
from datetime import datetime, timedelta
import logging

from .service.run_ols import ForecastOLSRunner, ForecastOLSConfig
from .models import (
    ForecastModelKind,
    ForecastModelVariable,
    ForecastModelFeatureSet,
    ForecastModelVersion,
    ForecastModelCoef,
    ForecastModelEvaluation
)

class ForecastModelService:
    """予測モデルの実行を管理するサービス"""

    @staticmethod
    def get_active_model(vegetable: str, month: int) -> Optional[ForecastModelVersion]:
        """
        指定された野菜と月のアクティブなモデルバージョンを取得
        """
        try:
            model_kind = ForecastModelKind.objects.get(
                vegetable__name=vegetable
            )
            model_version = ForecastModelVersion.objects.get(
                model_kind=model_kind,
                target_month=month,
                is_active=True
            )
            return model_version
        except (ForecastModelKind.DoesNotExist, ForecastModelVersion.DoesNotExist):
            return None

    @staticmethod
    def get_latest_metrics(vegetable: str, month: int) -> Optional[dict]:
        """
        指定された野菜と月の最新の予測精度メトリクスを取得
        """
        model_version = ForecastModelService.get_active_model(vegetable, month)
        if not model_version:
            return None

        try:
            metrics = ForecastModelEvaluation.objects.filter(
                model_version=model_version
            ).order_by('-created_at').first()

            if not metrics:
                return None

            return {
                'r2': metrics.heavy_r2,
                'std_error': metrics.standard_error,
                'mae': metrics.rmse,  # MAEがないのでRMSEを使用
                'rmse': metrics.rmse,
                'f_significance': metrics.sign_f
            }
        except ForecastModelEvaluation.DoesNotExist:
            return None

    @staticmethod
    def get_latest_evaluation(vegetable: str, month: int) -> dict:
        """
        指定された野菜と月の最新のモデル評価を取得
        """
        model_version = ForecastModelService.get_active_model(vegetable, month)
        if not model_version:
            return {
                'status': '未評価',
                'description': f'{month}月のアクティブなモデルが存在しません。'
            }

        try:
            metrics = ForecastModelEvaluation.objects.filter(
                model_version=model_version
            ).order_by('-created_at').first()

            if not metrics:
                return {
                    'status': '未評価',
                    'description': f'{month}月のモデル評価データが存在しません。'
                }

            # R²値に基づいてステータスを判定
            if metrics.heavy_r2 >= 0.8:
                status = '優良'
                desc = 'モデルの予測精度は良好です。'
            elif metrics.heavy_r2 >= 0.6:
                status = '良好'
                desc = 'モデルの予測精度は許容範囲内です。'
            else:
                status = '要注意'
                desc = 'モデルの予測精度が低下しています。改善が必要かもしれません。'

            return {
                'status': status,
                'description': f'{month}月のモデル評価: {desc} (R² = {metrics.heavy_r2:.3f})'
            }
        except ForecastModelEvaluation.DoesNotExist:
            return {
                'status': '未評価',
                'description': f'{month}月のモデル評価データが存在しません。'
            }

    @staticmethod
    def get_latest_variables(vegetable: str, month: int) -> list:
        """
        指定された野菜と月の最新の変数重要度を取得
        """
        model_version = ForecastModelService.get_active_model(vegetable, month)
        if not model_version:
            return []

        try:
            variables = ForecastModelCoef.objects.filter(
                model_version=model_version
            ).select_related('variable')

            if not variables:
                return []

            return [{
                'name': var.variable.name,
                'coefficient': var.coef,
                't_value': var.value_t,
                'p_value': var.sign_p
            } for var in variables]
        except ForecastModelCoef.DoesNotExist:
            return []

    @staticmethod
    def get_accuracy_history(vegetable: str, month: int, months_back: int = 6) -> Optional[dict]:
        """
        指定された野菜と月の予測精度推移データを取得
        """
        model_version = ForecastModelService.get_active_model(vegetable, month)
        if not model_version:
            return None

        end_date = timezone.now()
        start_date = end_date - timedelta(days=30 * months_back)

        try:
            metrics = ForecastModelEvaluation.objects.filter(
                model_version=model_version,
                created_at__range=(start_date, end_date)
            ).order_by('created_at')

            if not metrics:
                return None

            dates = [m.created_at.strftime('%Y-%m') for m in metrics]
            accuracy = [m.heavy_r2 for m in metrics]

            return {
                'data': [{
                    'x': dates,
                    'y': accuracy,
                    'type': 'scatter',
                    'name': '予測精度'
                }],
                'layout': {
                    'title': f'{month}月の予測精度推移',
                    'xaxis': {'title': '期間'},
                    'yaxis': {'title': 'R²'}
                }
            }
        except ForecastModelEvaluation.DoesNotExist:
            return None

    @transaction.atomic
    def run_model(self, tag_name: str, target_month: int, variables) -> bool:
        """
        指定されたモデルを実行する

        Args:
            tag_name: モデルの種類を示すタグ名
            target_month: 対象月（1-12）
            variables: 使用する変数のクエリセット

        Returns:
            bool: モデルの実行が成功したかどうか
        """
        logger = logging.getLogger(__name__)
        model_version = None  # 変数をスコープの外で初期化

        try:
            # モデル種類を取得
            logger.info(f"モデル種類の取得: tag_name={tag_name}")
            model_kind = ForecastModelKind.objects.get(tag_name=tag_name)

            # 変数を取得
            logger.info(f"変数の取得: {variables}")
            # variables = ForecastModelVariable.objects.filter(name__in=variable_names)
            if not variables:
                logger.error("指定された変数が見つかりません")
                return False

            # 重回帰分析の実行
            config = ForecastOLSConfig(
                region_name='広島',
                deactivate_previous=True
            )
            runner = ForecastOLSRunner(config=config)

            try:
                # variable_names = [var.name for var in variables]
                logger.info(f"モデル実行開始: {model_kind.tag_name}, 月={target_month}, 変数={variables}")

                model_version = runner.fit_and_persist(
                    model_kind.tag_name,
                    target_month,
                    variables
                )

                if model_version:
                    logger.info(f"モデル実行成功: バージョンID={model_version.id}")
                    return True
                else:
                    logger.error("モデル実行失敗: モデルバージョンが作成されませんでした")
                    return False
            except Exception as e:
                logger.error(f"重回帰分析の実行中にエラーが発生しました: {str(e)}", exc_info=True)
                if model_version:  # model_versionが存在する場合のみ削除
                    try:
                        model_version.delete()
                        logger.info("エラー時のモデルバージョンを削除しました")
                    except Exception as delete_error:
                        logger.error(f"モデルバージョンの削除中にエラーが発生しました: {str(delete_error)}")
                return False
        except Exception as e:
            logger.error(f"モデル実行中にエラーが発生しました: {str(e)}", exc_info=True)
            if model_version:  # model_versionが存在する場合のみ削除
                try:
                    model_version.delete()
                    logger.info("エラー時のモデルバージョンを削除しました")
                except Exception as delete_error:
                    logger.error(f"モデルバージョンの削除中にエラーが発生しました: {str(delete_error)}")
            return False


class ModelVariableDisplayService:
    """予測モデルの変数表示を管理するサービス"""
    
    @staticmethod
    def get_variable_name_mapping():
        """変数名の日本語マッピングを取得"""
        return {
            # 価格関連
            'price': '価格',
            'price_avg': '平均価格',
            'price_std': '価格標準偏差',
            'average_price': '平均価格',
            
            # 気象関連
            'temperature': '気温',
            'mean_temp': '平均気温',
            'max_temp': '最高気温',
            'min_temp': '最低気温',
            'sum_precipitation': '降水量の合計',
            'rain_days': '降水日数',
            'sunshine_duration': '日照時間',
            'sun_days': '日照日数',
            'ave_humidity': '平均湿度',
            'min_humidity': '最小湿度',
            'max_humidity': '最大湿度',
            'wind_speed': '風速',
            'snow_days': '降雪日数',
            'snow_coverage': '積雪量',

            # 生産関連
            'production': '生産量',
            'yield_per_area': '単位面積当たり収量',
            'planted_area': '作付面積',
            'shipment': '出荷量',

            # 市場関連
            'market_amount': '市場取扱量',
            'volume': '取引量',
            'transaction_volume': '取引量',
            'market_price': '市場価格',
            
            # その他
            'const': '定数項'
        }

    @classmethod
    def get_display_name(cls, variable_name: str) -> str:
        """変数の日本語表示名を取得"""
        mapping = cls.get_variable_name_mapping()
        return mapping.get(variable_name, variable_name)

    @staticmethod
    def get_term_display(previous_term: int) -> str:
        """期間を0.5カ月単位で表示"""
        months = previous_term * 0.5
        if months == 0:
            return "現在"
        return f"{months}カ月前"

    @classmethod
    def format_variable_display(cls, variable: ForecastModelVariable) -> str:
        """変数の表示形式を整形"""
        display_name = cls.get_display_name(variable.name)
        term_display = cls.get_term_display(variable.previous_term)
        return f"{display_name} ({term_display})"