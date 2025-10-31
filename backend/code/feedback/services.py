from django.utils import timezone
from datetime import datetime, timedelta
from forecast.models import (
    ForecastModelKind, 
    ForecastModelVersion, 
    ForecastModelCoef, 
    ForecastModelEvaluation
)

class FeedbackService:
    """
    フィードバックデータを取得・管理するサービスクラス
    """
    
    @staticmethod
    def format_variable_term(previous_term: int) -> str:
        """
        期間を月単位で表示するフォーマッター
        """
        months = previous_term * 0.5
        if months == 0:
            return "現在"
        elif months == 0.5:
            return "0.5か月前"
        else:
            return f"{months}か月前"
            
    @staticmethod
    def format_variable_name(name: str) -> str:
        """
        変数名を日本語表示用にフォーマット
        """
        name_mapping = {
            # 価格関連
            'price': '価格',
            'price_avg': '平均価格',
            'price_std': '価格標準偏差',
            
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
            'transaction_volume': '取引量',
            'market_price': '市場価格',
            
            # デフォルト値（マッピングがない場合）
            'const': '定数項',
        }
        return name_mapping.get(name, name)  # マッピングがない場合は元の名前を使用
    @staticmethod
    def get_active_model(vegetable: str, month: int):
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
    def get_latest_metrics(vegetable: str, month: int) -> dict:
        """
        指定された野菜と月の最新の予測精度メトリクスを取得
        """
        import logging
        logger = logging.getLogger(__name__)
        logger.debug("get_latest_metrics called: vegetable=%s, month=%s", vegetable, month)
        model_version = FeedbackService.get_active_model(vegetable, month)
        if not model_version:
            logger.debug("No active model found for %s month=%s", vegetable, month)
            return None

        try:
            logger.debug("Querying ForecastModelEvaluation for model_version id=%s", getattr(model_version, 'id', None))
            metrics = ForecastModelEvaluation.objects.filter(
                model_version=model_version
            ).order_by('-created_at').first()

            if not metrics:
                logger.debug("No ForecastModelEvaluation entries for model_version id=%s", getattr(model_version, 'id', None))
                # フォールバック: 同じ model_kind と target_month の最新の評価を探す
                try:
                    fallback = ForecastModelEvaluation.objects.filter(
                        model_version__model_kind=model_version.model_kind,
                        model_version__target_month=model_version.target_month
                    ).order_by('-created_at').first()
                    if fallback:
                        logger.info("Falling back to evaluation id=%s from model_version id=%s", getattr(fallback, 'id', None), getattr(fallback.model_version, 'id', None))
                        metrics = fallback
                    else:
                        logger.debug("No fallback evaluation found for model_kind id=%s target_month=%s", getattr(model_version.model_kind, 'id', None), model_version.target_month)
                        return None
                except Exception as e:
                    logger.exception("Error while attempting fallback evaluation lookup: %s", e)
                    return None

            result = {
                'r2': metrics.adjusted_r2,
                'std_error': metrics.standard_error,
                'mae': metrics.rmse,  # MAEがないのでRMSEを使用
                'rmse': metrics.rmse,
                'f_significance': metrics.sign_f
            }
            logger.debug("Returning metrics for model_version id=%s: %s", getattr(model_version, 'id', None), result)
            return result
        except Exception as e:
            logger.exception("Error while fetching latest metrics for %s month=%s: %s", vegetable, month, e)
            return None

    @staticmethod
    def get_latest_evaluation(vegetable: str, month: int) -> dict:
        """
        指定された野菜と月の最新のモデル評価を取得
        """
        model_version = FeedbackService.get_active_model(vegetable, month)
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
        model_version = FeedbackService.get_active_model(vegetable, month)
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
    def get_accuracy_history(vegetable: str, month: int, months_back: int = 6) -> dict:
        """
        指定された野菜と月の予測精度推移データを取得
        """
        model_version = FeedbackService.get_active_model(vegetable, month)
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