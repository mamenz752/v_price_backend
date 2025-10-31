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
        model_version = FeedbackService.get_active_model(vegetable, month)
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