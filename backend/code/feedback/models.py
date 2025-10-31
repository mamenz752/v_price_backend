from django.db import models
from django.utils import timezone

class PredictionMetrics(models.Model):
    """
    予測精度メトリクスを保存するモデル
    """
    vegetable = models.CharField(max_length=50, verbose_name='野菜')
    month = models.IntegerField(verbose_name='月')
    r2_score = models.FloatField(verbose_name='決定係数')
    std_error = models.FloatField(verbose_name='標準誤差')
    mae = models.FloatField(verbose_name='平均絶対誤差')
    rmse = models.FloatField(verbose_name='二乗平均平方根誤差')
    f_significance = models.FloatField(verbose_name='F値の有意確率')
    created_at = models.DateTimeField(default=timezone.now, verbose_name='作成日時')

    class Meta:
        verbose_name = '予測精度メトリクス'
        verbose_name_plural = '予測精度メトリクス'
        unique_together = ('vegetable', 'month', 'created_at')
        indexes = [
            models.Index(fields=['vegetable', 'month']),
            models.Index(fields=['created_at']),
        ]

class ModelEvaluation(models.Model):
    """
    モデル評価結果を保存するモデル
    """
    vegetable = models.CharField(max_length=50, verbose_name='野菜')
    month = models.IntegerField(verbose_name='月')
    status = models.CharField(max_length=20, verbose_name='ステータス')
    description = models.TextField(verbose_name='説明')
    created_at = models.DateTimeField(default=timezone.now, verbose_name='作成日時')

    class Meta:
        verbose_name = 'モデル評価'
        verbose_name_plural = 'モデル評価'
        unique_together = ('vegetable', 'month', 'created_at')
        indexes = [
            models.Index(fields=['vegetable', 'month']),
            models.Index(fields=['created_at']),
        ]

class VariableImportance(models.Model):
    """
    説明変数の重要度を保存するモデル
    """
    vegetable = models.CharField(max_length=50, verbose_name='野菜')
    month = models.IntegerField(verbose_name='月')
    variable_name = models.CharField(max_length=100, verbose_name='変数名')
    coefficient = models.FloatField(verbose_name='係数')
    t_value = models.FloatField(verbose_name='t値')
    p_value = models.FloatField(verbose_name='p値')
    created_at = models.DateTimeField(default=timezone.now, verbose_name='作成日時')

    class Meta:
        verbose_name = '変数重要度'
        verbose_name_plural = '変数重要度'
        unique_together = ('vegetable', 'month', 'variable_name', 'created_at')
        indexes = [
            models.Index(fields=['vegetable', 'month']),
            models.Index(fields=['created_at']),
        ]
