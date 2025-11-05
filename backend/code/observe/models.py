from django.db import models
from forecast.models import ForecastModelVersion

# Create your models here.
class TimeStampedModel(models.Model):
    """作成時刻・更新時刻を自動保存する共通基底クラス"""
    created_at = models.DateTimeField(auto_now_add=True)  # 挿入時に自動セット
    updated_at = models.DateTimeField(auto_now=True)      # 保存のたびに更新

    class Meta:
        abstract = True

class ObserveReport(TimeStampedModel):
    """予測値を表すモデル"""
    target_year = models.IntegerField()
    target_month = models.IntegerField()
    target_half = models.CharField(max_length=5)
    predict_price = models.FloatField()
    min_price = models.FloatField()
    max_price = models.FloatField()
    model_version = models.ForeignKey(
        ForecastModelVersion,
        on_delete=models.CASCADE,
        related_name="observe_reports",
    )

    def __str__(self):
        return f"ObserveReport: {self.target_year}-{self.target_month}-{self.target_half}"
    
class ObserveResult(TimeStampedModel):
    """観測値を表すモデル"""
    target_date = models.DateField()
    is_corrected = models.BooleanField(default=False)
    diff = models.FloatField()
    model_version = models.ForeignKey(
        ForecastModelVersion,
        on_delete=models.CASCADE,
        related_name="observe_results",
    )

    def __str__(self):
        return f"ObserveResult: {self.target_date} - {self.diff}";