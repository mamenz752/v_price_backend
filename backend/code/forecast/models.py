from django.db import models
from ingest.models import Vegetable

# Create your models here.
class TimeStampedModel(models.Model):
    """作成時刻・更新時刻を自動保存する共通基底クラス"""
    created_at = models.DateTimeField(auto_now_add=True)  # 挿入時に自動セット
    updated_at = models.DateTimeField(auto_now=True)      # 保存のたびに更新

    class Meta:
        abstract = True

class ForecastModelKind(TimeStampedModel):
    """予測モデルの種類を表すモデル"""
    tag_name = models.CharField(max_length=100, unique=True)
    vegetable = models.ForeignKey(
        Vegetable,
        on_delete=models.CASCADE,
        related_name="forecast_model_kinds",
    )

    def __str__(self):
        return self.tag_name

class ForecastModelVariable(models.Model):
    name = models.CharField(max_length=100)
    previous_term = models.IntegerField()

    def __str__(self):
        return self.name

class ForecastModelFeatureSet(TimeStampedModel):
    target_month = models.IntegerField()
    model_kind = models.ForeignKey(
        ForecastModelKind,
        on_delete=models.CASCADE,
        related_name="forecast_model_feature_sets",
    )
    variable = models.ForeignKey(
        ForecastModelVariable,
        on_delete=models.CASCADE,
        related_name="forecast_model_feature_sets",
    )

    def __str__(self):
        return self.target_month.__str__() + " - " + self.model_kind.tag_name + " - " + self.variable.name

class ForecastModelVersion(TimeStampedModel):
    target_month = models.IntegerField()
    is_active = models.BooleanField(default=False)
    model_kind = models.ForeignKey(
        ForecastModelKind,
        on_delete=models.CASCADE,
        related_name="forecast_model_versions",
    )

    def __str__(self):
        return self.target_month.__str__() + " - " + self.model_kind.tag_name + " - " + ("Active" if self.is_active else "Inactive")
    
class ForecastModelCoef(TimeStampedModel):
    model_version = models.ForeignKey(
        ForecastModelVersion,
        on_delete=models.CASCADE,
        related_name="coefficients",
        null=True,  # 既存のレコード用に一時的にnullを許可
        default=None
    )
    is_segment = models.BooleanField(default=False)
    variable = models.ForeignKey(
        ForecastModelVariable,
        on_delete=models.CASCADE,
        related_name="forecast_model_coefs",
    )
    coef = models.FloatField()
    value_t = models.FloatField()
    sign_p = models.FloatField()
    standard_error = models.FloatField()

    def __str__(self):
        return f"{self.model_version} - {self.variable.name} - Coef: {self.coef}"

class ForecastModelEvaluation(TimeStampedModel):
    model_version = models.ForeignKey(
        ForecastModelVersion,
        on_delete=models.SET_NULL,  # モデルバージョンが削除されても評価は保持
        related_name='evaluations',
        null=True,
        blank=True
    )
    multi_r = models.FloatField()
    heavy_r2 = models.FloatField()
    adjusted_r2 = models.FloatField()
    sign_f = models.FloatField()
    standard_error = models.FloatField()
    rmse = models.FloatField()
    reg_variation = models.FloatField()
    reg_variance = models.FloatField()
    res_variation = models.FloatField()
    res_variance = models.FloatField()
    total_variation = models.FloatField()

    def __str__(self):
        return f"{self.model_version} - R²: {self.heavy_r2}, RMSE: {self.rmse}"
    
