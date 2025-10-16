from django.db import models
import uuid
from ingest.models import Vegetable, Region

# Create your models here.
class TimeStampedModel(models.Model):
    """作成時刻・更新時刻を自動保存する共通基底クラス"""
    created_at = models.DateTimeField(auto_now_add=True)  # 挿入時に自動セット
    updated_at = models.DateTimeField(auto_now=True)      # 保存のたびに更新

    class Meta:
        abstract = True

class ComputeMarket(TimeStampedModel):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    target_year = models.IntegerField()
    target_month = models.IntegerField()
    target_half = models.CharField(max_length=5)  # '前半' or '後半'
    # high_price = models.FloatField(null=True, blank=True)
    # medium_price = models.FloatField(null=True, blank=True)
    # low_price = models.FloatField(null=True, blank=True)
    average_price = models.FloatField(null=True, blank=True)
    source_price = models.FloatField(null=True, blank=True)
    volume = models.FloatField(null=True, blank=True)
    trend = models.CharField(max_length=10, null=True, blank=True)
    vegetable = models.ForeignKey(
        Vegetable,
        on_delete=models.CASCADE,
        related_name="compute_markets",
    )
    region = models.ForeignKey(
        Region,
        on_delete=models.CASCADE,
        related_name="compute_markets",
        null=True,  # 一時的にnullを許可
        blank=True,  # 一時的に空欄を許可
    )

    def __str__(self):
        return f"{self.vegetable} - {self.region} - {self.target_year}/{self.target_month} {self.target_half}"
    
class ComputeWeather(TimeStampedModel):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    target_year = models.IntegerField()
    target_month = models.IntegerField()
    target_half = models.CharField(max_length=5)  # '前半' or '後半'
    max_temp = models.FloatField(null=True, blank=True)
    mean_temp = models.FloatField(null=True, blank=True)
    min_temp = models.FloatField(null=True, blank=True)
    sum_precipitation = models.FloatField(null=True, blank=True)
    sunshine_duration = models.FloatField(null=True, blank=True)
    ave_humidity = models.FloatField(null=True, blank=True)
    region = models.ForeignKey(
        Region,
        on_delete=models.CASCADE,
        related_name="compute_weathers",
    )

    def __str__(self):
        return f"{self.region} - {self.target_year}/{self.target_month} {self.target_half}"