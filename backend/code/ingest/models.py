from django.db import models
import uuid

# Create your models here.
class TimeStampedModel(models.Model):
    """作成時刻・更新時刻を自動保存する共通基底クラス"""
    created_at = models.DateTimeField(auto_now_add=True)  # 挿入時に自動セット
    updated_at = models.DateTimeField(auto_now=True)      # 保存のたびに更新

    class Meta:
        abstract = True

class Vegetable(models.Model):
    name = models.CharField(max_length=20)
    code = models.CharField(max_length=10, default='0')  # IntegerFieldからCharFieldに変更

    def __str__(self):
        return self.name
    
class Region(models.Model):
    name = models.CharField(max_length=20)
    p_area_code = models.CharField(max_length=10, default='0')  # 都道府県コード
    market_code = models.CharField(max_length=10, default='0')  # 市場コード
    fuken_code = models.CharField(max_length=10, default='0')   # 県コード
    station_code = models.CharField(max_length=10, default='0') # 気象庁観測所コード

    def __str__(self):
        return self.name

class IngestMarket(TimeStampedModel):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    target_date = models.DateField()
    high_price = models.FloatField(null=True, blank=True)
    medium_price = models.FloatField(null=True, blank=True)
    low_price = models.FloatField(null=True, blank=True)
    average_price = models.FloatField(null=True, blank=True)
    source_price = models.FloatField(null=True, blank=True)
    arrival_amount = models.FloatField(null=True, blank=True)
    weight_per = models.FloatField(null=True, blank=True)
    volume = models.FloatField(null=True, blank=True)
    trend = models.CharField(max_length=10, null=True, blank=True)
    vegetable = models.ForeignKey(
        Vegetable,
        on_delete=models.CASCADE,
        related_name="ingest_markets",
    )
    region = models.ForeignKey(
        Region,
        on_delete=models.CASCADE,
        related_name="ingest_markets",
        null=True,  # 一時的にnullを許可
        blank=True,  # 一時的に空欄を許可
    )

    def __str__(self):
        return str(self.target_date)
    
class IngestWeather(TimeStampedModel):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    target_date = models.DateField()
    max_temp = models.FloatField(null=True, blank=True)
    mean_temp = models.FloatField(null=True, blank=True)
    min_temp = models.FloatField(null=True, blank=True)
    sum_precipitation = models.FloatField(null=True, blank=True)
    sunshine_duration = models.FloatField(null=True, blank=True)
    ave_humidity = models.FloatField(null=True, blank=True)
    region = models.ForeignKey(
        Region,
        on_delete=models.CASCADE,
        related_name="ingest_weathers",
    )

    def __str__(self):
        return str(self.target_date)