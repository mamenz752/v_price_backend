from django.contrib import admin
from .models import ComputeMarket, ComputeWeather

# Register your models here.
@admin.register(ComputeMarket)
class ComputeMarketAdmin(admin.ModelAdmin):
    list_display = ('id', 'vegetable', 'region', 'target_year', 'target_month', 'target_half', 'average_price', 'source_price', 'volume', 'trend')
    list_filter = ('vegetable', 'region', 'target_year', 'target_month', 'target_half')
    search_fields = ('vegetable__name', 'region__name')

@admin.register(ComputeWeather)
class ComputeWeatherAdmin(admin.ModelAdmin):
    list_display = ('id', 'region', 'target_year', 'target_month', 'target_half', 'mean_temp', 'max_temp', 'min_temp', 'sum_precipitation', 'sunshine_duration', 'ave_humidity')
    list_filter = ('region', 'target_year', 'target_month', 'target_half')
    search_fields = ('region__name',)