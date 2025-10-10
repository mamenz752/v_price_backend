from django.contrib import admin

# Register your models here.
from django.utils.html import format_html
from .models import IngestMarket, IngestWeather, Vegetable, Region

@admin.register(IngestMarket)
class IngestMarketAdmin(admin.ModelAdmin):
    list_display = ("id", "target_date", "high_price", "medium_price", "low_price", "average_price", "arrival_amount", "weight_per", "vegetable")
    readonly_fields = ("id",)
    fields = ("target_date", "high_price", "medium_price", "low_price", "average_price", "arrival_amount", "weight_per", "vegetable")

@admin.register(IngestWeather)
class IngestWeatherAdmin(admin.ModelAdmin):
    list_display = ("id", "target_date", "max_temp", "mean_temp", "min_temp", "sum_precipitation", "sunshine_duration", "ave_humidity", "region")
    readonly_fields = ("id",)
    fields = ("target_date", "max_temp", "mean_temp", "min_temp", "sum_precipitation", "sunshine_duration", "ave_humidity", "region")

@admin.register(Vegetable)
class VegetableAdmin(admin.ModelAdmin):
    list_display = ("id", "name", "code")
    readonly_fields = ("id",)
    fields = ("name", "code")

@admin.register(Region)
class RegionAdmin(admin.ModelAdmin):
    list_display = ("id", "name", "price_code", "weather_code")
    readonly_fields = ("id",)
    fields = ("name", "price_code", "weather_code")