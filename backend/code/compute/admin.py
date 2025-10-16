from django.contrib import admin, messages
from django.template.response import TemplateResponse
from django.urls import reverse

from .models import ComputeMarket, ComputeWeather
from .service import (
    aggregate_all_data,
    aggregate_market_data,
    aggregate_weather_data,
    reset_compute_data,
)

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


def _build_context(request, title: str, description: str, button_label: str):
    context = admin.site.each_context(request)
    context.update(
        {
            "title": title,
            "description": description,
            "button_label": button_label,
            "back_url": reverse("admin:index"),
        }
    )
    return context


def compute_all_view(request):
    context = _build_context(
        request,
        "Computeデータ生成（価格＋天気）",
        "取り込み済みの価格・天気データを半月単位で集計し、Computeテーブルに保存します。",
        "価格と天気を更新する",
    )

    if request.method == "POST":
        results = aggregate_all_data()
        market_result = results["market"]
        weather_result = results["weather"]
        context.update(
            {
                "stats": [
                    f"市場データ: 新規 {market_result.created} 件 / 更新 {market_result.updated} 件",
                    f"天気データ: 新規 {weather_result.created} 件 / 更新 {weather_result.updated} 件",
                ],
                "success": True,
            }
        )
        messages.success(request, "市場・天気データをComputeテーブルへ反映しました。")

    return TemplateResponse(request, "admin/compute_actions.html", context)


def compute_market_view(request):
    context = _build_context(
        request,
        "Computeデータ生成（価格のみ）",
        "取り込み済みの市場データを半月単位で集計し、ComputeMarketに保存します。",
        "価格データを更新する",
    )

    if request.method == "POST":
        result = aggregate_market_data()
        context.update(
            {
                "stats": [
                    f"市場データ: 新規 {result.created} 件 / 更新 {result.updated} 件",
                ],
                "success": True,
            }
        )
        messages.success(request, "市場データをComputeMarketへ反映しました。")

    return TemplateResponse(request, "admin/compute_actions.html", context)


def compute_weather_view(request):
    context = _build_context(
        request,
        "Computeデータ生成（天気のみ）",
        "取り込み済みの天気データを半月単位で集計し、ComputeWeatherに保存します。",
        "天気データを更新する",
    )

    if request.method == "POST":
        result = aggregate_weather_data()
        context.update(
            {
                "stats": [
                    f"天気データ: 新規 {result.created} 件 / 更新 {result.updated} 件",
                ],
                "success": True,
            }
        )
        messages.success(request, "天気データをComputeWeatherへ反映しました。")

    return TemplateResponse(request, "admin/compute_actions.html", context)


def compute_reset_view(request):
    context = _build_context(
        request,
        "Computeデータの初期化",
        "ComputeMarketとComputeWeatherのデータをすべて削除します。元に戻せないためご注意ください。",
        "Computeデータを削除する",
    )

    if request.method == "POST":
        deleted = reset_compute_data()
        context.update(
            {
                "stats": [
                    f"市場データ: {deleted['market_deleted']} 件削除",
                    f"天気データ: {deleted['weather_deleted']} 件削除",
                ],
                "success": True,
            }
        )
        messages.success(request, "Computeテーブルのデータを削除しました。")

    return TemplateResponse(request, "admin/compute_actions.html", context)
