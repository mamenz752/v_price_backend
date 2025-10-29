from django.contrib import admin
from django.contrib import messages
from django.utils.html import format_html
from django.urls import path
from django.http import HttpResponseRedirect
from django.core.exceptions import PermissionDenied
from django.contrib.admin import helpers
from django.template.response import TemplateResponse
from .models import (
    ForecastModelKind,
    ForecastModelVariable,
    ForecastModelFeatureSet,
    ForecastModelVersion,
    ForecastModelCoef,
    ForecastModelEvaluation
)

# カスタムアクション：予測モデル関連データの一括削除
def delete_all_forecast_data(modeladmin, request, queryset):
    if not request.user.is_superuser:
        raise PermissionDenied

    if request.POST.get("post"):
        # 確認後の実際の削除処理
        ForecastModelEvaluation.objects.all().delete()
        ForecastModelCoef.objects.all().delete()
        ForecastModelVersion.objects.all().delete()
        ForecastModelFeatureSet.objects.all().delete()
        ForecastModelVariable.objects.all().delete()
        ForecastModelKind.objects.all().delete()

        messages.success(request, "すべての予測モデル関連データが削除されました。")
        return None

    # 削除前の確認ページ用のコンテキスト
    context = {
        "title": "予測モデル関連データの一括削除",
        "queryset": queryset,
        "action_checkbox_name": helpers.ACTION_CHECKBOX_NAME,
        "opts": modeladmin.model._meta,
        "objects_name": "予測モデル関連データ",
        "deletable_objects": [
            (ForecastModelKind._meta.verbose_name_plural, ForecastModelKind.objects.count()),
            (ForecastModelVariable._meta.verbose_name_plural, ForecastModelVariable.objects.count()),
            (ForecastModelFeatureSet._meta.verbose_name_plural, ForecastModelFeatureSet.objects.count()),
            (ForecastModelVersion._meta.verbose_name_plural, ForecastModelVersion.objects.count()),
            (ForecastModelCoef._meta.verbose_name_plural, ForecastModelCoef.objects.count()),
            (ForecastModelEvaluation._meta.verbose_name_plural, ForecastModelEvaluation.objects.count()),
        ],
    }

    return TemplateResponse(request, "admin/delete_all_forecast_data.html", context)

delete_all_forecast_data.short_description = "すべての予測モデル関連データを削除"

# Register your models here.

@admin.register(ForecastModelKind)
class ForecastModelKindAdmin(admin.ModelAdmin):
    list_display = ('id', 'tag_name', 'vegetable', 'created_at', 'updated_at')
    list_filter = ('vegetable', 'created_at')
    search_fields = ('tag_name', 'vegetable__name')
    date_hierarchy = 'created_at'
    actions = [delete_all_forecast_data]
    
    def get_urls(self):
        urls = super().get_urls()
        custom_urls = [
            path(
                'delete-all-forecast-data/',
                self.admin_site.admin_view(self.delete_all_forecast_data_view),
                name='delete-all-forecast-data',
            ),
        ]
        return custom_urls + urls

    def delete_all_forecast_data_view(self, request):
        return delete_all_forecast_data(self, request, self.model.objects.none())

@admin.register(ForecastModelVariable)
class ForecastModelVariableAdmin(admin.ModelAdmin):
    list_display = ('id', 'name', 'previous_term')
    list_filter = ('previous_term',)
    search_fields = ('name',)

@admin.register(ForecastModelFeatureSet)
class ForecastModelFeatureSetAdmin(admin.ModelAdmin):
    list_display = ('id', 'target_month', 'model_kind', 'variable', 'created_at', 'updated_at')
    list_filter = ('target_month', 'model_kind', 'variable', 'created_at')
    search_fields = ('model_kind__tag_name', 'variable__name')
    date_hierarchy = 'created_at'

@admin.register(ForecastModelVersion)
class ForecastModelVersionAdmin(admin.ModelAdmin):
    list_display = ('id', 'target_month', 'is_active', 'model_kind', 'created_at', 'updated_at')
    list_filter = ('target_month', 'is_active', 'model_kind', 'created_at')
    search_fields = ('model_kind__tag_name',)
    date_hierarchy = 'created_at'

@admin.register(ForecastModelCoef)
class ForecastModelCoefAdmin(admin.ModelAdmin):
    list_display = ('id', 'variable', 'coef', 'value_t', 'sign_p', 'standard_error', 'is_segment', 'created_at')
    list_filter = ('is_segment', 'variable', 'created_at')
    search_fields = ('variable__name',)
    date_hierarchy = 'created_at'

@admin.register(ForecastModelEvaluation)
class ForecastModelEvaluationAdmin(admin.ModelAdmin):
    list_display = ('id', 'multi_r', 'heavy_r2', 'adjusted_r2', 'rmse', 'sign_f', 'created_at')
    list_filter = ('created_at',)
    search_fields = ('id',)
    date_hierarchy = 'created_at'