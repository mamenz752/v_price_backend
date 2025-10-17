from django.contrib import admin
from .models import (
    ForecastModelKind,
    ForecastModelVariable,
    ForecastModelFeatureSet,
    ForecastModelVersion,
    ForecastModelCoef,
    ForecastModelEvaluation
)

# Register your models here.

@admin.register(ForecastModelKind)
class ForecastModelKindAdmin(admin.ModelAdmin):
    list_display = ('id', 'tag_name', 'vegetable', 'created_at', 'updated_at')
    list_filter = ('vegetable', 'created_at')
    search_fields = ('tag_name', 'vegetable__name')
    date_hierarchy = 'created_at'

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