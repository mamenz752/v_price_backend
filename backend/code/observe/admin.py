from django.contrib import admin
from .models import ObserveReport, ObserveResult

@admin.register(ObserveReport)
class ObserveReportAdmin(admin.ModelAdmin):
    list_display = ('id', 'target_year', 'target_month', 'target_half', 'predict_price', 'min_price', 'max_price', 'model_version', 'created_at', 'updated_at')
    list_filter = ('target_year', 'target_month', 'target_half', 'model_version', 'created_at', 'updated_at')
    search_fields = ('model_version__model_kind__tag_name',)
    date_hierarchy = 'created_at'
    readonly_fields = ('id', 'created_at', 'updated_at')
    ordering = ('-created_at',)

    def get_queryset(self, request):
        return super().get_queryset(request).select_related('model_version', 'model_version__model_kind')

@admin.register(ObserveResult)
class ObserveResultAdmin(admin.ModelAdmin):
    list_display = ('id', 'target_date', 'is_corrected', 'diff', 'model_version', 'created_at', 'updated_at')
    list_filter = ('target_date', 'is_corrected', 'model_version', 'created_at', 'updated_at')
    search_fields = ('model_version__model_kind__tag_name',)
    date_hierarchy = 'created_at'
    readonly_fields = ('id', 'created_at', 'updated_at')
    ordering = ('-created_at',)

    def get_queryset(self, request):
        return super().get_queryset(request).select_related('model_version', 'model_version__model_kind')
