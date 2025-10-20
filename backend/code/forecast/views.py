from django.shortcuts import render, redirect
from django.contrib import messages
from django.http import HttpResponseRedirect
from django.urls import reverse
from django.views.decorators.http import require_POST
from django.db.models import F

from forecast.models import ForecastModelKind, ForecastModelVersion, ForecastModelCoef, ForecastModelEvaluation
from .service.run_ols import ForecastOLSRunner, ForecastOLSConfig
# 遅延インポートのために、run_ols からのインポートは行わない

def index(request):
    """予測モデルのメイン画面"""
    # モデル種類一覧を取得
    model_kinds = ForecastModelKind.objects.all().order_by('tag_name')
    
    # アクティブなモデルバージョン一覧を取得
    active_models = ForecastModelVersion.objects.filter(is_active=True)\
        .select_related('model_kind')\
        .order_by('model_kind__tag_name', 'target_month')
        
    # すべてのモデルバージョン一覧
    all_models = ForecastModelVersion.objects.all()\
        .select_related('model_kind')\
        .order_by('-created_at')[:30]  # 最新30件だけ取得
    
    # モデル評価情報を取得
    model_evaluations = ForecastModelEvaluation.objects.all().order_by('-created_at')[:30]
    
    # モデル係数を取得
    model_coefficients = ForecastModelCoef.objects.select_related('variable')\
        .order_by('-created_at')[:100]  # 最新100件の係数
    
    # テンプレートに渡すコンテキスト
    context = {
        'model_kinds': model_kinds,
        'active_models': active_models,
        'all_models': all_models,
        'model_evaluations': model_evaluations,
        'model_coefficients': model_coefficients,
    }
    
    return render(request, 'forecast/index.html', context)

@require_POST
def run_model(request):
    """モデル実行ビュー（POSTのみ）"""
    model_name = request.POST.get('model_name')
    target_month = request.POST.get('target_month')
    
    # 入力検証
    if not model_name or not target_month:
        messages.error(request, 'モデル名と対象月は必須です')
        return redirect('forecast:index')
    
    try:
        target_month = int(target_month)
        if not 1 <= target_month <= 12:
            raise ValueError('対象月は1～12の値にしてください')
    except ValueError as e:
        messages.error(request, f'無効な対象月: {str(e)}')
        return redirect('forecast:index')
    
    # モデルの実行
    try:
        config = ForecastOLSConfig(region_name='広島', deactivate_previous=True)
        runner = ForecastOLSRunner(config=config)
        
        # モデル実行
        model_version = runner.fit_and_persist(model_name, target_month)
        
        if model_version:
            messages.success(request, f'モデル「{model_name}」（{target_month}月）を正常に実行しました')
        else:
            messages.warning(request, f'モデル「{model_name}」（{target_month}月）は既に実行済みか、データがありません')
    
    except Exception as e:
        messages.error(request, f'モデル実行中にエラーが発生しました: {str(e)}')
    
    return redirect('forecast:index')

@require_POST
def run_multiple_models(request):
    """複数モデル一括実行ビュー（POSTのみ）"""
    model_names = request.POST.getlist('model_names')
    target_months = request.POST.getlist('target_months')
    
    # 入力検証
    if not model_names or not target_months:
        messages.error(request, 'モデル名と対象月を少なくとも1つずつ選択してください')
        return redirect('forecast:index')
    
    # 対象月をint型に変換
    try:
        target_months = [int(m) for m in target_months]
        for m in target_months:
            if not 1 <= m <= 12:
                raise ValueError('対象月は1～12の値にしてください')
    except ValueError as e:
        messages.error(request, f'無効な対象月: {str(e)}')
        return redirect('forecast:index')
    
    # 複数モデルの実行
    try:
        config = ForecastOLSConfig(region_name='広島', deactivate_previous=True)
        runner = ForecastOLSRunner(config=config)
        
        # モデル一括実行
        results = runner.run_forecast_analysis(model_names, target_months)
        
        # 結果サマリーを生成
        total_models = len(model_names) * len(target_months)
        success_count = sum(1 for model in results.values() 
                           for result in model.values() if result['success'])
        
        messages.success(request, f'全{total_models}件中、{success_count}件のモデルを正常に実行しました')
        
        # エラーがあれば詳細を表示
        for model_name, month_results in results.items():
            for month, result in month_results.items():
                if not result['success'] and result['error']:
                    messages.warning(request, f'「{model_name}」（{month}月）: {result["error"]}')
    
    except Exception as e:
        messages.error(request, f'モデル実行中にエラーが発生しました: {str(e)}')
    
    return redirect('forecast:index')
