from django.shortcuts import render, redirect
from django.views.generic import TemplateView
from django.contrib import messages
from django.http import JsonResponse
from .services import FeedbackService
from forecast.models import (
    ForecastModelKind,
    ForecastModelVersion,
    ForecastModelVariable,
    ForecastModelCoef,
    ForecastModelEvaluation,
    ForecastModelFeatureSet
)

class IndexView(TemplateView):
    """
    フィードバック機能のインデックスページを表示するビュー
    """
    template_name = 'feedback/feedback.html'
    
    # インデックスへアクセスしたときは代表的な野菜ページへリダイレクトする
    # （データは各野菜用のサブビューが担当するため、index は直接レンダリングしない）
    def get(self, request, *args, **kwargs):
        from django.shortcuts import redirect
        # デフォルトはキャベツページへリダイレクトする。必要であれば別の野菜に変更してください。
        return redirect('feedback:cabbage')

def get_model_data(vegetable_name, current_month):
    """
    モデル管理用のデータを取得する共通関数
    """
    import logging
    logger = logging.getLogger(__name__)
    logger.debug(f"get_model_data called with vegetable_name={vegetable_name}, current_month={current_month}")

    from ingest.models import Vegetable
    
    # 野菜データの存在を確認
    vegetable_mapping = {
        'はくさい': 'はくさい',
        'キャベツ': 'キャベツ',
        'きゅうり': 'きゅうり',
        'トマト': 'トマト',
        'なす': 'なす',
        'だいこん': 'だいこん',
        'じゃがいも': 'じゃがいも',
        'たまねぎ': 'たまねぎ'
    }
    
    # データベースに登録されている野菜名を取得
    all_vegetables = Vegetable.objects.all()
    logger.debug(f"Registered vegetables: {[v.name for v in all_vegetables]}")

    # テスト_<野菜名>というタグ名でForecastModelKindを検索
    test_tag_name = f"テスト_{vegetable_name}"
    try:
        model_kind = ForecastModelKind.objects.get(tag_name=test_tag_name)
        logger.debug(f"Found model_kind with tag_name={test_tag_name}: {model_kind}")
    except ForecastModelKind.DoesNotExist:
        # モデル種類が存在しない場合は新規作成
        try:
            # まず正確な野菜名で検索
            vegetable = Vegetable.objects.get(name=vegetable_name)
        except Vegetable.DoesNotExist:
            # 正確な野菜名で見つからない場合、マッピングされた名前で検索
            mapped_name = vegetable_mapping.get(vegetable_name)
            logger.debug(f"Trying to find vegetable with mapped name: {mapped_name}")
            try:
                vegetable = Vegetable.objects.get(name=mapped_name)
            except Vegetable.DoesNotExist:
                logger.error(f"No Vegetable found with name={vegetable_name} or {mapped_name}")
                return {
                    'model_variables': [],
                    'active_models': [],
                    'all_models': [],
                    'model_coefficients': []
                }
        
        model_kind = ForecastModelKind.objects.create(
            tag_name=test_tag_name,
            vegetable=vegetable
        )
        logger.debug(f"Created new model_kind with tag_name={test_tag_name}")

    # 利用可能な変数一覧を取得
    service = FeedbackService()
    query_variables = list(ForecastModelVariable.objects.all().order_by('name'))
    
    # 変数がない場合はデフォルトの変数を作成
    if not query_variables:
        logger.debug("No variables found, creating default variables")
        default_variables = [
            ('price_avg', 1),  # 英語名を使用
            ('temp_avg', 1),
            ('rainfall', 1),
            ('sunshine', 1)
        ]
        for name, prev_term in default_variables:
            var = ForecastModelVariable.objects.create(
                name=name,
                previous_term=prev_term
            )
            query_variables.append(var)
            logger.debug(f"Created variable: {var.name} (ID: {var.id})")
    else:
        logger.debug(f"Found {len(query_variables)} existing variables")
        for var in query_variables:
            logger.debug(f"Variable: {var.name} (ID: {var.id})")

    # 変数をフォーマットしたリストを作成
    model_variables = []
    for var in query_variables:
        if not var.name.startswith('const'):  # 定数項を除外
            model_variables.append({
                'id': var.id,
                'name': service.format_variable_name(var.name),
                'term': service.format_variable_term(var.previous_term),
                'previous_term': var.previous_term
            })

    # アクティブモデルを取得
    active_models = ForecastModelVersion.objects.filter(
        model_kind=model_kind,
        is_active=True
    ).select_related('model_kind')

    # 全モデルを取得
    all_models = ForecastModelVersion.objects.filter(
        model_kind=model_kind
    ).select_related('model_kind').order_by('-created_at')

    # 現在の月のアクティブモデルの係数情報を取得
    try:
        current_model = ForecastModelVersion.objects.get(
            model_kind=model_kind,
            target_month=current_month,
            is_active=True
        )
        model_coefficients = ForecastModelCoef.objects.filter(
            model_version=current_model
        ).select_related('variable')
        logger.debug(f"current_model found: id={current_model.id}, target_month={current_model.target_month}")
        logger.debug(f"model_coefficients queryset length: {model_coefficients.count()}")
    except ForecastModelVersion.DoesNotExist:
        logger.debug(f"No current_model for month={current_month} (model_kind={model_kind.tag_name})")
        model_coefficients = []

    # デバッグログ: モデル一覧や係数の件数を出力
    try:
        logger.debug(f"active_models count: {active_models.count()}")
        logger.debug(f"all_models count: {all_models.count()}")
        logger.debug(f"model_coefficients count: {len(model_coefficients) if hasattr(model_coefficients, '__len__') else 'unknown'}")
    except Exception:
        # logger は get_model_data の冒頭で定義済み
        pass

    return {
        'model_variables': model_variables,
        'active_models': active_models,
        'all_models': all_models,
        'model_coefficients': model_coefficients
    }

class BaseVegetableView(TemplateView):
    """
    野菜フィードバックページの基本ビュー
    """
    vegetable_name = None  # サブクラスで設定する

    def get_context_data(self, **kwargs):
        if not self.vegetable_name:
            raise ValueError("vegetable_name must be set in subclass")

        context = super().get_context_data(**kwargs)
        current_month = int(self.request.GET.get('month', 1))
        
        service = FeedbackService()
        metrics = service.get_latest_metrics(self.vegetable_name, current_month)
        evaluation = service.get_latest_evaluation(self.vegetable_name, current_month)
        variables = service.get_latest_variables(self.vegetable_name, current_month)
        accuracy_data = service.get_accuracy_history(self.vegetable_name, current_month)

        # デバッグログ: 各種データが取得できているかを出力        

        # モデル管理データの取得
        model_data = get_model_data(self.vegetable_name, current_month)

        # モデルデータのデバッグ出力
        import logging
        logger = logging.getLogger(__name__)
        logger.debug(f"Model data received: {model_data}")

        context.update({
            'months': range(1, 13),
            'current_month': current_month,
            'metrics': metrics or self.get_default_metrics(),
            'evaluation': evaluation or self.get_default_evaluation(current_month),
            'variables': variables or self.get_default_variables(),
            'accuracy_data': accuracy_data or self.get_default_accuracy_data(current_month),
            'vegetable_name': self.vegetable_name,  # テンプレートで使用
            'debug': True,  # デバッグ情報を表示
            **model_data  # モデル管理用データ
        })
        
        # コンテキストのデバッグ出力
        logger.debug(f"Context model_variables: {context.get('model_variables', 'Not found')}")
        return context
    
    def get_default_metrics(self):
        return {
            'r2': None,
            'std_error': None,
            'mae': None,
            'rmse': None,
            'f_significance': None
        }
    
    def get_default_evaluation(self, month):
        return {
            'status': '未評価',
            'description': f'{month}月のアクティブなモデルが存在しません。'
        }
    
    def get_default_variables(self):
        return []
    
    def get_default_accuracy_data(self, month):
        return {
            'data': [{
                'x': [],
                'y': [],
                'type': 'scatter',
                'name': '予測精度'
            }],
            'layout': {
                'title': f'{month}月の予測精度（データなし）',
                'xaxis': {'title': '期間'},
                'yaxis': {'title': 'R²'}
            }
        }

class ChineseCabbageView(BaseVegetableView):
    """
    白菜のフィードバックページを表示するビュー
    """
    template_name = 'feedback/chinese_cabbage.html'
    vegetable_name = '白菜'

class CabbageView(BaseVegetableView):
    """
    キャベツのフィードバックページを表示するビュー
    """
    template_name = 'feedback/cabbage.html'
    vegetable_name = 'キャベツ'

class CucumberView(BaseVegetableView):
    """
    きゅうりのフィードバックページを表示するビュー
    """
    template_name = 'feedback/cucumber.html'
    vegetable_name = 'きゅうり'

class TomatoView(BaseVegetableView):
    """
    トマトのフィードバックページを表示するビュー
    """
    template_name = 'feedback/tomato.html'
    vegetable_name = 'トマト'

class EggplantView(BaseVegetableView):
    """
    なすのフィードバックページを表示するビュー
    """
    template_name = 'feedback/eggplant.html'
    vegetable_name = 'なす'

class RadishView(BaseVegetableView):
    """
    大根のフィードバックページを表示するビュー
    """
    template_name = 'feedback/radish.html'
    vegetable_name = 'だいこん'

class PotatoView(BaseVegetableView):
    """
    じゃがいものフィードバックページを表示するビュー
    """
    template_name = 'feedback/potato.html'
    vegetable_name = 'じゃがいも'

class OnionView(BaseVegetableView):
    """
    玉ねぎのフィードバックページを表示するビュー
    """
    template_name = 'feedback/onion.html'
    vegetable_name = 'たまねぎ'

def get_tag_name(vegetable_name):
    """
    野菜名からタグ名を生成
    """
    return f"テスト_{vegetable_name}"

def run_model(request, vegetable):
    """
    単一のモデルを実行
    """
    if request.method != 'POST':
        messages.error(request, '不正なリクエストです。')
        return redirect(request.META.get('HTTP_REFERER', '/'))

    target_month = request.POST.get('target_month')
    if not target_month:
        messages.error(request, 'ターゲット月を選択してください。')
        return redirect(request.META.get('HTTP_REFERER', '/'))
    
    try:
        target_month = int(target_month)
    except ValueError:
        messages.error(request, '無効なターゲット月が指定されました。')
        return redirect(request.META.get('HTTP_REFERER', '/'))

    # 通常の説明変数を取得
    variable_ids = request.POST.getlist('variables')
    
    # ComputeMarket変数を取得
    compute_market_variables = request.POST.getlist('compute_market_variables')
    
    # 少なくとも一つの変数が選択されている必要がある
    if not variable_ids and not compute_market_variables:
        messages.error(request, '説明変数を選択してください。')
        return redirect(request.META.get('HTTP_REFERER', '/'))
    
    vals = list(variable_ids)

    try:
        # モデル実行サービスを呼び出し
        from forecast.services import ForecastModelService
        tag_name = get_tag_name(vegetable)
        
        # ComputeMarket変数も含めてモデルを実行
        result = ForecastModelService().run_model(
            tag_name, 
            target_month, 
            vals, 
            compute_market_variables=compute_market_variables
        )
        
        if result:
            selected_vars = []
            if variable_ids:
                selected_vars.append(f"通常変数{len(variable_ids)}個")
            if compute_market_variables:
                selected_vars.append(f"市場変数{len(compute_market_variables)}個")
            var_info = "、".join(selected_vars)
            messages.success(request, f'{vegetable}の{target_month}月モデルを実行しました。（{var_info}）')
        else:
            messages.error(request, 'モデルの実行に失敗しました。')
    except Exception as e:
        messages.error(request, f'エラーが発生しました: {str(e)}')

    # リダイレクト時にモデル管理タブを表示
    redirect_url = request.META.get('HTTP_REFERER', '/')
    if '?' in redirect_url:
        redirect_url += '&tab=model'
    else:
        redirect_url += '?tab=model'
    
    return redirect(redirect_url)

def run_multiple_models(request, vegetable):
    """
    複数月のモデルを一括実行
    """
    if request.method != 'POST':
        messages.error(request, '不正なリクエストです。')
        return redirect(request.META.get('HTTP_REFERER', '/'))

    months = request.POST.getlist('target_months')
    variable_ids = request.POST.getlist('variables')

    if not months or not variable_ids:
        messages.error(request, '対象月と説明変数を選択してください。')
        return redirect(request.META.get('HTTP_REFERER', '/'))

    try:
        # 選択された変数のリストを取得
        variables = ForecastModelVariable.objects.filter(id__in=variable_ids)
        if not variables.exists():
            messages.error(request, '選択された説明変数が見つかりません。')
            return redirect(request.META.get('HTTP_REFERER', '/?tab=model'))

        # 変数名のリストを作成
        variable_names = [var.name for var in variables]

        from forecast.services import ForecastModelService
        service = ForecastModelService()
        success_count = 0
        tag_name = get_tag_name(vegetable)
        
        for month in months:
            if service.run_model(tag_name, int(month), variable_names):
                success_count += 1

        if success_count > 0:
            messages.success(request, f'{vegetable}の{success_count}個のモデルを実行しました。')
        else:
            messages.error(request, 'モデルの実行に失敗しました。')
    except Exception as e:
        messages.error(request, f'エラーが発生しました: {str(e)}')

    # リダイレクト時にモデル管理タブを表示
    redirect_url = request.META.get('HTTP_REFERER', '/')
    if '?' in redirect_url:
        redirect_url += '&tab=model'
    else:
        redirect_url += '?tab=model'
    
    return redirect(redirect_url)
