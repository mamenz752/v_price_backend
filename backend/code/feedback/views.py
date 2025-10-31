from django.shortcuts import render
from django.views.generic import TemplateView
from .services import FeedbackService

class IndexView(TemplateView):
    """
    フィードバック機能のインデックスページを表示するビュー
    """
    template_name = 'feedback/feedback.html'
    
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['title'] = 'フィードバック'
        return context

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

        context.update({
            'months': range(1, 13),
            'current_month': current_month,
            'metrics': metrics or self.get_default_metrics(),
            'evaluation': evaluation or self.get_default_evaluation(current_month),
            'variables': variables or self.get_default_variables(),
            'accuracy_data': accuracy_data or self.get_default_accuracy_data(current_month),
        })
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
    vegetable_name = '大根'

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
    vegetable_name = '玉ねぎ'
