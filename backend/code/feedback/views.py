from django.shortcuts import render
from django.views.generic import TemplateView

class IndexView(TemplateView):
    """
    フィードバック機能のインデックスページを表示するビュー
    """
    template_name = 'feedback/feedback.html'
    
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['title'] = 'フィードバック'
        return context

class ChineseCabbageView(TemplateView):
    """
    白菜のフィードバックページを表示するビュー
    """
    template_name = 'feedback/chinese_cabbage.html'
    
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['title'] = '白菜フィードバック'
        context['vegetable'] = '白菜'
        return context

class CabbageView(TemplateView):
    """
    キャベツのフィードバックページを表示するビュー
    """
    template_name = 'feedback/cabbage.html'
    
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['title'] = 'キャベツフィードバック'
        context['vegetable'] = 'キャベツ'
        return context

class CucumberView(TemplateView):
    """
    きゅうりのフィードバックページを表示するビュー
    """
    template_name = 'feedback/cucumber.html'
    
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['title'] = 'きゅうりフィードバック'
        context['vegetable'] = 'きゅうり'
        return context

class TomatoView(TemplateView):
    """
    トマトのフィードバックページを表示するビュー
    """
    template_name = 'feedback/tomato.html'
    
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['title'] = 'トマトフィードバック'
        context['vegetable'] = 'トマト'
        return context

class EggplantView(TemplateView):
    """
    なすのフィードバックページを表示するビュー
    """
    template_name = 'feedback/eggplant.html'
    
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['title'] = 'なすフィードバック'
        context['vegetable'] = 'なす'
        return context

class RadishView(TemplateView):
    """
    大根のフィードバックページを表示するビュー
    """
    template_name = 'feedback/radish.html'
    
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['title'] = '大根フィードバック'
        context['vegetable'] = '大根'
        return context

class PotatoView(TemplateView):
    """
    じゃがいものフィードバックページを表示するビュー
    """
    template_name = 'feedback/potato.html'
    
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['title'] = 'じゃがいもフィードバック'
        context['vegetable'] = 'じゃがいも'
        return context

class OnionView(TemplateView):
    """
    玉ねぎのフィードバックページを表示するビュー
    """
    template_name = 'feedback/onion.html'
    
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['title'] = '玉ねぎフィードバック'
        context['vegetable'] = '玉ねぎ'
        return context
