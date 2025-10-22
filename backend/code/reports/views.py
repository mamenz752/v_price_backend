import json
from django.core.serializers.json import DjangoJSONEncoder
from django.shortcuts import render
from django.views import generic
# CalcMarketモデルは存在しないため、インポート文を削除
# from .models import CalcMarket
from ingest.models import IngestMarket, Vegetable

# Create your views here.
class IndexView(generic.TemplateView):
    template_name = "reports/index.html"
    
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        # IngestMarketのデータを使用
        markets = IngestMarket.objects.all()[:100]  # パフォーマンスのため上限を設定
        context['calc_markets'] = markets
        context['calc_markets_json'] = json.dumps(list(markets.values()), cls=DjangoJSONEncoder)
        return context
    
class ChineseCabbageView(generic.TemplateView):
    template_name = "reports/chinese_cabbage.html"
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        # 白菜のデータを取得
        vegetable = Vegetable.objects.filter(name='はくさい').first()
        if vegetable:
            markets = IngestMarket.objects.filter(vegetable=vegetable)[:100]
            context['calc_markets_json'] = json.dumps(list(markets.values()), cls=DjangoJSONEncoder)
        else:
            context['calc_markets_json'] = json.dumps([], cls=DjangoJSONEncoder)
        return context
    
class CabbageView(generic.TemplateView):
    template_name = "reports/cabbage.html"
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        # キャベツのデータを取得
        vegetable = Vegetable.objects.filter(name='キャベツ').first()
        if vegetable:
            markets = IngestMarket.objects.filter(vegetable=vegetable)[:100]
            context['calc_markets_json'] = json.dumps(list(markets.values()), cls=DjangoJSONEncoder)
        else:
            context['calc_markets_json'] = json.dumps([], cls=DjangoJSONEncoder)
        return context
    
class CucumberView(generic.TemplateView):
    template_name = "reports/cucumber.html"
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        # きゅうりのデータを取得
        vegetable = Vegetable.objects.filter(name='きゅうり').first()
        if vegetable:
            markets = IngestMarket.objects.filter(vegetable=vegetable)[:100]
            context['calc_markets_json'] = json.dumps(list(markets.values()), cls=DjangoJSONEncoder)
        else:
            context['calc_markets_json'] = json.dumps([], cls=DjangoJSONEncoder)
        return context
    
class TomatoView(generic.TemplateView):
    template_name = "reports/tomato.html"
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        # トマトのデータを取得
        vegetable = Vegetable.objects.filter(name='トマト').first()
        if vegetable:
            markets = IngestMarket.objects.filter(vegetable=vegetable)[:100]
            context['calc_markets_json'] = json.dumps(list(markets.values()), cls=DjangoJSONEncoder)
        else:
            context['calc_markets_json'] = json.dumps([], cls=DjangoJSONEncoder)
        return context
    
class EggplantView(generic.TemplateView):
    template_name = "reports/eggplant.html"
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        # なすのデータを取得
        vegetable = Vegetable.objects.filter(name='なす').first()
        if vegetable:
            markets = IngestMarket.objects.filter(vegetable=vegetable)[:100]
            context['calc_markets_json'] = json.dumps(list(markets.values()), cls=DjangoJSONEncoder)
        else:
            context['calc_markets_json'] = json.dumps([], cls=DjangoJSONEncoder)
        return context
    
class RadishView(generic.TemplateView):
    template_name = "reports/radish.html"
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        # 大根のデータを取得
        vegetable = Vegetable.objects.filter(name='だいこん').first()
        if vegetable:
            markets = IngestMarket.objects.filter(vegetable=vegetable)[:100]
            context['calc_markets_json'] = json.dumps(list(markets.values()), cls=DjangoJSONEncoder)
        else:
            context['calc_markets_json'] = json.dumps([], cls=DjangoJSONEncoder)
        return context
    
class PotatoView(generic.TemplateView):
    template_name = "reports/potato.html"
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        # じゃがいものデータを取得
        vegetable = Vegetable.objects.filter(name='ばれいしょ').first()
        if vegetable:
            markets = IngestMarket.objects.filter(vegetable=vegetable)[:100]
            context['calc_markets_json'] = json.dumps(list(markets.values()), cls=DjangoJSONEncoder)
        else:
            context['calc_markets_json'] = json.dumps([], cls=DjangoJSONEncoder)
        return context
    
class OnionView(generic.TemplateView):
    template_name = "reports/onion.html"
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        # 玉ねぎのデータを取得
        vegetable = Vegetable.objects.filter(name='たまねぎ').first()
        if vegetable:
            markets = IngestMarket.objects.filter(vegetable=vegetable)[:100]
            context['calc_markets_json'] = json.dumps(list(markets.values()), cls=DjangoJSONEncoder)
        else:
            context['calc_markets_json'] = json.dumps([], cls=DjangoJSONEncoder)
        return context
    
# 生データ表示機能は削除されました