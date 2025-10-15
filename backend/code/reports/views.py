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
    
# 以下、IngestMarket データを表示するビュー
class IngestCabbageView(generic.ListView):
    template_name = "reports/ingest_cabbage.html"
    context_object_name = "market_data"
    paginate_by = 20  # ページネーション追加
    
    def get_queryset(self):
        # キャベツのデータを取得（vegetable.name = 'キャベツ'）
        vegetable = Vegetable.objects.filter(name='キャベツ').first()
        if vegetable:
            return IngestMarket.objects.filter(vegetable=vegetable).order_by('-target_date')
        return IngestMarket.objects.none()
    
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        vegetable = Vegetable.objects.filter(name='キャベツ').first()
        context['vegetable_name'] = vegetable.name if vegetable else 'キャベツ'
        context['vegetable_code'] = vegetable.code if vegetable else '不明'
        
        # 最新・最古日付の取得
        queryset = self.get_queryset()
        context['market_count'] = queryset.count()
        context['latest_date'] = queryset.first().target_date if queryset.exists() else None
        context['oldest_date'] = queryset.last().target_date if queryset.exists() else None
        
        # JSON データも追加（ページネーション前のクエリセット全体）
        context['market_data_json'] = json.dumps(list(queryset[:100].values()), cls=DjangoJSONEncoder)
        return context

class IngestChineseCabbageView(generic.ListView):
    template_name = "reports/ingest_chinese_cabbage.html"
    context_object_name = "market_data"
    paginate_by = 20  # ページネーション追加
    
    def get_queryset(self):
        # 白菜のデータを取得
        vegetable = Vegetable.objects.filter(name='はくさい').first()
        if vegetable:
            return IngestMarket.objects.filter(vegetable=vegetable).order_by('-target_date')
        return IngestMarket.objects.none()
    
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        vegetable = Vegetable.objects.filter(name='はくさい').first()
        context['vegetable_name'] = vegetable.name if vegetable else 'はくさい'
        context['vegetable_code'] = vegetable.code if vegetable else '不明'
        
        # 最新・最古日付の取得
        queryset = self.get_queryset()
        context['market_count'] = queryset.count()
        context['latest_date'] = queryset.first().target_date if queryset.exists() else None
        context['oldest_date'] = queryset.last().target_date if queryset.exists() else None
        
        # JSON データも追加（ページネーション前のクエリセット全体）
        context['market_data_json'] = json.dumps(list(queryset[:100].values()), cls=DjangoJSONEncoder)
        return context

class IngestRadishView(generic.ListView):
    template_name = "reports/ingest_radish.html"
    context_object_name = "market_data"
    paginate_by = 20  # ページネーション追加
    
    def get_queryset(self):
        # 大根のデータを取得
        vegetable = Vegetable.objects.filter(name='だいこん').first()
        if vegetable:
            return IngestMarket.objects.filter(vegetable=vegetable).order_by('-target_date')
        return IngestMarket.objects.none()
    
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        vegetable = Vegetable.objects.filter(name='だいこん').first()
        context['vegetable_name'] = vegetable.name if vegetable else 'だいこん'
        context['vegetable_code'] = vegetable.code if vegetable else '不明'
        
        # 最新・最古日付の取得
        queryset = self.get_queryset()
        context['market_count'] = queryset.count()
        context['latest_date'] = queryset.first().target_date if queryset.exists() else None
        context['oldest_date'] = queryset.last().target_date if queryset.exists() else None
        
        # JSON データも追加（ページネーション前のクエリセット全体）
        context['market_data_json'] = json.dumps(list(queryset[:100].values()), cls=DjangoJSONEncoder)
        return context

class IngestOnionView(generic.ListView):
    template_name = "reports/ingest_onion.html"
    context_object_name = "market_data"
    paginate_by = 20  # ページネーション追加
    
    def get_queryset(self):
        # 玉ねぎのデータを取得
        vegetable = Vegetable.objects.filter(name='たまねぎ').first()
        if vegetable:
            return IngestMarket.objects.filter(vegetable=vegetable).order_by('-target_date')
        return IngestMarket.objects.none()
    
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        vegetable = Vegetable.objects.filter(name='たまねぎ').first()
        context['vegetable_name'] = vegetable.name if vegetable else 'たまねぎ'
        context['vegetable_code'] = vegetable.code if vegetable else '不明'
        
        # 最新・最古日付の取得
        queryset = self.get_queryset()
        context['market_count'] = queryset.count()
        context['latest_date'] = queryset.first().target_date if queryset.exists() else None
        context['oldest_date'] = queryset.last().target_date if queryset.exists() else None
        
        # JSON データも追加（ページネーション前のクエリセット全体）
        context['market_data_json'] = json.dumps(list(queryset[:100].values()), cls=DjangoJSONEncoder)
        return context

class IngestPotatoView(generic.ListView):
    template_name = "reports/ingest_potato.html"
    context_object_name = "market_data"
    paginate_by = 20  # ページネーション追加
    
    def get_queryset(self):
        # じゃがいものデータを取得
        vegetable = Vegetable.objects.filter(name='ばれいしょ').first()
        if vegetable:
            return IngestMarket.objects.filter(vegetable=vegetable).order_by('-target_date')
        return IngestMarket.objects.none()
    
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        vegetable = Vegetable.objects.filter(name='ばれいしょ').first()
        context['vegetable_name'] = vegetable.name if vegetable else 'ばれいしょ'
        context['vegetable_code'] = vegetable.code if vegetable else '不明'
        
        # 最新・最古日付の取得
        queryset = self.get_queryset()
        context['market_count'] = queryset.count()
        context['latest_date'] = queryset.first().target_date if queryset.exists() else None
        context['oldest_date'] = queryset.last().target_date if queryset.exists() else None
        
        # JSON データも追加（ページネーション前のクエリセット全体）
        context['market_data_json'] = json.dumps(list(queryset[:100].values()), cls=DjangoJSONEncoder)
        return context

class IngestTomatoView(generic.ListView):
    template_name = "reports/ingest_tomato.html"
    context_object_name = "market_data"
    paginate_by = 20  # ページネーション追加
    
    def get_queryset(self):
        # トマトのデータを取得
        vegetable = Vegetable.objects.filter(name='トマト').first()
        if vegetable:
            return IngestMarket.objects.filter(vegetable=vegetable).order_by('-target_date')
        return IngestMarket.objects.none()
    
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        vegetable = Vegetable.objects.filter(name='トマト').first()
        context['vegetable_name'] = vegetable.name if vegetable else 'トマト'
        context['vegetable_code'] = vegetable.code if vegetable else '不明'
        
        # 最新・最古日付の取得
        queryset = self.get_queryset()
        context['market_count'] = queryset.count()
        context['latest_date'] = queryset.first().target_date if queryset.exists() else None
        context['oldest_date'] = queryset.last().target_date if queryset.exists() else None
        
        # JSON データも追加（ページネーション前のクエリセット全体）
        context['market_data_json'] = json.dumps(list(queryset[:100].values()), cls=DjangoJSONEncoder)
        return context

class IngestCucumberView(generic.ListView):
    template_name = "reports/ingest_cucumber.html"
    context_object_name = "market_data"
    paginate_by = 20  # ページネーション追加
    
    def get_queryset(self):
        # きゅうりのデータを取得
        vegetable = Vegetable.objects.filter(name='きゅうり').first()
        if vegetable:
            return IngestMarket.objects.filter(vegetable=vegetable).order_by('-target_date')
        return IngestMarket.objects.none()
    
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        vegetable = Vegetable.objects.filter(name='きゅうり').first()
        context['vegetable_name'] = vegetable.name if vegetable else 'きゅうり'
        context['vegetable_code'] = vegetable.code if vegetable else '不明'
        
        # 最新・最古日付の取得
        queryset = self.get_queryset()
        context['market_count'] = queryset.count()
        context['latest_date'] = queryset.first().target_date if queryset.exists() else None
        context['oldest_date'] = queryset.last().target_date if queryset.exists() else None
        
        # JSON データも追加（ページネーション前のクエリセット全体）
        context['market_data_json'] = json.dumps(list(queryset[:100].values()), cls=DjangoJSONEncoder)
        return context

class IngestEggplantView(generic.ListView):
    template_name = "reports/ingest_eggplant.html"
    context_object_name = "market_data"
    paginate_by = 20  # ページネーション追加
    
    def get_queryset(self):
        # なすのデータを取得
        vegetable = Vegetable.objects.filter(name='なす').first()
        if vegetable:
            return IngestMarket.objects.filter(vegetable=vegetable).order_by('-target_date')
        return IngestMarket.objects.none()
    
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        vegetable = Vegetable.objects.filter(name='なす').first()
        context['vegetable_name'] = vegetable.name if vegetable else 'なす'
        context['vegetable_code'] = vegetable.code if vegetable else '不明'
        
        # 最新・最古日付の取得
        queryset = self.get_queryset()
        context['market_count'] = queryset.count()
        context['latest_date'] = queryset.first().target_date if queryset.exists() else None
        context['oldest_date'] = queryset.last().target_date if queryset.exists() else None
        
        # JSON データも追加（ページネーション前のクエリセット全体）
        context['market_data_json'] = json.dumps(list(queryset[:100].values()), cls=DjangoJSONEncoder)
        return context