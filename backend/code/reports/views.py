import json
from django.core.serializers.json import DjangoJSONEncoder
from django.shortcuts import render
from django.views import generic
# CalcMarketモデルは存在しないため、インポート文を削除
# from .models import CalcMarket
from ingest.models import IngestMarket, Vegetable
from datetime import date, timedelta


def _select_price(market: IngestMarket):
    """平均価格を優先的に取得し、なければ中値、ソース価格を順に返す"""
    if not market:
        return None
    return market.average_price or market.medium_price or market.source_price


def _veg_context(veg_lookup_name: str, display_name: str):
    """指定した野菜名で最新データを取得し、テンプレート用のコンテキスト辞書を返す"""
    context = {}
    vegetable = Vegetable.objects.filter(name=veg_lookup_name).first()
    context['vegetable_name'] = display_name
    if not vegetable:
        # 空のデータを返す
        context.update({
            'recent_date': None,
            'recent_price': None,
            'price_change': None,
            # 'price_change_pct': None,
            'volume': None,
            'recently_price_data': json.dumps([], cls=DjangoJSONEncoder),
        })
        return context

    qs = IngestMarket.objects.filter(vegetable=vegetable).order_by('-target_date')
    latest = qs.first()

    source_price = latest.source_price if latest and hasattr(latest, 'source_price') else None
    volume = latest.volume if latest and hasattr(latest, 'volume') else None

    markets = qs[:14]
    context.update({
        'recent_date': latest.target_date if latest else None,
        'source_price': source_price,
        'volume': volume,
        'recently_price_data': json.dumps(list(markets.values()), cls=DjangoJSONEncoder),
    })
    # prepare 2-week series: arrival (bar) and price (line)
    # FIXME: 以下コメントアウトを解除
    # two_weeks_ago = date.today() - timedelta(days=14)
    # FIXME: 以下コードを削除
    two_weeks_ago = date(2025, 7, 31) - timedelta(days=14)

    qs_2w = IngestMarket.objects.filter(vegetable=vegetable, target_date__gte=two_weeks_ago).order_by('target_date')
    series = []
    for m in qs_2w:
        series.append({
            'target_date': m.target_date.isoformat(),
            'volume': m.volume,
            # Use source_price specifically for the 2-week chart as requested
            'price': m.source_price,
            'volume': m.volume,
        })
    context['recent_2weeks_json'] = json.dumps(series, cls=DjangoJSONEncoder)

    return context

# Create your views here.
class IndexView(generic.TemplateView):
    template_name = "reports/index.html"
    
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        # IngestMarketのデータを使用
        markets = IngestMarket.objects.all()[:14]  # パフォーマンスのため上限を設定
        context['recently_price'] = markets
        context['recently_price_data'] = json.dumps(list(markets.values()), cls=DjangoJSONEncoder)
        return context
    
class ChineseCabbageView(generic.TemplateView):
    template_name = "reports/chinese_cabbage.html"
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context.update(_veg_context('はくさい', '白菜'))
        return context
    
class CabbageView(generic.TemplateView):
    template_name = "reports/cabbage.html"
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context.update(_veg_context('キャベツ', 'キャベツ'))
        return context
    
class CucumberView(generic.TemplateView):
    template_name = "reports/cucumber.html"
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context.update(_veg_context('きゅうり', 'きゅうり'))
        return context
    
class TomatoView(generic.TemplateView):
    template_name = "reports/tomato.html"
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context.update(_veg_context('トマト', 'トマト'))
        return context
    
class EggplantView(generic.TemplateView):
    template_name = "reports/eggplant.html"
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context.update(_veg_context('なす', 'なす'))
        return context
    
class RadishView(generic.TemplateView):
    template_name = "reports/radish.html"
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context.update(_veg_context('だいこん', '大根'))
        return context
    
class PotatoView(generic.TemplateView):
    template_name = "reports/potato.html"
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context.update(_veg_context('ばれいしょ', 'ばれいしょ'))
        return context
    
class OnionView(generic.TemplateView):
    template_name = "reports/onion.html"
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context.update(_veg_context('たまねぎ', '玉ねぎ'))
        return context
    
# 生データ表示機能は削除されました