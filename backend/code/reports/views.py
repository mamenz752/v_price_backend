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

    # recently price data
    qs = IngestMarket.objects.filter(vegetable=vegetable).order_by('-target_date')
    latest = qs.first()

    source_price = latest.source_price if latest and hasattr(latest, 'source_price') else None
    volume = latest.volume if latest and hasattr(latest, 'volume') else None

    markets = list(qs[:14].values())  # リストとして評価

    # predict price data

    # season price data
    # FIXME: .today()に変更
    today = date(2025, 7, 31)
    one_year_ago = today - timedelta(days=365)
    two_years_ago = today - timedelta(days=365*2)
    five_years_ago = today - timedelta(days=365*5)

    # 現在のシーズンデータ（過去1年）
    current_season = IngestMarket.objects.filter(
        vegetable=vegetable,
        target_date__gte=one_year_ago,
        target_date__lte=today
    ).order_by('target_date')

    # 前年のシーズンデータ
    last_season = IngestMarket.objects.filter(
        vegetable=vegetable,
        target_date__gte=two_years_ago,
        target_date__lte=one_year_ago
    ).order_by('target_date')

    # 過去5年のデータ
    five_year_data = IngestMarket.objects.filter(
        vegetable=vegetable,
        target_date__gte=five_years_ago,
        target_date__lte=today
    ).order_by('target_date')

    # 各期間のデータをシリーズ化
    year_series = []
    
    # 現在シーズン
    for market in current_season:
        year_series.append({
            'target_date': market.target_date,
            'current_season_price': market.source_price,
            'last_season_price': None,
            'five_year_avg_price': None
        })

    # 前年のデータを対応する日付にマッピング
    last_season_dict = {
        market.target_date.strftime('%m-%d'): market.source_price 
        for market in last_season
    }

    # 5年データから日付ごとの平均を計算
    five_year_avg = {}
    five_year_counts = {}
    for market in five_year_data:
        date_key = market.target_date.strftime('%m-%d')
        if market.source_price is not None:
            if date_key not in five_year_avg:
                five_year_avg[date_key] = market.source_price
                five_year_counts[date_key] = 1
            else:
                five_year_avg[date_key] += market.source_price
                five_year_counts[date_key] += 1

    # 平均値を計算
    for key in five_year_avg:
        five_year_avg[key] = five_year_avg[key] / five_year_counts[key]

    # シリーズにデータを結合
    for item in year_series:
        date_key = item['target_date'].strftime('%m-%d')
        # 前年同日のデータを追加
        item['last_season_price'] = last_season_dict.get(date_key)
        # 5年平均を追加
        item['five_year_avg_price'] = five_year_avg.get(date_key)

    # 最新の日付から年月を取得
    latest_date = latest.target_date if latest else None
    if latest_date:
        current_year = latest_date.year
        current_month = latest_date.month
        first_half = "前半" if latest_date.day <= 15 else "後半"
        display_date = f"{current_year}年{current_month}月{first_half}"
    else:
        display_date = "データなし"

    context.update({
        'recent_date': latest_date,
        'source_price': source_price,
        'volume': volume,
        'display_date': display_date,  # 表示用の日付文字列を追加
        'recently_price_data': json.dumps(markets, cls=DjangoJSONEncoder),
        'predict_price_data': json.dumps(year_series, cls=DjangoJSONEncoder),
        'season_price_data': json.dumps(year_series, cls=DjangoJSONEncoder),
        'year_price_data': json.dumps(year_series, cls=DjangoJSONEncoder),
        'markets': markets
    })

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