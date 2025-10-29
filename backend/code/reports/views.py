import json
from django.core.serializers.json import DjangoJSONEncoder
from django.shortcuts import render
from django.views import generic
# CalcMarketモデルは存在しないため、インポート文を削除
# from .models import CalcMarket
from ingest.models import IngestMarket, Vegetable
from compute.models import ComputeMarket
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
    
    # 最新のデータと前回のデータを取得
    latest = None
    previous = None
    source_price = None
    previous_price = None
    volume = None
    price_diff = None
    price_diff_ratio = None
    latest_date = None
    last_year_diff = None
    last_year_ratio = None
    two_years_diff = None
    two_years_ratio = None
    avg_diff = None
    avg_ratio = None
    
    if qs.exists():
        latest = qs.first()
        latest_date = latest.target_date
        source_price = latest.source_price
        volume = latest.volume
        
        # 2番目のレコードを取得
        if qs.count() > 1:
            previous = qs[1]
            previous_price = previous.source_price
            
            # 前市比の計算
            if source_price is not None and previous_price is not None and previous_price != 0:
                price_diff = source_price - previous_price
                price_diff_ratio = round((price_diff / previous_price) * 100, 1)

        # 前年、前々年、平年比の計算
        if latest_date:
            # 前年のデータを取得（1年前から2年前の間のデータ）
            one_year_ago = latest_date - timedelta(days=365)
            two_years_ago = latest_date - timedelta(days=365*2)
            
            print(f"Debug: latest_date={latest_date}")
            print(f"Debug: source_price={source_price}")
            print(f"Debug: one_year_ago={one_year_ago}")
            print(f"Debug: two_years_ago={two_years_ago}")
            
            last_year_market = IngestMarket.objects.filter(
                vegetable=vegetable,
                target_date__lt=one_year_ago,
                target_date__gte=two_years_ago
            ).order_by('-target_date').first()

            print(f"Debug: last_year_market={last_year_market}")
            if last_year_market:
                print(f"Debug: last_year_market.target_date={last_year_market.target_date}")
                print(f"Debug: last_year_market.source_price={last_year_market.source_price}")

            if last_year_market and source_price is not None and last_year_market.source_price is not None:
                last_year_diff = source_price - last_year_market.source_price
                if last_year_market.source_price != 0:
                    last_year_ratio = round((last_year_diff / last_year_market.source_price) * 100, 1)
                    print(f"Debug: Calculated last_year - diff={last_year_diff}, ratio={last_year_ratio}")
            else:
                print("Debug: Could not calculate last_year_diff because:")
                print(f" - last_year_market exists: {last_year_market is not None}")
                print(f" - source_price exists: {source_price is not None}")
                print(f" - last_year_market.source_price exists: {last_year_market.source_price if last_year_market else None}")

            # 2年前より前の最新データを取得
            two_years_ago = latest_date - timedelta(days=365*2)
            two_years_market = IngestMarket.objects.filter(
                vegetable=vegetable,
                target_date__lte=two_years_ago
            ).order_by('-target_date').first()

            print(f"Debug: two_years_ago={two_years_ago}")
            print(f"Debug: two_years_market={two_years_market}")
            print(f"Debug: two_years_market source_price={two_years_market.source_price if two_years_market else None}")

            if two_years_market and source_price is not None and two_years_market.source_price is not None:
                two_years_diff = source_price - two_years_market.source_price
                if two_years_market.source_price != 0:
                    two_years_ratio = round((two_years_diff / two_years_market.source_price) * 100, 1)
                    print(f"Debug: Calculated two_years - diff={two_years_diff}, ratio={two_years_ratio}")

            # 過去5年の同日データを取得して平均を計算
            five_years_markets = IngestMarket.objects.filter(
                vegetable=vegetable,
                target_date__month=latest_date.month,
                target_date__day=latest_date.day,
                target_date__year__gte=latest_date.year - 5,
                target_date__year__lt=latest_date.year
            )

            if five_years_markets.exists() and source_price is not None:
                valid_prices = [m.source_price for m in five_years_markets if m.source_price is not None]
                if valid_prices:
                    avg_price = sum(valid_prices) / len(valid_prices)
                    avg_diff = source_price - avg_price
                    if avg_price != 0:
                        avg_ratio = round((avg_diff / avg_price) * 100, 1)
    
    # デバッグ用にprint文を追加
    print(f"Debug: source_price={source_price}, previous_price={previous_price}")
    print(f"Debug: price_diff={price_diff}, price_diff_ratio={price_diff_ratio}")
    print(f"Debug: last_year_diff={last_year_diff}, last_year_ratio={last_year_ratio}")
    print(f"Debug: two_years_diff={two_years_diff}, two_years_ratio={two_years_ratio}")
    print(f"Debug: avg_diff={avg_diff}, avg_ratio={avg_ratio}")
    
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

    # 1年前、2年前、5年平均の比較データを取得
    if latest_date:
        # 1年前のデータを取得（前後3日の範囲で検索）
        last_year_date = latest_date.replace(year=latest_date.year - 1)
        last_year_start = last_year_date - timedelta(days=3)
        last_year_end = last_year_date + timedelta(days=3)
        
        last_year_data = IngestMarket.objects.filter(
            vegetable=vegetable,
            target_date__gte=last_year_start,
            target_date__lte=last_year_end
        ).order_by('-target_date').first()

        print(f"Debug: Looking for last year data between {last_year_start} and {last_year_end}")
        print(f"Debug: Found last_year_data: {last_year_data}")
        if last_year_data:
            print(f"Debug: last_year_data.target_date = {last_year_data.target_date}")
            print(f"Debug: last_year_data.source_price = {last_year_data.source_price}")

        # 2年前のデータを取得（前後3日の範囲で検索）
        two_years_date = latest_date.replace(year=latest_date.year - 2)
        two_years_start = two_years_date - timedelta(days=3)
        two_years_end = two_years_date + timedelta(days=3)
        
        two_years_data = IngestMarket.objects.filter(
            vegetable=vegetable,
            target_date__gte=two_years_start,
            target_date__lte=two_years_end
        ).order_by('-target_date').first()

        print(f"Debug: Looking for two years data between {two_years_start} and {two_years_end}")
        print(f"Debug: Found two_years_data: {two_years_data}")
        if two_years_data:
            print(f"Debug: two_years_data.target_date = {two_years_data.target_date}")
            print(f"Debug: two_years_data.source_price = {two_years_data.source_price}")

        # 過去5年の同時期データを取得（前後3日の範囲で検索）
        five_years_data = []
        for year in range(latest_date.year - 5, latest_date.year):
            date_point = latest_date.replace(year=year)
            start_date = date_point - timedelta(days=3)
            end_date = date_point + timedelta(days=3)
            
            year_data = IngestMarket.objects.filter(
                vegetable=vegetable,
                target_date__gte=start_date,
                target_date__lte=end_date
            ).order_by('-target_date').first()
            
            if year_data and year_data.source_price is not None:
                five_years_data.append(year_data)

        # 平年価格（5年平均）を計算
        avg_price = None
        if five_years_data:
            valid_prices = [market.source_price for market in five_years_data]
            if valid_prices:
                avg_price = sum(valid_prices) / len(valid_prices)
                print(f"Debug: Calculated average price from {len(valid_prices)} years: {avg_price}")

        # 前年比の計算
        last_year_diff = None
        last_year_ratio = None
        if source_price is not None and last_year_data and last_year_data.source_price is not None:
            last_year_diff = source_price - last_year_data.source_price
            if last_year_data.source_price != 0:
                last_year_ratio = round((last_year_diff / last_year_data.source_price) * 100, 1)
                print(f"Debug: Calculated last year comparison - diff={last_year_diff}, ratio={last_year_ratio}")

        # 前々年比の計算
        two_years_diff = None
        two_years_ratio = None
        if source_price is not None and two_years_data and two_years_data.source_price is not None:
            two_years_diff = source_price - two_years_data.source_price
            if two_years_data.source_price != 0:
                two_years_ratio = round((two_years_diff / two_years_data.source_price) * 100, 1)
                print(f"Debug: Calculated two years comparison - diff={two_years_diff}, ratio={two_years_ratio}")

        # 平年比の計算
        avg_diff = None
        avg_ratio = None
        if source_price is not None and avg_price is not None and avg_price != 0:
            avg_diff = source_price - avg_price
            avg_ratio = round((avg_diff / avg_price) * 100, 1)
            print(f"Debug: Calculated average comparison - diff={avg_diff}, ratio={avg_ratio}")

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

    # 最新の価格見通し
    if latest_date:
        current_half = "前半" if latest_date.day <= 15 else "後半"
        latest_market = ComputeMarket.objects.filter(
            vegetable=vegetable,
            target_year=latest_date.year,
            target_month=latest_date.month,
            target_half=current_half
        ).first()

        print(f"Debug: Looking for trend with year={latest_date.year}, month={latest_date.month}, half={current_half}")
        print(f"Debug: Found latest_market: {latest_market}")
        
        if latest_market:
            latest_trend = latest_market.trend
            print(f"Debug: Found trend: {latest_trend}")
        else:
            latest_trend = "データなし"
            print("Debug: No trend data found")
    else:
        latest_trend = "データなし"
        print("Debug: No latest date available")


    # デバッグ情報の出力
    print(f"Debug: Context Values:")
    print(f"source_price: {source_price}")
    print(f"last_year_diff: {last_year_diff}")
    print(f"last_year_ratio: {last_year_ratio}")
    print(f"two_years_diff: {two_years_diff}")
    print(f"two_years_ratio: {two_years_ratio}")
    print(f"avg_diff: {avg_diff}")
    print(f"avg_ratio: {avg_ratio}")

    context.update({
        'recent_date': latest_date,
        'source_price': round(source_price) if source_price is not None else None,
        'price_diff': round(price_diff) if price_diff is not None else None,
        'price_diff_ratio': price_diff_ratio,
        'volume': volume,
        'display_date': display_date,  # 表示用の日付文字列を追加
        'latest_trend': latest_trend,  # データがない場合は "データなし" が設定される
        # 前年比データ
        'last_year_diff': round(last_year_diff) if last_year_diff is not None else None,
        'last_year_ratio': last_year_ratio,
        # 前々年比データ
        'two_years_diff': round(two_years_diff) if two_years_diff is not None else None,
        'two_years_ratio': two_years_ratio,
        # 平年比データ
        'avg_diff': round(avg_diff) if avg_diff is not None else None,
        'avg_ratio': avg_ratio,
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