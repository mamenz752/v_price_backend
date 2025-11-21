import pandas as pd
import numpy as np
import logging
from datetime import datetime
from typing import Tuple, Dict, List, Optional
from django.db.models import Q
import calendar
from compute.models import ComputeMarket, ComputeWeather
from forecast.models import ForecastModelKind, ForecastModelVariable, ForecastModelFeatureSet
from ingest.models import Region

# FIXME: モデル「キャベツ春まき」（5月）は既に実行済みか、データがありません

class DataHasher:
    """データフレームのハッシュ値を計算するクラス"""
    def hash_dataframe(self, df: pd.DataFrame) -> str:
        """DataFrameのハッシュ値を計算する"""
        return str(hash(tuple(map(tuple, df.values))))

class TransformRegistry:
    """変換関数のレジストリ"""
    def __init__(self):
        self.transforms = {}
    
    def register(self, name, transform_func):
        """変換関数を登録する"""
        self.transforms[name] = transform_func
        
    def get(self, name):
        """変換関数を取得する"""
        return self.transforms.get(name, lambda x: x)

class FeatureResolver:
    """特徴量の解決を行うクラス"""
    def __init__(self, feature_set):
        self.feature_set = feature_set
        
    def list_features(self):
        """特徴量のリストを返す"""
        # ForecastModelFeatureSetに対応するFeatureListを返す
        variables = []
        if isinstance(self.feature_set, ForecastModelFeatureSet):
            # feature_setに関連する変数を取得
            variables = [self.feature_set.variable]
        elif hasattr(self.feature_set, 'all'):
            # QuerySetの場合は複数の変数を取得
            variables = [fs.variable for fs in self.feature_set.all()]
        
        return variables

class MatrixBuilder:
    """
    前処理済みDB(ComputeMarket/ComputeWeather)から、指定期間・ForecastModelFeatureSetに従った X, y を作る。
    - 欠損は完全ケースで落とす（必要に応じて差し替え可）
    - data_hash は X と y をまとめて作成
    """
    def __init__(self,
                 transform_registry: Optional[TransformRegistry] = None,
                 hasher: Optional[DataHasher] = None) -> None:
        self.transforms = transform_registry or TransformRegistry()
        self.hasher = hasher or DataHasher()

    def build(self, feature_set: ForecastModelFeatureSet, period: str, target_col: str
              ) -> Tuple[pd.DataFrame, pd.Series, pd.DataFrame, str]:
        # 1) 対象期間のレコード取得（必要に応じて既存テーブルへ置換可能）
        qs = (ComputeMarket.objects
              .filter(target_month=period)
              .values("id", "average_price", "source_price", "volume"))
        df = pd.DataFrame.from_records(qs)
        if df.empty:
            raise ValueError(f"No data for period={period}")

        # 2) y（目的変数）- average_priceを使用
        y = pd.to_numeric(df["average_price"], errors="coerce").astype(float)

        # 3) X（説明変数）: FeatureSetに忠実に構築
        resolver = FeatureResolver(feature_set)
        feats = resolver.list_features()

        cols = []
        x_parts = []
        for f in feats:
            cols.append(f.name)
            # 関連する気象データを取得
            weather_data = ComputeWeather.objects.filter(
                target_month=period
            ).values(f.name)
            
            # weather_dataからシリーズを作成
            if weather_data:
                series = pd.Series([wd.get(f.name) for wd in weather_data])
            else:
                series = pd.Series([None] * len(df))
                
            # 変換関数を適用（もし定義されていれば）
            if hasattr(f, 'transform') and f.transform:
                series = self.transforms.get(f.transform).apply(series, **(getattr(f, 'params', {}) or {}))
            
            x_parts.append(series)

        X = pd.concat(x_parts, axis=1)
        X.columns = cols

        # 4) 欠損を除外（完全ケース）
        mask = X.notna().all(axis=1) & y.notna()
        X = X[mask].reset_index(drop=True)
        y = y[mask].reset_index(drop=True)
        raw_df = df.loc[mask, ["id"]].reset_index(drop=True)

        # 5) ハッシュ（重複実行防止）
        hash_input = pd.concat([X, y.rename("__y__")], axis=1)
        data_hash = self.hasher.hash_dataframe(hash_input)

        return X, y, raw_df, data_hash


class ForecastModelDataBuilder:
    """
    予測モデル用のデータセットを構築するクラス
    """
    
    def __init__(self, region_name: str = '広島', config: Dict = None):
        """
        初期化
        Args:
            region_name (str, optional): 対象地域名。デフォルトは'広島'。
            config (Dict): 設定辞書
                - 'historical_years': 過去データの年数（デフォルト5）
                - 'min_required_years': 最低必要年数（デフォルト2）
                - 'base_start_year': 基準開始年（デフォルト2021）
                - 'max_lookback_years': 最大遡及年数（デフォルト10）
        """
        self.region = Region.objects.get(name=region_name)
        self.config = config or {}
        self.historical_years = self.config.get('historical_years', 5)
        self.min_required_years = self.config.get('min_required_years', 2)
        self.base_start_year = self.config.get('base_start_year', 2021)
        self.max_lookback_years = self.config.get('max_lookback_years', 10)
        self.logger = logging.getLogger(__name__ + '.ForecastModelDataBuilder')
    
    def get_model_kind_by_name(self, model_name: str) -> Optional[ForecastModelKind]:
        """
        モデル名からモデル種類を取得
        
        Args:
            model_name (str): モデル名（例: "キャベツ春まき"）
            
        Returns:
            Optional[ForecastModelKind]: モデル種類オブジェクト、見つからない場合はNone
        """
        self.logger.debug(f"モデル種類の検索開始 - model_name='{model_name}'")
        try:
            # 登録済みのすべてのモデル種類を取得して確認
            all_models = ForecastModelKind.objects.all()
            self.logger.debug(f"登録済みモデル一覧: {[model.tag_name for model in all_models]}")
            
            model_kind = ForecastModelKind.objects.get(tag_name=model_name)
            self.logger.debug(f"モデル種類が見つかりました - id={model_kind.id}, tag_name={model_kind.tag_name}")
            return model_kind
        except ForecastModelKind.DoesNotExist:
            self.logger.warning(f"モデル種類 '{model_name}' は見つかりませんでした。")
            return None
    
    def get_weather_data_for_period(self, year: int, month: int, previous_term: int, 
                                   years_back: int = None) -> Dict:
        """
        指定された期間の気象データを取得する（過去N年間の平均値）
        
        Args:
            year (int): 対象年
            month (int): 対象月
            previous_term (int): 何半月前のデータか（半月単位：1=前回半月、2=1ヶ月前前半、3=1ヶ月前後半など）
            years_back (int, optional): 過去何年分のデータを使用するか。Noneの場合は設定値を使用
            
        Returns:
            Dict: 気象データの辞書（過去N年間の平均値）
        """
        if years_back is None:
            years_back = self.historical_years
            
        self.logger.debug(f"気象データ取得開始 - year={year}, month={month}, previous_term={previous_term}, years_back={years_back}")
        
        # previous_termを半月単位で計算（前半=0, 後半=1）
        # 2025年1月前半からprevious_term=3なら -> 2024年11月後半
        current_half_index = 0  # 前半をデフォルトとする
        
        # 現在の月と半期を半月インデックスに変換
        current_half_total = (year * 12 + (month - 1)) * 2 + current_half_index
        
        # previous_termだけ過去に遡る
        target_half_total = current_half_total - previous_term
        
        # 半月インデックスから年月半期に変換
        target_year = target_half_total // 24  # 1年=24半月
        remaining = target_half_total % 24
        target_month = (remaining // 2) + 1
        target_half = '前半' if (remaining % 2 == 0) else '後半'
        
        self.logger.info(f"計算結果 - target_year={target_year}, target_month={target_month}, target_half={target_half}")
        self.logger.debug(f"半月インデックス - current={current_half_total}, target={target_half_total}")
        
        # 過去N年間のデータを取得（target_yearからN年前まで）
        start_year = target_year - (years_back - 1)
        end_year = target_year
        
        self.logger.info(f"気象データ検索範囲: {start_year}年-{end_year}年 {target_month}月{target_half}")
        
        weather_data_list = ComputeWeather.objects.filter(
            region=self.region,
            target_year__gte=start_year,
            target_year__lte=end_year,
            target_month=target_month,
            target_half=target_half
        ).values(
            'target_year', 'max_temp', 'mean_temp', 'min_temp', 
            'sum_precipitation', 'sunshine_duration', 'ave_humidity'
        )
        
        self.logger.info(f"気象データクエリ結果: {len(weather_data_list)}件のデータを取得")
        if weather_data_list:
            years_found = [data['target_year'] for data in weather_data_list]
            self.logger.debug(f"取得した年度: {sorted(years_found)}")
        
        # データが不足している場合は範囲を拡張
        if len(weather_data_list) < self.min_required_years:
            extended_start = max(self.base_start_year, target_year - self.max_lookback_years)
            self.logger.warning(f"データ不足({len(weather_data_list)}件)のため検索範囲を{extended_start}年まで拡張")
            
            weather_data_list = ComputeWeather.objects.filter(
                region=self.region,
                target_year__gte=extended_start,
                target_year__lte=end_year,
                target_month=target_month,
                target_half=target_half
            ).values(
                'target_year', 'max_temp', 'mean_temp', 'min_temp', 
                'sum_precipitation', 'sunshine_duration', 'ave_humidity'
            )
            
            self.logger.info(f"拡張検索結果: {len(weather_data_list)}件のデータを取得")
        
        if not weather_data_list:
            self.logger.error(f"気象データが見つかりません: {start_year}-{end_year}年{target_month}月{target_half}, region={self.region.name}")
            return {}
        
        # 過去5年間の平均値を計算
        import statistics
        
        def safe_mean(values):
            """Noneを除外して平均値を計算"""
            valid_values = [v for v in values if v is not None]
            return statistics.mean(valid_values) if valid_values else None
        
        # 各気象変数の平均値を計算
        max_temps = [data['max_temp'] for data in weather_data_list]
        mean_temps = [data['mean_temp'] for data in weather_data_list]
        min_temps = [data['min_temp'] for data in weather_data_list]
        precipitations = [data['sum_precipitation'] for data in weather_data_list]
        sunshine_durations = [data['sunshine_duration'] for data in weather_data_list]
        humidities = [data['ave_humidity'] for data in weather_data_list]
        
        # 各変数の詳細ログ出力
        self.logger.debug(f"気象データ詳細 ({len(weather_data_list)}年間):")
        self.logger.debug(f"  - max_temp値: {[v for v in max_temps if v is not None]} → 平均: {safe_mean(max_temps)}")
        self.logger.debug(f"  - mean_temp値: {[v for v in mean_temps if v is not None]} → 平均: {safe_mean(mean_temps)}")
        self.logger.debug(f"  - min_temp値: {[v for v in min_temps if v is not None]} → 平均: {safe_mean(min_temps)}")
        self.logger.debug(f"  - precipitation値: {[v for v in precipitations if v is not None]} → 平均: {safe_mean(precipitations)}")
        self.logger.debug(f"  - sunshine値: {[v for v in sunshine_durations if v is not None]} → 平均: {safe_mean(sunshine_durations)}")
        self.logger.debug(f"  - humidity値: {[v for v in humidities if v is not None]} → 平均: {safe_mean(humidities)}")
        
        # 返却する気象データ辞書を構築（過去N年間の平均値）
        result = {
            'year': target_year,
            'month': target_month,
            'half': target_half,
            'max_temp': safe_mean(max_temps),
            'mean_temp': safe_mean(mean_temps),
            'min_temp': safe_mean(min_temps),
            'sum_precipitation': safe_mean(precipitations),
            'sunshine_duration': safe_mean(sunshine_durations),
            'ave_humidity': safe_mean(humidities),
            'data_count': len(weather_data_list),  # 平均計算に使用したデータ件数
            'years_used': years_back  # 使用した年数
        }
        
        self.logger.info(f"気象データ平均値計算完了: {len(weather_data_list)}件のデータから計算")
        
        return result
    
    def get_market_data_for_period(self, year: int, month: int, half: str, vegetable, variable_name: str,
                                 years_back: int = None) -> float:
        """
        指定された期間の市場データを取得する（過去N年間の平均値）
        
        Args:
            year (int): 対象年
            month (int): 対象月
            half (str): 対象半期（'前半' or '後半'）
            vegetable: 野菜オブジェクト
            variable_name (str): 変数名（'prev_price', 'prev_volume', 'years_price', 'years_volume'）
            years_back (int, optional): 過去何年分のデータを使用するか。Noneの場合は設定値を使用
            
        Returns:
            float: 市場データの過去N年間平均値
        """
        if years_back is None:
            years_back = self.historical_years
            
        # 過去N年間のデータを取得
        start_year = year - (years_back - 1)
        end_year = year
        
        self.logger.info(f"市場データ取得開始 - vegetable={vegetable.name}, year={year}, month={month}, half={half}, variable={variable_name}")
        self.logger.debug(f"過去{years_back}年間対象期間: {start_year}年-{end_year}年")
        
        market_data_list = ComputeMarket.objects.filter(
            vegetable=vegetable,
            region=self.region,
            target_year__gte=start_year,
            target_year__lte=end_year,
            target_month=month,
            target_half=half
        )
        
        self.logger.info(f"市場データクエリ結果: {market_data_list.count()}件のデータを取得")
        if market_data_list.exists():
            years_found = list(market_data_list.values_list('target_year', flat=True))
            self.logger.debug(f"取得した年度: {sorted(years_found)}")
        
        # データが不足している場合は範囲を拡張
        if market_data_list.count() < self.min_required_years:
            extended_start = max(self.base_start_year, year - self.max_lookback_years)
            self.logger.warning(f"市場データ不足({market_data_list.count()}件)のため検索範囲を{extended_start}年まで拡張")
            
            market_data_list = ComputeMarket.objects.filter(
                vegetable=vegetable,
                region=self.region,
                target_year__gte=extended_start,
                target_year__lte=end_year,
                target_month=month,
                target_half=half
            )
            
            self.logger.info(f"拡張検索結果: {market_data_list.count()}件のデータを取得")
        
        if not market_data_list:
            self.logger.error(f"市場データが見つかりません: {start_year}-{end_year}年{month}月{half}, vegetable={vegetable.name}")
            return None
        
        # 指定された変数の値を取得
        values = []
        for market_data in market_data_list:
            if hasattr(market_data, variable_name):
                value = getattr(market_data, variable_name, None)
                if value is not None:
                    values.append(value)
        
        self.logger.debug(f"{variable_name}の生データ: {values}")
        
        if not values:
            self.logger.warning(f"{variable_name}の有効なデータが見つかりませんでした")
            return None
        
        # 平均値を計算
        import statistics
        average_value = statistics.mean(values)
        
        self.logger.info(f"★{variable_name}の過去{years_back}年間平均値: {average_value:.2f} (データ件数: {len(values)})★")
        self.logger.debug(f"使用した年度とデータ: {list(zip([d.target_year for d in market_data_list if getattr(d, variable_name, None) is not None], values))}")
        
        return average_value
    
    def build_feature_dataset(self, model_name: str, target_month: int, year: int = None) -> pd.DataFrame:
        """
        モデル名と対象月に基づいて特徴量データセットを構築する
        
        Args:
            model_name (str): モデル名（例: "キャベツ春まき"）
            target_month (int): 対象月（1〜12）
            year (int, optional): 対象年。指定しない場合は現在の年
            
        Returns:
            pd.DataFrame: 構築された特徴量データセット
        """
        # 年が指定されていない場合は現在の年を使用
        if year is None:
            year = datetime.now().year
        
        # モデル種類を取得
        model_kind = self.get_model_kind_by_name(model_name)
        if not model_kind:
            return pd.DataFrame()
        
        # 対象月に関連する特徴セットを取得
        feature_sets = ForecastModelFeatureSet.objects.filter(
            model_kind=model_kind,
            target_month=target_month
        ).select_related('variable')
        
        if not feature_sets.exists():
            self.logger.warning(f"対象月 {target_month} に関連する特徴セットが見つかりませんでした。")
            return pd.DataFrame()
        
        # 特徴量データを収集
        features_data = []
        
        for feature_set in feature_sets:
            variable = feature_set.variable
            variable_name = variable.name
            previous_term = variable.previous_term
            
            # 変数がWeather関連の場合（過去5年間の平均値を使用）
            if variable_name in ['max_temp', 'mean_temp', 'min_temp', 'sum_precipitation', 'sunshine_duration', 'ave_humidity']:
                self.logger.info(f"★気象変数 {variable_name} (previous_term={previous_term}) の過去5年間平均値を取得中★")
                weather_data = self.get_weather_data_for_period(year, target_month, previous_term)
                if weather_data:
                    feature_value = weather_data.get(variable_name)  # 既に過去5年間の平均値
                    self.logger.info(f"★気象変数 {variable_name} の特徴量値: {feature_value}★")
                    
                    # 特徴量データに追加
                    features_data.append({
                        'variable_name': variable_name,
                        'previous_term': previous_term,
                        'value': feature_value,
                        'year': weather_data['year'],
                        'month': weather_data['month'],
                        'half': weather_data['half']
                    })
            
            # 市場データ関連の場合（過去5年間の平均値を使用）
            elif variable_name in ['prev_price', 'prev_volume', 'years_price', 'years_volume']:
                self.logger.info(f"★市場変数 {variable_name} (previous_term={previous_term}) の過去5年間平均値を取得中★")
                # 市場データの場合は現在と同じ半期を使用（予測対象期間に対応）
                current_half = '前半'  # 予測対象は通常前半で開始
                
                feature_value = self.get_market_data_for_period(
                    year, target_month, current_half, model_kind.vegetable, variable_name
                )
                
                if feature_value is not None:
                    self.logger.info(f"★市場変数 {variable_name} の特徴量値: {feature_value}★")
                    features_data.append({
                        'variable_name': variable_name,
                        'previous_term': previous_term,
                        'value': feature_value,
                        'year': year,
                        'month': target_month,
                        'half': current_half
                    })
            
            # その他の変数タイプの処理はここに追加
            # 例：その他の価格データなど
        
        # 特徴量データをデータフレームに変換
        df = pd.DataFrame(features_data)
        
        return df
    
    def get_previous_weather_for_model(self, model_name: str, target_month: int, year: int = None) -> Dict:
        """
        モデルの特徴セットに基づいて過去の気象データをすべて取得する
        
        Args:
            model_name (str): モデル名（例: "キャベツ春まき"）
            target_month (int): 対象月（1〜12）
            year (int, optional): 対象年。指定しない場合は現在の年
            
        Returns:
            Dict: 変数名をキー、過去の気象データのリストを値とする辞書
        """
        # 年が指定されていない場合は現在の年を使用
        if year is None:
            year = datetime.now().year
        
        # モデル種類を取得
        model_kind = self.get_model_kind_by_name(model_name)
        if not model_kind:
            return {}
        
        # 対象月に関連する特徴セットを取得
        feature_sets = ForecastModelFeatureSet.objects.filter(
            model_kind=model_kind,
            target_month=target_month
        ).select_related('variable')
        
        if not feature_sets.exists():
            self.logger.warning(f"対象月 {target_month} に関連する特徴セットが見つかりませんでした。")
            return {}
        
        # 気象データを格納する辞書
        weather_data_dict = {}
        
        # Weather関連の変数名リスト
        weather_variables = ['max_temp', 'mean_temp', 'min_temp', 'sum_precipitation', 'sunshine_duration', 'ave_humidity']
        
        # 特徴セットから気象データを取得
        for feature_set in feature_sets:
            variable = feature_set.variable
            variable_name = variable.name
            previous_term = variable.previous_term
            
            # Weather関連の変数のみ処理
            if variable_name in weather_variables:
                if variable_name not in weather_data_dict:
                    weather_data_dict[variable_name] = []
                
                # previous_termに基づいて気象データを取得
                weather_data = self.get_weather_data_for_period(year, target_month, previous_term)
                if weather_data:
                    # previous_termと値を辞書に格納
                    weather_data_dict[variable_name].append({
                        'previous_term': previous_term,
                        'value': weather_data.get(variable_name),
                        'year': weather_data['year'],
                        'month': weather_data['month'],
                        'half': weather_data['half']
                    })
        
        return weather_data_dict
    
    def analyze_weather_data_for_forecast(self, model_name: str, target_month: int, year: int = None) -> pd.DataFrame:
        """
        モデルと対象月に基づいて予測分析用の気象データを取得し、分析しやすい形式のDataFrameで返す
        
        Args:
            model_name (str): モデル名（例: "キャベツ春まき"）
            target_month (int): 対象月（1〜12）
            year (int, optional): 対象年。指定しない場合は現在の年
            
        Returns:
            pd.DataFrame: 分析用のデータフレーム
        """
        # 年が指定されていない場合は現在の年を使用
        if year is None:
            year = datetime.now().year
            
        self.logger.debug(f"analyze_weather_data_for_forecast - model_name={model_name}, target_month={target_month}, year={year}")
        
        # 気象データを取得
        weather_data_dict = self.get_previous_weather_for_model(model_name, target_month, year)
        
        if not weather_data_dict:
            self.logger.warning(f"{year}年の{target_month}月に関連する気象データが見つかりませんでした。")
            return pd.DataFrame()
        
        # データを整形
        rows = []
        
        for var_name, data_list in weather_data_dict.items():
            for data in data_list:
                rows.append({
                    'variable': var_name,
                    'previous_term': data['previous_term'],
                    'value': data['value'],
                    'year': data['year'],
                    'month': data['month'],
                    'half': data['half'],
                    'model': model_name,
                    'target_month': target_month
                })
        
        # DataFrameに変換
        df = pd.DataFrame(rows)
        
        if df.empty:
            self.logger.warning(f"{year}年の{target_month}月の気象データフレームが空です。")
            return df
            
        # previous_termでソート
        df = df.sort_values(['variable', 'previous_term'])
        
        self.logger.info(f"{year}年の{target_month}月に関連する気象データ {len(df)} 行を取得しました。")
        
        return df
    
    def get_target_price_data(self, model_name: str, target_month: int, year: int = None) -> List[Dict]:
        """
        予測対象となる価格データを取得する。
        2021年から指定された年（またはcurrentの年）までの範囲のデータを取得する
        
        Args:
            model_name (str): モデル名（例: "キャベツ春まき"）
            target_month (int): 対象月（1〜12）
            year (int, optional): 対象年。指定しない場合は現在の年
            
        Returns:
            List[Dict]: 複数年分の価格データの辞書リスト
        """
        # 年が指定されていない場合は現在の年を使用
        if year is None:
            year = datetime.now().year
            
        # モデル種類を取得
        model_kind = self.get_model_kind_by_name(model_name)
        if not model_kind:
            self.logger.warning(f"モデル種類 '{model_name}' は見つかりませんでした。")
            return []
        
        # 野菜情報を取得
        vegetable = model_kind.vegetable
        
        # 2021年から指定年までのデータを取得
        start_year = 2021
        end_year = year
        
        self.logger.info(f"{vegetable}の{target_month}月の価格データを{start_year}年から{end_year}年まで取得します。")
        
        # 指定期間のすべての価格データを取得
        market_data_list = ComputeMarket.objects.filter(
            vegetable=vegetable,
            region=self.region,
            target_month=target_month,
            target_year__gte=start_year,
            target_year__lte=end_year
        ).order_by('target_year', 'target_half')
        
        if not market_data_list:
            self.logger.warning(f"{vegetable}の{target_month}月の価格データが見つかりませんでした。")
            return []
        
        # 返却するデータリストを構築
        result_list = []
        
        for market_data in market_data_list:
            result = {
                'year': market_data.target_year,
                'month': market_data.target_month,
                'half': market_data.target_half,
                'average_price': market_data.average_price,
                'source_price': market_data.source_price,
                'volume': market_data.volume,
                'vegetable': vegetable.name
            }
            result_list.append(result)
        
        self.logger.info(f"{vegetable}の{target_month}月の価格データを{len(result_list)}件取得しました。")
        return result_list

    def build_forecast_dataset(self, model_name: str, target_month: int, year: int = None, vals: List[str] = None) -> Dict:
        """
        予測用のデータセットを構築する（特徴量Xと目的変数Yの両方）。
        2021年から指定年までの複数年のデータを使用する。
        
        Args:
            model_name (str): モデル名（例: "キャベツ春まき"）
            target_month (int): 対象月（1〜12）
            year (int, optional): 対象年。指定しない場合は現在の年
            vals (List[str]): 使用する変数名リスト
            
        Returns:
            Dict: 予測用データセット（X特徴量とY目的変数を含む）
        """
        self.logger.debug(f"build_forecast_dataset - vars={vals}, model={model_name}, month={target_month}, year={year}")
        
        # モデル種類が存在するか確認
        model_kind = self.get_model_kind_by_name(model_name)
        if not model_kind:
            self.logger.error(f"モデル種類 '{model_name}' が見つかりません")
            raise ValueError(f"モデル「{model_name}」は登録されていません。先にモデル種類を登録してください。")

        # 使用する変数リストの決定
        if vals:
            # 明示的に変数名リストが渡された場合はそれを使う
            variables = ForecastModelVariable.objects.filter(pk__in=vals)
            # variables_list = list(ForecastModelVariable.objects.filter(name__in=val_ids))
            if not variables:
                raise ValueError(f"指定された変数が見つかりませんでした: {vals}")
        else:
            # 既存の特徴量セットから変数を取得
            feature_sets = ForecastModelFeatureSet.objects.filter(
                model_kind=model_kind,
                target_month=target_month
            ).select_related('variable')
            if not feature_sets.exists():
                self.logger.error(f"対象月 {target_month} の特徴セット未設定 - model_kind.id={model_kind.id}, tag_name={model_kind.tag_name}")
                raise ValueError(f"モデル「{model_name}」（{target_month}月）の特徴量セットが未設定です。特徴量を設定してからモデルを実行してください。")

            # FIXME: どうしたらいいかわからん
            variables_prev = [fs.variable for fs in feature_sets]
        
        # 価格データ（目的変数Y）を取得 - 複数年分
        try:
            self.logger.debug(f"価格データ取得開始 - model={model_name}, month={target_month}, year={year}")
            price_data_list = self.get_target_price_data(model_name, target_month, year)
            if not price_data_list:
                self.logger.error(f"価格データが空 - model={model_name}, month={target_month}")
                raise ValueError(f"モデル「{model_name}」（{target_month}月）の価格データが見つかりません。2021年以降のデータが必要です。")
            self.logger.debug(f"価格データ取得成功 - {len(price_data_list)}件のデータを取得")
        except Exception as e:
            self.logger.error(f"価格データ取得エラー - {str(e)}")
            raise ValueError(f"モデル「{model_name}」（{target_month}月）の価格データ取得中にエラーが発生しました: {str(e)}")
        
        # 各年・各半期の価格データに対応する気象データを取得して結合
        rows = []
        weather_variables = ['max_temp', 'mean_temp', 'min_temp', 'sum_precipitation', 'sunshine_duration', 'ave_humidity']

        for price_data in price_data_list:
            price_year = price_data['year']
            price_half = price_data['half']
            price_value = price_data['average_price']

            # 変数ごとに previous_term に基づいて気象データを取得
            for variable in variables:
                var_name = variable.name
                prev_term = getattr(variable, 'previous_term', 0)

                # 気象変数の処理（過去5年間の平均値を使用）
                if var_name in weather_variables:
                    self.logger.info(f"★予測用気象変数 {var_name} (previous_term={prev_term}) の過去5年間平均値を取得中★")
                    weather_data = self.get_weather_data_for_period(price_year, target_month, prev_term)
                    if not weather_data:
                        # データが見つからなければスキップ
                        continue
                    
                    feature_value = weather_data.get(var_name)  # 既に過去5年間の平均値
                    self.logger.info(f"★予測用気象変数 {var_name} の特徴量値: {feature_value}★")

                    rows.append({
                        'variable': var_name,
                        'previous_term': prev_term,
                        'value': feature_value,
                        'year': weather_data['year'],
                        'month': weather_data['month'],
                        'half': weather_data['half'],
                        'model': model_name,
                        'target_month': target_month,
                        'price_year': price_year,
                        'price_half': price_half,
                        'price': price_value
                    })
                
                # 市場データ変数の処理（過去5年間の平均値を使用）
                elif var_name in ['prev_price', 'prev_volume', 'years_price', 'years_volume']:
                    self.logger.info(f"★予測用市場変数 {var_name} (previous_term={prev_term}) の過去5年間平均値を取得中★")
                    feature_value = self.get_market_data_for_period(
                        price_year, target_month, price_half, model_kind.vegetable, var_name
                    )
                    
                    if feature_value is not None:
                        self.logger.info(f"★予測用市場変数 {var_name} の特徴量値: {feature_value}★")
                        rows.append({
                            'variable': var_name,
                            'previous_term': prev_term,
                            'value': feature_value,  # 過去5年間の平均値
                            'year': price_year,
                            'month': target_month,
                            'half': price_half,
                            'model': model_name,
                            'target_month': target_month,
                            'price_year': price_year,
                            'price_half': price_half,
                            'price': price_value
                        })
        
        # 全データを結合
        if not rows:
            self.logger.error(f"気象データが取得できませんでした - model={model_name}, month={target_month}")
            raise ValueError(f"モデル「{model_name}」（{target_month}月）の気象データが取得できませんでした")

        combined_weather_df = pd.DataFrame(rows)
        
        # 結合した行数と扱った価格年数をログに出す
        self.logger.info(f"結合データ行数: {len(combined_weather_df)} (価格データ年数: {len(price_data_list)})")

        # 結果を辞書として返却
        result = {
            'X': combined_weather_df,
            'Y': price_data_list,
            'model': model_name,
            'target_month': target_month,
            'year': year or datetime.now().year
        }

        return result

# 使用例
"""
# データビルダーを初期化
builder = ForecastModelDataBuilder()

# キャベツ春まきモデルの5月のデータセットを取得
forecast_dataset = builder.build_forecast_dataset("キャベツ春まき", 5)

# 特徴量X（気象データ）
print("特徴量X（気象データ）:")
print(forecast_dataset['X'])

# 目的変数Y（価格データ）
print("\n目的変数Y（価格データ）:")
print(forecast_dataset['Y'])
"""