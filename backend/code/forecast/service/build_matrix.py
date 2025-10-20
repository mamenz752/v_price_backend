import pandas as pd
import numpy as np
from datetime import datetime
from typing import Tuple, Dict, List, Optional
from django.db.models import Q
import calendar
from compute.models import ComputeMarket, ComputeWeather
from forecast.models import ForecastModelKind, ForecastModelVariable, ForecastModelFeatureSet
from ingest.models import Region

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
    
    def __init__(self, region_name: str = '広島'):
        """
        初期化
        Args:
            region_name (str, optional): 対象地域名。デフォルトは'広島'。
        """
        self.region = Region.objects.get(name=region_name)
    
    def get_model_kind_by_name(self, model_name: str) -> Optional[ForecastModelKind]:
        """
        モデル名からモデル種類を取得
        
        Args:
            model_name (str): モデル名（例: "キャベツ春まき"）
            
        Returns:
            Optional[ForecastModelKind]: モデル種類オブジェクト、見つからない場合はNone
        """
        try:
            return ForecastModelKind.objects.get(tag_name=model_name)
        except ForecastModelKind.DoesNotExist:
            print(f"モデル種類 '{model_name}' は見つかりませんでした。")
            return None
    
    def get_weather_data_for_period(self, year: int, month: int, previous_term: int) -> Dict:
        """
        指定された期間の気象データを取得する
        
        Args:
            year (int): 対象年
            month (int): 対象月
            previous_term (int): 何ヶ月前のデータか（1〜24）
            
        Returns:
            Dict: 気象データの辞書
        """
        # 対象月から previous_term ヶ月前の年月を計算
        target_year = year
        target_month = month - previous_term
        
        # 月が負または0になった場合は前年の対応する月に調整
        while target_month <= 0:
            target_year -= 1
            target_month += 12
        
        # 対象期間の前半・後半を決定
        target_half = '後半' if previous_term <= 1 else '前半'
        
        # 対象期間の気象データを取得
        weather_data = ComputeWeather.objects.filter(
            region=self.region,
            target_year=target_year,
            target_month=target_month,
            target_half=target_half
        ).first()
        
        if not weather_data:
            print(f"警告: {target_year}年{target_month}月{target_half}の気象データが見つかりませんでした。")
            return {}
        
        # 返却する気象データ辞書を構築
        result = {
            'year': target_year,
            'month': target_month,
            'half': target_half,
            'max_temp': weather_data.max_temp,
            'mean_temp': weather_data.mean_temp,
            'min_temp': weather_data.min_temp,
            'sum_precipitation': weather_data.sum_precipitation,
            'sunshine_duration': weather_data.sunshine_duration,
            'ave_humidity': weather_data.ave_humidity
        }
        
        return result
    
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
            print(f"対象月 {target_month} に関連する特徴セットが見つかりませんでした。")
            return pd.DataFrame()
        
        # 特徴量データを収集
        features_data = []
        
        for feature_set in feature_sets:
            variable = feature_set.variable
            variable_name = variable.name
            previous_term = variable.previous_term
            
            # 変数がWeather関連の場合
            if variable_name in ['max_temp', 'mean_temp', 'min_temp', 'sum_precipitation', 'sunshine_duration', 'ave_humidity']:
                weather_data = self.get_weather_data_for_period(year, target_month, previous_term)
                if weather_data:
                    feature_value = weather_data.get(variable_name)
                    
                    # 特徴量データに追加
                    features_data.append({
                        'variable_name': variable_name,
                        'previous_term': previous_term,
                        'value': feature_value,
                        'year': weather_data['year'],
                        'month': weather_data['month'],
                        'half': weather_data['half']
                    })
            
            # その他の変数タイプの処理はここに追加
            # 例：価格データなど
        
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
            print(f"対象月 {target_month} に関連する特徴セットが見つかりませんでした。")
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
        # 気象データを取得
        weather_data_dict = self.get_previous_weather_for_model(model_name, target_month, year)
        
        if not weather_data_dict:
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
        
        # previous_termでソート
        df = df.sort_values(['variable', 'previous_term'])
        
        return df
    
    def get_target_price_data(self, model_name: str, target_month: int, year: int = None) -> Dict:
        """
        予測対象となる価格データを取得する
        
        Args:
            model_name (str): モデル名（例: "キャベツ春まき"）
            target_month (int): 対象月（1〜12）
            year (int, optional): 対象年。指定しない場合は現在の年
            
        Returns:
            Dict: 価格データの辞書
        """
        # 年が指定されていない場合は現在の年を使用
        if year is None:
            year = datetime.now().year
            
        # モデル種類を取得
        model_kind = self.get_model_kind_by_name(model_name)
        if not model_kind:
            return {}
        
        # 野菜情報を取得
        vegetable = model_kind.vegetable
        
        # 価格データを検索するためのターゲットハーフを決定
        target_half = '前半'  # デフォルトは前半
        
        # 最新年のデータを取得
        latest_market_data = ComputeMarket.objects.filter(
            vegetable=vegetable,
            region=self.region,
            target_month=target_month
        ).order_by('-target_year').first()
        
        if not latest_market_data:
            print(f"警告: {vegetable}の{target_month}月の価格データが見つかりませんでした。")
            return {}
        
        # 返却するデータ辞書を構築
        result = {
            'year': latest_market_data.target_year,
            'month': latest_market_data.target_month,
            'half': latest_market_data.target_half,
            'average_price': latest_market_data.average_price,
            'source_price': latest_market_data.source_price,
            'volume': latest_market_data.volume,
            'vegetable': vegetable.name
        }
        
        return result
    
    def build_forecast_dataset(self, model_name: str, target_month: int, year: int = None) -> Dict:
        """
        予測用のデータセットを構築する（特徴量Xと目的変数Yの両方）
        
        Args:
            model_name (str): モデル名（例: "キャベツ春まき"）
            target_month (int): 対象月（1〜12）
            year (int, optional): 対象年。指定しない場合は現在の年
            
        Returns:
            Dict: 予測用データセット（X特徴量とY目的変数を含む）
        """
        # 気象データ（特徴量X）を取得
        weather_data_df = self.analyze_weather_data_for_forecast(model_name, target_month, year)
        
        # 価格データ（目的変数Y）を取得
        price_data = self.get_target_price_data(model_name, target_month, year)
        
        # 結果を辞書として返却
        result = {
            'X': weather_data_df,
            'Y': price_data,
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