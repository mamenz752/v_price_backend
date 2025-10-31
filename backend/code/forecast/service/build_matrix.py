import pandas as pd
import numpy as np
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
        print(f"DEBUG: モデル種類の検索開始 - model_name='{model_name}'")
        try:
            # 登録済みのすべてのモデル種類を取得して確認
            all_models = ForecastModelKind.objects.all()
            print(f"DEBUG: 登録済みモデル一覧: {[model.tag_name for model in all_models]}")
            
            model_kind = ForecastModelKind.objects.get(tag_name=model_name)
            print(f"DEBUG: モデル種類が見つかりました - id={model_kind.id}, tag_name={model_kind.tag_name}")
            return model_kind
        except ForecastModelKind.DoesNotExist:
            print(f"警告: モデル種類 '{model_name}' は見つかりませんでした。")
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
        
        print(f"DEBUG: get_weather_data_for_period - region={self.region.name}, year={target_year}, month={target_month}, half={target_half}")
        
        # 対象期間の気象データを取得
        weather_data = ComputeWeather.objects.filter(
            region=self.region,
            target_year=target_year,
            target_month=target_month,
            target_half=target_half
        ).first()
        
        if not weather_data:
            print(f"警告: {target_year}年{target_month}月{target_half}の気象データが見つかりませんでした。region={self.region.name}")
            
            # 試しに他のhalfでも検索してみる
            alt_half = '前半' if target_half == '後半' else '後半'
            alt_weather = ComputeWeather.objects.filter(
                region=self.region,
                target_year=target_year,
                target_month=target_month,
                target_half=alt_half
            ).first()
            
            if alt_weather:
                print(f"INFO: 代替として {target_year}年{target_month}月{alt_half}の気象データを使用します")
                weather_data = alt_weather
            else:
                # 代替がなければ空辞書を返す
                return {}
        
        # 返却する気象データ辞書を構築
        result = {
            'year': target_year,
            'month': target_month,
            'half': weather_data.target_half,
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
        # 年が指定されていない場合は現在の年を使用
        if year is None:
            year = datetime.now().year
            
        print(f"DEBUG: analyze_weather_data_for_forecast - model_name={model_name}, target_month={target_month}, year={year}")
        
        # 気象データを取得
        weather_data_dict = self.get_previous_weather_for_model(model_name, target_month, year)
        
        if not weather_data_dict:
            print(f"警告: {year}年の{target_month}月に関連する気象データが見つかりませんでした。")
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
            print(f"警告: {year}年の{target_month}月の気象データフレームが空です。")
            return df
            
        # previous_termでソート
        df = df.sort_values(['variable', 'previous_term'])
        
        print(f"INFO: {year}年の{target_month}月に関連する気象データ {len(df)} 行を取得しました。")
        
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
            print(f"警告: モデル種類 '{model_name}' は見つかりませんでした。")
            return []
        
        # 野菜情報を取得
        vegetable = model_kind.vegetable
        
        # 2021年から指定年までのデータを取得
        start_year = 2021
        end_year = year
        
        print(f"INFO: {vegetable}の{target_month}月の価格データを{start_year}年から{end_year}年まで取得します。")
        
        # 指定期間のすべての価格データを取得
        market_data_list = ComputeMarket.objects.filter(
            vegetable=vegetable,
            region=self.region,
            target_month=target_month,
            target_year__gte=start_year,
            target_year__lte=end_year
        ).order_by('target_year', 'target_half')
        
        if not market_data_list:
            print(f"警告: {vegetable}の{target_month}月の価格データが見つかりませんでした。")
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
        
        print(f"INFO: {vegetable}の{target_month}月の価格データを{len(result_list)}件取得しました。")
        return result_list
    
    def build_forecast_dataset(self, model_name: str, target_month: int, year: int = None, variable_names: Optional[List[str]] = None) -> Dict:
        """
        予測用のデータセットを構築する（特徴量Xと目的変数Yの両方）。
        2021年から指定年までの複数年のデータを使用する。
        
        Args:
            model_name (str): モデル名（例: "キャベツ春まき"）
            target_month (int): 対象月（1〜12）
            year (int, optional): 対象年。指定しない場合は現在の年
            
        Returns:
            Dict: 予測用データセット（X特徴量とY目的変数を含む）
        """
        print(f"DEBUG: build_forecast_dataset - model_name={model_name}, target_month={target_month}, year={year}")
        
        # モデル種類が存在するか確認
        model_kind = self.get_model_kind_by_name(model_name)
        if not model_kind:
            print(f"DEBUG: モデル種類 '{model_name}' が見つかりません")
            raise ValueError(f"モデル「{model_name}」は登録されていません。先にモデル種類を登録してください。")

        # 使用する変数リストの決定
        variables = []
        if variable_names:
            # 明示的に変数名リストが渡された場合はそれを使う
            variables = list(ForecastModelVariable.objects.filter(name__in=variable_names))
            if not variables:
                raise ValueError(f"指定された変数が見つかりませんでした: {variable_names}")
        else:
            # 既存の特徴量セットから変数を取得
            feature_sets = ForecastModelFeatureSet.objects.filter(
                model_kind=model_kind,
                target_month=target_month
            ).select_related('variable')
            if not feature_sets.exists():
                print(f"DEBUG: 対象月 {target_month} の特徴セットが見つかりません")
                print(f"DEBUG: model_kind.id={model_kind.id}, tag_name={model_kind.tag_name}")
                raise ValueError(f"モデル「{model_name}」（{target_month}月）の特徴量セットが未設定です。特徴量を設定してからモデルを実行してください。")

            variables = [fs.variable for fs in feature_sets]
        
        # 価格データ（目的変数Y）を取得 - 複数年分
        try:
            print(f"DEBUG: 価格データ取得開始 - model={model_name}, month={target_month}, year={year}")
            price_data_list = self.get_target_price_data(model_name, target_month, year)
            if not price_data_list:
                print(f"DEBUG: 価格データが空です")
                raise ValueError(f"モデル「{model_name}」（{target_month}月）の価格データが見つかりません。2021年以降のデータが必要です。")
            print(f"DEBUG: 価格データ取得成功 - {len(price_data_list)}件のデータを取得")
        except Exception as e:
            print(f"DEBUG: 価格データ取得エラー - {str(e)}")
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

                # 現状は気象変数のみ処理（将来的に価格等も追加可能）
                if var_name in weather_variables:
                    weather_data = self.get_weather_data_for_period(price_year, target_month, prev_term)
                    if not weather_data:
                        # データが見つからなければスキップ
                        continue

                    rows.append({
                        'variable': var_name,
                        'previous_term': prev_term,
                        'value': weather_data.get(var_name),
                        'year': weather_data['year'],
                        'month': weather_data['month'],
                        'half': weather_data['half'],
                        'model': model_name,
                        'target_month': target_month,
                        'price_year': price_year,
                        'price_half': price_half,
                        'price': price_value
                    })
        
        # 全データを結合
        if not rows:
            print(f"DEBUG: 気象データが取得できませんでした")
            raise ValueError(f"モデル「{model_name}」（{target_month}月）の気象データが取得できませんでした")

        combined_weather_df = pd.DataFrame(rows)
        
        # 結合した行数と扱った価格年数をログに出す
        print(f"INFO: 結合データ行数: {len(combined_weather_df)} (価格データ年数: {len(price_data_list)})")

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