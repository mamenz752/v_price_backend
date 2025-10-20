from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, Dict, List
import numpy as np
import pandas as pd
import statsmodels.api as sm
from datetime import datetime
from django.db import transaction, IntegrityError
from forecast.models import (
    ForecastModelKind, ForecastModelVariable, ForecastModelFeatureSet,
    ForecastModelVersion, ForecastModelCoef, ForecastModelEvaluation
)
from .build_matrix import ForecastModelDataBuilder

@dataclass
class ForecastOLSConfig:
    """閾値や保存のバッチサイズなどの実行設定"""
    min_obs_margin: int = 1        # n >= p + min_obs_margin
    eval_batch_size: int = 1000
    region_name: str = '広島'      # 対象地域名
    deactivate_previous: bool = True  # 過去のモデルを非アクティブにするか

class ForecastOLSRunner:
    """
    予測モデルの重回帰分析を実行し、結果をDBに保存するクラス。
    build_matrix.pyのForecastModelDataBuilderを使用して特徴量行列を構築し、
    重回帰分析を行った結果をForecastModelVersion、ForecastModelEvaluation、ForecastModelCoefに保存する。
    """
    def __init__(self,
                 data_builder: Optional[ForecastModelDataBuilder] = None,
                 config: Optional[ForecastOLSConfig] = None) -> None:
        self.data_builder = data_builder or ForecastModelDataBuilder(region_name=config.region_name if config else '広島')
        self.cfg = config or ForecastOLSConfig()

    def prepare_regression_data(self, model_name: str, target_month: int, year: int = None) -> tuple:
        """
        回帰分析用のデータを準備する
        
        Args:
            model_name (str): モデル名（例: "キャベツ春まき"）
            target_month (int): 対象月（1〜12）
            year (int, optional): 対象年。指定しない場合は現在の年
            
        Returns:
            tuple: (X, y, variable_list)
                X: 特徴量行列
                y: 目的変数
                variable_list: 変数リスト
        """
        # ForecastModelDataBuilderからデータセットを取得
        forecast_dataset = self.data_builder.build_forecast_dataset(model_name, target_month, year)
        
        if not forecast_dataset or forecast_dataset['X'].empty or not forecast_dataset['Y']:
            raise ValueError(f"モデル '{model_name}' の {target_month} 月のデータセットが見つかりませんでした。")
        
        # 特徴量データフレームを準備
        X_df = forecast_dataset['X']
        
        # ピボットテーブルを使って特徴量行列を作成
        X = X_df.pivot_table(
            index=['model', 'target_month'], 
            columns=['variable', 'previous_term'],
            values='value',
            aggfunc='first'
        )
        
        # マルチインデックスをフラット化
        X.columns = [f"{col[0]}_{col[1]}" for col in X.columns]
        
        # 目的変数yを取得（平均価格を使用）
        y = pd.Series(forecast_dataset['Y'].get('average_price', np.nan))
        
        # 変数リストを作成
        variable_list = []
        for col in X.columns:
            var_name, prev_term = col.split('_')
            variable_list.append({
                'name': var_name,
                'previous_term': int(prev_term)
            })
        
        return X, y, variable_list

    def fit_and_persist(self, model_name: str, target_month: int, year: int = None) -> Optional[ForecastModelVersion]:
        """
        モデルの学習と結果の永続化を行う
        
        Args:
            model_name (str): モデル名（例: "キャベツ春まき"）
            target_month (int): 対象月（1〜12）
            year (int, optional): 対象年。指定しない場合は現在の年
            
        Returns:
            Optional[ForecastModelVersion]: 作成されたモデルバージョン
        """
        # 年が指定されていない場合は現在の年を使用
        if year is None:
            year = datetime.now().year
            
        # モデル種類を取得
        model_kind = self.data_builder.get_model_kind_by_name(model_name)
        if not model_kind:
            raise ValueError(f"モデル種類 '{model_name}' は見つかりませんでした。")
        
        # データの準備
        try:
            X, y, variable_list = self.prepare_regression_data(model_name, target_month, year)
        except Exception as e:
            print(f"データ準備エラー: {str(e)}")
            return None
            
        # 行列のサイズをチェック
        p = X.shape[1]
        n = len(y)
        if n < (p + self.cfg.min_obs_margin):
            raise ValueError(f"観測数が不足しています: n={n}, p={p}, 必要数 >= {p + self.cfg.min_obs_margin}")
        
        # OLS実行
        Xc = sm.add_constant(X, has_constant="add")
        model = sm.OLS(y, Xc).fit()
        
        # 予測・残差・指標
        y_pred = model.predict(Xc)
        resid = y - y_pred
        rmse = float(np.sqrt(((y - y_pred) ** 2).mean()))
        
        # 回帰統計量
        n_obs = model.nobs
        df_resid = model.df_resid
        df_model = model.df_model
        
        # 統計量の計算
        ssr = model.ssr  # 回帰変動（回帰による平方和）
        ess = model.ess  # 残差変動（残差平方和）
        tss = model.centered_tss  # 全変動
        msr = ssr / df_model  # 回帰分散
        mse = ess / df_resid  # 残差分散
        
        # DB保存（原子性）
        with transaction.atomic():
            # 以前のアクティブなモデルを非アクティブ化
            if self.cfg.deactivate_previous:
                ForecastModelVersion.objects.filter(
                    model_kind=model_kind,
                    target_month=target_month,
                    is_active=True
                ).update(is_active=False)
                
            # モデルバージョンの作成
            model_version = ForecastModelVersion.objects.create(
                target_month=target_month,
                is_active=True,
                model_kind=model_kind
            )
            
            # モデル評価の作成
            model_evaluation = ForecastModelEvaluation.objects.create(
                multi_r=float(np.sqrt(model.rsquared)),
                heavy_r2=float(model.rsquared),
                adjusted_r2=float(model.rsquared_adj),
                sign_f=float(model.f_pvalue),
                standard_error=float(np.sqrt(mse)),
                rmse=float(rmse),
                reg_variation=float(ssr),
                reg_variance=float(msr),
                res_variation=float(ess),
                res_variance=float(mse),
                total_variation=float(tss)
            )
            
            # 係数の保存
            se = model.bse
            tv = model.tvalues
            pv = model.pvalues
            
            # 変数辞書を作成（名前とprevious_termからvariableオブジェクトを取得）
            variable_dict = {}
            for var_info in variable_list:
                var_name = var_info['name']
                prev_term = var_info['previous_term']
                try:
                    var_obj = ForecastModelVariable.objects.get(name=var_name, previous_term=prev_term)
                    variable_dict[f"{var_name}_{prev_term}"] = var_obj
                except ForecastModelVariable.DoesNotExist:
                    print(f"警告: 変数 '{var_name}'（previous_term={prev_term}）が見つかりませんでした。")
            
            # 定数項のための特別処理
            const_var, _ = ForecastModelVariable.objects.get_or_create(
                name='const',
                previous_term=0
            )
            
            # 係数の作成
            for name in model.params.index:
                # 定数項の場合
                if name == 'const':
                    variable = const_var
                    is_segment = False
                else:
                    # 通常の変数の場合
                    if name not in variable_dict:
                        print(f"警告: 変数 '{name}' がvariable_dictに見つかりません。スキップします。")
                        continue
                    variable = variable_dict[name]
                    is_segment = False  # 必要に応じて変更
                
                ForecastModelCoef.objects.create(
                    is_segment=is_segment,
                    variable=variable,
                    coef=float(model.params[name]),
                    value_t=float(tv.get(name, np.nan)) if hasattr(tv, "get") else float(tv[name]),
                    sign_p=float(pv.get(name, np.nan)) if hasattr(pv, "get") else float(pv[name]),
                    standard_error=float(se.get(name, np.nan)) if hasattr(se, "get") else float(se[name])
                )

        return model_version
    
    def run_forecast_analysis(self, model_names: List[str], target_months: List[int], year: int = None) -> Dict:
        """
        複数のモデルと対象月に対して予測分析を実行する
        
        Args:
            model_names (List[str]): モデル名のリスト（例: ["キャベツ春まき", "キャベツ秋まき"]）
            target_months (List[int]): 対象月のリスト（例: [5, 11]）
            year (int, optional): 対象年。指定しない場合は現在の年
            
        Returns:
            Dict: モデル名と対象月をキーとした結果辞書
        """
        results = {}
        
        for model_name in model_names:
            if model_name not in results:
                results[model_name] = {}
                
            for target_month in target_months:
                try:
                    model_version = self.fit_and_persist(model_name, target_month, year)
                    results[model_name][target_month] = {
                        'success': model_version is not None,
                        'model_version_id': model_version.id if model_version else None,
                        'error': None
                    }
                except Exception as e:
                    results[model_name][target_month] = {
                        'success': False,
                        'model_version_id': None,
                        'error': str(e)
                    }
                    print(f"エラー（{model_name}, 月={target_month}）: {str(e)}")
        
        return results


# 使用例
if __name__ == "__main__":
    # このスクリプトを直接実行した場合に実行されるコード
    import os
    import sys
    import django
    
    # Djangoの設定を読み込む
    sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
    django.setup()
    
    # 実行設定
    config = ForecastOLSConfig(region_name='広島', deactivate_previous=True)
    
    # 実行クラスの初期化
    runner = ForecastOLSRunner(config=config)
    
    # キャベツ春まきの5月のモデルを実行
    try:
        print("キャベツ春まき、5月のモデルを実行中...")
        model_version = runner.fit_and_persist("キャベツ春まき", 5)
        if model_version:
            print(f"モデルバージョンID: {model_version.id} が作成されました")
        else:
            print("モデルの作成に失敗しました")
    except Exception as e:
        print(f"エラーが発生しました: {str(e)}")
    
    # 複数のモデルと月を一度に実行
    try:
        print("\n複数のモデルを実行中...")
        models_to_run = ["キャベツ春まき", "キャベツ秋まき"]
        months_to_run = [5, 11]  # 5月と11月
        
        results = runner.run_forecast_analysis(models_to_run, months_to_run)
        
        # 結果の表示
        for model_name, month_results in results.items():
            print(f"\nモデル: {model_name}")
            for month, result in month_results.items():
                status = "成功" if result['success'] else "失敗"
                model_id = result['model_version_id'] or "N/A"
                error = result['error'] or "なし"
                print(f"  月: {month} - 状態: {status}, モデルID: {model_id}, エラー: {error}")
    except Exception as e:
        print(f"実行エラー: {str(e)}")
