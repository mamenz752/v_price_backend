from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, Dict, List
import numpy as np
import pandas as pd
import statsmodels.api as sm
import logging
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

    def prepare_regression_data(self, model_name: str, target_month: int, vals: List[int]) -> tuple:
        """
        回帰分析用のデータを準備する
        複数年（2021-2025年）のデータを扱うように更新
        
        Args:
            model_name (str): モデル名（例: "キャベツ春まき"）
            target_month (int): 対象月（1〜12）
            variables (List[int]): 使用する変数のIDリスト

        Returns:
            tuple: (X, y, variable_list)
                X: 特徴量行列
                y: 目的変数
                variable_list: 変数リスト
        """
        # ForecastModelDataBuilderからデータセットを取得
        # variable_names が渡されていればそれをビルダーに伝えて特徴量セット未登録時も動作するようにする

        vals_list = list(vals)
        forecast_dataset = self.data_builder.build_forecast_dataset(model_name, target_month, vals=vals_list)

        if not forecast_dataset or forecast_dataset['X'].empty or not forecast_dataset['Y']:
            raise ValueError(f"モデル '{model_name}' の {target_month} 月のデータセットが見つかりませんでした。")
        
        # 特徴量データフレームを準備
        X_df = forecast_dataset['X']
        
        logger = logging.getLogger(__name__)
        logger.info(f"特徴量データフレーム準備: X_df shape={X_df.shape}")
        logger.debug(f"X_df columns: {X_df.columns.tolist()}")
        logger.debug(f"X_df sample:\n{X_df.head().to_string()}")
        
        try:
            # 複数年のデータを処理するために、price_yearとprice_halfをインデックスとしたピボットテーブルを作成
            # 各年のデータが1行となるように変換
            logger.info("ピボットテーブルの作成を開始")
            X = X_df.pivot_table(
                index=['price_year', 'price_half'],
                columns=['variable', 'previous_term'],
                values='value',
                aggfunc='first'
            )
            
            # マルチインデックスをフラット化
            X.columns = [f"{col[0]}_{col[1]}" for col in X.columns]
            
            print(f"INFO: ピボットテーブル作成成功 - 行数: {X.shape[0]}, 列数: {X.shape[1]}")
            print(f"DEBUG: ピボットテーブルのサンプルデータ:\n{X.columns.tolist()}            docker compose exec web tail -n 200 /code/logs/django.log")
            
        except Exception as e:
            # デバッグ情報を出力
            print(f"ピボットテーブル作成エラー: {str(e)}")
            print(f"X_df columns: {X_df.columns}")
            print(f"X_df sample data:\n{X_df.head().to_string()}")
            
            # price_yearとprice_halfがない場合のフォールバック
            if 'price_year' not in X_df.columns:
                print("警告: price_year列が見つかりません")
                if 'price' in X_df.columns:
                    # 価格データに基づいてグループ化
                    X = X_df.pivot_table(
                        index=['year', 'half'] if ('year' in X_df.columns and 'half' in X_df.columns) else 'price',
                        columns=['variable', 'previous_term'],
                        values='value',
                        aggfunc='first'
                    )
                else:
                    # 基本的なピボットテーブル
                    X = X_df.pivot_table(
                        index=['model', 'target_month'],
                        columns=['variable', 'previous_term'],
                        values='value',
                        aggfunc='first'
                    )
            
            # マルチインデックスをフラット化
            X.columns = [f"{col[0]}_{col[1]}" for col in X.columns]
        
        # 目的変数yを準備 - 複数年分
        y_values = {}
        
        # forecast_dataset['Y']がリスト（複数年）の場合の処理
        if isinstance(forecast_dataset['Y'], list):
            for price_data in forecast_dataset['Y']:
                if 'average_price' in price_data and 'year' in price_data and 'half' in price_data:
                    # 年と半期をキーとして使用
                    key = (price_data['year'], price_data['half'])
                    y_values[key] = price_data['average_price']
        else:
            # 単一のデータ辞書の場合
            price_data = forecast_dataset['Y']
            if price_data and 'average_price' in price_data:
                key = (price_data.get('year', 0), price_data.get('half', '前半'))
                y_values[key] = price_data['average_price']
        
        # Series化
        y = pd.Series(y_values)
        
        print(f"INFO: 目的変数y作成 - データポイント数: {len(y)}")
        
        # インデックスの調整（XとYのインデックスを合わせる）
        common_index = X.index.intersection(y.index)
        if len(common_index) < len(X):
            print(f"警告: インデックスの不一致 - 共通: {len(common_index)}, X: {len(X)}, y: {len(y)}")

        X = X.loc[common_index]
        y = y.loc[common_index]

        # 欠損値を含む行を除外
        mask = X.notna().all(axis=1)
        X = X[mask]
        y = y[mask]

        # インデックスの最終確認
        n = len(y)
        p = X.shape[1]

        print(f"確認：説明変数自動削除前：{X.columns.tolist()}")

        # 観測数が不足している場合、自動的に変数を削減して対応を試みる
        if n < (p + getattr(self.cfg, 'min_obs_margin', 1) if hasattr(self, 'cfg') else p + 1):
            # 利用可能な最大変数数
            min_obs_margin = getattr(self.cfg, 'min_obs_margin', 1) if hasattr(self, 'cfg') else 1
            max_allowed_p = max(n - min_obs_margin, 0)

            if max_allowed_p <= 0:
                raise ValueError(f"観測数が極端に不足しています: n={n}, 変数数(p)={p}. 変数を減らすかデータを増やしてください。")

            # 分散の小さい変数から削除する（単純なヒューリスティック）
            variances = X.var(axis=0).fillna(0)
            keep_cols = variances.sort_values(ascending=False).head(max_allowed_p).index.tolist()
            dropped = [c for c in X.columns if c not in keep_cols]

            print(f"警告: 観測数が不足しているため {len(dropped)} 個の変数を自動削除します: {dropped}")

            # 列を絞る
            X = X[keep_cols]
            p = X.shape[1]

        # 変数リストを作成
        variable_list = []
        for col in X.columns:
            try:
                parts = col.split('_')
                if len(parts) == 2:
                    var_name, prev_term = parts
                else:
                    var_name = '_'.join(parts[:-1])
                    prev_term = parts[-1]

                variable_list.append({
                    'name': var_name,
                    'previous_term': int(prev_term)
                })
            except Exception as e:
                print(f"変数リスト作成エラー（{col}）: {str(e)}")
                continue

        print(f"最終データセット - X: {X.shape}, y: {len(y)}, variables: {variable_list}")

        return X, y, variable_list

    def fit_and_persist(self, model_name: str, target_month: int, vals: List[int]) -> Optional[ForecastModelVersion]:
        """
        モデルの学習と結果の永続化を行う
        
        Args:
            model_name (str): モデル名（例: "キャベツ春まき"）
            target_month (int): 対象月（1〜12）
            vals (List[int]): 使用する変数のIDリスト
            
        Returns:
            Optional[ForecastModelVersion]: 作成されたモデルバージョン
        """
        logger = logging.getLogger(__name__)
        logger.info(f"fit_and_persist開始: モデル={model_name}, 月={target_month}, 変数={vals}")

        # 年が指定されていない場合は現在の年を使用
        # if year is None:
        #     year = datetime.now().year
            
        # モデル種類を取得
        model_kind = self.data_builder.get_model_kind_by_name(model_name)
        if not model_kind:
            raise ValueError(f"モデル種類 '{model_name}' は見つかりませんでした。")
        
        # データの準備
        logger.info(f"重回帰分析開始: モデル={model_name}, 月={target_month}, 変数={vals}")
        # 変数リストを作成
        # vals_ids = [var.id for var in variables]

        try:
            # prepare_regression_data のシグネチャを変えたため、キーワードで渡す
            # FIXME: ここがわからん
            X, y, variable_list = self.prepare_regression_data(model_name, target_month, vals=vals)
            logger.info(f"データ準備完了: X shape={X.shape}, y length={len(y)}")
            logger.info(f"データ準備完了: X shape={X.shape}, y length={len(y)}")
        except Exception as e:
            logger.error(f"データ準備エラー: {str(e)}", exc_info=True)
            raise ValueError(f"データの準備中にエラーが発生しました: {str(e)}")
            
        # 行列のサイズをチェック
        p = X.shape[1]
        n = len(y)
        logger.info(f"行列サイズ: 観測数(n)={n}, 変数数(p)={p}")
        if n < (p + self.cfg.min_obs_margin):
            raise ValueError(f"観測数が不足しています: n={n}, p={p}, 必要数 >= {p + self.cfg.min_obs_margin}")
        
        # OLS実行
        Xc = sm.add_constant(X, has_constant="add")
        model = sm.OLS(y, Xc).fit()
        
        # 予測・残差・指標
        y_pred = model.predict(Xc)
        resid = y - y_pred
        rmse = float(np.sqrt(((resid) ** 2).mean()))
        
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
        model_version = None  # モデルバージョン変数をトランザクション外で初期化
        logger.info(f"データベース保存開始: モデル={model_name}")
        
        with transaction.atomic():
            # 以前のアクティブなモデルを非アクティブ化
            try:
                if self.cfg.deactivate_previous:
                    deact_qs = ForecastModelVersion.objects.filter(
                        model_kind=model_kind,
                        target_month=target_month,
                        is_active=True
                    )
                    deact_count = deact_qs.update(is_active=False)
                    logger.info(f"非アクティブ化されたモデル数: {deact_count}")
            except Exception as e:
                logger.error(f"既存モデルの非アクティブ化でエラーが発生: {str(e)}")
                raise
            
            # モデルバージョンの作成
            logger.info(f"モデルバージョンの作成を開始: モデル={model_kind.tag_name}, 月={target_month}")
            try:
                model_version = ForecastModelVersion.objects.create(
                    target_month=target_month,
                    is_active=True,
                    model_kind=model_kind
                )

                # 新規: モデル作成直後に予測を実行
                from observe.services import ObserveService, ObserveServiceConfig
                observe_service = ObserveService(ObserveServiceConfig(region_name=self.cfg.region_name))
                
                # 現在の年と上半期/下半期を取得
                current_year = datetime.now().year
                current_month = datetime.now().month
                current_half = '前半' if current_month <= 6 else '後半'
                
                # 予測実行
                observe_service.predict_for_model_version(
                    model_version=model_version,
                    year=current_year,
                    month=target_month,
                    half=current_half
                )

                logger.info(f"モデルバージョン作成完了: ID={model_version.id}")
            except Exception as e:
                logger.error(f"モデルバージョン作成エラー: {str(e)}", exc_info=True)
                raise

            # 既存の特徴量セットを削除
            deleted_count, _ = ForecastModelFeatureSet.objects.filter(model_kind=model_kind, target_month=target_month).delete()
            fs_objs = []
            variables = ForecastModelVariable.objects.filter(pk__in=vals)
            for var in variables:
                fs = ForecastModelFeatureSet(
                    model_kind=model_kind,
                    target_month=target_month,
                    variable=var  # var は ForecastModelVariable オブジェクトの想定
                )
                fs_objs.append(fs)
            if fs_objs:
                ForecastModelFeatureSet.objects.bulk_create(fs_objs)
            logger.info("Recreated ForecastModelFeatureSet: deleted=%d created=%d for model_version=%s", deleted_count, len(fs_objs), model_version.id)

            # モデル評価の作成
            model_evaluation = ForecastModelEvaluation.objects.create(
                model_version=model_version,
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
            
            # モデル作成後、最新の予測も実行
            from observe.services import ObserveService, ObserveServiceConfig
            observe_service = ObserveService(ObserveServiceConfig(region_name=self.cfg.region_name))
            current_year = datetime.now().year

            # TODO: 予測実行の時期をうまいこと指定する
            try:
                observe_service.observe_latest_model(
                    model_kind.id,
                    current_year,
                    target_month,
                    "前半"
                )
                observe_service.observe_latest_model(
                    model_kind.id,
                    current_year,
                    target_month,
                    "後半"
                )
            except Exception as e:
                print(f"予測の実行中にエラーが発生しました: {str(e)}")
            
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
                    is_segment = True  # 定数項の場合はis_segmentをTrueに設定
                else:
                    # 通常の変数の場合
                    if name not in variable_dict:
                        print(f"警告: 変数 '{name}' がvariable_dictに見つかりません。スキップします。")
                        continue
                    variable = variable_dict[name]
                    is_segment = False  # 必要に応じて変更
                
                ForecastModelCoef.objects.create(
                    # FIXME: model_version を渡すのがバグ怪しい
                    model_version=model_version,
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
        logger = logging.getLogger(__name__)
        results = {}
        
        for model_name in model_names:
            if model_name not in results:
                results[model_name] = {}
                
            for target_month in target_months:
                logger.info(f"モデル実行開始: モデル={model_name}, 月={target_month}")
                try:
                    # モデル種類の存在確認
                    try:
                        model_kind = self.data_builder.get_model_kind_by_name(model_name)
                        if not model_kind:
                            raise ValueError(f"モデル種類 '{model_name}' が見つかりません")
                    except Exception as e:
                        logger.error(f"モデル種類の取得エラー: {str(e)}")
                        results[model_name][target_month] = {
                            'success': False,
                            'model_version_id': None,
                            'error': f"モデル種類エラー: {str(e)}"
                        }
                        continue

                    # 変数を取得してから実行
                    try:
                        # デフォルトの変数セットを取得
                        variables = ForecastModelVariable.objects.filter(
                            forecastmodelfeatureset__model_kind=model_kind,
                            forecastmodelfeatureset__target_month=target_month
                        ).distinct()
                        
                        if not variables:
                            raise ValueError("特徴量セットが設定されていません")

                        variable_names = [getattr(v, "name", str(v)) for v in variables]
                        model_version = self.fit_and_persist(
                            model_name,
                            target_month,
                            variable_names,
                            year
                        )
                        
                        results[model_name][target_month] = {
                            'success': model_version is not None,
                            'model_version_id': model_version.id if model_version else None,
                            'error': None
                        }
                        
                        logger.info(f"モデル実行成功: モデル={model_name}, 月={target_month}, ID={model_version.id if model_version else 'None'}")
                        
                    except Exception as e:
                        logger.error(f"モデル実行エラー: モデル={model_name}, 月={target_month}, エラー={str(e)}", exc_info=True)
                        results[model_name][target_month] = {
                            'success': False,
                            'model_version_id': None,
                            'error': str(e)
                        }
                        
                except Exception as e:
                    logger.error(f"予期せぬエラー: モデル={model_name}, 月={target_month}, エラー={str(e)}", exc_info=True)
                    results[model_name][target_month] = {
                        'success': False,
                        'model_version_id': None,
                        'error': f"予期せぬエラー: {str(e)}"
                    }
        
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
