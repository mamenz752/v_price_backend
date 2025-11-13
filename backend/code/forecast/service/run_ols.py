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
from observe.models import (
    ObserveReport
)
from .build_matrix import ForecastModelDataBuilder
from collections import defaultdict

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
            
            logger.info(f"INFO: ピボットテーブル作成成功 - 行数: {X.shape[0]}, 列数: {X.shape[1]}")
            logger.info(f"DEBUG: ピボットテーブルのサンプルデータ:\n{X.columns.tolist()}            docker compose exec web tail -n 200 /code/logs/django.log")
            
        except Exception as e:
            # デバッグ情報を出力
            logger.info(f"ピボットテーブル作成エラー: {str(e)}")
            logger.info(f"X_df columns: {X_df.columns}")
            logger.info(f"X_df sample data:\n{X_df.head().to_string()}")
            
            # price_yearとprice_halfがない場合のフォールバック
            if 'price_year' not in X_df.columns:
                logger.info("警告: price_year列が見つかりません")
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
        
        # FIXME: ここで予測実行されている可能性あり
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

            logger.info(f"モデル評価作成完了: ID={model_evaluation.id} for model_version={model_version.id}")
            
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

            # モデル作成後、最新の予測も実行
            from observe.services import ObserveService, ObserveServiceConfig
            observe_service = ObserveService(ObserveServiceConfig(region_name=self.cfg.region_name))
            current_year = datetime.now().year

            logging.info("最新モデルでの予測実行を開始")
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

    def update_predictions_for_period(self,
                                      updated_year: int,
                                      updated_month: int,
                                      updated_half: Optional[str] = "前半",
                                      variable_ids: Optional[List[int]] = None,
                                      create_if_missing: bool = True,
                                      look_ahead_years: int = 1,
                                      refit_models: bool = True,
                                      logger: Optional[logging.Logger] = None) -> int:
        """
        改良版: 集計時点より未来の日付での予測結果生成を保証
        - 引数 updated_year/updated_month/updated_half は「新しく観測が到着した期間」を表す（例: 2025,5,"前半"）。
        - この更新期間を参照している将来の target (model_kind, target_month, half) を全 FeatureSet から探索し、
          該当するすべての target に対して予測を実行・DB更新する。
        - ソースが「未来」を参照する場合は前年同位（term - 24）を使う（ユーザ要件）。
        - 🔥 重要: 予測対象が集計時点より未来であることを確実に保証する。
        - look_ahead_years は探索する将来年幅（デフォルト1年、必要に応じて拡張可）。
        """

        log = logger or logging.getLogger(__name__)
        TERMS_PER_YEAR = 12 * 2  # 月ごとに前半/後半で 24 term/年

        # ヘルパー: (year,month,half) -> term index (単調増加の整数)
        def to_term_index(y: int, m: int, half: str) -> int:
            half_idx = 0 if (half == "前半" or half is None) else 1
            return y * TERMS_PER_YEAR + (m - 1) * 2 + half_idx

        # ヘルパー: term index -> (year, month, half)
        def from_term_index(idx: int):
            y = idx // TERMS_PER_YEAR
            rem = idx % TERMS_PER_YEAR
            m = rem // 2 + 1
            half = "前半" if (rem % 2 == 0) else "後半"
            return y, m, half

        # 集計時点のterm_indexを計算
        updated_idx = to_term_index(updated_year, updated_month, updated_half)
        log.info("update_predictions_for_period: updated_term=%s (idx=%d)", 
                 f"{updated_year}-{updated_month} {updated_half}", updated_idx)

        # 予測対象の最小term_index（集計時点の次期以降）
        min_prediction_idx = updated_idx + 1
        
        # アクティブなモデルバージョンを取得
        active_versions = ForecastModelVersion.objects.filter(is_active=True).select_related('model_kind')
        if not active_versions.exists():
            log.info("update_predictions_for_period: no active model versions found")
            return 0

        # 🔥 Step1: 重回帰分析によるモデル更新（refit_models=Trueの場合）
        updated_models = {}  # model_version.id -> updated_model_version
        
        if refit_models:
            log.info("=== Step1: モデル再学習開始 ===")
            
            # 各アクティブモデルについて重回帰分析を実行
            for active_version in active_versions:
                try:
                    # FeatureSetから説明変数IDを取得
                    fs_qs = ForecastModelFeatureSet.objects.filter(
                        model_kind=active_version.model_kind,
                        target_month=active_version.target_month
                    ).select_related('variable')
                    
                    if variable_ids:
                        fs_qs = fs_qs.filter(variable_id__in=variable_ids)

                    feature_variable_ids = list(fs_qs.values_list('variable_id', flat=True))
                    
                    if not feature_variable_ids:
                        log.warning("No feature variables found for model_version=%s", active_version.id)
                        continue

                    log.info("Refitting model: model_kind=%s, target_month=%s, variables=%s", 
                            active_version.model_kind.tag_name, active_version.target_month, feature_variable_ids)

                    # 重回帰分析の実行（既存モデルを更新）
                    updated_model = self._refit_existing_model(
                        active_version, 
                        feature_variable_ids,
                        log
                    )
                    
                    if updated_model:
                        updated_models[active_version.id] = updated_model
                        log.info("Successfully refitted model_version=%s", active_version.id)
                    else:
                        log.warning("Failed to refit model_version=%s", active_version.id)
                        
                except Exception as e:
                    log.error("Error refitting model_version=%s: %s", active_version.id, e, exc_info=True)
            
            log.info("=== Step1完了: %d個のモデルを更新 ===", len(updated_models))

        # 🔥 Step2: 予測対象月ベースでモデルを探索
        candidate_targets: Dict[tuple, tuple] = {}  # (mk_id, ty, tmonth, half) -> (model_version, fs_list)
        
        # 全ての予測対象月（1-12月）について処理
        for target_month in range(1, 13):
            # この月に対応するアクティブなモデルバージョンを取得
            month_active_versions = active_versions.filter(target_month=target_month)
            
            for active_version in month_active_versions:
                # 更新されたモデルがあれば使用
                current_version = updated_models.get(active_version.id, active_version)
                
                # このモデルバージョンに関連するFeatureSetを取得
                qs = ForecastModelFeatureSet.objects.filter(
                    model_kind=current_version.model_kind,
                    target_month=current_version.target_month
                ).select_related('variable')
                
                if variable_ids:
                    qs = qs.filter(variable_id__in=variable_ids)

                fs_list = list(qs)
                if not fs_list:
                    log.debug("No feature sets found for model_kind=%s, target_month=%s", 
                             current_version.model_kind, target_month)
                    continue

                log.info("Found %d feature sets for model_kind=%s, target_month=%s", 
                        len(fs_list), current_version.model_kind, target_month)

                # この月の予測対象期間を生成
                for ty in range(updated_year, updated_year + look_ahead_years + 1):
                    for half in ("前半", "後半"):
                        target_idx = to_term_index(ty, target_month, half)
                        
                        # 🔥 重要: 予測対象が集計時点より未来であることを保証
                        if target_idx <= updated_idx:
                            continue  # 集計期間以前の予測はスキップ
                        
                        # 予測対象月に対応するモデルバージョンとFeatureSetを格納
                        key = (current_version.model_kind_id, ty, target_month, half)
                        candidate_targets[key] = (current_version, fs_list)
                        log.debug("Added prediction target: %s-%02d %s with model_version=%s (%d feature sets)", 
                                 ty, target_month, half, current_version.id, len(fs_list))

        if not candidate_targets:
            log.info("update_predictions_for_period: no future prediction targets found")
            return 0

        # 🔥 Step3: 予測実行と結果保存
        log.info("=== Step3: 予測実行開始 (対象数: %d) ===", len(candidate_targets))
        updated_count = 0

        # 各予測対象について処理
        for (mk_id, ty, tmonth, half), (active_version, fs_list) in candidate_targets.items():
            # 🔥 予測対象期間が集計期間より未来であることを再確認
            prediction_idx = to_term_index(ty, tmonth, half)
            if prediction_idx <= updated_idx:
                log.warning("Skipping non-future prediction: %s-%02d %s (idx=%d <= updated_idx=%d)", 
                           ty, tmonth, half, prediction_idx, updated_idx)
                continue

            # 🔥 重要: 予測対象月に対応するモデルバージョンは既に取得済み
            if not active_version:
                log.warning("update_predictions_for_period: no active model_version for model_kind_id=%s, target_month=%s", mk_id, tmonth)
                continue
            
            log.info("Processing prediction: model_kind_id=%s, model_version=%s, target=%s-%02d %s", 
                    mk_id, active_version.id, ty, tmonth, half)

            try:
                # 🎯 重要: 1つのモデルで全FeatureSetを使って1回の予測
                prediction_value = None
                
                # 統合予測の実装（ObserveServiceのロジックを使用）
                try:
                    from observe.services import ObserveService, ObserveServiceConfig
                    observe_service = ObserveService(ObserveServiceConfig(region_name=self.cfg.region_name))
                    
                    # ObserveServiceの予測メソッドを使用（24期前フォールバック付き）
                    # force_update=Trueで既存レコードの更新を許可
                    prediction_value = observe_service.predict_for_model_version(
                        model_version=active_version,
                        year=ty,
                        month=tmonth, 
                        half=half,
                        force_update=True
                    )
                    
                    log.info("ObserveService returned type: %s, value: %s", type(prediction_value), prediction_value)
                    
                    # 数値型であることを確認
                    if isinstance(prediction_value, (int, float)):
                        prediction_value = float(prediction_value)
                        log.info("Successfully generated prediction using ObserveService: %.3f", prediction_value)
                        # ObserveServiceで既にObserveReportの処理が完了しているため、後続の処理をスキップ
                        updated_count += 1
                        log.info("ObserveService completed prediction processing for model_version=%s, target=%s-%02d %s", 
                               active_version.id, ty, tmonth, half)
                        continue
                    elif hasattr(prediction_value, 'predict_price'):
                        # ObserveReportインスタンスが返された場合のフォールバック
                        log.warning("ObserveService returned ObserveReport instance, extracting predict_price")
                        prediction_value = float(prediction_value.predict_price)
                        log.info("Extracted prediction value: %.3f", prediction_value)
                        # ObserveServiceで既にObserveReportの処理が完了しているため、後続の処理をスキップ
                        updated_count += 1
                        continue
                    else:
                        log.error("ObserveService returned non-numeric value: %s, setting to None", type(prediction_value))
                        prediction_value = None
                    
                except Exception as obs_ex:
                    log.warning("ObserveService prediction failed: %s, trying fallback methods", obs_ex)
                    
                    # フォールバック1: 統合予測メソッド
                    if hasattr(self, "predict_with_features"):
                        try:
                            prediction_value = self.predict_with_features(
                                active_version, fs_list, 
                                year=ty, month=tmonth, half=half
                            )
                        except Exception as ex:
                            log.warning("predict_with_features failed: %s", ex)
                    
                    # フォールバック2: バッチ予測
                    if prediction_value is None and hasattr(self, "predict_batch"):
                        try:
                            raw = self.predict_batch(active_version, fs_list, year=ty, month=tmonth, half=half)
                            if isinstance(raw, (int, float)):
                                prediction_value = float(raw)
                            elif isinstance(raw, dict) and len(raw) == 1:
                                prediction_value = float(list(raw.values())[0])
                        except Exception as ex:
                            log.warning("predict_batch failed: %s", ex)
                    
                    # フォールバック3: 24期前データを使った予測
                    if prediction_value is None:
                        log.info("Attempting prediction with 24-period-ago fallback data")
                        try:
                            # 24期前（1年前）の同時期を計算
                            fallback_year = ty - 1
                            prediction_value = self._predict_with_fallback_data(
                                active_version, fs_list,
                                target_year=ty, target_month=tmonth, target_half=half,
                                fallback_year=fallback_year, fallback_month=tmonth, fallback_half=half
                            )
                            if prediction_value:
                                log.info("Successfully generated prediction using 24-period fallback: %.3f", prediction_value)
                        except Exception as fb_ex:
                            log.warning("24-period fallback prediction failed: %s", fb_ex)

                if prediction_value is None:
                    log.warning("Failed to generate prediction for model_kind_id=%s, target=%s-%02d %s after all attempts", 
                               mk_id, ty, tmonth, half)
                    continue

                # 🔥 重要: 1つの予測結果を1つのObserveReportとして保存
                from django.utils import timezone
                
                # 既存のObserveReportを確認（model_versionベース）
                existing_report = ObserveReport.objects.filter(
                    model_version=active_version,
                    target_year=ty,
                    target_month=tmonth, 
                    target_half=half
                ).first()
                
                if existing_report:
                    # 既存レコードを更新
                    # デバッグ: prediction_valueの型と値を確認
                    log.info("prediction_value type: %s, value: %s", type(prediction_value), prediction_value)
                    
                    # prediction_valueが正しい数値型かチェック
                    if not isinstance(prediction_value, (int, float)):
                        log.error("Invalid prediction_value type: %s, value: %s", type(prediction_value), prediction_value)
                        continue
                    
                    prediction_float = float(prediction_value)
                    
                    # モデルのRMSEを取得して信頼区間を計算
                    try:
                        model_evaluation = active_version.forecastmodelevaluation_set.latest('created_at')
                        rmse = model_evaluation.rmse
                        min_price = prediction_float - rmse
                        max_price = prediction_float + rmse
                    except:
                        # RMSEが取得できない場合は、予測値の±5%をデフォルトとして使用
                        margin = prediction_float * 0.05
                        min_price = prediction_float - margin
                        max_price = prediction_float + margin
                    
                    existing_report.predict_price = prediction_float
                    existing_report.min_price = min_price
                    existing_report.max_price = max_price
                    existing_report.updated_at = timezone.now()
                    existing_report.save()
                    updated_count += 1
                    log.info("Updated ObserveReport: model_version=%s, target_year=%d, target_month=%d, target_half=%s, value=%.3f", 
                           active_version.id, ty, tmonth, half, prediction_value)
                else:
                    if create_if_missing:
                        # デバッグ: prediction_valueの型と値を確認
                        log.info("Creating new record - prediction_value type: %s, value: %s", type(prediction_value), prediction_value)
                        
                        # prediction_valueが正しい数値型かチェック
                        if not isinstance(prediction_value, (int, float)):
                            log.error("Invalid prediction_value type for new record: %s, value: %s", type(prediction_value), prediction_value)
                            continue
                        
                        prediction_float = float(prediction_value)
                        
                        # モデルのRMSEを取得して信頼区間を計算
                        try:
                            model_evaluation = active_version.forecastmodelevaluation_set.latest('created_at')
                            rmse = model_evaluation.rmse
                            min_price = prediction_float - rmse
                            max_price = prediction_float + rmse
                        except:
                            # RMSEが取得できない場合は、予測値の±5%をデフォルトとして使用
                            margin = prediction_float * 0.05
                            min_price = prediction_float - margin
                            max_price = prediction_float + margin
                        
                        # 新規作成（未来日付）
                        ObserveReport.objects.create(
                            model_version=active_version,
                            target_year=ty,
                            target_month=tmonth,
                            target_half=half,
                            predict_price=prediction_float,
                            min_price=min_price,
                            max_price=max_price
                        )
                        updated_count += 1
                        log.info("Created ObserveReport: model_version=%s, target_year=%d, target_month=%d, target_half=%s, value=%.3f", 
                               active_version.id, ty, tmonth, half, prediction_value)

            except Exception as ex:
                log.exception("Prediction failed for model_kind_id=%s target=%s-%02d %s: %s", 
                             mk_id, ty, tmonth, half, ex)

        log.info("update_predictions_for_period: total updated_count=%d", updated_count)
        return updated_count

    def _predict_with_fallback_data(self, 
                                   model_version: ForecastModelVersion,
                                   fs_list: List[ForecastModelFeatureSet],
                                   target_year: int,
                                   target_month: int, 
                                   target_half: str,
                                   fallback_year: int,
                                   fallback_month: int,
                                   fallback_half: str) -> Optional[float]:
        """
        24期前（1年前）のデータを使って予測を実行
        データが不足している場合のフォールバック機能
        """
        log = logging.getLogger(__name__)
        
        try:
            # モデルの係数を取得
            coef_dict = {}
            const_coef = 0.0
            
            model_coefs = ForecastModelCoef.objects.filter(model_version=model_version)
            for coef in model_coefs:
                if coef.variable.name == 'const':
                    const_coef = coef.coef
                else:
                    key = f"{coef.variable.name}_{coef.variable.previous_term}"
                    coef_dict[key] = coef.coef
            
            if not coef_dict:
                log.warning("No coefficients found for model_version=%s", model_version.id)
                return None
            
            # 各FeatureSetに対応する説明変数の値を取得（24期前データを使用）
            feature_values = []
            
            for fs in fs_list:
                variable = fs.variable
                prev_term = getattr(variable, 'previous_term', 0)
                
                # 24期前の同時期からprevious_term期前のデータを取得
                source_year = fallback_year
                source_month = fallback_month
                source_half = fallback_half
                
                # previous_termの分だけ遡る
                TERMS_PER_YEAR = 24
                source_idx = (source_year * TERMS_PER_YEAR + 
                             (source_month - 1) * 2 + 
                             (0 if source_half == "前半" else 1)) - prev_term
                
                actual_year = source_idx // TERMS_PER_YEAR
                remaining = source_idx % TERMS_PER_YEAR
                actual_month = remaining // 2 + 1
                actual_half = "前半" if (remaining % 2 == 0) else "後半"
                
                # データを取得（ComputeMarket/ComputeWeatherから）
                feature_value = self._get_feature_value(
                    variable.name, actual_year, actual_month, actual_half, model_version.model_kind
                )
                
                if feature_value is not None:
                    feature_values.append((f"{variable.name}_{prev_term}", feature_value))
                    log.debug("Got fallback feature value: %s_%s = %.3f (from %d-%02d %s)", 
                             variable.name, prev_term, feature_value, actual_year, actual_month, actual_half)
                else:
                    log.warning("Could not get fallback feature value for %s_%s", variable.name, prev_term)
            
            if not feature_values:
                log.warning("No feature values available for fallback prediction")
                return None
            
            # 線形予測を計算
            prediction = const_coef
            used_features = 0
            
            for feature_key, feature_value in feature_values:
                if feature_key in coef_dict:
                    prediction += coef_dict[feature_key] * feature_value
                    used_features += 1
                    log.debug("Applied coefficient: %s * %.3f = %.3f", 
                             feature_key, feature_value, coef_dict[feature_key] * feature_value)
            
            if used_features == 0:
                log.warning("No matching coefficients found for available features")
                return None
            
            log.info("Fallback prediction calculated: %.3f (using %d features)", prediction, used_features)
            return float(prediction)
            
        except Exception as ex:
            log.exception("Error in _predict_with_fallback_data: %s", ex)
            return None

    def _refit_existing_model(self, 
                             model_version: ForecastModelVersion,
                             variable_ids: List[int],
                             logger: logging.Logger) -> Optional[ForecastModelVersion]:
        """
        既存のモデルバージョンを重回帰分析で更新
        新規作成ではなく、既存のmodel_version, coef, evaluationを更新
        """
        try:
            # 重回帰分析用のデータを準備
            X, y, variable_list = self.prepare_regression_data(
                model_version.model_kind.tag_name, 
                model_version.target_month, 
                vals=variable_ids
            )
            
            logger.info("Regression data prepared: X shape=%s, y length=%d", X.shape, len(y))
            
            # OLS実行
            import statsmodels.api as sm
            import numpy as np
            
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
            
            ssr = model.ssr  # 回帰変動
            ess = model.ess  # 残差変動
            tss = model.centered_tss  # 全変動
            msr = ssr / df_model  # 回帰分散
            mse = ess / df_resid  # 残差分散
            
            with transaction.atomic():
                # 1. 既存のForecastModelEvaluationを更新
                try:
                    evaluation = model_version.forecastmodelevaluation_set.latest('created_at')
                    evaluation.multi_r = float(np.sqrt(model.rsquared))
                    evaluation.heavy_r2 = float(model.rsquared)
                    evaluation.adjusted_r2 = float(model.rsquared_adj)
                    evaluation.sign_f = float(model.f_pvalue)
                    evaluation.standard_error = float(np.sqrt(mse))
                    evaluation.rmse = float(rmse)
                    evaluation.reg_variation = float(ssr)
                    evaluation.reg_variance = float(msr)
                    evaluation.res_variation = float(ess)
                    evaluation.res_variance = float(mse)
                    evaluation.total_variation = float(tss)
                    evaluation.updated_at = timezone.now()
                    evaluation.save()
                    logger.info("Updated ForecastModelEvaluation: id=%s", evaluation.id)
                except:
                    # 評価レコードが存在しない場合は新規作成
                    evaluation = ForecastModelEvaluation.objects.create(
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
                    logger.info("Created new ForecastModelEvaluation: id=%s", evaluation.id)

                # 2. 既存のForecastModelCoefを更新
                se = model.bse
                tv = model.tvalues
                pv = model.pvalues
                
                # 変数辞書を作成
                variable_dict = {}
                for var_info in variable_list:
                    var_name = var_info['name']
                    prev_term = var_info['previous_term']
                    try:
                        var_obj = ForecastModelVariable.objects.get(name=var_name, previous_term=prev_term)
                        variable_dict[f"{var_name}_{prev_term}"] = var_obj
                    except ForecastModelVariable.DoesNotExist:
                        logger.warning("Variable not found: %s_%s", var_name, prev_term)
                
                # 定数項のための特別処理
                const_var, _ = ForecastModelVariable.objects.get_or_create(
                    name='const',
                    previous_term=0
                )
                
                # 既存の係数をすべて削除してから再作成
                ForecastModelCoef.objects.filter(model_version=model_version).delete()
                
                # 係数の作成
                for name in model.params.index:
                    if name == 'const':
                        variable = const_var
                        is_segment = True  # 🔥 定数項はis_segment=True
                    else:
                        variable = variable_dict.get(name)
                        if not variable:
                            logger.warning("Could not find variable for coefficient: %s", name)
                            continue
                        is_segment = False
                    
                    ForecastModelCoef.objects.create(
                        model_version=model_version,
                        is_segment=is_segment,
                        variable=variable,
                        coef=float(model.params[name]),
                        value_t=float(tv.get(name, np.nan)) if hasattr(tv, "get") else float(tv[name]),
                        sign_p=float(pv.get(name, np.nan)) if hasattr(pv, "get") else float(pv[name]),
                        standard_error=float(se.get(name, np.nan)) if hasattr(se, "get") else float(se[name])
                    )

                # 3. ModelVersionのupdated_atを更新
                from django.utils import timezone
                model_version.updated_at = timezone.now()
                model_version.save()
                
                logger.info("Successfully updated model_version=%s with new coefficients", model_version.id)
                return model_version
                
        except Exception as e:
            logger.error("Error in _refit_existing_model: %s", e, exc_info=True)
            return None

    def _get_feature_value(self, variable_name: str, year: int, month: int, half: str, model_kind) -> Optional[float]:
        """
        指定された変数・期間の特徴量値を取得
        """  
        try:
            # 変数名に基づいてデータソースを判定
            if variable_name in ['気温', '平均気温', '最高気温', '最低気温', '降水量', '日照時間', '湿度']:
                # ComputeWeatherから取得
                from compute.models import ComputeWeather
                weather = ComputeWeather.objects.filter(
                    target_year=year,
                    target_month=month, 
                    target_half=half
                ).first()
                
                if weather:
                    if variable_name in ['気温', '平均気温']:
                        return weather.mean_temp
                    elif variable_name == '最高気温':
                        return weather.max_temp
                    elif variable_name == '最低気温':
                        return weather.min_temp
                    elif variable_name == '降水量':
                        return weather.sum_precipitation
                    elif variable_name == '日照時間':
                        return weather.sunshine_duration
                    elif variable_name == '湿度':
                        return weather.ave_humidity
                        
            elif variable_name in ['価格', '平均価格', 'キャベツ価格', 'トマト価格', '白菜価格']:
                # ComputeMarketから取得
                from compute.models import ComputeMarket
                market = ComputeMarket.objects.filter(
                    target_year=year,
                    target_month=month,
                    target_half=half,
                    vegetable=model_kind.vegetable
                ).first()
                
                if market:
                    return market.average_price or market.source_price
                    
        except Exception as ex:
            logging.getLogger(__name__).warning("Error getting feature value for %s: %s", variable_name, ex)
        
        return None


# 使用例
if __name__ == "__main__":
    # このスクリプトを直接実行した場合に実行されるコード
    # import os
    # import sys
    # import django
    
    # Djangoの設定を読み込む
    # sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
    # os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
    # django.setup()
    
    # 実行設定
    config = ForecastOLSConfig(region_name='広島', deactivate_previous=True)
    
    # 実行クラスの初期化
    runner = ForecastOLSRunner(config=config)

    logger = logging.getLogger(__name__)
    
    # キャベツ春まきの5月のモデルを実行
    try:
        logger.info("キャベツ春まき、5月のモデルを実行中...")
        model_version = runner.fit_and_persist("キャベツ春まき", 5)
        if model_version:
            logger.info(f"モデルバージョンID: {model_version.id} が作成されました")
        else:
            logger.info("モデルの作成に失敗しました")
    except Exception as e:
        logger.info(f"エラーが発生しました: {str(e)}")
    
    # 複数のモデルと月を一度に実行
    try:
        logger.info("\n複数のモデルを実行中...")
        models_to_run = ["キャベツ春まき", "キャベツ秋まき"]
        months_to_run = [5, 11]  # 5月と11月
        
        results = runner.run_forecast_analysis(models_to_run, months_to_run)
        
        # 結果の表示
        for model_name, month_results in results.items():
            logger.info(f"\nモデル: {model_name}")
            for month, result in month_results.items():
                status = "成功" if result['success'] else "失敗"
                model_id = result['model_version_id'] or "N/A"
                error = result['error'] or "なし"
                logger.info(f"  月: {month} - 状態: {status}, モデルID: {model_id}, エラー: {error}")
    except Exception as e:
        logger.info(f"実行エラー: {str(e)}")