import csv
import os
import datetime
import logging
import glob
import re
from pathlib import Path
from typing import Dict, List, Optional, Union, Any, Tuple, Set
from django.conf import settings
from django.db import transaction
from config.storage.azure_blob import get_blob_service_client

from .models import Vegetable, Region, IngestMarket, IngestWeather

logger = logging.getLogger(__name__)

class DataParser:
    """
    データ解析用のベースクラス
    Azuriteのファイルやローカルファイルからデータを解析する機能を提供
    """
    
    # @staticmethod
    # def get_blob_client():
    #     """
    #     Azure BlobServiceClientを取得する
    #     """
    #     try:
    #         conn_str = settings.AZURE_CONNECTION_STRING
    #         container_name = settings.AZURE_CONTAINER
    #         bsc = BlobServiceClient.from_connection_string(conn_str)
    #         container = bsc.get_container_client(container_name)
    #         return container
    #     except Exception as e:
    #         logger.error(f"Azure Blob接続エラー: {str(e)}")
    #         return None
    
    @staticmethod
    def parse_date_from_filename(filename: str) -> Optional[datetime.date]:
        """
        ファイル名から日付を抽出する
        例: 
        - price/2022/01/2022-01-05.txt -> 2022-01-05
        - 2022-01-05.txt -> 2022-01-05
        - weather/2022/01/2022_01_mid.csv -> 2022-01-15
        """
        try:
            # ファイル名からファイル拡張子を除いた部分を取得
            base_name = os.path.basename(filename)
            name_without_ext = os.path.splitext(base_name)[0]
            
            # price/YYYY/MM/YYYY-MM-DD.txt または YYYY-MM-DD.txt パターン
            date_match = re.search(r'(\d{4})-(\d{2})-(\d{2})', name_without_ext)
            if date_match:
                year = int(date_match.group(1))
                month = int(date_match.group(2))
                day = int(date_match.group(3))
                return datetime.date(year, month, day)
                
            # weather/YYYY/MM/YYYY_MM_mid.csv または YYYY_MM_mid.csv パターン
            match = re.search(r'(\d{4})_(\d{2})_(mid|last)', name_without_ext)
            if match:
                year = int(match.group(1))
                month = int(match.group(2))
                # 月の中旬または月末の日付を返す（簡易実装）
                if match.group(3) == 'mid':
                    day = 15  # 中旬を15日とする
                else:
                    # 月末の日を計算
                    if month == 12:
                        next_month = datetime.date(year+1, 1, 1)
                    else:
                        next_month = datetime.date(year, month+1, 1)
                    last_day = (next_month - datetime.timedelta(days=1)).day
                    day = last_day
                return datetime.date(year, month, day)
            
            # ディレクトリ構造からの抽出を試みる（例：price/2022/01/file.txt）
            path_parts = filename.split('/')
            if len(path_parts) >= 3:
                # 最後から3番目と2番目の部分が年と月を表しているか確認
                try:
                    year_part = path_parts[-3]
                    month_part = path_parts[-2]
                    if year_part.isdigit() and month_part.isdigit():
                        year = int(year_part)
                        month = int(month_part)
                        # 日付は不明なので月の初日とする
                        return datetime.date(year, month, 1)
                except (ValueError, IndexError):
                    pass
                
            return None
        except (ValueError, IndexError) as e:
            logger.error(f"日付の解析エラー: {filename}, {str(e)}")
            return None
    
    @staticmethod
    def get_file_content(file_path: str, is_azure_path: bool = False) -> Optional[str]:
        """
        ファイルの内容を取得する
        is_azure_path=Trueの場合、file_pathはAzureのBlobパスとして扱う
        """
        logger.info(f"ファイル読み込み開始: {file_path} (Azure: {is_azure_path})")
        
        # Azure Blobからの読み込み
        if is_azure_path:
            try:
                container = get_blob_service_client()
                if not container:
                    logger.error("Azure Blob接続に失敗しました")
                    return None
                
                # ファイルが存在するか確認
                blob_client = container.get_blob_client(file_path)
                try:
                    blob_properties = blob_client.get_blob_properties()
                except Exception as e:
                    logger.error(f"Azureファイルが存在しません: {file_path}, {str(e)}")
                    return None
                
                # 内容をダウンロード
                blob_content = blob_client.download_blob()
                content = blob_content.readall()
                
                # バイナリデータを文字列に変換
                try:
                    content_str = content.decode('utf-8')
                    logger.info(f"Azure Blobファイル読み込み成功(UTF-8): {file_path}, サイズ: {len(content_str)} バイト")
                    return content_str
                except UnicodeDecodeError:
                    try:
                        content_str = content.decode('shift_jis')
                        logger.info(f"Azure Blobファイル読み込み成功(Shift-JIS): {file_path}, サイズ: {len(content_str)} バイト")
                        return content_str
                    except UnicodeDecodeError:
                        content_str = content.decode('utf-8', errors='replace')
                        logger.info(f"Azure Blobファイル読み込み成功(エラー置換): {file_path}, サイズ: {len(content_str)} バイト")
                        return content_str
            except Exception as e:
                logger.error(f"Azure Blob読み込みエラー: {file_path}, {str(e)}")
                return None
        
        # ローカルファイルからの読み込み
        if not os.path.exists(file_path):
            logger.error(f"ローカルファイルが存在しません: {file_path}")
            return None
            
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
                logger.info(f"ファイル読み込み成功(UTF-8): {file_path}, サイズ: {len(content)} バイト")
                return content
        except Exception as e:
            logger.error(f"ファイル読み込みエラー(UTF-8): {file_path}, {str(e)}")
            try:
                # UTF-8で失敗した場合はShift-JISで試す
                with open(file_path, 'r', encoding='shift_jis') as f:
                    content = f.read()
                    logger.info(f"ファイル読み込み成功(Shift-JIS): {file_path}, サイズ: {len(content)} バイト")
                    return content
            except Exception as e2:
                logger.error(f"ファイル読み込みエラー(Shift-JIS): {file_path}, {str(e2)}")
                
                # バイナリモードでも試す
                try:
                    with open(file_path, 'rb') as f:
                        binary_content = f.read()
                        logger.info(f"バイナリ読み込み成功: {file_path}, サイズ: {len(binary_content)} バイト")
                        # バイナリデータをUTF-8文字列に変換（エラーは置き換え）
                        return binary_content.decode('utf-8', errors='replace')
                except Exception as e3:
                    logger.error(f"バイナリ読み込みエラー: {file_path}, {str(e3)}")
                    return None
                
    @staticmethod
    def _parse_csv_file(file_path: str) -> List[Dict[str, str]]:
        """
        CSVファイルを解析して辞書のリストを返す
        """
        rows = []
        try:
            content = DataParser.get_file_content(file_path)
            if not content:
                return rows
                
            # CSVReader用にStringIOに変換する必要はない
            # 直接ファイルを開いてCSV処理
            with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    rows.append(row)
        except Exception as e:
            logger.error(f"CSV解析エラー: {file_path}, {str(e)}")
        
        return rows
        
    @staticmethod
    def _parse_txt_file(file_path: str) -> Optional[str]:
        """
        テキストファイルを解析して内容を返す
        """
        return DataParser.get_file_content(file_path)


class MarketDataParser(DataParser):
    """
    市場価格データの解析を行うクラス
    """
    # FIXME: なぜか中央卸売市場はDB参照で，野菜コードは直書き
    ALL_V_KINDS_NUMBER = 56
    V_CODES = ['30100', '31100', '31700', '34100', '34300', '34400', '36200', '36600']
    # V_CODES = ['31700', '31100', '30100', '36600', '36200', '34400', '34300', '34100']

    @staticmethod
    def _sort_data(data):
        try:
            m_data = data["Markets"]
            logger.info("_sort_dataメソッド処理開始")
            logger.debug(f"データタイプ: {type(data)}")
            
            # JSONデータの構造を柔軟に扱うためのデータ正規化
            markets_data = None
            
            # データ構造の検出と正規化
            if isinstance(data, dict):
                markets_data = data.get("Markets", [])
            # FIXME: 不要なコードの削除
            # if isinstance(m_data, list) and len(data) > 0:
            #     # リスト形式の場合
            #     logger.info("データはリスト形式です")
            #     first_item = data[0]
            #     if isinstance(first_item, dict):
            #         # リストの最初の要素がオブジェクトの場合
            #         if "Markets" in first_item:
            #             markets_data = first_item.get("Markets", [])
            #             logger.info(f"リストの最初の要素からMarketsを抽出: {len(markets_data)}件")
            #         else:
            #             # Markets直下の要素がリストの場合
            #             logger.info(f"データはマーケットのリストとして処理")
            #             markets_data = data
            
            # elif isinstance(m_data, dict):
            #     # 辞書形式の場合
            #     logger.info("データは辞書形式です")
            #     # 利用可能なキーをログ
            #     top_keys = list(data.keys())
            #     logger.info(f"トップレベルのキー: {top_keys}")
                
            #     # 直接Marketsキーがある場合
            #     if "Markets" in data:
            #         markets_data = data.get("Markets", [])
            #         logger.info(f"ルート辞書からMarketsを抽出: {len(markets_data)}件")
            #     # データが直接マーケットリストである場合
            #     elif "MarketCode" in data:
            #         logger.info("データは単一市場データとして処理")
            #         markets_data = [data]
            #     # 他の階層構造の場合
            #     else:
            #         for key in data:
            #             if isinstance(data[key], dict) and "Markets" in data[key]:
            #                 markets_data = data[key].get("Markets", [])
            #                 logger.info(f"ネストされた辞書からMarketsを抽出: {len(markets_data)}件")
            #                 break
            #             elif isinstance(data[key], list) and len(data[key]) > 0:
            #                 # リスト内の最初の要素を確認
            #                 sample = data[key][0]
            #                 if isinstance(sample, dict) and "MarketCode" in sample:
            #                     markets_data = data[key]
            #                     logger.info(f"ネストされたリストからマーケットを抽出: {len(markets_data)}件")
            #                     break
            
            # マーケットデータが見つからなかった場合
            if not markets_data:
                logger.error("マーケットデータが見つかりません")
                # データ構造をダンプしてデバッグ
                if isinstance(data, dict):
                    logger.debug(f"データキー: {list(data.keys())}")
                    for key in list(data.keys())[:3]:  # 最初の3つのキーのみ
                        logger.debug(f"キー '{key}' の値の型: {type(data[key])}")
                elif isinstance(data, list) and len(data) > 0:
                    logger.debug(f"リストの最初の要素の型: {type(data[0])}")
                    if isinstance(data[0], dict):
                        logger.debug(f"最初の要素のキー: {list(data[0].keys())}")
                return []
            
            # マーケットコードを確認
            market_codes = []
            for market in markets_data[:min(10, len(markets_data))]:
                if isinstance(market, dict) and "MarketCode" in market:
                    market_codes.append(market.get("MarketCode"))
            logger.info(f"検出されたマーケットコード: {market_codes}")
            
            # 広島中央卸売市場縛り
            region = Region.objects.filter(name="広島").first()
            if not region:
                logger.error("広島地域が見つかりません")
                return []
                
            logger.info(f"広島市場コード: {region.market_code}")
            
            # 市場コード一致確認
            region_data = None
            market_codes_found = []
            
            # 指定された市場コードだけを使用する
            for market in markets_data:
                if isinstance(market, dict):
                    market_code = market.get("MarketCode")
                    if market_code:
                        market_codes_found.append(market_code)
                    if market_code == region.market_code:
                        region_data = market
                        logger.info(f"市場コード {region.market_code} に一致するデータが見つかりました")
                        break
            
            # 指定コードが見つからない場合は何もしない（代替の市場コードは使用しない）
            if not region_data:
                logger.warning(f"市場コード {region.market_code} が見つかりません。要件に従い他の市場コードは使用しません。")
            
            # どの市場コードも見つからない場合
            if not region_data:
                logger.error(f"市場コード {region.market_code} に一致するマーケットデータが見つかりません")
                if market_codes_found:
                    logger.error(f"利用可能な市場コード: {market_codes_found}")
                return []
                
            # 広島産地縛り
            p_area_code = region.p_area_code
            logger.info(f"広島産地コード: {p_area_code}")
            
            # アイテムデータ取得
            items_data = []
            if "Items" in region_data and isinstance(region_data["Items"], list):
                items_data = region_data["Items"]
                logger.info(f"アイテムデータの長さ: {len(items_data)}")
            else:
                logger.error("地域データにItemsキーがないか、リストではありません")
                logger.debug(f"地域データのキー: {list(region_data.keys()) if isinstance(region_data, dict) else 'dictではありません'}")
                return []
                
            # 野菜8品目縛り
            v_data = []
            item_count = min(len(items_data), MarketDataParser.ALL_V_KINDS_NUMBER)
            logger.info(f"処理する野菜アイテム数: {item_count}")
            logger.info(f"対象野菜コード: {MarketDataParser.V_CODES}")
            
            for v in items_data:
                item_code = v.get("ItemCode", "不明")
                if item_code in MarketDataParser.V_CODES:
                    logger.info(f"野菜コードマッチ: {item_code}")
                    v_data.append(v)
            
            logger.info(f"マッチした野菜データ数: {len(v_data)}")
            
            if not v_data:
                logger.warning("マッチする野菜データがありません")
                return []

            # 産地縛り
            v_h_data = []
            for i, veg in enumerate(v_data):
                if "Details" not in veg or not isinstance(veg["Details"], list):
                    logger.warning(f"野菜データ[{i}]にDetailsキーがないかリストではありません")
                    continue
                    
                v = veg["Details"]
                logger.info(f"野菜[{i}]の詳細データ数: {len(v)}")
                
                # 野菜コードをアイテムに追加
                item_code = veg.get("ItemCode", "不明")
                
                for j, detail in enumerate(v):
                    # ItemCodeをコピー
                    if "ItemCode" not in detail and item_code != "不明":
                        detail["ItemCode"] = item_code
                        
                    prod_area_code = detail.get("ProductionAreaCode", "不明")
                    if prod_area_code == p_area_code:
                        logger.info(f"産地コードマッチ[{i}][{j}]: {prod_area_code}")
                        v_h_data.append(detail)
            
            logger.info(f"最終的な産地フィルター後のデータ数: {len(v_h_data)}")
            return v_h_data
            
        except Exception as e:
            import traceback
            logger.error(f"_sort_dataメソッドでエラーが発生: {str(e)}")
            logger.error(traceback.format_exc())
            return []
    
    @staticmethod
    def _format_data_to_array(data):
        try:
            logger.info("_format_data_to_array処理開始")
            logger.info(f"入力データ数: {len(data) if data else 0}")
            
            if not data:
                logger.warning("データが空です")
                return []
                
            # 結果配列（シンプルに1アイテム1配列にする）
            result = []
            
            # _sort_dataですでに適切に加工されている場合は、
            # 各データを個別の配列として扱う
            for price_data in data:
                if "ItemCode" in price_data:
                    # データがすでにItemCodeを持っている場合、そのまま配列として追加
                    result.append([price_data])
                    logger.info(f"アイテムコード {price_data['ItemCode']} のデータを追加")
                else:
                    # ItemCodeがない場合でも、データが利用可能であればログを出して追加
                    available_keys = list(price_data.keys())
                    logger.warning(f"ItemCodeがないデータを検出: 利用可能なキー {available_keys}")
                    if len(available_keys) > 2:  # 最低限のデータがあることを確認
                        logger.info("ItemCodeがなくても十分なデータがあるため処理を続行")
                        result.append([price_data])
            
            logger.info(f"生成された配列の数: {len(result)}")
            return result
            
        except Exception as e:
            import traceback
            logger.error(f"_format_data_to_arrayメソッドでエラーが発生: {str(e)}")
            logger.error(traceback.format_exc())
            return []

    # TODO: データの配列ソート
    @staticmethod
    def _use_data_to_array(data):
        try:
            if not data or len(data) == 0:
                logger.warning("データ配列が空です")
                return None
                
            v_code = data[0].get("ItemCode", "")
            if not v_code:
                logger.warning("ItemCodeが取得できません")
                return None  # 条件に合わない場合はNoneを返す
                
            logger.info(f"野菜コード: {v_code}, データ数: {len(data)}")
            
            # 野菜コードに基づいて最適なデータを選択
            if v_code == MarketDataParser.V_CODES[0]:  # ダイコン
                # ダイコンの場合はデフォルトで最初の要素を選択
                logger.info(f"キャベツ: 最初のデータを選択")
                return data[0]
                
            elif v_code == MarketDataParser.V_CODES[1]:  # ハクサイ
                # ハクサイの場合はデフォルトで最初の要素を選択
                logger.info(f"ハクサイ: 最初のデータを選択")
                return data[0]
                
            elif v_code == MarketDataParser.V_CODES[2]:  # キャベツ
                # 品種名がNullのものを優先
                for item in data:
                    if item.get("VarietyName") is None:
                        logger.info(f"ダイコン: VarietyName=None のデータを選択")
                        return item
                # 条件に合うものがなければNoneを返す
                logger.info(f"ダイコン: 条件に合うデータがないためNoneを返す")
                return None
                
            elif v_code == MarketDataParser.V_CODES[3]:  # きゅうり
                # グレードA、クラスMのものを優先
                for item in data:
                    if item.get("Grade") == "A" and item.get("Class") == "M":
                        logger.info(f"トマト: Grade=A, Class=M のデータを選択")
                        return item
                # 条件に合うものがなければNoneを返す
                logger.info(f"トマト: 条件に合うデータがないためNoneを返す")
                return None
                
            elif v_code == MarketDataParser.V_CODES[4]:  # なす
                # グレードA、クラスLのものを優先
                for item in data:
                    if item.get("Grade") == "A" and item.get("Class") == "L":
                        logger.info(f"キュウリ: Grade=A, Class=L のデータを選択")
                        return item
                # 次にグレードAの最後のデータを探す
                a_grades = [i for i, item in enumerate(data) if item.get("Grade") == "A"]
                if a_grades:
                    last_a = max(a_grades)
                    logger.info(f"キュウリ: 最後のGrade=A データを選択 (インデックス={last_a})")
                    return data[last_a]
                # 次に「ﾕｳ」の最後のデータを探す
                yu_grades = [i for i, item in enumerate(data) if item.get("Grade") == "ﾕｳ"]
                if yu_grades:
                    last_yu = max(yu_grades)
                    logger.info(f"キュウリ: 最後のGrade=ﾕｳ データを選択 (インデックス={last_yu})")
                    return data[last_yu]
                # 条件に合うものがなければNoneを返す
                logger.info(f"キュウリ: 条件に合うデータがないためNoneを返す")
                return None
                
            elif v_code == MarketDataParser.V_CODES[5]:  # トマト
                # 条件の優先順位に従ってデータを探す
                for item in data:
                    if item.get("Class") == "ｺﾞｸｼ":
                        logger.info(f"ナス: Class=ｺﾞｸｼ のデータを選択")
                        return item
                for item in data:
                    if item.get("Grade") == "C" and item.get("Class") == "M":
                        logger.info(f"ナス: Grade=C, Class=M のデータを選択")
                        return item
                for item in data:
                    if item.get("Grade") == "A" and item.get("Class") == "15":
                        logger.info(f"ナス: Grade=A, Class=15 のデータを選択")
                        return item
                for item in data:
                    if item.get("Grade") == "A" and item.get("Class") == "3L":
                        logger.info(f"ナス: Grade=A, Class=3L のデータを選択")
                        return item
                # 条件に合うものがなければNoneを返す
                logger.info(f"ナス: 条件に合うデータがないためNoneを返す")
                return None
                
            elif v_code == MarketDataParser.V_CODES[6]:  # ばれいしょ
                # グレードが「ｼﾕｳ」以外のものを探す
                for item in data:
                    if item.get("Grade") != "ｼﾕｳ":
                        logger.info(f"ピーマン: Grade≠ｼﾕｳ のデータを選択")
                        return item
                # 条件に合うものがなければNoneを返す
                logger.info(f"ピーマン: 条件に合うデータがないためNoneを返す")
                return None
                
            elif v_code == MarketDataParser.V_CODES[7]:  # 玉ねぎ
                # グレードがNULL以外、クラスとバリエティ名がNULLのデータを探す
                for item in data:
                    if (item.get("Grade") is not None and 
                        item.get("Class") is None and 
                        item.get("VarietyName") is None):
                        logger.info(f"ジャガイモ: Grade≠NULL, Class=NULL, VarietyName=NULL のデータを選択")
                        return item
                # 条件に合うものがなければNoneを返す
                logger.info(f"ジャガイモ: 条件に合うデータがないためNoneを返す")
                return None
                
            else:
                # 未対応の野菜コードの場合は、Noneを返す
                logger.warning(f"未対応の野菜コード: {v_code}, Noneを返す")
                return None
                
        except Exception as e:
            # エラー発生時はログに記録し、Noneを返す
            logger.error(f"_use_data_to_arrayでエラーが発生: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return None
    
    @staticmethod
    def _parse_price_objects_pattern_one(data):
        logger.info("パターン1での価格データの解析を開始")
        data = data[0]
        item_code = data["ItemCode"]
        wpp = data["WeightPerPackage"]
        if wpp == None:
            logger.warning(f"WeightPerPackageがNullです。デフォルト値1を使用します。ItemCode={item_code}")
            wpp = 1
        logger.debug(f"パターン1解析: ItemCode={item_code}, WeightPerPackage={wpp}")
            
        h_price = 0
        m_price = 0
        l_price = 0
        volume = data["IncomingVolume"]

        if volume == None:
            volume = 0.0

        # h_price, m_price, l_priceの算出
        if data["MediumPrice"] == None and data["HighPrice"] == None and data["LowPrice"] == None:
            return None
        
        if data["MediumPrice"] == None:
            h_price = data["HighPrice"] / wpp
            l_price = data["LowPrice"] / wpp
            a_price = (h_price + l_price) / 2
        elif data["HighPrice"] == None and data["LowPrice"] == None:
            m_price = data["MediumPrice"] / wpp
            a_price = m_price
        elif data["HighPrice"] == None:
            m_price = data["MediumPrice"] / wpp
            l_price = data["LowPrice"] / wpp
            a_price = (m_price + l_price) / 2
        elif data["LowPrice"] == None:
            m_price = data["MediumPrice"] / wpp
            h_price = data["HighPrice"] / wpp
            a_price = (m_price + h_price) / 2
        else:
            h_price = data["HighPrice"] / wpp
            m_price = data["MediumPrice"] / wpp
            l_price = data["LowPrice"] / wpp
            a_price = (h_price + m_price + l_price) / 3

        # source_priceの算出
        if m_price == 0:
            s_price = (h_price + l_price) / 2
        else:
            s_price = m_price
        
        price = {
            "target_date": data["TargetDate"],
            "item_code": item_code,
            "high_price": h_price,
            "medium_price": m_price,
            "low_price": l_price,
            "average_price": a_price,
            "source_price": s_price,
            "arrival_amount": data["IncomingVolume"],
            "weight_per": wpp,
            "volume": volume,
            "grade": data["Grade"],
            "class": data["Class"],
            "trend": data["MarketTrend"],
            "variety_name": data["VarietyName"],
        }
        return price
    
    @staticmethod
    def _parse_price_objects_pattern_two(data):
        try:
            logger.info("_parse_price_objects_pattern_two処理開始")
            logger.debug(f"入力データキー: {list(data.keys())}")
            
            # 必須フィールドの存在確認
            price_fields_exist = False
            # 最低限必要な価格情報のいずれかが存在するか確認
            if "MediumPrice" in data or "HighPrice" in data or "LowPrice" in data:
                price_fields_exist = True
            
            if not price_fields_exist:
                logger.warning("価格フィールドがありません、デフォルト値を設定します")
                logger.info(f"利用可能なキー: {list(data.keys())}")
                # デフォルト値を設定
                m_price = 100
                a_price = 100
                s_price = 100
                h_price = 100
                l_price = 0
                
                # ItemCodeの処理（必須でない場合はデフォルト値）
                item_code = data.get("ItemCode", "00000")  # デフォルト値設定
                logger.info(f"アイテムコード: {item_code}")
                
                # WeightPerPackageの処理
                wpp = data.get("WeightPerPackage", 1)
                if wpp is None or wpp == 0:
                    wpp = 1
                
                # IncomingVolumeの処理
                volume = data.get("IncomingVolume", 0)
                if volume is None:
                    volume = 0.0
                
                # 追加のフィールドを確認（デフォルト値を設定）
                from datetime import datetime
                target_date = data.get("TargetDate", datetime.now().strftime("%Y-%m-%d"))
                grade = data.get("Grade", "")
                class_val = data.get("Class", "")
                trend = data.get("MarketTrend", "")
                variety_name = data.get("VarietyName", "")
                
                price = {
                    "target_date": target_date,
                    "item_code": item_code,
                    "high_price": h_price,
                    "medium_price": m_price,
                    "low_price": l_price,
                    "average_price": a_price,
                    "source_price": s_price,
                    "arrival_amount": volume,
                    "weight_per": wpp,
                    "volume": volume,
                    "grade": grade,
                    "class": class_val,
                    "trend": trend,
                    "variety_name": variety_name,
                }
                
                logger.info("デフォルト価格データの生成完了")
                return price
            
            # ItemCodeの処理（必須でない場合はデフォルト値）
            item_code = data.get("ItemCode", "00000")  # デフォルト値設定
            logger.info(f"アイテムコード: {item_code}")
            
            # WeightPerPackageの処理
            wpp = data.get("WeightPerPackage", 1)
            if wpp is None or wpp == 0:
                logger.info("WeightPerPackageが未設定または0、1を使用")
                wpp = 1
            else:
                logger.info(f"WeightPerPackage: {wpp}")
                
            h_price = 0
            m_price = 0
            l_price = 0
            
            # IncomingVolumeの処理
            volume = data.get("IncomingVolume", 0)
            logger.info(f"IncomingVolume: {volume}")

            if volume is None:
                logger.info("Volumeが未設定、0.0を使用")
                volume = 0.0

            # 価格データのログと数値変換
            high_price = data.get("HighPrice")
            medium_price = data.get("MediumPrice")
            low_price = data.get("LowPrice")
            
            # 文字列からfloatへの変換（stringの場合）
            try:
                if isinstance(high_price, str) and high_price.strip():
                    high_price = float(high_price.replace(',', ''))
                if isinstance(medium_price, str) and medium_price.strip():
                    medium_price = float(medium_price.replace(',', ''))
                if isinstance(low_price, str) and low_price.strip():
                    low_price = float(low_price.replace(',', ''))
            except ValueError as e:
                logger.error(f"価格データの変換エラー: {str(e)}")
                
            logger.info(f"変換後 - MediumPrice: {medium_price}, HighPrice: {high_price}, LowPrice: {low_price}")
                
            # h_price, m_price, l_priceの算出（None、存在しない場合を考慮）
            if (medium_price is None or medium_price == 0) and (high_price is None or high_price == 0) and (low_price is None or low_price == 0):
                logger.warning("全ての価格データがNoneまたは0です")
                # 少なくとも1つは0より大きい値を設定
                if "AveragePrice" in data and data["AveragePrice"] is not None and data["AveragePrice"] > 0:
                    m_price = data["AveragePrice"] / wpp
                    a_price = m_price
                    logger.info(f"全価格未設定だがAveragePriceを使用: m_price={m_price}, a_price={a_price}")
                else:
                    # 価格データがない場合はデフォルト値を設定
                    m_price = 100
                    a_price = 100
                    logger.warning(f"有効な価格データがないためデフォルト値を使用: m_price={m_price}, a_price={a_price}")
            elif medium_price is None or medium_price == 0:
                # MediumPriceが設定されていない場合
                if high_price is not None and high_price > 0 and low_price is not None and low_price > 0:
                    h_price = high_price / wpp
                    l_price = low_price / wpp
                    a_price = (h_price + l_price) / 2
                    logger.info(f"MediumPrice未設定: h_price={h_price}, l_price={l_price}, a_price={a_price}")
                elif high_price is not None and high_price > 0:
                    # LowPriceもない場合はHighPriceだけ使用
                    h_price = high_price / wpp
                    a_price = h_price
                    logger.info(f"MediumPrice,LowPrice未設定: h_price={h_price}, a_price={a_price}")
                elif low_price is not None and low_price > 0:
                    # HighPriceもない場合はLowPriceだけ使用
                    l_price = low_price / wpp
                    a_price = l_price
                    logger.info(f"MediumPrice,HighPrice未設定: l_price={l_price}, a_price={a_price}")
            elif (high_price is None or high_price == 0) and (low_price is None or low_price == 0):
                # HighPriceとLowPriceが設定されていない場合
                m_price = medium_price / wpp
                a_price = m_price
                logger.info(f"HighPrice,LowPrice未設定: m_price={m_price}, a_price={a_price}")
            elif high_price is None or high_price == 0:
                # HighPriceだけ設定されていない場合
                m_price = medium_price / wpp
                l_price = low_price / wpp
                a_price = (m_price + l_price) / 2
                logger.info(f"HighPrice未設定: m_price={m_price}, l_price={l_price}, a_price={a_price}")
            elif low_price is None or low_price == 0:
                # LowPriceだけ設定されていない場合
                m_price = medium_price / wpp
                h_price = high_price / wpp
                a_price = (m_price + h_price) / 2
                logger.info(f"LowPrice未設定: h_price={h_price}, m_price={m_price}, a_price={a_price}")
            else:
                # 全て設定されている場合
                h_price = high_price / wpp
                m_price = medium_price / wpp
                l_price = low_price / wpp
                a_price = (h_price + m_price + l_price) / 3
                logger.info(f"全価格設定: h_price={h_price}, m_price={m_price}, l_price={l_price}, a_price={a_price}")

            # source_priceの算出
            if m_price == 0:
                if h_price > 0 and l_price > 0:
                    s_price = (h_price + l_price) / 2
                    logger.info(f"m_price=0のため、s_price={s_price} (h_priceとl_priceの平均)")
                elif h_price > 0:
                    s_price = h_price
                    logger.info(f"m_price=0とl_price=0のため、s_price={s_price} (h_price)")
                elif l_price > 0:
                    s_price = l_price
                    logger.info(f"m_price=0とh_price=0のため、s_price={s_price} (l_price)")
                else:
                    s_price = a_price
                    logger.info(f"全価格=0のため、s_price={s_price} (a_price)")
            else:
                s_price = m_price
                logger.info(f"s_price={s_price} (m_price)")
            
            # 追加のフィールドを確認（デフォルト値を設定）
            target_date = data.get("TargetDate")
            grade = data.get("Grade", "")
            class_val = data.get("Class", "")
            trend = data.get("MarketTrend", "")
            variety_name = data.get("VarietyName", "")
            
            # 日付がない場合は現在日付を使用
            if target_date is None:
                from datetime import datetime
                target_date = datetime.now().strftime("%Y-%m-%d")
                logger.warning(f"TargetDateが未設定、現在日付を使用: {target_date}")
            
            logger.info(f"TargetDate: {target_date}, Grade: {grade}, Class: {class_val}")
            logger.info(f"MarketTrend: {trend}, VarietyName: {variety_name}")
            
            price = {
                "target_date": target_date,
                "item_code": item_code,
                "high_price": h_price,
                "medium_price": m_price,
                "low_price": l_price,
                "average_price": a_price,
                "source_price": s_price,
                "arrival_amount": volume,
                "weight_per": wpp,
                "volume": volume,
                "grade": grade,
                "class": class_val,
                "trend": trend,
                "variety_name": variety_name,
            }
            
            logger.info("価格データの解析完了")
            return price
            
        except Exception as e:
            import traceback
            logger.error(f"_parse_price_objects_pattern_twoメソッドでエラーが発生: {str(e)}")
            logger.error(traceback.format_exc())
            return None
    
    @staticmethod
    def parse_price_txt_to_object(file_path: str, vegetable: Vegetable) -> List[IngestMarket]:
        """
        テキストファイルから価格データを解析し、条件に合致するIngestMarketオブジェクトのリストを返す
        """
        logger.info(f"価格データ解析開始: ファイル={file_path}, 野菜={vegetable.name}(ID={vegetable.id})")
        markets = []
        try:
            content = DataParser._parse_txt_file(file_path)
            if not content or not content.strip():
                logger.warning(f"空のファイル: {file_path}")
                return []
                
            # ファイル名から日付を抽出
            target_date = DataParser.parse_date_from_filename(file_path)
            if not target_date:
                logger.error(f"日付の解析失敗: {file_path}")
                return []
            
            logger.info(f"対象日付: {target_date}, ファイル={os.path.basename(file_path)}")
            
            # デバッグ: コンテンツの先頭部分をログに出力
            if content:
                sample_content = content[:200].replace('\n', '\\n')
                logger.info(f"ファイル内容サンプル: {sample_content}...")
            
            # 文字列をJSONに変換
            try:
                import json
                json_data = json.loads(content)
                logger.info(f"JSONデータの解析成功: {file_path}, データサイズ={len(content)}バイト")
                
                # デバッグ: JSONの構造を確認
                if isinstance(json_data, list):
                    logger.info(f"JSONはリスト形式です。要素数: {len(json_data)}")
                    if len(json_data) > 0:
                        first_item = json_data[0]
                        logger.info(f"最初の要素のキー: {list(first_item.keys())}")
                        if 'Markets' in first_item:
                            logger.info(f"Marketsの要素数: {len(first_item['Markets'])}")
                else:
                    logger.info(f"JSONはリスト形式ではありません。型: {type(json_data)}")
                    
            except json.JSONDecodeError as je:
                logger.error(f"JSONデータの解析失敗: {file_path}, {str(je)}")
                # エラーの詳細情報を記録
                error_position = je.pos
                error_line = je.lineno
                error_column = je.colno
                logger.error(f"JSONエラー詳細 - 位置: {error_position}, 行: {error_line}, 列: {error_column}")
                
                # エラー周辺のコンテンツを表示
                if content and error_position < len(content):
                    start = max(0, error_position - 50)
                    end = min(len(content), error_position + 50)
                    error_context = content[start:end].replace('\n', '\\n')
                    logger.error(f"エラー周辺のコンテンツ: ...{error_context}...")
                
                return []
            
            # データの解析
            sorted_data = MarketDataParser._sort_data(json_data)
            formatted_data_arrays = MarketDataParser._format_data_to_array(sorted_data)
            
            # 各配列要素に対して処理
            for data_array in formatted_data_arrays:
                try:
                    # 各データ配列から価格データを解析
                    # デバッグ: データ配列の内容をログに出力
                    if data_array and len(data_array) > 0:
                        logger.info(f"解析する配列の要素数: {len(data_array)}")
                        # データ配列から条件に合致するデータを選択（条件に合わない場合はNoneが返される）
                        use_price_data = MarketDataParser._use_data_to_array(data_array)
                        if not use_price_data:
                            logger.info(f"条件に合致するデータがありません: {data_array[0]['ItemCode'] if data_array and 'ItemCode' in data_array[0] else '不明'}")
                            continue
                        # 選択したデータを解析して価格データに変換
                        price_data = MarketDataParser._parse_price_objects_pattern_two(use_price_data)
                    else:
                        logger.warning("空のデータ配列です")
                        continue
                    
                    if not price_data:
                        logger.warning(f"price_dataが取得できませんでした: {use_price_data if use_price_data else 'データなし'}")
                        continue
                    
                    # item_codeに基づいてvegetableを変更
                    current_vegetable = vegetable
                    if price_data and "item_code" in price_data:
                        item_code = price_data.get("item_code")
                        # item_codeに合致するVegetableを検索
                        matching_vegetable = Vegetable.objects.filter(code=item_code).first()
                        if matching_vegetable:
                            # 一致するVegetableがあれば、そちらを使用
                            current_vegetable = matching_vegetable
                            logger.info(f"item_code {item_code} に基づいてvegetableを {current_vegetable.name} に変更しました")

                    # 広島地域を取得
                    current_region = Region.objects.filter(name="広島").first()
                    if not current_region:
                        logger.error("広島地域が見つかりません")
                        return []

                    market = IngestMarket(
                        target_date=target_date,
                        high_price=price_data.get("high_price"),
                        medium_price=price_data.get("medium_price"),
                        low_price=price_data.get("low_price"),
                        average_price=price_data.get("average_price"),
                        source_price=price_data.get("source_price"),
                        arrival_amount=price_data.get("arrival_amount"),
                        weight_per=price_data.get("weight_per"),
                        volume=price_data.get("volume"),
                        trend=price_data.get("trend"),
                        vegetable=current_vegetable,
                        region=current_region
                    )
                    markets.append(market)
                    logger.info(f"野菜 {current_vegetable.name} (code: {current_vegetable.code}) の条件に合致するデータを追加しました")
                except Exception as e:
                    logger.error(f"個別の価格データ解析エラー: {str(e)}")
                    continue
            
            logger.info(f"条件に合致するデータ数: {len(markets)} / {len(formatted_data_arrays) if formatted_data_arrays else 0}")
            return markets
                
        except Exception as e:
            logger.error(f"価格データの解析エラー: {file_path}, {str(e)}")
            return []

class WeatherDataParser(DataParser):
    """
    天気データの解析を行うクラス
    """
    
    @staticmethod
    def parse_weather_csv_to_objects(file_path: str, region: Region) -> List[IngestWeather]:
        """
        CSVファイルから天気データを読み込み、IngestWeatherオブジェクトのリストを返す
        """
        logger.info(f"気象データ解析開始: ファイル={file_path}, 地域={region.name}(ID={region.id})")
        weather_objects = []
        
        try:
            with open(file_path, 'r', encoding='utf-8', errors='replace') as csv_file:
                logger.info(f"ファイルを開きました: {file_path}")
                csv_reader = csv.DictReader(csv_file)
                row_count = 0
                for row in csv_reader:
                    row_count += 1
                    try:
                        year = int(row['年'])
                        month = int(row['月'])
                        day = int(row['日'])
                        target_date = datetime.date(year, month, day)
                        
                        # 空値や '--' を None に変換
                        max_temp = float(row['最高気温']) if row['最高気温'] and row['最高気温'] != '--' else None
                        mean_temp = float(row['平均気温']) if row['平均気温'] and row['平均気温'] != '--' else None
                        min_temp = float(row['最低気温']) if row['最低気温'] and row['最低気温'] != '--' else None
                        sum_precipitation = float(row['降水量の合計']) if row['降水量の合計'] and row['降水量の合計'] != '--' else None
                        sunshine_duration = float(row['日照時間']) if row['日照時間'] and row['日照時間'] != '--' else None
                        ave_humidity = float(row['平均湿度']) if row['平均湿度'] and row['平均湿度'] != '--' else None
                        
                        logger.debug(f"気象データ行解析: 日付={year}/{month}/{day}, 最高気温={max_temp}, 平均気温={mean_temp}, 最低気温={min_temp}")
                        
                        weather = IngestWeather(
                            target_date=target_date,
                            max_temp=max_temp,
                            mean_temp=mean_temp,
                            min_temp=min_temp,
                            sum_precipitation=sum_precipitation,
                            sunshine_duration=sunshine_duration,
                            ave_humidity=ave_humidity,
                            region=region
                        )
                        weather_objects.append(weather)
                        
                    except (ValueError, KeyError) as e:
                        logger.error(f"天気データの行解析エラー: {row}, {str(e)}")
                        
                logger.info(f"気象データCSV解析完了: 行数={row_count}, 作成オブジェクト数={len(weather_objects)}")
        except Exception as e:
            logger.error(f"天気データCSVファイル解析エラー: {file_path}, {str(e)}")
            
        return weather_objects
        
    @staticmethod
    def parse_weather_csv_by_pattern(file_path: str, region: Region) -> List[IngestWeather]:
        """
        mid.csv または last.csv ファイルから天気データを読み込む
        ファイル名のパターンに基づいて適切に処理する
        """
        # ファイル名からmid/lastを判断
        file_name = os.path.basename(file_path)
        logger.info(f"パターンに基づく気象データCSVの解析開始: ファイル={file_name}, 地域={region.name}(ID={region.id})")
        
        # 通常のCSV解析を実行
        result = WeatherDataParser.parse_weather_csv_to_objects(file_path, region)
        logger.info(f"気象データ解析結果: {len(result)}レコードを生成")
        return result
    
class DataSaver:
    """
    データをDBに保存するクラス
    """
    
    @staticmethod
    def save_weather_data(weather_objects: List[IngestWeather]) -> int:
        """
        天気データのリストをデータベースに保存し、保存したオブジェクト数を返す
        """
        saved_count = 0
        for weather in weather_objects:
            try:
                # 既存データがあれば更新、なければ新規作成
                existing = IngestWeather.objects.filter(
                    target_date=weather.target_date,
                    region=weather.region
                ).first()
                
                if existing:
                    # 既存レコードの更新
                    existing.max_temp = weather.max_temp
                    existing.mean_temp = weather.mean_temp
                    existing.min_temp = weather.min_temp
                    existing.sum_precipitation = weather.sum_precipitation
                    existing.sunshine_duration = weather.sunshine_duration
                    existing.ave_humidity = weather.ave_humidity
                    existing.save()
                else:
                    # 新規レコード作成
                    weather.save()
                
                saved_count += 1
            except Exception as e:
                logger.error(f"天気データの保存エラー: {weather.target_date}, {str(e)}")
        
        return saved_count
    
    @staticmethod
    def save_price_data(markets: Union[IngestMarket, List[IngestMarket]]) -> int:
        """
        価格データをデータベースに保存し、保存した件数を返す
        単一のIngestMarketオブジェクトまたはリストを受け取る
        """
        if not markets:
            return 0
        
        # 単一のオブジェクトの場合はリストに変換
        if isinstance(markets, IngestMarket):
            markets = [markets]
        
        saved_count = 0
        for market in markets:
            try:
                # 既存データがあれば更新、なければ新規作成
                existing = IngestMarket.objects.filter(
                    target_date=market.target_date,
                    vegetable=market.vegetable
                ).first()
                
                if existing:
                    # 既存レコードの更新
                    existing.high_price = market.high_price
                    existing.medium_price = market.medium_price
                    existing.low_price = market.low_price
                    existing.average_price = market.average_price
                    existing.arrival_amount = market.arrival_amount
                    existing.weight_per = market.weight_per
                    existing.trend = market.trend  # 新しいフィールド
                    existing.save()
                else:
                    # 新規レコード作成
                    market.save()
                
                saved_count += 1
            except Exception as e:
                logger.error(f"価格データの保存エラー: {market.target_date}, {str(e)}")
                continue
        
        return saved_count
    
class FileProcessor:
    """
    ファイル処理とデータ取り込みを行うクラス
    """
    
    @staticmethod
    def process_weather_files_from_dir(directory: str, region: Region) -> int:
        """
        指定ディレクトリ内の全てのCSVファイルから天気データを取り込む
        新しいディレクトリ構造: weather/YYYY/MM/YYYY_MM_mid.csv
        """
        imported_count = 0
        
        try:
            dir_path = Path(directory)
            if not dir_path.exists():
                logger.error(f"ディレクトリが存在しません: {directory}")
                return imported_count
            
            # CSVファイルを検索（サブディレクトリも含め再帰的に）
            csv_files = list(dir_path.glob("**/*.csv"))
            logger.info(f"天気データCSVファイル検索結果: {directory}, ファイル数: {len(csv_files)}")
            
            for csv_file in csv_files:
                # ファイル名のパターンに基づいて処理
                logger.info(f"天気データファイル処理開始: {csv_file}")
                weather_objects = WeatherDataParser.parse_weather_csv_by_pattern(str(csv_file), region)
                saved_count = DataSaver.save_weather_data(weather_objects)
                imported_count += saved_count
                logger.info(f"天気データインポート: {csv_file.name}, {saved_count}件")
        
        except Exception as e:
            logger.error(f"天気データディレクトリのインポートエラー: {directory}, {str(e)}")
        
        return imported_count
    
    @staticmethod
    def process_price_files_from_dir(directory: str, vegetable: Vegetable) -> int:
        """
        指定ディレクトリ内の全てのテキストファイルから価格データを取り込む
        新しいディレクトリ構造: price/YYYY/MM/YYYY-MM-DD.txt
        """
        imported_count = 0
        
        try:
            dir_path = Path(directory)
            if not dir_path.exists():
                logger.error(f"ディレクトリが存在しません: {directory}")
                return imported_count
            
            # テキストファイルを検索（サブディレクトリも含め再帰的に）
            txt_files = list(dir_path.glob("**/*.txt"))
            logger.info(f"価格データテキストファイル検索結果: {directory}, ファイル数: {len(txt_files)}")
            
            for txt_file in txt_files:
                logger.info(f"価格データファイル処理開始: {txt_file}")
                try:
                    markets = MarketDataParser.parse_price_txt_to_object(str(txt_file), vegetable)
                    if markets:
                        logger.info(f"解析成功: {txt_file.name}, 市場データ数: {len(markets)}")
                        saved_count = DataSaver.save_price_data(markets)
                        if saved_count > 0:
                            imported_count += saved_count
                            logger.info(f"価格データインポート: {txt_file.name}, {saved_count}件")
                        else:
                            logger.warning(f"価格データ保存失敗: {txt_file.name}")
                    else:
                        logger.warning(f"価格データ解析失敗: {txt_file.name}")
                except Exception as e:
                    logger.error(f"ファイル処理エラー: {txt_file.name}, {str(e)}")
        
        except Exception as e:
            logger.error(f"価格データディレクトリのインポートエラー: {directory}, {str(e)}")
        
        return imported_count

    @staticmethod
    def process_all_weather_data(base_dir: str = None) -> Dict[str, int]:
        """
        全ての地域の天気データをインポートする
        Azuriteからのインポートを優先的に行い、失敗した場合にローカルファイルを使用
        新しいディレクトリ構造: weather/YYYY/MM/YYYY_MM_mid.csv
        """
        results = {}
        
        # 環境変数が設定されていて、Azuriteを使用する場合
        if hasattr(settings, 'AZURE_CONNECTION_STRING') and settings.AZURE_CONNECTION_STRING:
            logger.info("Azuriteからの天気データインポートを開始します")
            
            # Azure Blobクライアント取得
            container = get_blob_service_client()
            if container:
                # 全ての地域を取得
                regions = Region.objects.all()
                if not regions:
                    logger.warning("地域データがありません。先に地域データを登録してください。")
                    return results
                
                # プレフィックスを設定
                weather_prefix = settings.INGEST_PREFIX_WEATHER
                if not weather_prefix.endswith('/'):
                    weather_prefix += '/'
                
                try:
                    # Blobの一覧を取得
                    blobs = list(container.list_blobs(name_starts_with=weather_prefix))
                    logger.info(f"Azure Blob検索結果: プレフィックス={weather_prefix}, ファイル数={len(blobs)}")
                    
                    # CSVファイルのみをフィルタリング
                    csv_files = [blob.name for blob in blobs if blob.name.endswith('.csv')]
                    logger.info(f"CSVファイル数: {len(csv_files)}")
                    
                    # 地域ごとの結果をトラッキング
                    region_totals = {region.name: 0 for region in regions}
                    
                    # 各CSVファイルを処理
                    for region in regions:
                        logger.info(f"地域データインポート開始 (Azure): {region.name}")
                        total_imported = 0
                        
                        for csv_file in csv_files:
                            try:
                                # Azure Blobからデータ取得
                                content = DataParser.get_file_content(csv_file, is_azure_path=True)
                                if content:
                                    # 一時ファイルに書き出し
                                    import tempfile
                                    with tempfile.NamedTemporaryFile(mode='w+', delete=False, suffix='.csv') as temp_file:
                                        temp_path = temp_file.name
                                        temp_file.write(content)
                                    
                                    try:
                                        # 天気データ解析
                                        weather_objects = WeatherDataParser.parse_weather_csv_to_objects(temp_path, region)
                                        if weather_objects:
                                            # 保存
                                            saved_count = DataSaver.save_weather_data(weather_objects)
                                            total_imported += saved_count
                                            logger.info(f"Azureファイル処理成功: {csv_file}, 保存件数: {saved_count}")
                                    finally:
                                        # 一時ファイル削除
                                        import os
                                        if os.path.exists(temp_path):
                                            os.unlink(temp_path)
                            except Exception as e:
                                logger.error(f"Azureファイル処理エラー: {csv_file}, {str(e)}")
                        
                        region_totals[region.name] = total_imported
                        logger.info(f"地域データインポート完了 (Azure): {region.name}, インポート件数: {total_imported}件")
                    
                    # 結果を設定
                    results = region_totals
                    logger.info(f"Azure天気データインポート完了: 地域ごとの処理件数: {results}")
                    return results
                    
                except Exception as e:
                    logger.error(f"Azuriteからのデータ取得エラー: {str(e)}")
                    # ローカルファイル処理に続ける
            else:
                logger.error("Azure Blob接続に失敗しました。ローカルファイル処理を試みます。")
        
        # Azuriteが利用できない場合や失敗した場合は、ローカルファイルを処理
        logger.info("ローカルディレクトリから天気データインポートを開始します")
        
        if base_dir is None:
            base_dir = os.path.join(settings.DATA_BASE_DIR, settings.INGEST_PREFIX_WEATHER)
        
        logger.info(f"天気データインポート開始: ベースディレクトリ = {base_dir}")
        if not os.path.exists(base_dir):
            logger.error(f"ベースディレクトリが存在しません: {base_dir}")
            return results
            
        # ディレクトリの内容を確認
        try:
            dir_items = os.listdir(base_dir)
            logger.info(f"ディレクトリ内容: {base_dir}, アイテム数: {len(dir_items)}")
            logger.info(f"ディレクトリ内の最初の10アイテム: {dir_items[:10] if len(dir_items) > 10 else dir_items}")
            
            # 年別ディレクトリを確認
            year_dirs = [d for d in dir_items if os.path.isdir(os.path.join(base_dir, d)) and d.isdigit()]
            logger.info(f"年別ディレクトリ: {year_dirs}")
        except Exception as e:
            logger.error(f"ディレクトリ内容の取得エラー: {base_dir}, {str(e)}")
        
        # 全ての地域を取得
        regions = Region.objects.all()
        if not regions:
            logger.warning("地域データがありません。先に地域データを登録してください。")
            return results
        
        # 各地域のデータをインポート
        for region in regions:
            logger.info(f"地域データインポート開始: {region.name}")
            
            total_imported = 0
            # 年別ディレクトリをループ
            for year_dir in year_dirs:
                year_path = os.path.join(base_dir, year_dir)
                logger.info(f"年別ディレクトリ処理中: {year_path}")
                
                try:
                    # 月別ディレクトリをループ
                    month_dirs = [d for d in os.listdir(year_path) if os.path.isdir(os.path.join(year_path, d))]
                    logger.info(f"月別ディレクトリ: {month_dirs}")
                    
                    for month_dir in month_dirs:
                        month_path = os.path.join(year_path, month_dir)
                        logger.info(f"月別ディレクトリ処理中: {month_path}")
                        
                        # この月のデータをインポート
                        imported_count = FileProcessor.process_weather_files_from_dir(month_path, region)
                        total_imported += imported_count
                        logger.info(f"月別インポート完了: {year_dir}/{month_dir}, {imported_count}件")
                except Exception as e:
                    logger.error(f"ディレクトリ処理エラー: {year_path}, {str(e)}")
            
            results[region.name] = total_imported
            logger.info(f"地域データインポート完了: {region.name}, インポート件数: {total_imported}件")
        
        return results
    
    @staticmethod
    def process_all_price_data(base_dir: str = None) -> Dict[str, int]:
        """
        全ての野菜の価格データをインポートする
        Azuriteからのインポートを優先的に行い、失敗した場合にローカルファイルを使用
        新しいディレクトリ構造: price/YYYY/MM/YYYY-MM-DD.txt
        """
        results = {}
        
        # 環境変数が設定されていて、Azuriteを使用する場合
        if hasattr(settings, 'AZURE_CONNECTION_STRING') and settings.AZURE_CONNECTION_STRING:
            logger.info("Azuriteからの価格データインポートを開始します")
            
            # Azure Blobクライアント取得
            container = get_blob_service_client()
            if container:
                # 全ての野菜を取得
                vegetables = Vegetable.objects.all()
                if not vegetables:
                    logger.warning("野菜データがありません。先に野菜データを登録してください。")
                    return results
                
                # プレフィックスを設定
                price_prefix = settings.INGEST_PREFIX_PRICE
                if not price_prefix.endswith('/'):
                    price_prefix += '/'
                
                try:
                    # Blobの一覧を取得
                    blobs = list(container.list_blobs(name_starts_with=price_prefix))
                    logger.info(f"Azure Blob検索結果: プレフィックス={price_prefix}, ファイル数={len(blobs)}")
                    
                    # 価格データのパターンにマッチするものをフィルタリング（例: price/2019/01/2019-01-05.txt）
                    price_files = [blob for blob in blobs if re.match(r'price/\d{4}/\d{2}/\d{4}-\d{2}-\d{2}\.txt', blob.name)]
                    logger.info(f"価格データファイル数（パターンマッチ後）: {len(price_files)}")
                    
                    # ファイル名に基づいて野菜ごとの処理をトラッキング
                    vegetable_totals = {v.name: 0 for v in vegetables}
                    
                    # 一時的なデータインジェスター作成
                    ingestor = DataIngestor()
                    
                    # フィルタリングしたBlobファイルを処理
                    for blob in price_files:
                        blob_path = blob.name
                        logger.info(f"Azure Blobファイル処理: {blob_path}")
                        if ingestor.process_price_file_from_azure(blob_path):
                            # 保存成功した場合、野菜ごとのカウントを増やす
                            logger.info(f"ファイル処理成功: {blob_path}")
                            # 実際に保存された野菜は複数あるが、ここでは単純にカウントだけ
                            for veg_name in vegetable_totals:
                                vegetable_totals[veg_name] += 1
                    
                    # 結果を設定
                    results = vegetable_totals
                    logger.info(f"Azurite価格データインポート完了: 野菜ごとの処理件数: {results}")
                    return results
                    
                except Exception as e:
                    logger.error(f"Azuriteからのデータ取得エラー: {str(e)}")
                    # ローカルファイル処理に続ける
            else:
                logger.error("Azure Blob接続に失敗しました。ローカルファイル処理を試みます。")
        
        # Azuriteが利用できない場合や失敗した場合は、ローカルファイルを処理
        logger.info("ローカルディレクトリから価格データインポートを開始します")
        
        if base_dir is None:
            base_dir = os.path.join(settings.DATA_BASE_DIR, settings.INGEST_PREFIX_PRICE)
        
        logger.info(f"価格データインポート開始: ベースディレクトリ = {base_dir}")
        if not os.path.exists(base_dir):
            logger.error(f"ベースディレクトリが存在しません: {base_dir}")
            return results
            
        # ディレクトリの内容を確認
        try:
            dir_items = os.listdir(base_dir)
            logger.info(f"ディレクトリ内容: {base_dir}, アイテム数: {len(dir_items)}")
            logger.info(f"ディレクトリ内の最初の10アイテム: {dir_items[:10] if len(dir_items) > 10 else dir_items}")
            
            # 年別ディレクトリを確認
            year_dirs = [d for d in dir_items if os.path.isdir(os.path.join(base_dir, d)) and d.isdigit()]
            logger.info(f"年別ディレクトリ: {year_dirs}")
        except Exception as e:
            logger.error(f"ディレクトリ内容の取得エラー: {base_dir}, {str(e)}")
        
        # 全ての野菜を取得
        vegetables = Vegetable.objects.all()
        if not vegetables:
            logger.warning("野菜データがありません。先に野菜データを登録してください。")
            return results
            
        logger.info(f"野菜データ取得成功: {len(vegetables)}件")
        
        # 各野菜のデータをインポート
        for vegetable in vegetables:
            logger.info(f"野菜データインポート開始: {vegetable.name} (code: {vegetable.code})")
            
            total_imported = 0
            # 年別ディレクトリをループ
            for year_dir in year_dirs:
                year_path = os.path.join(base_dir, year_dir)
                logger.info(f"年別ディレクトリ処理中: {year_path}")
                
                try:
                    # 月別ディレクトリを確認
                    month_dirs = [d for d in os.listdir(year_path) if os.path.isdir(os.path.join(year_path, d))]
                    logger.info(f"月別ディレクトリ: {month_dirs}")
                    
                    # 月別ディレクトリをループ
                    for month_dir in month_dirs:
                        month_path = os.path.join(year_path, month_dir)
                        logger.info(f"月別ディレクトリ処理中: {month_path}")
                        
                        # この月のデータをインポート
                        imported_count = FileProcessor.process_price_files_from_dir(month_path, vegetable)
                        total_imported += imported_count
                        logger.info(f"月別インポート完了: {year_dir}/{month_dir}, {imported_count}件")
                except Exception as e:
                    logger.error(f"ディレクトリ処理エラー: {year_path}, {str(e)}")
            
            results[vegetable.name] = total_imported
            logger.info(f"野菜データインポート完了: {vegetable.name}, インポート件数: {total_imported}件")
        
        logger.info(f"価格データインポート完了: 合計野菜種類数: {len(results)}")
        return results
        
    @staticmethod
    def process_azurite_to_db() -> Dict[str, Dict[str, int]]:
        """
        Azuriteから全てのデータをインポートする
        """
        results = {
            'price': {},
            'weather': {}
        }
        
        # 価格データをインポート
        price_results = FileProcessor.process_all_price_data()
        results['price'] = price_results
        
        # 天気データをインポート
        weather_results = FileProcessor.process_all_weather_data()
        results['weather'] = weather_results
        
        return results


# 互換性のために旧APIも保持（新クラスへの橋渡し）
class DataIngestor:
    """
    データ取り込み用のサービスクラス（互換性のため維持）
    新しいクラスへの橋渡しを行う
    """
    
    def ingest_weather_file(self, filepath: str, target_date: datetime.date, half: str) -> dict:
        """
        Azure Storageから単一の気象ファイルを取得してIngestWeatherテーブルに保存
        
        Args:
            filepath: Azure Storage内のファイルパス (例: weather/2025/11/2025_11_mid.csv)
            target_date: 対象日付
            half: 期間 ("前半" または "後半")
            
        Returns:
            dict: {"success": bool, "records_created": int, "error": str}
        """
        logger.info(f"気象ファイル取り込み開始: {filepath}, half={half}")
        
        try:
            # Azure Storageからファイル内容を取得
            content = DataParser.get_file_content(filepath, is_azure_path=True)
            
            if not content:
                return {
                    "success": False,
                    "records_created": 0,
                    "error": f"File not found or empty: {filepath}"
                }
            
            logger.info(f"Downloaded weather file: {filepath} ({len(content)} characters)")
            
            # CSVデータを解析
            import csv
            from io import StringIO
            from django.utils import timezone
            
            csv_reader = csv.reader(StringIO(content))
            headers = next(csv_reader)  # ヘッダー行をスキップ
            
            records_created = 0
            
            # 期間に基づいて日付範囲を決定
            year = target_date.year
            month = target_date.month
            
            if half == "前半":
                start_day = 1
                end_day = 15
            else:  # "後半"
                start_day = 16
                # 月末日を計算
                if month == 12:
                    next_month = datetime.date(year + 1, 1, 1)
                else:
                    next_month = datetime.date(year, month + 1, 1)
                end_day = (next_month - datetime.timedelta(days=1)).day
            
            logger.info(f"日付範囲: {year}/{month}/{start_day} - {year}/{month}/{end_day}")
            
            # CSVの各行を処理（地域別の気象データ）
            with transaction.atomic():
                for row in csv_reader:
                    if len(row) < 6:  # 最低限必要なカラム数をチェック
                        continue
                    
                    try:
                        # CSVの構造に合わせて解析
                        region_name = row[0].strip() if row[0] else None
                        max_temp = float(row[1]) if row[1] and row[1] != '--' else None
                        mean_temp = float(row[2]) if row[2] and row[2] != '--' else None
                        min_temp = float(row[3]) if row[3] and row[3] != '--' else None
                        precipitation = float(row[4]) if row[4] and row[4] != '--' else None
                        sunshine_hours = float(row[5]) if row[5] and row[5] != '--' else None
                        humidity = float(row[6]) if len(row) > 6 and row[6] and row[6] != '--' else None
                        
                        if not region_name:
                            continue
                        
                        # 地域名から地域オブジェクトを取得
                        try:
                            region = Region.objects.get(name=region_name)
                        except Region.DoesNotExist:
                            logger.warning(f"Region not found: {region_name}, skipping row")
                            continue
                        
                        # 期間内の各日付でIngestWeatherレコードを作成
                        for day in range(start_day, end_day + 1):
                            record_date = datetime.date(year, month, day)
                            
                            # 既存レコードをチェック（重複回避）
                            existing = IngestWeather.objects.filter(
                                region=region,
                                target_date=record_date
                            ).first()
                            
                            if existing:
                                # 既存レコードを更新
                                existing.max_temp = max_temp
                                existing.mean_temp = mean_temp
                                existing.min_temp = min_temp
                                existing.sum_precipitation = precipitation
                                existing.sunshine_duration = sunshine_hours
                                existing.ave_humidity = humidity
                                existing.save()
                            else:
                                # 新規レコードを作成
                                IngestWeather.objects.create(
                                    region=region,
                                    target_date=record_date,
                                    max_temp=max_temp,
                                    mean_temp=mean_temp,
                                    min_temp=min_temp,
                                    sum_precipitation=precipitation,
                                    sunshine_duration=sunshine_hours,
                                    ave_humidity=humidity
                                )
                                records_created += 1
                            
                    except (ValueError, IndexError) as e:
                        logger.warning(f"Failed to parse weather row {row}: {e}")
                        continue
            
            logger.info(f"Weather file ingestion completed: {records_created} records created for {filepath}")
            
            return {
                "success": True,
                "records_created": records_created,
                "error": None
            }
            
        except Exception as e:
            logger.error(f"Failed to ingest weather file {filepath}: {str(e)}", exc_info=True)
            return {
                "success": False,
                "records_created": 0,
                "error": str(e)
            }

    def ingest_price_file(self, filepath: str, target_date) -> Dict:
        """
        Azure Storageから価格ファイルをダウンロードしてIngestMarketに格納
        observe/views.py のwebhook互換性のため
        
        Args:
            filepath: ダウンロード対象のファイルパス (例: price/2025/11/2025-11-13.txt)
            target_date: データの対象日付
            
        Returns:
            Dict: {'success': bool, 'records_created': int, 'error': str}
        """
        logger.info(f"価格ファイル取り込み開始: {filepath}")
        
        try:
            # 既存のprocess_price_file_from_azureメソッドを使用
            success = self.process_price_file_from_azure(filepath)
            
            if success:
                # 実際のレコード作成数は取得しづらいので概算で返す
                return {
                    'success': True,
                    'records_created': 1,  # 成功時は1として扱う
                    'error': None
                }
            else:
                return {
                    'success': False,
                    'records_created': 0,
                    'error': f"Failed to process file: {filepath}"
                }
        except Exception as e:
            logger.error(f"価格ファイル取り込みエラー: {filepath}, {str(e)}", exc_info=True)
            return {
                'success': False,
                'records_created': 0,
                'error': str(e)
            }
    
    def process_price_file_from_azure(self, blob_path: str) -> bool:
        """
        Azuriteの価格ファイルを処理する
        """
        try:
            logger.info(f"Azuriteの価格ファイル処理開始: {blob_path}")
            
            # ファイルパスの形式を確認
            path_parts = blob_path.split('/')
            logger.info(f"ファイルパス解析: {path_parts}")
            
            # ファイル名から日付を取得
            target_date = DataParser.parse_date_from_filename(blob_path)
            if not target_date:
                logger.error(f"日付の解析失敗: {blob_path}")
                return False
            
            logger.info(f"解析された日付: {target_date}")
            
            # 全ての野菜を取得
            vegetables = Vegetable.objects.all()
            if not vegetables:
                logger.error("野菜データがありません。先に野菜データを登録してください。")
                return False
            
            logger.info(f"登録されている野菜数: {vegetables.count()}")
            
            # Azure Blobからファイル内容を取得
            logger.info(f"Azure Blobからファイル内容を取得中: {blob_path}")
            content = DataParser.get_file_content(blob_path, is_azure_path=True)
            
            if not content:
                logger.error(f"ファイル内容の取得失敗: {blob_path}")
                return False
            
            logger.info(f"ファイル内容の取得成功: サイズ {len(content)} バイト")
            
            # ファイル内容のサンプルを表示
            content_sample = content[:200].replace('\n', '\\n') if len(content) > 200 else content.replace('\n', '\\n')
            logger.info(f"ファイル内容サンプル: {content_sample}...")
            
            # 内容をJSONに変換
            try:
                import json
                # JSONデコードの前に、BOMを削除（もし存在すれば）
                if content.startswith('\ufeff'):
                    content = content[1:]
                    logger.info("BOM（バイトオーダーマーク）を削除しました")
                
                # 空白文字の削除
                content = content.strip()
                
                # JSON構文の詳細チェック
                try:
                    # 先頭と末尾の文字をチェック
                    if not (content.startswith('[') or content.startswith('{')):
                        logger.warning(f"JSONの先頭文字が不正です: '{content[0]}'")
                    if not (content.endswith(']') or content.endswith('}')):
                        logger.warning(f"JSONの末尾文字が不正です: '{content[-1]}'")
                        
                    # バランスチェック
                    brackets_count = {
                        '[': content.count('['),
                        ']': content.count(']'),
                        '{': content.count('{'),
                        '}': content.count('}'),
                    }
                    logger.info(f"JSONの括弧バランス: {brackets_count}")
                except:
                    logger.warning("JSONの構文チェックに失敗しました")
                
                json_data = json.loads(content)
                logger.info(f"JSONデータの解析成功: {blob_path}")
                
                # JSONデータ構造の確認
                if isinstance(json_data, list):
                    logger.info(f"JSONはリスト形式です。要素数: {len(json_data)}")
                    if len(json_data) > 0:
                        first_item_keys = list(json_data[0].keys())
                        logger.info(f"最初の要素のキー: {first_item_keys}")
                else:
                    logger.info(f"JSONはオブジェクト形式です。キー: {list(json_data.keys())}")
                
            except json.JSONDecodeError as je:
                logger.error(f"JSONデータの解析失敗: {blob_path}, {str(je)}")
                # JSONエラーの詳細を記録
                error_position = je.pos
                error_line = je.lineno
                error_column = je.colno
                logger.error(f"JSONエラー詳細 - 位置: {error_position}, 行: {error_line}, 列: {error_column}")
                
                # エラー周辺のコンテンツを表示
                if content and error_position < len(content):
                    start = max(0, error_position - 50)
                    end = min(len(content), error_position + 50)
                    error_context = content[start:end].replace('\n', '\\n')
                    logger.error(f"エラー周辺のコンテンツ: ...{error_context}...")
                
                # JSONフォーマットの修正を試みる
                try:
                    logger.info("JSONフォーマットの修正を試みます")
                    
                    # 一般的な修正（例: 末尾のカンマ除去）
                    if content.find(",]") != -1:
                        content = content.replace(",]", "]")
                        logger.info("末尾のカンマを修正しました")
                    
                    if content.find(",}") != -1:
                        content = content.replace(",}", "}")
                        logger.info("末尾のカンマを修正しました")
                    
                    # 再試行
                    json_data = json.loads(content)
                    logger.info("修正後のJSONデータの解析に成功しました")
                except:
                    logger.error("JSONフォーマット修正の試みも失敗しました")
                    return False
                
            # 一番最初の野菜で処理（実際にはデータ内のitem_codeで後から変更される）
            vegetable = vegetables.first()
            
            try:
                # データの解析
                sorted_data = MarketDataParser._sort_data(json_data)
                formatted_data_arrays = MarketDataParser._format_data_to_array(sorted_data)
                
                # データが取得できたかチェック
                if not formatted_data_arrays:
                    logger.warning(f"フォーマット済みデータ配列が空です: {blob_path}")
                    return False
                
                success = False
                saved_count = 0
                total_count = len(formatted_data_arrays)
                
                for data_array in formatted_data_arrays:
                    if not data_array:
                        continue
                        
                    # データ配列から条件に合致するデータを選択（条件に合わない場合はNoneが返される）
                    use_price_data = MarketDataParser._use_data_to_array(data_array)
                    if not use_price_data:
                        logger.info(f"条件に合致するデータがありません: {data_array[0]['ItemCode'] if data_array and 'ItemCode' in data_array[0] else '不明'}")
                        continue
                        
                    # 選択したデータを解析して価格データに変換
                    price_data = MarketDataParser._parse_price_objects_pattern_two(use_price_data)
                    
                    if not price_data:
                        logger.warning(f"price_dataが取得できませんでした")
                        continue
                    
                    # item_codeに基づいてvegetableを変更
                    current_vegetable = vegetable
                    if price_data and "item_code" in price_data:
                        item_code = price_data.get("item_code")
                        # item_codeに合致するVegetableを検索
                        matching_vegetable = Vegetable.objects.filter(code=item_code).first()
                        if matching_vegetable:
                            current_vegetable = matching_vegetable
                            logger.info(f"item_code {item_code} に基づいてvegetableを {current_vegetable.name} に変更しました")
                    
                    # 広島地域を取得
                    current_region = Region.objects.filter(name="広島").first()
                    if not current_region:
                        logger.error("広島地域が見つかりません")
                        continue
                    
                    market = IngestMarket(
                        target_date=target_date,
                        high_price=price_data.get("high_price"),
                        medium_price=price_data.get("medium_price"),
                        low_price=price_data.get("low_price"),
                        average_price=price_data.get("average_price"),
                        source_price=price_data.get("source_price"),
                        arrival_amount=price_data.get("arrival_amount"),
                        weight_per=price_data.get("weight_per"),
                        volume=price_data.get("volume"),
                        trend=price_data.get("trend"),
                        vegetable=current_vegetable,
                        region=current_region
                    )
                    
                    # 保存
                    if DataSaver.save_price_data(market) > 0:
                        saved_count += 1
                        success = True
                        logger.info(f"野菜 {current_vegetable.name} の条件に合致するデータを保存しました")
                
                logger.info(f"条件に合致するデータ数: {saved_count} / {total_count}")
                return success
                
            except Exception as e:
                logger.error(f"データ解析エラー: {blob_path}, {str(e)}")
                import traceback
                logger.error(traceback.format_exc())
                return False
                
        except Exception as e:
            logger.error(f"Azuriteファイル処理エラー: {blob_path}, {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return False
    
    @staticmethod
    def parse_date_from_filename(filename: str) -> Optional[datetime.date]:
        return DataParser.parse_date_from_filename(filename)
    
    @staticmethod
    def import_weather_csv(file_path: str, region: Region) -> List[IngestWeather]:
        return WeatherDataParser.parse_weather_csv_to_objects(file_path, region)
    
    @staticmethod
    def import_price_txt(file_path: str, vegetable: Vegetable) -> List[IngestMarket]:
        return MarketDataParser.parse_price_txt_to_object(file_path, vegetable)
    
    @staticmethod
    def save_weather_data(weather_objects: List[IngestWeather]) -> int:
        return DataSaver.save_weather_data(weather_objects)
    
    @staticmethod
    def save_price_data(market: IngestMarket) -> bool:
        return DataSaver.save_price_data(market)
    
    @staticmethod
    def import_weather_files_from_dir(directory: str, region: Region) -> int:
        return FileProcessor.process_weather_files_from_dir(directory, region)
    
    @staticmethod
    def import_price_files_from_dir(directory: str, vegetable: Vegetable) -> int:
        return FileProcessor.process_price_files_from_dir(directory, vegetable)
    
    @staticmethod
    def import_all_weather_data(base_dir: str = None) -> Dict[str, int]:
        return FileProcessor.process_all_weather_data(base_dir)
    
    @staticmethod
    def import_all_price_data(base_dir: str = None) -> Dict[str, int]:
        return FileProcessor.process_all_price_data(base_dir)
