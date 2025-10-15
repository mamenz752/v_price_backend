import csv
import os
import datetime
import logging
import glob
import re
from pathlib import Path
from typing import Dict, List, Optional, Union, Any, Tuple, Set
from django.conf import settings
from azure.storage.blob import BlobServiceClient

from .models import Vegetable, Region, IngestMarket, IngestWeather

logger = logging.getLogger(__name__)

class DataParser:
    """
    データ解析用のベースクラス
    Azuriteのファイルやローカルファイルからデータを解析する機能を提供
    """
    
    @staticmethod
    def parse_date_from_filename(filename: str) -> Optional[datetime.date]:
        """
        ファイル名から日付を抽出する
        例: 2022-01-05.txt -> 2022-01-05
        """
        try:
            # ファイル名からファイル拡張子を除いた部分を取得
            base_name = os.path.basename(filename)
            name_without_ext = os.path.splitext(base_name)[0]
            
            # 日付形式の場合、日付オブジェクトに変換
            if '-' in name_without_ext:
                date_parts = name_without_ext.split('-')
                year = int(date_parts[0])
                month = int(date_parts[1])
                day = int(date_parts[2])
                return datetime.date(year, month, day)
                
            # 2022_01_mid.csv のようなフォーマットの場合
            match = re.match(r'(\d{4})_(\d{2})_(mid|last)', name_without_ext)
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
                
            return None
        except (ValueError, IndexError) as e:
            logger.error(f"日付の解析エラー: {filename}, {str(e)}")
            return None
    
    @staticmethod
    def get_file_content(file_path: str) -> Optional[str]:
        """
        ファイルの内容を取得する
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return f.read()
        except Exception as e:
            logger.error(f"ファイル読み込みエラー: {file_path}, {str(e)}")
            try:
                # UTF-8で失敗した場合はShift-JISで試す
                with open(file_path, 'r', encoding='shift_jis') as f:
                    return f.read()
            except Exception as e2:
                logger.error(f"ファイル読み込みエラー(Shift-JIS): {file_path}, {str(e2)}")
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
    V_CODES = ['31700', '31100', '30100', '36600', '36200', '34400', '34300', '34100']

    @staticmethod
    def _sort_data(data):
        # 広島中央卸売市場縛り
        region = Region.objects.filter(name="広島").first()
        region_data = next((market for market in data[0]["Markets"][:30] if market["MarketCode"] == region.market_code), None)

        # 広島産地縛り
        p_area_code = region.p_area_code
        items_data = region_data['Items']

        # 野菜8品目縛り
        v_data = []
        for v in items_data[:MarketDataParser.ALL_V_KINDS_NUMBER]:
            for v_code in MarketDataParser.V_CODES:
                if v['ItemCode'] == v_code:
                    v_data.append(v)

        # 産地縛り
        v_h_data = []
        for i in range(len(v_data)):
            v = v_data[i]["Details"]
            for j in range(len(v)):
                if v[j]["ProductionAreaCode"] == p_area_code:
                    v_h_data.append(v[j])
                    
        return v_h_data
    
    @staticmethod
    def _format_data_to_array(data):
        v_code_array = []
        v_array = []
        
        for price_data in data:
            v_code_array.append(price_data["ItemCode"])
        v_code_array = set(v_code_array)
        
        for v_code in v_code_array:
            array = []
            for price_data in data:
                current_v_code = price_data["ItemCode"]
                if current_v_code == v_code:
                    array.append(price_data)
            v_array.append(array)

        return v_array
    
    @staticmethod
    def _parse_price_objects_pattern_one(data):
        data = data[0]
        item_code = data["ItemCode"]
        wpp = data["WeightPerPackage"]
        if wpp == None:
            wpp = 1
            
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
        # MEMO: pattern_oneと違う点は以下のコメントアウト
        # data = data[0]
        item_code = data["ItemCode"]
        wpp = data["WeightPerPackage"]
        if wpp == None:
            wpp = 1
            
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
    def parse_price_txt_to_object(file_path: str, vegetable: Vegetable) -> Optional[IngestMarket]:
        """
        テキストファイルから価格データを解析し、IngestMarketオブジェクトを返す
        """
        try:
            content = DataParser._parse_txt_file(file_path)
            if not content or not content.strip():
                logger.warning(f"空のファイル: {file_path}")
                return None
                
            # ファイル名から日付を抽出
            target_date = DataParser.parse_date_from_filename(file_path)
            if not target_date:
                logger.error(f"日付の解析失敗: {file_path}")
                return None
            
            # データの解析
            sorted_data = MarketDataParser._sort_data(content)
            formatted_data = MarketDataParser._format_data(sorted_data)
            price_data = (lambda: (a := MarketDataParser._parse_price_objects_pattern_one(formatted_data)) if a is not None else MarketDataParser._parse_price_objects_pattern_two(formatted_data))()
            
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
                vegetable=vegetable
            )
            return market
                
        except Exception as e:
            logger.error(f"価格データの解析エラー: {file_path}, {str(e)}")
            return None

class WeatherDataParser(DataParser):
    """
    天気データの解析を行うクラス
    """
    
    @staticmethod
    def parse_weather_csv_to_objects(file_path: str, region: Region) -> List[IngestWeather]:
        """
        CSVファイルから天気データを読み込み、IngestWeatherオブジェクトのリストを返す
        """
        weather_objects = []
        
        try:
            with open(file_path, 'r', encoding='utf-8', errors='replace') as csv_file:
                csv_reader = csv.DictReader(csv_file)
                for row in csv_reader:
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
        
        # 通常のCSV解析を実行
        return WeatherDataParser.parse_weather_csv_to_objects(file_path, region)
    
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
    def save_price_data(market: IngestMarket) -> bool:
        """
        価格データをデータベースに保存し、成功したかどうかを返す
        """
        if not market:
            return False
        
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
            
            return True
        except Exception as e:
            logger.error(f"価格データの保存エラー: {market.target_date}, {str(e)}")
            return False
    
class FileProcessor:
    """
    ファイル処理とデータ取り込みを行うクラス
    """
    
    @staticmethod
    def process_weather_files_from_dir(directory: str, region: Region) -> int:
        """
        指定ディレクトリ内の全てのCSVファイルから天気データを取り込む
        """
        imported_count = 0
        
        try:
            dir_path = Path(directory)
            if not dir_path.exists():
                logger.error(f"ディレクトリが存在しません: {directory}")
                return imported_count
            
            # CSVファイルを検索
            csv_files = list(dir_path.glob("*.csv"))
            for csv_file in csv_files:
                # ファイル名のパターンに基づいて処理
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
        """
        imported_count = 0
        
        try:
            dir_path = Path(directory)
            if not dir_path.exists():
                logger.error(f"ディレクトリが存在しません: {directory}")
                return imported_count
            
            # テキストファイルを検索
            txt_files = list(dir_path.glob("*.txt"))
            for txt_file in txt_files:
                market = MarketDataParser.parse_price_txt_to_object(str(txt_file), vegetable)
                if market and DataSaver.save_price_data(market):
                    imported_count += 1
                    logger.info(f"価格データインポート: {txt_file.name}")
        
        except Exception as e:
            logger.error(f"価格データディレクトリのインポートエラー: {directory}, {str(e)}")
        
        return imported_count

    @staticmethod
    def process_all_weather_data(base_dir: str = None) -> Dict[str, int]:
        """
        全ての地域の天気データをインポートする
        """
        results = {}
        
        if base_dir is None:
            base_dir = os.path.join(settings.INGEST_ROOT, settings.INGEST_PREFIX_WEATHER)
        
        # 全ての地域を取得
        regions = Region.objects.all()
        if not regions:
            logger.warning("地域データがありません。先に地域データを登録してください。")
            return results
        
        # 各地域のデータをインポート
        for region in regions:
            # 通常はregion.weather_codeに基づいて適切なディレクトリを選択する
            # ここでは簡略化のため、base_dirのみを使用
            imported_count = FileProcessor.process_weather_files_from_dir(base_dir, region)
            results[region.name] = imported_count
        
        return results
    
    @staticmethod
    def process_all_price_data(base_dir: str = None) -> Dict[str, int]:
        """
        全ての野菜の価格データをインポートする
        """
        results = {}
        
        if base_dir is None:
            base_dir = os.path.join(settings.INGEST_ROOT, settings.INGEST_PREFIX_PRICE)
        
        # 全ての野菜を取得
        vegetables = Vegetable.objects.all()
        if not vegetables:
            logger.warning("野菜データがありません。先に野菜データを登録してください。")
            return results
        
        # 各野菜のデータをインポート
        for vegetable in vegetables:
            # 通常はvegetable.codeに基づいて適切なディレクトリを選択する
            # ここでは簡略化のため、base_dirのみを使用
            imported_count = FileProcessor.process_price_files_from_dir(base_dir, vegetable)
            results[vegetable.name] = imported_count
        
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
    
    @staticmethod
    def parse_date_from_filename(filename: str) -> Optional[datetime.date]:
        return DataParser.parse_date_from_filename(filename)
    
    @staticmethod
    def import_weather_csv(file_path: str, region: Region) -> List[IngestWeather]:
        return WeatherDataParser.parse_weather_csv_to_objects(file_path, region)
    
    @staticmethod
    def import_price_txt(file_path: str, vegetable: Vegetable) -> Optional[IngestMarket]:
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
