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
                
            # TODO: 実際のファイル形式に合わせて解析ロジックを実装
            # 仮実装としてダミーデータを返す
            # 実際のファイルフォーマットに基づいてこのロジックを修正する必要があります
            
            # ダミーデータではなく、実際のファイルを解析する例：
            # ここではファイルの形式が不明なので仮実装していますが、
            # 実際のデータ形式に合わせて実装してください
            lines = content.strip().split('\n')
            
            # トレンド情報を抽出（例：上昇、下降、安定など）
            # 実際のファイル内容に合わせて調整する必要があります
            trend = None
            if len(lines) > 5 and '傾向' in lines[5].lower():
                trend_line = lines[5].split(':')
                if len(trend_line) > 1:
                    trend = trend_line[1].strip()
                    
            # 価格データを抽出
            # 実際のファイル内容に合わせて調整する必要があります
            high_price = 100.0  # ダミーデータ
            medium_price = 80.0  # ダミーデータ
            low_price = 60.0     # ダミーデータ
            average_price = 80.0  # ダミーデータ
            arrival_amount = 500.0  # ダミーデータ
            weight_per = 100.0    # ダミーデータ
            
            # ファイル内容から実際のデータを抽出（例）
            for line in lines:
                if '最高値:' in line:
                    try:
                        high_price = float(line.split(':')[1].strip())
                    except (ValueError, IndexError):
                        pass
                elif '中央値:' in line:
                    try:
                        medium_price = float(line.split(':')[1].strip())
                    except (ValueError, IndexError):
                        pass
                elif '最安値:' in line:
                    try:
                        low_price = float(line.split(':')[1].strip())
                    except (ValueError, IndexError):
                        pass
                elif '平均:' in line:
                    try:
                        average_price = float(line.split(':')[1].strip())
                    except (ValueError, IndexError):
                        pass
                elif '入荷量:' in line:
                    try:
                        arrival_amount = float(line.split(':')[1].strip())
                    except (ValueError, IndexError):
                        pass
                elif '重量:' in line:
                    try:
                        weight_per = float(line.split(':')[1].strip())
                    except (ValueError, IndexError):
                        pass
            
            market = IngestMarket(
                target_date=target_date,
                high_price=high_price,
                medium_price=medium_price,
                low_price=low_price,
                average_price=average_price,
                arrival_amount=arrival_amount,
                weight_per=weight_per,
                trend=trend,
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
