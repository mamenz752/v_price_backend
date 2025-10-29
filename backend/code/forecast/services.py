from .models import ForecastModelVariable

class ModelVariableDisplayService:
    """予測モデルの変数表示を管理するサービス"""
    
    @staticmethod
    def get_variable_name_mapping():
        """変数名の日本語マッピングを取得"""
        return {
            'max_temp': '最高気温',
            'mean_temp': '平均気温',
            'min_temp': '最低気温',
            'sum_precipitation': '降水量',
            'sunshine_duration': '日照時間',
            'ave_humidity': '平均湿度',
            'average_price': '平均価格',
            'volume': '取引量',
            'const': '定数項'
        }

    @classmethod
    def get_display_name(cls, variable_name: str) -> str:
        """変数の日本語表示名を取得"""
        mapping = cls.get_variable_name_mapping()
        return mapping.get(variable_name, variable_name)

    @staticmethod
    def get_term_display(previous_term: int) -> str:
        """期間を0.5カ月単位で表示"""
        months = previous_term * 0.5
        if months == 0:
            return "現在"
        return f"{months}カ月前"

    @classmethod
    def format_variable_display(cls, variable: ForecastModelVariable) -> str:
        """変数の表示形式を整形"""
        display_name = cls.get_display_name(variable.name)
        term_display = cls.get_term_display(variable.previous_term)
        return f"{display_name} ({term_display})"