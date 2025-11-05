from django import template
from forecast.services import ModelVariableDisplayService

register = template.Library()
display_service = ModelVariableDisplayService()

@register.filter
def split(value, arg):
    """文字列を指定された区切り文字で分割"""
    return value.split(arg)

@register.filter
def variable_display_name(value):
    """変数名を日本語表示に変換"""
    return display_service.get_display_name(value)

@register.filter
def term_display(value):
    """期間を0.5カ月単位で表示"""
    return display_service.get_term_display(value)

@register.filter
def format_variable(variable):
    """変数の完全な表示形式を返す"""
    return display_service.format_variable_display(variable)