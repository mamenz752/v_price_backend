from django import template
from feedback.services import FeedbackService

register = template.Library()

@register.filter
def format_variable(var):
    """
    変数の表示用フォーマットを行うフィルター
    """
    service = FeedbackService()
    return f"{service.format_variable_name(var['name'])} ({service.format_variable_term(var['previous_term'])})"

@register.filter
def variable_display_name(name):
    """
    変数名を日本語表示用にフォーマットするフィルター
    """
    service = FeedbackService()
    return service.format_variable_name(name)

@register.filter
def term_display(term):
    """
    期間を月単位でフォーマットするフィルター
    """
    service = FeedbackService()
    return service.format_variable_term(term)