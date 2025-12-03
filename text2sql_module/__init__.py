"""
Text2SQL模块
提供自然语言转SQL的功能
"""
# 导入传统Text2SQL处理器
try:
    from .huaxiang_processor import Text2SQLProcessor as LegacyText2SQLProcessor
    from .huaxiang_processor import text2sql_processor
except ImportError:
    # 如果无法导入，提供占位符
    LegacyText2SQLProcessor = None
    text2sql_processor = None

# 导入基于LangGraph的Text2SQL处理器
try:
    from .text2sql_processor_langgraph import Text2SQLProcessor
    from .text2sql_processor_langgraph import Text2SQLProcessor as Text2SQLProcessorLangGraph
    # 明确提供别名以便区分
except ImportError:
    # 如果无法导入LangGraph版本，回退到传统版本
    if LegacyText2SQLProcessor is not None:
        Text2SQLProcessor = LegacyText2SQLProcessor
        Text2SQLProcessorLangGraph = None
    else:
        Text2SQLProcessor = None
        Text2SQLProcessorLangGraph = None

__all__ = ['Text2SQLProcessor', 'Text2SQLProcessorLangGraph', 'LegacyText2SQLProcessor', 'text2sql_processor']