"""
Text2SQL模块
提供自然语言转SQL的功能
"""
# 导入传统Text2SQL处理器
try:
    from .text2sql_processor import Text2SQLProcessor as LegacyText2SQLProcessor
    # 不再直接导入实例，而是在需要时创建
    # 提供一个函数来获取处理器实例
    def get_text2sql_processor():
        """获取Text2SQL处理器实例"""
        return LegacyText2SQLProcessor()
    
    # 为了向后兼容，提供一个懒加载的实例
    text2sql_processor = None
except ImportError:
    # 如果无法导入，提供占位符
    LegacyText2SQLProcessor = None
    text2sql_processor = None
    get_text2sql_processor = lambda: None

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