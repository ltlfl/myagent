import sys
import logging
import os

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# 全局变量，用于标记是否已初始化
_logger_initialized = False


def init_logger(level=logging.INFO, log_file='myagent.log'):
    """
    初始化日志系统
    
    Args:
        level: 日志级别，默认为INFO
        log_file: 日志文件路径，默认为myagent.log
    """
    global _logger_initialized
    
    # 如果已经初始化过，则不再重复初始化
    if _logger_initialized:
        return
    
    # 获取根日志记录器
    root_logger = logging.getLogger()
    
    # 清除现有的处理器（如果有）
    if root_logger.handlers:
        root_logger.handlers.clear()
    
    # 设置日志级别
    root_logger.setLevel(level)
    
    # 创建并添加文件处理器
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(level)
    
    # 创建并添加控制台处理器
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(level)
    
    # 定义日志格式
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    stream_handler.setFormatter(formatter)
    
    # 添加处理器到根日志记录器
    root_logger.addHandler(file_handler)
    root_logger.addHandler(stream_handler)
    
    # 标记为已初始化
    _logger_initialized = True
    
    # 获取logger_set.py的日志记录器
    logger = logging.getLogger(__name__)
    logger.info(f"日志系统初始化完成，将记录到{log_file}文件")


def get_logger(name=None):
    """
    获取配置好的logger实例
    
    Args:
        name: logger的名称，默认为None（返回根logger）
    
    Returns:
        配置好的logger实例
    """
    # 如果日志系统尚未初始化，则先初始化
    if not _logger_initialized:
        init_logger()
    
    # 返回指定名称的logger
    return logging.getLogger(name)


# 模块级别的logger，可以直接从该模块导入使用
logger = get_logger(__name__)