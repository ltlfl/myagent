"""
统一错误处理和日志记录工具
"""

import logging
import traceback
from typing import Dict, Any, Optional
from datetime import datetime

# 配置日志格式
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('myagent.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


class ErrorHandler:
    """统一错误处理器"""
    
    @staticmethod
    def handle_error(
        error: Exception,
        context: str,
        session_id: Optional[str] = None,
        additional_info: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        统一处理错误
        
        Args:
            error: 异常对象
            context: 错误上下文描述
            session_id: 会话ID
            additional_info: 额外信息
        
        Returns:
            标准化的错误响应
        """
        error_msg = str(error)
        error_type = type(error).__name__
        
        # 记录错误日志
        logger.error(f"错误发生在 {context}: {error_type}: {error_msg}")
        logger.debug(f"错误堆栈: {traceback.format_exc()}")
        
        # 构建错误响应
        error_response = {
            'success': False,
            'error': f'{context}失败: {error_msg}',
            'error_type': error_type,
            'timestamp': datetime.now().isoformat(),
            'session_id': session_id
        }
        
        # 添加额外信息
        if additional_info:
            error_response.update(additional_info)
        
        return error_response
    
    @staticmethod
    def log_info(message: str, context: str = "", session_id: Optional[str] = None):
        """记录信息日志"""
        log_msg = f"[{context}] {message}" if context else message
        if session_id:
            log_msg = f"[{session_id}] {log_msg}"
        logger.info(log_msg)
    
    @staticmethod
    def log_warning(message: str, context: str = "", session_id: Optional[str] = None):
        """记录警告日志"""
        log_msg = f"[{context}] {message}" if context else message
        if session_id:
            log_msg = f"[{session_id}] {log_msg}"
        logger.warning(log_msg)
    
    @staticmethod
    def log_debug(message: str, context: str = "", session_id: Optional[str] = None):
        """记录调试日志"""
        log_msg = f"[{context}] {message}" if context else message
        if session_id:
            log_msg = f"[{session_id}] {log_msg}"
        logger.debug(log_msg)


def safe_execute(
    func,
    error_context: str,
    session_id: Optional[str] = None,
    default_return: Any = None,
    raise_on_error: bool = False
):
    """
    安全执行函数，统一处理异常
    
    Args:
        func: 要执行的函数
        error_context: 错误上下文
        session_id: 会话ID
        default_return: 出错时的默认返回值
        raise_on_error: 是否重新抛出异常
    
    Returns:
        函数执行结果或默认返回值
    """
    try:
        return func()
    except Exception as e:
        error_handler = ErrorHandler()
        error_response = error_handler.handle_error(e, error_context, session_id)
        
        if raise_on_error:
            raise e
        
        if default_return is not None:
            return default_return
        
        return error_response


# 全局错误处理器实例
error_handler = ErrorHandler()