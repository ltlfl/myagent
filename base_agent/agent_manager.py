"""
Agent管理器 - 基于精简版实现
"""
from typing import Dict, List, Any, Optional
import logging

# 导入精简版实现
from base_agent.simple_agent_manager import (
    SimpleAgentManager, get_manager, process_query, 
    create_session as simple_create_session, get_status, ConversationContext
)

# 配置日志
logger = logging.getLogger(__name__)

# 创建管理器实例，保持向后兼容
agent_manager = get_manager()

# 添加必要的兼容性方法，确保现有代码能正常运行

def get_conversation_history(session_id: str) -> Dict[str, Any]:
    """获取对话历史"""
    try:
        context = agent_manager.session_manager.get_context(session_id)
        if context:
            return {
                'success': True,
                'history': context.history,
                'session_id': session_id
            }
        else:
            return {
                'success': False,
                'error': f"会话 {session_id} 不存在",
                'session_id': session_id
            }
    except Exception as e:
        logger.error(f"获取对话历史失败: {e}")
        return {
            'success': False,
            'error': f"获取对话历史失败: {str(e)}",
            'session_id': session_id
        }

def clear_conversation(session_id: str) -> Dict[str, Any]:
    """清空对话历史"""
    try:
        context = agent_manager.session_manager.get_context(session_id)
        if context:
            context.history = []
            return {
                'success': True,
                'message': "对话历史已清空",
                'session_id': session_id
            }
        else:
            return {
                'success': False,
                'error': f"会话 {session_id} 不存在",
                'session_id': session_id
            }
    except Exception as e:
        logger.error(f"清空对话历史失败: {e}")
        return {
            'success': False,
            'error': f"清空对话历史失败: {str(e)}",
            'session_id': session_id
        }

def process_text2sql_query(query: str, session_id: str) -> Dict[str, Any]:
    """处理Text2SQL查询"""
    try:
        # 使用process_query方法并指定意图，正真使用的是SimpleAgentManager的process_query方法
        result = process_query(query, session_id)
        
        # 确保返回格式包含必要的字段
        if result['success'] and result.get('intent') == 'data_retrieval':
            # 确保有sql_query字段
            if 'sql_query' not in result and 'generated_sql' in result:
                result['sql_query'] = result['generated_sql']
            
            # 确保有execution_result字段
            if 'execution_result' not in result and 'execution' in result:
                result['execution_result'] = result['execution']
        
        return result
    except Exception as e:
        logger.error(f"Text2SQL查询处理失败: {e}")
        return {
            'success': False,
            'error': f"Text2SQL查询处理失败: {str(e)}",
            'session_id': session_id,
            'agent': 'text2sql'
        }

# 重新导出原有方法以确保向后兼容
def create_session(session_id: str, user_id: Optional[str] = None) -> str:
    """创建会话"""
    return simple_create_session(session_id, user_id)

def get_agent_status() -> Dict[str, Any]:
    """获取智能体状态"""
    return get_status()

# 保持AgentManager类名的兼容性，以便旧代码可能的直接引用
class AgentManager(SimpleAgentManager):
    """兼容类，继承自SimpleAgentManager"""
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config)

logger.info("Agent管理器初始化完成（使用精简版实现）")