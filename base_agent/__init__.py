"""
基础智能体模块 - 多智能体系统的核心管理器
提供智能体协调、会话管理、查询处理等核心功能
"""

from .agent_manager import agent_manager, AgentManager, ConversationContext

__all__ = ['agent_manager', 'AgentManager', 'ConversationContext']