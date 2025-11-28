"""
精简版Agent管理器 - 展示模块化和简化设计
"""
import logging
from typing import Dict, List, Any, Optional, Callable, TypeVar
from dataclasses import dataclass, field
from datetime import datetime

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class ConversationContext:
    """对话上下文 - 保持简洁的数据结构"""
    session_id: str
    user_id: Optional[str] = None
    history: List[Dict[str, Any]] = field(default_factory=list)
    current_intent: Optional[str] = None
    metadata_cache: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)


class BaseErrorHandler:
    """统一错误处理器 - 集中处理异常和错误响应"""
    
    @staticmethod
    def handle_error(func: Callable) -> Callable:
        """错误处理装饰器"""
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                # 获取当前方法名作为操作类型
                operation = func.__name__[1:] if func.__name__.startswith('_') else func.__name__
                logger.error(f"{operation} 处理错误: {e}")
                return {
                    'success': False,
                    'error': f'{operation} 处理失败: {str(e)}',
                    'session_id': kwargs.get('session_id', args[1] if len(args) > 1 else None)
                }
        return wrapper
    
    @staticmethod
    def create_error_response(message: str, session_id: Optional[str] = None, **kwargs) -> Dict[str, Any]:
        """创建标准化错误响应"""
        response = {'success': False, 'error': message}
        if session_id:
            response['session_id'] = session_id
        response.update(kwargs)
        return response


class SessionManager:
    """会话管理器 - 负责会话的创建、获取和管理"""
    
    def __init__(self):
        self.contexts: Dict[str, ConversationContext] = {}
        self.current_session: Optional[str] = None
    
    def create_session(self, session_id: str, user_id: Optional[str] = None) -> str:
        """创建或获取会话"""
        if session_id not in self.contexts:
            self.contexts[session_id] = ConversationContext(
                session_id=session_id,
                user_id=user_id
            )
            logger.info(f"创建新会话: {session_id}")
        
        self.current_session = session_id
        return session_id
    
    def get_context(self, session_id: str) -> Optional[ConversationContext]:
        """获取会话上下文"""
        return self.contexts.get(session_id)
    
    def update_context(self, session_id: str, **kwargs):
        """更新会话上下文"""
        if session_id in self.contexts:
            context = self.contexts[session_id]
            for key, value in kwargs.items():
                if hasattr(context, key):
                    setattr(context, key, value)
            context.updated_at = datetime.now()
    
    def add_to_history(self, session_id: str, role: str, content: str, metadata: Optional[Dict] = None):
        """添加对话历史"""
        if session_id in self.contexts:
            self.contexts[session_id].history.append({
                'role': role,
                'content': content,
                'metadata': metadata or {},
                'timestamp': datetime.now().isoformat()
            })


class AgentRegistry:
    """Agent注册中心 - 负责代理的注册和管理"""
    """
    把方法名作为key,把具体方法作为value,注册到agents,intent_handlers字典中
    """
    
    def __init__(self):
        self.agents: Dict[str, Any] = {}
        self.intent_handlers: Dict[str, Callable] = {}
    
    def register_agent(self, category: str, name: str, agent_instance):
        """注册代理实例"""
        if category not in self.agents:
            self.agents[category] = {}
        self.agents[category][name] = agent_instance
    
    def register_intent_handler(self, intent: str, handler: Callable):
        """注册意图处理器"""
        self.intent_handlers[intent] = handler
    
    def get_agent(self, category: str, name: str) -> Any:
        """获取代理实例"""
        return self.agents.get(category, {}).get(name)
    
    def get_intent_handler(self, intent: str) -> Optional[Callable]:
        """获取意图处理器"""
        return self.intent_handlers.get(intent)


class SimpleAgentManager:
    """精简版Agent管理器 - 核心控制类"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """初始化Agent管理器"""
        self.config = config or {}
        self.session_manager = SessionManager()
        self.agent_registry = AgentRegistry()
        self.error_handler = BaseErrorHandler()
        
        # 注册代理
        self._register_agents()
        
        logger.info("精简版Agent管理器初始化完成")
    
    def create_session(self, session_id: str, user_id: Optional[str] = None) -> str:
        """创建会话"""
        return self.session_manager.create_session(session_id, user_id)
    
    def get_conversation_history(self, session_id: str) -> Dict[str, Any]:
        """获取对话历史"""
        context = self.session_manager.get_context(session_id)
        if context:
            return {
                'success': True,
                'history': context.history,
                'session_id': session_id
            }
        else:
            return self.error_handler.create_error_response(f"会话 {session_id} 不存在", session_id)
    
    def clear_conversation(self, session_id: str) -> Dict[str, Any]:
        """清空对话历史"""
        context = self.session_manager.get_context(session_id)
        if context:
            context.history = []
            return {
                'success': True,
                'message': "对话历史已清空",
                'session_id': session_id
            }
        else:
            return self.error_handler.create_error_response(f"会话 {session_id} 不存在", session_id)
    
    def _register_agents(self):
        """注册功能性代理"""
        # 使用延迟导入以减少初始化开销
        try:
            from metdata_agent import asset_understanding, data_recommender
            from query_agent.intent_parser import IntentParser
            from text2sql_module import text2sql_processor
            
            # 注册代理
            self.agent_registry.register_agent('metadata', 'understanding', asset_understanding)
            self.agent_registry.register_agent('metadata', 'recommender', data_recommender)
            self.agent_registry.register_agent('query', 'intent_parser', IntentParser())
            self.agent_registry.register_agent('text2sql', 'processor', text2sql_processor)
            
            # 尝试注册客户画像智能体（如果存在）
            try:
                from customer_profile_agent import customer_profile_analyzer
                self.agent_registry.register_agent('customer_profile', 'analyzer', customer_profile_analyzer)
                logger.info("客户画像智能体注册成功")
            except ImportError as e:
                logger.warning(f"客户画像智能体导入失败，可能尚未创建: {e}")
            
            # 注册意图处理器
            self.agent_registry.register_intent_handler('data_retrieval', self._handle_data_query_with_text2sql)
            self.agent_registry.register_intent_handler('metadata_query', self._handle_metadata_query)
            self.agent_registry.register_intent_handler('table_info', self._handle_table_info)
            self.agent_registry.register_intent_handler('schema_query', self._handle_schema_query)
            self.agent_registry.register_intent_handler('data_validation', self._handle_conversation_query)
            
            self.agent_registry.register_intent_handler('data_ranking', self._handle_data_query_with_text2sql)
            self.agent_registry.register_intent_handler('data_count', self._handle_data_query_with_text2sql)
            
            # 注册客户画像相关意图处理器
            self.agent_registry.register_intent_handler('customer_segmentation', self._handle_customer_profile_query)
            self.agent_registry.register_intent_handler('customer_profiling', self._handle_customer_profile_query)
            self.agent_registry.register_intent_handler('customer_risk_analysis', self._handle_customer_profile_query)
            self.agent_registry.register_intent_handler('customer_insight', self._handle_customer_profile_query)
            
        except ImportError as e:
            logger.warning(f"代理导入失败: {e}")
    
    @BaseErrorHandler.handle_error
    def _handle_customer_profile_query(self, query: str, session_id: str) -> Dict[str, Any]:
        """处理客户画像相关查询"""
        # 获取客户画像分析器代理
        profile_analyzer = self.agent_registry.get_agent('customer_profile', 'analyzer')
        if not profile_analyzer:
            return self.error_handler.create_error_response(
                "客户画像分析器不可用", session_id, agent='customer_profile')
        
        # 调用客户画像分析器进行处理
        try:
            result = profile_analyzer.analyze(query, session_id)
            
            # 添加到对话历史
            self.session_manager.add_to_history(
                session_id, 'user', query,
                {'query_type': 'customer_profile', 'intent': 'customer_profiling'}
            )
            self.session_manager.add_to_history(
                session_id, 'agent', f"客户画像分析结果: {result.get('message', '')}",
                {'query_type': 'customer_profile', 'result': result}
            )
            
            return {
                'success': True,
                'intent': 'customer_profiling',
                'analysis_result': result,
                'session_id': session_id
            }
        except Exception as e:
            return self.error_handler.create_error_response(
                f"客户画像分析失败: {str(e)}", session_id, agent='customer_profile')
    
    @BaseErrorHandler.handle_error
    def process_query(self, query: str, session_id: str = 'default') -> Dict[str, Any]:
        """处理用户查询的主要入口"""
        # 确保会话存在
        if session_id not in self.session_manager.contexts:
            self.session_manager.create_session(session_id)
        
        # 记录用户查询
        self.session_manager.add_to_history(session_id, 'user', query)
        logger.info(f"处理查询: {query}")
        
        # 解析意图
        intent_result = self._parse_intent(query, session_id)
        if not intent_result['success']:
            return intent_result
        
        intent = intent_result['intent']
        entities = intent_result['entities']
        
        # 更新上下文
        self.session_manager.update_context(session_id, current_intent=intent.value)
        
        # 根据意图选择处理器
        handler = self.agent_registry.get_intent_handler(intent.value) or self._handle_general_query
        return handler(query, entities, session_id)
    
    @BaseErrorHandler.handle_error
    def _parse_intent(self, query: str, session_id: str) -> Dict[str, Any]:
        """解析查询意图"""
        # 从注册中心获取意图解析器 IntentParser()
        intent_parser = self.agent_registry.get_agent('query', 'intent_parser')
        if not intent_parser:
            return self.error_handler.create_error_response("意图解析器未注册", session_id)
        
        result = intent_parser.parse_intent(query)
        
        # 统一返回格式
        if hasattr(result, 'intent'):
            logger.info(f"解析意图成功: {result.intent.value}")
            return {
                'success': True,
                'intent': result.intent,
                'entities': {
                    'parsed_intent': result,
                    'entities': result.entities,
                    'attributes': getattr(result, 'attributes', None),
                    'conditions': getattr(result, 'conditions', None),
                    'aggregations': getattr(result, 'aggregations', None),
                    'order_by': getattr(result, 'order_by', None),
                    'limit': getattr(result, 'limit', None)
                }
            }
        return result
    
    @BaseErrorHandler.handle_error
    def _handle_data_query_with_text2sql(self, query: str, entities: Dict[str, Any], session_id: str) -> Dict[str, Any]:
        """使用Text2SQL处理数据查询"""
        text2sql_processor = self.agent_registry.get_agent('text2sql', 'processor')
        if not text2sql_processor:
            return self.error_handler.create_error_response("Text2SQL处理器未注册", session_id)
        
        # 获取会话上下文
        context = self.session_manager.get_context(session_id)
        conversation_history = context.history if context else []
        
        logger.info(f"使用Text2SQL处理数据查询: {query}")
        # 确保传递的历史记录只包含对话内容，不包含复杂的metadata结构
        simplified_history = []
        for item in conversation_history:
            # 只提取对话的基本信息，确保格式清晰
            simplified_item = {
                'role': item['role'],
                'content': item['content']
            }
            simplified_history.append(simplified_item)
        
        # 传递简化后的历史记录，确保模型能更好地理解上下文关联
        result = text2sql_processor.process_query(query, session_id, entities, simplified_history)
        
        if result['success']:
            # 保存当前查询和实体信息到上下文缓存
            if context:
                context.metadata_cache['last_query'] = query
                context.metadata_cache['last_entities'] = entities
                context.metadata_cache['last_result'] = result
            
            # 记录助手回复
            self.session_manager.add_to_history(session_id, 'assistant', 
                                              result['explanation'],
                                              {'sql_query': result['sql_query'], 'execution_result': result.get('execution_result', {})})
            
            # 添加标准信息
            result.update({'agent': 'text2sql', 'intent': 'data_retrieval'})
        
        return result
    
    @BaseErrorHandler.handle_error
    def _handle_metadata_query(self, query: str, entities: Dict[str, Any], session_id: str) -> Dict[str, Any]:
        """处理元数据查询"""
        asset_understanding = self.agent_registry.get_agent('metadata', 'understanding')
        data_recommender = self.agent_registry.get_agent('metadata', 'recommender')
        
        if 'tables' in entities and asset_understanding:
            # 分析特定表
            results = []
            for table_name in entities['tables']:
                result = asset_understanding.analyze_table_structure(table_name)
                if result['success']:
                    results.append(result)
            
            response = {
                'success': True,
                'intent': 'metadata_query',
                'type': 'table_analysis',
                'results': results,
                'session_id': session_id
            }
        elif data_recommender:
            # 获取数据推荐
            recommendations = data_recommender.recommend_tables(query)
            response = {
                'success': True,
                'intent': 'metadata_query',
                'type': 'recommendations',
                'recommendations': recommendations,
                'session_id': session_id
            }
        else:
            return self.error_handler.create_error_response("元数据处理代理未注册", session_id)
        
        # 记录助手回复
        result_count = len(response.get('results', response.get('recommendations', [])))
        self.session_manager.add_to_history(session_id, 'assistant', 
                                          f"已处理元数据查询，返回{result_count}个结果")
        
        return response
    
    @BaseErrorHandler.handle_error
    def _handle_table_info(self, query: str, entities: Dict[str, Any], session_id: str) -> Dict[str, Any]:
        """处理表信息查询"""
        asset_understanding = self.agent_registry.get_agent('metadata', 'understanding')
        if not asset_understanding:
            return self.error_handler.create_error_response("元数据理解代理未注册", session_id)
        
        if 'tables' not in entities or not entities['tables']:
            return self.error_handler.create_error_response("未指定要查询的表名", session_id)
        
        table_name = entities['tables'][0]  # 取第一个表名
        result = asset_understanding.analyze_table_structure(table_name)
        
        if result['success']:
            # 获取字段语义分析（如果可用）
            field_analysis = asset_understanding.analyze_field_semantics(table_name)
            if field_analysis.get('success'):
                result['field_semantics'] = field_analysis.get('fields', [])
        
        # 记录助手回复
        self.session_manager.add_to_history(session_id, 'assistant', 
                                          f"已获取表 {table_name} 的详细信息")
        
        result['session_id'] = session_id
        return result
    
    @BaseErrorHandler.handle_error
    def _handle_schema_query(self, query: str, entities: Dict[str, Any], session_id: str) -> Dict[str, Any]:
        """处理模式查询"""
        try:
            from utils.database import db_manager
            tables = db_manager.get_all_tables()
            
            # 获取推荐表
            recommendations = []
            data_recommender = self.agent_registry.get_agent('metadata', 'recommender')
            if data_recommender:
                recommendations = data_recommender.recommend_tables(query)
            
            response = {
                'success': True,
                'intent': 'schema_query',
                'tables': tables,
                'recommendations': recommendations,
                'session_id': session_id
            }
            
            # 记录助手回复
            self.session_manager.add_to_history(session_id, 'assistant', 
                                              f"已获取数据库模式信息，共{len(tables)}个表")
            
            return response
        except Exception as e:
            return self.error_handler.create_error_response(f"获取模式信息失败: {str(e)}", session_id)
    
    @BaseErrorHandler.handle_error
    def _handle_conversation_query(self, query: str, entities: Dict[str, Any], session_id: str) -> Dict[str, Any]:
        """处理对话查询"""
        query_lower = query.lower()
        
        responses = {
            ('你是谁', '你是什么'): "我是多智能体数据查询系统，可以帮助您：\n• 自然语言查询数据库\n• 生成SQL代码\n• 分析数据结构\n• 提供查询建议",
            ('你能做什么', '功能'): "我可以帮您：\n• 使用自然语言查询数据库数据\n• 生成和执行SQL语句\n• 分析数据库表结构\n• 提供数据查询建议\n• 支持多种查询类型（检索、分析、统计等）",
            ('怎么用', '如何使用'): "使用方法：\n1. 直接输入自然语言查询，如'显示所有用户信息'\n2. 使用'sql <查询>'命令进行Text2SQL查询\n3. 使用'tables'命令查看所有表\n4. 使用'table <表名>'查看表结构\n5. 输入'help'查看更多帮助",
            ('帮助', 'help'): "帮助信息：\n• 自然语言查询：直接输入问题\n• SQL查询：sql <查询内容>\n• 查看表列表：tables\n• 查看表结构：table <表名>\n• 查看状态：status\n• 查看历史：history\n• 清除历史：clear\n• 退出：quit/exit"
        }
        
        response_text = None
        for keywords, text in responses.items():
            if any(keyword in query_lower for keyword in keywords):
                response_text = text
                break
        
        if not response_text:
            response_text = "我理解您的问题，但我主要专注于数据查询相关任务。您可以：\n• 询问数据库相关的问题\n• 请求生成SQL查询\n• 查看表结构和数据\n• 输入'help'查看更多功能"
        
        response = {
            'success': True,
            'intent': 'conversation',
            'message': response_text,
            'suggestions': [
                '查询所有表的信息',
                '获取特定表的结构',
                '执行数据查询',
                '分析字段含义'
            ],
            'session_id': session_id
        }
        
        # 记录助手回复
        self.session_manager.add_to_history(session_id, 'assistant', response_text)
        
        return response
    
    def _handle_general_query(self, query: str, entities: Dict[str, Any], session_id: str) -> Dict[str, Any]:
        """处理通用查询"""
        suggestions = self._generate_query_suggestions(query)
        
        response = {
            'success': True,
            'intent': 'general',
            'message': '我不太确定您的查询意图，以下是一些建议：',
            'suggestions': suggestions,
            'session_id': session_id
        }
        
        # 记录助手回复
        self.session_manager.add_to_history(session_id, 'assistant', response['message'])
        
        return response
    
    def _generate_query_suggestions(self, query: str) -> List[str]:
        """生成查询建议"""
        suggestions = [
            "查询所有表的信息",
            "获取特定表的结构",
            "执行数据查询",
            "分析字段含义"
        ]
        
        # 根据查询内容生成更具体的建议
        keywords_mapping = {
            '客户': "查询客户信息",
            '订单': "查询订单信息",
            '统计': "统计数据汇总"
        }
        
        for keyword, suggestion in keywords_mapping.items():
            if keyword in query:
                suggestions.insert(0, suggestion)
                break
        
        return suggestions[:5]  # 返回最多5个建议
    
    # 其他处理方法可以按照类似模式实现
    # ...
    
    def get_agent_status(self) -> Dict[str, Any]:
        """获取所有agent的状态"""
        try:
            from utils.database import db_manager
            
            return {
                'success': True,
                'manager': {
                    'status': 'active',
                    'sessions': len(self.session_manager.contexts),
                    'current_session': self.session_manager.current_session
                },
                'agents': {category: {name: 'active' for name in agents} 
                          for category, agents in self.agent_registry.agents.items()},
                'database': {
                    'connected': hasattr(db_manager, 'execute_query')
                }
            }
        except Exception as e:
            return self.error_handler.create_error_response(f"获取状态失败: {str(e)}")


# 导出函数式API
_simple_manager = None

def get_manager(config: Optional[Dict[str, Any]] = None) -> SimpleAgentManager:
    """获取管理器单例"""
    global _simple_manager
    if _simple_manager is None:
        _simple_manager = SimpleAgentManager(config)
    return _simple_manager
#所谓函数式api其实就是让我们在调用的时候不需要实例化，直接调用函数即可
#result = get_manager().process_query(query, session_id)-》result = process_query(query, session_id)
def process_query(query: str, session_id: str = 'default') -> Dict[str, Any]:
    """函数式API：处理查询"""
    return get_manager().process_query(query, session_id)

def create_session(session_id: str, user_id: Optional[str] = None) -> str:
    """函数式API：创建会话"""
    return get_manager().session_manager.create_session(session_id, user_id)

def get_status() -> Dict[str, Any]:
    """函数式API：获取状态"""
    return get_manager().get_agent_status()
