#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
AutoGen多智能体协作系统
利用AutoGen的智能体对话协议实现Text2SQL和客户细分功能的自动协作
"""

import os
import sys
import asyncio
import logging
import hashlib
from datetime import datetime
from typing import Dict, Any, List, Optional, Set
from dotenv import load_dotenv
import autogen
from autogen import Agent, AssistantAgent, UserProxyAgent, GroupChat, GroupChatManager

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# 加载环境变量
load_dotenv()

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_database_schema(table_name: str = None) -> str:
    """获取数据库表结构信息 - 全局函数版本"""
    try:
        # 导入数据库管理器
        from utils.database import db_manager
        
        # 首先获取数据库中所有表名
        all_tables = db_manager.get_all_tables()
        
        if table_name:
            # 检查表是否存在
            if table_name not in all_tables:
                # 如果表不存在，返回所有可用表名供参考
                available_tables = "，".join(all_tables[:10])
                return f"未找到表 '{table_name}'。数据库中可用表包括：{available_tables}"
            
            # 获取特定表的结构
            table_schema = db_manager.get_table_schema(table_name)
            if table_schema:
                schema_info = f"表 {table_name} 结构：\n"
                for column in table_schema:
                    schema_info += f"- {column['column_name']} ({column['data_type']})"
                    if column['column_key'] == 'PRI':
                        schema_info += " [主键]"
                    if column['is_nullable'] == 'NO':
                        schema_info += " [非空]"
                    if column['column_comment']:
                        schema_info += f" - {column['column_comment']}"
                    schema_info += "\n"
                return schema_info
            else:
                return f"表 {table_name} 结构为空"
        else:
            # 获取所有表的结构概览
            if all_tables:
                schema_info = "数据库表结构概览：\n"
                schema_info += f"总表数：{len(all_tables)}\n\n"
                
                # 查找包含风险相关字段的表
                risk_tables = []
                for table in all_tables[:15]:  # 限制检查前15个表
                    try:
                        table_schema = db_manager.get_table_schema(table)
                        # 检查是否包含风险相关字段
                        risk_fields = [col for col in table_schema if 
                                     'risk' in col['column_name'].lower() or 
                                     'overdue' in col['column_name'].lower() or
                                     'level' in col['column_name'].lower()]
                        if risk_fields:
                            risk_tables.append((table, risk_fields))
                    except:
                        continue
                
                if risk_tables:
                    schema_info += "包含风险相关字段的表：\n"
                    for table, fields in risk_tables:
                        schema_info += f"\n表 {table}：\n"
                        for field in fields:
                            schema_info += f"  - {field['column_name']} ({field['data_type']})\n"
                
                # 显示所有表的结构概览
                schema_info += "\n所有表结构概览：\n"
                for table in all_tables[:15]:  # 限制显示前10个表
                    table_schema = db_manager.get_table_schema(table)
                    schema_info += f"\n表 {table} 结构：\n"
                    for column in table_schema[:10]:  # 每个表显示前5个字段
                        schema_info += f"  - {column['column_name']} ({column['data_type']})"
                        if column['column_key'] == 'PRI':
                            schema_info += " [主键]"
                        schema_info += "\n"
                    if len(table_schema) > 5:
                        schema_info += f"  ... 还有 {len(table_schema) - 5} 个字段\n"
                
                if len(all_tables) > 10:
                    schema_info += f"\n... 还有 {len(all_tables) - 10} 个表未显示"
                
                return schema_info
            else:
                return "数据库中没有表"
    except Exception as e:
        logger.error(f"获取数据库结构出错: {e}")
        return f"获取数据库结构异常: {str(e)}"


class QueryClassifier:
    """查询分类器，负责识别查询类型"""
    
    @staticmethod
    def classify_query(query: str) -> str:
        """分类查询类型
        
        Args:
            query: 用户查询文本
            
        Returns:
            查询类型：text2sql 或 segmentation
        """
        # Text2SQL查询关键词
        text2sql_keywords = [
            "查询", "统计", "获取", "计算", "有哪些", "有多少", 
            "显示", "列出", "找出", "哪些", "多少", "求和", 
            "平均值", "最大值", "最小值", "排序", "筛选"
        ]
        
        # 客户细分查询关键词
        segmentation_keywords = [
            "客户细分", "客户对比", "分析客群", "目标客群", "高价值客户",
            "普通客户", "客户特征", "年龄段客户", "对比不同", "年龄段",
            "客群特征", "客户行为", "存款行为"
        ]
        
        # 特殊处理：如果包含"对比"和"年龄/年龄段"，优先识别为segmentation
        if "对比" in query and any(age_word in query for age_word in ["年龄", "年龄段"]):
            return "segmentation"
        
        # 检查是否包含细分关键词
        for keyword in segmentation_keywords:
            if keyword in query:
                return "segmentation"
        
        # 检查是否包含SQL查询关键词
        for keyword in text2sql_keywords:
            if keyword in query:
                return "text2sql"
        
        # 默认返回text2sql
        return "text2sql"


class Text2SQLFunctionAgent(AssistantAgent):
    """Text2SQL功能智能体，集成现有的Text2SQL处理器"""
    
    def __init__(self, name="text2sql_agent", llm_config=None):
        """初始化Text2SQLFunctionAgent
        
        Args:
            name: 智能体名称
            llm_config: LLM配置参数
        """
        default_llm_config = llm_config or {
            "model": "qwen2.5-14b-instruct",
            "temperature": 0.1,
            "max_tokens": 5000,
            "timeout": 120,
            "api_key": os.getenv("OPENAI_API_KEY"),
            "base_url": os.getenv("OPENAI_BASE_URL", "https://dashscope.aliyuncs.com/compatible-mode/v1")
        }
        
        system_message = """你是一个专业的Text2SQL数据查询智能体，擅长将自然语言转换为SQL查询。
        你的主要职责是：
        1. 理解用户的自然语言查询
        2. 调用SQL查询工具获取数据
        3. 当查询不明确时，向用户提问以获取更多信息
        4. 总结查询结果并提供自然语言解释
        如果你收到客户细分相关的查询，请将其转交给客户细分智能体处理。
        """
        
        super().__init__(name=name, system_message=system_message, llm_config=default_llm_config)
        
        # 初始化Text2SQL处理器
        try:
            from text2sql_module.text2sql_processor_langgraph import Text2SQLProcessorLangGraph
            self.text2sql_processor = Text2SQLProcessorLangGraph()
            logger.info("Text2SQLProcessorLangGraph初始化成功")
        except Exception as e:
            logger.error(f"初始化Text2SQL处理器失败: {e}")
            self.text2sql_processor = None


class CustomerSegmentationFunctionAgent(AssistantAgent):
    """客户细分功能智能体，集成现有的客户细分处理器"""
    
    def __init__(self, name="segmentation_agent", llm_config=None):
        """初始化CustomerSegmentationFunctionAgent
        
        Args:
            name: 智能体名称
            llm_config: LLM配置参数
        """
        default_llm_config = llm_config or {
            "model": "qwen2.5-14b-instruct",
            "temperature": 0.1,
            "max_tokens": 5000,
            "timeout": 120,
            "api_key": os.getenv("OPENAI_API_KEY"),
            "base_url": os.getenv("OPENAI_BASE_URL", "https://dashscope.aliyuncs.com/compatible-mode/v1")
        }
        
        system_message = """你是一个专业的客户分析智能体，擅长客户细分和对比分析。
        你的主要职责是：
        1. 分析用户的客户细分需求
        2. 提取目标客群和对照组信息
        3. 调用客户细分工具执行分析
        4. 生成客户对比分析报告
        5. 当需求不明确时，向用户追问以获取更多细节
        如果你收到简单的SQL查询请求，请将其转交给Text2SQL智能体处理。
        """
        
        super().__init__(name=name, system_message=system_message, llm_config=default_llm_config)
        
        # 初始化客户细分处理器
        try:
            from huaxiang.CustomerSegmentation import CustomerSegmentationLangGraph
            self.segmentation_processor = CustomerSegmentationLangGraph()
            logger.info("CustomerSegmentationLangGraph初始化成功")
        except Exception as e:
            logger.error(f"初始化客户细分处理器失败: {e}")
            self.segmentation_processor = None


class IntegratedMultiAgentSystem:
    """基于AutoGen的集成多智能体系统，实现智能体自动协作"""
    
    def __init__(self):
        """初始化集成系统"""
        self.session_id = f"session_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        self.conversation_history: List[Dict[str, Any]] = []
        self.query_classifier = QueryClassifier()
        
        # 初始化AutoGen智能体系统
        self._init_autogen_agents()
        
        logger.info(f"集成多智能体系统初始化完成，会话ID: {self.session_id}")
    
    def _init_autogen_agents(self):
        """初始化AutoGen智能体系统"""
        # 初始化LLM配置 - 配置正确的API基础URL以支持Qwen模型
        llm_config = {
            "model": "qwen2.5-14b-instruct",
            "temperature": 0.1,
            "max_tokens": 5000,
            "timeout": 120,
            "api_key": os.getenv("OPENAI_API_KEY"),
            "base_url": os.getenv("OPENAI_BASE_URL", "https://dashscope.aliyuncs.com/compatible-mode/v1")
        }
        
        # 创建Text2SQL智能体
        self.text2sql_agent = Text2SQLFunctionAgent(llm_config=llm_config)
        
        # 创建客户细分智能体
        self.segmentation_agent = CustomerSegmentationFunctionAgent(llm_config=llm_config)
        
        # 创建决策与调度智能体
        self.decision_agent = AssistantAgent(
            name="decision_agent",
            llm_config=llm_config,
            system_message="""你是一个智能决策调度中心，负责协调Text2SQL和客户细分智能体。
            你的核心职责是：
            1. 快速分析用户的查询需求，明确问题本质
            2. 判断应该由哪个智能体来处理（Text2SQL或客户细分）
            3. 直接转发查询给合适的智能体，避免不必要的中间步骤
            4. 当查询不明确时，向用户追问以获取更多信息
            5. 协调智能体之间的协作，解决复杂问题
            
            智能体分工原则：
            - Text2SQL智能体：处理数据查询、统计、简单分析等SQL相关任务
            - 客户细分智能体：处理客户群体对比、行为分析、细分建模等复杂分析任务
            
            决策流程优化：
            - 对于"分析存款多的客户群体和贷款多的客户群体的区别"这类问题，直接转给客户细分智能体
            - 对于"查询客户总数"这类简单查询，直接转给Text2SQL智能体
            - 仅在确实需要了解表结构时才使用get_database_schema工具
            
            避免的行为：
            - 不要重复查询表结构
            - 不要进行不必要的中间分析
            - 不要代替其他智能体执行任务
            
            调用工具的格式（仅在必要时使用）：
            ```python
            get_database_schema(table_name="表名")  # 获取特定表结构
            get_database_schema()  # 获取所有表结构概览
            ```
            
            示例：
            ```python
            # 仅在确实需要了解表结构时才调用
            get_database_schema(table_name="customer_info")
            ```"""
        )
        
        # 设置工具映射，实现无缝集成
        self._setup_function_map()
        
        # 创建智能体群组和管理器
        self._create_group_chat()
    
    def _setup_function_map(self):
        """设置函数映射，集成现有工具"""
        # 定义可用的工具函数
        def execute_sql_query(question: str) -> str:
            """执行SQL查询"""
            try:
                # 检查text2sql_processor是否可用
                if hasattr(self.text2sql_agent, 'text2sql_processor') and self.text2sql_agent.text2sql_processor:
                    # 使用text2sql_processor处理自然语言查询
                    result = self.text2sql_agent.text2sql_processor.process_query(
                        question=question,
                        session_id=self.session_id,
                        entities=None,
                        conversation_history=self.conversation_history
                    )
                    
                    # 转换为AutoGen兼容的字符串格式
                    if result.get('success', False):
                        explanation = result.get('explanation', '查询执行成功')
                        sql_query = result.get('sql_query', '')
                        query_result = result.get('query_result', [])
                        
                        # 构建详细的响应
                        response_str = f"SQL查询执行成功:\n"
                        if sql_query:
                            response_str += f"生成的SQL: {sql_query}\n"
                        response_str += f"解释: {explanation}\n"
                        
                        if query_result:
                            response_str += f"返回 {len(query_result)} 条记录：\n"
                            # 显示前5条记录
                            for i, row in enumerate(query_result[:10]):
                                response_str += f"记录 {i+1}: {dict(row)}\n"
                            
                            if len(query_result) > 5:
                                response_str += f"... 还有 {len(query_result) - 5} 条记录\n"
                        
                        return response_str
                    else:
                        error = result.get('error', '未知错误')
                        return f"SQL查询执行失败: {error}"
                else:
                    # 当text2sql_processor不可用时，返回友好的错误信息
                    # 注意：绝对不应该直接执行用户输入作为SQL语句，这会导致SQL注入漏洞
                    logger.warning("Text2SQL处理器不可用，无法执行查询")
                    return "查询服务暂时不可用\n\n原因：Text2SQL处理模块未正确初始化\n建议：请检查服务状态或联系系统管理员进行故障排查。\n安全提示：系统已自动阻止了潜在的SQL注入风险。"
            except Exception as e:
                logger.error(f"执行SQL查询出错: {e}")
                return f"SQL查询执行异常: {str(e)}"
        
        def execute_segmentation_analysis(query: str) -> str:
            """执行客户细分分析"""
            try:
                if hasattr(self.segmentation_agent, 'segmentation_processor') and self.segmentation_agent.segmentation_processor:
                    result = self.segmentation_agent.segmentation_processor.process_query(query)
                    # 转换为AutoGen兼容的字符串格式
                    if result.get('success', True):
                        explanation = result.get('explanation', result.get('content', '客户细分分析完成'))
                        return f"客户细分分析完成: {explanation}"
                    else:
                        error = result.get('error', '未知错误')
                        return f"客户细分分析失败: {error}"
                else:
                    return "客户细分处理器未初始化"
            except Exception as e:
                logger.error(f"执行客户细分分析出错: {e}")
                return f"客户细分分析异常: {str(e)}"
        
        # get_database_schema函数已移至全局作用域
        
        # 创建函数映射
        function_map = {
            "execute_sql_query": execute_sql_query,
            "execute_segmentation_analysis": execute_segmentation_analysis,
            "get_database_schema": get_database_schema,  # 现在直接引用全局函数
            "generate_control_group_query": lambda target_query: self._generate_control_group_query(target_query)
        }
        
        # 如果user_proxy已经存在，只更新函数映射
        #UserProxyAgent配置function_map是因为它确实会自主调用这些函数
        if hasattr(self, 'user_proxy') and self.user_proxy:
            self.user_proxy.function_map = function_map
        else:
            # 创建用户代理
            self.user_proxy = UserProxyAgent(
                name="user_proxy",
                system_message="""你是用户代理，负责接收用户请求并协调智能体完成任务。
                
                你的主要职责包括：
                1. 接收用户的分析需求
                2. 对于客户细分分析需求，自动生成对照组查询问题
                3. 协调Text2SQL智能体和客户细分智能体完成分析任务
                4. 整合分析结果并返回给用户
                要求：
                - 分析的结果不要写当前的查询的缺点，而是写当前查询的优点
                - 内容最多写四节，1.查询数据分析，2.对照问题分析，3.对比分析，4.总结
                
                对于客户细分分析需求（如对比分析、群体分析等），你需要：
                - 首先识别目标组查询需求
                - 使用generate_control_group_query工具生成合理的对照组查询问题
                - 将目标组和对照组查询问题转交给客户细分智能体进行深度分析
                
                调用generate_control_group_query工具的格式：
                ```python
                generate_control_group_query(target_query="目标组查询问题")
                ```
                
                示例：
                ```python
                # 当用户请求分析存款多的客户群体时
                control_result = generate_control_group_query(target_query="存款多的客户群体")
                if control_result.get('success'):
                    control_query = control_result.get('control_query')
                    # 将目标组和对照组转交给客户细分智能体
                    execute_segmentation_analysis(query=f"分析目标组：存款多的客户群体，对照组：{control_query}")
                ```
                
                重要说明：
                - 对于简单查询需求，直接转交给Text2SQL智能体
                - 对于复杂分析需求，先生成对照组，再转交给客户细分智能体
                - 确保分析完成后返回完整的结果""",
                human_input_mode="NEVER",  # 自动模式
                max_consecutive_auto_reply=3,  # 减少连续回复次数，防止无限循环
                is_termination_msg=lambda x: x.get("content", "").strip().endswith("<END>") or "分析完成" in x.get("content", ""),
                code_execution_config={"use_docker": False},  # 禁用Docker依赖
                function_map=function_map
            )
        
        # 保存原始处理器实例
        text2sql_processor = getattr(self.text2sql_agent, 'text2sql_processor', None)
        segmentation_processor = getattr(self.segmentation_agent, 'segmentation_processor', None)
        
        # 重新创建智能体实例，包含完整的系统提示
        text2sql_system_message = """你是一个专业的Text2SQL智能体，擅长将自然语言转换为SQL查询。
        你的主要职责是：
        1. 理解用户的自然语言查询，识别查询意图
        2. 将自然语言转换为SQL查询语句
        3. 调用SQL查询工具获取数据
        4. 分析查询结果，提供有意义的解释
        5. 当查询不明确时，向用户提问以获取更多信息
        6. 总结查询结果并提供自然语言解释
        
        智能体分工：
        - 你负责处理数据查询、统计、简单分析等SQL相关任务
        - 客户细分智能体负责处理客户群体对比、行为分析、细分建模等复杂分析任务
        - 如果收到复杂分析需求（如客户群体对比），请转交给客户细分智能体
        
        重要说明：
        - execute_sql_query工具会自动将自然语言转换为SQL语句并执行
        - 你只需要提供自然语言查询，工具会处理转换和执行
        - 不需要手动编写SQL语句
        - 对于复杂分析需求，请直接转交给客户细分智能体
        - 完成任务后，请以"分析完成"作为结束标志
        
        数据库表结构查询：
        - 如果你不确定数据库中是否存在特定表，请先使用get_database_schema()查询表结构
        - 不要基于猜测生成表名，必须使用实际存在的表名
        - 如果查询涉及风险、逾期等字段，先查询包含这些字段的实际表
        
        调用工具的格式：
        ```python
        # 查询表结构
        get_database_schema()  # 获取所有表结构概览
        get_database_schema(table_name="表名")  # 获取特定表结构
        
        # 执行查询
        execute_sql_query(question="你的自然语言查询问题")
        ```
        
        示例：
        ```python
        # 先查询表结构
        schema_info = get_database_schema()
        print(schema_info)
        
        # 然后执行查询
        execute_sql_query(question="查询所有客户的总数")
        execute_sql_query(question="按性别分组统计客户数量")
        execute_sql_query(question="查询存款余额大于10000的客户")
        
        # 复杂分析需求（应转交给客户细分智能体）
        # "分析存款多的客户群体和贷款多的客户群体的区别"
        # "对比不同年龄段客户的消费行为"
        # "分析高价值客户的特征"
        
        # 完成任务后添加结束标志
        "分析完成"
        ```"""
        
        segmentation_system_message = """你是一个专业的客户细分分析智能体，擅长处理复杂客户分析需求。
        你的主要职责是：
        1. 分析用户的客户细分和对比分析需求
        2. 提取目标客群和对照组信息
        3. 调用客户细分工具执行深度分析
        4. 生成详细的客户对比分析报告
        5. 提供有洞察力的分析结论和建议
        
        重要要求：
        - 分析的结果不要写当前的查询的缺点，而是写当前查询的优点
        - 内容最多写四节，1.查询数据分析，2.对照问题分析，3.对比分析，4.总结
        
        智能体分工：
        - 你负责处理客户群体对比、行为分析、细分建模等复杂分析任务
        - Text2SQL智能体负责处理数据查询、统计、简单分析等SQL相关任务
        - 如果收到简单查询需求，请转交给Text2SQL智能体
        
        典型分析场景：
        - 分析存款多的客户群体和贷款多的客户群体的区别
        - 对比不同年龄段客户的消费行为
        - 分析高价值客户的特征
        - 客户群体细分和画像分析
        
        重要说明：
        - 完成任务后，请以"分析完成"作为结束标志
        
        数据库表结构查询原则：
        - 必须首先使用get_database_schema()查询实际表结构
        - 不要基于猜测生成表名，必须使用实际存在的表名
        - 如果查询涉及风险、逾期等字段，先查询包含这些字段的实际表
        - 如果找不到特定表，请检查可用表结构并调整查询策略
        
        你可以使用execute_segmentation_analysis工具来执行客户细分分析。
        
        调用工具的格式：
        ```python
        execute_segmentation_analysis(query="你的分析需求")
        ```
        
        示例：
        ```python
        execute_segmentation_analysis(query="分析存款多的客户群体和贷款多的客户群体的区别")
        execute_segmentation_analysis(query="对比不同年龄段客户的消费行为")
        execute_segmentation_analysis(query="分析高价值客户的特征")
        
        # 完成任务后添加结束标志
        "分析完成"
        ```"""
        
        # 重新创建智能体实例
        llm_config = {
            "model": "qwen2.5-14b-instruct", 
            "temperature": 0.1, 
            "max_tokens": 5000, 
            "timeout": 120,
            "api_key": os.getenv("OPENAI_API_KEY"),
            "base_url": os.getenv("OPENAI_BASE_URL", "https://dashscope.aliyuncs.com/compatible-mode/v1")
        }
        
        # 定义函数配置
        function_config = {
            "execute_sql_query": {
                "name": "execute_sql_query",
                "description": "执行SQL查询并返回结果",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "question": {
                            "type": "string",
                            "description": "要执行的SQL查询问题"
                        }
                    },
                    "required": ["question"]
                }
            },
            "execute_segmentation_analysis": {
                "name": "execute_segmentation_analysis",
                "description": "执行客户细分分析并返回结果",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "query": {
                            "type": "string",
                            "description": "客户细分分析需求"
                        }
                    },
                    "required": ["query"]
                }
            },
            "generate_control_group_query": {
                "name": "generate_control_group_query",
                "description": "根据目标组查询问题生成对照组查询问题",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "target_query": {
                            "type": "string",
                            "description": "目标组查询问题"
                        }
                    },
                    "required": ["target_query"]
                }
            }
        }
        
        # 重新创建Text2SQL智能体
        self.text2sql_agent = AssistantAgent(
            name="text2sql_agent",
            system_message=text2sql_system_message,
            llm_config={
                **llm_config,
                "functions": [function_config["execute_sql_query"]]
            }
        )
        self.text2sql_agent.text2sql_processor = text2sql_processor
        
        # 重新创建客户细分智能体
        self.segmentation_agent = AssistantAgent(
            name="segmentation_agent",
            system_message=segmentation_system_message,
            llm_config={
                **llm_config,
                "functions": [function_config["execute_segmentation_analysis"]]
            }
        )
        self.segmentation_agent.segmentation_processor = segmentation_processor
    
    def _create_group_chat(self):
        """创建智能体群组聊天"""
        # 定义智能体列表
        agents = [
            self.user_proxy,
            self.decision_agent,
            self.text2sql_agent,
            self.segmentation_agent
        ]
        
        # 创建群组聊天（移除不支持的speaker_transitions参数）
        self.group_chat = GroupChat(
            agents=agents,
            messages=[],
            max_round=20,  # 增加对话轮次，避免复杂分析被提前终止
            speaker_selection_method="round_robin",  # 可以根据需要调整为其他选择方法
            allow_repeat_speaker=True
        )
        
        # 创建群组聊天管理器
        self.group_chat_manager = GroupChatManager(
            groupchat=self.group_chat,
            llm_config={
                "model": "qwen2.5-14b-instruct", 
                "timeout": 120,
                "api_key": os.getenv("OPENAI_API_KEY"),
                "base_url": os.getenv("OPENAI_BASE_URL", "https://dashscope.aliyuncs.com/compatible-mode/v1")
            }
        )
    
    def process_query(self, query: str) -> Dict[str, Any]:
        """处理用户查询，利用AutoGen的智能体对话协议实现自动协作
        
        Args:
            query: 用户查询文本
            
        Returns:
            处理结果，包含success、content等字段
        """
        if not query or not query.strip():
            return {"success": False, "error": "查询不能为空"}
        
        # 添加用户查询到历史
        user_message = {
            'role': 'user',
            'content': query.strip(),
            'timestamp': datetime.now().isoformat()
        }
        self.conversation_history.append(user_message)
        
        try:
            # 使用AutoGen的智能体对话协议处理查询
            response = self.user_proxy.initiate_chat(
                self.group_chat_manager,
                message=query,
                silent=True  # 静默模式，不打印中间过程
            )
            
            # 获取最终响应
            final_response = response.chat_history[-1]['content'] if response.chat_history else "未获取到响应"
            
            # 添加系统响应到历史
            self.conversation_history.append({
                'role': 'assistant',
                'content': final_response,
                'timestamp': datetime.now().isoformat()
            })
            
            # 记录中间对话历史
            for msg in response.chat_history:
                logger.debug(f"[{msg['name']}] {msg['content'][:100]}...")
            
            return {
                "success": True,
                "explanation": final_response,
                "response": final_response,
                "chat_history": response.chat_history
            }
            
        except Exception as e:
            logger.error(f"处理查询时发生错误: {e}", exc_info=True)
            return {"success": False, "error": f"处理查询失败: {str(e)}"}
    
    def _process_text2sql_query(self, query: str) -> Dict[str, Any]:
        """使用Text2SQL处理器处理查询（备用方法）"""
        try:
            if hasattr(self.text2sql_agent, 'text2sql_processor') and self.text2sql_agent.text2sql_processor:
                result = self.text2sql_agent.text2sql_processor.process_query(
                    question=query,
                    session_id=self.session_id,
                    entities=None,
                    conversation_history=self.conversation_history[:-1]  # 不包含当前查询
                )
                
                # 确保返回标准格式
                if 'explanation' not in result and 'content' in result:
                    result['explanation'] = result['content']
                
                return result
            else:
                return {"success": False, "error": "SQL查询处理器未初始化"}
                
        except Exception as e:
            logger.error(f"Text2SQL处理失败: {e}")
            return {
                "success": False,
                "error": f"SQL查询处理失败: {str(e)}"
            }
    
    def _process_segmentation_query(self, query: str) -> Dict[str, Any]:
        """使用客户细分处理器处理查询（备用方法）"""
        try:
            if hasattr(self.segmentation_agent, 'segmentation_processor') and self.segmentation_agent.segmentation_processor:
                result = self.segmentation_agent.segmentation_processor.process_query(query)
                
                # 确保返回标准格式
                if 'success' not in result:
                    result['success'] = True
                
                # 确保有explanation字段
                if 'explanation' not in result:
                    result['explanation'] = result.get('content', '客户细分分析完成')
                
                return result
            elif self.segmentation_processor:
                # 兼容原有方式
                result = self.segmentation_processor.process_query(query)
                
                # 确保返回标准格式
                if 'success' not in result:
                    result['success'] = True
                
                # 确保有explanation字段
                if 'explanation' not in result:
                    result['explanation'] = result.get('content', '客户细分分析完成')
                
                return result
            else:
                return {"success": False, "error": "客户细分处理器未初始化"}
                
        except Exception as e:
            logger.error(f"客户细分处理失败: {e}")
            return {
                "success": False,
                "error": f"客户细分分析失败: {str(e)}"
            }
    
    def _retry_with_fallback(self, query: str, max_retries: int = 3) -> Dict[str, Any]:
        """内置容错与重试机制
        
        Args:
            query: 用户查询文本
            max_retries: 最大重试次数
            
        Returns:
            处理结果
        """
        retries = 0
        last_error = None
        
        while retries < max_retries:
            try:
                result = self.process_query(query)
                if result.get('success'):
                    return result
                
                # 分析错误类型
                error_msg = result.get('error', '')
                last_error = error_msg
                
                # 根据错误类型决定是否重试
                if "SQL语法错误" in error_msg or "数据库连接" in error_msg:
                    logger.warning(f"尝试重试处理查询，错误: {error_msg}, 重试次数: {retries + 1}")
                    retries += 1
                    # 添加错误信息到查询中，帮助智能体修正
                    query = f"{query}\n上一次执行出现错误: {error_msg}\n请修正后重试"
                else:
                    # 不支持重试的错误，直接返回
                    break
                    
            except Exception as e:
                logger.error(f"重试过程中发生异常: {e}")
                last_error = str(e)
                retries += 1
        
        # 所有重试都失败，使用备用方法
        logger.warning(f"所有重试都失败，使用备用方法处理")
        query_type = self.query_classifier.classify_query(query)
        
        if query_type == "segmentation":
            return self._process_segmentation_query(query)
        else:
            return self._process_text2sql_query(query)
    
    def update_agent_permissions(self, agent_name: str, permissions: Dict[str, bool]):
        """轻量化定制 - 更新智能体权限
        
        Args:
            agent_name: 智能体名称
            permissions: 权限配置字典
        """
        agent_map = {
            "text2sql_agent": self.text2sql_agent,
            "segmentation_agent": self.segmentation_agent,
            "decision_agent": self.decision_agent
        }
        
        if agent_name in agent_map:
            agent = agent_map[agent_name]
            # 设置权限信息
            if not hasattr(agent, 'permissions'):
                agent.permissions = {}
            agent.permissions.update(permissions)
            
            # 根据权限更新系统提示
            if permissions.get('restricted_query', False):
                agent.system_message += "\n\n注意：你只能执行查询操作，不能修改数据库。"
            
            logger.info(f"更新智能体 {agent_name} 的权限: {permissions}")
        else:
            logger.warning(f"智能体 {agent_name} 不存在")
    
    def _generate_control_group_query(self, target_query: str) -> Dict[str, Any]:
        """生成对照组查询问题
        
        Args:
            target_query: 目标组查询问题
            
        Returns:
            包含对照组查询问题的字典
        """
        try:
            # 使用LLM生成对照组查询问题
            control_prompt = f"""
            请根据以下目标组查询问题，生成一个合理的对照组查询问题。
            对照组应该与目标组形成对比，通常具有以下特征：
            - 与目标组互补或相反
            - 具有可比性
            - 能够形成有效的对比分析
            
            目标组查询：{target_query}
            
            请只返回对照组查询问题，不要添加其他内容。
            """
            
            # 使用智能体协作生成对照组查询
            result = self.process_query(control_prompt)
            
            if result.get('success'):
                control_query = result.get('explanation', '').strip()
                logger.info(f"生成的对照组查询: {control_query}")
                
                return {
                    "success": True,
                    "control_query": control_query,
                    "target_query": target_query
                }
            else:
                logger.error(f"智能体协作生成对照组失败: {result.get('error', '未知错误')}")
                return {
                    "success": False,
                    "error": f"智能体协作生成对照组失败: {result.get('error', '未知错误')}"
                }
            
        except Exception as e:
            logger.error(f"生成对照组查询失败: {e}")
            return {
                "success": False,
                "error": f"生成对照组查询失败: {str(e)}"
            }
    
    def clear_conversation(self):
        """清空对话历史"""
        self.conversation_history.clear()
        self.group_chat.messages.clear()
        logger.info("对话历史已清空")


class MultiAgentSystemCLI:
    """多智能体系统命令行交互界面"""
    
    def __init__(self):
        """初始化CLI"""
        self.multi_agent_system = IntegratedMultiAgentSystem()
        self.display_width = 80
    
    def display_help(self):
        """显示帮助信息"""
        help_text = """
        多智能体系统命令行工具
        可用命令：
        - 查询 [文本]: 发送查询到多智能体系统
        - 权限 [智能体名称] [权限配置]: 设置智能体权限
        - 清空: 清空对话历史
        - 退出/quit: 退出系统
        - 帮助/help: 显示此帮助信息
        """
        print(help_text)
    
    def display_result(self, result: Dict[str, Any]):
        """格式化显示查询结果"""
        print("-" * self.display_width)
        
        if result.get('success'):
            print("查询成功!")
            print("\n结果解释:")
            print(result.get('explanation', ''))
            
            # 显示对话历史摘要
            if 'chat_history' in result:
                print("\n智能体协作摘要:")
                for i, msg in enumerate(result['chat_history'], 1):
                    if i <= 3 or i == len(result['chat_history']):  # 只显示前3条和最后一条
                        print(f"[{msg['name']}]: {msg['content'][:50]}..." if len(msg['content']) > 50 else f"[{msg['name']}]: {msg['content']}")
        else:
            print("查询失败!")
            print(f"错误信息: {result.get('error', '未知错误')}")
        
        print("-" * self.display_width)
    
    def process_input(self, user_input: str):
        """处理用户输入"""
        if not user_input or user_input.strip() == "":
            print("请输入命令或查询内容")
            return True
        
        command = user_input.strip().lower()
        
        # 处理退出命令
        if command in ["退出", "quit"]:
            print("感谢使用，再见!")
            return False
        
        # 处理帮助命令
        elif command in ["帮助", "help"]:
            self.display_help()
            return True
        
        # 处理清空命令
        elif command == "清空":
            self.multi_agent_system.clear_conversation()
            print("对话历史已清空")
            return True
        
        # 处理权限设置命令
        elif command.startswith("权限 "):
            try:
                parts = user_input.split(" ", 2)
                if len(parts) < 3:
                    print("权限命令格式错误，请使用: 权限 [智能体名称] [权限配置]")
                    return True
                
                agent_name = parts[1]
                permissions_str = parts[2]
                
                # 解析权限配置（简单格式：restricted_query=true,human_approval=false）
                permissions = {}
                for perm in permissions_str.split(","):
                    if "=" in perm:
                        key, value = perm.split("=")
                        permissions[key.strip()] = value.strip().lower() == "true"
                
                self.multi_agent_system.update_agent_permissions(agent_name, permissions)
                print(f"已更新智能体 {agent_name} 的权限")
                
            except Exception as e:
                print(f"设置权限时出错: {str(e)}")
                
            return True
        
        # 处理查询命令
        elif command.startswith("查询 "):
            query = user_input[2:].strip()
            if not query:
                print("查询内容不能为空")
                return True
            
            try:
                print(f"正在处理查询: {query}")
                # 使用容错重试机制处理查询
                result = self.multi_agent_system._retry_with_fallback(query)
                self.display_result(result)
            except Exception as e:
                print(f"处理查询时出错: {str(e)}")
                
            return True
        
        # 默认将输入作为查询
        else:
            try:
                print(f"正在处理查询: {command}")
                # 使用容错重试机制处理查询
                result = self.multi_agent_system._retry_with_fallback(command)
                self.display_result(result)
            except Exception as e:
                print(f"处理查询时出错: {str(e)}")
                
            return True
    
    def run(self):
        """运行交互式命令行"""
        print("欢迎使用AutoGen多智能体协作系统")
        print("输入'帮助'查看可用命令")
        print("-" * self.display_width)
        
        while True:
            try:
                user_input = input("\n请输入命令或查询内容: ")
                if not self.process_input(user_input):
                    break
            except KeyboardInterrupt:
                print("\n\n程序被用户中断")
                break
            except Exception as e:
                print(f"程序运行出错: {str(e)}")


def run_demo():
    """运行演示"""
    print("AutoGen多智能体协作系统演示")
    print("=" * 80)
    
    # 创建多智能体系统
    system = IntegratedMultiAgentSystem()
    
    # 演示查询案例
    test_queries = [
        "查询2024年各月份的存款总额",
        "对比高价值客户和普通客户的存款变化",
        "分析35岁以上客户的投资偏好",
        "统计各分行的客户数量"
    ]
    
    print("演示查询案例:")
    for i, query in enumerate(test_queries, 1):
        print(f"\n案例 {i}: {query}")
        print("-" * 60)
        
        try:
            # 使用容错重试机制处理查询
            result = system._retry_with_fallback(query)
            
            if result.get('success'):
                print(f"结果: {result.get('explanation', '')[:200]}..." if len(result.get('explanation', '')) > 200 else f"结果: {result.get('explanation', '')}")
            else:
                print(f"失败: {result.get('error', '未知错误')}")
        except Exception as e:
            print(f"处理失败: {str(e)}")
        
    print("\n" + "=" * 80)
    print("演示完成。请运行命令行界面进行交互: python test_autogen_multi_agent.py")


def main():
    """主函数"""
    # 检查参数
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "--demo":
        # 运行演示
        run_demo()
    else:
        # 运行交互式命令行
        cli = MultiAgentSystemCLI()
        cli.run()


if __name__ == "__main__":
    main()