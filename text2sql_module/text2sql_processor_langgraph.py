"""
Text2SQL模块 - 基于LangGraph实现的自然语言转SQL处理器
保持与原始Text2SQLProcessor相同的功能接口
"""
import os
import json
import logging
# 导入dotenv来加载.env文件
try:
    from dotenv import load_dotenv
    load_dotenv()
    # 尝试从text2sql_module目录加载.env文件
    module_dir = os.path.dirname(os.path.abspath(__file__))
    env_path = os.path.join(module_dir, '.env')
    if os.path.exists(env_path):
        load_dotenv(dotenv_path=env_path)
        logging.info(f"从 {env_path} 加载环境变量成功")
except ImportError:
    logging.warning("python-dotenv 未安装，无法自动加载.env文件")

from typing import Dict, List, Any, Optional
from langchain_openai import ChatOpenAI
from langchain_community.utilities import SQLDatabase
from langchain_classic.chains import create_sql_query_chain
from langchain_core.output_parsers import StrOutputParser
from langchain_core.messages import HumanMessage, AIMessage, SystemMessage
from langchain_core.prompts import ChatPromptTemplate

from prompt.prompts_manager import prompts_manager
from langgraph.graph import StateGraph, END
from dataclasses import dataclass, field
from typing import TypedDict, Union

# 配置日志 - 使用统一的日志模块
from logger_set import get_logger
logger = get_logger(__name__)

# 定义LangGraph状态
def default_state():
    return {
        "original_query": "",
        "enhanced_query": "",
        "generated_sql": "",
        "initial_sql": "",
        "refined_sql": "",
        "execution_result": {},
        "explanation": "",
        "session_id": "default",
        "entities": None,
        "target_sql": "",  # 保持向后兼容
        "target_sql_part": None,  # 与Text2SQLState保持一致
        "conversation_history": None,
        "validation_result": None,
        "refined_validation_result": None,
        "error": None,
        "success": False
    }

class Text2SQLState(TypedDict):
    original_query: str
    enhanced_query: str
    generated_sql: str
    initial_sql: str
    target_sql: str
    target_sql_part: str
    refined_sql: str
    execution_result: Dict[str, Any]
    explanation: str
    session_id: str
    entities: Optional[Dict[str, Any]]
    conversation_history: Optional[List[Dict]]
    validation_result: Optional[Dict[str, Any]]
    refined_validation_result: Optional[Dict[str, Any]]
    error: Optional[str]
    success: bool

class Text2SQLProcessorLangGraph:
    """基于LangGraph实现的Text2SQL处理器，保持与原始处理器相同的接口"""
    
    def __init__(self, db_uri: Optional[str] = None, model_name: str = "qwen-plus"):
        """
        初始化Text2SQL处理器
        
        Args:
            db_uri: 数据库连接URI
            model_name: 使用的模型名称
        """
        self.db_uri = db_uri or os.getenv('URI', 'mysql+pymysql://root:123456@localhost:3306/mysql2')
        self.model_name = model_name
        
        # 初始化模型配置
        self.openai_api_key = os.getenv('QWEN_API_KEY') or os.getenv('OPENAI_API_KEY')
        self.openai_api_base = os.getenv('OPENAI_BASE_URL', 'https://dashscope.aliyuncs.com/compatible-mode/v1')
        
        # 初始化组件
        self._init_database()
        self._init_model()
        self._init_prompts()
        self._init_workflow()
        
        logger.info(f"Text2SQL处理器初始化完成，模型: {model_name}")
    
    def _init_database(self):
        """初始化数据库连接"""
        try:
            self.db = SQLDatabase.from_uri(self.db_uri)
            logger.info("数据库连接初始化成功")
        except Exception as e:
            logger.error(f"数据库连接初始化失败: {e}")
            raise
    
    def _init_model(self):
        """初始化语言模型"""
        try:
            # 再次尝试获取环境变量，确保最新值 
            self.model = ChatOpenAI(
                model=self.model_name,
                openai_api_key= "sk-820e321dee794acea8332b3ce2e6abc5",
                openai_api_base=self.openai_api_base,
                temperature=0.1
            )
            logger.info("语言模型初始化成功")
            
        except Exception as e:
            logger.error(f"语言模型初始化失败: {e}")
            # 不抛出异常，允许模块继续运行
            self.model = None
    
    def _init_prompts(self):
        """初始化提示词 - 所有提示词均从prompts_manager统一获取"""
        try:
            # SQL生成提示词 - 来自prompts_manager.text2sql.sql_generation
            try:
                # 直接从prompts_manager获取SQL生成系统提示词
                sql_generation_prompt = prompts_manager.get_prompt('text2sql', 'sql_generation')
                if not sql_generation_prompt or not isinstance(sql_generation_prompt, str):
                    sql_generation_prompt = """你是一个MySQL SQL专家，请根据用户问题和数据库信息生成正确的SQL查询。
                    
                    重要要求：
                    1. 必须严格使用数据库表结构中显示的实际字段名，不要使用通用的字段名猜测
                    2. 仔细检查表结构中的字段名，确保字段名完全匹配
                    3. 如果字段名包含大写字母或特殊字符，必须原样使用
                    4. 不要使用customer_id、user_id等通用字段名，必须使用表结构中的实际字段名
                    5. MySQL特定规则：当需要使用IN子查询且子查询包含LIMIT子句时，必须使用派生表加JOIN的方式替代IN子查询
                    6. MySQL特定规则：所有派生表必须添加别名
                    7. MySQL特定规则：避免使用SELECT *，只查询必要的字段
                    8. MySQL特定规则：确保表名和别名之间有空格
                    """
                    logger.warning("使用增强的SQL生成提示词")
                
                # 创建包含自定义提示词的SQL查询链
                # 目前没有对大数据库信息进行优化，只有显示写入前10个表信息
                human_prompt = """
                数据库信息:
                {table_info}
                
                重要提示：请严格使用表结构中显示的实际字段名，不要使用通用字段名猜测！
                
                用户问题:
                {input}
                {target_sql_part}
                最多返回{top_k}条结果
                """
                
                self.prompt = ChatPromptTemplate.from_messages([
                    ("system", sql_generation_prompt),
                    ("human", human_prompt)
                ])
                
                # 只有当模型初始化成功时才创建查询链
                # 语言模型，数据库连接，提示词，最多返回结果数，通过这几个生成SQL查询
                if self.model:
                    self.query_chain = create_sql_query_chain(
                        self.model, 
                        self.db, 
                        prompt=self.prompt,
                        k=None  # 避免LIMIT限制
                    )
                    logger.debug("SQL查询链初始化成功")
                else:
                    self.query_chain = None
                    logger.warning("模型未初始化成功，无法创建SQL查询链")
            except Exception as e:
                logger.error(f"SQL生成提示词初始化失败: {e}")
                self.query_chain = None
            
            # 结果解释提示词 - 来自prompts_manager.text2sql.explanation
            try:
                # 直接使用prompts_manager中的explanation模板
                explanation_template = prompts_manager.get_prompt('text2sql', 'explanation')
                if not explanation_template:
                    # 如果获取失败，创建默认模板
                    logger.warning("解释提示词模板为空，使用默认模板")
                    explanation_template = ChatPromptTemplate.from_template(
                        "请基于以下信息提供解释:\n问题: {question}\nSQL: {sql_query}\n结果: {query_result}\n原始结果: {raw_result}"
                    )
                
                # 只有当模型初始化成功时才创建解释链
                if self.model:
                    self.explanation_chain = explanation_template | self.model | StrOutputParser()
                    logger.debug("解释链初始化成功")
                else:
                    self.explanation_chain = None
                    logger.warning("模型未初始化成功，无法创建解释链")
            except Exception as e:
                logger.error(f"解释提示词初始化失败: {e}")
                self.explanation_chain = None     
            # SQL优化提示词 - 来自prompts_manager.text2sql.huobiyouhua
            try:
                # 直接从prompts_manager获取货币优化提示词
                huobiyouhua_prompt = prompts_manager.get_prompt('text2sql', 'huobiyouhua')
            
                
                # 创建优化SQL的提示词模板
                self.react_prompt_template = ChatPromptTemplate.from_template(
                    huobiyouhua_prompt
                )
            except Exception as e:
                self.react_prompt_template = None
            
            logger.info("所有提示词初始化成功")
        except Exception as e:
            logger.error(f"提示词初始化失败: {e}")
            # 初始化关键变量为None，避免后续调用时出现属性错误
            self.query_chain = None
            self.explanation_chain = None
            self.react_prompt_template = None
            raise
    
    def _init_workflow(self):
        """初始化LangGraph工作流"""
        try:
            # 创建状态图
            self.graph = StateGraph(Text2SQLState)
            
            # 添加节点
            self.graph.add_node("enhance_query", self._enhance_query_node)
            self.graph.add_node("generate_sql", self._generate_sql_node)
            self.graph.add_node("validate_sql", self._validate_sql_node)
            self.graph.add_node("refine_sql", self._refine_sql_node)
            # 优化：使用同一个验证函数处理初始SQL和优化后SQL的验证
            
            self.graph.add_node("execute_sql", self._execute_sql_node)
            self.graph.add_node("generate_explanation", self._generate_explanation_node)
            
            # 设置入口点
            self.graph.set_entry_point("enhance_query")
            
            # 添加边
            self.graph.add_edge("enhance_query", "generate_sql")
            self.graph.add_edge("generate_sql", "validate_sql")
            self.graph.add_edge("validate_sql", "refine_sql")  # 先优化SQL
            self.graph.add_edge("refine_sql", "execute_sql")  # 然后执行优化后的SQL
            
            # 添加条件边：如果SQL执行失败但有修正的SQL，则重新验证和执行
            self.graph.add_conditional_edges(
                "execute_sql",
                self._should_retry_after_error,
                {
                    "retry": "validate_sql",  # 重新验证修正后的SQL
                    "continue": "generate_explanation",  # 正常继续
                    "fail": END  # 彻底失败
                }
            )
            
            self.graph.add_edge("generate_explanation", END)
            
            # 编译图
            self.app = self.graph.compile()
            
            logger.info("LangGraph工作流初始化成功")
        except Exception as e:
            logger.error(f"LangGraph工作流初始化失败: {e}")
            raise
    
    def _enhance_query_node(self, state: Text2SQLState) -> Dict[str, Any]:
        """增强查询节点，处理会话历史和实体信息"""
        try:
            question = state.get('original_query', '')
            conversation_history = state.get('conversation_history')
            entities = state.get('entities')
            enhanced_question = question
            
            # 1. 处理会话历史
            if conversation_history:
                # 大模型处理历史对话
                history_text = "\n".join([f"{item['role']}: {item['content']}" for item in conversation_history])
                prompt_text = prompts_manager.put_history_to_question()
                # 构建完整的提示信息
                full_prompt = f"{prompt_text}\n\n{history_text}\n\n当前问题: {question}\n请根据对话历史重写当前问题，确保包含所有必要的上下文信息。"
                try:
                    llm_response = self.model.invoke([HumanMessage(content=full_prompt)])
                    enhanced_question = llm_response.content
                    logger.info(f"大模型改写后的问题: {enhanced_question}")
                except Exception as e:
                    logger.error(f"模型调用失败: {e}")
                    # 检查是否为API相关错误
                    error_str = str(e).lower()
                    api_error = any(keyword in error_str for keyword in ["api", "key", "quota", "limit", "timeout", "authentication", "auth", "invalid key"])
                    if api_error:
                        return {'error': '模型调用失败，可能是API密钥配置错误或账户额度不足，请检查API设置或联系管理员', 'success': False}
                    else:
                        return {'error': f'模型调用失败: {str(e)}', 'success': False}
                
                # 只提取用户的历史记录，并取最近5条
                user_history = [item for item in conversation_history if item['role'] == 'user'][-5:]
                if user_history:
                    # 构建上下文提示词
                    history_text = "\n".join([f"历史问题 {i+1}: {item['content']}" for i, item in enumerate(user_history)])
                    enhanced_question = f"""
                        上下文信息：
                        {history_text}
                        
                        当前问题：{enhanced_question}
                    """
                    logger.info(f"将用户历史记录作为上下文提示词传递给大模型: {enhanced_question[:100]}...")
            else:
                logger.info(f"当前问题: {question}")
            
            # 2. 处理实体信息
            if entities and 'order_by' in entities and entities['order_by']:
                enhanced_question = f"{enhanced_question} 请按{entities['order_by']}排序"
            
            return {
                    'enhanced_query': enhanced_question,
                    'target_sql_part': state.get('target_sql')  # 传递给Text2SQLState中定义的target_sql_part字段
                    }
        except Exception as e:
            logger.error(f"查询增强失败: {e}")
            return {'error': f'查询增强失败: {str(e)}', 'success': False}
    
    def _generate_sql_node(self, state: Text2SQLState) -> Dict[str, Any]:
        """生成SQL节点"""
        try:
            enhanced_question = state.get('enhanced_query', '')
            target_sql = state.get('target_sql_part', None)  # 从target_sql_part字段获取
            empty_result = state.get('empty_result', False)  # 检查是否因为结果为空而重试
            retry_count = state.get('retry_count', 0)
            logger.info(f"_generate_sql_node对照组获取目标SQL: {target_sql}")
            
            # 获取数据库表信息，确保LLM使用正确的字段名
            table_info_json_str = self.get_table_info()
            #print(f"&&&&&&&&&&生成sql语句结构信息: {table_info_json_str}&&&&&&&&&&&&&")没有问题
            
            # 准备提示词参数，包含表结构信息
            prompt_params = {
                "question": enhanced_question,
                "top_k": None,
                "target_sql_part": "请在原始SQL的结构上根据问题直接修改，格式生成新的SQL语句。要求两个SQL语句是严格的对照组，除了关键问题有区别外所有结构必须保持完全一致。原始SQL:\n" + target_sql if target_sql else "",
                "table_info": f"\n\n数据库表结构信息：\n{table_info_json_str}" if table_info_json_str else ""
            }
            
            # 如果是因为查询结果为空而重试，添加额外提示要求LLM放松查询条件
            if empty_result:
                logger.info(f"检测到空结果重试，添加放松查询条件的提示（第{retry_count}次重试）")
                
                # 如果有目标SQL，在原始目标SQL的基础上放松条件
                if target_sql:
                    prompt_params['target_sql_part'] += "\n\n【重要提示】上一次查询结果为空，请在保持查询意图的前提下，放松查询条件（例如：放宽时间范围、减少过滤条件、增加模糊匹配等）以获取更多结果。"
                else:
                    # 如果没有目标SQL，直接在问题中添加提示
                    prompt_params['question'] = f"{enhanced_question}\n\n【重要提示】上一次查询结果为空，请在保持查询意图的前提下，放松查询条件（例如：放宽时间范围、减少过滤条件、增加模糊匹配等）以获取更多结果。"
            
            # 生成初始SQL查询 - 添加重试机制
            max_retries = 3
            retry_count = 0
            sql_query = None
            
            while retry_count < max_retries:
                try:
                    sql_query = self.query_chain.invoke(prompt_params)
                    logger.debug("生成初始SQL查询成功")
                    break
                except Exception as invoke_error:
                    retry_count += 1
                    logger.error(f"模型调用失败 (尝试 {retry_count}/{max_retries}): {invoke_error}")
                    
                    # 检查是否为API相关错误
                    error_str = str(invoke_error).lower()
                    api_error = any(keyword in error_str for keyword in ["api", "key", "quota", "limit", "timeout", "authentication", "auth", "invalid key"])
                    if api_error:
                        logger.warning("检测到API相关错误，可能是API密钥配置或账户额度问题")
                        return {'error': '模型调用失败，可能是API密钥配置错误或账户额度不足，请检查API设置或联系管理员', 'success': False}
                    
                    # 不是API错误，等待后重试
                    if retry_count < max_retries:
                        import time
                        time.sleep(1)  # 等待1秒后重试
            
            # 处理模型调用失败的情况
            if sql_query is None:
                logger.error("模型调用多次失败，无法生成SQL")
                return {'error': '模型调用失败，无法生成SQL', 'success': False}
            
            cleaned_sql = self._clean_sql_query(sql_query)
            
            logger.info(f"生成的SQL: {cleaned_sql}")
            
            return {
                'generated_sql': cleaned_sql,
                'initial_sql': cleaned_sql
            }
        except Exception as e:
            logger.error(f"SQL生成失败: {e}")
            return {'error': f'SQL生成失败: {str(e)}', 'success': False}
    
    def _validate_sql_node(self, state: Text2SQLState) -> Dict[str, Any]:
        """通用SQL验证节点 - 可同时处理初始SQL和优化后SQL的验证"""
        try:
            # 智能判断要验证的SQL：优先使用refined_sql，其次使用generated_sql
            sql = state.get('refined_sql', state.get('generated_sql', ''))
            
            # 判断是验证初始SQL还是优化后SQL（用于日志和错误消息）
            is_refined_sql = 'refined_sql' in state and state['refined_sql'] == sql
            sql_type = "优化后的SQL" if is_refined_sql else "生成的SQL"
            
            # 先进行基础验证
            basic_validation = self.validate_sql(sql)
            if not basic_validation['valid']:
                result_key = 'refined_validation_result' if is_refined_sql else 'validation_result'
                return {
                    result_key: basic_validation,
                    'validation_result': basic_validation,
                    'error': f'{sql_type}不符合安全规范: {basic_validation["error"]}',
                    'success': False
                }
            
            # 获取SQL验证提示词 - 来自prompts_manager.common.sql_validation
            try:
                validation_prompt = prompts_manager.get_prompt('common', 'sql_validation')
                logger.debug("成功获取SQL验证提示词")
            except Exception as e:
                logger.warning(f"无法从prompts_manager获取SQL验证提示词: {e}，使用默认提示词")
                # 如果获取不到提示词，则使用默认提示词
                validation_prompt = """
                请严格按照以下要求验证SQL查询：
                1. 必须只返回JSON格式的结果，不要添加任何额外说明文字
                2. JSON格式为: {"valid": true/false, "error": "错误信息或null"}
                3. 验证规则：
                   - 确保SQL是只读查询（SELECT、SHOW、DESCRIBE）
                   - 确保不包含危险操作（如删除、修改表结构等）
                   - 检查SQL注入风险
                   - 验证SQL语法是否正确
                """
            table_info_json_str = self.get_table_info()
            
            # 构建验证请求
            validation_input = f"{validation_prompt}\n数据库表结构信息：\n{table_info_json_str}\n\n待验证SQL: {sql}\n\n请严格按照上述要求，仅返回JSON格式结果，不要添加任何其他文字："
            
            response = self.model.invoke([HumanMessage(content=validation_input)])
            
            # 解析验证结果
            try:
                validation_result = json.loads(response.content)
                if 'valid' not in validation_result:
                    logger.info("LLM验证结果格式不完整，使用基础验证结果")
                    validation_result = basic_validation
            except json.JSONDecodeError as e:
                logger.info(f"LLM验证结果非JSON格式，使用基础验证结果（错误：{str(e)}）")
                validation_result = basic_validation
            
            if not validation_result['valid']:
                result_key = 'refined_validation_result' if is_refined_sql else 'validation_result'
                return {
                    result_key: validation_result,
                    'validation_result': validation_result,
                    'error': f'{sql_type}不符合安全规范: {validation_result["error"]}',
                    'success': False
                }
            
            # 根据SQL类型返回不同的结果键
            result_key = 'refined_validation_result' if is_refined_sql else 'validation_result'
            return {result_key: validation_result}
        except Exception as e:
            logger.error(f"SQL验证失败: {e}")
            return {'error': f'SQL验证失败: {str(e)}', 'success': False}
    
    def _refine_sql_node(self, state: Text2SQLState) -> Dict[str, Any]:
        """优化SQL节点"""
        try:
            question = state.get('original_query', '')
            initial_sql = state.get('initial_sql', '')
            
            # 获取数据库表信息（JSON字符串）
            table_info_json_str = self.get_table_info()
            logger.debug(f"用于SQL优化的表结构信息: {table_info_json_str[:200]}...")
            
            # 确保react_prompt_template已初始化
            if not self.react_prompt_template:
                logger.warning("SQL优化提示词模板未初始化，尝试重新初始化")
                # 重新尝试初始化提示词模板
                try:
                    huobiyouhua_prompt = prompts_manager.get_prompt('text2sql', 'huobiyouhua')
                    self.react_prompt_template = ChatPromptTemplate.from_template(huobiyouhua_prompt)
                    logger.info("SQL优化提示词模板重新初始化成功")
                except Exception as init_error:
                    logger.error(f"重新初始化提示词模板失败: {init_error}")
                    # 如果无法初始化提示词模板，直接返回原始SQL
                    return {'refined_sql': initial_sql}
            
            # 使用SQL优化提示词模板生成提示内容
            try:
                messages = self.react_prompt_template.invoke({
                    "question": question,
                    "table_info": table_info_json_str,  # 直接传递JSON字符串，让提示词处理格式
                    "initial_sql": initial_sql
                })
            except Exception as template_error:
                logger.error(f"提示词模板调用失败: {template_error}")
                # 创建基本的优化提示
                messages = [
                    HumanMessage(content=f"""你是一个专业的SQL优化专家，请对以下生成的SQL进行优化。

                用户问题: {question}
                数据库表结构: {table_info_json_str}
                初步生成的SQL: {initial_sql}

                请严格使用数据库表结构中实际存在的字段名，确保SQL语法正确，并与用户问题匹配。请直接输出优化后的SQL语句。""")
                                ]
                            
            # 调用模型生成优化的SQL - 添加重试机制
            max_retries = 3
            retry_count = 0
            llm_response = None
            
            while retry_count < max_retries:
                try:
                    llm_response = self.model.invoke(messages)
                    logger.debug("使用优化提示词生成SQL成功")
                    break
                except Exception as invoke_error:
                    retry_count += 1
                    logger.error(f"模型调用失败 (尝试 {retry_count}/{max_retries}): {invoke_error}")
                    
                    # 检查是否为API相关错误
                    if "API" in str(invoke_error) or "key" in str(invoke_error).lower() or "quota" in str(invoke_error).lower():
                        logger.warning("检测到API相关错误，可能是API密钥配置或账户额度问题")
                        # 不再重试，直接处理
                        break
                    
                    # 不是API错误，等待后重试
                    if retry_count < max_retries:
                        import time
                        time.sleep(1)  # 等待1秒后重试
            
            # 处理模型调用失败的情况
            if llm_response is None:
                logger.error("模型调用多次失败，无法生成优化SQL")
                # 使用原始SQL作为后备
                fallback_sql = state.get('initial_sql', state.get('generated_sql', ''))
                if fallback_sql:
                    logger.info(f"使用后备SQL: {fallback_sql[:100]}...")
                return {'refined_sql': fallback_sql, 'error': '模型调用失败，无法优化SQL', 'success': False}
            
            refined_sql = llm_response.content
            
            # 清洗生成的SQL
            cleaned_refined_sql = self._clean_sql_query(refined_sql)
            
            # 验证优化后的SQL是否包含虚构字段
            try:
                # 简单的验证逻辑：检查SQL中是否包含表结构中不存在的字段
                # 这里可以根据需要扩展更复杂的验证逻辑
                basic_validation = self.validate_sql(cleaned_refined_sql)
                if basic_validation.get('valid', False):
                    logger.info(f"SQL优化成功，生成的SQL已通过基础验证")
                else:
                    logger.warning(f"优化后的SQL验证失败: {basic_validation.get('error', '未知错误')}")
            except Exception as validate_error:
                logger.warning(f"验证优化后SQL时出错: {validate_error}")
            
            return {'refined_sql': cleaned_refined_sql}
        except Exception as e:
            logger.error(f"SQL优化失败: {e}")
            # 使用原始SQL作为后备，优先initial_sql，然后是generated_sql
            fallback_sql = state.get('initial_sql', state.get('generated_sql', ''))
            if fallback_sql:
                logger.info(f"使用后备SQL: {fallback_sql[:100]}...")
            else:
                logger.error("无法获取任何SQL作为后备，返回空字符串")
            return {'refined_sql': fallback_sql}
            
          
        
    
    def _validate_refined_sql_node(self, state: Text2SQLState) -> Dict[str, Any]:
        """验证优化后的SQL节点 - 复用_validate_sql_node的验证逻辑"""
        try:
            # 创建临时状态，将refined_sql放入generated_sql字段
            # 这样可以直接复用_validate_sql_node的完整验证逻辑
            temp_state = state.copy()
            temp_state['generated_sql'] = state.get('refined_sql', '')
            
            # 调用_validate_sql_node进行验证
            result = self._validate_sql_node(temp_state)
            
            # 调整返回字段名，将validation_result改为refined_validation_result
            if 'validation_result' in result:
                # 复制原始结果
                adjusted_result = result.copy()
                # 重命名字段
                adjusted_result['refined_validation_result'] = adjusted_result.pop('validation_result')
                return adjusted_result
            
            return result
        except Exception as e:
            logger.error(f"优化后SQL验证失败: {e}")
            return {'error': f'优化后SQL验证失败: {str(e)}', 'success': False}
    
    def _execute_sql_node(self, state: Text2SQLState) -> Dict[str, Any]:
        """执行SQL节点，包含智能重试机制"""
        try:
            # 优先使用refined_sql，如果不存在则尝试使用generated_sql或initial_sql
            sql_query = state.get('refined_sql', state.get('generated_sql', state.get('initial_sql', '')))
            print(f"**************_execute_sql_node执行SQL: {sql_query}")
            original_query = state.get('original_query', '')
            
            # 记录使用的SQL来源
            if 'refined_sql' in state:
                logger.debug("使用优化后的SQL")
            elif 'generated_sql' in state:
                logger.debug("使用生成的SQL")
            elif 'initial_sql' in state:
                logger.debug("使用初始SQL")
            
            # 确保SQL不为空
            if not sql_query or not sql_query.strip():
                logger.error("尝试执行空SQL查询，所有可能的SQL来源字段都为空")
                return {
                    'error': 'SQL查询为空',
                    'success': False
                }
            
            logger.info(f"准备执行SQL: {sql_query}...")
            
            # 导入我们自己的DatabaseManager
            from utils.database import db_manager
            
            # 执行SQL查询
            result = db_manager.execute_query(sql_query)
            
            # 处理查询结果
            data = result if result else []
            
            # 保存原始结果
            raw_result = result
            
            # 计算实际行数
            row_count = len(data) if isinstance(data, list) else 1
            
           
            execution_result = {
                'success': True,
                'data': data,
                'row_count': row_count,
                'raw_result': raw_result
            }
            
            # 检查查询结果是否为空，且不是最后一次重试
            retry_count = state.get('retry_count', 0)
            if row_count == 0 and retry_count < 2:  # 最多重试2次空结果
                logger.info(f"查询结果为空，尝试调整查询条件（第{retry_count + 1}次重试）")
                # 设置空结果标识，以便在_should_retry_after_error中处理
                state['empty_result'] = True
                # 更新重试计数
                state['retry_count'] = retry_count + 1
                return {
                    'execution_result': execution_result,
                    'row_count': row_count,
                    'error': '查询结果为空，尝试调整查询条件',
                    'success': False
                }
            
            return {
                'execution_result': execution_result,
                'row_count': row_count
            }
        except Exception as e:
            logger.error(f"SQL执行失败: {e}")
            
            # 智能重试机制：分析错误并尝试修正SQL
            return self._retry_with_error_analysis(state, str(e))
    
    def _retry_with_error_analysis(self, state: Text2SQLState, error_message: str) -> Dict[str, Any]:
        """基于错误分析的智能重试机制"""
        try:
            # 与_execute_sql_node保持一致的SQL获取逻辑
            sql_query = state.get('refined_sql', state.get('generated_sql', state.get('initial_sql', '')))
            original_query = state.get('original_query', '')
            print(f"**************_retry_with_error_analysis执行SQL: {sql_query}")  
            
            logger.info(f"开始智能重试，分析错误: {error_message}")
            
            # 获取数据库表信息用于错误分析（现在是JSON字符串）
            table_info_json_str = self.get_table_info()
            
            # 解析JSON字符串获取表名列表
            try:
                import json
                table_info_dict = json.loads(table_info_json_str)
                
                # 优先从tables数组中提取表名
                if 'tables' in table_info_dict and isinstance(table_info_dict['tables'], list):
                    tables = table_info_dict['tables']
                    logger.info(f"从tables数组获取到表列表: {tables}")
                # 如果tables数组不存在或为空，从table_info对象中提取表名
                elif 'table_info' in table_info_dict and isinstance(table_info_dict['table_info'], dict):
                    tables = list(table_info_dict['table_info'].keys())
                    logger.info(f"从table_info对象获取到表列表: {tables}")
                else:
                    # 如果上述两种方式都失败，使用所有键作为后备
                    tables = list(table_info_dict.keys())
                    logger.warning("JSON结构中未找到tables数组或table_info对象，使用所有键作为后备")
                    
                table_info_str = f"可用表名: {', '.join(tables)}"
            except Exception as e:
                logger.warning(f"解析表信息JSON失败: {e}")
                table_info_str = ""  # 如果解析失败，使用空字符串
            
            # 构建更智能的错误分析提示词
            error_analysis_prompt = f"""
                你是一个SQL专家，需要分析SQL执行错误并智能修正SQL语句。

                原始问题：{original_query}

                失败的SQL语句：{sql_query}

                执行错误信息：{error_message}

                {table_info_str}

                请仔细分析错误信息，识别具体的错误类型，并基于以下策略修正SQL语句：

                常见错误类型及修正策略：
                1. 字段不存在错误（如Unknown column）：
                - 检查字段名拼写是否正确
                - 检查表别名是否正确（如ci.OPEN_DT中的ci是否对应正确的表）
                - 根据语义选择最合适的字段名
                - 如果字段不存在但语义相近，选择最接近的字段

                2. 表不存在错误：
                - 检查表名拼写是否正确
                - 检查表是否在可用表列表中
                - 根据查询语义选择最合适的表

                3. 语法错误：
                - 检查SQL语法是否符合标准
                - 修正括号、引号等语法问题
                - 确保关键字使用正确

                4. 权限错误：
                - 确保查询是只读操作（SELECT、SHOW、DESCRIBE）
                - 避免使用危险操作

                修正原则：
                - 保持查询的原始语义不变
                - 只修正错误部分，不要改变查询逻辑
                - 确保修正后的SQL能够正确执行
                - 优先选择语义最接近的修正方案

                请直接返回修正后的SQL语句，不要添加任何解释：
                """
            
            # 调用大模型分析错误并修正SQL
            response = self.model.invoke([HumanMessage(content=error_analysis_prompt)])
            corrected_sql = response.content.strip()
            
            # 清理修正后的SQL
            corrected_sql = self._clean_sql_query(corrected_sql)
            
            logger.info(f"大模型修正后的SQL: {corrected_sql}")
            
            # 验证修正后的SQL
            validation_result = self.validate_sql(corrected_sql)
            if not validation_result['valid']:
                logger.error(f"修正后的SQL验证失败: {validation_result['error']}")
                return {
                    'error': f'SQL执行失败: {error_message}，且修正后的SQL验证失败: {validation_result["error"]}',
                    'success': False
                }
            
            # 尝试执行修正后的SQL
            try:
                result = self.db.run(corrected_sql)
                
                # 处理查询结果
                if isinstance(result, list):
                    data = result
                elif hasattr(result, '__iter__') and not isinstance(result, (str, dict)):
                    data = list(result)
                else:
                    data = [result] if result is not None else []
                
                # 保存原始结果
                raw_result = result
                
                # 计算实际行数
                row_count = len(data) if isinstance(data, list) else 1
                
                logger.info(f"重试成功，修正后的SQL执行成功，返回{row_count}行结果")
                
                execution_result = {
                    'success': True,
                    'data': data,
                    'row_count': row_count,
                    'raw_result': raw_result,
                    'original_error': error_message,
                    'corrected_sql': corrected_sql
                }
                
                return {
                    'execution_result': execution_result,
                    'row_count': row_count,
                    'refined_sql': corrected_sql,  # 更新状态中的SQL
                    'success': True
                }
                
            except Exception as retry_error:
                logger.error(f"重试执行仍然失败: {retry_error}")
                return {
                    'error': f'SQL执行失败: {error_message}，且重试修正后仍然失败: {str(retry_error)}',
                    'success': False
                }
                
        except Exception as analysis_error:
            logger.error(f"错误分析过程失败: {analysis_error}")
            return {
                'error': f'SQL执行失败: {error_message}，且错误分析过程失败: {str(analysis_error)}',
                'success': False
            }
    
    def _should_retry_after_error(self, state: Text2SQLState) -> str:
        """判断是否应该重试SQL执行"""
        # 检查是否有错误信息
        error = state.get('error')
        execution_result = state.get('execution_result', {})
        empty_result = state.get('empty_result', False)
        
        # 如果执行成功，继续后续流程
        if execution_result.get('success', False):
            return "continue"
        
        # 如果查询结果为空且在重试次数限制内，尝试调整查询条件
        if empty_result:
            # 检查是否已经重试过，避免无限循环
            retry_count = state.get('retry_count', 0)
            if retry_count >= 2:  # 空结果最多重试2次
                logger.info("空结果已达到最大重试次数，流程继续")
                return "continue"  # 空结果重试次数用完后，继续到解释生成阶段
            
            logger.info(f"查询结果为空，将尝试调整查询条件（第{retry_count}次重试）")
            return "retry"
        
        # 如果有错误，但重试机制已经修正了SQL，则重试
        if error and 'corrected_sql' in execution_result:
            # 检查是否已经重试过，避免无限循环
            retry_count = state.get('retry_count', 0)
            if retry_count >= 4:  # 最多重试4次
                logger.info("已达到最大重试次数，流程终止")
                return "fail"
            
            logger.info(f"检测到修正后的SQL，将重新验证和执行（第{retry_count + 1}次重试）")
            # 更新重试计数
            state['retry_count'] = retry_count + 1
            return "retry"
        
        # 如果有错误但没有修正的SQL，或者重试次数已用完，则彻底失败
        if error:
            logger.info("SQL执行失败且无法重试，流程终止")
            return "fail"
        
        # 如果没有错误信息，继续后续流程
        return "continue"

    def _generate_explanation_node(self, state: Text2SQLState) -> Dict[str, Any]:
        """生成解释节点"""
        try:
            question = state.get('original_query', '')
            sql_query = state.get('refined_sql', '')
            execution_result = state.get('execution_result', {})
            
            if not execution_result.get('success', False):
                return {
                    'explanation': f"查询执行失败：{execution_result.get('error', '未知错误')}",
                    'success': False
                }
            
            # 格式化查询结果
            result_text = self._format_result_for_explanation(execution_result)
            
            # 将原始结果也传递给大模型
            raw_result_str = str(execution_result.get('raw_result', ''))
            
            # 生成解释
            explanation = self.explanation_chain.invoke({
                'question': question,
                'sql_query': sql_query,
                'query_result': result_text,
                'raw_result': raw_result_str
            })
            
            return {
                'explanation': explanation.strip(),
                'success': True
            }
        except Exception as e:
            logger.error(f"解释生成失败: {e}")
            return {
                'explanation': f"已执行SQL查询，但生成解释时出错：{str(e)}",
                'success': False
            }
    
    def _clean_sql_query(self, sql_query: str) -> str:
        """清洗SQL查询"""
        if not isinstance(sql_query, str):
            return str(sql_query)
        
        cleaned = sql_query.strip()
        
        # 首先尝试从JSON格式中提取SQL
        if cleaned.startswith('{') and '"sql":' in cleaned:
            try:
                json_data = json.loads(cleaned)
                if 'sql' in json_data and isinstance(json_data['sql'], str):
                    cleaned = json_data['sql'].strip()
            except json.JSONDecodeError:
                pass
        
        # 提取代码块中的SQL内容
        if "```sql" in cleaned:
            start = cleaned.find("```sql") + 6
            end = cleaned.find("```", start)
            if end != -1:
                cleaned = cleaned[start:end].strip()
        elif "```" in cleaned:
            start = cleaned.find("```") + 3
            end = cleaned.find("```", start)
            if end != -1:
                cleaned = cleaned[start:end].strip()
        
        # 移除常见前缀和后缀
        prefixes = ["SQLQuery:", "SQL:", "Query:", "sql:", "SQLResult", "Result:", "Answer:"]
        for prefix in prefixes:
            if cleaned.startswith(prefix):
                cleaned = cleaned[len(prefix):].strip()
            if cleaned.endswith(prefix):
                cleaned = cleaned[:-len(prefix)].strip()
        
        # 提取SQL关键字开始的内容
        sql_keywords = ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'WITH', 'SHOW', 'DESCRIBE']
        for keyword in sql_keywords:
            pos = cleaned.upper().find(keyword)
            if pos != -1:
                cleaned = cleaned[pos:].strip()
                break
        
        return cleaned
    
    def _format_result_for_explanation(self, execution_result: Dict[str, Any]) -> str:
        """格式化查询结果用于解释生成"""
        try:
            row_count = execution_result.get('row_count', 0)
            raw_result_str = str(execution_result.get('raw_result', ''))
            data = execution_result.get('data', [])
            
            if row_count == 0:
                return f"查询结果为空。原始结果: {raw_result_str}"
            else:
                formatted_data = "\n".join([str(row) for row in data])
                return f"【重要提示】请显示所有符合条件的记录，不要遗漏任何数据！\n查询返回{row_count}行数据，请完整列出每一行的信息：\n{formatted_data}\n\n原始结果: {raw_result_str}"
        except Exception as e:
            logger.error(f"结果格式化失败: {e}")
            return f"查询结果格式化失败，请直接分析原始结果，并确保显示所有符合条件的记录：{str(execution_result.get('raw_result', ''))}"
    
    def get_table_info(self) -> str:
        """获取数据库表信息，返回优化格式的JSON字符串"""
        try:
            # 使用我们自己的DatabaseManager获取更详细的表信息
            from utils.database import db_manager
            
            # 获取所有表名
            tables = db_manager.get_all_tables()
            
            # 创建更结构化的表信息字典
            table_info_json = {
                'success': True,
                'table_count': len(tables),
                'tables': tables,
                'table_info': {}
            }
            
            for table in tables[:15]:  # 限制获取前15个表的信息
                try:
                    # 获取表的详细结构信息
                    schema = db_manager.get_table_schema(table)
                    
                    # 构建更详细的字段信息
                    fields_info = []
                    field_dict = {}
                    
                    for field in schema:
                        field_info = {
                            'name': field['column_name'],
                            'type': field['data_type'],
                            'nullable': field['is_nullable'],
                            'key': field['column_key'],
                            'comment': field['column_comment'] if field['column_comment'] else '无注释'
                        }
                        fields_info.append(field_info)
                        field_dict[field['column_name']] = {
                            'type': field['data_type'],
                            'comment': field['column_comment'] if field['column_comment'] else '无注释'
                        }
                    
                    # 保持原始表名作为键
                    table_key = table
                    
                    # 添加详细表信息
                    table_info_json['table_info'][table_key] = {
                        'fields': field_dict,
                        'fields_detailed': fields_info,
                        'field_count': len(fields_info)
                    }
                    
                    #logger.info(f"表 {table} 的详细结构信息已提取，包含 {len(fields_info)} 个字段")
                    
                except Exception as e:
                    logger.warning(f"获取表{table}信息失败: {e}")
                    table_key = table
                    table_info_json['table_info'][table_key] = {
                        'error': str(e),
                        'fields': {},
                        'fields_detailed': [],
                        'field_count': 0
                    }
            
            # 添加表间关系信息（如果有）
            try:
                relationships = db_manager.get_table_relationships()
                if relationships:
                    table_info_json['relationships'] = relationships
                    logger.info(f"成功获取 {len(relationships)} 个表间关系")
            except Exception as e:
                logger.warning(f"获取表关系信息失败: {e}")
            
            # 转换为JSON字符串格式
            import json
            json_str = json.dumps(table_info_json, ensure_ascii=False, indent=2)
            
            # 记录信息但限制日志长度
            logger.info(f"成功获取 {len(tables)} 个表的优化格式JSON信息")
            return json_str
            
        except Exception as e:
            logger.error(f"获取表信息失败: {e}")
            import json
            error_json = {
                'success': False,
                'error': str(e),
                'table_count': 0,
                'tables': [],
                'table_info': {}
            }
            return json.dumps(error_json, ensure_ascii=False, indent=2)
    
    def validate_sql(self, sql_query: str) -> Dict[str, Any]:
        """验证SQL查询语法（基础验证）"""
        try:
            sql_upper = sql_query.upper().strip()
            
            if not sql_upper.startswith(('SELECT', 'SHOW', 'DESCRIBE')):
                return {
                    'valid': False,
                    'error': '只支持SELECT、SHOW、DESCRIBE查询'
                }
            
            dangerous_keywords = ['DROP', 'DELETE', 'UPDATE', 'INSERT', 'ALTER', 'CREATE']
            for keyword in dangerous_keywords:
                if keyword in sql_upper:
                    return {
                        'valid': False,
                        'error': f'不支持包含{keyword}的查询'
                    }
            
            return {
                'valid': True,
                'message': 'SQL语法检查通过'
            }
        except Exception as e:
            return {
                'valid': False,
                'error': f'SQL验证失败: {str(e)}'
            }
    
    def _validate_sql(self, sql: str) -> Dict[str, Any]:
        """使用LLM进行SQL验证"""
        try:
            # 先进行基础验证
            basic_validation = self.validate_sql(sql)
            if not basic_validation['valid']:
                return basic_validation
            
            # 获取SQL验证提示词
            try:
                validation_prompt = prompts_manager.get_prompt('sql_validation')
            except:
                validation_prompt = """
                请严格按照以下要求验证SQL查询：
                1. 必须只返回JSON格式的结果，不要添加任何额外说明文字
                2. JSON格式为: {"valid": true/false, "error": "错误信息或null"}
                3. 验证规则：
                   - 确保SQL是只读查询（SELECT、SHOW、DESCRIBE）
                   - 确保不包含危险操作（如删除、修改表结构等）
                   - 检查SQL注入风险
                   - 验证SQL语法是否正确
                """
            
            # 构建验证请求
            validation_input = f"{validation_prompt}\n\n待验证SQL: {sql}\n\n请严格按照上述要求，仅返回JSON格式结果，不要添加任何其他文字："
            
            response = self.model.invoke([HumanMessage(content=validation_input)])
            
            # 解析验证结果
            try:
                validation_result = json.loads(response.content)
                if 'valid' not in validation_result:
                    logger.info("LLM验证结果格式不完整，使用基础验证结果")
                    return basic_validation
                return validation_result
            except json.JSONDecodeError as e:
                logger.info(f"LLM验证结果非JSON格式，使用基础验证结果（错误：{str(e)}）")
                return basic_validation
        except Exception as e:
            logger.error(f"SQL验证过程出错: {e}")
            return self.validate_sql(sql)
    
    def process_query(self, question: str,target_sql: str = None, session_id: str = 'default', entities: Dict[str, Any] = None, conversation_history: List[Dict] = None) -> Dict[str, Any]:
        """
        处理自然语言查询（保持与原始接口相同）
        
        Args:
            question: 用户的问题
            session_id: 会话ID
            entities: 解析出的实体信息，包含排序等结构化数据
            conversation_history: 完整的会话历史记录
            
        Returns:
            处理结果字典
        """
        try:
            logger.info(f"处理查询 [{session_id}]: {question}")
            logger.info(f"检查历史记录: {conversation_history}")
            
            # 初始化状态
            logger.info(f"process_query有没有接受到这个target_sql: {target_sql}")
            initial_state = default_state()
            initial_state.update({
                'original_query': question,
                'session_id': session_id,
                'entities': entities,
                'target_sql': target_sql,  # 保持向后兼容
                'target_sql_part': target_sql,  # 同时设置到Text2SQLState中定义的字段
                'conversation_history': conversation_history
            })
            
            # 执行工作流
            result = self.app.invoke(initial_state)
            
            # 构建与原始处理器相同格式的响应
            if result.get('success', False):
                # 与_execute_sql_node保持一致的SQL获取逻辑，优先使用refined_sql，其次generated_sql，最后initial_sql
                sql_query = result.get('refined_sql', result.get('generated_sql', result.get('initial_sql', '')))
                response = {
                    'success': True,
                    'question': question,
                    'initial_sql': result.get('initial_sql', ''),
                    'sql_query': sql_query,
                    'execution_result': result.get('execution_result', {}),
                    'explanation': result.get('explanation', ''),
                    'row_count': result.get('row_count', 0),
                    'session_id': session_id,
                    'metadata': {
                        'model': self.model_name,
                        'db_uri': self.db_uri.split('@')[-1] if '@' in self.db_uri else 'unknown'
                    }
                }
                logger.info(f"查询处理成功，返回{response['row_count']}行结果")
                return response
            else:
                response = {
                    'success': False,
                    'error': result.get('error', '未知错误'),
                    'session_id': session_id
                }
                # 添加更多调试信息
                if result.get('initial_sql'):
                    response['initial_sql'] = result['initial_sql']
                # 使用与成功响应相同的SQL获取逻辑
                sql_query = result.get('refined_sql', result.get('generated_sql', result.get('initial_sql', '')))
                if sql_query:
                    response['sql_query'] = sql_query
                logger.error(f"查询处理失败: {response['error']}")
                return response
                
        except Exception as e:
            logger.error(f"查询处理失败: {e}")
            return {
                'success': False,
                'error': f'查询处理失败: {str(e)}',
                'session_id': session_id
            }

# 不再在模块导入时创建全局实例，避免配置问题
# text2sql_processor = Text2SQLProcessorLangGraph()
