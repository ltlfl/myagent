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
                    sql_generation_prompt = "你是一个SQL专家，请根据用户问题和数据库信息生成正确的SQL查询。"
                    logger.warning("使用默认SQL生成提示词")
                
                # 创建包含自定义提示词的SQL查询链
                human_prompt = """
                数据库信息:
                {table_info}
                
                用户问题:
                {input}
                
                最多返回{top_k}条结果
                """
                
                self.prompt = ChatPromptTemplate.from_messages([
                    ("system", sql_generation_prompt),
                    ("human", human_prompt)
                ])
                
                # 只有当模型初始化成功时才创建查询链
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
                    
                    explanation_template = ChatPromptTemplate.from_template(
                        "请基于以下信息提供解释:\n问题: {question}\nSQL: {sql_query}\n结果: {query_result}\n原始结果: {raw_result}"
                    )
                    logger.warning("使用默认解释提示词模板")
                
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
                print("--------------------------")
                print(huobiyouhua_prompt)
                if not huobiyouhua_prompt or not isinstance(huobiyouhua_prompt, str):
                    huobiyouhua_prompt = "请优化以下SQL查询。"
                    logger.warning("使用默认SQL优化提示词")
                
                # 创建优化SQL的提示词模板
                self.react_prompt_template = ChatPromptTemplate.from_template(
                    huobiyouhua_prompt
                )
                logger.debug("SQL优化提示词模板初始化成功")
            except Exception as e:
                logger.error(f"SQL优化提示词初始化失败: {e}")
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
            self.graph.add_node("validate_refined_sql", self._validate_sql_node)
            self.graph.add_node("execute_sql", self._execute_sql_node)
            self.graph.add_node("generate_explanation", self._generate_explanation_node)
            
            # 设置入口点
            self.graph.set_entry_point("enhance_query")
            
            # 添加边
            self.graph.add_edge("enhance_query", "generate_sql")
            self.graph.add_edge("generate_sql", "validate_sql")
            self.graph.add_edge("validate_sql", "refine_sql")
            self.graph.add_edge("refine_sql", "validate_refined_sql")
            self.graph.add_edge("validate_refined_sql", "execute_sql")
            self.graph.add_edge("execute_sql", "generate_explanation")
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
                llm_response = self.model.invoke([HumanMessage(content=full_prompt)])
                enhanced_question = llm_response.content
                print(f"大模型改写后的问题: {enhanced_question}")
                
                # 只提取用户的历史记录，并取最近3条
                user_history = [item for item in conversation_history if item['role'] == 'user'][-3:]
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
                print(f"当前问题: {question}")
            
            # 2. 处理实体信息
            if entities and 'order_by' in entities and entities['order_by']:
                enhanced_question = f"{enhanced_question} 请按{entities['order_by']}排序"
            
            return {'enhanced_query': enhanced_question}
        except Exception as e:
            logger.error(f"查询增强失败: {e}")
            return {'error': f'查询增强失败: {str(e)}', 'success': False}
    
    def _generate_sql_node(self, state: Text2SQLState) -> Dict[str, Any]:
        """生成SQL节点"""
        try:
            enhanced_question = state.get('enhanced_query', '')
            
            # 生成初始SQL查询
            sql_query = self.query_chain.invoke({"question": enhanced_question, "top_k": None})
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
            
            # 构建验证请求
            validation_input = f"{validation_prompt}\n\n待验证SQL: {sql}\n\n请严格按照上述要求，仅返回JSON格式结果，不要添加任何其他文字："
            
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
            
            # 获取数据库表信息
            table_info_result = self.get_table_info()
            if not table_info_result['success']:
                logger.warning(f"获取表信息失败，使用原始SQL: {table_info_result['error']}")
                return {'refined_sql': initial_sql}
            
            # 格式化表信息
            table_info_str = ""
            for table, info in table_info_result['detailed_info'].items():
                if 'schema' in info:
                    table_info_str += f"\n\n表 {table}:\n{info['schema']}"
            
            # 使用SQL优化提示词模板生成提示内容
            # 提示词来源: prompts_manager.text2sql.huobiyouhua
            # 对于ChatPromptTemplate，使用invoke生成消息列表
            messages = self.react_prompt_template.invoke({
                "question": question,
                "table_info": table_info_str,
                "initial_sql": initial_sql
            })
            
            # 调用模型生成优化的SQL
            llm_response = self.model.invoke(messages)
            logger.debug("使用化提示词生成优化SQL")
            refined_sql = llm_response.content
            
            # 清洗生成的SQL
            cleaned_refined_sql = self._clean_sql_query(refined_sql)
            
            logger.info(f"优化后的SQL: {cleaned_refined_sql}")
            
            return {'refined_sql': cleaned_refined_sql}
        except Exception as e:
            logger.error(f"SQL优化失败: {e}")
            # 使用原始SQL作为后备
            return {'refined_sql': state.get('initial_sql', '')}
    
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
        """执行SQL节点"""
        try:
            sql_query = state.get('refined_sql', '')
            
            # 执行SQL查询
            result = self.db.run(sql_query)
            
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
            
            logger.info(f"SQL执行成功，返回{row_count}行结果")
            
            execution_result = {
                'success': True,
                'data': data,
                'row_count': row_count,
                'raw_result': raw_result
            }
            
            return {
                'execution_result': execution_result,
                'row_count': row_count
            }
        except Exception as e:
            logger.error(f"SQL执行失败: {e}")
            return {
                'error': f'SQL执行失败: {str(e)}',
                'success': False
            }
    
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
    
    def get_table_info(self) -> Dict[str, Any]:
        """获取数据库表信息"""
        try:
            tables = self.db.get_usable_table_names()
            table_info = {}
            
            for table in tables[:10]:  # 限制获取前10个表的信息
                try:
                    schema = self.db.get_table_info([table])
                    table_info[table] = {
                        'schema': schema,
                        'columns_count': schema.count('CREATE TABLE')
                    }
                except Exception as e:
                    logger.warning(f"获取表{table}信息失败: {e}")
                    table_info[table] = {'error': str(e)}
            
            # 返回符合_refine_sql_node期望格式的结果
            return {
                'success': True,
                'table_count': len(tables),
                'tables': tables,
                'detailed_info': table_info
            }
        except Exception as e:
            logger.error(f"获取表信息失败: {e}")
            return {
                'success': False,
                'error': str(e),
                'table_count': 0,
                'tables': [],
                'detailed_info': {}
            }
    
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
    
    def process_query(self, question: str, session_id: str = 'default', entities: Dict[str, Any] = None, conversation_history: List[Dict] = None) -> Dict[str, Any]:
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
            
            # 初始化状态
            initial_state = default_state()
            initial_state.update({
                'original_query': question,
                'session_id': session_id,
                'entities': entities,
                'conversation_history': conversation_history
            })
            
            # 执行工作流
            result = self.app.invoke(initial_state)
            
            # 构建与原始处理器相同格式的响应
            if result.get('success', False):
                response = {
                    'success': True,
                    'question': question,
                    'initial_sql': result.get('initial_sql', ''),
                    'sql_query': result.get('refined_sql', ''),
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
                if result.get('refined_sql'):
                    response['sql_query'] = result['refined_sql']
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
