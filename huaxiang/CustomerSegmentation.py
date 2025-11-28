"""设计一个问题分析智能体，用于客户圈选和数据准备。你返回的数据精准而全面，确保客户分析的科学性。
"""
import os
import json
import logging
from typing import Dict, List, Any, Optional, Union, TypedDict
from langchain_openai import ChatOpenAI
from langchain_community.utilities import SQLDatabase
from langchain_core.messages import HumanMessage, AIMessage, SystemMessage
from langchain_core.prompts import ChatPromptTemplate
from langgraph.graph import StateGraph, END
from dataclasses import dataclass, field

# 导入Text2SQLProcessorLangGraph
from text2sql_module.text2sql_processor_langgraph import Text2SQLProcessorLangGraph

# 导入dotenv来加载.env文件
try:
    from dotenv import load_dotenv
    load_dotenv()
    # 尝试从当前目录加载.env文件
    module_dir = os.path.dirname(os.path.abspath(__file__))
    env_path = os.path.join(module_dir, '.env')
    if os.path.exists(env_path):
        load_dotenv(dotenv_path=env_path)
        logging.info(f"从 {env_path} 加载环境变量成功")
except ImportError:
    logging.warning("python-dotenv 未安装，无法自动加载.env文件")

# 尝试导入prompts_manager
try:
    from prompt.prompts_manager import prompts_manager
except ImportError:
    # 如果从prompt目录导入失败，尝试直接导入
    try:
        import prompts_manager
    except ImportError:
        logging.warning("无法导入prompts_manager模块，将使用默认提示词")
        # 创建一个简单的默认prompts_manager
        class DefaultPromptsManager:
            def get_prompt(self, category, name):
                return None
        prompts_manager = DefaultPromptsManager()

# 配置日志
try:
    # 尝试导入统一的日志模块
    from logger_set import get_logger
    logger = get_logger(__name__)
except ImportError:
    # 如果无法导入自定义日志模块，使用标准日志配置
    logging.basicConfig(level=logging.INFO, 
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)

def default_state():
    return {
        "original_query": "",
        "Primitive_sql": "",
        "corresponding_sql": "",
        "execution_result": {},
        "explanation": "",
        "session_id": "default",
        "conversation_history": None,
        "error": None,
        "success": False
    }

class CustomerSegmentationState(TypedDict):
    original_query: str
    enhanced_query: str
    target_query_question: str
    control_query_question: str
    Primitive_sql: str
    corresponding_sql: str
    target_sql: str
    control_sql: str
    target_sql_execution_result: Dict[str, Any]
    control_sql_execution_result: Dict[str, Any]
    execution_result: Dict[str, Any]
    explanation: str
    session_id: str
    conversation_history: Optional[List[Dict]]
    error: Optional[str]
    success: bool
class CustomerSegmentationLangGraph:
    """基于LangGraph实现的客户圈选和数据准备智能体"""
    
    def __init__(self, db_uri: Optional[str] = None, model_name: str = "qwen-plus"):
        """
        初始化客户圈选和数据准备智能体
        
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
        
        # 初始化Text2SQL处理器
        self._init_text2sql_processor()
        
        self._init_workflow()
        
        logger.info(f"客户圈选智能体初始化完成，模型: {model_name}")
    
    def _init_database(self):
        """初始化数据库连接"""
        try:
            self.db = SQLDatabase.from_uri(self.db_uri)
            logger.info("数据库连接初始化成功")
        except Exception as e:
            logger.error(f"数据库连接初始化失败: {e}")
            # 不抛出异常，允许模块继续运行
            self.db = None
    
    def _init_model(self):
        """初始化语言模型"""
        try:
            # 使用环境变量中的API密钥，而不是硬编码
            self.model = ChatOpenAI(
                model=self.model_name,
                openai_api_key= self.openai_api_key,
                openai_api_base=self.openai_api_base,
                temperature=0.1
            )
            logger.info("语言模型初始化成功")
            
        except Exception as e:
            logger.error(f"语言模型初始化失败: {e}")
            # 不抛出异常，允许模块继续运行
            self.model = None
    
    def _init_text2sql_processor(self):
        """初始化Text2SQL处理器"""
        try:
            # 使用相同的数据库连接和模型名称初始化Text2SQL处理器
            self.text2sql_processor = Text2SQLProcessorLangGraph(
                db_uri=self.db_uri,
                model_name=self.model_name
            )
            logger.info("Text2SQL处理器初始化成功")
        except ImportError as e:
            logger.error(f"导入Text2SQLProcessorLangGraph失败: {e}")
            logger.warning("Text2SQLProcessorLangGraph模块不可用，将使用备用SQL生成方法")
            self.text2sql_processor = None
        except Exception as e:
            logger.error(f"Text2SQL处理器初始化失败: {e}")
            logger.warning("Text2SQL处理器初始化失败，将使用备用SQL生成方法")
            self.text2sql_processor = None
    
    def _init_prompts(self):
        """初始化提示词 - 所有提示词均从prompts_manager统一获取"""
        try:
            # 只需要初始化非SQL生成相关的提示词
            # SQL生成逻辑现在将使用Text2SQLProcessorLangGraph
            try:
                # 直接从prompts_manager获取货币优化提示词（保留用于其他可能的优化需求）
                huobiyouhua_prompt = prompts_manager.get_prompt('text2sql', 'huobiyouhua')
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
            self.react_prompt_template = None
            # 不再需要query_chain和explanation_chain，因为将使用text2sql_processor
    
    def _init_workflow(self):
        """初始化LangGraph工作流"""
        try:
            # 创建状态图
            self.graph = StateGraph(CustomerSegmentationState)
            # 添加节点 - 移除target_query_question和control_query_question节点
            self.graph.add_node("enhance_query", self._enhance_query_node)
            self.graph.add_node("target_query_sql", self._target_query_sql_node)
            self.graph.add_node("control_query_sql", self._control_query_sql_node)
            self.graph.add_node("generate_explanation", self._generate_explanation_node)
            # 设置入口点
            self.graph.set_entry_point("enhance_query")
            # 添加边 - 直接从enhance_query连接到target_query_sql
            self.graph.add_edge("enhance_query", "target_query_sql")
            self.graph.add_edge("target_query_sql", "control_query_sql")
            self.graph.add_edge("control_query_sql", "generate_explanation")
            self.graph.add_edge("generate_explanation", END)
            # 编译图
            self.app = self.graph.compile()
            
            logger.info("LangGraph工作流初始化成功")
        except Exception as e:
            logger.error(f"LangGraph工作流初始化失败: {e}")
            raise
    def _enhance_query_node(self, state: CustomerSegmentationState) -> Dict[str, Any]:
        """增强查询节点，使用问题分析提示词并直接生成目标客群和对照组查询问题"""
        try:
            question = state.get('original_query', '')
            conversation_history = state.get('conversation_history', [])
            
            # 构建上下文信息
            history_text = ""
            if conversation_history:
                # 只提取用户的历史记录，并取最近3条
                user_history = [item for item in conversation_history if item['role'] == 'user'][-3:]
                if user_history:
                    history_text = "\n".join([f"历史问题 {i+1}: {item['content']}" for i, item in enumerate(user_history)])
                    logger.info(f"将用户历史记录作为上下文: {history_text[:100]}...")
            
            # 添加必要的客户圈选上下文信息
            if "客户" not in question and "客群" not in question:
                question = f"针对银行客户，{question}"
                logger.info(f"添加了客户上下文: {question[:100]}...")
            
            # 使用问题分析提示词
            if self.model:
                try:
                    # 获取问题分析提示词
                    problem_analysis_prompt = prompts_manager.get_prompt('text2sql', 'Problem_Analysis_prompt') or \
                        "你是一个数据分析专家，请分析以下问题并直接返回JSON格式的结果，包含target_query_question和control_query_question两个字段。target_query_question是用于调用text2sql的目标客群查询问题，control_query_question是用于调用text2sql的对照组查询问题。"
                    
                    # 构建完整的提示信息
                    if hasattr(problem_analysis_prompt, 'format'):
                        # 如果是PromptTemplate类型，使用format方法
                        formatted_prompt = problem_analysis_prompt.format(question=question)
                    else:
                        # 如果是字符串类型，直接使用
                        formatted_prompt = str(problem_analysis_prompt)
                    
                    full_prompt = f"{formatted_prompt}\n\n"
                    if history_text:
                        full_prompt += f"对话历史:\n{history_text}\n\n"
                    full_prompt += "请直接返回JSON格式的结果，不要包含任何其他文字。"
                    
                    # 调用模型生成结果
                    llm_response = self.model.invoke([HumanMessage(content=full_prompt)])
                    response_content = llm_response.content.strip()
                    #logger.info(f"模型返回的分析结果: {response_content[:100]}...")
                    
                    # 解析JSON结果
                    try:
                        # 尝试提取JSON部分
                        json_start = response_content.find('{')
                        json_end = response_content.rfind('}')
                        if json_start != -1 and json_end != -1:
                            json_result = json.loads(response_content[json_start:json_end+1])
                            
                            # 验证必要字段是否存在
                            if 'target_query_question' not in json_result:
                                logger.error("JSON结果中缺少target_query_question字段")
                                return {'error': '问题分析失败：缺少目标客群查询问题'}
                            
                            # 如果没有对照组查询问题，设置为空字符串
                            if 'control_query_question' not in json_result:
                                json_result['control_query_question'] = ''
                                logger.warning("JSON结果中缺少control_query_question字段，设置为空")
                            
                            logger.info(f"成功生成目标客群查询问题: {json_result['target_query_question'][:100]}...")
                            if json_result['control_query_question']:
                                logger.info(f"成功生成对照组查询问题: {json_result['control_query_question'][:100]}...")
                            else:
                                logger.info("未生成对照组查询问题")
                            
                            # 返回包含两个查询问题的结果
                            return {
                                'enhanced_query': question,
                                'target_query_question': json_result['target_query_question'],
                                'control_query_question': json_result['control_query_question']
                            }
                        else:
                            logger.error("无法从模型响应中提取有效的JSON")
                            # 返回备用结果
                            return {
                                'enhanced_query': question,
                                'target_query_question': question,
                                'control_query_question': ''
                            }
                    except json.JSONDecodeError as json_error:
                        logger.error(f"JSON解析失败: {json_error}")
                        # 返回备用结果
                        return {
                            'enhanced_query': question,
                            'target_query_question': question,
                            'control_query_question': ''
                        }
                except Exception as llm_error:
                    logger.error(f"使用模型进行问题分析失败: {llm_error}")
                    # 返回备用结果
                    return {
                        'enhanced_query': question,
                        'target_query_question': question,
                        'control_query_question': ''
                    }
            else:
                logger.warning("模型未初始化，使用原始问题")
                # 返回备用结果
                return {
                    'enhanced_query': question,
                    'target_query_question': question,
                    'control_query_question': ''
                }
        except Exception as e:
            logger.error(f"查询增强和问题分析失败: {e}")
            return {
                'error': f'查询增强和问题分析失败: {str(e)}',
                'success': False,
                'target_query_question': question,
                'control_query_question': ''
            }

    

    
    def _target_query_sql_node(self, state: CustomerSegmentationState) -> Dict[str, Any]:
        """生成目标客群SQL查询 - 使用Text2SQLProcessorLangGraph"""
        try:
            target_query_question = state.get('target_query_question', '')
            session_id = state.get('session_id', 'default')
            conversation_history = state.get('conversation_history', None)
            
            # 参数验证
            if not target_query_question:
                logger.error("目标客群查询问题为空")
                return {'target_sql': '', 'error': '目标客群查询问题为空', 'target_sql_execution_result': {'sql_query': '', 'formatted_result': '[]', 'raw_result': '[]'}}
            
            # 如果Text2SQL处理器初始化成功，使用它生成SQL
            if self.text2sql_processor:
                try:
                    # 直接使用目标查询问题，不添加字段信息
                    result = self.text2sql_processor.process_query(
                        question=target_query_question,
                        session_id=session_id,
                        conversation_history=conversation_history
                    )
                    
                    # 详细日志记录Text2SQLProcessor的返回结果，这里有返回结果
                 #   logger.info(f"Text2SQLProcessor返回结果: {result}")
                       
                    if result.get('success', False):
                        # 直接从Text2SQLProcessor返回的结果中获取sql_query字段
                        target_sql = result.get('sql_query', '')
                        logger.info(f"通过Text2SQLProcessor生成目标客群SQL: {target_sql[:100]}...")
                        
                        # 提取执行结果信息并格式化存储
                        execution_result = result.get('execution_result', {})
                        # Text2SQLProcessor返回的execution_result中没有formatted_result字段，直接使用data字段
                        # 如果data字段不存在，则使用raw_result
                        data = execution_result.get('data', [])
                        if data:
                            formatted_result = str(data)
                        else:
                            formatted_result = str(execution_result.get('raw_result', []))
                        
                        target_sql_execution_result = {
                            'sql_query': target_sql,
                            'formatted_result': formatted_result,
                            'raw_result': execution_result.get('raw_result', [])
                        }


                        print(f"________________________________")
                       # print(f"Text2SQLProcessor生成的目标客群SQL执行结果: {target_sql_execution_result}")
                        return {
                            'target_sql': target_sql,
                            'target_sql_execution_result': target_sql_execution_result
                        }
                    else:
                        error_message = result.get('error', '未知错误')
                        logger.error(f"Text2SQLProcessor处理目标客群查询失败: {error_message}")
                        # 如果失败，尝试使用备用方法
                        fallback_result = self._fallback_sql_generation(target_query_question)
                        # 确保备用方法结果中包含执行结果信息
                        if 'target_sql_execution_result' not in fallback_result:
                            fallback_result['target_sql_execution_result'] = {
                                'sql_query': fallback_result.get('target_sql', ''),
                                'formatted_result': '[]',
                                'raw_result': '[]'
                            }
                        return fallback_result
                except Exception as e:
                    logger.error(f"调用Text2SQLProcessor时发生异常: {e}")
                    # 如果调用时发生异常，尝试使用备用方法
                    fallback_result = self._fallback_sql_generation(target_query_question)
                    # 确保备用方法结果中包含执行结果信息
                    if 'target_sql_execution_result' not in fallback_result:
                        fallback_result['target_sql_execution_result'] = {
                            'sql_query': fallback_result.get('target_sql', ''),
                            'formatted_result': '[]',
                            'raw_result': '[]'
                        }
                    print(f"备用方法生成的目标客群SQL: {fallback_result['target_sql']}")
                    return fallback_result
            else:
                logger.warning("Text2SQL处理器未初始化，使用备用SQL生成方法")
                # 使用备用方法生成SQL
                fallback_result = self._fallback_sql_generation(target_query_question)
                # 确保备用方法结果中包含执行结果信息
                if 'target_sql_execution_result' not in fallback_result:
                    fallback_result['target_sql_execution_result'] = {
                        'sql_query': fallback_result.get('target_sql', ''),
                        'formatted_result': '[]',
                        'raw_result': '[]'
                    }
                print(f"备用方法生成的目标客群SQL: {fallback_result['target_sql']}")
                return fallback_result
        except Exception as e:
            logger.error(f"生成目标客群SQL失败: {e}")
            # 返回错误信息并确保包含执行结果信息
            return {'target_sql': '', 
                    'error': f'生成目标客群SQL失败: {str(e)}',
                    'target_sql_execution_result': {'sql_query': '', 'formatted_result': '[]', 'raw_result': '[]'}}
    
    def _control_query_sql_node(self, state: CustomerSegmentationState) -> Dict[str, Any]:
        """生成对照组SQL查询 - 使用Text2SQLProcessorLangGraph"""
        try:
            control_query_question = state.get('control_query_question', '')
            target_query_question = state.get('target_query_question', '')
            target_sql = state.get('target_sql', '')
            #这一步成功获取target_sql
            print(f"_control_query_sql_node对照组获取目标SQL: {target_sql}")
            session_id = state.get('session_id', 'default')
            conversation_history = state.get('conversation_history', None)
            
            # 如果没有对照组查询问题或查询问题不完整，根据目标客群问题生成对照组问题
            if not control_query_question or len(control_query_question.strip()) < 10:
                logger.info(f"对照组查询问题为空或不完整，基于目标客群问题生成对照组问题: {target_query_question[:50]}...")
                
                # 为了生成正确的对照组SQL，创建一个明确的对照组查询问题
                # 动态生成对照组查询问题，避免硬编码
                if target_query_question:
                    # 使用LLM动态生成对照组查询问题
                    if self.model:
                        try:
                            prompt = f"""
                            请为以下目标客群查询生成对应的对照组查询问题。对照组应该是与目标客群在关键逻辑上相反的群体。
                            如果客户没有指定具体的条件，例如收入、年龄等，不要添加具体的数值，保持问题的通用性。
                            
                            目标查询: {target_query_question}
                            
                            请直接返回对照组查询问题，不要包含任何其他文字。
                            """
                            llm_response = self.model.invoke([HumanMessage(content=prompt)])
                            control_query_question = llm_response.content.strip()
                            logger.info(f"通过LLM动态生成对照组查询问题: {control_query_question[:100]}...")
                        except Exception as e:
                            logger.error(f"生成对照组查询问题失败: {e}")
                            # 如果LLM生成失败，使用简单的通用方法
                            control_query_question = f"分析非{target_query_question}"
                            # 如果替换后太长或不合理，使用更简单的方法
                            if len(control_query_question) > 100:
                                control_query_question = f"分析对照组客群"
                    else:
                        # 通用对照组生成方法
                        control_query_question = f"分析非{target_query_question}"
                        # 如果替换后太长或不合理，使用更简单的方法
                        if len(control_query_question) > 100:
                            control_query_question = f"分析对照组客群"
                
                logger.info(f"生成的对照组查询问题: {control_query_question}")
            
            print(f"_control_query_sql_node下对照组问题：{control_query_question}")
            
            # 如果Text2SQL处理器初始化成功，使用它生成SQL
            if self.text2sql_processor:
                try:
                    # 直接使用对照组查询问题，并传递目标SQL
                    result = self.text2sql_processor.process_query(
                        question=control_query_question,
                        target_sql=target_sql,  # 传递目标SQL以便生成正确的对照组SQL
                        session_id=session_id,
                        conversation_history=conversation_history
                    )

                    if result.get('success', False):
                        # 直接从Text2SQLProcessor返回的结果中获取sql_query字段
                        control_sql = result.get('sql_query', '')
                        logger.info(f"通过Text2SQLProcessor生成对照组SQL: {control_sql[:100]}...")
                        
                        # 对生成的对照组SQL进行检查和修正，确保它是目标客群SQL的正确互补
                        control_sql = self._check_and_correct_control_sql(target_sql, control_sql)
                        
                        # 提取执行结果信息并格式化存储
                        execution_result = result.get('execution_result', {})
                        # Text2SQLProcessor返回的execution_result中没有formatted_result字段，直接使用data字段
                        # 如果data字段不存在，则使用raw_result
                        data = execution_result.get('data', [])
                        if data:
                            formatted_result = str(data)
                        else:
                            formatted_result = str(execution_result.get('raw_result', []))
                        
                        control_sql_execution_result = {
                            'sql_query': control_sql,
                            'formatted_result': formatted_result,
                            'raw_result': execution_result.get('raw_result', [])
                        }
                        print(f"_control_query_sql_node下Text2SQLProcessor返回结果(对照组): {control_sql_execution_result.get('sql_query', '')}")  
                        return {
                            'control_sql': control_sql,
                            'control_sql_execution_result': control_sql_execution_result
                        }
                    else:
                        error_message = result.get('error', '未知错误')
                        logger.error(f"Text2SQLProcessor处理对照组查询失败: {error_message}")
                        # 如果失败，尝试使用备用方法并映射结果
                        control_sql = self._generate_control_sql_from_target(target_sql)
                        return {
                            'control_sql': control_sql,
                            'control_sql_execution_result': {
                                'sql_query': control_sql,
                                'formatted_result': '[]',
                                'raw_result': '[]'
                            }
                        }
                except Exception as e:
                    logger.error(f"调用Text2SQLProcessor时发生异常: {e}")
                    # 如果调用时发生异常，直接基于目标SQL生成对照组SQL
                    control_sql = self._generate_control_sql_from_target(target_sql)
                    return {
                        'control_sql': control_sql,
                        'control_sql_execution_result': {
                            'sql_query': control_sql,
                            'formatted_result': '[]',
                            'raw_result': '[]'
                        }
                    }
            else:
                logger.warning("Text2SQL处理器未初始化，直接基于目标SQL生成对照组SQL")
                # 直接基于目标SQL生成对照组SQL
                control_sql = self._generate_control_sql_from_target(target_sql)
                return {
                    'control_sql': control_sql,
                    'control_sql_execution_result': {
                        'sql_query': control_sql,
                        'formatted_result': '[]',
                        'raw_result': '[]'
                    }
                }
        except Exception as e:
            logger.error(f"生成对照组SQL失败: {e}")
            # 返回错误信息并确保包含执行结果信息
            return {'control_sql': '', 
                    'error': f'生成对照组SQL失败: {str(e)}',
                    'control_sql_execution_result': {'sql_query': '', 'formatted_result': '[]', 'raw_result': '[]'}}
    
    def _fallback_sql_generation(self, query_question: str) -> Dict[str, Any]:
        """备用SQL生成方法，当Text2SQLProcessor失败时使用"""
        try:
            # 验证查询问题
            if not query_question:
                logger.error("查询问题为空，无法生成SQL")
                return {'error': '查询问题为空，无法生成SQL'}
            
            # 获取SQL生成提示词
            sql_generation_prompt = prompts_manager.get_prompt('text2sql', 'target_query') or \
                "你是一个SQL专家，请根据用户的问题生成有效的SQL查询语句。请注意，你只能生成SQL语句本身，不要包含任何其他文本或解释。"
            
            # 构建完整提示词
            full_prompt = f"{sql_generation_prompt}\n\n问题：{query_question}"
            
            # 检查模型是否可用
            if not self.model:
                logger.error("模型未初始化，无法生成SQL")
                return {'error': '模型未初始化，无法生成SQL'}
            
            # 调用模型生成SQL
            response = self.model.invoke([HumanMessage(content=full_prompt)])
            sql_result = response.content
            
            # 提取SQL语句（尝试找到以SELECT开头的部分）
            sql_lines = sql_result.split('\n')
            sql_lines = [line.strip() for line in sql_lines if line.strip()]
            
            # 查找可能的SQL语句
            sql_query = ''
            for i, line in enumerate(sql_lines):
                if line.upper().startswith('SELECT'):
                    sql_query = line
                    # 继续添加后续行，直到找到以分号结尾的行
                    j = i + 1
                    while j < len(sql_lines):
                        sql_query += ' ' + sql_lines[j]
                        if sql_lines[j].strip().endswith(';'):
                            break
                        j += 1
                    break
            
            # 如果没有找到以SELECT开头的语句，使用整个响应
            if not sql_query and sql_lines:
                sql_query = sql_lines[0]
            
            # 确保SQL语句以分号结尾
            if sql_query and not sql_query.strip().endswith(';'):
                sql_query += ';'
            
            # 验证生成的SQL是否有效
            if not sql_query.strip():
                logger.error("生成的SQL为空")
                return {'error': '生成的SQL为空'}
            
            # 基本验证SQL语法
            if not sql_query.upper().startswith('SELECT'):
                logger.warning(f"生成的SQL可能无效（不以SELECT开头）: {sql_query}")
                # 尝试添加简单的SELECT语句作为后备
                simple_sql = f"SELECT * FROM customer WHERE 1=1;"
                logger.info(f"使用简单SQL作为后备: {simple_sql}")
                sql_query = simple_sql
            
            logger.info(f"通过备用方法生成SQL: {sql_query[:100]}...")
            
            # 默认返回target_sql字段，调用者会根据需要映射到相应字段
            return {'target_sql': sql_query}
        except Exception as e:
            logger.error(f"备用SQL生成失败: {e}")
            # 提供最基本的后备SQL
            fallback_sql = "SELECT * FROM customer WHERE 1=1;"
            logger.info(f"使用最基本的后备SQL: {fallback_sql}")
            
            return {'target_sql': fallback_sql, 'error': f'备用SQL生成失败: {str(e)}'}
            
    def _generate_control_sql_from_target(self, target_sql: str) -> str:
        """
        完全依赖大模型基于目标SQL生成对照组SQL
        大模型将根据业务理解智能设计逻辑互补的对照组
        """
        try:
            if not target_sql:
                logger.error("目标SQL为空，无法生成对照组SQL")
                return "SELECT * FROM customer WHERE 1=1;"
            
            # 完全依赖大模型生成对照组SQL
            if self.model:
                try:
                    prompt = f"""
                    作为数据分析专家，请你基于以下目标客群SQL，智能设计一个逻辑互补的对照组SQL。
                    你的任务不是简单地反转所有条件，而是要基于业务理解设计一个合适的对照组。
                    
                    请按照以下步骤进行思考：
                    1. 仔细分析目标SQL的业务含义和筛选条件
                    2. 理解目标客群的核心特征是什么
                    3. 思考什么样的客户群体适合作为对照组（通常是那些不具备目标客群核心特征但在其他方面相似的客户）
                    4. 设计对照组SQL，确保它能准确选择目标客群之外的客户群体
                    5. 保持与原SQL相似的查询结构、选择的字段、聚合函数和排序方式
                    
                    例如，如果目标客群是高消费客户（消费金额>1000元），对照组可以是：
                    - 消费金额<=1000元的客户
                    - 或者是消费频率相似但金额较低的客户
                    - 或者是其他合理的业务互补群体
                    
                    目标客群SQL: {target_sql}
                    
                    请直接返回生成的对照组SQL，确保它是有效的SQL语句，不要包含任何其他文字。
                    """
                    llm_response = self.model.invoke([HumanMessage(content=prompt)])
                    llm_generated_sql = llm_response.content.strip()
                    
                    # 验证返回的是有效的SQL语句
                    if llm_generated_sql and 'SELECT' in llm_generated_sql.upper():
                        logger.info(f"通过大模型智能生成了对照组SQL")
                        return llm_generated_sql
                    else:
                        logger.warning("大模型返回的不是有效的SQL语句")
                except Exception as e:
                    logger.error(f"使用大模型生成对照组SQL失败: {e}")
            
            # 如果没有可用的模型，作为后备方案，简单地在WHERE条件前添加NOT
            import re
            
            # 检查原SQL是否有WHERE子句
            if "WHERE" in target_sql.upper():
                # 提取WHERE子句
                match = re.search(r'WHERE\s+(.*?)(?:\s+GROUP\s+BY|\s+HAVING|\s+ORDER\s+BY|\s+LIMIT|\s*$)', target_sql, re.IGNORECASE | re.DOTALL)
                if match:
                    where_clause = match.group(1)
                    # 添加NOT前缀
                    control_sql = target_sql.replace(f"WHERE {where_clause}", f"WHERE NOT ({where_clause})", 1)
                    logger.info("在无模型情况下简单添加NOT条件生成对照组SQL")
                    return control_sql
            
            # 如果原SQL没有WHERE子句，对照组SQL可以查询所有记录（或适当的默认条件）
            control_sql = target_sql
            logger.info("原SQL没有WHERE子句，使用相同SQL作为对照组")
            return control_sql
        except Exception as e:
            logger.error(f"从目标SQL生成对照组SQL失败: {e}")
            return "SELECT * FROM customer WHERE 1=1;"
            
    def _check_and_correct_control_sql(self, target_sql: str, control_sql: str) -> str:
        """
        完全依赖大模型检查并修正对照组SQL
        大模型将根据目标客群SQL智能设计逻辑互补的对照组
        """
        try:
            if not target_sql:
                logger.warning("目标SQL为空，无法修正对照组SQL")
                return control_sql
            
            # 直接使用LLM进行智能修正，不使用任何规则化逻辑
            if self.model:
                try:
                    prompt = f"""
                    作为数据分析专家，请你智能设计一个与目标客群SQL逻辑互补的对照组SQL。
                    你需要基于业务理解，而不是简单地反转所有条件。
                    
                    请分析以下目标客群SQL的业务含义，然后设计一个合适的对照组SQL：
                    1. 理解目标客群的特征和筛选条件
                    2. 思考什么样的客户群体适合作为对照组（通常是不具备目标客群核心特征的群体）
                    3. 设计对照组SQL，确保它能准确选择目标客群之外的客户
                    4. 保持与原SQL相似的查询结构和统计指标
                    
                    目标客群SQL: {target_sql}
                    当前生成的对照组SQL: {control_sql}
                    
                    请直接返回修正后的对照组SQL，确保它是有效的SQL语句，不要包含任何其他文字。
                    """
                    llm_response = self.model.invoke([HumanMessage(content=prompt)])
                    corrected_sql = llm_response.content.strip()
                    
                    # 验证返回的是有效的SQL语句
                    if corrected_sql and 'SELECT' in corrected_sql.upper():
                        logger.info(f"通过大模型智能修正了对照组SQL")
                        return corrected_sql
                    else:
                        logger.warning("大模型返回的不是有效的SQL语句")
                except Exception as e:
                    logger.error(f"使用大模型修正对照组SQL失败: {e}")
           
        except Exception as e:
            logger.error(f"检查并修正对照组SQL失败: {e}")
            return control_sql

    
    def _generate_explanation_node(self, state: CustomerSegmentationState) -> Dict[str, Any]:
        """生成分析结果解释"""
        try:
            original_query = state.get('original_query', '')
            target_sql = state.get('target_sql', '')
            control_sql = state.get('control_sql', '')
            # 获取SQL执行结果相关信息
            target_sql_execution_result = state.get('target_sql_execution_result', {})
            control_sql_execution_result = state.get('control_sql_execution_result', {})
            
            # 获取解释生成提示词
            explanation_prompt = prompts_manager.get_prompt('text2sql', 'explanation') or \
                "你是一个数据分析专家，请基于以下信息提供分析解释："
            
            # 构建完整提示词，包含terminal日志中提到的必要参数
            explanation_content = f"{explanation_prompt}\n\n"
            explanation_content += f"原始问题：{original_query}\n\n"
            explanation_content += f"目标客群SQL：{target_sql}\n\n"
            # 添加sql_query参数（使用target_sql作为主查询）
            explanation_content += f"sql_query：{target_sql}\n\n"
            # 添加query_result参数
            target_formatted_result = str(target_sql_execution_result.get('formatted_result', '[]'))
            explanation_content += f"query_result：{target_formatted_result}\n\n"
            # 添加raw_result参数
            raw_result = str(target_sql_execution_result.get('raw_result', '[]'))
            explanation_content += f"raw_result：{raw_result}\n\n"
            if control_sql:
                explanation_content += f"对照组SQL：{control_sql}\n\n"
                # 添加对照组SQL执行结果
                control_formatted_result = str(control_sql_execution_result.get('formatted_result', '[]'))
                explanation_content += f"对照组查询结果：{control_formatted_result}\n\n"
                control_raw_result = str(control_sql_execution_result.get('raw_result', '[]'))
                explanation_content += f"对照组原始结果：{control_raw_result}\n\n"
            explanation_content += "请提供对查询逻辑的解释和分析建议，包括目标客群与对照组的对比分析："
            
            # 调用大模型生成解释
            response = self.model.invoke([HumanMessage(content=explanation_content)])
            explanation = response.content.strip()
            
            logger.info("生成分析结果解释完成")
            logger.info(f"最终解释内容: {explanation[:100]}...")
            return {'explanation': explanation, 'success': True}
        except Exception as e:
            logger.error(f"生成分析结果解释失败: {e}")
            return {'explanation': '', 'error': f'生成分析结果解释失败: {str(e)}', 'success': False}
    
    def process_query(self, query: str,session_id: str = 'default', conversation_history: Optional[List[Dict]] = None) -> Dict[str, Any]:
        """
        处理用户查询，执行客户圈选分析
        
        Args:
            query: 用户查询文本
            session_id: 会话ID，用于关联会话历史
            conversation_history: 会话历史
            
        Returns:
            包含分析结果的字典，包含SQL执行结果信息
        """
        try:
            # 准备初始状态
            initial_state = {
                "original_query": query,
                "conversation_history": conversation_history,
                "session_id": session_id,
                "Primitive_sql": "",
                "corresponding_sql": "",
                "execution_result": {},
                "explanation": "",
                "session_id": str(os.urandom(8).hex()),
                "error": None,
                "success": False
            }
            
            # 执行工作流
            logger.info(f"开始处理查询: {query[:50]}...")
            result = self.app.invoke(initial_state)
            if result.get('success', False):
                response={
                    'success': True,
                    "original_query": query,
                    "conversation_history": conversation_history,
                    "session_id": session_id,
                    "explanation": result.get('explanation', ''),
                }
                
                print(f"****成功获取到最终的解释: {response['explanation']}...")
                return response
            else:
                logger.error(f"查询处理失败: {result.get('error', '未知错误')}")
                



        except Exception as e:
            logger.error(f"处理查询时发生异常: {e}")
            
            # 创建默认的执行结果信息
            default_execution_result = {
                'sql_query': '',
                'formatted_result': '[]',
                'raw_result': '[]'
            }
            return {
                'success': False,
                'error': f'处理查询时发生异常: {str(e)}',
                'explanation': '',
                # 确保异常情况下也包含执行结果信息
                'target_sql_execution_result': default_execution_result,
                'control_sql_execution_result': default_execution_result,
                'sql_query': '',
               # 'query_result': '[]',
                'raw_result': '[]'
            }