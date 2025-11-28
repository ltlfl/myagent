"""
Text2SQL模块 - 独立的自然语言转SQL处理器
基于LangChain实现，支持多种数据库
"""
import os
import json
import logging
from typing import Dict, List, Any, Optional
from langchain_openai import ChatOpenAI
from langchain_community.utilities import SQLDatabase
from langchain_classic.chains import create_sql_query_chain
from langchain_core.output_parsers import StrOutputParser
from langchain_core.messages import HumanMessage, AIMessage, SystemMessage
from langchain_core.prompts import ChatPromptTemplate
from prompt.prompts_manager import prompts_manager

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Text2SQLProcessor:
    """Text2SQL处理器"""
    
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
        self._init_chain()
        
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
            self.model = ChatOpenAI(
                model=self.model_name,
                openai_api_key=self.openai_api_key,
                openai_api_base=self.openai_api_base,
                temperature=0.1
            )
            logger.info("语言模型初始化成功")
        except Exception as e:
            logger.error(f"语言模型初始化失败: {e}")
            raise
    
    def _init_chain(self):
        """初始化SQL生成链：
        query_chain用于将自然语言问题转换为SQL查询语句的链
        explanation_chain ：用于将SQL查询结果转换为自然语言解释的链
        react_prompt_template ：用于SQL优化（特别是货币换算逻辑）的提示词模板"""

        try:
            # 从提示词管理器获取SQL生成提示词
            sql_generation_prompt = prompts_manager.get_prompt('text2sql', 'sql_generation')
            
            # 创建包含自定义提示词的SQL查询链
            # 创建一个包含数据库信息和自定义提示词的prompt
            
            
            # 定义系统提示词和人类提示词
            system_prompt = sql_generation_prompt
            #{table_info}参数由 create_sql_query_chain 自动从传入的 db 对象获取的，不需要在调用时显式传递
            human_prompt = """
            数据库信息:
            {table_info}
            
            用户问题:
            {input}
            
            最多返回{top_k}条结果
            """
            
            prompt = ChatPromptTemplate.from_messages([
                ("system", system_prompt),
                ("human", human_prompt)
            ])
            
            # 创建自定义的SQL查询链
            self.query_chain = create_sql_query_chain(
                self.model, 
                self.db, 
                prompt=prompt,
                k=None  # 避免LIMIT限制
            )
            
            # 从提示词管理器获取结果解释提示词
            explanation_prompt = prompts_manager.get_prompt('text2sql', 'explanation')
            
            self.explanation_chain = explanation_prompt | self.model | StrOutputParser()
            
            # 初始化React思考提示词,重点是货币换算逻辑
            self.react_prompt = prompts_manager.get_prompt('text2sql', 'huobiyouhua')
            self.react_prompt_template = ChatPromptTemplate.from_template(
            self.react_prompt
        )
            
            logger.info("SQL处理链初始化成功")
        except Exception as e:
            logger.error(f"SQL处理链初始化失败: {e}")
            raise
    
    def _refine_sql_with_react(self, question: str, initial_sql: str) -> Dict[str, Any]:
        """使用React思考方式优化和完善SQL语句"""
        try:
            # 获取数据库表信息
            table_info_result = self.get_table_info()
            if not table_info_result['success']:
                return {
                    'success': False,
                    'error': f'获取表信息失败: {table_info_result["error"]}',
                    'sql_query': initial_sql  # 返回原始SQL作为后备
                }
            
            # 格式化表信息
            table_info_str = ""
            for table, info in table_info_result['detailed_info'].items():
                if 'schema' in info:
                    table_info_str += f"\n\n表 {table}:\n{info['schema']}"
            
            # 构建React思考提示
            react_input = {
                "question": question,
                "table_info": table_info_str,
                "initial_sql": initial_sql
            }
            
            # 对于ChatPromptTemplate，使用invoke生成消息列表
            messages = self.react_prompt_template.invoke(react_input)
            
            # 调用模型生成优化的SQL
            llm_response = self.model.invoke(messages)
            refined_sql = llm_response.content
            
            # 清理生成的SQL
            cleaned_refined_sql = self._clean_sql_query(refined_sql)
            
            logger.info(f"优化后的SQL: {cleaned_refined_sql}")
            
            return {
                'success': True,
                'sql_query': cleaned_refined_sql
            }
            
        except Exception as e:
            logger.error(f"SQL优化失败: {e}")
            return {
                'success': False,
                'error': f'SQL优化失败: {str(e)}',
                'sql_query': initial_sql  # 返回原始SQL作为后备
            }
    
    def process_query(self, question: str, session_id: str = 'default', entities: Dict[str, Any] = None, conversation_history: List[Dict] = None) -> Dict[str, Any]:
        """
        处理自然语言查询
        
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
            print(f"检查历史记录: {conversation_history}")

            # 大模型处理历史对话
            history_text = ""
            if conversation_history:
                history_text = "\n".join([f"{item['role']}: {item['content']}" for item in conversation_history])
                
                prompt_text = prompts_manager.put_history_to_question()
                # 构建完整的提示信息
                full_prompt = f"{prompt_text}\n\n{history_text}\n\n当前问题: {question}\n请根据对话历史重写当前问题，确保包含所有必要的上下文信息。"
                llm_response = self.model.invoke([HumanMessage(content=full_prompt)])
                question = llm_response.content
                print(f"大模型改写后的问题: {question}")
            else:
                # 即使没有历史记录，也打印当前问题
                print(f"当前问题: {question}")
            # 第一步：生成SQL查询
            # 构建增强查询，包含所有上下文信息
            enhanced_question = question
            
            # 1. 如果有entities信息，添加结构化信息
            if entities and 'order_by' in entities and entities['order_by']:
                enhanced_question = f"{enhanced_question} 请按{entities['order_by']}排序"
            
            # 2. 如果有会话历史，将其作为上下文信息传递给大模型
            if conversation_history:
                # 只提取用户的历史记录，并取最近3条
                user_history = [item for item in conversation_history if item['role'] == 'user'][-3:]
                if user_history:
                    # 构建上下文提示词，让大模型判断历史问题与当前问题的相关性
                    history_text = "\n".join([f"历史问题 {i+1}: {item['content']}" for i, item in enumerate(user_history)])
                    enhanced_question = f"""
                        上下文信息：
                        {history_text}
                        
                        当前问题：{enhanced_question}
                    """
                    logger.info(f"将用户历史记录作为上下文提示词传递给大模型: {enhanced_question[:100]}...")
            
            # 生成初始SQL查询
            sql_result = self._generate_sql(enhanced_question)
            if not sql_result['success']:
                return sql_result
            
            initial_sql = sql_result['sql_query']
            
            # 验证初始生成的SQL
            validation_result = self._validate_sql(initial_sql)
            if not validation_result['valid']:
                logger.warning(f"初始SQL验证失败: {validation_result['error']}")
                return {
                    'success': False,
                    'error': f'生成的SQL不符合安全规范: {validation_result["error"]}',
                    'session_id': session_id,
                    'initial_sql': initial_sql
                }
            
            # 第二步：使用React思考方式优化SQL
            refined_result = self._refine_sql_with_react(question, initial_sql)
            sql_query = refined_result['sql_query']  # 使用优化后的SQL，如果优化失败则使用原始SQL
            
            # 验证优化后的SQL
            validation_result = self._validate_sql(sql_query)
            if not validation_result['valid']:
                logger.warning(f"优化后SQL验证失败: {validation_result['error']}")
                return {
                    'success': False,
                    'error': f'优化后的SQL不符合安全规范: {validation_result["error"]}',
                    'session_id': session_id,
                    'initial_sql': initial_sql,
                    'sql_query': sql_query
                }
            
            # 第三步：执行SQL查询
            execution_result = self._execute_sql(sql_query)
            
            # 第四步：生成自然语言解释 - 包含原始结果供模型处理复杂格式
            # 确保将原始结果传递给大模型，让大模型处理复杂的格式问题
            explanation_result = self._generate_explanation(
                question, sql_query, execution_result
            )
            
            # 构建响应
            response = {
                'success': True,
                'question': question,
                'initial_sql': initial_sql,  # 添加初始SQL以便调试
                'sql_query': sql_query,
                'execution_result': execution_result,
                'explanation': explanation_result.get('explanation', ''),
                'row_count': execution_result.get('row_count', 0),
                'session_id': session_id,
                'metadata': {
                    'model': self.model_name,
                    'db_uri': self.db_uri.split('@')[-1] if '@' in self.db_uri else 'unknown'
                }
            }
            
            logger.info(f"查询处理成功，返回{response['row_count']}行结果")
            return response
            
        except Exception as e:
            logger.error(f"查询处理失败: {e}")
            return {
                'success': False,
                'error': f'查询处理失败: {str(e)}',
                'session_id': session_id
            }
    
    def _clean_sql_query(self, sql_query: str) -> str:
        """清理SQL查询,移除前缀,后缀和代码块标记，并从JSON中提取SQL"""
        if not isinstance(sql_query, str):
            return str(sql_query)
        
        cleaned = sql_query.strip()
        
        # 首先尝试从JSON格式中提取SQL
        # 检查是否包含JSON格式的输出
        if cleaned.startswith('{') and '"sql":' in cleaned:
            try:
                # json已在文件顶部导入
                # 尝试解析JSON
                json_data = json.loads(cleaned)
                if 'sql' in json_data and isinstance(json_data['sql'], str):
                    cleaned = json_data['sql'].strip()
            except json.JSONDecodeError:
                # 如果JSON解析失败，继续其他清理步骤
                pass
        
        # 提取代码块中的SQL内容
        if "```sql" in cleaned:
            start = cleaned.find("```sql") + 6
            end = cleaned.find("```", start)
            if end != -1:
                cleaned = cleaned[start:end].strip()
        elif "```" in cleaned:
            # 处理没有语言标识的代码块
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

    def _generate_sql(self, question: str) -> Dict[str, Any]:
        """生成SQL查询"""
        try:
            # 注意：尽管prompt模板中使用{input}，但LangChain的create_sql_query_chain内部仍然需要'question'参数
            # table_info会由create_sql_query_chain自动从db对象获取
            # top_k可以设置一个合理的默认值，或者使用None表示不限制
            sql_query = self.query_chain.invoke({"question": question, "top_k": None})
            cleaned_sql = self._clean_sql_query(sql_query)
            
            logger.info(f"生成的SQL: {cleaned_sql}")
            
            return {
                'success': True,
                'sql_query': cleaned_sql
            }
            
        except Exception as e:
            logger.error(f"SQL生成失败: {e}")
            return {
                'success': False,
                'error': f'SQL生成失败: {str(e)}'
            }
    
    def _execute_sql(self, sql_query: str) -> Dict[str, Any]:
        """执行SQL查询"""
        try:
            # 执行SQL查询
            result = self.db.run(sql_query)
            
            # 处理查询结果，确保data键包含实际数据
            # 将原始结果转换为列表格式，以便在_display_execution_result中正确显示
            if isinstance(result, list):
                data = result
            elif hasattr(result, '__iter__') and not isinstance(result, (str, dict)):
                # 如果结果是可迭代对象但不是字符串或字典，尝试转换为列表
                data = list(result)
            else:
                # 对于其他情况，将结果包装在列表中
                data = [result] if result is not None else []
            
            # 保存原始结果供大模型处理
            raw_result = result
            raw_result_str = str(result)
            
            # 计算实际行数
            row_count = len(data) if isinstance(data, list) else 1
            
            logger.info(f"SQL执行成功，返回{row_count}行结果")
            
            # 返回包含实际数据的结果对象
            return {
                'success': True,
                'data': data,  # 包含实际的查询结果数据
                'row_count': row_count,
                'raw_result': raw_result
            } 
        except Exception as e:
            logger.error(f"SQL执行失败: {e}")
            return {
                'success': False,
                'error': f'SQL执行失败: {str(e)}',
                'sql_query': sql_query
            }
    
    def _generate_explanation(self, question: str, sql_query: str, execution_result: Dict[str, Any]) -> Dict[str, Any]:
        """生成自然语言解释"""
        try:
            if not execution_result['success']:
                return {
                    'explanation': f"查询执行失败：{execution_result['error']}"
                }
            
            # 格式化查询结果
            result_text = self._format_result_for_explanation(execution_result)
            
            # 将原始结果也传递给大模型，让大模型处理复杂的格式问题
            raw_result_str = str(execution_result.get('raw_result', ''))
            
            # 生成解释，同时提供原始结果和格式化结果
            explanation = self.explanation_chain.invoke({
                'question': question,
                'sql_query': sql_query,
                'query_result': result_text,
                'raw_result': raw_result_str
            })
            
            return {
                'success': True,
                'explanation': explanation.strip()
            }
            
        except Exception as e:
            logger.error(f"解释生成失败: {e}")
            return {
                'explanation': f"已执行SQL查询，但生成解释时出错：{str(e)}"
            }
    
    def _format_result_for_explanation(self, execution_result: Dict[str, Any]) -> str:
        """格式化查询结果用于解释生成"""
        try:
            row_count = execution_result.get('row_count', 0)
            raw_result_str = str(execution_result.get('raw_result', ''))
            data = execution_result.get('data', [])
            
            # 当数据为空时，直接返回原始结果信息供大模型处理
            if row_count == 0:
                return f"查询结果为空。原始结果: {raw_result_str}"
            else:
                # 更明确地指示大模型显示所有符合条件的记录
                # 直接使用data字段中的结构化数据，确保大模型能访问到所有行
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
            
            return {
                'success': True,
                'tables': tables,
                'table_count': len(tables),
                'detailed_info': table_info
            }
            
        except Exception as e:
            logger.error(f"获取表信息失败: {e}")
            return {
                'success': False,
                'error': f'获取表信息失败: {str(e)}'
            }
    
    def validate_sql(self, sql_query: str) -> Dict[str, Any]:
        """验证SQL查询语法（基础验证）"""
        try:
            # 尝试解释SQL查询（不执行）
            # 这里可以使用SQL解析器，简化处理只做基本检查
            sql_upper = sql_query.upper().strip()
            
            if not sql_upper.startswith(('SELECT', 'SHOW', 'DESCRIBE')):
                return {
                    'valid': False,
                    'error': '只支持SELECT、SHOW、DESCRIBE查询'
                }
            
            # 基本语法检查
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
        """
        使用LLM进行SQL验证（基于prompts_manager中的sql_validation提示词）
        
        Args:
            sql: 待验证的SQL语句
            
        Returns:
            验证结果
        """
        try:
            # 先进行基础验证
            basic_validation = self.validate_sql(sql)
            if not basic_validation['valid']:
                return basic_validation
            
            # 获取SQL验证提示词
            try:
                validation_prompt = prompts_manager.get_prompt('sql_validation')
            except:
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
            
            # 构建验证请求，增强指令确保返回标准JSON
            validation_input = f"{validation_prompt}\n\n待验证SQL: {sql}\n\n请严格按照上述要求，仅返回JSON格式结果，不要添加任何其他文字："
            
            # HumanMessage已在文件顶部导入
            response = self.model.invoke([HumanMessage(content=validation_input)])
            
            # 解析验证结果
            # json已在文件顶部导入
            try:
                validation_result = json.loads(response.content)
                # 确保结果包含必需的键
                if 'valid' not in validation_result:
                    logger.info("LLM验证结果格式不完整，使用基础验证结果")
                    return basic_validation
                return validation_result
            except json.JSONDecodeError as e:
                # 如果解析失败，记录详细信息但不要用warning级别
                logger.info(f"LLM验证结果非JSON格式，使用基础验证结果（错误：{str(e)}）")
                return basic_validation
                
        except Exception as e:
            logger.error(f"SQL验证过程出错: {e}")
            # 出错时返回基础验证结果
            return self.validate_sql(sql)

# 不要在模块导入时创建全局实例，避免因为配置问题导致导入失败
# 需要时在代码中显式创建实例
# text2sql_processor = Text2SQLProcessor()