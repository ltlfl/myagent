"""
大语言模型客户端 - 支持ChatOpenAI调用Qwen等模型
"""
import os
import json
import logging
from typing import Dict, Any, List, Optional
from openai import OpenAI
from prompt.prompts_manager import prompts_manager

# 尝试导入python-dotenv
try:
    from dotenv import load_dotenv
    load_dotenv()  # 加载.env文件
    DOTENV_AVAILABLE = True
except ImportError:
    DOTENV_AVAILABLE = False
    logging.warning("python-dotenv未安装，将直接从环境变量读取配置")

class LLMClient:
    """大语言模型客户端"""
    
    def __init__(self, api_key: str = None, base_url: str = None, model: str = None):
        """
        初始化LLM客户端
        
        Args:
            api_key: OpenAI API密钥
            base_url: API基础URL
            model: 模型名称
        """
        self.logger = logging.getLogger(__name__)
        
        # 从环境变量或参数获取配置
        self.api_key = api_key or os.getenv('OPENAI_API_KEY')
        self.base_url = base_url or os.getenv('OPENAI_BASE_URL', 'https://dashscope.aliyuncs.com/compatible-mode/v1')
        self.model = model or os.getenv('LLM_MODEL', 'qwen-plus')
        
        if not self.api_key:
            # 提供更详细的错误信息
            error_msg = "API密钥未配置，请按以下方式之一配置：\n"
            error_msg += "1. 在.env文件中添加：OPENAI_API_KEY=your_api_key_here\n"
            error_msg += "2. 设置环境变量：set OPENAI_API_KEY=your_api_key_here\n"
            error_msg += "3. 在初始化时传入：LLMClient(api_key='your_api_key_here')"
            
            if not DOTENV_AVAILABLE:
                error_msg += "\n\n提示：安装python-dotenv可支持.env文件配置：pip install python-dotenv"
            
            raise ValueError(error_msg)
        
        # 初始化OpenAI客户端
        self.client = OpenAI(
            api_key=self.api_key,
            base_url=self.base_url
        )
        
        self.logger.info(f"LLM客户端初始化完成，模型: {self.model}")
        if DOTENV_AVAILABLE:
            self.logger.info("使用.env文件配置")
    
    def chat_completion(self, messages: List[Dict[str, str]], 
                       temperature: float = 0.7,
                       max_tokens: int = 2000) -> Dict[str, Any]:
        """
        聊天补全
        
        Args:
            messages: 消息列表
            temperature: 温度参数
            max_tokens: 最大token数
            
        Returns:
            响应结果
        """
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=messages,
                temperature=temperature,
                max_tokens=max_tokens
            )
            
            return {
                'success': True,
                'content': response.choices[0].message.content,
                'usage': {
                    'prompt_tokens': response.usage.prompt_tokens,
                    'completion_tokens': response.usage.completion_tokens,
                    'total_tokens': response.usage.total_tokens
                }
            }
            
        except Exception as e:
            self.logger.error(f"LLM调用失败: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def parse_query_intent(self, query: str, schema_info: str = None) -> Dict[str, Any]:
        """
        解析查询意图
        
        Args:
            query: 用户查询
            schema_info: 数据库模式信息
            
        Returns:
            解析结果
        """
        # 从提示词管理器获取意图解析系统提示词
        system_prompt = prompts_manager.get_prompt('intent_parsing', 'system_prompt')

        user_prompt = f"""请分析以下查询意图：

用户查询：{query}

{f'数据库模式信息：{schema_info}' if schema_info else ''}

请返回JSON格式的分析结果。"""

        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ]
        
        result = self.chat_completion(messages, temperature=0.3)
        
        if result['success']:
            try:
                # 尝试解析JSON
                content = result['content']
                # 提取JSON部分
                if '```json' in content:
                    json_start = content.find('```json') + 7
                    json_end = content.find('```', json_start)
                    json_str = content[json_start:json_end].strip()
                else:
                    json_str = content.strip()
                
                parsed_result = json.loads(json_str)
                return {
                    'success': True,
                    'result': parsed_result,
                    'usage': result['usage']
                }
            except json.JSONDecodeError as e:
                self.logger.error(f"JSON解析失败: {e}")
                return {
                    'success': False,
                    'error': f"JSON解析失败: {e}",
                    'raw_content': result['content']
                }
        else:
            return result
    
    def generate_sql(self, query: str, schema_info: str, intent_info: Dict = None) -> Dict[str, Any]:
        """
        生成SQL查询
        
        Args:
            query: 用户查询
            schema_info: 数据库模式信息
            intent_info: 意图分析信息
            
        Returns:
            SQL生成结果
        """
        # 从提示词管理器获取SQL生成系统提示词
        system_prompt = prompts_manager.get_prompt('text2sql', 'sql_generation')

        user_prompt = f"""请生成SQL查询：

用户查询：{query}

数据库模式信息：
{schema_info}

{f'意图分析结果：{json.dumps(intent_info, ensure_ascii=False, indent=2)}' if intent_info else ''}

请返回JSON格式的SQL生成结果。"""

        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ]
        
        result = self.chat_completion(messages, temperature=0.1)
        
        if result['success']:
            try:
                content = result['content']
                if '```json' in content:
                    json_start = content.find('```json') + 7
                    json_end = content.find('```', json_start)
                    json_str = content[json_start:json_end].strip()
                else:
                    json_str = content.strip()
                
                parsed_result = json.loads(json_str)
                return {
                    'success': True,
                    'result': parsed_result,
                    'usage': result['usage']
                }
            except json.JSONDecodeError as e:
                self.logger.error(f"JSON解析失败: {e}")
                return {
                    'success': False,
                    'error': f"JSON解析失败: {e}",
                    'raw_content': result['content']
                }
        else:
            return result

# 延迟初始化，避免在模块导入时就创建实例
def get_llm_client():
    """获取LLM客户端实例（延迟初始化）"""
    try:
        return LLMClient()
    except ValueError as e:
        logging.warning(f"LLM客户端初始化失败: {e}")
        return None

# 全局LLM客户端实例（延迟初始化）
llm_client = None

def initialize_llm_client():
    """初始化LLM客户端"""
    global llm_client
    if llm_client is None:
        llm_client = get_llm_client()
    return llm_client