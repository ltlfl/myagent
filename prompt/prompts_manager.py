"""
提示词管理器 - 统一管理所有模型提示词模板
"""
from typing import Dict, Any, List
from langchain_core.prompts import PromptTemplate, ChatPromptTemplate

class PromptsManager:
    """提示词管理器类"""
    
    def __init__(self):
        """初始化提示词管理器"""
        self.prompts = {
            # Text2SQL相关提示词
            'text2sql': {
                'explanation': self._create_explanation_prompt(),
                'huobiyouhua': self._create_huobiyouhua_prompt(),
                'sql_generation': self._create_sql_generation_prompt(),
                'response_generation': self._create_response_generation_prompt()
            },
            # 查询意图解析相关提示词
            'intent_parsing': {
                'system_prompt': self._create_intent_parsing_system_prompt()
            },
            # 通用提示词
            'common': {
                'sql_validation': self._create_sql_validation_prompt()
            }
        }
    
    def _create_explanation_prompt(self) -> PromptTemplate:
        """创建结果解释提示词"""
        template = """
        基于以下用户问题和SQL查询结果，请提供自然语言的回答：

        用户问题：{question}
        SQL查询：{sql_query}
        查询结果：{query_result}
        原始查询结果：{raw_result}

        请用简洁、清晰的中文回答用户的问题。请同时考虑格式化的查询结果和原始查询结果，确保回答准确反映所有查询到的数据。如果查询结果为空，请说明没有找到相关数据。
        """
        return PromptTemplate.from_template(template)
    
    def _create_sql_generation_prompt(self) -> str:
        """创建SQL生成系统提示词"""
        return """
你是一个专业的SQL查询生成助手。请根据用户的自然语言查询和数据库模式信息，生成准确的SQL查询语句。

要求：
0. 考虑用户历史问题与当前问题的相关性
1. 生成标准SQL语法，兼容MySQL
2. 确保查询安全，避免SQL注入
3. 使用正确的表名和字段名
4. 添加适当的WHERE条件进行过滤
5. 对于聚合查询，使用正确的GROUP BY
6. 返回JSON格式结果

重要规则：
- **当用户查询中包含"所有信息"、"完整信息"、"全部信息"等关键词时，必须执行以下操作**：
  1. 使用INNER JOIN关联所有相关表
  2. 返回所有表中的所有字段（使用*）
  3. 确保通过外键正确关联所有表
- **当处理涉及不同货币的金额比较（如存款最多、金额最高等）时，必须考虑货币换算**：
  1. 假设有以下汇率：1美元(USD) = 7人民币(CNY)，1欧元(EUR) = 7/0.9人民币(CNY)
  2. 请使用CASE语句将所有货币金额转换为同一币种（建议转换为人民币CNY）后再进行比较
  3. 对于存款最多等查询，必须先转换货币单位，然后再计算总额并排序

返回格式：
{{
    "sql": "生成的SQL语句",
    "explanation": "SQL解释说明",
    "tables_used": ["使用的表名"],
    "fields_used": ["使用的字段名"],
    "confidence": 0.9,
    "alternatives": ["替代SQL语句1", "替代SQL语句2"]
}}

    请仔细分析用户需求和数据库结构，生成准确的SQL查询。

    特别示例 - 查询特定客户的所有关联信息：
    当用户提问"给出客户编号：C1896179073998708285的客户的所有表的信息"时，请生成如下SQL：
    SELECT *
    FROM customer_info
    INNER JOIN deposit_business ON customer_info.CUST_NO = deposit_business.CUST_NO
    INNER JOIN loan_business ON customer_info.CUST_NO = loan_business.CUST_NO
    INNER JOIN product_info ON (
        deposit_business.PROD_CD = product_info.PROD_CD OR 
        loan_business.PROD_CD = product_info.PROD_CD
    )
    WHERE customer_info.CUST_NO = 'C1896179073998708285';
        """
    
    def _create_response_generation_prompt(self) -> str:
        """创建响应生成提示词"""
        template = """
            你是一个数据分析助手。根据SQL查询结果回答用户的问题。

            SQL查询：{sql_query}
            查询结果：{context}
            用户问题：{prompt}

            请根据查询结果给出清晰、准确的回答。如果查询结果为空，请说明没有找到相关数据。
        """
        return template
    
    def _create_intent_parsing_system_prompt(self) -> str:
        """创建查询意图解析系统提示词"""
        return """
        你是一个专业的数据库查询意图分析助手。请分析用户的自然语言查询，识别其意图和关键信息。

        请返回JSON格式的分析结果，包含以下字段：
        {{
            "intent": "查询意图类型",
            "query_type": "查询类型",
            "entities": ["实体列表"],
            "attributes": ["属性列表"],
            "conditions": [{{"field": "字段", "operator": "操作符", "value": "值"}}],
            "aggregations": [{{"function": "聚合函数", "field": "字段"}}],
            "order_by": [{{"field": "字段", "direction": "asc/desc"}}],
            "limit": 数字或null,
            "confidence": 0.9,
            "explanation": "解释说明"
        }}

        意图类型包括：
        - data_retrieval: 数据检索
        - data_analysis: 数据分析  
        - data_summary: 数据汇总
        - data_comparison: 数据对比
        - data_ranking: 数据排序
        - data_count: 数据计数
        - data_validation: 数据验证

        查询类型包括：
        - select: 查询
        - aggregate: 聚合
        - filter: 过滤
        - join: 连接
        - sort: 排序
        - limit: 限制

        请仔细分析用户查询，返回准确的JSON格式结果。
        """
    
    def _create_sql_validation_prompt(self) -> str:
        """创建SQL验证提示词"""
        return """
        你是一个SQL语法验证专家。请检查以下SQL查询是否有效且安全。

        要求：
        1. 只允许SELECT、SHOW、DESCRIBE查询
        2. 不允许修改数据库结构或数据的操作（如INSERT、UPDATE、DELETE、DROP、ALTER等）
        3. 检查SQL语法是否正确

        请返回验证结果和说明。
                """
    
    def get_prompt(self, category: str, prompt_name: str) -> Any:
        """
        获取指定提示词
        
        Args:
            category: 提示词类别
            prompt_name: 提示词名称
            
        Returns:
            提示词模板或字符串
        """
        if category in self.prompts and prompt_name in self.prompts[category]:
            return self.prompts[category][prompt_name]
        raise ValueError(f"未找到提示词: {category}.{prompt_name}")
    
    def get_all_prompts(self) -> Dict[str, Dict[str, Any]]:
        """获取所有提示词"""
        return self.prompts


    def put_history_to_question(self) -> str:
        """把历史对话根据上下文关系重新改写问题"""
        return """
        你是一个语言模型，你的任务是根据历史对话和当前问题，重新改写当前问题，使它与历史对话紧密相关。

        要求：
        1. 重新改写当前问题，使它与历史对话紧密相关
        2. 保持问题的语义和意图不变
        3. 如果当前问题与历史对话不相关，直接返回当前问题
        4. 保持问题的长度在30个字符以下
        5. 如果问题包含多个独立的问题，每个问题都需要重新改写
        请返回改写后的问题。    
        举例：
        历史对话：
        用户：年龄在四十岁以上的客户有多少逾期的？
        改写后的问题：年龄在四十岁以上的客户有多少逾期的？
        用户：给出这四人的名字。  
        改写后的问题：年龄在四十岁以上的客户有多少逾期的？给出这些人的名字。
        用户：这些客户的手机号是多少？  
        改写后的问题：年龄在四十岁以上的客户有多少逾期的？这些客户的手机号是多少？
        
                """
    def _create_huobiyouhua_prompt(self) -> str:
        """获取获取历史对话根据上下文关系重新改写问题提示词"""
        return """
            你是一个专业的SQL优化专家，请对以下生成的SQL进行优化，重点关注问题和SQL语句是否匹配。
            你需要先判断SQL语句的含义，然后和用户问题进行匹配，确保SQL语句的执行结果符合用户的需求。
            如果SQL语句和用户问题不匹配，你需要根据用户问题重新生成SQL语句。
            如果涉及到货币换算，你需要根据用户问题和数据库表结构，判断是否需要进行货币换算。
            请使用正确的汇率进行货币换算：
            - 1美元 = 7人民币
            - 1欧元 = 7/0.9人民币
            
            请分析以下内容：
            - 用户问题: {question}
            - 数据库表结构: {table_info}
            - 初步生成的SQL: {initial_sql}
            
            优化要求：
            1. 保持原始SQL的基本结构和表选择不变
            2. 修正SQL语句，确保和用户问题匹配
            3. 只优化必要的部分，不要引入额外的表连接或过滤条件
            4. 确保SQL语法正确，可以直接执行
            
            请直接输出优化后的SQL语句，不要包含其他解释。
            """

# 创建全局提示词管理器实例
prompts_manager = PromptsManager()