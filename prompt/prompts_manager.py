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
                'Problem_Analysis_prompt': self._Problem_Analysis_prompt(),
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
0. 如果提供了原始SQL作为参考，请严格仿照其结构和格式生成新的SQL语句，确保两个SQL语句成为严格的对照组。表结构、JOIN方式、字段顺序、SELECT字段、GROUP BY、ORDER BY等所有结构必须完全一致。WHERE条件中，除了需要修改的关键条件（如金额阈值）外，其他条件（如交易类型、状态等）必须完全相同。
1. 生成标准SQL语法，兼容MySQL
2. 确保查询安全，避免SQL注入
3. 使用正确的表名和字段名
4. 添加适当的WHERE条件进行过滤
5. 对于聚合查询，使用正确的GROUP BY
6. 如果提供了原始SQL作为参考，请优先遵循人类提示词中的要求，生成符合要求的SQL语句

重要规则：
- **当用户查询中包含"所有信息"、"完整信息"、"全部信息"等关键词时，必须执行以下操作**：
  1. 使用INNER JOIN关联所有相关表
  2. 返回所有表中的所有字段（使用*）
  3. 确保通过外键正确关联所有表
- **当生成对照SQL时，除了明确需要修改的条件外，其他所有条件必须与原始SQL完全一致，特别是交易类型等业务条件**
- **MySQL特定规则：当需要使用IN子查询且子查询包含LIMIT子句时，必须使用派生表加JOIN的方式替代IN子查询**
- **MySQL特定规则：所有派生表必须添加别名**
- **MySQL特定规则：避免使用SELECT *，只查询必要的字段（除非用户明确要求获取所有字段）**
- **MySQL特定规则：确保表名和别名之间有空格**

返回格式：
如果没有提供原始SQL作为参考，请返回SQL语句本身；如果提供了原始SQL作为参考，请直接返回SQL语句，不要返回JSON格式。
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
        {
            "intent": "查询意图类型",
            "query_type": "查询类型",
            "entities": ["实体列表"],
           
        }

        意图类型包括：
        - data_retrieval: 数据检索
        - data_analysis: 数据分析  
        - data_comparison: 数据对比
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
        4. 严格检查是否使用了不存在的表或字段，不能使用不存在的字段名
        
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
        """创建SQL优化提示词"""
        return """
            你是一个专业的SQL优化专家，请对以下生成的SQL进行优化，重点关注问题和SQL语句是否匹配，以及严格使用数据库中实际存在的表和字段。
            
            请分析以下内容：
            - 用户问题: {question}
            - 数据库表结构: {table_info}
            - 初步生成的SQL: {initial_sql}
            
            优化要求：
            1. **严格检查字段名**：必须使用数据库表结构中明确列出的实际字段名，**绝对不允许使用任何不存在的虚构字段**
            2. **表名验证**：确保SQL中使用的所有表名都存在于提供的表结构中
            3. 保持原始SQL的基本结构和表选择不变
            4. 修正SQL语句，确保和用户问题匹配
            5. 只优化必要的部分，不要引入额外的表连接或过滤条件
            6. 确保SQL语法正确，可以直接执行
            7. 对于表别名，确保在SELECT、WHERE、GROUP BY、ORDER BY等所有子句中保持一致
            
            重要提示：
            - 在优化过程中，请仔细核对每一个字段名是否在表结构中存在
            - 如果发现SQL中使用了不存在的字段或表，请立即修正
            - 不要臆测或创造表结构中未定义的字段
            
            请直接输出优化后的SQL语句，不要包含其他解释。
            """
    def _Problem_Analysis_prompt(self) -> PromptTemplate:
        """设计一个问题分析智能体，用于客户圈选和数据准备。你返回的数据精准而全面，确保客户分析的科学性。"""
        template = """
            请你对用户问题：{question}进行以下分析：
            1.分析当前的问题所需要的客户数据字段。
            2.如果客户没有指明返回字段，默认返回所有表中关联的客户数据字段,这些字段应该尽可能全面。
            3.根据客户数据字段，圈选符合条件的客户，客户数据字段包括客户ID、客户类型、客户状态、存款流失率、月末余额、存款金额、存款时间等等
            4.根据符合条件的客户圈选对照组
            5.请严格按照以下JSON格式输出，不要包含任何其他文字：
            {{
                "analysis_type": "分析类型",
                "required_fields": ["需要的字段1", "需要的字段2", ...],
                "target_criteria": {{
                    "条件1": "值1",
                    "条件2": "值2",
                    ...
                }},
                "control_criteria": {{
                    "条件1": "值1",
                    "条件2": "值2",
                    ...
                }},
                "target_query_question": "用于调用text2sql的目标客群查询问题",
                "control_query_question": "用于调用text2sql的对照组查询问题"
            }}
            
            例如：
            问题：分析烟台分行存款流失超过50%且月末余额<1万元的个人客户
            输出格式示例：
            {{
                "analysis_type": "存款流失分析",
                "required_fields": ["客户ID", "客户类型", "客户状态", "存款流失率", "月末余额", "存款金额", "存款时间"],
                "target_criteria": {{
                    "客户类型": "个人客户",
                    "客户状态": "正常",
                    "存款流失率": ">50%",
                    "月末余额": "<1万元"
                }},
                "control_criteria": {{
                    "客户类型": "个人客户",
                    "客户状态": "正常",
                    "存款流失率": "<=50%",
                    "匹配条件": "客户存款金额与目标客群相同且存款时间在目标客群存款时间范围内"
                }},
                "target_query_question": "查询客户类型为个人客户，客户状态为正常，客户存款流失率超过50%，客户月末余额<1万元的客户数据，返回客户ID、客户类型、客户状态、存款流失率、月末余额、存款金额、存款时间等字段",
                "control_query_question": "查询客户类型为个人客户，客户状态为正常，客户存款流失率在50%以下，并且客户存款金额与目标客群相同、存款时间在目标客群存款时间范围内的客户数据，返回客户ID、客户类型、客户状态、存款流失率、月末余额、存款金额、存款时间等字段"
            }}
        """
        return PromptTemplate.from_template(template)
# 创建全局提示词管理器实例
prompts_manager = PromptsManager()