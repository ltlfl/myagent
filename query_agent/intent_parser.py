"""
意图解析模块 - 查询智能体的核心组件（集成LLM版本）
"""
import re
import logging
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass
from enum import Enum

# 导入LLM客户端
try:
    from utils.llm_client import llm_client, initialize_llm_client
    LLM_AVAILABLE = True
except ImportError:
    LLM_AVAILABLE = False
    logging.warning("LLM客户端不可用，将使用基于规则的解析")

class QueryType(Enum):
    """查询类型枚举"""
    SELECT = "select"
    AGGREGATE = "aggregate"
    FILTER = "filter"
    JOIN = "join"
    SORT = "sort"
    LIMIT = "limit"
    UNKNOWN = "unknown"

class QueryIntent(Enum):
    """查询意图枚举"""
    DATA_RETRIEVAL = "data_retrieval"      # 数据检索
    DATA_ANALYSIS = "data_analysis"        # 数据分析
    DATA_SUMMARY = "data_summary"          # 数据汇总
    DATA_COMPARISON = "data_comparison"    # 数据对比
    DATA_RANKING = "data_ranking"          # 数据排序
    DATA_COUNT = "data_count"              # 数据计数
    DATA_VALIDATION = "data_validation"    # 数据验证
    UNKNOWN = "unknown"

@dataclass
class ParsedIntent:
    """解析后的意图"""
    intent: QueryIntent
    query_type: QueryType
    entities: List[str]
    attributes: List[str]
    conditions: List[Dict[str, Any]]
    aggregations: List[Dict[str, Any]]
    order_by: List[Dict[str, Any]]
    limit: Optional[int]
    confidence: float
    raw_query: str
    llm_analysis: Optional[Dict[str, Any]] = None  # LLM分析结果

class IntentParser:
    """意图解析器（集成LLM版本）"""
    
    
    def __init__(self, use_llm: bool = True):
        self.logger = logging.getLogger(__name__)
        self.use_llm = use_llm and LLM_AVAILABLE
        self.llm_client = None  # 初始化LLM客户端属性
        self._setup_patterns()
        
        if self.use_llm:
            self.logger.info("意图解析器初始化完成，使用LLM增强解析")
        else:
            self.logger.info("意图解析器初始化完成，使用基于规则的解析")
    
    def _setup_patterns(self):
        """设置解析模式（保留原有规则作为备用）"""
        # 查询类型模式
        self.query_type_patterns = {
            QueryType.SELECT: [
                r'查询|显示|获取|找出|看看|查看|列出',
                r'什么|哪些|多少|几个',
                r'select|show|get|find|list'
            ],
            QueryType.AGGREGATE: [
                r'统计|汇总|合计|平均|最大|最小|总和',
                r'count|sum|avg|max|min|total',
                r'总计|平均数|最大值|最小值'
            ],
            QueryType.FILTER: [
                r'条件|筛选|过滤|满足|符合',
                r'where|filter|condition',
                r'大于|小于|等于|不等于|包含|不包含'
            ],
            QueryType.JOIN: [
                r'关联|连接|联合|合并',
                r'join|union|combine',
                r'和.*一起|与.*相关'
            ],
            QueryType.SORT: [
                r'排序|排列|升序|降序',
                r'order|sort|rank',
                r'从高到低|从低到高|按.*排序'
            ],
            QueryType.LIMIT: [
                r'前.*个|限制|只显示|最多',
                r'limit|top|first',
                r'前\d+|最多\d+|仅\d+'
            ]
        }
        
        # 意图模式
        self.intent_patterns = {
            QueryIntent.DATA_RETRIEVAL: [
                r'查询|显示|获取|找出|看看|查看',
                r'什么|哪些|怎么|如何',
                r'select|show|get|find'
            ],
            QueryIntent.DATA_ANALYSIS: [
                r'分析|解析|研究|探索',
                r'analyze|explore|study',
                r'趋势|模式|规律'
            ],
            QueryIntent.DATA_SUMMARY: [
                r'汇总|总结|概况|概览',
                r'summary|overview|profile',
                r'总体|整体|综合'
            ],
            QueryIntent.DATA_COMPARISON: [
                r'比较|对比|差异|区别',
                r'compare|difference|versus',
                r'比.*多|比.*少|超过|低于'
            ],
            QueryIntent.DATA_RANKING: [
                r'排名|排行|排序|等级',
                r'rank|rating|grade',
                r'第.*名|最高|最低|最佳|最差'
            ],
            QueryIntent.DATA_COUNT: [
                r'多少|几个|数量|计数',
                r'count|number|quantity',
                r'总数|个数|数量'
            ],
            QueryIntent.DATA_VALIDATION: [
                r'验证|检查|确认|是否',
                r'validate|check|verify',
                r'正确|错误|异常|问题'
            ]
        }
    
    def parse_intent(self, query: str, schema_info: str = None) -> ParsedIntent:
        """
        解析查询意图（集成LLM版本）
        
        Args:
            query: 用户查询
            schema_info: 数据库模式信息
            
        Returns:
            解析后的意图
        """
        try:
            # 首先尝试使用LLM解析
            if self.use_llm:
                llm_result = self._parse_with_llm(query, schema_info)
                if llm_result['success']:
                    return self._convert_llm_result_to_intent(query, llm_result['result'])
                else:
                    self.logger.warning(f"LLM解析失败，回退到规则解析: {llm_result.get('error', '未知错误')}")
            
            # 回退到基于规则的解析
            return self._parse_with_rules(query)
            
        except Exception as e:
            self.logger.error(f"意图解析失败: {e}")
            # 返回默认意图
            return ParsedIntent(
                intent=QueryIntent.UNKNOWN,
                query_type=QueryType.UNKNOWN,
                entities=[],
                attributes=[],
                conditions=[],
                aggregations=[],
                order_by=[],
                limit=None,
                confidence=0.0,
                raw_query=query
            )
    
    def _parse_with_llm(self, query: str, schema_info: str = None) -> Dict[str, Any]:
        """使用LLM解析意图"""
        try:
            if self.llm_client is None:
                self.llm_client = initialize_llm_client()
                if self.llm_client is None:
                    return {'success': False, 'error': 'LLM客户端初始化失败'}
            
            return self.llm_client.parse_query_intent(query, schema_info)
        except Exception as e:
            self.logger.error(f"LLM解析调用失败: {e}")
            return {'success': False, 'error': str(e)}
    
    def _convert_llm_result_to_intent(self, query: str, llm_result: Dict[str, Any]) -> ParsedIntent:
        """将LLM结果转换为ParsedIntent对象"""
        try:
            # 转换意图类型
            intent_str = llm_result.get('intent', 'unknown')
            intent = self._string_to_intent(intent_str)
            
            # 转换查询类型
            query_type_str = llm_result.get('query_type', 'unknown')
            query_type = self._string_to_query_type(query_type_str)
            
            # 提取其他信息
            entities = llm_result.get('entities', [])
            attributes = llm_result.get('attributes', [])
            conditions = llm_result.get('conditions', [])
            aggregations = llm_result.get('aggregations', [])
            order_by = llm_result.get('order_by', [])
            limit = llm_result.get('limit')
            confidence = llm_result.get('confidence', 0.8)
            
            return ParsedIntent(
                intent=intent,
                query_type=query_type,
                entities=entities,
                attributes=attributes,
                conditions=conditions,
                aggregations=aggregations,
                order_by=order_by,
                limit=limit,
                confidence=confidence,
                raw_query=query,
                llm_analysis=llm_result
            )
            
        except Exception as e:
            self.logger.error(f"LLM结果转换失败: {e}")
            # 返回基于规则的解析结果
            return self._parse_with_rules(query)
    
    def _string_to_intent(self, intent_str: str) -> QueryIntent:
        """将字符串转换为QueryIntent枚举"""
        intent_mapping = {
            'data_retrieval': QueryIntent.DATA_RETRIEVAL,
            'data_analysis': QueryIntent.DATA_ANALYSIS,
            'data_summary': QueryIntent.DATA_SUMMARY,
            'data_comparison': QueryIntent.DATA_COMPARISON,
            'data_ranking': QueryIntent.DATA_RANKING,
            'data_count': QueryIntent.DATA_COUNT,
            'data_validation': QueryIntent.DATA_VALIDATION
        }
        return intent_mapping.get(intent_str.lower(), QueryIntent.UNKNOWN)
    
    def _string_to_query_type(self, query_type_str: str) -> QueryType:
        """将字符串转换为QueryType枚举"""
        type_mapping = {
            'select': QueryType.SELECT,
            'aggregate': QueryType.AGGREGATE,
            'filter': QueryType.FILTER,
            'join': QueryType.JOIN,
            'sort': QueryType.SORT,
            'limit': QueryType.LIMIT
        }
        return type_mapping.get(query_type_str.lower(), QueryType.UNKNOWN)
    
    def _parse_with_rules(self, query: str) -> ParsedIntent:
        """使用基于规则的方法解析意图（原有逻辑）"""
        query_lower = query.lower()
        
        # 解析查询类型
        query_type = self._parse_query_type(query_lower)
        
        # 解析意图
        intent = self._parse_query_intent(query_lower)
        
        # 解析实体
        entities = self._extract_entities(query)
        
        # 解析属性
        attributes = self._extract_attributes(query)
        
        # 解析条件
        conditions = self._extract_conditions(query)
        
        # 解析聚合
        aggregations = self._extract_aggregations(query)
        
        # 解析排序
        order_by = self._extract_order_by(query)
        
        # 解析限制
        limit = self._extract_limit(query)
        
        # 计算置信度
        confidence = self._calculate_confidence(query, intent, query_type)
        
        return ParsedIntent(
            intent=intent,
            query_type=query_type,
            entities=entities,
            attributes=attributes,
            conditions=conditions,
            aggregations=aggregations,
            order_by=order_by,
            limit=limit,
            confidence=confidence,
            raw_query=query
        )
    
    def _parse_query_type(self, query: str) -> QueryType:
        """解析查询类型"""
        scores = {}
        for query_type, patterns in self.query_type_patterns.items():
            score = 0
            for pattern in patterns:
                matches = re.findall(pattern, query)
                score += len(matches)
            scores[query_type] = score
        
        # 返回得分最高的查询类型
        if max(scores.values()) == 0:
            return QueryType.UNKNOWN
        
        return max(scores, key=scores.get)
    
    def _parse_query_intent(self, query: str) -> QueryIntent:
        """解析查询意图"""
        scores = {}
        for intent, patterns in self.intent_patterns.items():
            score = 0
            for pattern in patterns:
                matches = re.findall(pattern, query)
                score += len(matches)
            scores[intent] = score
        
        # 返回得分最高的意图
        if max(scores.values()) == 0:
            return QueryIntent.UNKNOWN
        
        return max(scores, key=scores.get)
    
    def _extract_entities(self, query: str) -> List[str]:
        """提取实体"""
        entities = []
        
        # 提取表名相关实体
        table_patterns = [
            r'客户表|顾客表|用户表|customer',
            r'产品表|商品表|服务表|product',
            r'账户表|账号表|account',
            r'贷款表|借款表|loan',
            r'存款表|储蓄表|deposit'
        ]
        
        for pattern in table_patterns:
            matches = re.findall(pattern, query, re.IGNORECASE)
            entities.extend(matches)
        
        return list(set(entities))
    
    def _extract_attributes(self, query: str) -> List[str]:
        """提取属性"""
        attributes = []
        
        # 提取字段名相关属性
        field_patterns = [
            r'姓名|名字|name',
            r'编号|ID|号码|no',
            r'状态|情况|status',
            r'时间|日期|time|date',
            r'金额|数额|amount|balance'
        ]
        
        for pattern in field_patterns:
            matches = re.findall(pattern, query, re.IGNORECASE)
            attributes.extend(matches)
        
        return list(set(attributes))
    
    def _extract_conditions(self, query: str) -> List[Dict[str, Any]]:
        """提取条件"""
        conditions = []
        
        # 简单的条件提取逻辑
        condition_patterns = [
            r'(.*?)(大于|超过|>)(\d+)',
            r'(.*?)(小于|低于|<)(\d+)',
            r'(.*?)(等于|是|=)(.+?)',
            r'(.*?)(包含|含有)(.+?)'
        ]
        
        for pattern in condition_patterns:
            matches = re.findall(pattern, query)
            for match in matches:
                if len(match) >= 3:
                    field = match[0].strip()
                    operator = match[1].strip()
                    value = match[2].strip()
                    
                    # 转换操作符
                    op_mapping = {
                        '大于': '>', '超过': '>', '>': '>',
                        '小于': '<', '低于': '<', '<': '<',
                        '等于': '=', '是': '=', '=': '=',
                        '包含': 'LIKE', '含有': 'LIKE'
                    }
                    
                    conditions.append({
                        'field': field,
                        'operator': op_mapping.get(operator, '='),
                        'value': value
                    })
        
        return conditions
    
    def _extract_aggregations(self, query: str) -> List[Dict[str, Any]]:
        """提取聚合函数"""
        aggregations = []
        
        agg_patterns = {
            'count': [r'计数|数量|个数|count'],
            'sum': [r'求和|总和|合计|sum'],
            'avg': [r'平均|平均值|mean|avg'],
            'max': [r'最大|最高|max'],
            'min': [r'最小|最低|min']
        }
        
        for func, patterns in agg_patterns.items():
            for pattern in patterns:
                if re.search(pattern, query, re.IGNORECASE):
                    aggregations.append({'function': func, 'field': '*'})
                    break
        
        return aggregations
    
    def _extract_order_by(self, query: str) -> List[Dict[str, Any]]:
        """提取排序信息"""
        order_by = []
        
        # 检测排序关键词
        if re.search(r'排序|排列|order|sort', query, re.IGNORECASE):
            # 默认按第一个字段排序
            direction = 'desc' if re.search(r'降序|从高到低|desc', query, re.IGNORECASE) else 'asc'
            order_by.append({'field': 'id', 'direction': direction})
        
        return order_by
    
    def _extract_limit(self, query: str) -> Optional[int]:
        """提取限制数量"""
        # 匹配数字限制
        limit_patterns = [
            r'前(\d+)个',
            r'限制(\d+)',
            r'最多(\d+)',
            r'top\s*(\d+)',
            r'limit\s*(\d+)'
        ]
        
        for pattern in limit_patterns:
            match = re.search(pattern, query, re.IGNORECASE)
            if match:
                return int(match.group(1))
        
        return None
    
    def _calculate_confidence(self, query: str, intent: QueryIntent, query_type: QueryType) -> float:
        """计算解析置信度"""
        confidence = 0.5  # 基础置信度
        
        # 根据查询长度调整
        if len(query) > 10:
            confidence += 0.1
        
        # 根据意图和查询类型调整
        if intent != QueryIntent.UNKNOWN:
            confidence += 0.2
        
        if query_type != QueryType.UNKNOWN:
            confidence += 0.2
        
        return min(confidence, 1.0)