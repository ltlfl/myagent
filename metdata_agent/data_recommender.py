"""
数据推荐模块 - 元数据智能体的核心组件
"""
import logging
from typing import Dict, Any, List, Optional, Tuple
from utils.database import db_manager
from .asset_understanding import asset_understanding

class DataRecommender:
    """数据推荐类"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self._query_patterns = {
            'customer_info': ['客户', '顾客', '用户', 'cust', 'customer'],
            'product_info': ['产品', '商品', '服务', 'prod', 'product'],
            'account_info': ['账户', '账号', 'acct', 'account'],
            'transaction': ['交易', '流水', '记录', 'trans', 'transaction'],
            'loan': ['贷款', '借款', 'loan'],
            'deposit': ['存款', '储蓄', 'deposit'],
            'balance': ['余额', '结余', 'bal', 'balance'],
            'status': ['状态', '情况', 'status', 'state']
        }
        
        # 中文到英文表名的映射
        self._table_name_mapping = {
            '客户': 'customer_info',
            '顾客': 'customer_info',
            '用户': 'customer_info',
            '产品': 'product_info',
            '商品': 'product_info',
            '账户': 'deposit_business',
            '账号': 'deposit_business',
            '存款': 'deposit_business',
            '贷款': 'loan_business',
            '借款': 'loan_business'
        }
    
    def recommend_tables(self, query: str, limit: int = 5) -> Dict[str, Any]:
        """
        基于查询推荐相关表
        
        Args:
            query: 用户查询
            limit: 推荐数量限制
            
        Returns:
            推荐结果
        """
        try:
            # 获取所有表
            all_tables = db_manager.get_all_tables()
            
            if not all_tables:
                return {
                    'success': False,
                    'error': '数据库中没有找到任何表',
                    'recommendations': []
                }
            
            # 计算表与查询的相关性分数
            table_scores = []
            query_lower = query.lower()
            
            for table_name in all_tables:
                score = self._calculate_table_relevance(table_name, query_lower)
                if score > 0:
                    table_scores.append({
                        'table_name': table_name,
                        'relevance_score': score,
                        'reason': self._get_recommendation_reason(table_name, query_lower)
                    })
            
            # 按分数排序
            table_scores.sort(key=lambda x: x['relevance_score'], reverse=True)
            
            # 获取表详细信息
            recommendations = []
            for table_info in table_scores[:limit]:
                table_detail = asset_understanding.analyze_table_structure(table_info['table_name'])
                if table_detail['success']:
                    recommendations.append({
                        **table_info,
                        'table_detail': table_detail
                    })
            
            return {
                'success': True,
                'query': query,
                'total_tables': len(all_tables),
                'matched_tables': len(table_scores),
                'recommendations': recommendations,
                'message': f'找到 {len(recommendations)} 个相关表推荐'
            }
            
        except Exception as e:
            self.logger.error(f"表推荐失败: {e}")
            return {
                'success': False,
                'error': str(e),
                'recommendations': []
            }
    
    def _calculate_table_relevance(self, table_name: str, query: str) -> float:
        """计算表与查询的相关性分数"""
        score = 0.0
        table_lower = table_name.lower()
        
        # 直接匹配表名
        if table_lower in query:
            score += 10.0
        
        # 检查中文表名映射
        for chinese_name, english_table in self._table_name_mapping.items():
            if chinese_name in query and english_table == table_name:
                score += 15.0  # 给中文映射更高的权重
                break
        
        # 关键词匹配
        for pattern, keywords in self._query_patterns.items():
            for keyword in keywords:
                if keyword in table_lower and keyword in query:
                    score += 5.0
                elif keyword in table_lower:
                    score += 2.0
                elif keyword in query:
                    score += 1.0
        
        # 部分匹配
        for word in query.split():
            if len(word) > 2 and word in table_lower:
                score += 1.5
        
        return score
    
    def _get_recommendation_reason(self, table_name: str, query: str) -> str:
        """获取推荐理由"""
        table_lower = table_name.lower()
        
        # 检查中文表名映射
        for chinese_name, english_table in self._table_name_mapping.items():
            if chinese_name in query and english_table == table_name:
                return f"中文关键词 '{chinese_name}' 映射到表 '{table_name}'"
        
        # 直接匹配
        if table_lower in query:
            return f"表名 '{table_name}' 直接匹配查询关键词"
        
        # 关键词匹配
        matched_keywords = []
        for pattern, keywords in self._query_patterns.items():
            for keyword in keywords:
                if keyword in table_lower and keyword in query:
                    matched_keywords.append(keyword)
        
        if matched_keywords:
            return f"包含相关关键词: {', '.join(matched_keywords)}"
        
        # 部分匹配
        partial_matches = []
        for word in query.split():
            if len(word) > 2 and word in table_lower:
                partial_matches.append(word)
        
        if partial_matches:
            return f"部分匹配: {', '.join(partial_matches)}"
        
        return "基于语义相似度推荐"
    
    def recommend_fields(self, table_name: str, query: str, limit: int = 10) -> Dict[str, Any]:
        """
        基于查询推荐相关字段
        
        Args:
            table_name: 表名
            query: 用户查询
            limit: 推荐数量限制
            
        Returns:
            字段推荐结果
        """
        try:
            # 获取表结构
            table_analysis = asset_understanding.analyze_table_structure(table_name)
            
            if not table_analysis['success']:
                return {
                    'success': False,
                    'error': f"无法获取表 {table_name} 的结构信息",
                    'recommendations': []
                }
            
            # 计算字段相关性分数
            field_scores = []
            query_lower = query.lower()
            
            for field in table_analysis['fields']:
                score = self._calculate_field_relevance(field, query_lower)
                if score > 0:
                    field_scores.append({
                        'field_name': field['name'],
                        'relevance_score': score,
                        'field_info': field,
                        'reason': self._get_field_recommendation_reason(field, query_lower)
                    })
            
            # 按分数排序
            field_scores.sort(key=lambda x: x['relevance_score'], reverse=True)
            
            return {
                'success': True,
                'table_name': table_name,
                'query': query,
                'total_fields': len(table_analysis['fields']),
                'matched_fields': len(field_scores),
                'recommendations': field_scores[:limit],
                'message': f'在表 {table_name} 中找到 {len(field_scores)} 个相关字段'
            }
            
        except Exception as e:
            self.logger.error(f"字段推荐失败: {e}")
            return {
                'success': False,
                'error': str(e),
                'recommendations': []
            }
    
    def _calculate_field_relevance(self, field: Dict[str, Any], query: str) -> float:
        """计算字段与查询的相关性分数"""
        score = 0.0
        field_name = field['name'].lower()
        field_comment = field.get('comment', '').lower()
        
        # 中文关键词映射
        chinese_field_mapping = {
            '姓名': ['name', 'cust_name', 'customer_name'],
            '电话': ['phone', 'mobile', 'tel', 'telephone', 'mobile_no'],
            '手机': ['mobile', 'phone', 'cell_phone', 'mobile_no'],
            '号码': ['no', 'number', 'id', 'code'],
            '地址': ['address', 'addr'],
            '金额': ['amount', 'amt', 'balance', 'bal'],
            '时间': ['time', 'date', 'created_at', 'updated_at', 'timestamp'],
            '状态': ['status', 'state'],
            '类型': ['type', 'category', 'kind'],
            '编号': ['id', 'no', 'code', 'number']
        }
        
        # 检查中文关键词匹配
        for chinese_word, english_words in chinese_field_mapping.items():
            if chinese_word in query:
                for eng_word in english_words:
                    if eng_word in field_name:
                        score += 10.0  # 中文关键词匹配给予更高权重
                        break
        
        # 字段名直接匹配
        if field_name in query:
            score += 8.0
        
        # 注释匹配
        if field_comment and any(word in field_comment for word in query.split()):
            score += 6.0
        
        # 关键词匹配
        field_keywords = ['name', 'id', 'no', 'code', 'status', 'time', 'date', 'amt', 'bal']
        for keyword in field_keywords:
            if keyword in field_name and keyword in query:
                score += 4.0
        
        # 业务关键词匹配
        business_keywords = ['cust', 'prod', 'acct', 'loan', 'deposit', 'trans']
        for keyword in business_keywords:
            if keyword in field_name and keyword in query:
                score += 5.0
        
        # 部分匹配
        for word in query.split():
            if len(word) > 2 and word in field_name:
                score += 1.0
        
        # 主键加分
        if field.get('is_primary_key', False):
            score += 2.0
        
        return score
    
    def _get_field_recommendation_reason(self, field: Dict[str, Any], query: str) -> str:
        """获取字段推荐理由"""
        field_name = field['name'].lower()
        field_comment = field.get('comment', '').lower()
        
        # 中文关键词映射
        chinese_field_mapping = {
            '姓名': ['name', 'cust_name', 'customer_name'],
            '电话': ['phone', 'mobile', 'tel', 'telephone', 'mobile_no'],
            '手机': ['mobile', 'phone', 'cell_phone', 'mobile_no'],
            '号码': ['no', 'number', 'id', 'code'],
            '地址': ['address', 'addr'],
            '金额': ['amount', 'amt', 'balance', 'bal'],
            '时间': ['time', 'date', 'created_at', 'updated_at', 'timestamp'],
            '状态': ['status', 'state'],
            '类型': ['type', 'category', 'kind'],
            '编号': ['id', 'no', 'code', 'number']
        }
        
        # 检查中文关键词匹配
        for chinese_word, english_words in chinese_field_mapping.items():
            if chinese_word in query:
                for eng_word in english_words:
                    if eng_word in field_name:
                        return f"中文关键词 '{chinese_word}' 映射到字段 '{field['name']}'"
        
        # 直接匹配
        if field_name in query:
            return f"字段名 '{field['name']}' 直接匹配查询关键词"
        
        # 注释匹配
        if field_comment:
            for word in query.split():
                if word in field_comment:
                    return f"字段注释 '{field['comment']}' 包含相关关键词"
        
        # 关键词匹配
        if field.get('is_primary_key', False):
            return f"主键字段 '{field['name']}' 通常为重要查询条件"
        
        # 类型匹配
        if '时间' in query and any(time_word in field_name for time_word in ['time', 'date', 'dt', 'tm']):
            return f"时间相关字段 '{field['name']}' 匹配查询需求"
        
        if '姓名' in query and 'name' in field_name:
            return f"姓名相关字段 '{field['name']}' 匹配查询需求"
        
        if '电话' in query or '手机' in query:
            if any(phone_word in field_name for phone_word in ['phone', 'mobile', 'tel']):
                return f"电话相关字段 '{field['name']}' 匹配查询需求"
        
        return "基于字段语义相关性推荐"
    
    def suggest_joins(self, tables: List[str]) -> Dict[str, Any]:
        """
        建议表之间的连接关系
        
        Args:
            tables: 表名列表
            
        Returns:
            连接建议结果
        """
        try:
            if len(tables) < 2:
                return {
                    'success': False,
                    'error': '至少需要两个表才能建议连接关系',
                    'suggestions': []
                }
            
            # 获取表关系
            relationships = db_manager.get_table_relationships()
            
            # 查找表之间的直接关系
            join_suggestions = []
            
            for i, table1 in enumerate(tables):
                for table2 in tables[i+1:]:
                    # 查找外键关系
                    direct_joins = []
                    for rel in relationships:
                        if (rel['table_name'] == table1 and rel['referenced_table_name'] == table2) or \
                           (rel['table_name'] == table2 and rel['referenced_table_name'] == table1):
                            direct_joins.append(rel)
                    
                    if direct_joins:
                        for join in direct_joins:
                            if join['table_name'] == table1:
                                join_suggestions.append({
                                    'table1': table1,
                                    'table2': table2,
                                    'join_type': 'INNER JOIN',
                                    'on_condition': f"{table1}.{join['column_name']} = {table2}.{join['referenced_column_name']}",
                                    'relationship': 'foreign_key',
                                    'confidence': 1.0
                                })
                            else:
                                join_suggestions.append({
                                    'table1': table1,
                                    'table2': table2,
                                    'join_type': 'INNER JOIN',
                                    'on_condition': f"{table1}.{join['referenced_column_name']} = {table2}.{join['column_name']}",
                                    'relationship': 'foreign_key',
                                    'confidence': 1.0
                                })
                    else:
                        # 尝试基于字段名推断连接关系
                        inferred_join = self._infer_join_relationship(table1, table2)
                        if inferred_join:
                            join_suggestions.append(inferred_join)
            
            return {
                'success': True,
                'tables': tables,
                'suggestions': join_suggestions,
                'message': f'找到 {len(join_suggestions)} 个连接建议'
            }
            
        except Exception as e:
            self.logger.error(f"连接建议失败: {e}")
            return {
                'success': False,
                'error': str(e),
                'suggestions': []
            }
    
    def _infer_join_relationship(self, table1: str, table2: str) -> Optional[Dict[str, Any]]:
        """推断表之间的连接关系"""
        try:
            # 获取两个表的字段
            schema1 = db_manager.get_table_schema(table1)
            schema2 = db_manager.get_table_schema(table2)
            
            fields1 = [field['column_name'] for field in schema1]
            fields2 = [field['column_name'] for field in schema2]
            
            # 查找可能的连接字段
            possible_joins = []
            
            for field1 in fields1:
                for field2 in fields2:
                    # 同名字段
                    if field1 == field2:
                        possible_joins.append({
                            'field1': field1,
                            'field2': field2,
                            'confidence': 0.8
                        })
                    # 相似字段（如 id 和 user_id）
                    elif self._is_similar_field(field1, field2):
                        possible_joins.append({
                            'field1': field1,
                            'field2': field2,
                            'confidence': 0.6
                        })
            
            # 返回置信度最高的连接
            if possible_joins:
                best_join = max(possible_joins, key=lambda x: x['confidence'])
                return {
                    'table1': table1,
                    'table2': table2,
                    'join_type': 'LEFT JOIN',
                    'on_condition': f"{table1}.{best_join['field1']} = {table2}.{best_join['field2']}",
                    'relationship': 'inferred',
                    'confidence': best_join['confidence']
                }
            
            return None
            
        except Exception as e:
            self.logger.error(f"推断连接关系失败: {e}")
            return None
    
    def _is_similar_field(self, field1: str, field2: str) -> bool:
        """判断两个字段是否相似"""
        f1_lower = field1.lower()
        f2_lower = field2.lower()
        
        # ID字段相似性
        if f1_lower.endswith('_id') and f2_lower.endswith('_id'):
            base1 = f1_lower[:-3]
            base2 = f2_lower[:-3]
            return base1 == base2 or base1 in base2 or base2 in base1
        
        # NO字段相似性
        if f1_lower.endswith('_no') and f2_lower.endswith('_no'):
            base1 = f1_lower[:-3]
            base2 = f2_lower[:-3]
            return base1 == base2 or base1 in base2 or base2 in base1
        
        return False

# 全局数据推荐实例
data_recommender = DataRecommender()