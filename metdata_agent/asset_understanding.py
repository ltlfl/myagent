"""
数据资产理解模块 - 元数据智能体的核心组件
"""
import logging
import re
from typing import Dict, Any, List, Optional
from utils.database import db_manager

class AssetUnderstanding:
    """数据资产理解类"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self._field_type_mapping = {
            'VARCHAR': '文本',
            'INT': '整数',
            'BIGINT': '长整数',
            'DECIMAL': '小数',
            'FLOAT': '浮点数',
            'DOUBLE': '双精度浮点数',
            'DATE': '日期',
            'DATETIME': '日期时间',
            'TIMESTAMP': '时间戳',
            'TEXT': '长文本',
            'BOOLEAN': '布尔值',
            'TINYINT': '小整数'
        }
    
    def analyze_table_structure(self, table_name: str) -> Dict[str, Any]:
        """
        分析表结构
        
        Args:
            table_name: 表名
            
        Returns:
            表结构分析结果
        """
        try:
            # 获取表结构
            schema = db_manager.get_table_schema(table_name)
            
            if not schema:
                return {
                    'success': False,
                    'error': f'表 {table_name} 不存在或无法访问',
                    'table_name': table_name
                }
            
            # 分析字段
            fields = []
            primary_keys = []
            foreign_keys = []
            
            for field in schema:
                field_info = {
                    'name': field['column_name'],
                    'type': field['data_type'],
                    'type_chinese': self._field_type_mapping.get(field['data_type'].upper(), field['data_type']),
                    'nullable': field['is_nullable'] == 'YES',
                    'default': field['column_default'],
                    'comment': field['column_comment'] or '',
                    'key_type': field['column_key']
                }
                
                # 判断主键
                if field['column_key'] == 'PRI':
                    primary_keys.append(field['column_name'])
                    field_info['is_primary_key'] = True
                else:
                    field_info['is_primary_key'] = False
                
                fields.append(field_info)
            
            # 获取外键信息
            relationships = db_manager.get_table_relationships()
            for rel in relationships:
                if rel['table_name'] == table_name:
                    foreign_keys.append({
                        'field': rel['column_name'],
                        'references_table': rel['referenced_table_name'],
                        'references_field': rel['referenced_column_name'],
                        'constraint_name': rel['constraint_name']
                    })
            
            # 生成表描述
            table_description = self._generate_table_description(table_name, fields, primary_keys, foreign_keys)
            
            return {
                'success': True,
                'table_name': table_name,
                'description': table_description,
                'fields': fields,
                'primary_keys': primary_keys,
                'foreign_keys': foreign_keys,
                'field_count': len(fields),
                'has_relationships': len(foreign_keys) > 0
            }
            
        except Exception as e:
            self.logger.error(f"分析表结构失败: {e}")
            return {
                'success': False,
                'error': str(e),
                'table_name': table_name
            }
    
    def _generate_table_description(self, table_name: str, fields: List[Dict], 
                                 primary_keys: List[str], foreign_keys: List[Dict]) -> str:
        """生成表描述"""
        # 基于表名推断业务含义
        business_meaning = self._infer_business_meaning(table_name)
        
        # 统计字段类型
        field_types = {}
        for field in fields:
            field_type = field['type_chinese']
            field_types[field_type] = field_types.get(field_type, 0) + 1
        
        # 生成描述
        description = f"{business_meaning}，包含{len(fields)}个字段。"
        
        if primary_keys:
            description += f"主键为：{', '.join(primary_keys)}。"
        
        if foreign_keys:
            fk_info = []
            for fk in foreign_keys:
                fk_info.append(f"{fk['field']}关联{fk['references_table']}")
            description += f"包含外键关系：{', '.join(fk_info)}。"
        
        # 主要字段类型
        main_types = [f"{k}({v}个)" for k, v in field_types.items() if v > 0]
        if main_types:
            description += f"主要字段类型：{', '.join(main_types)}。"
        
        return description
    
    def _infer_business_meaning(self, table_name: str) -> str:
        """基于表名推断业务含义"""
        table_lower = table_name.lower()
        
        # 客户相关
        if 'cust' in table_lower or 'customer' in table_lower:
            return "客户信息表"
        elif 'user' in table_lower:
            return "用户信息表"
        
        # 产品相关
        elif 'prod' in table_lower or 'product' in table_lower:
            return "产品信息表"
        
        # 账户相关
        elif 'account' in table_lower or 'acct' in table_lower:
            return "账户信息表"
        elif 'deposit' in table_lower:
            return "存款业务表"
        elif 'loan' in table_lower:
            return "贷款业务表"
        
        # 交易相关
        elif 'trans' in table_lower or 'transaction' in table_lower:
            return "交易记录表"
        elif 'order' in table_lower:
            return "订单信息表"
        
        # 时间相关
        elif 'log' in table_lower:
            return "日志记录表"
        elif 'history' in table_lower:
            return "历史记录表"
        
        # 配置相关
        elif 'config' in table_lower or 'setting' in table_lower:
            return "配置信息表"
        
        else:
            return f"{table_name}数据表"
    
    def analyze_field_semantics(self, table_name: str, field_name: str) -> Dict[str, Any]:
        """
        分析字段语义
        
        Args:
            table_name: 表名
            field_name: 字段名
            
        Returns:
            字段语义分析结果
        """
        try:
            # 获取字段信息
            schema = db_manager.get_table_schema(table_name)
            field_info = None
            
            for field in schema:
                if field['column_name'] == field_name:
                    field_info = field
                    break
            
            if not field_info:
                return {
                    'success': False,
                    'error': f'字段 {field_name} 在表 {table_name} 中不存在'
                }
            
            # 分析字段语义
            semantics = self._analyze_field_name_semantics(field_name)
            
            # 分析数据类型语义
            type_semantics = self._analyze_data_type_semantics(field_info['data_type'])
            
            # 分析字段用途
            usage = self._infer_field_usage(field_name, field_info['column_comment'])
            
            return {
                'success': True,
                'field_name': field_name,
                'table_name': table_name,
                'data_type': field_info['data_type'],
                'chinese_type': self._field_type_mapping.get(field_info['data_type'].upper(), field_info['data_type']),
                'semantics': semantics,
                'type_semantics': type_semantics,
                'usage': usage,
                'comment': field_info['column_comment'] or '',
                'nullable': field_info['is_nullable'] == 'YES',
                'is_primary_key': field_info['column_key'] == 'PRI'
            }
            
        except Exception as e:
            self.logger.error(f"分析字段语义失败: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def _analyze_field_name_semantics(self, field_name: str) -> Dict[str, str]:
        """分析字段名语义"""
        field_lower = field_name.lower()
        
        semantics = {
            'category': '',
            'meaning': '',
            'format': ''
        }
        
        # 分类分析
        if any(keyword in field_lower for keyword in ['id', 'no', 'code', 'key']):
            semantics['category'] = '标识符'
        elif any(keyword in field_lower for keyword in ['name', 'nm', 'title']):
            semantics['category'] = '名称'
        elif any(keyword in field_lower for keyword in ['amt', 'amount', 'bal', 'balance']):
            semantics['category'] = '金额'
        elif any(keyword in field_lower for keyword in ['date', 'time', 'dt', 'tm']):
            semantics['category'] = '时间'
        elif any(keyword in field_lower for keyword in ['phone', 'mobile', 'tel']):
            semantics['category'] = '联系方式'
        elif any(keyword in field_lower for keyword in ['addr', 'address']):
            semantics['category'] = '地址'
        elif any(keyword in field_lower for keyword in ['status', 'state', 'flag']):
            semantics['category'] = '状态'
        else:
            semantics['category'] = '其他'
        
        # 具体含义
        if 'cust' in field_lower or 'customer' in field_lower:
            semantics['meaning'] = '客户'
        elif 'prod' in field_lower or 'product' in field_lower:
            semantics['meaning'] = '产品'
        elif 'acct' in field_lower or 'account' in field_lower:
            semantics['meaning'] = '账户'
        elif 'loan' in field_lower:
            semantics['meaning'] = '贷款'
        elif 'deposit' in field_lower:
            semantics['meaning'] = '存款'
        
        # 格式分析
        if field_lower.endswith('_no') or field_lower.endswith('_num'):
            semantics['format'] = '编号'
        elif field_lower.endswith('_cd') or field_lower.endswith('_code'):
            semantics['format'] = '代码'
        elif field_lower.endswith('_nm') or field_lower.endswith('_name'):
            semantics['format'] = '名称'
        elif field_lower.endswith('_dt') or field_lower.endswith('_date'):
            semantics['format'] = '日期'
        elif field_lower.endswith('_tm') or field_lower.endswith('_time'):
            semantics['format'] = '时间'
        
        return semantics
    
    def _analyze_data_type_semantics(self, data_type: str) -> Dict[str, str]:
        """分析数据类型语义"""
        type_upper = data_type.upper()
        
        semantics = {
            'category': '',
            'usage': '',
            'constraints': []
        }
        
        if 'VARCHAR' in type_upper or 'TEXT' in type_upper:
            semantics['category'] = '文本类型'
            semantics['usage'] = '存储字符串信息'
            semantics['constraints'] = ['长度限制']
        elif 'INT' in type_upper or 'BIGINT' in type_upper:
            semantics['category'] = '整数类型'
            semantics['usage'] = '存储数值信息'
            semantics['constraints'] = ['数值范围']
        elif 'DECIMAL' in type_upper or 'FLOAT' in type_upper or 'DOUBLE' in type_upper:
            semantics['category'] = '小数类型'
            semantics['usage'] = '存储精确数值'
            semantics['constraints'] = ['精度限制', '范围限制']
        elif 'DATE' in type_upper or 'DATETIME' in type_upper or 'TIMESTAMP' in type_upper:
            semantics['category'] = '时间类型'
            semantics['usage'] = '存储时间信息'
            semantics['constraints'] = ['格式限制']
        elif 'BOOLEAN' in type_upper or 'TINYINT' in type_upper:
            semantics['category'] = '布尔类型'
            semantics['usage'] = '存储是/否信息'
            semantics['constraints'] = ['值域限制']
        
        return semantics
    
    def _infer_field_usage(self, field_name: str, comment: str) -> str:
        """推断字段用途"""
        field_lower = field_name.lower()
        
        # 基于字段名推断
        if 'id' in field_lower and 'cust' in field_lower:
            return '客户唯一标识'
        elif 'name' in field_lower and 'cust' in field_lower:
            return '客户姓名'
        elif 'mobile' in field_lower or 'phone' in field_lower:
            return '联系电话'
        elif 'status' in field_lower:
            return '状态标识'
        elif 'create' in field_lower and 'time' in field_lower:
            return '创建时间'
        elif 'update' in field_lower and 'time' in field_lower:
            return '更新时间'
        
        # 基于注释推断
        if comment:
            return comment
        
        return '通用数据字段'

# 全局资产理解实例
asset_understanding = AssetUnderstanding()