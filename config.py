"""
多智能体系统配置文件
"""
import os

# 数据库配置
DATABASE_CONFIG = {
    'host': 'localhost',
    'port': 3306,
    'user': 'root',
    'password': '123456',
    'database': 'mysql2',
    'charset': 'utf8mb4'
}

# 智能体配置
AGENT_CONFIG = {
    'base_agent': {
        'name': '基础智能体',
        'description': '提供基础数据操作能力',
        'enabled_tools': ['sql_executor', 'python_engine', 'api_client']
    },
    'metadata_agent': {
        'name': '元数据智能体',
        'description': '数据资产理解与血缘分析',
        'enabled_tools': ['asset_understanding', 'data_recommender']
    },
    'query_agent': {
        'name': '查询智能体',
        'description': '自然语言查询转换',
        'enabled_tools': ['intent_parser', 'code_generator']
    }
}

# API配置
API_CONFIG = {
    'host': 'localhost',
    'port': 8000,
    'debug': True,
    'cors_origins': ['*']
}

# 日志配置
LOGGING_CONFIG = {
    'level': 'INFO',
    'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    'file_path': 'logs/agent_system.log'
}

# 工具配置
TOOL_CONFIG = {
    'sql_executor': {
        'max_rows': 1000,
        'timeout': 30
    },
    'python_engine': {
        'max_execution_time': 60,
        'allowed_modules': ['pandas', 'numpy', 'matplotlib', 'seaborn']
    },
    'api_client': {
        'timeout': 30,
        'max_retries': 3
    }
}