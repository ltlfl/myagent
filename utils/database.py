"""
数据库连接和操作工具类
"""
import pymysql
import logging
from typing import List, Dict, Any, Optional
from contextlib import contextmanager
from config import DATABASE_CONFIG

class DatabaseManager:
    """数据库管理器"""
    
    def __init__(self):
        self.config = DATABASE_CONFIG
        self.logger = logging.getLogger(__name__)
    
    @contextmanager
    def get_connection(self):
        """获取数据库连接的上下文管理器"""
        connection = None
        try:
            connection = pymysql.connect(**self.config)
            yield connection
        except Exception as e:
            self.logger.error(f"数据库连接错误: {e}")
            raise
        finally:
            if connection:
                connection.close()
    
    def execute_query(self, query: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
        """执行查询语句"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor(pymysql.cursors.DictCursor)
                cursor.execute(query, params)
                result = cursor.fetchall()
                cursor.close()
                return result
        except Exception as e:
            self.logger.error(f"查询执行错误: {e}")
            raise
    
    def execute_update(self, query: str, params: Optional[tuple] = None) -> int:
        """执行更新语句"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(query, params)
                affected_rows = cursor.rowcount
                conn.commit()
                cursor.close()
                return affected_rows
        except Exception as e:
            self.logger.error(f"更新执行错误: {e}")
            conn.rollback()
            raise
    
    def get_table_schema(self, table_name: str) -> List[Dict[str, Any]]:
        """获取表结构信息"""
        query = """
        SELECT 
            COLUMN_NAME as column_name,
            DATA_TYPE as data_type,
            IS_NULLABLE as is_nullable,
            COLUMN_KEY as column_key,
            COLUMN_DEFAULT as column_default,
            COLUMN_COMMENT as column_comment
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
        ORDER BY ORDINAL_POSITION
        """
        return self.execute_query(query, (self.config['database'], table_name))
    
    def get_all_tables(self) -> List[str]:
        """获取数据库中所有表名"""
        query = """
        SELECT TABLE_NAME 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = %s AND TABLE_TYPE = 'BASE TABLE'
        """
        result = self.execute_query(query, (self.config['database'],))
        return [row['TABLE_NAME'] for row in result]
    
    def get_table_relationships(self) -> List[Dict[str, Any]]:
        """获取表关系信息"""
        query = """
        SELECT 
            TABLE_NAME as table_name,
            COLUMN_NAME as column_name,
            REFERENCED_TABLE_NAME as referenced_table_name,
            REFERENCED_COLUMN_NAME as referenced_column_name,
            CONSTRAINT_NAME as constraint_name
        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE 
        WHERE TABLE_SCHEMA = %s 
        AND REFERENCED_TABLE_NAME IS NOT NULL
        """
        return self.execute_query(query, (self.config['database'],))

# 全局数据库管理器实例
db_manager = DatabaseManager()