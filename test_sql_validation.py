#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试SQL验证功能，确保修改后的validate_sql方法能正确处理用户画像写入操作
"""

import os
import sys
from loguru import logger

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from text2sql_module.huaxiang_processor import Text2SQLProcessor
from utils.llm_client import LLMClient

# 配置日志
logger.remove()
logger.add(sys.stdout, level="INFO")
logger.add("test_validation.log", level="DEBUG")

def test_sql_validation():
    """
    测试SQL验证功能
    """
    logger.info("开始测试SQL验证功能")
    
    try:
        # 初始化处理器（仅用于验证功能，不需要真实数据库连接）
        # 使用最小配置初始化，避免实际连接数据库
        llm_client = LLMClient()
        # 直接使用LLMClient的model属性
        processor = Text2SQLProcessor(
            model=llm_client.model,
            db_url="sqlite:///:memory:"
        )
        
        # 测试只读查询
        test_queries = [
            # 只读查询测试
            {"sql": "SELECT * FROM customer_info", "allow_write": False, "expected_valid": True},
            {"sql": "DESCRIBE customer_info", "allow_write": False, "expected_valid": True},
            {"sql": "SHOW TABLES", "allow_write": False, "expected_valid": True},
            
            # 写入查询测试（只读模式）
            {"sql": "INSERT INTO customer_info VALUES (1, 'Test')", "allow_write": False, "expected_valid": False},
            {"sql": "CREATE TABLE test_table (id INT)", "allow_write": False, "expected_valid": False},
            {"sql": "UPDATE customer_info SET name = 'Test'", "allow_write": False, "expected_valid": False},
            
            # 写入查询测试（写入模式）
            {"sql": "CREATE TABLE customer_profile (CUST_NO VARCHAR(20) PRIMARY KEY, CUST_NAM VARCHAR(100), PROFILE_VALUE VARCHAR(200))", "allow_write": True, "expected_valid": True},
            {"sql": "INSERT INTO customer_profile VALUES ('1001', '张三', '高价值客户')", "allow_write": True, "expected_valid": True},
            {"sql": "INSERT IGNORE INTO customer_profile (CUST_NO, CUST_NAM, PROFILE_VALUE) SELECT CUST_NO, CUST_NAM, '高价值客户' FROM customer_info", "allow_write": True, "expected_valid": True},
            
            # 高风险操作测试（写入模式下也应禁止）
            {"sql": "DROP TABLE customer_info", "allow_write": True, "expected_valid": False},
            {"sql": "TRUNCATE TABLE customer_info", "allow_write": True, "expected_valid": False},
            {"sql": "ALTER TABLE customer_info ADD COLUMN test INT", "allow_write": True, "expected_valid": False},
            
            # SQL注入测试
            {"sql": "SELECT * FROM customer_info WHERE name = 'admin' -- ' OR 1=1", "allow_write": False, "expected_valid": False},
            {"sql": "SELECT * FROM customer_info; DROP TABLE users", "allow_write": False, "expected_valid": False},
            {"sql": "SELECT * FROM customer_info; DROP TABLE users", "allow_write": True, "expected_valid": False},
        ]
        
        # 运行测试
        passed_tests = 0
        failed_tests = 0
        
        for i, test in enumerate(test_queries, 1):
            result = processor.validate_sql(test["sql"], allow_write=test["allow_write"])
            is_pass = result["valid"] == test["expected_valid"]
            
            if is_pass:
                passed_tests += 1
                logger.info(f"测试 {i} 通过: SQL='{test['sql'][:50]}...', allow_write={test['allow_write']}, 结果={result['valid']}")
            else:
                failed_tests += 1
                logger.error(f"测试 {i} 失败: SQL='{test['sql'][:50]}...', allow_write={test['allow_write']}, 期望={test['expected_valid']}, 实际={result['valid']}, 错误信息={result.get('error')}")
        
        # 输出测试结果摘要
        logger.info(f"\n测试完成: 通过 {passed_tests}, 失败 {failed_tests}, 总计 {len(test_queries)}")
        
        # 测试画像生成的典型SQL
        profile_sql = """
        CREATE TABLE IF NOT EXISTS customer_age_profile (CUST_NO VARCHAR(20) PRIMARY KEY, CUST_NAM VARCHAR(100), PROFILE_VALUE VARCHAR(200));
        
        INSERT IGNORE INTO customer_age_profile (CUST_NO, CUST_NAM, PROFILE_VALUE)
        SELECT CUST_NO, CUST_NAM, '年轻客户(18-30岁)' as PROFILE_VALUE
        FROM customer_info
        WHERE age >= 18 AND age <= 30;
        """
        
        logger.info("\n测试画像生成的典型SQL语句")
        # 测试只读模式
        result_readonly = processor.validate_sql(profile_sql, allow_write=False)
        logger.info(f"只读模式验证结果: {result_readonly}")
        
        # 测试写入模式
        result_write = processor.validate_sql(profile_sql, allow_write=True)
        logger.info(f"写入模式验证结果: {result_write}")
        
        # 总结
        if failed_tests == 0:
            logger.success("所有测试通过！SQL验证功能工作正常")
        else:
            logger.warning(f"有 {failed_tests} 个测试失败，请检查代码")
            
        return failed_tests == 0
        
    except Exception as e:
        logger.error(f"测试过程中出错: {e}")
        return False

if __name__ == "__main__":
    success = test_sql_validation()
    sys.exit(0 if success else 1)
