"""
测试修改后的Customer Segmentation智能体
"""
import os
import sys
import logging

# 设置日志级别
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def test_customer_segmentation():
    """
    测试Customer Segmentation智能体
    """
    try:
        logger.info("开始测试Customer Segmentation智能体...")
        
        # 导入Customer Segmentation智能体
        from huaxiang.CustomerSegmentation import CustomerSegmentationLangGraph
        
        logger.info("成功导入CustomerSegmentationLangGraph")
        
        # 初始化智能体
        agent = CustomerSegmentationLangGraph()
        
        logger.info("成功初始化CustomerSegmentationLangGraph")
        
        # 检查Text2SQLProcessor是否正确初始化
        if hasattr(agent, 'text2sql_processor') and agent.text2sql_processor is not None:
            logger.info("Text2SQLProcessor成功初始化")
        else:
            logger.warning("Text2SQLProcessor未初始化，但智能体仍可使用备用方法")
        
        # 准备一个简单的测试查询
        test_query = "浙江省杭州分行的存款金额与浙江省其他城市分行的存款金额对比"
        
        logger.info(f"执行测试查询: {test_query}")
        
        # 执行查询处理（使用try-except避免实际执行时出错）
        try:
            # 检查模型是否初始化成功
            if not hasattr(agent, 'model') or agent.model is None:
                logger.warning("语言模型未成功初始化，可能无法执行查询")
                return False
            
            # 检查数据库连接
            if not hasattr(agent, 'db') or agent.db is None:
                logger.warning("数据库连接未成功初始化，将使用模拟模式")
            
            logger.info("开始执行查询处理...")
            result = agent.process_query(test_query)
            
            if result:
                logger.info(f"查询处理成功，返回结果类型: {type(result)}")
                # 检查结果中是否包含必要字段
                required_fields = ['explanation', 'success']
                for field in required_fields:
                    if field not in result:
                        logger.warning(f"返回结果中缺少必要字段: {field}")
            else:
                logger.warning("查询处理返回空结果")
            
        except Exception as e:
            logger.error(f"执行查询时发生异常: {e}", exc_info=True)
            print(f"\n执行查询时发生异常: {str(e)}")
            print("\n错误详情:")
            import traceback
            traceback.print_exc()
            print("\n请检查以下几点:")
            print("1. API密钥是否正确设置在环境变量中")
            print("2. 数据库连接是否可用")
            print("3. 模型名称和API基础URL是否正确")
        
        logger.info("测试完成")
        return True
    
    except ImportError as e:
        logger.error(f"导入错误: {e}")
        print(f"导入错误: {e}")
        return False
    except Exception as e:
        logger.error(f"测试过程中发生异常: {e}")
        print(f"测试过程中发生异常: {e}")
        return False

if __name__ == "__main__":
    test_customer_segmentation()
