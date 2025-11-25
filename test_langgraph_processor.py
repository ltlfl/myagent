"""
LangGraph版Text2SQL处理器 - 命令行交互界面
基于run.py架构，专注于Text2SQLProcessorLangGraph功能
"""
import os
import sys
from datetime import datetime
from typing import Dict, Any, List, Optional

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# 加载环境变量
try:
    from dotenv import load_dotenv
    # 加载当前目录和text2sql_module目录的.env文件
    load_dotenv()
    module_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'text2sql_module')
    env_path = os.path.join(module_dir, '.env')
    if os.path.exists(env_path):
        load_dotenv(dotenv_path=env_path)
except ImportError:
    pass  # 静默处理，不在日志中打印

# 使用统一的日志模块
from logger_set import get_logger
logger = get_logger(__name__)

class LangGraphText2SQLCLI:
    """基于LangGraph的Text2SQL命令行界面"""
    
    def __init__(self):
        """初始化CLI，创建会话并加载处理器"""
        self.session_id = f"cli_session_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        self.running = True
        self.processor = None
        self.conversation_history: List[Dict[str, str]] = []
        
        # 初始化Text2SQL处理器
        self._init_processor()
        
        # 显示欢迎信息
        self._display_welcome()
    
    def _init_processor(self):
        """初始化Text2SQL处理器"""
        try:
            # 延迟导入以避免模块级别错误
            from text2sql_module.text2sql_processor_langgraph import Text2SQLProcessorLangGraph
            self.processor = Text2SQLProcessorLangGraph()
        except Exception as e:
            logger.error(f"初始化处理器失败: {e}")
            self.processor = None
    
    def _display_welcome(self):
        """显示欢迎信息"""
        print("=" * 60)
        print("🤖 LangGraph Text2SQL 查询系统")
        print("=" * 60)
        print("欢迎使用基于LangGraph的Text2SQL查询系统！")
        print("我可以帮您通过自然语言查询数据库")
        print("\n输入 'help' 查看帮助，输入 'q' 或 'exit' 退出")
        print("=" * 60)
    
    def display_help(self):
        """显示帮助信息"""
        help_text = """
            📖 帮助信息

            🔍 查询示例：
            • "查询所有客户信息"
            • "统计订单数量"
            • "查找最近30天的销售数据"
            • "查询辽宁省客户数量"

            🛠️ 命令：
            • help - 显示帮助信息
            • history - 查看对话历史
            • clear - 清空对话历史
            • tables - 查看所有数据表
            • quit/exit/q - 退出系统

            💡 提示：
            • 使用自然语言描述您的查询需求
            • 支持多轮对话，可以基于上下文提问
            • 系统会自动生成并执行SQL查询
        """
        print(help_text)
    
    def display_history(self):
        """显示对话历史"""
        if not self.conversation_history:
            print("\n暂无对话历史")
            return
        
        print(f"\n对话历史 (会话: {self.session_id})")
        print("-" * 50)
        
        for i, msg in enumerate(self.conversation_history, 1):
            role = "👤 用户" if msg['role'] == 'user' else "🤖 助手"
            print(f"{i}. {role}")
            print(f"   {msg['content'][:100]}{'...' if len(msg['content']) > 100 else ''}")
            print()
        
        print("-" * 50)
    
    def clear_history(self):
        """清空对话历史"""
        self.conversation_history = []
        print("✅ 对话历史已清空")
    
    def display_tables(self):
        """显示所有数据表"""
        if not self.processor:
            print("❌ 处理器未初始化，无法获取表信息")
            return
        
        try:
            table_info = self.processor.get_table_info()
            
            if table_info.get('success'):
                print(f"\n数据库中共有 {table_info.get('table_count', 0)} 个表")
                print("表名列表:")
                for i, table in enumerate(table_info.get('tables', []), 1):
                    print(f"  {i}. {table}")
            else:
                print(f"❌ 获取表信息失败: {table_info.get('error', '未知错误')}")
        except Exception as e:
            logger.error(f"获取表信息错误: {e}")
            print(f"❌ 获取表信息时发生错误: {str(e)}")
    
    def process_text2sql_query(self, query: str):
        """处理Text2SQL查询"""
        if not self.processor:
            print("❌ 处理器未初始化，无法执行查询")
            return
        
        # 添加用户查询到会话历史
        self.conversation_history.append({
            'role': 'user',
            'content': query,
            'timestamp': datetime.now().isoformat()
        })
        
        try:
            # 执行查询
            result = self.processor.process_query(
                question=query,
                session_id=self.session_id,
                entities=None,
                conversation_history=self.conversation_history[:-1]  # 不包含当前查询
            )
            
            # 显示结果
            self.display_text2sql_result(result)
            
            # 如果查询成功，添加响应到历史
            if result.get('success'):
                response_content = result.get('explanation', '查询成功')
                self.conversation_history.append({
                    'role': 'assistant',
                    'content': response_content,
                    'timestamp': datetime.now().isoformat()
                })
                
        except Exception as e:
            logger.error(f"查询处理失败: {e}")
            print(f"❌ 查询处理失败: {str(e)}")
    
    def display_text2sql_result(self, result: Dict[str, Any]):
        """显示Text2SQL查询结果"""
        if not result.get('success'):
            print(f"❌ 查询失败: {result.get('error', '未知错误')}")
            return
        
        print(f"\n✅ 查询成功")
        
        # 显示生成的SQL
        if 'sql_query' in result:
            print(f"\n📝 生成的SQL:")
            print(f"```sql")
            print(result['sql_query'])
            print(f"```")
        
        # 显示查询结果统计
        row_count = result.get('row_count', 0)
        print(f"\n📊 查询结果: {row_count} 行数据")
        
        # 显示部分查询结果
        if row_count > 0:
            execution_result = result.get('execution_result', {})
            data = execution_result.get('data', [])
            
            if data:
                print(f"\n📋 数据预览 (最多显示10行):")
                for i, row in enumerate(data[:10]):
                    print(f"  {i+1}: {row}")
                
                if row_count > 10:
                    print(f"  ... 还有 {row_count - 10} 行数据")
        
        # 显示自然语言解释
        if 'explanation' in result:
            print(f"\n💬 结果说明:")
            print(result['explanation'])
    
    def process_input(self, user_input: str):
        """处理用户输入"""
        user_input = user_input.strip()
        
        if not user_input:
            return
        
        # 处理命令
        if user_input.lower() in ['exit', 'quit', 'q']:
            self.running = False
            print("\n👋 感谢使用，再见！")
        elif user_input.lower() == 'help':
            self.display_help()
        elif user_input.lower() == 'history':
            self.display_history()
        elif user_input.lower() == 'clear':
            self.clear_history()
        elif user_input.lower() == 'tables':
            self.display_tables()
        else:
            # 处理为查询
            self.process_text2sql_query(user_input)
    
    def run(self):
        """运行CLI交互循环"""
        while self.running:
            try:
                user_input = input("\n💬 请输入您的查询: ")
                self.process_input(user_input)
            except KeyboardInterrupt:
                print("\n\n👋 感谢使用，再见！")
                self.running = False
            except Exception as e:
                logger.error(f"交互错误: {e}")
                print(f"❌ 发生错误: {str(e)}")


def main():
    """主函数"""
    try:
        cli = LangGraphText2SQLCLI()
        cli.run()
    except Exception as e:
        logger.error(f"程序执行失败: {e}")
        print(f"❌ 程序执行失败: {str(e)}")
        sys.exit(1)
    sys.exit(0)


# 运行CLI
if __name__ == "__main__":
    # 加载环境变量
    load_dotenv()
    
    # # 配置日志
    # configure_logging()
    
    # 启动LangGraph Text2SQL CLI
    print("欢迎使用LangGraph Text2SQL处理器\n")
    print("输入 'help' 获取可用命令，输入 'exit' 退出程序\n")
    
    try:
        cli = LangGraphText2SQLCLI()
        cli.run()
    except KeyboardInterrupt:
        print("\n程序已被用户中断")
    except Exception as e:
        print(f"\n程序运行出错: {e}")
