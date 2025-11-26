"""
多智能体数据查询系统 - 主启动文件
提供命令行交互界面
"""
import os
import sys
import logging
from datetime import datetime
from typing import Dict, Any

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from base_agent.agent_manager import agent_manager
from config import DATABASE_CONFIG

# 使用封装好的日志模块
from logger_set import get_logger

# 获取run.py的日志记录器
logger = get_logger(__name__)
logger.info("使用封装的日志系统初始化完成")

class MultiAgentCLI:
    """多智能体系统命令行界面"""
    
    def __init__(self):
        """初始化CLI"""
        self.session_id = f"cli_session_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        self.running = True
        
        # 创建会话
        agent_manager.create_session(self.session_id)
        
        print("=" * 60)
        print("🤖 多智能体数据查询系统")
        print("=" * 60)
        print("欢迎使用多智能体数据查询系统！")
        print("我可以帮您：")
        print("• 自然语言查询数据库")
        print("• 生成SQL代码")
        print("• 分析数据结构")
        print("• 提供查询建议")
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
            • status - 查看系统状态
            • history - 查看对话历史
            • clear - 清空对话历史
            • tables - 查看所有数据表
            • table <表名> - 查看表结构
            • quit/exit - 退出系统

            💡 提示：
            • 使用自然语言描述您的查询需求
            • 支持多轮对话，可以基于上下文提问
            • 系统会自动生成并执行SQL查询
        """
        print(help_text)
    
    def display_status(self):
        """显示系统状态"""
        try:
            status = agent_manager.get_agent_status()
            
            if status['success']:
                print("\n系统状态")
                print("-" * 30)
                print(f"管理器状态: {status['manager']['status']}")
                print(f"活跃会话数: {status['manager']['sessions']}")
                print(f"数据库连接: {'✅ 已连接' if status['database']['connected'] else '❌ 未连接'}")
                
                print("\n智能体状态:")
                for category, agents in status['agents'].items():
                    print(f"  {category}:")
                    for agent_name, agent_status in agents.items():
                        print(f"    • {agent_name}: ✅ {agent_status}")
                print("-" * 30)
            else:
                print(f"❌ 获取状态失败: {status['error']}")
                
        except Exception as e:
            print(f"❌ 状态查询错误: {e}")
    
    def display_history(self):
        """显示对话历史"""
        try:
            history = agent_manager.get_conversation_history(self.session_id)
            
            if history['success']:
                print(f"\n对话历史 (会话: {self.session_id})")
                print("-" * 50)
                
                if not history['history']:
                    print("暂无对话历史")
                else:
                    for i, msg in enumerate(history['history'], 1):
                        role = "👤 用户" if msg['role'] == 'user' else "🤖 助手"
                        timestamp = msg['timestamp'][:19]  # 只显示到秒
                        print(f"{i}. [{timestamp}] {role}")
                        print(f"   {msg['content'][:100]}{'...' if len(msg['content']) > 100 else ''}")
                        print()
                
                print("-" * 50)
            else:
                print(f"❌ 获取历史失败: {history['error']}")
                
        except Exception as e:
            print(f"❌ 历史查询错误: {e}")
    
    def clear_history(self):
        """清空对话历史"""
        try:
            result = agent_manager.clear_conversation(self.session_id)
            
            if result['success']:
                print("✅ 对话历史已清空")
            else:
                print(f"❌ 清空失败: {result['error']}")
                
        except Exception as e:
            print(f"❌ 清空错误: {e}")
    
   
        
    
    def process_text2sql_query(self, query: str):
        """处理Text2SQL查询"""
        print(f"🔍 使用Text2SQL处理查询: {query}")
        
        try:
            result = agent_manager.process_text2sql_query(query, self.session_id)
            self.display_text2sql_result(result)
        except Exception as e:
            print(f"❌ Text2SQL查询处理失败: {e}")
    
    def display_text2sql_result(self, result: Dict[str, Any]):
        """显示Text2SQL查询结果"""
        if not result.get('success'):
            print(f"❌ 查询失败: {result.get('error', '未知错误')}")
            return
        
        print(f"\n✅ 查询成功 (使用 {result.get('agent', 'text2sql')} 智能体)")
        
        # 显示生成的SQL
        if 'sql_query' in result and not ('generated_sql' in result and result['sql_query'] == result['generated_sql']):
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
        
        # 显示元数据
        metadata = result.get('metadata', {})
        if metadata:
            print(f"\n🔧 查询元数据:")
            print(f"  模型: {metadata.get('model', 'unknown')}")
            print(f"  数据库: {metadata.get('db_uri', 'unknown')}")
    
    def process_query(self, query: str):
        """处理用户查询"""
        try:
            print(f"\n🔍 正在处理查询: {query}")
            print("⏳ 请稍候...")
            
            result = agent_manager.process_query(query, self.session_id)
            
            if result['success']:
                self.display_result(result)
            else:
                print(f"❌ 查询处理失败: {result['error']}")
                
        except Exception as e:
            print(f"❌ 查询处理错误: {e}")
    
    def _normalize_result(self, result: Dict[str, Any]) -> Dict[str, Any]:
        """标准化结果格式，统一字段名"""
        # 统一SQL字段名
        if 'sql_query' in result:
            result['generated_sql'] = result['sql_query']
        
        # 统一执行结果字段名
        if 'execution_result' in result:
            result['execution'] = result['execution_result']
        
        # 确保有intent字段
        if 'intent' not in result and 'agent' in result:
            if result['agent'] == 'text2sql':
                result['intent'] = 'data_retrieval'
        
        return result

    def display_result(self, result: Dict[str, Any]):
        """显示查询结果"""
        # 标准化结果格式
        result = self._normalize_result(result)
        
        print("\n✅ 查询成功!")
        print("-" * 50)
        
        # 显示意图类型
        intent_map = {
            'data_query': '数据查询',
            'data_retrieval': ' 数据查询',
            'metadata_query': ' 元数据查询',
            'table_info': '表信息查询',
            'schema_query': '模式查询',
            'conversation': '对话查询',
            'general': '通用查询'
        }
        
        intent_type = result.get('intent', 'unknown')
        print(f"查询类型: {intent_map.get(intent_type, intent_type)}")
        
        # 显示回复消息（用于对话查询）
        if result.get('message'):
            print(f"\n回复:")
            print(result['message'])
        
        # 显示解释（Text2SQL返回的解释）
        if result.get('explanation'):
            print(f"\n查询解释:")
            # 仅记录简要信息，避免重复输出详细内容
            logger.info(f"{result['explanation']},查询成功完成，已显示解释结果")
        
        # 显示生成的SQL
        if result.get('generated_sql'):
            print(f"\n生成的SQL:")
            print("```sql")
            print(result['generated_sql'])
            print("```")
        
        # 显示SQL解释
        if result.get('sql_explanation'):
            print(f"\n SQL解释:")
            print(result['sql_explanation'])
        
        # 显示执行结果
        if result.get('execution'):
            self._display_execution_result(result['execution'])
        
        # 显示替代方案
        if result.get('alternatives'):
            print(f"\n替代方案:")
            for i, alt in enumerate(result['alternatives'], 1):
                print(f"{i}. {alt.get('description', '无描述')}")
                print(f"   SQL: {alt.get('query', '无SQL')}")
        
        # 显示元数据查询结果
        if result.get('type') == 'table_analysis' and result.get('results'):
            print(f"\n表分析结果:")
            for table_result in result['results']:
                print(f"• 表名: {table_result['table_name']}")
                print(f"  字段数: {len(table_result['fields'])}")
        
        # 显示推荐结果
        if result.get('type') == 'recommendations' and result.get('recommendations'):
            print(f"\n推荐表:")
            for rec in result['recommendations'][:10]:
                print(f"• {rec}")
        
        # 显示建议
        if result.get('suggestions'):
            print(f"\n查询建议:")
            for suggestion in result['suggestions']:
                print(f"• {suggestion}")
        
        print("-" * 50)
    
    def _display_execution_result(self, execution: Dict[str, Any]):
        """显示SQL执行结果"""
        if execution['success']:
            # 优先获取data字段
            data = execution.get('data', [])
            
            # 如果data为空，但execution_result中有数据，尝试从中获取
            if not data and 'execution_result' in execution and isinstance(execution['execution_result'], dict):
                data = execution['execution_result'].get('data', [])
            
            # 打印执行结果的详细信息用于调试
            print(f"\n查询结果 (共 {len(data)} 行):")
            
            if data:
                self._display_data_table(data)
            else:
                # 检查是否有其他可能的数据来源
                print("查询结果为空")
                # 如果有row_count字段但data为空，显示这个信息
                if 'row_count' in execution and execution['row_count'] > 0:
                    print(f"注意: 虽然data为空，但row_count显示有{execution['row_count']}行数据")
        else:
            print(f"\n❌ 执行失败: {execution.get('error', '未知错误')}")
    
    def _display_data_table(self, data: list[Any]):
        """显示数据表格"""
        if not data:
            return
        
        # 显示表头
        if isinstance(data[0], dict):
            headers = list(data[0].keys())
            if headers:
                print(" | ".join(f"{header:<12}" for header in headers))
                print("-" * (13 * len(headers)))
                
                # 显示前10行数据
                for row in data[:10]:
                    print(" | ".join(f"{str(value)[:12]:<12}" for value in row.values()))
        else:
            # 如果不是字典格式，直接显示
            for i, row in enumerate(data[:10], 1):
                print(f"{i}. {row}")
        
        if len(data) > 10:
            print(f"... 还有 {len(data) - 10} 行数据")
    
    def handle_command(self, user_input: str) -> bool:
        """处理命令"""
        user_input = user_input.strip()
        
        if not user_input:
            return True
        
        # 解析命令和参数
        parts = user_input.split()
        command = parts[0].lower() if parts else ''
        args = parts[1:] if len(parts) > 1 else []
        
        # 检查是否为命令
        if user_input.lower() in ['quit', 'exit', 'q']:
            print("\n👋 感谢使用，再见！")
            return False
        
        elif user_input.lower() == 'help':
            self.display_help()
        
        elif user_input.lower() == 'status':
            self.display_status()
        
        elif user_input.lower() == 'history':
            self.display_history()
        
        elif user_input.lower() == 'clear':
            self.clear_history()
        
       
        
        else:
            # 处理查询
            self.process_query(user_input)
        
        return True
    
    def run(self):
        """运行CLI"""
        try:
            # 检查数据库连接
            print("🔗 检查数据库连接...")
            status = agent_manager.get_agent_status()
            if not status['database']['connected']:
                print("⚠️ 警告: 数据库连接失败，请检查配置")
            else:
                print("✅ 数据库连接正常")
            
            print("\n💬 请输入您的查询或命令:")
            
            while self.running:
                try:
                    user_input = input("\n> ").strip()
                    self.running = self.handle_command(user_input)
                    
                except KeyboardInterrupt:
                    print("\n\n👋 检测到中断信号，正在退出...")
                    break
                except EOFError:
                    print("\n\n👋 输入结束，正在退出...")
                    break
                    
        except Exception as e:
            logger.error(f"CLI运行错误: {e}")
            print(f"❌ 系统错误: {e}")

def main():
    """主函数"""
    try:
        # 检查配置
        if not DATABASE_CONFIG:
            print("❌ 数据库配置错误，请检查config.py文件")
            return
        
        # 启动CLI
        cli = MultiAgentCLI()
        cli.run()
        
    except Exception as e:
        logger.error(f"系统启动失败: {e}")
        print(f"❌ 系统启动失败: {e}")

if __name__ == "__main__":
    main()