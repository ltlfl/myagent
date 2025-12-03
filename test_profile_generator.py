"""
测试用户画像生成器
"""
import os
import sys
import json
from text2sql_module.huaxiang_processor import CustomerProfileGenerator


def test_customer_profile_generator():
    """测试用户画像生成器的主要功能"""
    print("开始测试用户画像生成器...")
    
    # 初始化画像生成器
    profile_generator = CustomerProfileGenerator()
    
    # 测试表名和列名 - 可以根据实际情况修改
    table_name = input("请输入要分析的表名 (默认: customer_info): ") or "customer_info"
    column_name = input("请输入要分析的列名 (例如: 年龄, 多产品持有率等): ")
    
    if not column_name:
        print("错误: 必须输入列名")
        return
    
    print(f"\n正在处理 {table_name}.{column_name}...")
    
    # 1. 生成画像并获取待确认的画像列表
    profiles = profile_generator.process_profile_generation(table_name, column_name)
    
    if not profiles:
        print("没有生成有效的画像")
        return
    
    print(f"\n成功生成了 {len(profiles)} 个画像标签\n")
    
    # 2. 逐个展示并获取用户确认
    for i, profile in enumerate(profiles):
        if profile['status'] != 'pending':
            print(f"[跳过] 画像 {i+1}: {profile.get('error', '未知错误')}")
            continue
        
        print(f"\n画像 {i+1}/{len(profiles)}")
        print(f"标签名称: {profile['label']['label_name']}")
        print(f"标签描述: {profile['label']['description']}")
        print(f"筛选条件: {profile['label']['condition']}")
        print(f"输出表名: {profile['output_table']}")
        print(f"\n自然语言描述:")
        print(f"{profile['natural_language']}")
        
        # 显示SQL预览（只显示前200个字符）
        sql_preview = profile['sql'][:200] + ('...' if len(profile['sql']) > 200 else '')
        print(f"\nSQL预览:")
        print(f"{sql_preview}")
        
        # 获取用户确认
        confirm = input("\n是否执行该画像生成? (y/n, 默认: n): ").lower() == 'y'
        
        # 执行或取消
        result = profile_generator.confirm_and_execute(profile, confirm)
        
        if result['status'] == 'success':
            print(f"✅ 执行成功: {result['message']}")
        elif result['status'] == 'cancelled':
            print(f"❌ 已取消: {result['message']}")
        else:
            print(f"❌ 执行失败: {result.get('error', '未知错误')}")
        
        print("-" * 50)
    
    print("\n所有画像处理完成！")


def batch_test_profile_generator():
    """批量测试多个列的画像生成"""
    print("开始批量测试用户画像生成器...")
    
    profile_generator = CustomerProfileGenerator()
    table_name = input("请输入要分析的表名 (默认: customer_info): ") or "customer_info"
    
    # 输入多个列名，用逗号分隔
    columns_input = input("请输入要批量分析的列名，用逗号分隔 (例如: 年龄,多产品持有率): ")
    columns = [col.strip() for col in columns_input.split(',') if col.strip()]
    
    if not columns:
        print("错误: 必须输入至少一个列名")
        return
    
    results_summary = []
    
    for column_name in columns:
        print(f"\n{'-' * 60}")
        print(f"处理列: {column_name}")
        print(f"{'-' * 60}")
        
        profiles = profile_generator.process_profile_generation(table_name, column_name)
        column_results = {
            'column': column_name,
            'total_profiles': len(profiles),
            'pending_profiles': [p for p in profiles if p['status'] == 'pending'],
            'failed_profiles': [p for p in profiles if p['status'] != 'pending']
        }
        
        results_summary.append(column_results)
        
        print(f"生成了 {len(profiles)} 个画像标签")
        print(f"待确认: {len(column_results['pending_profiles'])}")
        print(f"失败: {len(column_results['failed_profiles'])}")
    
    # 显示批量测试摘要
    print(f"\n{'-' * 60}")
    print("批量测试摘要")
    print(f"{'-' * 60}")
    
    for result in results_summary:
        print(f"列名: {result['column']}")
        print(f"  总画像数: {result['total_profiles']}")
        print(f"  待确认: {len(result['pending_profiles'])}")
        print(f"  失败: {len(result['failed_profiles'])}")
        
        # 如果有待确认的画像，询问是否要执行
        if result['pending_profiles']:
            confirm_all = input(f"  是否执行该列的所有待确认画像? (y/n): ").lower() == 'y'
            if confirm_all:
                for profile in result['pending_profiles']:
                    print(f"    执行: {profile['label']['description']}")
                    exec_result = profile_generator.confirm_and_execute(profile, True)
                    print(f"      {'成功' if exec_result['status'] == 'success' else '失败'}")
    
    print("\n批量测试完成！")


def main():
    """主函数"""
    print("用户画像生成器测试工具")
    print("=" * 40)
    
    while True:
        print("\n测试选项:")
        print("1. 单一列测试")
        print("2. 批量列测试")
        print("3. 退出")
        
        choice = input("请选择测试模式 (1-3): ")
        
        if choice == '1':
            test_customer_profile_generator()
        elif choice == '2':
            batch_test_profile_generator()
        elif choice == '3':
            print("感谢使用，再见！")
            break
        else:
            print("无效的选择，请重新输入")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n程序被用户中断")
    except Exception as e:
        print(f"程序出错: {e}")
        import traceback
        traceback.print_exc()
