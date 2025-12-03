# 用户画像生成器 (CustomerProfileGenerator)

## 功能概述

用户画像生成器是一个专门用于基于数据库表数据自动生成客户画像并写入新表的工具类。它能够：

1. **自动标签生成**：根据输入的表和列，智能地生成合适的画像标签
2. **SQL语句生成**：为每个画像标签生成独立的SQL语句，创建新表并插入数据
3. **SQL验证**：使用大语言模型对生成的SQL进行安全和语法验证
4. **用户确认执行**：提供自然语言描述，让用户确认是否执行每个画像的生成

## 核心特性

### 1. 智能标签划分

- **年龄字段**：自动划分为"25岁以下"、"25-35岁"、"35-50岁"、"50岁以上"四段
- **比例/评分字段**：使用四分位数自动划分为"低"、"中"、"高"三档
- **数值字段**：默认划分为"低值"和"高值"两档
- **字符串字段**：自动提取前5个最常见的值作为分类标签

### 2. 安全的数据写入

- 所有操作都在**新创建的表**中进行，不会修改任何原始表
- 每个画像标签生成一个独立的表，避免错误影响多个标签
- 使用`INSERT IGNORE`避免主键冲突
- 严格的SQL验证确保操作安全

### 3. 用户友好的交互

- 为每个画像提供自然语言描述，便于理解
- 支持单个列或批量列的画像生成
- 详细的执行日志和状态反馈

## 类结构和主要方法

### CustomerProfileGenerator 类

```python
class CustomerProfileGenerator:
    def __init__(self, db_uri=None, model_name="qwen-plus"): ...
    def generate_profile_labels(self, table_name, column_name): ...
    def generate_profile_sql(self, table_name, labels, output_table_prefix="customer_profile_"): ...
    def validate_profile_sql(self, sql, table_info): ...
    def execute_profile_sql(self, sql): ...
    def drop_profile_table(self, table_name): ...
    def process_profile_generation(self, table_name, column_name): ...
    def confirm_and_execute(self, profile_info, confirm): ...
```

## 使用方法

### 基本使用流程

1. **初始化生成器**

```python
from text2sql_module.text2sql_processor import CustomerProfileGenerator

# 使用默认配置
profile_generator = CustomerProfileGenerator()

# 或自定义数据库连接和模型
profile_generator = CustomerProfileGenerator(
    db_uri="mysql+pymysql://user:password@localhost:3306/mydb",
    model_name="gpt-4"
)
```

2. **生成并处理画像**

```python
# 生成画像并获取待确认列表
profiles = profile_generator.process_profile_generation("customer_info", "年龄")

# 展示并获取用户确认
for profile in profiles:
    if profile['status'] == 'pending':
        print(f"画像描述: {profile['natural_language']}")
        # 获取用户确认（这里需要您实现用户输入逻辑）
        user_confirm = True  # 假设用户确认
        
        # 执行或取消
        result = profile_generator.confirm_and_execute(profile, user_confirm)
        print(f"执行结果: {result['status']}")
```

### 使用测试工具

项目提供了一个交互式测试工具，可以直接运行：

```bash
python test_profile_generator.py
```

测试工具支持两种模式：
- **单一列测试**：对单个列生成画像并逐个确认
- **批量列测试**：对多个列批量生成画像

## 输出表结构

生成的画像表具有统一的结构：

```sql
CREATE TABLE customer_profile_[标签名] (
    CUST_NO VARCHAR(50) PRIMARY KEY,  # 客户编号
    CUST_NAM VARCHAR(100),            # 客户姓名
    PROFILE_VALUE VARCHAR(50) NOT NULL,  # 画像值
    CREATE_TIME TIMESTAMP DEFAULT CURRENT_TIMESTAMP  # 创建时间
);
```

## 注意事项

1. **数据要求**：源表必须包含`CUST_NO`和`CUST_NAM`字段
2. **权限要求**：数据库用户需要有创建表和插入数据的权限
3. **性能考虑**：处理大数据量时可能需要优化查询
4. **依赖要求**：需要安装numpy库用于四分位数计算

## 依赖安装

```bash
pip install numpy
```

## 示例场景

### 场景1：年龄画像分析

对客户年龄进行分段，生成不同年龄段的客户群体画像。

输入：
```python
profiles = profile_generator.process_profile_generation("customer_info", "年龄")
```

输出：生成4个画像表，分别包含不同年龄段的客户。

### 场景2：客户价值分层

基于客户的消费金额或产品持有率，将客户分为高、中、低价值群体。

输入：
```python
profiles = profile_generator.process_profile_generation("customer_info", "多产品持有率")
```

输出：生成3个画像表，分别包含高、中、低持有率的客户。

## 错误处理

- 当列不存在时，会返回空的画像列表并记录错误日志
- 当列数据为空时，会返回空的画像列表
- SQL验证失败时，会提供详细的错误信息和可能的改进建议
- 执行失败时，会返回错误状态和具体错误信息

## 扩展建议

1. 添加更多类型的标签生成策略
2. 支持自定义标签划分规则
3. 实现画像结果的可视化展示
4. 添加定期更新画像的调度功能
