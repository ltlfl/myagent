# 多智能体协作架构设计

## 1. 系统架构概述

基于Microsoft AutoGen框架，我们设计了一个多智能体系统，集成Text2SQL和客户细分功能。系统采用AutoGen的group chat机制实现智能体协调，包含以下主要组件：

1. **UserProxyAgent**：用户代理，作为用户和其他智能体之间的接口，负责整合分析结果并生成最终报告
2. **Text2SQLAgent**：基于AssistantAgent实现，负责自然语言转SQL查询处理
3. **CustomerSegmentationAgent**：基于AssistantAgent实现，负责客户细分分析和对比查询
4. **GroupChatManager**：由AutoGen提供的group chat管理器，负责智能体间的通信和任务协调
5. **QueryClassifier**：查询分类器，用于识别查询类型（简单查询或客户细分分析）

## 2. 智能体定义和角色

### 2.1 UserProxyAgent

**角色**：用户交互接口和最终分析报告生成者

**核心功能**：
- 接收用户输入的分析需求
- 对于客户细分分析需求，自动生成对照组查询问题
- 协调Text2SQL智能体和客户细分智能体完成分析任务
- 整合所有智能体提供的分析数据和初步结论
- 生成最终的分析总结报告

**关键属性**：
- `function_map`：工具映射，整合所有可用工具
- `is_termination_msg`：判断对话终止的条件

### 2.2 Text2SQLAgent

**角色**：SQL生成和执行专家，基于AssistantAgent实现

**核心功能**：
- 理解用户的自然语言查询，识别查询意图
- 调用SQL查询工具获取数据
- 分析查询结果，提供有意义的解释
- 当查询不明确时，向用户提问以获取更多信息
- 总结查询结果并提供自然语言解释

**关键接口**：
- `text2sql_processor`：集成的Text2SQL处理器，用于实际的查询处理

**工作规则**：
- 负责处理数据查询、统计、简单分析等SQL相关任务
- 如果收到复杂分析需求（如客户群体对比），请转交给客户细分智能体
- 完成任务后，以"分析完成"作为结束标志

### 2.3 CustomerSegmentationAgent

**角色**：客户细分和数据分析专家，基于AssistantAgent实现

**核心功能**：
- 分析用户的客户细分和对比分析需求
- 提取目标客群和对照组信息
- 调用execute_segmentation_analysis工具执行深度分析
- 提供详细的客户分析数据和初步结论

**关键接口**：
- `segmentation_processor`：集成的客户细分处理器，用于实际的分析处理

**工作规则**：
- 负责处理客户群体对比、行为分析、细分建模等复杂分析任务
- 如果收到简单查询需求，请转交给Text2SQL智能体
- 完成任务后，以"分析完成"作为结束标志

### 2.4 工具集成

系统通过`function_map`机制整合了以下关键工具：

1. **execute_sql_query**：
   - 功能：将自然语言转换为SQL查询并执行
   - 参数：`question`（自然语言查询问题）
   - 返回：SQL执行结果和解释

2. **execute_segmentation_analysis**：
   - 功能：执行客户细分分析
   - 参数：`query`（客户细分分析需求）
   - 返回：客户细分分析数据和初步结论

3. **get_database_schema**：
   - 功能：获取数据库表结构信息
   - 参数：可选的`table_name`（特定表名）
   - 返回：数据库表结构概览

4. **generate_control_group_query**：
   - 功能：根据目标组查询问题生成对照组查询问题
   - 参数：`target_query`（目标组查询问题）
   - 返回：对照组查询问题

## 3. 协作流程

### 3.1 基本查询流程

1. 用户通过UserProxyAgent提交查询
2. GroupChatManager根据智能体的系统消息和决策规则协调处理
3. Text2SQLAgent调用execute_sql_query工具处理查询
4. 查询结果通过Text2SQLAgent返回给UserProxyAgent
5. UserProxyAgent将结果展示给用户

### 3.2 客户细分分析流程

1. 用户提交客户细分相关查询
2. UserProxyAgent调用generate_control_group_query工具生成对照组查询问题
3. UserProxyAgent将目标组和对照组查询问题转交给CustomerSegmentationAgent
4. CustomerSegmentationAgent调用execute_segmentation_analysis工具执行深度分析
5. 分析结果通过CustomerSegmentationAgent返回给UserProxyAgent
6. UserProxyAgent整合分析结果，生成最终的分析总结报告

### 3.3 智能体协调机制

系统采用以下规则进行智能体协调：

1. **决策调度规则**：
   - 用户代理负责接收用户请求并协调智能体
   - Text2SQL智能体负责处理数据查询、统计、简单分析等SQL相关任务
   - 客户细分智能体负责处理客户群体对比、行为分析、细分建模等复杂分析任务
   - 所有智能体必须严格遵守各自的职责分工

2. **错误处理规则**：
   - **重大问题**：无需征求用户同意，直接重新调用相应智能体生成新的SQL或分析方案
   - **轻微问题**：仅指出问题，不进行修改或重新生成

3. **任务终止条件**：
   - 当智能体的回复以"<END>"结尾或包含"分析完成"时，认为任务完成

## 4. 数据流转

- **用户查询** → UserProxyAgent → GroupChatManager → 相应的智能体（Text2SQLAgent或CustomerSegmentationAgent）
- **简单查询** → Text2SQLAgent → execute_sql_query工具 → 返回结果 → UserProxyAgent → 用户
- **客户细分查询** → UserProxyAgent → generate_control_group_query工具 → CustomerSegmentationAgent → execute_segmentation_analysis工具 → 返回结果 → UserProxyAgent → 用户

## 5. 实现考虑因素

### 5.1 对话历史管理
- 维护统一的对话历史存储
- 在多智能体间共享相关历史记录
- 支持上下文感知的查询处理

### 5.2 错误处理

**错误分类**：
- **重大问题**：SQL语法错误、表/字段不存在、执行失败等
- **轻微问题**：SQL查询结果为空但语法正确、查询内容可以优化等

**处理规则**：
- **重大问题**：无需征求用户同意，直接重新调用相应智能体生成新的SQL或分析方案
  - 对于SQL相关错误，如果原任务是客户细分分析需求，重新调用CustomerSegmentationAgent生成新的查询方案
  - 对于简单查询的SQL错误，重新调用Text2SQLAgent生成新的SQL查询
- **轻微问题**：仅指出问题，不进行修改或重新生成
  - 如SQL查询结果为空但语法正确、查询内容可以优化等情况

### 5.3 性能优化
- 实现工具调用的异常处理机制，提高系统稳定性
- 优化智能体间的通信效率
- 减少连续回复次数，防止无限循环

### 5.4 扩展性
- 模块化设计，便于添加新的智能体类型
- 通过`function_map`机制标准化工具接口，确保新工具可以无缝集成
- 支持自定义智能体行为和协作规则

## 6. 技术栈

- **框架**：Microsoft AutoGen
- **语言模型**：qwen-plus（与现有系统兼容）
- **数据库**：MySQL（通过SQLAlchemy接口）
- **工作流**：基于LangGraph的状态管理（用于Text2SQL和客户细分处理器）
- **日志**：统一的日志记录系统
- **查询分类**：自定义的QueryClassifier组件

## 7. 代码结构

系统的主要代码结构如下：

```
IntegratedMultiAgentSystem
├── __init__()                    # 初始化系统
├── _init_autogen_agents()        # 初始化AutoGen智能体
├── _setup_function_map()         # 设置工具映射
├── _create_group_chat()          # 创建group chat
├── _generate_control_group_query()  # 生成对照组查询
└── run()                         # 运行系统
```

**核心组件关系**：
- IntegratedMultiAgentSystem是系统的主入口点
- 系统初始化时创建所有必要的智能体和工具
- 通过group chat机制实现智能体间的通信和协作
- QueryClassifier用于识别查询类型并指导智能体选择

## 8. 系统特点

1. **无缝集成**：将Text2SQL和客户细分功能无缝集成到同一多智能体系统中
2. **智能协调**：通过AutoGen的group chat机制实现智能体间的智能协调
3. **灵活扩展**：支持通过添加新工具和智能体来扩展系统功能
4. **友好错误处理**：针对不同类型的错误提供不同的处理策略
5. **完整分析流程**：支持从查询提交到最终报告生成的完整分析流程
6. **对照分析支持**：自动生成对照组查询，支持更全面的客户细分分析