# 多智能体协作架构设计

## 1. 系统架构概述

基于Microsoft AutoGen框架，我们将设计一个多智能体系统，集成Text2SQL和客户细分功能。系统将包含以下主要组件：

1. **UserProxyAgent**：用户代理，作为用户和其他智能体之间的接口
2. **Text2SQLAgent**：负责自然语言转SQL查询处理
3. **CustomerSegmentationAgent**：负责客户细分分析和对比查询
4. **TaskOrchestrator**：任务协调器，管理任务分发和执行流程

## 2. 智能体定义和角色

### 2.1 Text2SQLAgent

**角色**：SQL生成和执行专家

**核心功能**：
- 将自然语言转换为SQL查询
- 验证SQL查询的安全性和正确性
- 执行SQL查询并返回结果
- 提供查询结果的自然语言解释

**接口要求**：
- `process_query(question, session_id, conversation_history)`：处理自然语言查询并返回SQL执行结果
- `get_table_info()`：获取数据库表结构信息

### 2.2 CustomerSegmentationAgent

**角色**：客户细分和数据分析专家

**核心功能**：
- 分析用户查询并提取目标客群和对照组
- 生成对比分析SQL查询
- 执行对比分析并生成结果解释

**接口要求**：
- `process_query(query)`：处理客户细分分析查询
- 能够利用Text2SQLAgent生成和执行SQL

### 2.3 UserProxyAgent

**角色**：用户交互接口

**核心功能**：
- 接收用户输入
- 将用户查询传递给TaskOrchestrator
- 展示智能体的响应给用户
- 管理对话历史

### 2.4 TaskOrchestrator

**角色**：任务协调和调度

**核心功能**：
- 分析用户查询类型
- 基于查询类型选择合适的处理智能体
- 协调多智能体之间的信息传递
- 处理任务执行结果的整合

## 3. 协作流程

### 3.1 基本查询流程

1. 用户通过UserProxyAgent提交查询
2. TaskOrchestrator分析查询类型
   - 如果是简单的SQL查询请求，直接分发给Text2SQLAgent
   - 如果是客户细分或对比分析请求，分发给CustomerSegmentationAgent
3. CustomerSegmentationAgent在需要时调用Text2SQLAgent生成和执行SQL
4. 结果通过UserProxyAgent返回给用户

### 3.2 客户细分分析流程

1. 用户提交客户细分相关查询
2. CustomerSegmentationAgent分析查询，提取目标客群和对照组
3. CustomerSegmentationAgent请求Text2SQLAgent为目标客群生成SQL
4. CustomerSegmentationAgent请求Text2SQLAgent为对照组生成SQL
5. CustomerSegmentationAgent整合两次查询结果，生成对比分析
6. 结果通过UserProxyAgent返回给用户

## 4. 数据流转

- **用户查询** → UserProxyAgent → TaskOrchestrator
- **简单查询** → Text2SQLAgent → 执行SQL → 返回结果
- **客户细分查询** → CustomerSegmentationAgent → Text2SQLAgent → 返回结果 → 分析对比 → 返回结果
- **所有结果** → UserProxyAgent → 用户

## 5. 实现考虑因素

### 5.1 对话历史管理
- 维护统一的对话历史存储
- 在多智能体间共享相关历史记录
- 支持上下文感知的查询处理

### 5.2 错误处理
- 实现跨智能体的错误传播机制
- 提供友好的错误提示给用户
- 支持查询重试和回退机制

### 5.3 性能优化
- 缓存常用查询结果
- 优化智能体间的通信效率
- 实现异步处理以提高响应速度

### 5.4 扩展性
- 模块化设计，便于添加新的智能体类型
- 标准化接口，确保新智能体可以无缝集成
- 支持自定义智能体行为和协作规则

## 6. 技术栈

- **框架**：Microsoft AutoGen
- **语言模型**：与现有系统兼容的LLM（如qwen-plus）
- **数据库**：MySQL（通过SQLAlchemy接口）
- **工作流**：基于LangGraph的状态管理
- **日志**：统一的日志记录系统