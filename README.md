# Apache Flink® SQL Practice

**This repository provides a practice for Flink's Tabel API & SQL API.**

请配合我的专栏：[Flink SQL原理和实战]( https://blog.csdn.net/qq_35815527/category_9634641.html ) 使用

### Apache Flink

Apache Flink（以下简称Flink）是第三代流处理引擎，支持精确的流处理，能同时满足各种规模下对高吞吐和低延迟的需求等优势。

## 为什么要用 SQL

以下是本人基于[Apache Flink® SQL Training]( https://github.com/ververica/sql-training )翻译的Flink SQL介绍:

SQL 是 Flink的强大抽象处理功能，位于 Flink 分层抽象的顶层。

####  DataStream API非常棒 

 非常有表现力的流处理API转换、聚合和连接事件Java和Scala控制如何处理事件的时间戳、水印、窗口、计时器、触发器、允许延迟……维护和更新应用程序状态键控状态、操作符状态、状态后端、检查点 

####  但并不是每个人都适合 

- 编写分布式程序并不总是那么容易理解新概念：时间，状态等

- 需要知识和技能–连续应用程序有特殊要求–编程经验（Java / Scala）
- 用户希望专注于他们的业务逻辑 

#### 而SQL API 就做的很好

- 关系api是声明性的，用户说什么是需要的，

- 系统决定如何计算it查询，可以有效地优化，让Flink处理状态和时间，

- 每个人都知道和使用SQL 

#### 结论

Flink SQL 简单、声明性和简洁的关系API表达能力强，

足以支持大量的用例，

用于批处理和流数据的统一语法和语义 

------

*Apache Flink, Flink®, Apache®, the squirrel logo, and the Apache feather logo are either registered trademarks or trademarks of The Apache Software Foundation.*

