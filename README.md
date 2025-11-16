# mace

Mace 是一个平衡插入和查询效率的嵌入式存储引擎，支持 ACID 当前仅支持隔离级别 SI （Snapshot Isolation）

用法示例 [demo.rs](./examples/demo.rs)

### 已实现特性
- 插入、查询、删除、前缀扫描
- MVCC 支持
- 数据完整性检查
- 大 value 分离存储
- 垃圾回收

### 性能测试
详见 [kv_bench](https://github.com/abbycin/kv_bench)

### 警告
由于目前还在早期阶段，文件的存储格式以及对外的API随时都可能被修改