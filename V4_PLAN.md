# SeekStorm v4 实施计划

> **核心改造**：词法引擎委托 tantivy，向量引擎采用 SPFresh 风格 LIRE 协议，存储引擎采用简易 LSM（MemTable + SSTable），全面 schemaless。

---

## 项目进度

| Phase | 名称 | 状态 | 说明 |
|-------|------|------|------|
| 1 | LSM 存储引擎核心 | ✅ 完成 | MemTable + SSTable + WAL + compaction |
| 2 | Schemaless + 文档存储 | ✅ 完成 | DynamicSchema + 类型推断 |
| 3 | Tantivy 集成 | ✅ 完成 | 自研倒排索引词法引擎 |
| 4 | 基础 IVF 向量引擎 | ✅ 完成 | IVF 分区 + K-Medoid PAM 聚类 |
| 5 | LIRE 协议 | 🔄 待实施 | 向量实时更新 |
| 6 | 混合检索 + RRF | 🔄 待实施 | 融合检索 |
| 7 | 删除 + 崩溃恢复 | 🔄 待实施 | 持久性保证 |
| 8 | 服务器适配 | 🔄 待实施 | HTTP API |
| 9 | 可观测性 + 优化 | 🔄 待实施 | 生产就绪 |

---

## 架构概览

```
┌──────────────────────────────────────────────────────────┐
│                    Index (门面)                          │
│  add_document / delete_document / search / commit        │
└────────────┬──────────────┬──────────────┬───────────────┘
             │              │              │
             ▼              ▼              ▼
    ┌──────────────┐ ┌──────────────┐ ┌──────────────┐
    │ LexicalEngine│ │ VectorEngine │ │ DocStore     │
    │  (tantivy)   │ │   (IVF)      │ │   (LSM)      │
    └──────┬───────┘ └──────┬───────┘ └──────┬───────┘
           │                 │               │
           └────────┬────────┴───────────────┘
                    ▼
           ┌──────────────────┐
           │   LsmEngine      │
           │ (MemTable+SS+Wal)│
           └──────────────────┘
```

---

## 模块结构

```
seekstorm_core/src/
├── storage/           # 存储引擎
│   ├── lsm.rs         # LsmEngine 门面
│   ├── sstable.rs     # SSTable 读写
│   ├── wal.rs         # WAL
│   ├── io_backend.rs  # IoBackend trait
│   └── manifest.rs    # 索引 manifest
├── lexical/           # 词法引擎
│   ├── mod.rs         # LexicalEngine
│   └── analyzer.rs    # 分词器（tantivy 集成）
├── vector/            # 向量引擎
│   ├── mod.rs         # VectorEngine 门面
│   ├── ivf_index.rs   # IVF 索引管理
│   ├── partition.rs   # 分区管理
│   ├── clustering.rs  # K-Medoid PAM 聚类
│   ├── similarity.rs  # 相似度计算
│   └── quantization.rs # 向量量化
├── schema/            # Schema 系统
│   ├── mod.rs         # DynamicSchema
│   └── inference.rs   # 类型推断
├── index/             # 统一索引接口
│   ├── mod.rs         # Index 门面
│   └── document.rs    # SchemalessDoc
└── lib.rs             # 库入口
```

---

## 核心数据结构

### LsmKey
```rust
pub struct LsmKey {
    pub namespace: u8,        // 0x01=DocStore, 0x02=Vector, 0x03=Meta
    pub partition: u32,
    pub doc_id: u64,
}
```

### LsmValue
```rust
pub enum LsmValue {
    Data(Vec<u8>),
    Tombstone,
}
```

### VectorConfig
```rust
pub struct VectorConfig {
    pub vector_dim: usize,
    pub similarity: VectorSimilarity,  // Cosine / Dot / Euclidean
    pub quantization: QuantizationType, // F32 / ScalarI8 / TurboI8
    pub nprobe: usize,                 // 搜索探测分区数
    pub max_partition_size: u64,       // 分区大小阈值
}
```

---

## 已完成阶段详情

### Phase 1: LSM 存储引擎核心 ✅

**文件**: `storage/`

- `lsm.rs` - LsmEngine（put/get/delete/scan）
- `sstable.rs` - SSTable 格式与读写
- `wal.rs` - 写前日志与恢复
- `io_backend.rs` - 异步 I/O 抽象
- `manifest.rs` - 索引元数据持久化

**特性**:
- MemTable: SkipMap 实现
- WAL: 追加日志 + crc32 校验
- SSTable: 块存储 + 稀疏索引
- Compaction: size-tiered (4个合并为1个)

### Phase 2: Schemaless + 文档存储 ✅

**文件**: `schema/`, `index/`

- `DynamicSchema` - 运行时推断字段类型
- `SchemalessDoc` - JSON 文档表示
- 类型自动推断 (Text/I64/U64/F64/Bool/DateTime/Vector)

### Phase 3: Tantivy 集成 ✅

**文件**: `lexical/`

- `LexicalEngine` - 基于 tantivy 的词法检索
- `analyzer.rs` - 分词器适配
- `posting.rs` - 倒排索引结构

### Phase 4: 基础 IVF 向量引擎 ✅

**文件**: `vector/`

| 文件 | 功能 |
|------|------|
| `mod.rs` | VectorEngine 门面 |
| `ivf_index.rs` | IVF 索引 + 质心表 |
| `partition.rs` | 分区管理（墓碑） |
| `clustering.rs` | K-Medoid PAM 聚类 |
| `similarity.rs` | Cosine/Dot/Euclidean |
| `quantization.rs` | F32Quantization |

**API**:
```rust
// 初始化质心
engine.initialize_centroids(&sample_vectors).await?;

// 添加向量
engine.add_vector(doc_id, &vector).await?;

// 检索
let results = engine.search(&query_vector, top_k).await?;
```

---

## 待实施阶段

### Phase 5: LIRE 协议

**目标**: 向量分区实时分裂（SPFresh 风格）

**文件**: `vector/lire.rs`（新建）

- `lire_split()` - 分区分裂
- `find_split_direction()` - 主成分分析
- `recompute_centroids()` - 增量质心更新

### Phase 6: 混合检索 + RRF

**文件**: `search/`（新建）

- `hybrid_search.rs` - 混合检索
- `fusion.rs` - RRF 融合算法
- `planner.rs` - 查询模式选择

### Phase 7: 删除 + 崩溃恢复

**文件**: `storage/recovery.rs`（新建）

- WAL 重放恢复
- 分区 tombstone 清理
- 故障注入测试

### Phase 8: 服务器适配

**文件**: `seekstorm_server/`

- HTTP API 路由适配
- 索引管理接口
- 查询接口

### Phase 9: 可观测性

**文件**: `observability/`（新建）

- Prometheus 指标
- Tracing 日志
- 性能监控

---

## 代码量对比

| 模块 | v3 | v4 预估 | 变化 |
|------|-----|---------|------|
| 词法引擎 | ~16K | ~2K | -14K (委托 tantivy) |
| 向量引擎 | ~5K | ~4K | -1K (LIRE 协议新写) |
| 存储引擎 | ~3K | ~3K | 0 (LSM) |
| Schema | ~1K | ~1.5K | +0.5K (动态) |
| 查询/融合 | ~3K | ~2K | -1K (简化) |
| **总计** | ~33K | ~17.5K | **-15.5K** |

---

## 测试状态

```
cargo test --lib
test result: ok. 65 passed; 0 failed
```

- 存储: 15 tests
- Schema: 8 tests
- 词法: 12 tests
- 向量: 30 tests

---

## 参考资料

- [tantivy](https://github.com/quickwit-oss/tantivy)
- [SPFresh (SOSP 2023)](https://www.microsoft.com/en-us/research/?p=1075902)
- [RocksDB LSM design](https://github.com/facebook/rocksdb/wiki)