# SeekStorm v4 完整改造方案

> **核心改造**：词法引擎替换为 tantivy，向量引擎替换为 SPFresh 风格 LIRE，存储引擎采用**简易 LSM**（MemTable + 单层 SSTable + size-tiered compaction），不使用 mmap，全面 schemaless。

---

## 1. 改造目标与范围

### 1.1 目标

| 维度 | v3 现状 | v4 目标 |
|---|---|---|
| 词法引擎 | 自研倒排索引（~12K 行） | 委托给 tantivy 0.21 |
| 向量引擎 | 静态 IVF + 全量重建 | SPFresh LIRE 协议，就地更新 |
| 存储引擎 | `memmap2` + 自研 segment | 简易 LSM（MemTable + SSTable），无 mmap |
| Schema | 静态 schema，建索引时定义 | Schemaless，字段自动推断 |
| 删除 | `delete.bin` + 显式 GC | tombstone + LSM compaction 自动清理 |
| 实时性 | 软提交 + 硬提交 | MemTable 即可见，亚毫秒延迟 |

### 1.2 范围

**包含**：
- 库 crate（`seekstorm_core`）全面重写
- 服务器 crate（`seekstorm_server`）API 兼容性保留
- 客户端 crate（`seekstorm_client`）无需改动

**不包含**：
- 分布式集群（单节点）
- GPU 加速
- v3 索引文件格式兼容（全新格式，旧索引需重新构建）

### 1.3 代码量预估

| 模块 | v3 行数 | v4 预估 | 说明 |
|---|---|---|---|
| 词法引擎相关 | ~16K | ~2K | 删除自研，加 tantivy 适配层 |
| 向量引擎相关 | ~5K | ~4K | LIRE 协议新写，复用 clustering |
| 存储引擎 | ~3K | ~3K | 简易 LSM 核心实现 |
| Schema 系统 | ~1K | ~1.5K | 动态 schema + 类型推断 |
| 查询/融合 | ~3K | ~2K | 保留 RRF，简化 |
| 服务器 | ~5K | ~5K | 保留，适配新 API |
| **总计** | ~33K | ~17.5K | 净减约 15K 行 |

---

## 2. 简易 LSM 设计

### 2.1 设计取舍

**为什么是"简易"LSM**：
- 仅 2 层：MemTable + SSTable（无 L0/L1/L2 多层）
- size-tiered compaction（SSTable 数 ≥ 4 时合并为一个）
- 单 WAL，单线程写入
- 不实现 leveled compaction、Bloom filter 优化等高级特性

**收益**：
- 实现复杂度低（核心约 1500 行）
- 写吞吐高（无多层 compaction 开销）
- 代码可维护

**代价**：
- 读放大略高（多 SSTable 合并）—— 用 moka 页缓存缓解
- 空间放大略高（旧 SSTable 未及时清理）—— 定时 compaction 缓解

### 2.2 整体架构

```
┌──────────────────────────────────────────────────────────┐
│  上层：LexicalEngine / VectorEngine / DocStore           │
└────────────────────┬─────────────────────────────────────┘
                     │
                     ▼
            ┌──────────────────────┐
            │   LsmEngine (门面)   │
            └────┬──────────┬──────┘
                 │          │
        ┌────────▼──┐  ┌───▼──────────┐
        │  MemTable  │  │     WAL       │
        │ (SkipMap)  │  │ (append+fsync)│
        └────┬───────┘  └───┬───────────┘
             │              │
             ▼              ▼
        ┌──────────────────────────┐
        │   SSTable 层（单层）    │
        │  ┌──┐ ┌──┐ ┌──┐ ┌──┐  │  ← size-tiered
        │  │S1│ │S2│ │S3│ │S4│  │
        │  └──┘ └──┘ └──┘ └──┘  │
        └──────────┬──────────────┘
                   │
                   ▼
        ┌────────────────────┐
        │   IoBackend        │
        │ (io_uring / fs)    │
        └────────────────────┘
```

### 2.3 核心数据结构

```rust
// seekstorm_core/src/storage/lsm.rs

use crossbeam_skiplist::SkipMap;
use std::sync::Arc;

pub struct LsmEngine {
    /// 内存表（活跃）
    memtable: RwLock<Arc<MemTable>>,
    /// 内存表（刷盘中）
    flushing: RwLock<Option<Arc<MemTable>>>,
    /// SSTable 列表
    sstables: RwLock<Vec<Arc<SSTable>>>,
    /// WAL
    wal: Arc<Wal>,
    /// I/O 后端
    io: Arc<dyn IoBackend>,
    /// 配置
    config: LsmConfig,
    /// compaction 锁
    compaction_lock: Mutex<()>,
}

pub struct MemTable {
    table: SkipMap<LsmKey, LsmValue>,
    bytes: AtomicU64,
}

pub struct LsmKey {
    pub namespace: u8,             // 0x01=DocStore, 0x02=VectorPart, 0x03=Meta
    pub partition_or_segment: u32, // 分区或 segment ID
    pub doc_id: u64,
    pub lsn: u64,                   // MVCC 版本号
}

pub enum LsmValue {
    Data(Vec<u8>),
    Tombstone,
}

pub struct LsmConfig {
    pub memtable_max_bytes: u64,    // 默认 64 MiB
    pub sstable_compact_threshold: usize, // 默认 4
    pub wal_sync: WalSync,
    pub io_backend: IoBackendKind,
}
```

### 2.4 SSTable 文件格式

```
sstable-NNNN.bin
┌────────────────────────────┐
│ Header                     │
│   magic: "SKST"            │
│   version: u32             │
│   sstable_id: u32          │
│   namespace: u8            │
│   entry_count: u32         │
├────────────────────────────┤
│ Data block 0 (4 KiB)       │  ← sorted KV entries
│ Data block 1               │
│ ...                        │
├────────────────────────────┤
│ Index block                │  ← 每 data block 一条稀疏索引
│   (key, block_offset)      │
├────────────────────────────┤
│ Footer                     │
│   index_offset: u64        │
│   magic: "SKST_END"        │
└────────────────────────────┘
```

### 2.5 关键操作

#### 写入

```rust
impl LsmEngine {
    pub async fn put(&self, key: LsmKey, value: LsmValue) -> Result<()> {
        // [1] WAL 持久化
        self.wal.append(WalOp::Put { key: key.clone(), value: value.clone() }).await?;
        
        // [2] 写入 MemTable
        let memtable = self.memtable.read().await.clone();
        memtable.insert(key, value);
        
        // [3] 若 MemTable 满，触发 flush
        if memtable.bytes.load() > self.config.memtable_max_bytes {
            self.maybe_flush().await?;
        }
        
        Ok(())
    }
    
    pub async fn delete(&self, namespace: u8, doc_id: u64) -> Result<()> {
        let key = LsmKey { namespace, partition_or_segment: 0, doc_id, lsn: self.next_lsn() };
        self.put(key, LsmValue::Tombstone).await
    }
    
    async fn maybe_flush(&self) -> Result<()> {
        // 双 MemTable 切换
        let old_memtable = {
            let mut active = self.memtable.write().await;
            let new_memtable = Arc::new(MemTable::new());
            let old = std::mem::replace(&mut *active, new_memtable);
            old
        };
        
        // 标记为 flushing
        *self.flushing.write().await = Some(old_memtable.clone());
        
        // 异步刷盘
        tokio::spawn(async move {
            let sstable = self.flush_memtable(&old_memtable).await?;
            self.sstables.write().await.push(Arc::new(sstable));
            *self.flushing.write().await = None;
            
            // 检查是否需要 compaction
            if self.sstables.read().await.len() >= self.config.sstable_compact_threshold {
                self.trigger_compaction().await?;
            }
            
            Ok::<(), anyhow::Error>(())
        });
        
        Ok(())
    }
}
```

#### 读取

```rust
impl LsmEngine {
    pub async fn get(&self, key: &LsmKey) -> Result<Option<LsmValue>> {
        // [1] 查 MemTable
        let memtable = self.memtable.read().await.clone();
        if let Some(v) = memtable.get(key) {
            return Ok(Some(v));
        }
        
        // [2] 查 flushing MemTable
        if let Some(flushing) = self.flushing.read().await.as_ref() {
            if let Some(v) = flushing.get(key) {
                return Ok(Some(v));
            }
        }
        
        // [3] 查 SSTable（从新到旧）
        let sstables = self.sstables.read().await;
        for sst in sstables.iter().rev() {
            if let Some(v) = sst.get(key).await? {
                return Ok(Some(v));
            }
        }
        
        Ok(None)
    }
    
    pub async fn scan_prefix(&self, namespace: u8, partition: u32) -> Result<Vec<(LsmKey, LsmValue)>> {
        let mut results = Vec::new();
        let mut seen_keys: HashSet<u64> = HashSet::new();
        
        // MemTable
        let memtable = self.memtable.read().await;
        for (k, v) in memtable.range(namespace, partition) {
            if seen_keys.insert(k.doc_id) {
                results.push((k.clone(), v.clone()));
            }
        }
        
        // SSTable（从新到旧，跳过已见 key）
        let sstables = self.sstables.read().await;
        for sst in sstables.iter().rev() {
            for (k, v) in sst.scan_prefix(namespace, partition).await? {
                if seen_keys.insert(k.doc_id) {
                    results.push((k.clone(), v.clone()));
                }
            }
        }
        
        Ok(results)
    }
}
```

### 2.6 Compaction

```rust
impl LsmEngine {
    async fn trigger_compaction(&self) -> Result<()> {
        let _guard = self.compaction_lock.lock().await;
        
        let sstables = self.sstables.read().await.clone();
        if sstables.len() < self.config.sstable_compact_threshold {
            return Ok(());
        }
        
        // 合并所有 SSTable
        let merged = self.compact_sstables(&sstables).await?;
        
        // 原子替换
        let mut current = self.sstables.write().await;
        *current = vec![Arc::new(merged)];
        
        // 删除旧 SSTable 文件
        for sst in &sstables {
            sst.delete_file().await?;
        }
        
        Ok(())
    }
    
    async fn compact_sstables(&self, sstables: &[Arc<SSTable>]) -> Result<SSTable> {
        let new_id = self.next_sstable_id();
        let mut writer = SStableWriter::new(new_id, self.io.clone());
        
        // 多路归并
        let mut mergers: Vec<_> = sstables.iter().map(|s| s.iter()).collect();
        
        while let Some((key, value)) = merge_next(&mut mergers).await? {
            // 跳过 tombstone（底层无更旧数据，可丢弃）
            if matches!(value, LsmValue::Tombstone) {
                continue;
            }
            writer.write(key, value).await?;
        }
        
        writer.finish().await
    }
}
```

---

## 3. 工作区与 Crate 结构调整

### 3.1 新的工作区

```
seekstorm/
├── Cargo.toml                   # workspace
├── seekstorm_core/              # 库（原 seekstorm）
│   ├── Cargo.toml
│   └── src/
│       ├── lib.rs
│       ├── schema/
│       │   ├── mod.rs           # DynamicSchema
│       │   ├── field.rs        # FieldMeta
│       │   └── inference.rs    # 类型推断
│       ├── storage/
│       │   ├── mod.rs           # LsmEngine 门面
│       │   ├── lsm.rs           # MemTable + SSTable + compaction
│       │   ├── sstable.rs       # SSTable 读写
│       │   ├── wal.rs           # WAL
│       │   ├── io_backend.rs    # IoBackend trait + 实现
│       │   └── manifest.rs      # 索引 manifest
│       ├── lexical/
│       │   ├── mod.rs           # LexicalEngine
│       │   ├── tantivy_adapter.rs
│       │   ├── lsm_directory.rs # tantivy::Directory for LSM
│       │   └── tokenizer.rs     # 自定义分词器
│       ├── vector/
│       │   ├── mod.rs           # VectorEngine
│       │   ├── ivf_index.rs    # Leveled IVF
│       │   ├── lire.rs          # LIRE 协议
│       │   ├── partition.rs     # 分区管理
│       │   ├── quantization.rs  # TurboQuant / SQ
│       │   ├── clustering.rs    # K-Medoid PAM（v3 复用）
│       │   ├── similarity.rs     # Cosine / Dot / Euclidean
│       │   └── inference.rs     # Model2Vec
│       ├── index/
│       │   ├── mod.rs           # Index 统一抽象
│       │   ├── document.rs      # SchemalessDoc
│       │   ├── ingest.rs        # 摄入流水线
│       │   └── commit.rs        # Commit / flush
│       ├── query/
│       │   ├── mod.rs
│       │   ├── planner.rs       # QueryMode 选择
│       │   ├── fusion.rs         # RRF
│       │   └── filter.rs        # 字段过滤
│       ├── search/
│       │   ├── lexical_search.rs
│       │   ├── vector_search.rs
│       │   └── hybrid_search.rs
│       └── utils.rs
│       └── min_heap.rs          # v3 复用
├── seekstorm_server/           # 服务器（保留）
└── seekstorm_client/           # 客户端（保留）
```

### 3.2 Cargo.toml 变更

```toml
# seekstorm_core/Cargo.toml
[dependencies]
# 新增
tantivy = "0.21"
tantivy-jieba = "0.9"           # optional feature
crossbeam-skiplist = "0.1"
moka = { version = "0.12", features = ["future"] }

# Linux io_uring
[target.'cfg(target_os = "linux")'.dependencies]
tokio-epoll-uring = "0.5"

# 保留
tokio, serde, serde_json, hyper, hyper-util, utoipa,
model2vec-rs, half, rayon, crossbeam-channel,
rkyv, zstd, lz4_flex, bytemuck, ahash, num_cpus

# 移除
# memmap2 = ...            ← 不再用 mmap
# aho-corasick = ...        ← tantivy 接管
# symspell_complete_rs = ... ← 简化
# regex-syntax = ...        ← 不再需要
```

### 3.3 删除的 v3 模块

| v3 文件 | 行数 | 处置 |
|---|---|---|
| `intersection.rs` | 2301 | **删除** |
| `intersection_simd.rs` | 600 | **删除**（移到 `vector/similarity.rs`） |
| `union.rs` | 1479 | **删除** |
| `compress_postinglist.rs` | 977 | **删除** |
| `index_posting.rs` | 942 | **删除** |
| `tokenizer.rs` | 1668 | **删除** |
| `realtime_search.rs` | 2095 | **替换为 `vector/lire.rs`** |
| `single.rs` | 417 | **删除** |
| `iterator.rs` | 413 | **删除** |
| `compatible.rs` | 21 | **删除** |
| `doc_store.rs` | 415 | **替换为 `storage/lsm.rs`** |
| `highlighter.rs` | 382 | **替换为 tantivy::Snippet 包装** |
| `geo_search.rs` | 144 | **暂不实现，标记 TODO** |
| `index.rs` | 5928 | **拆分** → `index/mod.rs` + `storage/manifest.rs` |
| `search.rs` | 3768 | **拆分** → `search/*.rs` |
| `ingest.rs` | 1278 | **重构** → `index/ingest.rs` |
| `vector.rs` | 1536 | **替换** → `vector/{ivf_index,lire,partition}.rs` |
| `vector_similarity.rs` | 3146 | **精简** → `vector/similarity.rs`（~600 行） |
| `min_heap.rs` | 1260 | **保留** |
| `clustering.rs` | - | **保留** |
| `utils.rs` | 203 | **保留** |

---

## 4. 词法引擎改造：tantivy 集成

### 4.1 LexicalEngine

```rust
// seekstorm_core/src/lexical/mod.rs

pub struct LexicalEngine {
    tv_index: tantivy::Index,
    tv_writer: Mutex<tantivy::IndexWriter>,
    tv_reader: tantivy::IndexReader,
    doc_id_field: tantivy::schema::Field,
    schema: Arc<RwLock<DynamicSchema>>,
    lsm: Arc<LsmEngine>,
}

impl LexicalEngine {
    pub async fn new(
        lsm: Arc<LsmEngine>,
        schema: Arc<RwLock<DynamicSchema>>,
        config: &LexicalConfig,
    ) -> Result<Self> {
        // 使用 LSM-backed Directory
        let directory = LsmDirectory::new(lsm.clone());
        let tv_schema = build_initial_tantivy_schema(&schema.read().await);
        let tv_index = tantivy::Index::open_or_create(directory, tv_schema)?;
        let tv_writer = tv_index.writer(50_000_000)?; // 50 MB heap
        let tv_reader = tv_index.reader()?;
        
        Ok(Self {
            tv_index,
            tv_writer: Mutex::new(tv_writer),
            tv_reader,
            doc_id_field: /* from schema */,
            schema,
            lsm,
        })
    }
    
    pub async fn add_document(&self, doc_id: DocId, doc: &SchemalessDoc) -> Result<()> {
        let mut tv_doc = tantivy::doc!();
        tv_doc.add_u64(self.doc_id_field, doc_id);
        
        // 遍历 schemaless doc 字段，转换为 tantivy 字段
        let schema = self.schema.read().await;
        for (field_name, value) in doc.fields() {
            if let Some(field_meta) = schema.get_field(field_name) {
                if field_meta.indexed {
                    add_to_tantivy_doc(&mut tv_doc, field_meta, value);
                }
            }
        }
        
        let mut writer = self.tv_writer.lock().await;
        writer.add_document(tv_doc)?;
        // 不立即 commit，攒批
        Ok(())
    }
    
    pub async fn delete_document(&self, doc_id: DocId) -> Result<()> {
        let writer = self.tv_writer.lock().await;
        writer.delete_term(tantivy::Term::from_field_u64(self.doc_id_field, doc_id));
        Ok(())
    }
    
    pub async fn commit(&self) -> Result<()> {
        let writer = self.tv_writer.lock().await;
        writer.commit()?;
        self.tv_reader.reload()?;
        Ok(())
    }
    
    pub async fn search(&self, query: &LexicalQuery) -> Result<Vec<ScoredDoc>> {
        let searcher = self.tv_reader.searcher();
        let (query_parser, _) = self.build_tantivy_query(query)?;
        let top_docs = searcher.search(&query_parser, &tantivy::collector::TopDocs::with_limit(query.top_k))?;
        
        Ok(top_docs.into_iter().map(|(score, doc_addr)| {
            ScoredDoc { doc_id: /* extract */, score: score as f64 }
        }).collect())
    }
}
```

### 4.2 LsmDirectory（tantivy Directory trait）

```rust
// seekstorm_core/src/lexical/lsm_directory.rs

pub struct LsmDirectory {
    lsm: Arc<LsmEngine>,
    index_id: IndexId,
}

impl tantivy::directory::Directory for LsmDirectory {
    fn open_read(&self, path: &Path) -> tantivy::Result<Arc<OwnedBytes>> {
        let key = self.path_to_key(path);
        let bytes = self.lsm.blocking_get(&key)
            .map_err(|e| tantivy::TantivyError::from(e))?
            .map(|v| match v {
                LsmValue::Data(b) => b,
                LsmValue::Tombstone => Vec::new(),
            })
            .unwrap_or_default();
        Ok(Arc::new(OwnedBytes::new(bytes)))
    }
    
    fn open_write(&self, path: &Path) -> tantivy::Result<Box<dyn Write>> {
        Ok(Box::new(LsmWriter::new(self.lsm.clone(), self.path_to_key(path))))
    }
    
    fn atomic_write(&self, path: &Path, data: &[u8]) -> tantivy::Result<()> {
        let key = self.path_to_key(path);
        self.lsm.blocking_put(key, LsmValue::Data(data.to_vec()))
            .map_err(|e| tantivy::TantivyError::from(e))
    }
    
    fn atomic_read(&self, path: &Path) -> tantivy::Result<Vec<u8>> {
        let key = self.path_to_key(path);
        self.lsm.blocking_get(&key)
            .map_err(|e| tantivy::TantivyError::from(e))?
            .map(|v| match v {
                LsmValue::Data(b) => b,
                LsmValue::Tombstone => Vec::new(),
            })
            .ok_or_else(|| tantivy::TantivyError::IoError("not found".into()))
    }
    
    fn exists(&self, path: &Path) -> bool {
        let key = self.path_to_key(path);
        self.lsm.blocking_get(&key).map(|o| o.is_some()).unwrap_or(false)
    }
    
    fn delete(&self, path: &Path) -> tantivy::Result<()> {
        let key = self.path_to_key(path);
        self.lsm.blocking_delete(key.namespace, key.doc_id)
            .map_err(|e| tantivy::TantivyError::from(e))
    }
    
    fn sync_directory(&self) -> tantivy::Result<()> {
        // LSM 的 fsync 由 WAL 保证
        Ok(())
    }
}
```

### 4.3 Schema 桥接

```rust
// seekstorm_core/src/lexical/tantivy_adapter.rs

pub fn build_initial_tantivy_schema(
    dynamic: &DynamicSchema,
) -> tantivy::schema::Schema {
    let mut schema_builder = tantivy::schema::Schema::builder();
    
    // 必须的 doc_id 字段
    let doc_id_opts = tantivy::schema::IndexRecordOption::Basic;
    schema_builder.add_u64_field("doc_id", tantivy::schema::INDEXED);
    
    // 遍历 dynamic schema 添加字段
    for (name, meta) in dynamic.fields() {
        match meta.data_type {
            FieldType::Text => {
                let opts = tantivy::schema::TextOptions::default()
                    .set_indexing_metadata(
                        tantivy::schema::TextFieldIndexing::default()
                            .set_tokenizer(&meta.tokenizer_name())
                    );
                schema_builder.add_text_field(&name, opts);
            }
            FieldType::I64 => schema_builder.add_i64_field(&name, tantivy::schema::INDEXED),
            FieldType::U64 => schema_builder.add_u64_field(&name, tantivy::schema::INDEXED),
            FieldType::F64 => schema_builder.add_f64_field(&name, tantivy::schema::INDEXED),
            FieldType::Bool => schema_builder.add_bool_field(&name, tantivy::schema::INDEXED),
            _ => {} // 其他类型暂不索引
        }
    }
    
    schema_builder.build()
}
```

---

## 5. 向量引擎改造：SPFresh LIRE

### 5.1 VectorEngine

```rust
// seekstorm_core/src/vector/mod.rs

pub struct VectorEngine {
    ivf: RwLock<Arc<IvfIndex>>,
    lsm: Arc<LsmEngine>,
    config: VectorConfig,
    inference: Option<InferenceModel>,
}

pub struct VectorConfig {
    pub similarity: VectorSimilarity,
    pub quantization: Quantization,
    pub max_partition_size: usize,    // 默认 100_000
    pub min_partition_size: usize,    // 默认 1_000
    pub nprobe: usize,                // 默认 8
    pub vector_dim: usize,
}
```

### 5.2 IVF 索引

```rust
// seekstorm_core/src/vector/ivf_index.rs

pub struct IvfIndex {
    pub partitions: RwLock<HashMap<PartitionId, Arc<Partition>>>,
    pub centroid_table: RwLock<CentroidTable>,
    pub config: VectorConfig,
    pub next_partition_id: AtomicU32,
}

pub struct Partition {
    pub id: PartitionId,
    pub centroid: Vec<f32>,
    pub vector_count: AtomicU64,
    pub segment_id: u32,        // LSM 中的 segment 标识
    pub parent: Option<PartitionId>,
    pub level: u8,
    pub tombstones: RwLock<FixedBitSet>,  // 分区内 tombstone 位图
}

pub struct CentroidTable {
    pub centroids: Vec<Vec<f32>>,
    pub partition_ids: Vec<PartitionId>,
    // 用于快速最近邻查找（可选用 cover tree）
}
```

### 5.3 LIRE 协议实现

```rust
// seekstorm_core/src/vector/lire.rs

impl VectorEngine {
    pub async fn add_vector(&self, doc_id: DocId, vector: &[f32]) -> Result<()> {
        // [1] 量化
        let quantized = self.config.quantization.encode(vector);
        
        // [2] 找最近质心
        let partition_id = self.find_nearest_partition(&quantized).await?;
        
        // [3] 写入 LSM（向量存储在 VectorPart 命名空间）
        let key = LsmKey {
            namespace: 0x02, // VectorPart
            partition_or_segment: partition_id,
            doc_id,
            lsn: self.lsm.next_lsn(),
        };
        self.lsm.put(key, LsmValue::Data(quantized.encode())).await?;
        
        // [4] 更新 partition 计数
        let ivf = self.ivf.read().await;
        let partition = ivf.partitions.get(&partition_id).unwrap();
        partition.vector_count.fetch_add(1, Ordering::Relaxed);
        
        // [5] 触发 LIRE 分裂（若溢出）
        if partition.vector_count.load(Ordering::Relaxed) > self.config.max_partition_size {
            drop(ivf);
            self.lire_split(partition_id).await?;
        }
        
        Ok(())
    }
    
    pub async fn lire_split(&self, partition_id: PartitionId) -> Result<()> {
        // [1] 加锁分区
        let ivf = self.ivf.read().await;
        let partition = ivf.partitions.get(&partition_id).unwrap().clone();
        drop(ivf);
        
        // [2] 找分裂方向（主成分）
        let split_direction = self.find_split_direction(&partition).await?;
        
        // [3] 分配新分区
        let new_partition_id = self.allocate_partition_id().await?;
        let new_partition = Partition::new(
            new_partition_id,
            /* centroid will be computed */,
            partition.level + 1,
            Some(partition_id),
        );
        
        // [4] 迁移边界向量
        let vectors = self.scan_partition_vectors(partition_id).await?;
        let mut moved_count = 0;
        for (doc_id, quantized) in vectors {
            if self.should_move_to_new(&quantized, &split_direction) {
                // 在 LSM 中标记迁移：在旧分区 tombstone，写入新分区
                self.lsm.delete(0x02, /* old partition, doc_id */).await?;
                let new_key = LsmKey {
                    namespace: 0x02,
                    partition_or_segment: new_partition_id,
                    doc_id,
                    lsn: self.lsm.next_lsn(),
                };
                self.lsm.put(new_key, LsmValue::Data(quantized.encode())).await?;
                moved_count += 1;
            }
        }
        
        // [5] 重新计算质心（增量）
        let (old_centroid, new_centroid) = self.recompute_centroids(&partition, &new_partition).await?;
        
        // [6] 原子更新分区表
        let mut ivf = self.ivf.write().await;
        let new_ivf = IvfIndex {
            partitions: {
                let mut m = ivf.partitions.clone();
                m.insert(new_partition_id, Arc::new(new_partition));
                let mut p = m.get(&partition_id).unwrap().clone();
                Arc::make_mut(&mut p).centroid = old_centroid;
                m
            },
            centroid_table: /* rebuild */,
            ..*ivf
        };
        *ivf = Arc::new(new_ivf);
        
        // [7] WAL 记录分裂
        self.lsm.wal_append(WalOp::PartitionSplit {
            old: partition_id,
            new: new_partition_id,
            moved_count,
        }).await?;
        
        Ok(())
    }
    
    pub async fn delete_vector(&self, doc_id: DocId, partition_id: PartitionId) -> Result<()> {
        // LSM tombstone
        self.lsm.delete(0x02, /* partition_id, doc_id */).await?;
        
        // 分区 tombstone 位图
        let ivf = self.ivf.read().await;
        let partition = ivf.partitions.get(&partition_id).unwrap();
        let local_idx = self.find_local_index(&partition, doc_id).await?;
        partition.tombstones.write().await.set(local_idx, true);
        partition.vector_count.fetch_sub(1, Ordering::Relaxed);
        
        Ok(())
    }
}
```

### 5.4 向量检索

```rust
impl VectorEngine {
    pub async fn search(&self, query: &[f8], top_k: usize, filter: &Filter) -> Result<Vec<ScoredDoc>> {
        let quantized_query = self.config.quantization.encode(query);
        
        // [1] 找 top-nprobe 最近质心
        let ivf = self.ivf.read().await;
        let probed = ivf.centroid_table.find_nearest(&quantized_query, self.config.nprobe);
        
        // [2] 并行扫描分区
        let mut all_candidates = Vec::new();
        for partition_id in probed {
            let partition = ivf.partitions.get(&partition_id).unwrap();
            
            // 从 LSM 扫描该分区的所有向量
            let vectors = self.lsm.scan_prefix(0x02, partition_id).await?;
            let tombstones = partition.tombstones.read().await;
            
            for (key, value) in vectors {
                if matches!(value, LsmValue::Tombstone) { continue; }
                if tombstones.get(local_idx) { continue; }
                
                // 应用字段过滤
                if !filter.matches(doc_id) { continue; }
                
                let score = self.similarity(&quantized_query, &value);
                all_candidates.push(ScoredDoc { doc_id: key.doc_id, score });
            }
        }
        
        // [3] top-k via min_heap
        Ok(self.min_heap_top_k(all_candidates, top_k))
    }
}
```

---

## 6. 存储引擎改造：简易 LSM 完整实现

### 6.1 模块组织

```
storage/
├── mod.rs           # LsmEngine 门面
├── lsm.rs           # MemTable + SSTable 管理
├── sstable.rs       # SSTable 读写
├── wal.rs           # WAL
├── io_backend.rs    # IoBackend trait + 实现
└── manifest.rs      # Manifest
```

### 6.2 IoBackend

```rust
// seekstorm_core/src/storage/io_backend.rs

#[async_trait]
pub trait IoBackend: Send + Sync {
    async fn read_at(&self, path: &Path, offset: u64, buf: &mut [u8]) -> Result<usize>;
    async fn write_at(&self, path: &Path, offset: u64, buf: &[u8]) -> Result<usize>;
    async fn fsync(&self, path: &Path) -> Result<()>;
    async fn create_file(&self, path: &Path) -> Result<()>;
    async fn delete_file(&self, path: &Path) -> Result<()>;
    async fn file_size(&self, path: &Path) -> Result<u64>;
}

pub struct AsyncFsBackend { /* tokio::fs based */ }

#[cfg(target_os = "linux")]
pub struct IoUringBackend { /* tokio-epoll-uring based */ }

pub fn select_backend(kind: IoBackendKind) -> Box<dyn IoBackend> {
    match kind {
        #[cfg(target_os = "linux")]
        IoBackendKind::IoUring => Box::new(IoUringBackend::new()),
        _ => Box::new(AsyncFsBackend::new()),
    }
}
```

### 6.3 WAL 实现

```rust
// seekstorm_core/src/storage/wal.rs

pub struct Wal {
    path: PathBuf,
    file: Mutex<tokio::fs::File>,
    sync_policy: WalSync,
    current_size: AtomicU64,
    rotate_size: u64,
}

pub enum WalSync {
    EveryCommit,
    Periodic(Duration),
    None,
}

impl Wal {
    pub async fn append(&self, op: WalOp) -> Result<Lsn> {
        let lsn = self.next_lsn();
        let mut buf = Vec::new();
        buf.extend_from_slice(&op.encode());
        let record = WalRecord { lsn, op, crc32: crc32fast::hash(&buf) };
        
        let mut file = self.file.lock().await;
        let encoded = record.encode();
        file.write_all(&encoded).await?;
        
        match self.sync_policy {
            WalSync::EveryCommit => file.sync_data().await?,
            WalSync::Periodic(_) => { /* 由后台任务定时 sync */ }
            WalSync::None => {}
        }
        
        self.current_size.fetch_add(encoded.len() as u64, Ordering::Relaxed);
        
        // 检查轮转
        if self.current_size.load(Ordering::Relaxed) > self.rotate_size {
            self.rotate().await?;
        }
        
        Ok(lsn)
    }
    
    pub async fn recover(&self) -> Result<Vec<WalRecord>> {
        let mut file = tokio::fs::File::open(&self.path).await?;
        let mut records = Vec::new();
        let mut buf = vec![0u8; 4096];
        
        loop {
            let n = file.read(&mut buf).await?;
            if n == 0 { break; }
            // 解析记录，校验 crc32
            records.extend(WalRecord::decode_all(&buf[..n])?);
        }
        
        Ok(records)
    }
}
```

### 6.4 SSTable 实现

```rust
// seekstorm_core/src/storage/sstable.rs

pub struct SSTable {
    pub id: u32,
    pub path: PathBuf,
    pub index: Vec<(LsmKey, u64)>,  // (key, data_block_offset)
    pub entry_count: u32,
    io: Arc<dyn IoBackend>,
}

impl SSTable {
    pub async fn get(&self, key: &LsmKey) -> Result<Option<LsmValue>> {
        // 二分查找 index
        let block_offset = match self.index.binary_search_by(|(k, _)| k.cmp(key)) {
            Ok(i) => self.index[i].1,
            Err(i) if i > 0 => self.index[i - 1].1,
            _ => return Ok(None),
        };
        
        // 读取 data block
        let mut buf = vec![0u8; 4096];
        self.io.read_at(&self.path, block_offset, &mut buf).await?;
        
        // 在 block 内查找
        Self::find_in_block(&buf, key)
    }
    
    pub async fn scan_prefix(&self, namespace: u8, partition: u32) -> Result<Vec<(LsmKey, LsmValue)>> {
        // 顺序扫描所有 data block
        // ...
    }
}

pub struct SStableWriter {
    id: u32,
    path: PathBuf,
    io: Arc<dyn IoBackend>,
    current_block: Vec<u8>,
    blocks: Vec<(LsmKey, u64)>,  // (first_key, offset)
    current_offset: u64,
}

impl SStableWriter {
    pub async fn write(&mut self, key: LsmKey, value: LsmValue) -> Result<()> {
        let entry = self.encode_entry(&key, &value);
        
        if self.current_block.len() + entry.len() > 4096 {
            // 刷一个 block
            self.flush_block().await?;
        }
        
        if self.current_block.is_empty() {
            // 记录 block 的 first key
            self.blocks.push((key.clone(), self.current_offset));
        }
        
        self.current_block.extend_from_slice(&entry);
        Ok(())
    }
    
    pub async fn finish(mut self) -> Result<SSTable> {
        if !self.current_block.is_empty() {
            self.flush_block().await?;
        }
        
        // 写 index block
        let index_offset = self.current_offset;
        let mut index_buf = Vec::new();
        for (key, offset) in &self.blocks {
            index_buf.extend_from_slice(&key.encode());
            index_buf.extend_from_slice(&offset.to_le_bytes());
        }
        self.io.write_at(&self.path, index_offset, &index_buf).await?;
        
        // 写 footer
        let footer = Footer { index_offset, magic: "SKST_END" };
        self.io.write_at(&self.path, self.current_offset + index_buf.len() as u64, &footer.encode()).await?;
        
        Ok(SSTable { id: self.id, path: self.path, index: self.blocks, entry_count: 0, io: self.io })
    }
}
```

### 6.5 Manifest

```rust
// seekstorm_core/src/storage/manifest.rs

pub struct Manifest {
    pub index_id: IndexId,
    pub index_name: String,
    pub schema: DynamicSchema,
    pub config: IndexConfig,
    pub sstables: Vec<SSTableMeta>,
    pub wal_path: PathBuf,
    pub last_checkpoint_lsn: u64,
    pub created_at: u64,
    pub updated_at: u64,
}

impl Manifest {
    pub async fn save(&self, path: &Path) -> Result<()> {
        let tmp = path.with_extension("tmp");
        let json = serde_json::to_string_pretty(self)?;
        tokio::fs::write(&tmp, json).await?;
        tokio::fs::sync_directory(/* parent */).await?;
        // 原子 rename
        tokio::fs::rename(&tmp, path).await?;
        Ok(())
    }
    
    pub async fn load(path: &Path) -> Result<Self> {
        let json = tokio::fs::read_to_string(path).await?;
        Ok(serde_json::from_str(&json)?)
    }
}
```

---

## 7. Schemaless 改造

### 7.1 DynamicSchema

**设计原则**：
1. **堆外 schemaless**：用户写入任意 JSON，无需预定义 schema
2. **内部有类型系统**：系统按值自动推断字段类型并持久化
3. **默认索引规则**：
   - 所有**顶层标量属性**默认建**全文索引**（Text/I64/U64/F64/Bool/DateTime/Bytes）
   - **Vector 类型**默认建**向量索引**，不建词法索引
   - **Array（嵌套）类型**不建索引（后续阶段支持嵌套查询再开放）
4. 用户后续可通过 API 显式覆盖默认（如对某字段关闭索引、指定分词器）

```rust
// seekstorm_core/src/schema/mod.rs

pub struct DynamicSchema {
    fields: RwLock<IndexMap<String, FieldMeta>>,
    next_field_id: AtomicU32,
}

pub struct FieldMeta {
    pub id: FieldId,
    pub name: String,
    pub data_type: FieldType,
    pub stored: bool,              // 默认 true（schemaless 堆外存原文）
    pub index_lexical: bool,       // 默认 true 仅对标量类型
    pub index_vector: bool,        // 默认 true 仅对 Vector 类型
    pub vector_dim: Option<usize>,
    pub tokenizer: Option<TokenizerType>,
}

pub enum FieldType {
    // 标量（默认 index_lexical=true）
    Text,
    I64, U64, F64, Bool, DateTime,
    Bytes,
    // 向量（默认 index_vector=true，index_lexical=false）
    Vector(VectorSimilarity),
    // 嵌套（默认不索引）
    Array(Box<FieldType>),
}

impl DynamicSchema {
    pub async fn infer_and_add(&self, doc: &SchemalessDoc) -> Result<Vec<FieldChange>> {
        let mut changes = Vec::new();
        let mut fields = self.fields.write().await;

        for (name, value) in doc.fields() {
            if !fields.contains_key(name) {
                // 新字段，推断类型
                let field_type = infer_field_type(value);
                let meta = FieldMeta {
                    id: self.next_field_id.fetch_add(1, Ordering::Relaxed),
                    name: name.clone(),
                    data_type: field_type.clone(),
                    stored: true,
                    // 标量类型默认建词法索引；Vector 与 Array 不建
                    index_lexical: is_scalar(&field_type),
                    // 仅 Vector 类型建向量索引
                    index_vector: matches!(field_type, FieldType::Vector(_)),
                    vector_dim: vector_dim(value),
                    tokenizer: matches!(field_type, FieldType::Text)
                        .then_some(TokenizerType::Default),
                };
                fields.insert(name.clone(), meta);
                changes.push(FieldChange::Added(name.clone()));
            } else {
                // 类型检查
                let existing = fields.get(name).unwrap();
                let new_type = infer_field_type(value);
                if !type_compatible(&existing.data_type, &new_type) {
                    return Err(SchemaError::TypeConflict {
                        field: name.clone(),
                        existing: existing.data_type.clone(),
                        got: new_type,
                    });
                }
            }
        }

        Ok(changes)
    }
}

fn is_scalar(t: &FieldType) -> bool {
    matches!(t, FieldType::Text | FieldType::I64 | FieldType::U64
        | FieldType::F64 | FieldType::Bool | FieldType::DateTime
        | FieldType::Bytes)
}
```

**词法索引对非 Text 字段的编码**：数值/Bool/DateTime 在写入 posting 前需统一序列化为 term 字符串（如 `i64.to_string()`、`true/false`、RFC3339 字符串），查询时同样序列化。Bytes 走 base64 编码。

### 7.2 类型推断

```rust
// seekstorm_core/src/schema/inference.rs

pub fn infer_field_type(value: &serde_json::Value) -> FieldType {
    match value {
        serde_json::Value::String(s) => {
            // 检查 RFC3339 时间
            if chrono::DateTime::parse_from_rfc3339(s).is_ok() {
                return FieldType::DateTime;
            }
            // 检查是否是向量（base64 + __binary）
            if s.starts_with("__vec__") {
                return FieldType::Vector(VectorSimilarity::Cosine);
            }
            FieldType::Text
        }
        serde_json::Value::Number(n) => {
            if n.is_i64() { FieldType::I64 }
            else if n.is_u64() { FieldType::U64 }
            else { FieldType::F64 }
        }
        serde_json::Value::Bool(_) => FieldType::Bool,
        serde_json::Value::Array(arr) => {
            if !arr.is_empty() && arr.iter().all(|v| v.is_number()) {
                // 数值数组 → 向量
                FieldType::Vector(VectorSimilarity::Cosine)
            } else if !arr.is_empty() {
                let inner = infer_field_type(&arr[0]);
                FieldType::Array(Box::new(inner))
            } else {
                FieldType::Array(Box::new(FieldType::Text))
            }
        }
        _ => FieldType::Bytes,
    }
}
```

### 7.3 SchemalessDoc

```rust
// seekstorm_core/src/index/document.rs

pub struct SchemalessDoc {
    pub fields: IndexMap<String, serde_json::Value>,
}

impl SchemalessDoc {
    pub fn from_json(json: &str) -> Result<Self> {
        let value: serde_json::Value = serde_json::from_str(json)?;
        match value {
            serde_json::Value::Object(map) => {
                let fields: IndexMap<_, _> = map.into_iter().collect();
                Ok(Self { fields })
            }
            _ => Err(anyhow!("document must be JSON object"))
        }
    }
    
    pub fn fields(&self) -> impl Iterator<Item = (&String, &serde_json::Value)> {
        self.fields.iter()
    }
}
```

---

## 8. 删除机制（完整整合）

### 8.1 统一删除流程

```rust
// seekstorm_core/src/index/mod.rs

impl Index {
    pub async fn delete_document(&self, doc_id: DocId) -> Result<()> {
        // [1] WAL 记录
        self.lsm.wal_append(WalOp::Delete { doc_id }).await?;
        self.lsm.wal_fsync().await?;
        
        // [2] DocStore tombstone（LSM 自动处理）
        self.lsm.delete(0x01, doc_id).await?;
        
        // [3] 向量分区 tombstone
        if let Some(partition_id) = self.vector_engine.find_partition(doc_id).await {
            self.vector_engine.delete_vector(doc_id, partition_id).await?;
        }
        
        // [4] tantivy 删除
        self.lexical_engine.delete_document(doc_id).await?;
        
        Ok(())
    }
    
    pub async fn batch_delete(&self, doc_ids: Vec<DocId>) -> Result<()> {
        // 单 WAL 记录，减少 fsync
        self.lsm.wal_append(WalOp::BatchDelete { doc_ids: doc_ids.clone() }).await?;
        
        for doc_id in &doc_ids {
            self.lsm.delete(0x01, *doc_id).await?;
            if let Some(pid) = self.vector_engine.find_partition(*doc_id).await {
                self.vector_engine.delete_vector(*doc_id, pid).await?;
            }
            self.lexical_engine.delete_document(*doc_id).await?;
        }
        
        Ok(())
    }
}
```

### 8.2 物理清理

| 命名空间 | 清理时机 | 清理方式 |
|---|---|---|
| DocStore | SSTable compaction | 合并时跳过 tombstone，丢弃物理数据 |
| VectorPart | LIRE-aware compaction | 分区 tombstone 比例 > 20% 触发重写 |
| TantivySeg | tantivy segment merge | tantivy 自动应用 `.del` |

### 8.3 查询时过滤

```rust
impl Index {
    pub async fn get_document(&self, doc_id: DocId) -> Result<Option<Document>> {
        let value = self.lsm.get(&LsmKey::doc_store(doc_id)).await?;
        match value {
            Some(LsmValue::Data(bytes)) => Ok(Some(Document::from_bytes(&bytes)?)),
            Some(LsmValue::Tombstone) => Ok(None),
            None => Ok(None),
        }
    }
}
```

### 8.4 硬删除 API（合规场景）

```rust
impl Index {
    pub async fn hard_delete(&self, doc_id: DocId) -> Result<()> {
        // 软删除
        self.delete_document(doc_id).await?;
        
        // 强制 compaction
        self.lsm.force_compact().await?;
        self.lexical_engine.force_merge().await?;
        self.vector_engine.force_compact_partition(doc_id).await?;
        
        Ok(())
    }
}
```

---

## 9. 查询规划器与融合

### 9.1 QueryMode

```rust
// seekstorm_core/src/query/planner.rs

pub enum QueryMode {
    PureLexical,
    PureVector,
    HybridLexicalRerank,
    HybridVectorRerank,
    HybridParallel,
    Auto,
}

pub fn select_mode(query: &QueryRequest) -> QueryMode {
    if query.mode != QueryMode::Auto {
        return query.mode;
    }
    
    match (query.text.is_some(), query.vector.is_some()) {
        (true, false) => QueryMode::PureLexical,
        (false, true) => QueryMode::PureVector,
        (true, Some(_)) if query.text.as_ref().map(|t| t.len()).unwrap_or(0) < 32 => {
            QueryMode::HybridLexicalRerank
        }
        (true, Some(_)) => QueryMode::HybridVectorRerank,
        _ => QueryMode::PureLexical,
    }
}
```

### 9.2 RRF 融合

```rust
// seekstorm_core/src/query/fusion.rs

pub fn rrf_fusion(
    lexical_results: Vec<ScoredDoc>,
    vector_results: Vec<ScoredDoc>,
    k: u32,  // 默认 60
    top_k: usize,
) -> Vec<ScoredDoc> {
    let mut scores: HashMap<DocId, f64> = HashMap::new();
    
    for (rank, doc) in lexical_results.iter().enumerate() {
        *scores.entry(doc.doc_id).or_default() += 1.0 / (k as f64 + rank as f64 + 1.0);
    }
    
    for (rank, doc) in vector_results.iter().enumerate() {
        *scores.entry(doc.doc_id).or_default() += 1.0 / (k as f64 + rank as f64 + 1.0);
    }
    
    let mut merged: Vec<_> = scores.into_iter()
        .map(|(doc_id, score)| ScoredDoc { doc_id, score })
        .collect();
    merged.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap());
    merged.truncate(top_k);
    merged
}
```

---

## 10. 数据流

### 10.1 索引流程

```
HTTP POST /index/{id}/document
        │
        ▼
[1] 鉴权 + 解析 JSON
        │
        ▼
[2] SchemalessDoc::from_json
        │
        ▼
[3] DynamicSchema.infer_and_add(doc)  ← 可能新增字段
        │
        ▼
[4] 分配 doc_id
        │
        ▼
[5] WAL.append(Insert { doc_id, payload })
        │
        ▼
   ┌────┴────────────────────┐
   ▼                         ▼
[6] DocStore:               [7] 若有向量字段：
   LsmEngine.put(0x01,       VectorEngine.add_vector(doc_id, vec)
     doc_id, doc_bytes)        │
                              ▼
                          [7a] 量化
                              │
                              ▼
                          [7b] 找最近质心
                              │
                              ▼
                          [7c] LsmEngine.put(0x02,
                                    partition_id, doc_id, vec)
                              │
                              ▼
                          [7d] 若溢出 → LIRE 分裂
   │
   ▼
[8] LexicalEngine.add_document(doc_id, doc)
   │
   ▼
[9] 返回 { doc_id, status: indexed }
   │
   ▼
（异步）CommitTask：
  - tantivy writer.commit()
  - LsmEngine.maybe_flush() (MemTable 满)
  - LsmEngine.maybe_compact() (SSTable 数 ≥ 4)
```

### 10.2 检索流程

```
HTTP POST /index/{id}/query
        │
        ▼
[1] 解析 QueryRequest
        │
        ▼
[2] QueryPlanner::select_mode(req)
        │
        ▼
   ┌────┴────────────────────────────────────┐
   ▼                                         ▼
[3a] LexicalEngine.search                  [3b] VectorEngine.search
   - tantivy Searcher                       - 找 top-nprobe 质心
   - BM25 top-k                             - 扫描分区（跳过 tombstone）
   - 应用 filter                            - 应用 filter
   │                                         │
   ▼                                         ▼
   排序结果 L                              排序结果 V
   │                                         │
   └──────────────┬──────────────────────────┘
                  ▼
[4] Fusion::rrf(L, V, k=60, top_k)
                  │
                  ▼
[5] 懒加载存储字段（从 DocStore）
                  │
                  ▼
[6] 返回 SearchResponse
```

---

## 11. 崩溃恢复

```rust
// seekstorm_core/src/index/mod.rs

impl Index {
    pub async fn open(path: &Path) -> Result<Self> {
        // [1] 加载 manifest
        let manifest = Manifest::load(&path.join("manifest.json")).await?;
        
        // [2] 初始化 LSM
        let lsm = LsmEngine::open(
            path.join("lsm"),
            manifest.config.lsm_config.clone(),
        ).await?;
        
        // [3] 重放 WAL（从 last_checkpoint_lsn）
        let wal_records = lsm.wal.recover().await?;
        for record in wal_records.iter().filter(|r| r.lsn > manifest.last_checkpoint_lsn) {
            match &record.op {
                WalOp::Put { key, value } => {
                    lsm.apply_put(key.clone(), value.clone()).await?;
                }
                WalOp::Delete { doc_id } => {
                    lsm.apply_delete(0x01, *doc_id).await?;
                }
                WalOp::PartitionSplit { old, new, moved_count } => {
                    lsm.apply_partition_split(*old, *new, *moved_count).await?;
                }
                _ => {}
            }
        }
        
        // [4] 加载 schema
        let schema = Arc::new(RwLock::new(manifest.schema));
        
        // [5] 初始化词法引擎
        let lexical = LexicalEngine::new(lsm.clone(), schema.clone(), &manifest.config.lexical).await?;
        
        // [6] 初始化向量引擎
        let vector = VectorEngine::open(lsm.clone(), &manifest.config.vector).await?;
        
        Ok(Self {
            lsm,
            lexical,
            vector,
            schema,
            manifest: RwLock::new(manifest),
        })
    }
}
```

---

## 12. 并发模型

| 操作 | 并发策略 |
|---|---|
| 文档插入 | 单 WAL 写入者（串行），多 reader |
| 文档删除 | 同插入 |
| MemTable 写 | `RwLock<Arc<MemTable>>`，写时复制切换 |
| MemTable flush | 双 MemTable（active + flushing），不阻塞写 |
| SSTable 读 | `Arc<Vec<Arc<SSTable>>>`，无锁读 |
| Compaction | `Mutex` 串行化，长任务后台运行 |
| 向量检索 | `RwLock<Arc<IvfIndex>>`，无锁读 |
| LIRE 分裂 | 分区级锁，新分区原子替换 |
| Schema 变更 | `RwLock<DynamicSchema>` 写锁（罕见） |
| tantivy 写 | `Mutex<IndexWriter>`（tantivy 要求单写） |
| tantivy 读 | `IndexReader` 多读 |

---

## 13. API 变更

### 13.1 库 API

```rust
// 新的 builder 模式
let index = IndexBuilder::new("/data/myindex")
    .schemaless(true)
    .lexical(LexicalConfig::default())
    .vector(VectorConfig {
        similarity: VectorSimilarity::Cosine,
        quantization: Quantization::TurboQuant,
        max_partition_size: 100_000,
        ..Default::default()
    })
    .storage(StorageConfig {
        memtable_max_bytes: 64 * 1024 * 1024,
        sstable_compact_threshold: 4,
        wal_sync: WalSync::EveryCommit,
        io_backend: IoBackendKind::Auto,
        ..Default::default()
    })
    .build()
    .await?;

// Schemaless 插入
index.add_document(json!({
    "title": "Rust 检索",
    "body": "tantivy 很快",
    "tags": ["search", "rust"],
    "embedding": vec![0.1_f32; 384],
})).await?;

// 混合检索
let results = index.search(QueryRequest {
    text: Some("fast search engine".into()),
    vector: Some(query_vector),
    mode: QueryMode::HybridParallel,
    filters: vec![Filter::new("tags").equals("rust")],
    top_k: 10,
}).await?;
```

### 13.2 HTTP API（保留 v3 兼容）

| 端点 | 方法 | 说明 |
|---|---|---|
| `POST /index/{id}` | POST | 创建索引（schemaless 默认开启） |
| `POST /index/{id}/document` | POST | 插入文档（schemaless） |
| `POST /index/{id}/query` | POST | 混合查询 |
| `DELETE /index/{id}/document/{doc_id}` | DELETE | 删除文档 |
| `POST /index/{id}/commit` | POST | 强制 commit |
| `GET /index/{id}/schema` | GET | **新增**：返回当前推断的 schema |
| `POST /index/{id}/compact` | POST | **新增**：强制 compaction |

---

## 14. 实施阶段

### Phase 1：MVP（2-3 周）

**目标**：简易 LSM + tantivy 集成 + schemaless 插入与查询

**任务**：
1. 创建 `seekstorm_core` crate 骨架
2. 实现 `storage/lsm.rs`（MemTable + SSTable + WAL + compaction）
3. 实现 `lexical/lsm_directory.rs`（tantivy Directory trait）
4. 实现 `schema/mod.rs` + `schema/inference.rs`
5. 实现 `index/mod.rs`（add_document / search 基本流程）
6. 单元测试 + 集成测试

**交付**：单索引库，纯词法检索可用，schemaless 插入

### Phase 2：LIRE 协议（2-3 周）

**目标**：SPFresh 风格向量索引

**任务**：
1. 实现 `vector/ivf_index.rs`（IVF 基础结构）
2. 实现 `vector/lire.rs`（分裂协议）
3. 实现 `vector/partition.rs`（分区管理）
4. 实现 `vector/quantization.rs`（TurboQuant / SQ）
5. 复用 v3 的 `clustering.rs`（K-Medoid 初始质心）
6. 向量检索单元测试

**交付**：纯向量检索可用

### Phase 3：混合检索（1-2 周）

**目标**：查询规划器 + RRF 融合

**任务**：
1. 实现 `query/planner.rs`
2. 实现 `query/fusion.rs`（RRF）
3. 实现 `query/filter.rs`（字段过滤）
4. 实现 `search/hybrid_search.rs`
5. 混合检索测试

**交付**：完整混合检索

### Phase 4：删除与崩溃恢复（1 周）

**目标**：完整删除 + 崩溃恢复

**任务**：
1. 实现 tombstone 机制
2. 实现 LIRE-aware 分区 compaction
3. 实现崩溃恢复流程
4. 故障注入测试

**交付**：生产级持久性

### Phase 5：服务器适配（1 周）

**目标**：HTTP API 兼容

**任务**：
1. 适配 `seekstorm_server` 到新 API
2. 保留 v3 HTTP 端点
3. 添加新端点（`GET /schema`、`POST /compact`）
4. 集成测试

**交付**：独立服务器二进制

### Phase 6：可观测性（1 周）

**目标**：生产就绪

**任务**：
1. Prometheus 指标
2. tracing 集成
3. 查询 explain
4. 性能基准测试

**交付**：生产就绪

**总周期**：约 8-11 周

---

## 15. 关键代码骨架清单

### 必须新实现的文件

| 文件 | 行数预估 | 核心内容 |
|---|---|---|
| `storage/lsm.rs` | ~800 | MemTable + SSTable 管理 + compaction |
| `storage/sstable.rs` | ~500 | SSTable 读写 |
| `storage/wal.rs` | ~300 | WAL |
| `storage/io_backend.rs` | ~200 | IoBackend trait + 实现 |
| `storage/manifest.rs` | ~150 | Manifest |
| `lexical/mod.rs` | ~300 | LexicalEngine |
| `lexical/lsm_directory.rs` | ~250 | tantivy Directory |
| `lexical/tantivy_adapter.rs` | ~200 | schema 桥接 |
| `vector/mod.rs` | ~200 | VectorEngine |
| `vector/ivf_index.rs` | ~400 | IVF 索引 |
| `vector/lire.rs` | ~500 | LIRE 协议 |
| `vector/partition.rs` | ~300 | 分区管理 |
| `vector/quantization.rs` | ~400 | TurboQuant / SQ |
| `vector/similarity.rs` | ~600 | SIMD 相似度 |
| `schema/mod.rs` | ~200 | DynamicSchema |
| `schema/inference.rs` | ~150 | 类型推断 |
| `index/mod.rs` | ~400 | Index 统一抽象 |
| `index/document.rs` | ~100 | SchemalessDoc |
| `index/ingest.rs` | ~200 | 摄入流水线 |
| `query/planner.rs` | ~100 | QueryMode 选择 |
| `query/fusion.rs` | ~100 | RRF |
| `search/*.rs` | ~600 | 检索实现 |
| **总计** | ~6350 | |

### 从 v3 复用

| 文件 | 行数 | 说明 |
|---|---|---|
| `min_heap.rs` | 1260 | top-k 排序堆 |
| `clustering.rs` | - | K-Medoid PAM |
| `utils.rs` | 203 | 工具函数 |

---

## 16. 测试策略

### 16.1 单元测试

每个模块独立单元测试：
- `lsm.rs`：MemTable put/get/scan、SSTable 读写、compaction 正确性
- `wal.rs`：append、recover、轮转
- `lsm_directory.rs`：tantivy Directory trait 行为
- `lire.rs`：分裂、迁移、tombstone
- `inference.rs`：各 JSON 类型推断

### 16.2 集成测试

```rust
// seekstorm_core/tests/integration.rs

#[tokio::test]
async fn test_schemaless_insert_and_search() {
    let index = IndexBuilder::new(tempdir()).build().await.unwrap();
    
    // 插入不同 schema 的文档
    index.add_document(json!({"title": "hello", "body": "world"})).await.unwrap();
    index.add_document(json!({"title": "foo", "body": "bar", "tags": ["a", "b"]})).await.unwrap();
    index.add_document(json!({"title": "baz", "embedding": vec![0.1; 384]})).await.unwrap();
    
    index.commit().await.unwrap();
    
    // 检索
    let results = index.search(QueryRequest {
        text: Some("hello".into()),
        mode: QueryMode::PureLexical,
        top_k: 10,
        ..Default::default()
    }).await.unwrap();
    
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].doc_id, 0);
}

#[tokio::test]
async fn test_delete_and_recover() {
    let index = IndexBuilder::new(tempdir()).build().await.unwrap();
    
    let doc_id = index.add_document(json!({"title": "test"})).await.unwrap();
    index.delete_document(doc_id).await.unwrap();
    
    // 立即查询，应不存在
    let doc = index.get_document(doc_id).await.unwrap();
    assert!(doc.is_none());
    
    // 模拟崩溃重启
    drop(index);
    let index = Index::open(tempdir()).await.unwrap();
    
    // 重启后仍应不存在
    let doc = index.get_document(doc_id).await.unwrap();
    assert!(doc.is_none());
}

#[tokio::test]
async fn test_lire_split() {
    let index = IndexBuilder::new(tempdir())
        .vector(VectorConfig {
            max_partition_size: 100,  // 小阈值触发分裂
            ..Default::default()
        })
        .build()
        .await.unwrap();
    
    // 插入 200 个向量，触发分裂
    for i in 0..200 {
        let vec = vec![i as f32 / 200.0; 384];
        index.add_document(json!({"embedding": vec})).await.unwrap();
    }
    
    // 验证分区数 > 1
    let partition_count = index.vector_engine.partition_count().await;
    assert!(partition_count > 1);
}
```

### 16.3 性能基准

```rust
// seekstorm_core/benches/bench.rs

#[bench]
fn bench_insert(b: &mut Bencher) {
    b.iter(|| {
        // 插入 1000 文档
    });
}

#[bench]
fn bench_search(b: &mut Bencher) {
    b.iter(|| {
        // 检索 top-10
    });
}
```

### 16.4 故障注入测试

- 写入中途 kill 进程 → 重启后状态一致
- compaction 中途 kill → 重启后 SSTable 完整
- WAL 损坏 → 启动失败并报错（不破坏数据）

---

## 17. 风险与对策

| 风险 | 概率 | 对策 |
|---|---|---|
| tantivy schemaless 运行时新增字段开销大 | 中 | 性能测试，必要时限制字段数 |
| LIRE 分裂时质心表更新开销 | 中 | 增量更新，避免全表重建 |
| LSM 读放大（多 SSTable） | 中 | moka 页缓存 + compaction 频率调优 |
| Windows 无 io_uring | 高 | 回退 tokio::fs，可接受 |
| tantivy Directory trait 异步阻塞 | 中 | 用 `spawn_blocking` 包装，或自定义异步 Directory |
| 向量分区 tombstone 累积 | 中 | tombstone 比例 > 20% 触发重写 |
| MemTable flush 期间写阻塞 | 低 | 双 MemTable 切换 |
| Compaction 风暴 | 低 | 串行 compaction + 限速 |

---

## 18. 参考资料

- [tantivy](https://github.com/quickwit-oss/tantivy)
- [tantivy 0.21](https://quickwit.io/blog/tantivy-0.21)
- [tantivy Directory trait](https://docs.rs/tantivy/latest/tantivy/directory/trait.Directory.html)
- [SPFresh (SOSP 2023)](https://www.microsoft.com/en-us/research/?p=1075902)
- [SPFresh GitHub](https://github.com/SPFresh/SPFresh)
- [Async hazard: mmap is secretly blocking IO](https://huonw.github.io/blog/2024/08/async-hazard-mmap/)
- [tokio-epoll-uring](https://github.com/neondatabase/neon/pull/9546)
- [fjall: Rust LSM-tree](https://github.com/fjall-rs/fjall)（参考实现）
- [RocksDB LSM design](https://github.com/facebook/rocksdb/wiki)
- [Turbopuffer architecture (uses SPFresh)](https://lqhl.me/blog/turbopuffer/)
