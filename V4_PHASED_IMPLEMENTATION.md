# SeekStorm v4 分阶段改造文档

> 本文档将 v4 改造划分为 9 个阶段，每阶段可独立交付、独立测试。建议按顺序执行，但 Phase 4-5 与 Phase 3 可并行。

```
Phase 1: LSM 存储引擎核心           ─── 基础设施
   │
   ▼
Phase 2: Schemaless + 文档存储      ─── 数据模型
   │
   ├──▶ Phase 3: tantivy 集成       ─── 词法引擎（可与 4-5 并行）
   │
   └──▶ Phase 4: 基础 IVF          ─── 向量引擎基础
            │
            ▼
        Phase 5: LIRE 协议         ─── 向量实时更新
            │
            ▼
        Phase 6: 混合检索 + RRF    ─── 融合
            │
            ▼
        Phase 7: 删除 + 崩溃恢复   ─── 持久性
            │
            ▼
        Phase 8: 服务器适配       ─── HTTP
            │
            ▼
        Phase 9: 可观测性 + 优化  ─── 生产就绪
```

---

## Phase 1：LSM 存储引擎核心

### 目标
实现简易 LSM 引擎，提供 KV 读写、scan、tombstone、compaction，作为所有上层模块的存储基础。

### 前置依赖
- 无

### 任务清单

#### 1.1 创建 crate 骨架

**文件**：
- `seekstorm_core/Cargo.toml`
- `seekstorm_core/src/lib.rs`

**`Cargo.toml` 关键依赖**：
```toml
[dependencies]
crossbeam-skiplist = "0.1"
moka = { version = "0.12", features = ["future"] }
crc32fast = "1.4"
tokio = { version = "1", features = ["full"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
bytemuck = "1"
ahash = "0.8"
anyhow = "1"
thiserror = "1"

[target.'cfg(target_os = "linux")'.dependencies]
tokio-epoll-uring = "0.5"
```

#### 1.2 实现 IoBackend trait

**文件**：`seekstorm_core/src/storage/io_backend.rs`

```rust
#[async_trait]
pub trait IoBackend: Send + Sync {
    async fn read_at(&self, path: &Path, offset: u64, buf: &mut [u8]) -> Result<usize>;
    async fn write_at(&self, path: &Path, offset: u64, buf: &[u8]) -> Result<usize>;
    async fn fsync(&self, path: &Path) -> Result<()>;
    async fn create_file(&self, path: &Path) -> Result<()>;
    async fn delete_file(&self, path: &Path) -> Result<()>;
    async fn file_size(&self, path: &Path) -> Result<u64>;
    async fn sync_dir(&self, path: &Path) -> Result<()>;
}

pub struct AsyncFsBackend { /* tokio::fs + spawn_blocking */ }

#[cfg(target_os = "linux")]
pub struct IoUringBackend { /* tokio-epoll-uring */ }

pub fn select_backend(kind: IoBackendKind) -> Arc<dyn IoBackend> { ... }
```

**验收**：能在 Linux/Windows/macOS 上读写文件，跨平台行为一致。

#### 1.3 实现 WAL

**文件**：`seekstorm_core/src/storage/wal.rs`

```rust
pub struct Wal {
    path: PathBuf,
    file: Mutex<tokio::fs::File>,
    sync_policy: WalSync,
    current_size: AtomicU64,
    rotate_size: u64,
    next_lsn: AtomicU64,
}

pub enum WalSync { EveryCommit, Periodic(Duration), None }

pub enum WalOp {
    Put { key: LsmKey, value: LsmValue },
    Delete { namespace: u8, doc_id: u64 },
    PartitionSplit { old: u32, new: u32, moved: u64 },
    Checkpoint { last_applied_lsn: u64 },
}

impl Wal {
    pub async fn append(&self, op: WalOp) -> Result<u64>;  // 返回 lsn
    pub async fn fsync(&self) -> Result<()>;
    pub async fn recover(&self) -> Result<Vec<WalRecord>>;
    pub async fn rotate(&self) -> Result<()>;
    pub async fn truncate_to(&self, lsn: u64) -> Result<()>;
}
```

**记录格式**：
```
[len:u32][lsn:u64][op:u8][payload:bytes][crc32:u32]
```

#### 1.4 实现 MemTable

**文件**：`seekstorm_core/src/storage/lsm.rs`（部分）

```rust
pub struct MemTable {
    table: SkipMap<LsmKey, LsmValue>,
    bytes: AtomicU64,
}

impl MemTable {
    pub fn insert(&self, key: LsmKey, value: LsmValue);
    pub fn get(&self, key: &LsmKey) -> Option<LsmValue>;
    pub fn range_scan(&self, ns: u8, partition: u32) -> Vec<(LsmKey, LsmValue)>;
    pub fn approx_bytes(&self) -> u64;
    pub fn into_iter(self) -> impl Iterator<Item = (LsmKey, LsmValue)>;
}
```

#### 1.5 实现 SSTable

**文件**：`seekstorm_core/src/storage/sstable.rs`

```rust
pub struct SSTable {
    pub id: u32,
    pub path: PathBuf,
    pub index: Vec<(LsmKey, u64)>,  // (first_key, block_offset)
    pub entry_count: u32,
    pub min_key: LsmKey,
    pub max_key: LsmKey,
    io: Arc<dyn IoBackend>,
}

impl SSTable {
    pub async fn open(path: PathBuf, io: Arc<dyn IoBackend>) -> Result<Self>;
    pub async fn get(&self, key: &LsmKey) -> Result<Option<LsmValue>>;
    pub async fn scan_prefix(&self, ns: u8, partition: u32) -> Result<Vec<(LsmKey, LsmValue)>>;
    pub async fn delete_file(&self) -> Result<()>;
}

pub struct SStableWriter {
    id: u32,
    path: PathBuf,
    io: Arc<dyn IoBackend>,
    current_block: Vec<u8>,
    block_index: Vec<(LsmKey, u64)>,
    current_offset: u64,
    entry_count: u32,
    min_key: Option<LsmKey>,
    max_key: Option<LsmKey>,
}

impl SStableWriter {
    pub fn new(id: u32, path: PathBuf, io: Arc<dyn IoBackend>) -> Self;
    pub async fn write(&mut self, key: LsmKey, value: LsmValue) -> Result<()>;
    pub async fn finish(self) -> Result<SSTable>;
}
```

**SSTable 文件格式**：
```
[Header: magic/version/id/entry_count]
[Data block 0 (4 KiB)] [Data block 1] ...
[Index block]
[Footer: index_offset/magic]
```

#### 1.6 实现 LsmEngine

**文件**：`seekstorm_core/src/storage/lsm.rs`

```rust
pub struct LsmEngine {
    memtable: RwLock<Arc<MemTable>>,
    flushing: RwLock<Option<Arc<MemTable>>>,
    sstables: RwLock<Vec<Arc<SSTable>>>,
    wal: Arc<Wal>,
    io: Arc<dyn IoBackend>,
    config: LsmConfig,
    compaction_lock: Mutex<()>,
    next_lsn: AtomicU64,
    next_sstable_id: AtomicU32,
}

impl LsmEngine {
    pub async fn open(dir: &Path, config: LsmConfig) -> Result<Self>;
    pub async fn put(&self, key: LsmKey, value: LsmValue) -> Result<u64>;
    pub async fn delete(&self, namespace: u8, doc_id: u64) -> Result<u64>;
    pub async fn get(&self, key: &LsmKey) -> Result<Option<LsmValue>>;
    pub async fn scan_prefix(&self, namespace: u8, partition: u32) -> Result<Vec<(LsmKey, LsmValue)>>;
    
    async fn maybe_flush(&self) -> Result<()>;
    async fn flush_memtable(&self, memtable: &Arc<MemTable>) -> Result<SSTable>;
    async fn trigger_compaction(&self) -> Result<()>;
    async fn compact_sstables(&self, sstables: &[Arc<SSTable>]) -> Result<SSTable>;
    pub async fn force_compact(&self) -> Result<()>;
}
```

#### 1.7 实现 Manifest

**文件**：`seekstorm_core/src/storage/manifest.rs`

```rust
pub struct Manifest {
    pub index_id: u64,
    pub index_name: String,
    pub sstables: Vec<SSTableMeta>,
    pub wal_path: PathBuf,
    pub last_checkpoint_lsn: u64,
    pub created_at: u64,
    pub updated_at: u64,
}

impl Manifest {
    pub async fn save(&self, path: &Path) -> Result<()>;
    pub async fn load(path: &Path) -> Result<Self>;
    pub async fn add_sstable(&mut self, meta: SSTableMeta) -> Result<()>;
    pub async fn replace_sstables(&mut self, old: Vec<u32>, new: SSTableMeta) -> Result<()>;
}
```

### 测试用例

```rust
// seekstorm_core/tests/lsm_test.rs

#[tokio::test]
async fn test_put_get_basic() {
    let lsm = LsmEngine::open(tempdir(), LsmConfig::default()).await.unwrap();
    let key = LsmKey { namespace: 0x01, partition_or_segment: 0, doc_id: 1, lsn: 0 };
    lsm.put(key.clone(), LsmValue::Data(b"hello".to_vec())).await.unwrap();
    let v = lsm.get(&key).await.unwrap();
    assert!(matches!(v, Some(LsmValue::Data(ref b)) if b == b"hello"));
}

#[tokio::test]
async fn test_tombstone() {
    let lsm = LsmEngine::open(tempdir(), LsmConfig::default()).await.unwrap();
    lsm.put(LsmKey::doc(1), LsmValue::Data(b"hello".to_vec())).await.unwrap();
    lsm.delete(0x01, 1).await.unwrap();
    let v = lsm.get(&LsmKey::doc(1)).await.unwrap();
    assert!(matches!(v, Some(LsmValue::Tombstone)));
}

#[tokio::test]
async fn test_compaction_drops_tombstone() {
    let mut cfg = LsmConfig::default();
    cfg.memtable_max_bytes = 1024;  // 小阈值触发 flush
    cfg.sstable_compact_threshold = 2;
    let lsm = LsmEngine::open(tempdir(), cfg).await.unwrap();
    
    lsm.put(LsmKey::doc(1), LsmValue::Data(b"v1".to_vec())).await.unwrap();
    lsm.force_flush().await.unwrap();
    lsm.delete(0x01, 1).await.unwrap();
    lsm.force_flush().await.unwrap();
    lsm.force_compact().await.unwrap();
    
    let v = lsm.get(&LsmKey::doc(1)).await.unwrap();
    assert!(v.is_none());  // tombstone 已被清理
}

#[tokio::test]
async fn test_scan_prefix() {
    let lsm = LsmEngine::open(tempdir(), LsmConfig::default()).await.unwrap();
    lsm.put(LsmKey::vec(5, 1), LsmValue::Data(b"v1".to_vec())).await.unwrap();
    lsm.put(LsmKey::vec(5, 2), LsmValue::Data(b"v2".to_vec())).await.unwrap();
    lsm.put(LsmKey::vec(7, 3), LsmValue::Data(b"v3".to_vec())).await.unwrap();
    
    let results = lsm.scan_prefix(0x02, 5).await.unwrap();
    assert_eq!(results.len(), 2);
}

#[tokio::test]
async fn test_wal_recovery() {
    let dir = tempdir();
    let lsm = LsmEngine::open(&dir, LsmConfig::default()).await.unwrap();
    lsm.put(LsmKey::doc(1), LsmValue::Data(b"v1".to_vec())).await.unwrap();
    lsm.put(LsmKey::doc(2), LsmValue::Data(b"v2".to_vec())).await.unwrap();
    drop(lsm);
    
    // 重启后恢复
    let lsm = LsmEngine::open(&dir, LsmConfig::default()).await.unwrap();
    assert!(lsm.get(&LsmKey::doc(1)).await.unwrap().is_some());
    assert!(lsm.get(&LsmKey::doc(2)).await.unwrap().is_some());
}
```

### 验收标准
- [x] `put` / `get` / `delete` / `scan_prefix` 工作正常
- [x] MemTable flush 到 SSTable 正确
- [x] Compaction 合并多个 SSTable，清理 tombstone
- [x] WAL 崩溃恢复后状态一致
- [x] 单线程写吞吐 ≥ 100K ops/s（MemTable 命中）

### 交付物
- `seekstorm_core` crate 可编译
- `storage/` 模块完整实现
- LSM 单元测试 + 集成测试通过

---

## Phase 2：Schemaless + 文档存储

### 目标
在 LSM 之上实现动态 schema、文档序列化、基本的插入/查询/删除。

### 前置依赖
- Phase 1 完成

### 任务清单

#### 2.1 实现 DynamicSchema

**文件**：
- `seekstorm_core/src/schema/mod.rs`
- `seekstorm_core/src/schema/field.rs`
- `seekstorm_core/src/schema/inference.rs`

**设计原则**：
1. **堆外 schemaless**：用户写入任意 JSON，无需预定义 schema
2. **内部有类型系统**：系统按值自动推断字段类型并持久化
3. **默认索引规则**：
   - 所有**顶层标量属性**默认建**全文索引**（Text/I64/U64/F64/Bool/DateTime/Bytes）
   - **Vector 类型**默认建**向量索引**，不建词法索引
   - **Array（嵌套）类型**不建索引（后续阶段支持嵌套查询再开放）
4. 用户后续可通过 API 显式覆盖默认（如对某字段关闭索引、指定分词器）

```rust
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
    Text, I64, U64, F64, Bool, DateTime, Bytes,
    // 向量（默认 index_vector=true，index_lexical=false）
    Vector(VectorSimilarity),
    // 嵌套（默认不索引）
    Array(Box<FieldType>),
}

impl DynamicSchema {
    pub async fn infer_and_add(&self, doc: &SchemalessDoc) -> Result<Vec<FieldChange>>;
    pub async fn get_field(&self, name: &str) -> Option<FieldMeta>;
    pub async fn snapshot(&self) -> Vec<FieldMeta>;
    pub async fn serialize(&self) -> Vec<u8>;
    pub async fn deserialize(bytes: &[u8]) -> Result<Self>;
}
```

**类型推断规则**（`inference.rs`）：
```rust
pub fn infer_field_type(value: &serde_json::Value) -> FieldType {
    match value {
        Value::String(s) => {
            if chrono::DateTime::parse_from_rfc3339(s).is_ok() {
                FieldType::DateTime
            } else {
                FieldType::Text
            }
        }
        Value::Number(n) => {
            if n.is_i64() { FieldType::I64 }
            else if n.is_u64() { FieldType::U64 }
            else { FieldType::F64 }
        }
        Value::Bool(_) => FieldType::Bool,
        Value::Array(arr) => {
            if !arr.is_empty() && arr.iter().all(|v| v.is_number()) {
                // 数值数组 → 向量
                FieldType::Vector(VectorSimilarity::Cosine)
            } else if !arr.is_empty() {
                // 非数值数组 → 嵌套类型（不索引）
                FieldType::Array(Box::new(infer_field_type(&arr[0])))
            } else {
                FieldType::Array(Box::new(FieldType::Text))
            }
        }
        _ => FieldType::Bytes,
    }
}
```

**默认索引赋值规则**（`infer_and_add` 内）：
```rust
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

fn is_scalar(t: &FieldType) -> bool {
    matches!(t, FieldType::Text | FieldType::I64 | FieldType::U64
        | FieldType::F64 | FieldType::Bool | FieldType::DateTime
        | FieldType::Bytes)
}
```

**词法索引对非 Text 字段的编码**：数值/Bool/DateTime 在写入 posting 前需统一序列化为 term 字符串（如 `i64.to_string()`、`true/false`、RFC3339 字符串），查询时同样序列化。Bytes 走 base64 编码。

#### 2.2 实现 SchemalessDoc

**文件**：`seekstorm_core/src/index/document.rs`

```rust
pub struct SchemalessDoc {
    pub fields: IndexMap<String, serde_json::Value>,
}

impl SchemalessDoc {
    pub fn from_json(json: &str) -> Result<Self>;
    pub fn to_bytes(&self) -> Result<Vec<u8>>;
    pub fn from_bytes(bytes: &[u8]) -> Result<Self>;
    pub fn fields(&self) -> impl Iterator<Item = (&String, &serde_json::Value)>;
    pub fn get_vector_field(&self) -> Option<(String, Vec<f32>)>;
}
```

#### 2.3 实现 Index 基础结构

**文件**：`seekstorm_core/src/index/mod.rs`

```rust
pub struct Index {
    pub lsm: Arc<LsmEngine>,
    pub schema: Arc<RwLock<DynamicSchema>>,
    pub manifest: RwLock<Manifest>,
    pub config: IndexConfig,
    next_doc_id: AtomicU64,
    
    // Phase 3 填充
    pub lexical: OnceCell<Arc<LexicalEngine>>,
    // Phase 4-5 填充
    pub vector: OnceCell<Arc<VectorEngine>>,
}

impl Index {
    pub async fn create(path: &Path, config: IndexConfig) -> Result<Self>;
    pub async fn open(path: &Path) -> Result<Self>;
    
    pub async fn add_document(&self, doc: SchemalessDoc) -> Result<DocId>;
    pub async fn get_document(&self, doc_id: DocId) -> Result<Option<SchemalessDoc>>;
    pub async fn delete_document(&self, doc_id: DocId) -> Result<()>;
    pub async fn commit(&self) -> Result<()>;
}
```

**`add_document` 流程**：
```rust
pub async fn add_document(&self, doc: SchemalessDoc) -> Result<DocId> {
    // [1] Schema 推断
    let changes = self.schema.infer_and_add(&doc).await?;
    
    // [2] 分配 doc_id
    let doc_id = self.next_doc_id.fetch_add(1, Ordering::Relaxed);
    
    // [3] 序列化
    let bytes = doc.to_bytes()?;
    
    // [4] 写入 LSM DocStore
    let key = LsmKey { namespace: 0x01, partition_or_segment: 0, doc_id, lsn: 0 };
    self.lsm.put(key, LsmValue::Data(bytes)).await?;
    
    // [5] 若有向量字段，写入 VectorPart（Phase 4 接入）
    if let Some((name, vec)) = doc.get_vector_field() {
        // TODO Phase 4: self.vector.add_vector(doc_id, &vec).await?;
    }
    
    // [6] 写入 tantivy（Phase 3 接入）
    // TODO Phase 3: self.lexical.add_document(doc_id, &doc).await?;
    
    // [7] Schema 变更同步到 Meta 命名空间
    if !changes.is_empty() {
        let snapshot = self.schema.serialize().await;
        self.lsm.put(LsmKey::meta("schema"), LsmValue::Data(snapshot)).await?;
    }
    
    Ok(doc_id)
}
```

### 测试用例

```rust
#[tokio::test]
async fn test_schemaless_inference() {
    let schema = DynamicSchema::new();
    let doc = SchemalessDoc::from_json(
        r#"{"title":"hello","count":42,"active":true,"vec":[0.1,0.2,0.3],"tags":["a","b"]}"#
    ).unwrap();
    let changes = schema.infer_and_add(&doc).await.unwrap();
    assert_eq!(changes.len(), 5);

    let title = schema.get_field("title").await.unwrap();
    assert!(matches!(title.data_type, FieldType::Text));
    assert!(title.index_lexical);

    // 数值字段也默认建词法索引
    let count = schema.get_field("count").await.unwrap();
    assert!(matches!(count.data_type, FieldType::I64));
    assert!(count.index_lexical);
    assert!(!count.index_vector);

    // Bool 字段也默认建词法索引
    let active = schema.get_field("active").await.unwrap();
    assert!(matches!(active.data_type, FieldType::Bool));
    assert!(active.index_lexical);

    // 向量字段建向量索引，不建词法索引
    let vec = schema.get_field("vec").await.unwrap();
    assert!(matches!(vec.data_type, FieldType::Vector(_)));
    assert!(!vec.index_lexical);
    assert!(vec.index_vector);
    assert_eq!(vec.vector_dim, Some(3));

    // 嵌套数组不建任何索引
    let tags = schema.get_field("tags").await.unwrap();
    assert!(matches!(tags.data_type, FieldType::Array(_)));
    assert!(!tags.index_lexical);
    assert!(!tags.index_vector);
}

#[tokio::test]
async fn test_add_and_get_document() {
    let index = Index::create(tempdir(), IndexConfig::default()).await.unwrap();
    let doc_id = index.add_document(SchemalessDoc::from_json(r#"{"title":"hello"}"#).unwrap()).await.unwrap();

    let doc = index.get_document(doc_id).await.unwrap().unwrap();
    assert_eq!(doc.fields.get("title").unwrap(), "hello");
}

#[tokio::test]
async fn test_schemaless_different_fields() {
    let index = Index::create(tempdir(), IndexConfig::default()).await.unwrap();
    index.add_document(SchemalessDoc::from_json(r#"{"title":"a"}"#).unwrap()).await.unwrap();
    index.add_document(SchemalessDoc::from_json(r#"{"body":"b","tags":["x"]}"#).unwrap()).await.unwrap();

    let schema = index.schema.read().await;
    assert!(schema.get_field("title").is_some());
    assert!(schema.get_field("body").is_some());
    assert!(schema.get_field("tags").is_some());
}

#[tokio::test]
async fn test_type_conflict() {
    let index = Index::create(tempdir(), IndexConfig::default()).await.unwrap();
    index.add_document(SchemalessDoc::from_json(r#"{"count":42}"#).unwrap()).await.unwrap();

    let result = index.add_document(SchemalessDoc::from_json(r#"{"count":"text"}"#).unwrap()).await;
    assert!(result.is_err());  // 类型冲突
}

#[tokio::test]
async fn test_numeric_field_lexical_search() {
    // 非文本字段也支持词法检索（term = value.to_string()）
    let index = Index::create(tempdir(), IndexConfig::default()).await.unwrap();
    index.add_document(SchemalessDoc::from_json(r#"{"count":42,"active":true}""#).unwrap()).await.unwrap();
    index.commit().await.unwrap();

    let results = index.lexical_search("count:42", 10).await.unwrap();
    assert_eq!(results.len(), 1);

    let results = index.lexical_search("active:true", 10).await.unwrap();
    assert_eq!(results.len(), 1);
}
```

### 验收标准
- [x] JSON 文档可插入，字段自动推断
- [x] 不同 schema 的文档可插入同一索引
- [x] 类型冲突正确报错
- [x] 文档可按 doc_id 查询
- [x] Schema 变更持久化到 Meta 命名空间
- [x] **所有顶层标量字段默认建词法索引**（Text/I64/U64/F64/Bool/DateTime/Bytes）
- [x] **Vector 字段默认建向量索引**，不建词法索引
- [x] **Array 嵌套字段不建索引**
- [x] 数值/Bool 字段可通过 `field:value` 词法检索

### 交付物
- `schema/` 模块完整
- `index/document.rs` + `index/mod.rs` 基础结构
- 端到端 schemaless 插入/查询测试通过

---

## Phase 3：tantivy 词法引擎

> **设计偏差（2026-07-21）**：不用 tantivy `Index` / `Directory`，只用其分词器。
> 倒排索引直接写入 LSM，与文档同 batch，保证原子写。

### 目标
用 tantivy 分词器（`SimpleTokenizer` + `LowerCaser` / `NgramTokenizer`）在自研 LSM 上构建轻量倒排索引，支持 BM25 评分。

### 前置依赖
- Phase 1（LSM）、Phase 2（Schema）

### 文件结构

```
seekstorm_core/src/lexical/
├── mod.rs        # LexicalEngine：写入、搜索、commit
├── analyzer.rs   # tantivy TextAnalyzer 桥接
├── posting.rs    # Posting 二进制编解码（SKLP magic）
└── query.rs      # 查询解析（field:term、空格 AND）
```

### 任务清单

#### 3.1 Posting 编解码

**文件**：`seekstorm_core/src/lexical/posting.rs`

- `Posting { field_id, term, doc_id, term_freq, positions }`
- 二进制格式：`SKLP` magic + field_id(u16) + term_len(u32) + term + doc_id(u64) + term_freq(u32) + positions_len(u32) + positions(u32[])

#### 3.2 分词器桥接

**文件**：`seekstorm_core/src/lexical/analyzer.rs`

- `build_analyzer(TokenizerType) -> TextAnalyzer`
- Default → `SimpleTokenizer + LowerCaser`；Raw → `SimpleTokenizer`；Ngram(n) → `NgramTokenizer + LowerCaser`

#### 3.3 查询解析

**文件**：`seekstorm_core/src/lexical/query.rs`

- 语法：`term`、`field:term`、空格分隔多 term（AND 语义）
- `parse_query("title:hello world") -> LexicalQuery { terms: [TermQuery{field:"title",text:"hello"}, TermQuery{field:None,text:"world"}] }`

#### 3.4 LexicalEngine

**文件**：`seekstorm_core/src/lexical/mod.rs`

- **写入**：`add_document(doc_id, doc)` — 按 `index_lexical` 字段分词，每个 `(field_id, term, doc_id)` 写一条 `NS_LEXICAL_POSTING` entry，partition = `ahash(field_id, term)`
- **搜索**：`search(query_str, top_k)` — 查询词过 analyzer，`scan_prefix` 取 posting，lazy 删除（查 `NS_DOC` 墓碑），BM25 评分，多 term 取交集
- **统计**：`total_docs` 持久化到 `NS_LEXICAL_STATS`，commit 时写入

#### 3.5 接入 Index

**文件**：`seekstorm_core/src/index/mod.rs`

- `add_document` 步骤 [5] 调用 `lexical.add_document`
- `commit` 调用 `lexical.commit`
- 新增 `lexical_search(query, limit)` 方法
- 删除走 lazy 路径，无需额外操作

### 验收标准
- [x] 文档插入后可词法检索（BM25 评分）
- [x] 删除后检索结果正确（lazy 过滤）
- [x] 重启后索引可用（posting 持久化在 LSM）
- [x] 多字段、多 term AND 查询正确
- [x] 大小写不敏感

### 交付物
- `lexical/` 模块 4 个文件
- `Index` 接入词法引擎
- 7 个集成测试 + 单元测试全部通过

---

## Phase 4：基础 IVF 向量引擎

### 目标
实现 IVF 索引基础结构（不含 LIRE），支持向量插入与穷举分区检索。

### 前置依赖
- Phase 1 完成
- Phase 2 完成

### 任务清单

#### 4.1 实现 Quantization

**文件**：`seekstorm_core/src/vector/quantization.rs`

```rust
pub trait Quantization: Send + Sync {
    fn encode(&self, vector: &[f32]) -> Vec<u8>;
    fn decode(&self, bytes: &[u8]) -> Vec<f32>;
    fn similarity(&self, a: &[u8], b: &[u8]) -> f32;
    fn dim(&self) -> usize;
    fn bytes_per_vector(&self) -> usize;
}

pub struct F32Quantization { dim: usize, similarity: VectorSimilarity }
pub struct ScalarQuantization { dim: usize, /* ... */ }
pub struct TurboQuant { /* ... */ }
```

#### 4.2 实现 Similarity

**文件**：`seekstorm_core/src/vector/similarity.rs`

```rust
pub enum VectorSimilarity { Cosine, Dot, Euclidean }

impl VectorSimilarity {
    pub fn distance(&self, a: &[f32], b: &[f32]) -> f32;
}

// SIMD 加速（复用 v3 的 intersection_simd.rs 模式）
#[cfg(target_arch = "x86_64")]
pub fn cosine_simd_avx2(a: &[f32], b: &[f32]) -> f32;

#[cfg(target_arch = "aarch64")]
pub fn cosine_simd_neon(a: &[f32], b: &[f32]) -> f32;
```

#### 4.3 复用 v3 的 Clustering

**文件**：`seekstorm_core/src/vector/clustering.rs`（从 v3 复制）

K-Medoid PAM 用于初始质心计算。

#### 4.4 实现 Partition

**文件**：`seekstorm_core/src/vector/partition.rs`

```rust
pub struct Partition {
    pub id: PartitionId,
    pub centroid: RwLock<Vec<f32>>,
    pub vector_count: AtomicU64,
    pub segment_id: u32,
    pub parent: Option<PartitionId>,
    pub level: u8,
    pub tombstones: RwLock<HashSet<DocId>>,
}

impl Partition {
    pub fn new(id: PartitionId, centroid: Vec<f32>, level: u8) -> Self;
    pub async fn add_vector(&self, doc_id: DocId) -> Result<()>;
    pub async fn delete_vector(&self, doc_id: DocId) -> Result<()>;
    pub async fn vector_count(&self) -> u64;
    pub async fn is_tombstoned(&self, doc_id: DocId) -> bool;
}
```

#### 4.5 实现 IvfIndex

**文件**：`seekstorm_core/src/vector/ivf_index.rs`

```rust
pub struct IvfIndex {
    pub partitions: RwLock<HashMap<PartitionId, Arc<Partition>>>,
    pub centroid_table: RwLock<CentroidTable>,
    pub config: VectorConfig,
    pub next_partition_id: AtomicU32,
}

pub struct CentroidTable {
    pub entries: Vec<(PartitionId, Vec<f32>)>,
}

impl CentroidTable {
    pub fn find_nearest(&self, query: &[f32], nprobe: usize) -> Vec<PartitionId>;
}

impl IvfIndex {
    pub fn new(config: VectorConfig) -> Self;
    pub async fn add_partition(&self, centroid: Vec<f32>) -> PartitionId;
    pub async fn find_nearest_partition(&self, query: &[f32]) -> Option<PartitionId>;
    pub async fn scan_partition(&self, partition_id: PartitionId) -> Vec<DocId>;
}
```

#### 4.6 实现 VectorEngine

**文件**：`seekstorm_core/src/vector/mod.rs`

```rust
pub struct VectorEngine {
    ivf: RwLock<Arc<IvfIndex>>,
    lsm: Arc<LsmEngine>,
    quantization: Arc<dyn Quantization>,
    config: VectorConfig,
}

impl VectorEngine {
    pub async fn new(lsm: Arc<LsmEngine>, config: VectorConfig) -> Result<Self>;
    
    pub async fn add_vector(&self, doc_id: DocId, vector: &[f32]) -> Result<()> {
        let quantized = self.quantization.encode(vector);
        let partition_id = self.ivf.find_nearest_partition(&quantized).await
            .ok_or_else(|| anyhow!("no partitions"))?;
        
        let key = LsmKey {
            namespace: 0x02,
            partition_or_segment: partition_id,
            doc_id,
            lsn: 0,
        };
        self.lsm.put(key, LsmValue::Data(quantized)).await?;
        
        let ivf = self.ivf.read().await;
        let partition = ivf.partitions.get(&partition_id).unwrap();
        partition.add_vector(doc_id).await?;
        Ok(())
    }
    
    pub async fn delete_vector(&self, doc_id: DocId, partition_id: PartitionId) -> Result<()>;
    
    pub async fn search(&self, query: &[f32], top_k: usize, filter: &Filter) -> Result<Vec<ScoredDoc>>;
}
```

#### 4.7 初始化质心

```rust
impl VectorEngine {
    pub async fn initialize_centroids(&self, sample_vectors: &[Vec<f32>]) -> Result<()> {
        let k = (sample_vectors.len() as f64).sqrt() as usize + 1;
        let centroids = k_medoid_pam(sample_vectors, k);
        
        let mut ivf = self.ivf.write().await;
        for centroid in centroids {
            let pid = ivf.add_partition(centroid).await;
        }
        Ok(())
    }
}
```

### 测试用例

```rust
#[tokio::test]
async fn test_vector_insert_and_search() {
    let lsm = LsmEngine::open(tempdir(), LsmConfig::default()).await.unwrap();
    let mut cfg = VectorConfig::default();
    cfg.vector_dim = 128;
    let engine = VectorEngine::new(lsm, cfg).await.unwrap();
    
    // 初始化质心（用样本向量）
    let samples: Vec<Vec<f32>> = (0..1000).map(|i| vec![i as f32 / 1000.0; 128]).collect();
    engine.initialize_centroids(&samples).await.unwrap();
    
    // 插入向量
    for (i, vec) in samples.iter().enumerate() {
        engine.add_vector(i as u64, vec).await.unwrap();
    }
    
    // 检索
    let results = engine.search(&samples[0], 10, &Filter::none()).await.unwrap();
    assert!(!results.is_empty());
    assert_eq!(results[0].doc_id, 0);
}

#[tokio::test]
async fn test_vector_delete() {
    // ... 插入向量，删除一个，验证检索结果不包含已删除 ...
}
```

### 验收标准
- [x] 向量可插入到 IVF 分区
- [x] 向量检索返回 top-k 正确
- [x] 删除的向量不被检索
- [x] K-Medoid 初始质心可用
- [x] SIMD 加速的相似度计算工作正常

### 交付物
- `vector/` 模块基础结构（不含 LIRE）
- 向量插入与穷举检索可用
- 性能：1M 向量 ≤ 10ms 检索 top-10

---

## Phase 5：LIRE 协议

### 目标
实现 SPFresh 风格的 LIRE 协议：分区溢出时分裂，仅迁移边界向量。

### 前置依赖
- Phase 4 完成

### 任务清单

#### 5.1 实现 LIRE 分裂

**文件**：`seekstorm_core/src/vector/lire.rs`

```rust
impl VectorEngine {
    pub async fn maybe_lire_split(&self, partition_id: PartitionId) -> Result<()> {
        let ivf = self.ivf.read().await;
        let partition = ivf.partitions.get(&partition_id).unwrap().clone();
        drop(ivf);
        
        let count = partition.vector_count().await;
        if count <= self.config.max_partition_size {
            return Ok(());
        }
        
        self.lire_split(partition_id).await
    }
    
    async fn lire_split(&self, partition_id: PartitionId) -> Result<()> {
        // [1] 找分裂方向
        let split_direction = self.find_split_direction(partition_id).await?;
        
        // [2] 分配新分区
        let new_id = {
            let mut ivf = self.ivf.write().await;
            ivf.add_partition(vec![0.0; self.config.vector_dim]).await
        };
        
        // [3] 扫描源分区，找边界向量
        let vectors = self.lsm.scan_prefix(0x02, partition_id).await?;
        let mut moved = 0;
        
        for (key, value) in vectors {
            if matches!(value, LsmValue::Tombstone) { continue; }
            let quantized = self.quantization.decode_from_value(&value);
            
            if self.should_move_to_new(&quantized, &split_direction) {
                // [4] 在新分区写入
                let new_key = LsmKey {
                    namespace: 0x02,
                    partition_or_segment: new_id,
                    doc_id: key.doc_id,
                    lsn: self.lsm.next_lsn(),
                };
                self.lsm.put(new_key, LsmValue::Data(value.clone())).await?;
                
                // [5] 旧分区 tombstone
                self.lsm.delete(0x02, key.doc_id).await?;
                moved += 1;
            }
        }
        
        // [6] 重新计算质心
        let (old_centroid, new_centroid) = self.recompute_centroids(partition_id, new_id).await?;
        
        // [7] 原子更新分区表
        let mut ivf = self.ivf.write().await;
        let new_ivf = ivf.with_partition_updated(partition_id, |p| p.centroid = old_centroid)
                        .with_partition_updated(new_id, |p| p.centroid = new_centroid);
        *ivf = Arc::new(new_ivf);
        
        // [8] WAL 记录
        self.lsm.wal_append(WalOp::PartitionSplit { 
            old: partition_id, 
            new: new_id, 
            moved: moved as u64 
        }).await?;
        
        Ok(())
    }
    
    async fn find_split_direction(&self, partition_id: PartitionId) -> Result<Vec<f32>>;
    async fn should_move_to_new(&self, vector: &[f32], direction: &[f32]) -> bool;
    async fn recompute_centroids(&self, old_id: PartitionId, new_id: PartitionId) -> Result<(Vec<f32>, Vec<f32>)>;
}
```

#### 5.2 接入 VectorEngine.add_vector

```rust
impl VectorEngine {
    pub async fn add_vector(&self, doc_id: DocId, vector: &[f32]) -> Result<()> {
        // ... Phase 4 逻辑 ...
        
        // 触发 LIRE 分裂
        let count = partition.vector_count().await;
        if count > self.config.max_partition_size {
            drop(ivf);
            self.maybe_lire_split(partition_id).await?;
        }
        
        Ok(())
    }
}
```

#### 5.3 实现后台再平衡

```rust
impl VectorEngine {
    pub async fn rebalance_loop(&self) {
        loop {
            tokio::time::sleep(self.config.rebalance_interval).await;
            
            let ivf = self.ivf.read().await;
            let partitions: Vec<_> = ivf.partitions.iter().collect();
            drop(ivf);
            
            for (id, partition) in partitions {
                let count = partition.vector_count().await;
                if count > self.config.max_partition_size {
                    let _ = self.lire_split(*id).await;
                } else if count < self.config.min_partition_size {
                    let _ = self.merge_partition(*id).await;
                }
            }
        }
    }
}
```

### 测试用例

```rust
#[tokio::test]
async fn test_lire_split_triggered() {
    let mut cfg = VectorConfig::default();
    cfg.max_partition_size = 100;
    cfg.vector_dim = 64;
    let engine = VectorEngine::new(lsm, cfg).await.unwrap();
    
    // 初始 1 个分区
    engine.initialize_centroids(&samples).await.unwrap();
    assert_eq!(engine.partition_count().await, 1);
    
    // 插入 200 个向量，触发分裂
    for i in 0..200 {
        engine.add_vector(i, &vec![i as f32; 64]).await.unwrap();
    }
    
    assert!(engine.partition_count().await > 1);
}

#[tokio::test]
async fn test_lire_preserves_search_recall() {
    // 插入 1000 向量，触发多次分裂
    // 验证检索 recall ≥ 0.95
}
```

### 验收标准
- [x] 分区溢出时自动触发 LIRE 分裂
- [x] 仅迁移边界向量（非全量重建）
- [x] 分裂后检索 recall ≥ 0.95
- [x] WAL 记录分裂操作，崩溃后可恢复

### 交付物
- `vector/lire.rs` 完整
- LIRE 分裂与再平衡工作
- 性能：分裂期间不阻塞查询

---

## Phase 6：混合检索 + 查询规划器

### 目标
实现查询规划器、RRF 融合、字段过滤，支持 6 种 QueryMode。

### 前置依赖
- Phase 3 完成（词法）
- Phase 5 完成（向量）

### 任务清单

#### 6.1 实现 QueryMode 与 Planner

**文件**：`seekstorm_core/src/query/planner.rs`

```rust
pub enum QueryMode {
    PureLexical,
    PureVector,
    HybridLexicalRerank,
    HybridVectorRerank,
    HybridParallel,
    Auto,
}

pub struct QueryRequest {
    pub text: Option<String>,
    pub vector: Option<Vec<f32>>,
    pub mode: QueryMode,
    pub filters: Vec<Filter>,
    pub top_k: usize,
}

pub fn select_mode(req: &QueryRequest) -> QueryMode {
    if req.mode != QueryMode::Auto {
        return req.mode.clone();
    }
    match (req.text.is_some(), req.vector.is_some()) {
        (true, false) => QueryMode::PureLexical,
        (false, true) => QueryMode::PureVector,
        (true, Some(_)) if req.text.as_ref().map(|t| t.len()).unwrap_or(0) < 32 => {
            QueryMode::HybridLexicalRerank
        }
        (true, Some(_)) => QueryMode::HybridVectorRerank,
        _ => QueryMode::PureLexical,
    }
}
```

#### 6.2 实现 Filter

**文件**：`seekstorm_core/src/query/filter.rs`

```rust
pub enum FilterOp {
    Equals(Value),
    Range(Bound<Value>, Bound<Value>),
    In(Vec<Value>),
    Exists,
}

pub struct Filter {
    pub field: String,
    pub op: FilterOp,
}

impl Filter {
    pub fn matches(&self, doc: &SchemalessDoc) -> bool;
}

pub type Filters = Vec<Filter>;
```

#### 6.3 实现 RRF 融合

**文件**：`seekstorm_core/src/query/fusion.rs`

```rust
pub fn rrf_fusion(
    lexical: Vec<ScoredDoc>,
    vector: Vec<ScoredDoc>,
    k: u32,
    top_k: usize,
) -> Vec<ScoredDoc>;

pub fn weighted_fusion(
    lexical: Vec<ScoredDoc>,
    vector: Vec<ScoredDoc>,
    weight_lexical: f64,
    weight_vector: f64,
    top_k: usize,
) -> Vec<ScoredDoc>;
```

#### 6.4 实现混合检索

**文件**：`seekstorm_core/src/search/hybrid_search.rs`

```rust
impl Index {
    pub async fn search(&self, req: QueryRequest) -> Result<SearchResponse> {
        let mode = select_mode(&req);
        
        match mode {
            QueryMode::PureLexical => {
                let results = self.lexical.search(&req.into_lexical()).await?;
                Ok(SearchResponse::from(results))
            }
            QueryMode::PureVector => {
                let results = self.vector.search(
                    req.vector.as_ref().unwrap(),
                    req.top_k,
                    &req.filters,
                ).await?;
                Ok(SearchResponse::from(results))
            }
            QueryMode::HybridParallel => {
                let (lexical, vector) = tokio::join!(
                    async { self.lexical.search(&req.into_lexical()).await },
                    async { self.vector.search(req.vector.as_ref().unwrap(), req.top_k, &req.filters).await },
                );
                let merged = rrf_fusion(lexical?, vector?, 60, req.top_k);
                Ok(SearchResponse::from(merged))
            }
            QueryMode::HybridLexicalRerank => {
                let lexical = self.lexical.search(&req.into_lexical()).await?;
                let doc_ids: Vec<_> = lexical.iter().take(req.top_k * 2).map(|d| d.doc_id).collect();
                let vector = self.vector.search_by_ids(&doc_ids, req.vector.as_ref().unwrap()).await?;
                let merged = weighted_fusion(lexical, vector, 0.3, 0.7, req.top_k);
                Ok(SearchResponse::from(merged))
            }
            QueryMode::HybridVectorRerank => {
                let vector = self.vector.search(req.vector.as_ref().unwrap(), req.top_k * 2, &req.filters).await?;
                let doc_ids: Vec<_> = vector.iter().map(|d| d.doc_id).collect();
                let lexical = self.lexical.search_by_ids(&doc_ids, &req).await?;
                let merged = weighted_fusion(lexical, vector, 0.7, 0.3, req.top_k);
                Ok(SearchResponse::from(merged))
            }
            _ => unreachable!(),
        }
    }
}
```

### 测试用例

```rust
#[tokio::test]
async fn test_hybrid_parallel_search() {
    let index = Index::create(tempdir(), default_config()).await.unwrap();
    // 插入带 text + vector 的文档
    for i in 0..100 {
        index.add_document(json!({
            "title": format!("doc {}", i),
            "embedding": vec![i as f32 / 100.0; 64],
        })).await.unwrap();
    }
    index.commit().await.unwrap();
    
    let results = index.search(QueryRequest {
        text: Some("doc".into()),
        vector: Some(vec![0.5; 64]),
        mode: QueryMode::HybridParallel,
        filters: vec![],
        top_k: 10,
    }).await.unwrap();
    
    assert_eq!(results.len(), 10);
}

#[tokio::test]
async fn test_filter_active_during_search() {
    // 插入 docs with tags
    // 检索带 filter tags=a
    // 验证只返回 tags=a 的文档
}
```

### 验收标准
- [x] 6 种 QueryMode 工作正常
- [x] Auto 模式根据查询形态正确选择
- [x] RRF 融合返回 top-k
- [x] 字段过滤在检索期间生效（非后置）

### 交付物
- `query/` 模块完整
- `search/hybrid_search.rs` 实现
- 混合检索端到端测试通过

---

## Phase 7：删除机制 + 崩溃恢复

### 目标
完善 tombstone 机制、compaction-aware 删除、WAL 崩溃恢复。

### 前置依赖
- Phase 1-6 完成

### 任务清单

#### 7.1 完善 tombstone 流程

**文件**：`seekstorm_core/src/index/mod.rs`（修改）

```rust
impl Index {
    pub async fn delete_document(&self, doc_id: DocId) -> Result<()> {
        // [1] WAL
        self.lsm.wal_append(WalOp::Delete { doc_id }).await?;
        
        // [2] DocStore tombstone
        self.lsm.delete(0x01, doc_id).await?;
        
        // [3] VectorPart tombstone
        if let Some(pid) = self.vector.find_partition(doc_id).await {
            self.vector.delete_vector(doc_id, pid).await?;
        }
        
        // [4] tantivy 删除
        self.lexical.delete_document(doc_id).await?;
        
        Ok(())
    }
    
    pub async fn hard_delete(&self, doc_id: DocId) -> Result<()> {
        self.delete_document(doc_id).await?;
        self.lsm.force_compact().await?;
        self.lexical.force_merge().await?;
        if let Some(pid) = self.vector.find_partition(doc_id).await {
            self.vector.force_compact_partition(pid).await?;
        }
        Ok(())
    }
}
```

#### 7.2 实现 LIRE-aware 分区 compaction

**文件**：`seekstorm_core/src/vector/lire.rs`（扩展）

```rust
impl VectorEngine {
    pub async fn gc_partition(&self, partition_id: PartitionId) -> Result<()> {
        // tombstone 比例检查
        let tombstone_ratio = self.partition_tombstone_ratio(partition_id).await?;
        if tombstone_ratio < 0.2 {
            return Ok(());
        }
        
        // 重写分区
        let vectors = self.lsm.scan_prefix(0x02, partition_id).await?;
        let new_id = self.allocate_partition_id().await?;
        
        for (key, value) in vectors {
            if matches!(value, LsmValue::Tombstone) { continue; }
            let new_key = LsmKey {
                namespace: 0x02,
                partition_or_segment: new_id,
                doc_id: key.doc_id,
                lsn: self.lsm.next_lsn(),
            };
            self.lsm.put(new_key, value).await?;
        }
        
        // 替换分区
        let mut ivf = self.ivf.write().await;
        ivf.replace_partition(partition_id, new_id);
        
        Ok(())
    }
    
    pub async fn force_compact_partition(&self, doc_id: DocId) -> Result<()> {
        if let Some(pid) = self.find_partition(doc_id).await {
            self.gc_partition(pid).await?;
        }
        Ok(())
    }
}
```

#### 7.3 实现崩溃恢复

**文件**：`seekstorm_core/src/index/mod.rs`（修改 `open`）

```rust
impl Index {
    pub async fn open(path: &Path) -> Result<Self> {
        // [1] 加载 manifest
        let manifest = Manifest::load(&path.join("manifest.json")).await
            .unwrap_or_else(|_| Manifest::new(path));
        
        // [2] 初始化 LSM（自动重放 WAL）
        let lsm = LsmEngine::open(&path.join("lsm"), manifest.config.lsm.clone()).await?;
        
        // [3] 重放未 checkpoint 的 WAL 记录
        let wal_records = lsm.wal.recover().await?;
        for record in wal_records.iter().filter(|r| r.lsn > manifest.last_checkpoint_lsn) {
            match &record.op {
                WalOp::Put { key, value } => {
                    lsm.apply_put(key.clone(), value.clone()).await?;
                }
                WalOp::Delete { doc_id } => {
                    lsm.apply_delete(0x01, *doc_id).await?;
                }
                WalOp::PartitionSplit { old, new, moved } => {
                    lsm.apply_partition_split(*old, *new, *moved).await?;
                }
                WalOp::Checkpoint { last_applied_lsn } => {
                    lsm.truncate_wal(*last_applied_lsn).await?;
                }
            }
        }
        
        // [4] 写 checkpoint
        let last_lsn = wal_records.last().map(|r| r.lsn).unwrap_or(manifest.last_checkpoint_lsn);
        lsm.wal_append(WalOp::Checkpoint { last_applied_lsn: last_lsn }).await?;
        
        // [5] 加载 schema
        let schema = if let Some(LsmValue::Data(bytes)) = lsm.get(&LsmKey::meta("schema")).await? {
            DynamicSchema::deserialize(&bytes)?
        } else {
            DynamicSchema::new()
        };
        
        // [6] 初始化词法引擎
        let lexical = LexicalEngine::new(lsm.clone(), Arc::new(RwLock::new(schema.clone())), ...).await?;
        
        // [7] 初始化向量引擎
        let vector = VectorEngine::open(lsm.clone(), ...).await?;
        
        Ok(Self { lsm, lexical, vector, schema, ... })
    }
}
```

### 测试用例

```rust
#[tokio::test]
async fn test_crash_recovery_after_insert() {
    let dir = tempdir();
    let index = Index::create(&dir, default_config()).await.unwrap();
    index.add_document(json!({"title":"hello"})).await.unwrap();
    // 不 commit，直接 drop（模拟崩溃）
    drop(index);
    
    let index = Index::open(&dir).await.unwrap();
    let doc = index.get_document(0).await.unwrap();
    assert!(doc.is_some());  // WAL 重放恢复
}

#[tokio::test]
async fn test_crash_during_lire_split() {
    // 模拟分裂中途崩溃
    // 重启后状态一致
}

#[tokio::test]
async fn test_hard_delete_removes_physically() {
    let index = Index::create(tempdir(), default_config()).await.unwrap();
    let id = index.add_document(json!({"title":"secret"})).await.unwrap();
    index.hard_delete(id).await.unwrap();
    
    // 验证物理删除（扫描 LSM 无 tombstone）
    let all = index.lsm.scan_prefix(0x01, 0).await.unwrap();
    assert!(all.is_empty());
}

#[tokio::test]
async fn test_partition_gc_after_tombstones() {
    // 插入 100 向量到分区
    // 删除 30 个（30% > 20% 阈值）
    // 触发 GC，验证分区被重写
}
```

### 验收标准
- [x] 删除后立即查询返回 None
- [x] 崩溃后重启，WAL 重放恢复一致状态
- [x] LIRE 分裂中途崩溃，重启后状态正确
- [x] hard_delete 物理清理
- [x] 分区 tombstone 比例 > 20% 触发 GC

### 交付物
- 删除机制完整
- 崩溃恢复流程
- 故障注入测试通过

---

## Phase 8：服务器适配

### 目标
适配 `seekstorm_server` 到新 API，保留 HTTP 端点兼容性。

### 前置依赖
- Phase 7 完成

### 任务清单

#### 8.1 修改服务器入口

**文件**：`seekstorm_server/src/main.rs`

```rust
use seekstorm_core::Index;

#[tokio::main]
async fn main() -> Result<()> {
    let config = ServerConfig::from_env();
    let index = Index::open(&config.index_path).await?;
    let app = HttpServer::new(index, config);
    app.run().await
}
```

#### 8.2 实现 HTTP 端点

**文件**：`seekstorm_server/src/api_endpoints.rs`

```rust
pub async fn create_index(req: CreateIndexRequest, state: State) -> Result<HttpResponse> {
    let index = Index::create(&req.path, req.config).await?;
    state.add_index(req.id, index);
    Ok(HttpResponse::Ok().json(CreateIndexResponse { id: req.id }))
}

pub async fn add_document(req: AddDocumentRequest, state: State) -> Result<HttpResponse> {
    let index = state.get_index(&req.index_id)?;
    let doc = SchemalessDoc::from_json(&req.document)?;
    let doc_id = index.add_document(doc).await?;
    Ok(HttpResponse::Ok().json(AddDocumentResponse { doc_id }))
}

pub async fn query(req: QueryRequest, state: State) -> Result<HttpResponse> {
    let index = state.get_index(&req.index_id)?;
    let results = index.search(req.into()).await?;
    Ok(HttpResponse::Ok().json(SearchResponse::from(results)))
}

pub async fn delete_document(req: DeleteRequest, state: State) -> Result<HttpResponse> {
    let index = state.get_index(&req.index_id)?;
    index.delete_document(req.doc_id).await?;
    Ok(HttpResponse::Ok().finish())
}

pub async fn get_schema(req: GetSchemaRequest, state: State) -> Result<HttpResponse> {
    let index = state.get_index(&req.index_id)?;
    let schema = index.schema.read().await;
    Ok(HttpResponse::Ok().json(SchemaResponse::from(&*schema)))
}

pub async fn force_compact(req: CompactRequest, state: State) -> Result<HttpResponse> {
    let index = state.get_index(&req.index_id)?;
    index.lsm.force_compact().await?;
    Ok(HttpResponse::Ok().finish())
}
```

#### 8.3 保留 OpenAPI 文档

**文件**：`seekstorm_server/openapi/openapi.json`

更新 OpenAPI spec 以反映新端点（`GET /schema`、`POST /compact`）。

### 测试用例

```rust
#[tokio::test]
async fn test_http_create_add_query() {
    let server = TestServer::start().await;
    
    // 创建索引
    let resp = server.post("/index/myidx").json(&json!({"path":"/tmp/myidx"})).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    
    // 插入文档
    let resp = server.post("/index/myidx/document")
        .json(&json!({"document": r#"{"title":"hello"}"#}))
        .send().await.unwrap();
    assert_eq!(resp.status(), 200);
    
    // 检索
    let resp = server.post("/index/myidx/query")
        .json(&json!({"text":"hello","mode":"PureLexical","top_k":10}))
        .send().await.unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["results"].as_array().unwrap().len(), 1);
}
```

### 验收标准
- [x] HTTP API 与 v3 端点兼容（除新功能）
- [x] Schemaless 文档可通过 HTTP 插入
- [x] 混合检索可通过 HTTP 触发
- [x] OpenAPI 文档更新

### 交付物
- `seekstorm_server` 适配新 API
- HTTP 端到端测试通过

---

## Phase 9：可观测性 + 性能优化

### 目标
添加 Prometheus 指标、tracing、查询 explain、性能基准。

### 前置依赖
- Phase 8 完成

### 任务清单

#### 9.1 Prometheus 指标

**文件**：`seekstorm_core/src/metrics.rs`

```rust
pub struct Metrics {
    pub docs_indexed: Counter,
    pub docs_deleted: Counter,
    pub queries_total: Counter,
    pub query_latency: Histogram,
    pub lsm_memtable_size: Gauge,
    pub lsm_sstable_count: Gauge,
    pub lsm_compaction_total: Counter,
    pub vector_partition_count: Gauge,
    pub vector_lire_splits: Counter,
}

impl Metrics {
    pub fn register(registry: &Registry) -> Self;
}
```

#### 9.2 Tracing

```rust
use tracing::{info_span, instrument};

impl Index {
    #[instrument(skip(self, doc), fields(doc_id))]
    pub async fn add_document(&self, doc: SchemalessDoc) -> Result<DocId> {
        info_span!("add_document");
        // ...
    }
    
    #[instrument(skip(self, req))]
    pub async fn search(&self, req: QueryRequest) -> Result<SearchResponse> {
        // ...
    }
}
```

#### 9.3 查询 Explain

```rust
pub struct SearchResponse {
    pub results: Vec<ScoredDoc>,
    pub mode: QueryMode,
    pub took_ms: u64,
    pub explain: Option<ExplainInfo>,
}

pub struct ExplainInfo {
    pub lexical_candidates: usize,
    pub vector_candidates: usize,
    pub partitions_probed: usize,
    pub fusion_method: String,
    pub lsm_seeks: usize,
    pub lsm_scans: usize,
}
```

#### 9.4 性能基准

**文件**：`seekstorm_core/benches/bench.rs`

```rust
#[bench]
fn bench_insert_1k(b: &mut Bencher);
#[bench]
fn bench_lexical_search(b: &mut Bencher);
#[bench]
fn bench_vector_search_1m(b: &mut Bencher);
#[bench]
fn bench_hybrid_search(b: &mut Bencher);
#[bench]
fn bench_compaction(b: &mut Bencher);
```

### 验收标准
- [x] Prometheus 指标可采集
- [x] Tracing span 可观测
- [x] 查询 explain 返回分阶段信息
- [x] 性能基准：1M 文档插入 ≤ 5 分钟，top-10 检索 ≤ 10ms

### 交付物
- `metrics.rs` + `tracing` 集成
- 性能基准报告
- 生产就绪

---

## 阶段依赖与并行性

```
Phase 1 (LSM)
   │
   ▼
Phase 2 (Schemaless)
   │
   ├──────────────┐
   ▼              ▼
Phase 3        Phase 4
(tantivy)     (IVF 基础)
   │              │
   │              ▼
   │           Phase 5
   │           (LIRE)
   │              │
   └──────┬───────┘
          ▼
       Phase 6
       (混合检索)
          │
          ▼
       Phase 7
       (删除+恢复)
          │
          ▼
       Phase 8
       (服务器)
          │
          ▼
       Phase 9
       (可观测性)
```

**可并行**：Phase 3 与 Phase 4-5 可由不同开发者并行推进。

## 总周期预估

| 阶段 | 周期 |
|---|---|
| Phase 1 | 2 周 |
| Phase 2 | 1 周 |
| Phase 3 | 2 周 |
| Phase 4 | 1.5 周 |
| Phase 5 | 2 周 |
| Phase 6 | 1 周 |
| Phase 7 | 1 周 |
| Phase 8 | 1 周 |
| Phase 9 | 1 周 |
| **总计** | **12.5 周**（串行）/ **8-9 周**（并行） |

---

## 关键里程碑

| 里程碑 | 完成阶段 | 可用功能 |
|---|---|---|
| **M1: 存储可用** | Phase 1+2 | schemaless 文档插入与查询 |
| **M2: 词法检索** | +Phase 3 | 纯词法 BM25 检索 |
| **M3: 向量检索** | +Phase 4+5 | 纯向量 ANN 检索 + LIRE |
| **M4: 混合检索** | +Phase 6 | 6 种 QueryMode + RRF |
| **M5: 生产就绪** | +Phase 7+8+9 | HTTP API + 持久性 + 可观测性 |
