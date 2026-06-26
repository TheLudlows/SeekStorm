# SeekStorm vs Lucene vs Tantivy 线程模型深度对比

> 生成日期: 2026-06-24
> 项目: SeekStorm

---

## 目录

1. [概述](#1-概述)
2. [SeekStorm 线程模型](#2-seekstorm-线程模型)
3. [Lucene 线程模型](#3-lucene-线程模型)
4. [Tantivy 线程模型](#4-tantivy-线程模型)
5. [深度对比](#5-深度对比)
6. [设计哲学差异](#6-设计哲学差异)

---

## 1. 概述

| 特性 | SeekStorm | Lucene | Tantivy |
|------|-----------|--------|---------|
| 语言 | Rust | Java | Rust |
| 并发模型 | Async/Await + Tokio | Synchronous | Synchronous + Arc/RwLock |
| 主要运行时 | INDEX_RUNTIME (多线程) | 线程池 (ForkJoinPool) | 线程池 (rayon) |
| 写入并发 | 分片级别信号量控制 | 单线程 IndexWriter | 多线程安全 IndexWriter |
| 读取并发 | RwLock + 并行分片读取 | 线程安全 IndexReader | 多线程 Searcher |

---

## 2. SeekStorm 线程模型

### 2.1 全局运行时

```rust
// 位置: seekstorm/src/lib.rs:482-489
pub static INDEX_RUNTIME: LazyLock<Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(num_cpus::get())      // 使用所有 CPU 核心
        .thread_name("seekstorm-indexer")    // 命名线程便于调试
        .enable_all()                         // 启用所有 Tokio 特性
        .build()
        .unwrap()
});
```

**设计特点**:
- **多线程运行时**: 使用所有 CPU 核心
- **全局单例**: 整个程序共享一个 Tokio Runtime
- **命名线程**: 方便调试和监控
- **Lazy 初始化**: 首次使用时创建，避免启动开销

### 2.2 写入并发模型

#### 分片级别并发控制

```rust
// 位置: seekstorm/src/index.rs:2721
struct Shard {
    // 每个分片独立的信号量
    semaphore: Arc<Semaphore>,
}

// 创建分片时初始化信号量为 1
let mut index = Shard {
    semaphore: Arc::new(Semaphore::new(1)),  // 默认单线程写入
    // ...
};
```

#### 异步索引执行流程

```rust
// 位置: seekstorm/src/index.rs:5270-5290
async fn index_document(&self, document: Document, file: FileType) {
    let shard_number = self.read().await.shard_number;
    let docid_global_arc = self.read().await.docid_global.clone();
    let mut docid_global = docid_global_arc.write().await;
    let docid_global_clone = *docid_global;
    
    // 通过哈希分配到分片
    let shard_id = *docid_global % shard_number;
    let shard_arc = self.read().await.shard_vec[shard_id].clone();
    
    // 获取分片信号量许可
    let semaphore = shard_arc.read().await.semaphore.clone();
    let permit = semaphore.acquire_owned().await.unwrap();
    
    // 原子递增文档 ID
    *docid_global += 1;
    drop(docid_global);
    
    // 在专用线程池中异步执行
    INDEX_RUNTIME.handle().spawn(async move {
        shard_arc
            .index_document_shard(document, file, docid_global_clone)
            .await;
        drop(permit);  // 释放许可
    });
}
```

**设计分析**:

```
HTTP 请求
    │
    ▼
获取 API Key → 验证 → 获取 Index
    │
    ▼
计算 Shard ID = docid % shard_number
    │
    ▼
获取 Shard 信号量 (acquire_owned)
    │
    ├─ 如果获取成功 → 异步执行索引
    │                   └─ 完成后释放 permit
    │
    └─ 如果信号量满 → 阻塞等待许可
```

**关键特性**:
1. **分片隔离**: 每个分片独立控制并发，互不干扰
2. **异步非阻塞**: 使用 `acquire_owned()` 获取许可，任务完成时自动释放
3. **负载均衡**: 通过哈希分配文档到不同分片
4. **并发控制**: 默认每个分片单线程写入，可调整信号量大小

### 2.3 读取并发模型

#### 数据结构

```rust
// 位置: seekstorm/src/index.rs:1542-1546
pub type ShardArc = Arc<RwLock<Shard>>;  // 分片级锁
pub type IndexArc = Arc<RwLock<Index>>;  // 索引级锁
```

#### 并行分片读取

```rust
// 位置: seekstorm/src/search.rs:2083
let shard_vec = futures::future::join_all(
    index_ref.shard_vec.iter().map(|s| s.read())
).await;

// 并行读取所有分片，每个分片获取读锁
// 使用 futures::join_all 实现真正的并发
```

**搜索流程**:

```
搜索请求
    │
    ▼
获取 Index 读锁
    │
    ▼
并行获取所有 Shard 读锁 (join_all)
    │
    ├─ Shard 0: read() → 并行搜索
    ├─ Shard 1: read() → 并行搜索
    ├─ Shard 2: read() → 并行搜索
    └─ Shard N: read() → 并行搜索
    │
    ▼
合并所有分片结果
    │
    ▼
返回最终结果
```

**关键特性**:
1. **多粒度锁**: Index 级别 + Shard 级别
2. **共享读锁**: 多个读取操作可以同时持有锁
3. **真正的并发**: 使用 `join_all` 并行执行分片搜索
4. **结果合并**: 分片结果在最后合并，减少锁持有时间

### 2.4 近实时搜索支持

```rust
// 位置: seekstorm/src/index.rs:5497-5518
async fn index_document_shard_2(&self, document_item: DocumentItem, ...) {
    let docid_local = docid_global / shard_mut.shard_number;
    
    // 检查是否需要自动提交 (每 65,536 文档)
    let do_commit = shard_mut.block_id != docid_local >> 16;
    if do_commit {
        if shard_mut.is_vector_indexing {
            shard_mut.commit_vector_shard().await;
        }
        shard_mut.commit_lexical_shard(docid_local).await;
        shard_mut.block_id = docid_local >> 16;
    }
    
    // 写入完成后继续在内存中可搜索
}
```

**软提交机制**:
- **写入后立即可搜索**: 文档写入内存缓冲区后，`realtime=true` 的查询可以立即看到
- **无需显式 commit**: 内存中的数据通过读锁可见
- **自动硬提交**: 每 65,536 文档自动将内存数据持久化到磁盘

### 2.5 并发控制机制总结

```
┌─────────────────────────────────────────────────────────────────┐
│                    SeekStorm 并发控制层次                          │
└─────────────────────────────────────────────────────────────────┘

Level 0: 线程池 (INDEX_RUNTIME)
  ├─ Worker Threads = CPU 核心数
  ├─ 全局共享运行时
  └─ 异步任务调度

Level 1: 索引级别 (IndexArc = Arc<RwLock<Index>>)
  ├─ 读锁: 允许多个读取操作
  ├─ 写锁: 独占访问
  └─ 保护: Shard 列表等全局状态

Level 2: 分片级别 (ShardArc = Arc<RwLock<Shard>>)
  ├─ 读锁: 允许多个并发搜索
  ├─ 写锁: 索引修改
  └─ 保护: Posting List、向量聚类等

Level 3: 分片内部 (Semaphore)
  ├─ 信号量: 控制分片内并发写入数
  ├─ 默认值: 1 (单线程写入)
  └─ 作用: 防止内部数据竞争
```

---

## 3. Lucene 线程模型

### 3.1 写入并发模型

#### 单线程 IndexWriter

```java
// Lucene 推荐: 单线程控制 IndexWriter
IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig(analyzer));

// 写入操作 (通常单线程)
writer.addDocument(document);
writer.commit();

// 或者使用 try-with-resources 自动提交
try (IndexWriter writer = new IndexWriter(...)) {
    writer.addDocument(document);
}  // 自动提交
```

**设计哲学**:
- **推荐单线程**: Lucene 的 IndexWriter 设计为单线程使用
- **线程不安全**: 多线程调用 `addDocument()` 可能导致数据不一致
- **例外**: 段合并可以在后台线程池中并行执行

#### 段合并并发

```java
// Lucene 8.x 默认合并策略
IndexWriterConfig config = new IndexWriterConfig(analyzer);

// 合并调度器 (默认 ConcurrentMergeScheduler)
config.setMergeScheduler(new ConcurrentMergeScheduler());

// 自定义合并策略
config.setMergePolicy(new TieredMergePolicy());
```

**并发合并**:
- **后台线程池**: `ConcurrentMergeScheduler` 使用线程池
- **独立锁**: 每个合并操作有独立锁，不影响索引/搜索
- **可配置**: 可以禁用自动合并或使用自定义策略

### 3.2 读取并发模型

#### 线程安全的 IndexReader

```java
// 打开 IndexReader (线程安全)
IndexReader reader = DirectoryReader.open(index);

// 多线程安全共享
IndexSearcher searcher = new IndexSearcher(reader);

// 在多个线程中使用
ExecutorService executor = Executors.newFixedThreadPool(4);
for (int i = 0; i < 4; i++) {
    executor.submit(() -> {
        TopDocs docs = searcher.search(query, 10);
        // ...
    });
}
```

**设计特点**:
- **完全线程安全**: IndexReader 可以在多线程中安全使用
- **无锁读取**: 读取操作不获取锁
- **快照一致性**: 读者看到的是打开时的索引快照

### 3.3 近实时搜索支持

#### NRT (Near Real-Time) Search

```java
// 创建支持 NRT 的 IndexWriter
IndexWriter writer = new IndexWriter(directory, config);

// 获取 NRT IndexReader (包含未提交的文档)
IndexReader nrtReader = DirectoryReader.open(writer);

// 创建 NRT Searcher
IndexSearcher searcher = new IndexSearcher(writer);

// 搜索时可以包含最近写入但未提交的文档
TopDocs docs = searcher.search(query, 10);
```

**NRT 实现机制**:
- **双缓冲区**: 内存中有未提交的段，可以直接搜索
- **原子刷新**: `IndexWriter.getReader()` 获取包含最新数据的读者
- **控制刷新频率**: 通过 `IndexWriterConfig` 配置

### 3.4 锁策略

#### 文件系统锁

```java
// 防止多进程同时写入
FSLockFactory lockFactory = new NativeFSLockFactory();
FSLock lock = lockFactory.obtainLock(directory);

// 写入时获取文件锁
writer.lock();  // 防止其他进程同时修改
```

**锁类型**:
- **NativeFSLock**: 使用操作系统文件锁
- **SimpleFSLock**: 简单的 Java 文件锁实现
- **NoLock**: 不使用锁 (仅用于单进程)

#### 内部锁

```java
// Lucene 内部使用的锁
ReentrantReadWriteLock segmentLock;      // 段锁
synchronized void deleteDocuments(...)   // 文档删除锁
```

---

## 4. Tantivy 线程模型

### 4.1 写入并发模型

#### 线程安全的 IndexWriter

```rust
// Tantivy IndexWriter 是线程安全的
let writer = IndexWriter::new(directory, schema)?;

// 多线程安全写入
use rayon::prelude::*;

(0..1000).into_par_iter().for_each(|i| {
    let mut doc = Document::default();
    doc.add_text("title", &format!("Document {}", i));
    writer.add_document(doc).unwrap();
});
```

**实现机制**:
```rust
// Tantivy 内部使用 RwLock 保护
pub struct IndexWriter {
    schema: Schema,
    index: Index,
    // ...
}

// add_document 使用内部锁
impl IndexWriter {
    pub fn add_document(&mut self, doc: Document) -> Result<()> {
        // 内部使用 RwLock 保护共享状态
        // ...
    }
}
```

**设计特点**:
- **原生线程安全**: Rust 的类型系统保证线程安全
- **推荐单线程**: 虽然线程安全，但推荐单线程以避免锁竞争
- **SegmentWriter 层**: 每个 SegmentWriter 是单线程的

### 4.2 读取并发模型

#### 多线程 Searcher

```rust
// IndexReader 可以 Clone
let reader = index.reader()?;

// 多线程搜索
use rayon::prelude::*;

let results: Vec<_> = (0..10).into_par_iter()
    .map(|_| {
        let searcher = reader.searcher();
        searcher.search(&query, &collector).unwrap()
    })
    .collect();
```

**设计特点**:
- **Arc 共享**: IndexReader 使用 Arc 实现线程间共享
- **Searcher Clone**: Searcher 可以安全克隆
- **无锁读取**: 读取操作不获取全局锁

### 4.3 近实时搜索支持

#### 自动提交机制

```rust
// Tantivy 的 IndexWriter 会自动管理段
let writer = IndexWriter::new(directory, schema)?;

// 添加文档
writer.add_document(doc)?;

// 显式提交 (将内存段刷新到磁盘)
writer.commit()?;

// 或使用 MmapDirectory 自动管理
let directory = MmapDirectory::open(path)?;
```

**实现机制**:
- **内存段**: 新文档写入内存中的段
- **自动刷新**: 定期将内存段持久化
- **MmapDirectory**: 使用内存映射文件提高性能

---

## 5. 深度对比

### 5.1 写入并发对比

| 特性 | SeekStorm | Lucene | Tantivy |
|------|-----------|--------|---------|
| **并发模型** | 分片信号量控制 | 单线程推荐 | 多线程安全 |
| **并发单位** | Shard (分片) | IndexWriter | IndexWriter |
| **控制方式** | Semaphore | 应用层控制 | RwLock |
| **推荐模式** | 多线程写入 | 单线程写入 | 单线程写入 |
| **扩展性** | 线性扩展 (增加分片) | 受限于单线程 | 受限于锁竞争 |

**深入分析**:

#### SeekStorm: 分片隔离 + 信号量

```rust
// 写入并发: O(shard_count)
// 每个分片: 信号量控制 (默认 1)
// 总并发: shard_count * semaphore_permits

// 示例: 8 个分片，信号量 = 2
// 最大并发写入: 8 * 2 = 16
```

**优势**:
- ✅ 分片隔离，互不干扰
- ✅ 可线性扩展 (增加分片即可)
- ✅ 灵活控制 (调整信号量大小)
- ✅ 无全局锁瓶颈

**劣势**:
- ❌ 需要预先设置分片数
- ❌ 分片间负载可能不均衡

#### Lucene: 单线程写入

```java
// 写入并发: O(1) - 单线程
// 所有写入顺序执行
```

**优势**:
- ✅ 实现简单，不易出错
- ✅ 段合并可以在后台线程
- ✅ 充分利用 CPU 缓存 (单线程局部性好)

**劣势**:
- ❌ 吞吐量受限 (单线程)
- ❌ 大索引写入慢

#### Tantivy: 多线程安全

```rust
// 写入并发: O(num_threads)
// 但受限于 RwLock 竞争
```

**优势**:
- ✅ 原生线程安全
- ✅ 可以利用多核

**劣势**:
- ❌ RwLock 竞争成为瓶颈
- ❌ 推荐单线程使用 (文档说明)

### 5.2 读取并发对比

| 特性 | SeekStorm | Lucene | Tantivy |
|------|-----------|--------|---------|
| **并发模型** | 分片并行读取 | 全局共享 | 全局共享 |
| **锁粒度** | Shard 级别 RwLock | 无锁 | 无锁 |
| **最大并发** | 受限于分片数 | 理论无限制 | 理论无限制 |
| **性能瓶颈** | 结果合并 | 无 | 无 |

**深入分析**:

#### SeekStorm: 分片并行 + 结果合并

```rust
// 读取并发: O(shard_count)
// 每个分片独立搜索
// 最后合并结果

let shard_vec = futures::future::join_all(
    index_ref.shard_vec.iter().map(|s| s.read())
).await;

// 然后合并和排序
result_object.results.sort_by(|a, b| {
    result_ordering_root(&shard_vec, ...)
});
```

**性能特点**:
- ✅ 真正的并行搜索
- ✅ 利用多核 CPU
- ❌ 结果合并有开销
- ❌ 排序需要协调所有分片

#### Lucene/Tantivy: 无锁读取

```java
// 读取并发: O(num_threads)
// 无锁设计，真正的高并发

// 多线程可以同时读取相同的 IndexReader
for (int i = 0; i < 100; i++) {
    executor.submit(() -> {
        // 无锁读取
        TopDocs docs = searcher.search(query, 10);
    });
}
```

**性能特点**:
- ✅ 无锁，真正的高并发
- ✅ 无额外开销
- ✅ 读吞吐量极高

### 5.3 近实时搜索对比

| 特性 | SeekStorm | Lucene | Tantivy |
|------|-----------|--------|---------|
| **机制** | 软提交 (内存可见) | NRT IndexReader | 自动提交 |
| **是否需要 commit** | 自动 (64K) | 可选 | 自动 |
| **查询延迟** | `realtime=true` 时最低 | `getReader()` 后可见 | 取决于刷新策略 |
| **实现方式** | 读锁保护内存数据 | 双缓冲区段 | 内存段 |

**深入分析**:

#### SeekStorm: 软提交 + 读锁保护

```rust
// 写入后立即可搜索 (realtime=true)
async fn search(&self, query_string: String, realtime: bool) {
    let shard_ref = self.read().await;
    
    if realtime {
        // 读取内存中的 Level 0 段
        search_level0(&shard_ref.segments_level0, query);
    }
    
    // 读取磁盘上的 Level 1+ 段
    search_committed(&shard_ref.segments_index, query);
}
```

**特点**:
- ✅ 写入后立即可搜索
- ✅ 读锁保护内存数据
- ✅ 自动硬提交 (64K 文档)

#### Lucene: NRT 双缓冲区

```java
// Lucene NRT 实现机制
IndexWriter writer = new IndexWriter(directory, config);

// 内存中有两个缓冲区:
// 1. 正在写入的缓冲区 (writer exclusive)
// 2. 可读取的缓冲区 (reader shared)

IndexReader reader = DirectoryReader.open(writer);
// reader 可以看到 buffer 2 中的数据
```

**特点**:
- ✅ 原子刷新
- ✅ reader 不受写入影响
- ❌ 需要显式调用 `getReader()`

#### Tantivy: 自动内存管理

```rust
// Tantivy 自动管理内存段
let writer = IndexWriter::new(directory, schema)?;
writer.add_document(doc)?;
// 段自动刷新到磁盘

// 使用 MmapDirectory 时
let directory = MmapDirectory::open(path)?;
// 操作系统管理内存映射
```

**特点**:
- ✅ 透明内存管理
- ✅ Mmap 高效
- ❌ 控制粒度较粗

### 5.4 锁策略对比

| 特性 | SeekStorm | Lucene | Tantivy |
|------|-----------|--------|---------|
| **读锁类型** | RwLock | 无锁 | Arc (无锁) |
| **写锁类型** | RwLock + Semaphore | 文件系统锁 | RwLock |
| **锁粒度** | 细 (分片级别) | 粗 (文件级别) | 中 (索引级别) |
| **锁开销** | 中 | 低 | 中低 |

**锁层次对比图**:

```
┌─────────────────────────────────────────────────────────────────┐
│                       SeekStorm 锁层次                               │
├─────────────────────────────────────────────────────────────────┤
│  Index (Arc<RwLock<Index>>)                                       │
│    ├─ 读锁: 多个搜索可以同时持有                                 │
│    └─ 写锁: 独占 (分片操作)                                      │
│                                                                   │
│  Shard (Arc<RwLock<Shard>>)                                      │
│    ├─ 读锁: 多个分片可以同时搜索                                  │
│    └─ 写锁: 分片内独占索引修改                                     │
│                                                                   │
│  Semaphore (分片内部)                                             │
│    └─ 信号量: 控制分片内并发写入数 (默认 1)                        │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                        Lucene 锁层次                                 │
├─────────────────────────────────────────────────────────────────┤
│  Directory (文件系统锁)                                           │
│    └─ NativeFSLock: 防止多进程同时修改                            │
│                                                                   │
│  IndexWriter (推荐单线程，无锁)                                   │
│    └─ 应用层保证线程安全                                          │
│                                                                   │
│  IndexReader (无锁)                                                │
│    └─ 完全线程安全，无需锁                                         │
│                                                                   │
│  SegmentMergeScheduler (独立线程池)                               │
│    └─ 每个合并任务独立锁                                           │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                        Tantivy 锁层次                                │
├─────────────────────────────────────────────────────────────────┤
│  Index (Arc<Index>)                                                │
│    └─ Arc 引用计数，无读写锁                                       │
│                                                                   │
│  IndexWriter (RwLock 保护)                                       │
│    ├─ 读锁: 可以并发读取元数据                                    │
│    └─ 写锁: 独占写入                                              │
│                                                                   │
│  IndexReader (Arc)                                                 │
│    └─ Arc 共享，无锁                                              │
└─────────────────────────────────────────────────────────────────┘
```

---

## 6. 设计哲学差异

### 6.1 并发模型哲学

| 方面 | SeekStorm | Lucene | Tantivy |
|------|-----------|--------|---------|
| **并发模型** | 异步优先 | 同步优先 | 同步优先 |
| **控制粒度** | 细粒度 (分片+信号量) | 粗粒度 (文件) | 中粒度 (索引) |
| **扩展性** | 线性扩展 | 垂直扩展 | 受限扩展 |
| **一致性** | 弱一致性 | 强一致性 | 强一致性 |

### 6.2 性能取向

| 方面 | SeekStorm | Lucene | Tantivy |
|------|-----------|--------|---------|
| **写入吞吐** | 高 (分片并行) | 中 (单线程) | 中高 (多线程但有锁) |
| **读取吞吐** | 高 (分片并行) | 极高 (无锁) | 高 (无锁) |
| **延迟** | 低 (异步) | 低 (同步) | 低 (同步) |
| **实时性** | 高 (软提交) | 中 (NRT) | 中 (自动提交) |

### 6.3 适用场景

| 场景 | 推荐引擎 | 原因 |
|------|----------|------|
| **高并发写入 + 实时搜索** | SeekStorm | 分片并行，软提交机制 |
| **复杂查询 + 高并发读取** | Lucene | 无锁读取，成熟生态 |
| **Rust 原生应用** | Tantivy | 类型安全，无缝集成 |
| **向量搜索** | SeekStorm | 原生支持 IVF 聚类 |
| **传统全文搜索** | Lucene | 功能最丰富 |

### 6.4 权衡取舍

```
┌─────────────────────────────────────────────────────────────────┐
│                     三者的设计权衡                                     │
└─────────────────────────────────────────────────────────────────┘

SeekStorm:
  并发写入 ↑    ──→  复杂度 ↑    ──→  维护成本 ↑
  线性扩展  ↑    ──→  内存使用 ↑   ──→  调试难度 ↑
  实时性   ↑    ──→  一致性 ↓   ──→

Lucene:
  生态     ↑    ──→  库大小 ↑     ──→  启动成本 ↑
  功能     ↑    ──→  学习曲线 ↑   ──→  灵活性 ↓
  写入     ↓    ──→

Tantivy:
  类型安全 ↑    ──→  编译时间 ↑   ──→
  内存安全 ↑    ──→  性能优化 ↓   ──→
  生态     ↓    ──→
```

---

## 7. 总结

### 7.1 核心差异总结

1. **并发控制方式不同**:
   - SeekStorm: 分片信号量 + RwLock
   - Lucene: 应用层控制 + 文件锁
   - Tantivy: RwLock + Arc

2. **扩展性不同**:
   - SeekStorm: 线性扩展 (增加分片)
   - Lucene: 垂直扩展 (更好的硬件)
   - Tantivy: 受限于锁竞争

3. **实时性实现不同**:
   - SeekStorm: 软提交 (读锁保护内存)
   - Lucene: NRT (双缓冲区)
   - Tantivy: 自动提交 (内存段)

4. **适用场景不同**:
   - SeekStorm: 高并发写入 + 向量搜索
   - Lucene: 复杂查询 + 成熟生态
   - Tantivy: Rust 原生应用

### 7.2 选择建议

| 需求 | 推荐方案 |
|------|----------|
| 高 QPS 写入 + 实时搜索 | SeekStorm |
| 向量搜索 + 混合搜索 | SeekStorm |
| 复杂全文查询 + 高并发读取 | Lucene |
| 需要 Lucene 生态 | Lucene |
| Rust 原生集成 | Tantivy |
| 单机部署，中等规模 | Tantivy |

---

## 附录: 代码位置参考

| 功能 | SeekStorm | Lucene | Tantivy |
|------|-----------|--------|---------|
| 运行时定义 | `seekstorm/src/lib.rs:482` | `org.apache.lucene.util.ThreadPool` | `rayon::ThreadPool` |
| 索引类型 | `seekstorm/src/index.rs:1542` | `org.apache.lucene.index.IndexWriter` | `tantivy::IndexWriter` |
| 信号量控制 | `seekstorm/src/index.rs:1584` | - | - |
| 并行搜索 | `seekstorm/src/search.rs:2083` | `org.apache.lucene.search.IndexSearcher` | `tantivy::Searcher` |