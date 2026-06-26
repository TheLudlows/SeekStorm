# SeekStorm 全文索引存储结构全景分析

---

## 一、整体架构：Index → Shard → Segment

```
Index (index.rs:1693)
  ├── shard_vec: Vec<Arc<RwLock<Shard>>>     ← 索引由多个分片组成
  ├── schema_map / meta / synonyms_map         ← 元数据层
  ├── symspell_option / completion_option       ← 拼写纠错 & 自动补全
  └── 向量搜索配置（dimensions, precision, quantization...）

Shard (index.rs:1550)                          ← 真正持有数据的单元
  ├── 倒排索引
  │   ├── segments_index: Vec<SegmentIndex>     ← 已提交的不可变层（Level 1+）
  │   ├── segments_level0: Vec<SegmentLevel0>   ← 未提交的可变层（Level 0）
  │   ├── level_index: Vec<LevelIndex>          ← 每层的元信息
  │   └── postings_buffer: Vec<u8>              ← Level 0 的原始缓冲区
  ├── 文档存储 (docstore)
  ├── 分面存储 (facet)
  ├── 向量存储 (vector)
  ├── 删除追踪 (delete)
  └── 字段元信息 / BM25 缓存 / 分词器...
```

---

## 二、磁盘文件清单

| 文件 | 常量 | 读写模式 | 内容 |
|------|------|---------|------|
| `index.bin` | `INDEX_FILENAME` | Mmap 只读 | 倒排索引（词项字典 + 压缩倒排列表 + 文档长度） |
| `docstore.bin` | `DOCSTORE_FILENAME` | Mmap 只读 | 压缩存储的原文档 |
| `delete.bin` | `DELETE_FILENAME` | File 追加写 | 已删除文档 ID |
| `facet.bin` | `FACET_FILENAME` | **MmapMut 可写** | 每文档分面值（定长记录） |
| `facet.json` | `FACET_VALUES_FILENAME` | JSON | 字符串分面值字典 |
| `vector.bin` | `VECTOR_FILENAME` | Mmap 只读 | 向量索引 |
| `schema.json` | `SCHEMA_FILENAME` | JSON | 索引模式定义 |
| `index.json` | `META_FILENAME` | JSON | 索引元配置 |
| `synonyms.json` | `SYNONYMS_FILENAME` | JSON | 同义词表 |
| `dictionary.csv` | `DICTIONARY_FILENAME` | CSV | 拼写纠错词典 |
| `completions.csv` | `COMPLETIONS_FILENAME` | CSV | 自动补全词典 |
| `files/` | `FILE_PATH` | 文件目录 | 二进制附件（如 PDF） |

---

## 三、倒排索引存储（核心）

### 3.1 分层结构

SeekStorm 采用 **LSM 式分层**：写入进 Level 0（内存），commit 后成为不可变层。

```
                    写入
                     ↓
    ┌─────────────────────────────────┐
    │  Level 0 (可变，内存)            │
    │  segments_level0: Vec<SegmentLevel0>
    │  postings_buffer: Vec<u8>  (400MB) │
    │  segment: AHashMap<u64, PostingListObject0>
    └─────────────┬───────────────────┘
                  │ commit
                  ↓
    ┌─────────────────────────────────┐
    │  Level 1+ (不可变，mmap/ram)     │
    │  segments_index: Vec<SegmentIndex>
    │  segment: AHashMap<u64, PostingListObjectIndex>
    │  byte_array_blocks / pointer    │
    └─────────────────────────────────┘
```

### 3.2 段（Segment）— 哈希分区

词项通过 64 位哈希分配到不同的 Segment（条带），实现并行查询。

**SegmentLevel0**（`index.rs:997`）— Level 0 段：
```rust
pub(crate) struct SegmentLevel0 {
    pub segment: AHashMap<u64, PostingListObject0>,  // key_hash → 倒排列表
    pub positions_compressed: Vec<u8>,                // 压缩位置数据
}
```

**SegmentIndex**（`index.rs:988`）— 已提交段：
```rust
pub(crate) struct SegmentIndex {
    pub byte_array_blocks: Vec<Vec<u8>>,                    // Ram 模式：压缩块数据
    pub byte_array_blocks_pointer: Vec<(usize, usize, u32)>, // Mmap 模式：(偏移, 长度, key数)
    pub segment: AHashMap<u64, PostingListObjectIndex>,      // key_hash → 倒排列表元数据
}
```

### 3.3 Level 0 倒排列表（链表式内存缓冲）

**PostingListObject0**（`index.rs:805`）— 每个词项一个对象：

```
postings_buffer 布局 (400MB Vec<u8>):

每个 posting 条目:
┌──────────┬──────────┬──────────────┬──────────────────┐
│ next (4B)│ docid(2B)│ pos_size(2-3B)│ positions (变长) │
│ u32 链指针│ u16 文档ID│ 含 embed_flag │ VInt 压缩位置   │
└──────────┴──────────┴──────────────┴──────────────────┘

PostingListObject0:
  pointer_first ──→ 链表头
  pointer_last  ──→ 链表尾（追加写入）
  posting_count    条目数
  max_block_score  最大分数（Top-K 剪枝）
  ngram_type       ngram 类型（短语搜索加速）
  position_count   位置总数
```

**位置压缩**：VInt 编码（stop bit `0b10000000`），首位置绝对值，后续 delta-1 编码。极短位置列表可嵌入 pos_size 字段（embed_flag=1），避免额外存储。

**字段编码**：一个词项在各字段中的位置被打包进同一条 posting，通过 `field_id_bits` 位宽编码字段 ID：
- 单字段：仅存 position_count
- 仅最长字段：`0b11000000` 前缀 + VInt
- 多字段：`FIELD_STOP_BIT` 分隔 + `(position_count << field_id_bits) | field_id`

### 3.4 已提交层倒排列表（块压缩）

**PostingListObjectIndex**（`index.rs:790`）— 持有多个 Block：

```
PostingListObjectIndex
  ├── posting_count / posting_count_ngram_1/2/3   ← 词频 + ngram 子频
  ├── max_list_score                               ← Top-K 剪枝
  └── blocks: Vec<BlockObjectIndex>                ← 每 64K 文档一个块

BlockObjectIndex (index.rs:778):
  ├── max_block_score: f32       ← 块级 WAND 剪枝
  ├── block_id: u32
  ├── compression_type_pointer: u32  ← 高2位=压缩类型, 低30位=位置数据大小
  ├── posting_count: u16
  ├── max_docid: u16
  └── pointer_pivot_p_docid: u16    ← 指针大小分界点
```

**四种 DocID 压缩格式**（`CompressionType`，`index.rs:834`）：

| 类型 | 条件 | 存储 |
|------|------|------|
| **Array** | 稀疏（< 4096 篇） | 排序 u16 数组，每篇 2 字节 |
| **Bitmap** | 密集（≥ 4096 篇） | 固定 8192 字节位图（65536 bit） |
| **RLE** | 连续游程 | `[runs_count, start1, len1, ...]` u16 对 |
| **Delta** | 默认关闭 | 变位宽 delta 编码 |

### 3.5 磁盘文件布局（index.bin）

```
index.bin 按层按块写入 (commit.rs:202-370):

┌──────────────────────────────────────────────┐
│ 4B: INDEX_HEADER_SIZE (版本头)               │
├──────────────────────────────────────────────┤
│ 块 (每块 = ROARING_BLOCK_SIZE = 65536 文档)   │
│  ┌─ 2B: longest_field_id                     │
│  ├─ per_field × 65536B: document_length_array│
│  ├─ 8B: indexed_doc_count                    │
│  ├─ 8B: positions_sum_normalized             │
│  ├─ segment_heads: (compressed_size, key_count) × N │
│  ├─ key_heads: key_count × key_head_size      │
│  │   ┌─ 8B: key_hash                         │
│  │   ├─ 2B: posting_count - 1                │
│  │   ├─ 2B: max_docid                        │
│  │   ├─ 2B: max_p_docid                      │
│  │   ├─ ngram counts (0-3B, 取决于 ngram 配置) │
│  │   ├─ 2B: pointer_pivot_p_docid            │
│  │   └─ 4B: compression_type_pointer         │
│  └─ key_bodies: 每个词项的压缩数据            │
│      ┌─ positions (VInt 压缩)                │
│      ├─ position_size_metadata (2-3B/posting) │
│      └─ docids (Array/Bitmap/RLE)            │
└──────────────────────────────────────────────┘

key_head_size:
  20B (无 ngram)
  22B (bigram)
  23B (trigram)
```

**Mmap 模式查询流程**：`byte_array_blocks_pointer[i]` 给出第 i 块的 `(offset, size, key_count)` → key head 在 `offset - key_count * key_head_size` 处 → 二分查找 key_hash → 定位 key body → 解压 docid + positions。

---

## 四、文档存储（docstore.bin）

```
docstore.bin 每层每块布局 (doc_store.rs:270-362):

┌────────────────────────────────────────┐
│ 4B: size_sum (u32 LE)                 │ ← 总大小
├────────────────────────────────────────┤
│ 65536 × 4B: pointer_table             │ ← 每文档一个 u32 结束偏移
│   pointer[i] = 文档 i 压缩数据的结束位置  │
├────────────────────────────────────────┤
│ 变长: compressed_docs                  │ ← 逐文档压缩的 JSON
│   doc[0] doc[1] doc[2] ... doc[N]     │   (None/Lz4/Snappy/Zstd)
└────────────────────────────────────────┘

读取文档: pointer[i-1]..pointer[i] → 解压 → 反序列化 JSON
```

**压缩选项**（`DocumentCompression`，`index.rs:510`）：
- `None` — 最快，最大
- `Lz4` — 快速压缩
- `Snappy` — 默认，平衡
- `Zstd` — 最小，最慢

---

## 五、分面存储（facet.bin + facet.json）

```
facet.bin 布局 — 定长记录数组 (MmapMut 可写):

每条文档记录 = facets_size_sum 字节
  ┌─────────┬─────────┬──────────┬─────────┐
  │ field_0 │ field_1 │ field_2  │  ...    │
  │ offset₀ │ offset₁ │ offset₂  │         │
  └─────────┴─────────┴──────────┴─────────┘

地址 = (facets_size_sum × docid) + facet.offset

各类型字节宽度:
  U8/I8                              → 1B    原始数值
  U16/I16/String16/StringSet16      → 2B    数值或 facet_value_id
  U32/I32/F32/String32/StringSet32  → 4B
  U64/I64/F64/Timestamp/Point       → 8B    (Point 为 Morton 编码)

facet.json — IndexMap<String, (Vec<String>, usize)>
  字符串值 → (词项列表, 文档计数)
  IndexMap 保持插入序 → 插入序号即 facet_value_id
```

**关键设计**：数值直接存原始值，字符串存整数 ID（字典序号），查询时无需访问 docstore。

---

## 六、向量存储（vector.bin）

```
vector.bin 每层布局 (vector.rs:969-1121):

┌────────────────────────────────────────┐
│ 4B: cluster_number (u32)              │
├────────────────────────────────────────┤
│ cluster_number × 4B: child_count[]    │ ← 每聚类子向量数
├────────────────────────────────────────┤
│ 向量记录 (按聚类排序):                  │
│  ┌─ VectorHeader (20B, packed):        │
│  │   doc_id: u16                       │
│  │   field_id: u32                     │
│  │   chunk_id: u32                     │
│  │   scale: f32                        │ ← 量化参数
│  │   norm: f32                         │
│  │   zero_point: i16                   │ ← 量化零点
│  │   sum_q: i32                        │ ← 量化总和
│  ├─ embedding data:                    │
│  │   F32 → dimensions × 4B             │
│  │   I8  → dimensions × 1B             │
│  └─────────────────────────────────────┘
└────────────────────────────────────────┘

精度:  Precision::F32 (4B/维) 或 Precision::I8 (1B/维)
量化:  Quantization::ScalarQuantizationI8 / TurboQuantI8 / None
未提交向量: block_vector_buffer: Vec<ParentMedoid>
```

**TurboQuant**（`vector_similarity.rs:1824`）：基于 FWHT 的快速量化，存储 `seed_mask` 向量。

---

## 七、删除追踪（delete.bin）

```
delete.bin — 简单追加写入:

┌────────┬────────┬────────┬─────┐
│ u64    │ u64    │ u64    │ ... │
│ doc_id │ doc_id │ doc_id │     │
└────────┴────────┴────────┴─────┘

内存: delete_hashset: AHashSet<usize>
写入: append u64 → flush
读取: 全量加载到 AHashSet
```

**注意**：未使用 Roaring Bitmap，而是简单哈希集合 + 追加文件。删除检查在搜索热路径中为 O(1) 哈希查找。

---

## 八、Ngram 索引存储

Ngram 索引**没有独立文件**，嵌入在倒排列表中：

- 每个 `PostingListObject0/Index` 内含 `posting_count_ngram_1/2/3` 和压缩计数
- Ngram 词项作为独立的 key_hash 存入同一个 `AHashMap<u64, PostingListObject>`
- key_hash 的低 3 位存储 `NgramType`（7 种：FF, FR, RF, FFF, RFF, FFR, FRF）
- 短语搜索通过交集 ngram 倒排列表实现，避免逐位置比对

---

## 九、辅助存储结构

### 9.1 文档长度压缩数组

```
document_length_compressed_array: Vec<[u8; 65536]>

每字段每块 65536 字节，每字节通过 DOCUMENT_LENGTH_COMPRESSION
查找表（256 项）解码为实际文档长度。用于 BM25 评分。
```

### 9.2 BM25 组件缓存

```
bm25_component_cache: [f32; 256]

256 项预计算缓存，避免搜索时重复计算 BM25 公式中的常量部分。
```

### 9.3 拼写纠错 & 自动补全

- **SymSpell**（`symspell_option`）：基于 `dictionary.csv` 的删除距离拼写纠错
- **PruningRadixTrie**（`completion_option`）：基于 `completions.csv` 的前缀自动补全

---

## 十、存储结构关系总图

```
┌──────────────────────────────────────────────────────────────┐
│                         Index                                 │
│  schema_map  meta  synonyms  symspell  completion  vec_config│
└──────────────────────┬───────────────────────────────────────┘
                       │ shard_vec
        ┌──────────────┼──────────────┐
        ▼              ▼              ▼
   ┌─────────┐   ┌─────────┐   ┌─────────┐
   │ Shard 0 │   │ Shard 1 │   │ Shard N │
   └────┬────┘   └─────────┘   └─────────┘
        │
        ├── index.bin (mmap)  ─────────── 倒排索引
        │   ├── LevelIndex[]           层元信息
        │   ├── SegmentIndex[]         不可变段
        │   │   ├── AHashMap<hash, PostingListObjectIndex>
        │   │   │              └── BlockObjectIndex[]
        │   │   ├── byte_array_blocks[] (Ram) 或 pointer[] (Mmap)
        │   │   └── key_heads + key_bodies (压缩词项+倒排)
        │   └── SegmentLevel0[]        可变段
        │       ├── AHashMap<hash, PostingListObject0>
        │       └── postings_buffer (400MB 链表)
        │
        ├── docstore.bin (mmap) ──────── 文档存储
        │   └── pointer_table + compressed_docs
        │
        ├── facet.bin (MmapMut) ──────── 分面数据
        │   └── 定长记录数组 (facets_size_sum × doc_count)
        ├── facet.json ──────────────── 字符串分面值字典
        │
        ├── vector.bin (mmap) ───────── 向量索引
        │   └── cluster_headers + VectorHeader[] + embeddings
        │
        ├── delete.bin ──────────────── 删除追踪
        │   └── AHashSet<usize> (内存) + 追加文件
        │
        ├── schema.json / index.json / synonyms.json ── 元数据
        └── dictionary.csv / completions.csv ────────── 纠错/补全
```

---

## 十一、关键设计特点总结

| 设计决策 | 体现 | 效果 |
|---------|------|------|
| **LSM 式分层** | Level 0 内存链表 → commit → 不可变压缩块 | 写入高吞吐，查询需合并多层 |
| **哈希分区段** | `AHashMap<u64, Posting>` 按 key_hash 分段 | 并行查询，无 B-tree 开销 |
| **多级压缩 DocID** | Array/Bitmap/RLE 按密度自动选择 | 稀疏列表省空间，密集列表 O(1) 检查 |
| **VInt 位置编码** | stop bit 变长 + delta + 嵌入式 | 短位置列表零额外开销 |
| **分面定长记录** | `facets_size_sum × docid + offset` | 查询时 O(1) 直接寻址，无需 docstore |
| **文档级压缩** | 每文档独立压缩 + 指针表 | 随机读取只解压一篇，非整块 |
| **Mmap vs Ram** | `AccessType` 枚举控制 | 小索引全内存，大索引利用 OS 页缓存 |
| **Ngram 内嵌** | 共用倒排列表，key_hash 低 3 位标记类型 | 短语搜索无需额外索引结构 |
| **删除用 HashSet** | `AHashSet<usize>` 而非 Roaring Bitmap | 简单实现，适合删除率不高的场景 |

---

## 十二、核心数据结构代码定位

| 结构体 | 文件 | 行号 | 用途 |
|--------|------|------|------|
| `Index` | index.rs | 1693-1767 | 索引根对象 |
| `Shard` | index.rs | 1550-1689 | 分片数据持有 |
| `LevelIndex` | index.rs | 767-773 | 层级元信息 |
| `SegmentIndex` | index.rs | 988-992 | 不可变段（已提交） |
| `SegmentLevel0` | index.rs | 997-1000 | 可变段（未提交） |
| `BlockObjectIndex` | index.rs | 778-786 | 块级倒排元数据 |
| `PostingListObjectIndex` | index.rs | 790-802 | 已提交倒排列表 |
| `PostingListObject0` | index.rs | 805-831 | Level 0 倒排列表 |
| `CompressionType` | index.rs | 834-840 | DocID 压缩类型 |
| `FacetField` | index.rs | 1519-1536 | 分面字段定义 |
| `SchemaField` | index.rs | 1099-1150 | 模式字段定义 |
| `IndexMetaObject` | index.rs | 1334-1415 | 索引元配置 |
| `IndexedField` | index.rs | 1220-1226 | 已索引字段运行时信息 |
| `VectorHeader` | vector.rs | 62-73 | 向量记录头 |
| `ParentMedoid` | clustering.rs | 42-56 | 未提交向量缓冲 |
| `TurboQuant` | vector_similarity.rs | 1824-1832 | FWHT 快速量化 |
| `NgramSet` | index.rs | 1829-1846 | Ngram 索引配置位掩码 |
| `NgramType` | index.rs | 1848-1867 | Ngram 类型标记 |
