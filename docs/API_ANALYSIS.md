# SeekStorm API 分析报告

> 生成日期: 2026-06-24
> 项目: SeekStorm

---

## 1. 技术架构

### Web 框架
- **HTTP 层**: Hyper 1.9.0 - 底层 HTTP 库
- **异步运行时**: Tokio 1.52.3
- **文档生成**: utoipa 5.5.0 (OpenAPI 3.0)
- **路由方式**: 基于路径模式匹配的自定义路由 (不使用高级框架如 actix-web/axum)

### 认证机制
- **API Key 验证**: 通过 HTTP Header `apikey` 传递
- **Master Key**: 用于创建/删除 API Key，通过环境变量 `MASTER_KEY_SECRET` 配置
- **多租户支持**: 每个 API Key 对应一个租户，数据隔离

### 速率限制
- 基于 API Key 的令牌桶算法
- 可配置每秒查询限制 (`rate_limit`)
- 宽容计数机制 (GRACE_VIOLATION_COUNT = 10)

---

## 2. API 端点分类详解

### 2.1 系统信息 (Info)

| 方法 | 路径 | 功能 |
|------|------|------|
| GET | `/api/v1/live` | 返回服务器状态和版本信息 (含 SIMD 状态) |
| GET | `/version` | 返回版本号字符串 |
| GET | `/` | 返回嵌入式 Web UI (HTML) |

---

### 2.2 API Key 管理 (API Key)

| 方法 | 路径 | 功能 | 权限 |
|------|------|------|------|
| POST | `/api/v1/apikey` | 创建新 API Key，返回 Base64 编码的 Key | **Master Key** |
| GET | `/api/v1/apikey` | 获取该 API Key 关联的所有索引信息 | API Key |
| DELETE | `/api/v1/apikey` | 删除指定 API Key 及其所有索引 | **Master Key** |

**请求/响应类型**:
```rust
// 创建 API Key 配额
pub struct ApikeyQuotaObject {
    pub indices_max: usize,           // 最大索引数
    pub indices_size_max: usize,      // 最大索引总大小 (MB)
    pub documents_max: usize,         // 最大文档数
    pub operations_max: usize,        // 每月最大操作数
    pub rate_limit: Option<usize>,    // 每秒查询限制
}

// 删除 API Key 请求
pub struct DeleteApikeyRequest {
    pub apikey_base64: String,        // Base64 编码的 API Key
}
```

---

### 2.3 索引管理 (Index)

| 方法 | 路径 | 功能 |
|------|------|------|
| POST | `/api/v1/index` | 创建新索引，返回 index_id |
| GET | `/api/v1/index/{index_id}` | 获取索引元信息 |
| DELETE | `/api/v1/index/{index_id}` | 删除索引 |
| PATCH | `/api/v1/index/{index_id}` | 提交索引 (commit) |
| PUT | `/api/v1/index/{index_id}` | 关闭索引 (close) |

**创建索引请求类型**:
```rust
pub struct CreateIndexRequest {
    pub index_name: String,
    pub schema: Vec<SchemaField>,
    pub similarity: LexicalSimilarity,       // Bm25f / Bm25fProximity
    pub tokenizer: TokenizerType,            // 词分词器类型
    pub stemmer: StemmerType,
    pub stop_words: StopwordType,
    pub frequent_words: FrequentwordType,
    pub ngram_indexing: u8,                  // N-gram 索引设置
    pub document_compression: DocumentCompression, // Snappy/Lz4/Zstd/None
    pub synonyms: Vec<Synonym>,
    pub spelling_correction: Option<SpellingCorrection>,
    pub query_completion: Option<QueryCompletion>,
    pub clustering: Clustering,              // 向量搜索聚类
    pub inference: Inference,                // 推理模型配置
}
```

**索引响应类型**:
```rust
pub struct IndexResponseObject {
    pub id: u64,
    pub name: String,
    pub schema: HashMap<String, SchemaField>,
    pub indexed_doc_count: usize,
    pub committed_doc_count: usize,
    pub operations_count: u64,
    pub query_count: u64,
    pub version: String,
    pub facets_minmax: HashMap<String, MinMaxFieldJson>,
}
```

**Commit 说明**:
- 将 RAM 中的未提交文档持久化到磁盘
- 自动触发: 每 64K 文档/分片 或 close_index 时
- 手动调用通常不需要，除非需要立即持久化

---

### 2.4 文档管理 (Document)

| 方法 | 路径 | 功能 |
|------|------|------|
| POST | `/api/v1/index/{index_id}/doc` | 索引单个或批量文档 |
| PATCH | `/api/v1/index/{index_id}/doc` | 更新单个或批量文档 |
| GET | `/api/v1/index/{index_id}/doc/{document_id}` | 获取指定文档 |
| DELETE | `/api/v1/index/{index_id}/doc/{document_id}` | 删除单个文档 |
| DELETE | `/api/v1/index/{index_id}/doc` | 批量删除/按查询删除/清空索引 |

**文档类型**:
```rust
pub type Document = IndexMap<String, Value>;  // 任意 JSON 键值对
```

**批量更新请求格式**:
```json
[
  [doc_id1, {"field1": "value1", ...}],
  [doc_id2, {"field1": "value2", ...}]
]
```

**删除逻辑** (按请求体):
- `"clear"` → 清空所有文档
- `u64` → 删除单个文档
- `[u64]` → 批量删除
- `SearchRequestObject` → 按查询条件删除

---

### 2.5 PDF 文件管理 (PDF File)

| 方法 | 路径 | 功能 |
|------|------|------|
| POST | `/api/v1/index/{index_id}/file` | 索引 PDF 文件 |
| GET | `/api/v1/index/{index_id}/file/{document_id}` | 获取 PDF 文件 |

**PDF 索引流程**:
1. 提取 PDF 元数据 (title, creation date)
2. 从 metatag/首行/文件名提取标题
3. 转换为 JSON 文档 (`title`, `body`, `url`, `date`)
4. 保存原始 PDF 到 `files` 子目录

**请求头**:
- `file`: 文件路径 (用于 `url` 字段)
- `date`: Unix 时间戳 (备用日期)

---

### 2.6 查询接口 (Query)

| 方法 | 路径 | 功能 |
|------|------|------|
| POST | `/api/v1/index/{index_id}/query` | 完整查询功能 |
| GET | `/api/v1/index/{index_id}/query` | 简化查询 (仅 URL 参数) |
| POST | `/api/v2/index/{index_id}/query` | 优化的向量搜索 (返回 doc_id 向量) |

**查询请求类型**:
```rust
pub struct SearchRequestObject {
    pub query_string: String,
    pub query_vector: Option<Value>,      // Base64 或数组
    pub enable_empty_query: bool,
    pub offset: usize,
    pub length: usize,
    pub result_type: ResultType,          // Count/Topk/TopkCount
    pub realtime: bool,                   // 包含未提交文档
    pub highlights: Vec<Highlight>,
    pub field_filter: Vec<String>,        // 搜索字段过滤
    pub fields: Vec<String>,              // 返回字段过滤
    pub distance_fields: Vec<DistanceField>,
    pub query_facets: Vec<QueryFacet>,
    pub facet_filter: Vec<FacetFilter>,
    pub result_sort: Vec<ResultSort>,
    pub query_type_default: QueryType,    // Union/Intersection/Phrase/Not
    pub query_rewriting: QueryRewriting,  // 搜索/建议/重写模式
    pub search_mode: SearchMode,          // Lexical/Vector/Hybrid
}
```

**搜索模式**:
```rust
pub enum SearchMode {
    Lexical,                              // 全文搜索
    Vector {                              // 向量搜索
        similarity_threshold: Option<f32>,
        ann_mode: AnnMode,                // Nprobe/Nscan
    },
    Hybrid {                              // 混合搜索 (RRF 融合)
        similarity_threshold: Option<f32>,
        ann_mode: AnnMode,
    },
}
```

**查询类型**:
```rust
pub enum QueryType {
    Union,        // OR (默认)
    Intersection, // AND
    Phrase,       // 短语 ""
    Not,          // 排除 -
}
```

**查询重写模式**:
```rust
pub enum QueryRewriting {
    SearchOnly,           // 仅搜索，无建议
    SearchSuggest { ... }, // 搜索 + 建议
    SearchRewrite { ... }, // 自动重写 + 建议
    SuggestOnly { ... },  // 仅返回建议
}
```

**查询响应类型**:
```rust
pub struct SearchResultObject {
    pub time: u128,
    pub original_query: String,
    pub query: String,                 // 重写后的查询
    pub offset: usize,
    pub length: usize,
    pub count: usize,                 // 返回结果数
    pub count_total: usize,           // 总匹配数
    pub query_terms: Vec<String>,
    pub results: Vec<Document>,       // 每个结果包含 _id 和 _score
    pub facets: HashMap<String, Facet>,
    pub suggestions: Vec<String>,
}
```

---

### 2.7 迭代器接口 (Iterator)

| 方法 | 路径 | 功能 |
|------|------|------|
| GET | `/api/v1/index/{index_id}/doc_id` | 通过 URL 参数迭代文档 ID |
| POST | `/api/v1/index/{index_id}/doc_id` | 通过 JSON 迭代文档 ID |

**迭代器请求类型**:
```rust
pub struct GetIteratorRequest {
    pub document_id: Option<u64>,    // 起始文档 ID (None=开头/结尾)
    pub skip: usize,                 // 跳过数量
    pub take: isize,                 // 获取数量 (>0 向前, <0 向后)
    pub include_deleted: bool,       // 包含已删除文档
    pub include_document: bool,      // 同时获取文档内容
    pub fields: Vec<String>,         // 返回字段
}
```

**分页逻辑**:
- 下一页: 取最后一个 `document_id`, `skip=1`, `take=+page_size`
- 上一页: 取第一个 `document_id`, `skip=1`, `take=-page_size`
- 检测结束: `返回长度 < 请求长度` 或 `skip < 请求 skip`

---

### 2.8 同义词管理 (Synonyms)

| 方法 | 路径 | 功能 |
|------|------|------|
| POST | `/api/v1/index/{index_id}/synonyms` | 添加同义词 |
| PUT | `/api/v1/index/{index_id}/synonyms` | 设置同义词 (替换) |
| GET | `/api/v1/index/{index_id}/synonyms` | 获取同义词列表 |

**同义词类型**:
```rust
pub struct Synonym {
    pub terms: Vec<String>,
    pub multiway: bool,  // 是否多向同义词
}
```

---

## 3. 错误处理

所有 HTTP 错误类型:

| 状态码 | 错误类型 | 描述 |
|--------|----------|------|
| 200 | - | 成功 |
| 400 | `BadRequest` | 请求格式错误 |
| 401 | `Unauthorized` | API Key 无效或缺失 |
| 404 | `IndexNotFound` | 索引不存在 |
| 404 | `ApiKeyNotFound` | API Key 不存在 |
| 404 | `DocumentNotFound` | 文档不存在 |
| 404 | `SynonymsNotFound` | 同义词不存在 |
| 404 | `FileNotFound` | 文件不存在 |
| 429 | `RateLimitExceeded` | 超出速率限制 |
| 501 | `NotImplemented` | 未实现的端点 |

---

## 4. 请求处理流程

### HTTP 请求处理流程 (http_server.rs:177-1481)

```
1. 提取 API Key → 验证
2. 解析路径 (最多 6 段)
3. 模式匹配路由 → 调用对应 API 函数
4. 速率限制检查
5. 权限验证 (Master Key vs API Key)
6. 索引存在性检查
7. 调用业务逻辑 (api_endpoints.rs)
8. 序列化响应 (JSON)
9. 返回 HTTP 响应
```

### 关键逻辑点

1. **API Key 验证**: 通过 SHA256 哈希匹配
2. **索引查找**: `apikey_object.index_list.get(index_id)`
3. **速率限制**: 令牌桶算法，带宽容计数
4. **实时搜索**: `realtime=true` 时包含未提交文档
5. **Auto Commit**: 每 64K 文档自动触发 commit

---

## 5. 特色功能

1. **混合搜索**: 结合 BM25 和向量相似度的 RRF 融合
2. **近似最近邻**: 支持聚类加速 (Nprobe/Nscan)
3. **地理搜索**: Morton 编码范围查询
4. **拼写纠正**: SymSpell 算法
5. **查询补全**: 前缀字典
6. **高亮**: KWIC (Keyword In Context) 片段
7. **分面**: 字段值统计和过滤
8. **实时搜索**: 无需 commit 即可搜索新索引文档
9. **多分片**: 负载均衡的分布式索引

---

## 6. 数据流

### 索引流程
```
文档 → Tokenizer → 索引结构 → RAM → Commit → Mmap/Disk
       ↓
    向量化 (可选) → HNSW 聚类 → 量化 (I8) → 存储
```

### 查询流程
```
查询字符串 → 分词 → 查询解析 → 倒排索引查询 → BM25 评分
                        ↓ (如果 search_mode=Vector/Hybrid)
                    向量化 → HNSW 搜索 → 相似度计算
                        ↓
                    结果融合 (RRF) → 高亮 → 分页 → 返回
```

---

## 7. API 设计特点

- **RESTful 风格**: 资源导向的 URL 设计
- **JSON 序列化**: 所有请求/响应使用 JSON
- **多版本支持**: `/api/v1` 和 `/api/v2` 共存
- **OpenAPI 文档**: 自动生成，支持 Swagger UI
- **嵌入式 UI**: 直接提供 Web 界面用于测试
- **原子操作**: 文件原子写入保证数据一致性

---

## 8. 核心数据结构索引

### 相似度度量 (LexicalSimilarity)
- `Bm25f`: 考虑多字段文档的 BM25
- `Bm25fProximity`: 考虑词邻近度的 BM25 (默认)

### 分词器类型 (TokenizerType)
- `AsciiAlphabetic`: 仅 ASCII 字母 (基准兼容)
- `UnicodeAlphanumeric`: Unicode 字母数字 (默认)
- `UnicodeAlphanumericFolded`: 带变音折叠的 Unicode
- `Whitespace`: 按空格分词
- `WhitespaceLowercase`: 按空格分词并转小写
- `UnicodeAlphanumericZH`: 中文分词 (需 `zh` feature)

### 文档压缩 (DocumentCompression)
- `None`: 无压缩 (最快，最大)
- `Lz4`: Lz4 压缩 (快，中等大小)
- `Snappy`: Snappy 压缩 (默认，比 Lz4 稍小)
- `Zstd`: Zstd 压缩 (最慢，最小)

### 结果类型 (ResultType)
- `Count`: 仅统计总数
- `Topk`: 仅返回 Top-K 结果
- `TopkCount`: 返回 Top-K + 总数 (默认)

### 访问类型 (AccessType)
- `Ram`: 完整预加载到内存
- `Mmap`: 内存映射文件 (默认)

---

## 附录: 文件位置参考

| 功能 | 文件路径 |
|------|----------|
| HTTP 服务器 | `seekstorm_server/src/http_server.rs` |
| API 端点实现 | `seekstorm_server/src/api_endpoints.rs` |
| 多租户支持 | `seekstorm_server/src/multi_tenancy.rs` |
| 索引数据结构 | `seekstorm/src/index.rs` |
| 搜索逻辑 | `seekstorm/src/search.rs` |
| 向量搜索 | `seekstorm/src/vector_similarity.rs` |
| 分词器 | `seekstorm/src/tokenizer.rs` |