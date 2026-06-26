# SeekStorm 文档索引完整流程分析

> 生成日期: 2026-06-24
> 项目: SeekStorm

---

## 目录

1. [概述](#1-概述)
2. [入口函数](#2-入口函数)
3. [详细流程](#3-详细流程)
4. [全文索引构建](#4-全文索引构建)
5. [向量索引构建](#5-向量索引构建)
6. [文档存储](#6-文档存储)
7. [索引数据结构](#7-索引数据结构)
8. [压缩与编码](#8-压缩与编码)

---

## 1. 概述

SeekStorm 的文档索引流程是一个高度优化的多阶段管道，支持以下功能：

- **全文索引**: 基于 BM25 的倒排索引
- **向量索引**: 支持 IVF (Inverted File) 聚类和量化
- **混合索引**: 同时支持全文和向量搜索
- **实时搜索**: 索引后立即可搜索
- **自动提交**: 定期将内存数据持久化到磁盘

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           文档索引流程                                    │
└─────────────────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ 1. HTTP 请求层 (http_server.rs)                                          │
│    - 接收 POST /api/v1/index/{index_id}/doc 请求                         │
│    - 验证 API Key                                                        │
│    - 获取索引和 Shard                                                     │
└─────────────────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ 2. 索引入口层 (Index::index_document)                                   │
│    - 分配全局文档 ID (docid_global)                                      │
│    - 计算 Shard ID: shard_id = docid_global % shard_number              │
│    - 获取 Shard 信号量 (控制并发)                                          │
│    - 在 INDEX_RUNTIME 中异步执行                                         │
└─────────────────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ 3. 文档预处理 (index_document_shard)                                    │
│    ┌───────────────┬───────────────┬───────────────┬───────────────┐     │
│    │ 文本字段处理  │ 向量字段处理  │ 分面字段处理  │ 原始文档存储  │     │
│    │ (分词)       │ (推理+量化)   │ (MMAP写入)    │ (压缩存储)    │     │
│    └───────────────┴───────────────┴───────────────┴───────────────┘     │
└─────────────────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ 4. 索引结构构建 (index_document_shard_2)                                │
│    ┌───────────────┬───────────────┬───────────────┐                    │
│    │ 倒排索引      │ 向量索引      │ N-gram 索引   │                    │
│    │ (PostingList) │ (IVF+量化)    │ (位置增强)    │                    │
│    └───────────────┴───────────────┴───────────────┘                    │
└─────────────────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ 5. 自动提交检查                                                          │
│    - 检查是否跨越 block 边界 (每 65,536 文档)                              │
│    - 如需提交: 调用 commit_lexical_shard / commit_vector_shard           │
└─────────────────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ 6. 完成并释放信号量                                                      │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 2. 入口函数

### 2.1 API 层入口

```rust
// 位置: seekstorm_server/src/http_server.rs:870-928
("api", "v1", "index", _, "doc", _, &Method::POST) => {
    // 1. 验证 API Key
    let Some(apikey) = apikey_header else { return HttpServerError::Unauthorized.into(); };
    let Some(apikey_hash) = get_apikey_hash(apikey, &apikey_list).await else {
        return HttpServerError::Unauthorized.into();
    };

    // 2. 速率限制检查
    if rate_limit(&apikey_list, apikey_hash).await {
        return HttpServerError::RateLimitExceeded.into();
    }

    // 3. 获取索引 Shard
    let Ok(index_id) = parts[3].parse() else {
        return HttpServerError::BadRequest("index_id invalid or missing".to_string()).into();
    };
    let Some(index_arc) = apikey_object.index_list.get(&index_id) else {
        return HttpServerError::IndexNotFound.into();
    };

    // 4. 读取请求体并解析文档
    let request_bytes = req.into_body().collect().await.unwrap().to_bytes();
    let request_string = str::from_utf8(&request_bytes).unwrap();

    let status_object = if !request_string.trim().starts_with('[') {
        // 单个文档
        let document_object = serde_json::from_str(request_string)?;
        index_document_api(&index_arc_clone, document_object).await
    } else {
        // 批量文档
        let document_object_vec = serde_json::from_str(request_string)?;
        index_documents_api(&index_arc_clone, document_object_vec).await
    };
}
```

### 2.2 索引层入口

```rust
// 位置: seekstorm/src/index.rs:5270-5290
async fn index_document(&self, document: Document, file: FileType) {
    // 1. 获取 Shard 数量和全局文档 ID
    let shard_number = self.read().await.shard_number;
    let docid_global_arc = self.read().await.docid_global.clone();
    let mut docid_global = docid_global_arc.write().await;
    let docid_global_clone = *docid_global;

    // 2. 计算 Shard ID (负载均衡)
    let shard_id = *docid_global % shard_number;

    // 3. 获取 Shard 和信号量
    let shard_arc = self.read().await.shard_vec[shard_id].clone();
    let semaphore = shard_arc.read().await.semaphore.clone();
    let permit = semaphore.acquire_owned().await.unwrap();

    // 4. 原子递增全局文档 ID
    *docid_global += 1;
    drop(docid_global);

    // 5. 在专用线程池中执行索引
    INDEX_RUNTIME.handle().spawn(async move {
        shard_arc
            .index_document_shard(document, file, docid_global_clone)
            .await;
        drop(permit);  // 释放信号量
    });
}
```

---

## 3. 详细流程

### 阶段 1: 文档字段提取和预处理

```rust
// 位置: seekstorm/src/index.rs:5320-5478
async fn index_document_shard(&self, document: Document, file: FileType, docid_global: usize) {
    let shard_arc_clone = self.clone();
    let shard_ref = self.read().await;
    
    // 获取 Schema 配置
    let schema = shard_ref.indexed_schema_vec.clone();
    let ngram_indexing = shard_ref.meta.ngram_indexing;
    let indexed_field_vec_len = shard_ref.indexed_field_vec.len();
    let tokenizer_type = shard_ref.meta.tokenizer;
    let segment_number_mask1 = shard_ref.segment_number_mask1;
    
    drop(shard_ref);

    let token_per_field_max: u32 = u16::MAX as u32;
    let mut unique_terms: AHashMap<String, TermObject> = AHashMap::new();
    let mut field_vec: Vec<(usize, u8, u32, u32)> = Vec::new();

    // 遍历 Schema 中的每个字段
    for schema_field in schema.iter() {
        if !schema_field.index_lexical {
            continue;  // 跳过非索引字段
        }

        if let Some(field_value) = document.get(&schema_field.field) {
            let mut non_unique_terms: Vec<NonUniqueTermObject> = Vec::new();
            let mut nonunique_terms_count = 0u32;

            // 提取字段文本
            let text = match schema_field.field_type {
                FieldType::Json => {
                    if matches!(field_value, Value::Object { .. }) {
                        let mut strings_vec: Vec<String> = Vec::new();
                        object_values_to_string_vec_recursive(field_value, &mut strings_vec);
                        strings_vec.join(" ")
                    } else {
                        serde_json::from_value::<String>(field_value.clone())
                            .unwrap_or(field_value.to_string())
                    }
                }
                FieldType::Text | FieldType::String16 | FieldType::String32 => {
                    serde_json::from_value::<String>(field_value.clone())
                        .unwrap_or(field_value.to_string())
                }
                _ => field_value.to_string(),
            };

            // 调用 Tokenizer 分词
            tokenizer(
                &shard_ref2,
                &text,
                &mut unique_terms,
                &mut non_unique_terms,
                tokenizer_type,
                segment_number_mask1,
                &mut nonunique_terms_count,
                token_per_field_max,
                MAX_POSITIONS_PER_TERM,
                false,
                &mut query_type_mut,
                ngram_indexing,
                schema_field.indexed_field_id,
                indexed_field_vec_len,
            ).await;

            // 压缩文档长度
            let document_length_compressed: u8 = int_to_byte4(nonunique_terms_count);
            let document_length_normalized: u32 =
                DOCUMENT_LENGTH_COMPRESSION[document_length_compressed as usize];

            // 保存字段信息
            field_vec.push((
                schema_field.indexed_field_id,
                document_length_compressed,
                document_length_normalized,
                nonunique_terms_count,
            ));
        }
    }
}
```

### 阶段 2: N-gram 处理

```rust
// 位置: seekstorm/src/index.rs:5401-5467
let ngrams: Vec<String> = unique_terms
    .iter()
    .filter(|term| term.1.ngram_type != NgramType::SingleTerm)
    .map(|term| term.1.term.clone())
    .collect();

for term in ngrams.iter() {
    let ngram = unique_terms.get(term).unwrap();

    match ngram.ngram_type {
        NgramType::SingleTerm => {}
        NgramType::NgramFF | NgramType::NgramFR | NgramType::NgramRF => {
            let term_ngram1 = ngram.term_ngram_1.clone();
            let term_ngram2 = ngram.term_ngram_0.clone();

            // 统计每个 n-gram 组成词的位置数量
            for indexed_field_id in 0..indexed_field_vec_len {
                let positions_count_ngram1 =
                    unique_terms[&term_ngram1].field_positions_vec[indexed_field_id].len();
                let positions_count_ngram2 =
                    unique_terms[&term_ngram2].field_positions_vec[indexed_field_id].len();

                if positions_count_ngram1 > 0 {
                    ngram.field_vec_ngram1.push((indexed_field_id, positions_count_ngram1 as u32));
                }
                if positions_count_ngram2 > 0 {
                    ngram.field_vec_ngram2.push((indexed_field_id, positions_count_ngram2 as u32));
                }
            }
        }
        _ => {
            // 三词 N-gram (NgramFFF 等)
            let term_ngram1 = ngram.term_ngram_2.clone();
            let term_ngram2 = ngram.term_ngram_1.clone();
            let term_ngram3 = ngram.term_ngram_0.clone();

            for indexed_field_id in 0..indexed_field_vec_len {
                // ... 类似的统计逻辑
            }
        }
    }
}
```

### 阶段 3: 同义词扩展

```rust
// 位置: seekstorm/src/index.rs:5859-5888
let mut unique_terms = document_item.unique_terms;

if !shard_mut.synonyms_map.is_empty() {
    let unique_terms_clone = unique_terms.clone();
    for term in unique_terms_clone.iter() {
        if term.1.ngram_type == NgramType::SingleTerm {
            // 查找同义词
            let synonym = shard_mut.synonyms_map.get(&term.1.key_hash).cloned();
            if let Some(synonym) = synonym {
                for synonym_term in synonym {
                    let mut term_clone = term.1.clone();
                    term_clone.key_hash = synonym_term.1.0;
                    term_clone.key0 = synonym_term.1.1;
                    term_clone.term = synonym_term.0.clone();

                    if let Some(existing) = unique_terms.get_mut(&synonym_term.0) {
                        // 同义词词项已存在，合并位置
                        existing.field_positions_vec
                            .iter_mut()
                            .zip(term_clone.field_positions_vec.iter())
                            .for_each(|(x1, x2)| {
                                x1.extend_from_slice(x2);
                                x1.sort_unstable();
                            });
                    } else {
                        // 新增同义词词项
                        unique_terms.insert(synonym_term.0.clone(), term_clone);
                    };
                }
            }
        }
    }
}
```

### 阶段 4: 构建 Posting List

```rust
// 位置: seekstorm/src/index.rs:5890-5892
for term in unique_terms {
    shard_mut.index_posting(term.1, docid_local, false, 0, 0, 0);
}
```

### 阶段 5: 向量索引和分面处理

```rust
// 位置: seekstorm/src/index.rs:5497-5918
async fn index_document_shard_2(&self, document_item: DocumentItem, file: FileType, docid_global: usize) {
    let mut shard_mut = self.write().await;
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

    // 向量索引
    if shard_mut.is_vector_indexing {
        shard_mut.index_vector_shard(docid_local, &document_item.document).await;
    }

    // 分面字段处理
    if !shard_mut.facets.is_empty() {
        let facets_size_sum = shard_mut.facets_size_sum;
        for i in 0..shard_mut.facets.len() {
            let facet = &mut shard_mut.facets[i];
            if let Some(field_value) = document_item.document.get(&facet.name) {
                let address = (facets_size_sum * docid_local) + facet.offset;

                match facet.field_type {
                    FieldType::U8 => { /* 写入 MMAP */ }
                    FieldType::U16 => { /* 写入 MMAP */ }
                    FieldType::String16 => { /* 更新分面字典 */ }
                    FieldType::Point => { /* Morton 编码 */ }
                    // ...
                }
            }
        }
    }

    // 存储原始文档
    if !shard_mut.stored_field_names.is_empty() {
        shard_mut.store_document(docid_local, document_item.document);
    }

    // 提交后预热
    if do_commit {
        drop(shard_mut);
        warmup(self).await;
    }
}
```

---

## 4. 全文索引构建

### 4.1 Tokenizer 分词流程

```rust
// 位置: seekstorm/src/tokenizer.rs:122-400
pub(crate) async fn tokenizer(
    index: &Shard,
    text: &str,
    unique_terms: &mut AHashMap<String, TermObject>,
    non_unique_terms: &mut Vec<NonUniqueTermObject>,
    tokenizer: TokenizerType,
    // ...
) {
    // 文本预处理
    let text_normalized = match tokenizer {
        TokenizerType::UnicodeAlphanumeric => text.to_lowercase(),
        TokenizerType::UnicodeAlphanumericFolded => {
            fold_diacritics_accents_ligatures_zalgo_umlaut(text)  // 变音折叠
        }
        TokenizerType::AsciiAlphabetic => text.to_ascii_lowercase(),
        // ...
    };

    // 分词循环
    let mut start = false;
    let mut start_pos = 0;
    let mut position = 0u16;
    
    for char in text_normalized.char_indices() {
        start = match char.1 {
            token if regex_syntax::is_word_character(token) => {
                if !start {
                    start_pos = char.0;
                }
                true
            }
            _ => {
                if start {
                    let token = &text_normalized[start_pos..char.0];
                    process_token(token, &mut unique_terms, &mut non_unique_terms, position);
                }
                false
            }
        };
        if start {
            position += 1;
        }
    }
}

fn process_token(
    token: &str,
    unique_terms: &mut AHashMap<String, TermObject>,
    non_unique_terms: &mut Vec<NonUniqueTermObject>,
    position: u16
) {
    // 1. 计算词项哈希
    let term_hash = calculate_hash(token);
    let key_hash = (term_hash as u128) * 11400714819323198485u128;
    let key0 = (key_hash >> (64 - 8)) as u8;

    // 2. 更新或创建 TermObject
    if let Some(term_object) = unique_terms.get_mut(token) {
        term_object.field_positions_vec[indexed_field_id].push(position);
    } else {
        let mut field_positions_vec = vec![vec![]; indexed_field_number];
        field_positions_vec[indexed_field_id].push(position);
        
        unique_terms.insert(token.to_string(), TermObject {
            term: token.to_string(),
            term_hash,
            key_hash,
            key0,
            field_positions_vec,
            // ...
        });
    }

    // 3. 记录非唯一词项（用于 n-gram）
    non_unique_terms.push(NonUniqueTermObject {
        term: token.to_string(),
        position,
        indexed_field_id,
    });
}
```

### 4.2 Posting List 构建

```rust
// 位置: seekstorm/src/index_posting.rs:16-846
pub(crate) fn index_posting(
    &mut self,
    term: TermObject,
    doc_id: usize,
    restore: bool,
    posting_count_ngram_1_compressed: u8,
    posting_count_ngram_2_compressed: u8,
    posting_count_ngram_3_compressed: u8,
) {
    // 1. 位置差分压缩
    let mut field_positions_vec: Vec<Vec<u16>> = Vec::new();
    for positions_uncompressed in term.field_positions_vec.iter() {
        let mut positions: Vec<u16> = Vec::new();
        let mut previous_position: u16 = 0;
        for pos in positions_uncompressed.iter() {
            if positions.is_empty() {
                positions.push(*pos);
            } else {
                positions.push(*pos - previous_position - 1);  // 差分
            }
            previous_position = *pos;
        }
        field_positions_vec.push(positions);
    }

    // 2. 获取或创建 PostingListObject0
    let strip_object0 = self.segments_level0.get_mut(term.key0 as usize).unwrap();
    let value = strip_object0
        .segment
        .entry(term.key_hash)
        .or_insert(PostingListObject0 {
            posting_count_ngram_1_compressed,
            posting_count_ngram_2_compressed,
            posting_count_ngram_3_compressed,
            ..Default::default()
        });
    let exists: bool = value.posting_count > 0;

    // 3. 确定 Posting Pointer Size
    let mut posting_pointer_size = 
        if value.size_compressed_positions_key < 32_768 && value.posting_count < 65_535 {
            value.pointer_pivot_p_docid = value.posting_count as u16 + 1;
            2u8
        } else {
            3u8
        };

    // 4. 位置压缩（变长编码）
    let positions_compressed_pointer = if !embed_flag {
        for field_positions in field_positions_vec.iter() {
            compress_positions(
                field_positions,
                &mut strip_object0.positions_compressed,
                &mut positions_compressed_pointer,
            );
        }
        positions_compressed_pointer
    } else {
        0
    };

    // 5. 写入 Posting Buffer
    let mut write_pointer_base = self.postings_buffer_pointer;
    let mut write_pointer = self.postings_buffer_pointer + 8;

    // 写入字段向量信息（位置数量等）
    write_field_vec(
        &mut self.postings_buffer,
        &mut write_pointer,
        &field_vec,
        // ...
    );

    // 写入位置数据
    if !embed_flag {
        block_copy_mut(
            &mut strip_object0.positions_compressed,
            0,
            &mut self.postings_buffer,
            write_pointer,
            positions_compressed_pointer,
        );
        write_pointer += positions_compressed_pointer;
    }

    // 6. 更新 PostingListObject0
    let docid_lsb = (doc_id & 0xFFFF) as u16;
    if exists {
        // 更新现有 Posting List
        value.posting_count += 1;
        value.position_count += positions_count_sum;
        value.size_compressed_positions_key += positions_stack;
        
        if docid_lsb > value.docid_old {
            value.docid_delta_max = 
                cmp::max(value.docid_delta_max, docid_lsb - value.docid_old - 1);
        }
        value.docid_old = docid_lsb;

        // 链接到上一个 Posting
        write_u32(
            write_pointer_base as u32,
            &mut self.postings_buffer,
            value.pointer_last,
        );
        value.pointer_last = write_pointer_base;
    } else {
        // 创建新 Posting List
        *value = PostingListObject0 {
            pointer_first: write_pointer_base,
            pointer_last: write_pointer_base,
            posting_count: 1,
            position_count: positions_count_sum,
            ngram_type: term.ngram_type.clone(),
            term_ngram1: term.term_ngram_2,
            term_ngram2: term.term_ngram_1,
            term_ngram3: term.term_ngram_0,
            size_compressed_positions_key: value.size_compressed_positions_key + positions_stack,
            docid_delta_max: docid_lsb,
            docid_old: docid_lsb,
            ..*value
        };
    }

    // 7. 写入文档 ID
    write_pointer_base += 4;
    write_u16_ref(docid_lsb, &mut self.postings_buffer, &mut write_pointer_base);

    // 8. 写入压缩位置大小
    write_u16_ref(
        if embed_flag {
            compressed_position_size | 0b10000000_00000000
        } else {
            compressed_position_size & 0b01111111_11111111
        } as u16,
        &mut self.postings_buffer,
        &mut write_pointer_base,
    );

    self.postings_buffer_pointer = write_pointer;
}
```

### 4.3 位置压缩算法

```rust
// 位置使用变长编码 (VLE) 压缩
pub(crate) fn compress_positions(
    positions: &[u16],
    buffer: &mut [u8],
    pointer: &mut usize,
) {
    let mut count = 0;
    for pos in positions {
        count += 1;
        
        if pos < 128 {
            // 1 字节: 0xxxxxxx
            buffer[*pointer] = pos as u8;
        } else if pos < 16384 {
            // 2 字节: 1xxxxxxx 0xxxxxxx
            buffer[*pointer] = ((pos >> 7) as u8) | 0b10000000;
            buffer[*pointer + 1] = (pos & 0b01111111) as u8;
        } else if pos < 2097152 {
            // 3 字节: 1xxxxxxx 1xxxxxxx 0xxxxxxx
            buffer[*pointer] = ((pos >> 14) as u8) | 0b10000000;
            buffer[*pointer + 1] = ((pos >> 7) & 0b01111111) as u8 | 0b10000000;
            buffer[*pointer + 2] = (pos & 0b01111111) as u8;
        }
        *pointer += encoded_size;
    }
}
```

---

## 5. 向量索引构建

### 5.1 向量推理流程

```rust
// 位置: seekstorm/src/vector.rs
pub enum Inference {
    Model2Vec {
        model: Model,           // 预定义模型 (PotionBase2M 等)
        chunk_size: usize,      // 文本分块大小
        quantization: Quantization,  // 量化方式
    },
    Model2VecCustom {
        path: String,           // 自定义模型路径
        chunk_size: usize,
        quantization: Quantization,
    },
    External {
        dimensions: usize,
        precision: Precision,
        quantization: Quantization,
        similarity: VectorSimilarity,
    },
}

// 文档向量化
async fn index_vector_shard(&mut self, docid_local: usize, document: &Document) {
    for schema_field in self.indexed_schema_vec.iter() {
        if !schema_field.index_vector {
            continue;
        }

        if let Some(field_value) = document.get(&schema_field.field) {
            let embedding = match &self.meta.inference {
                Inference::Model2Vec { model, .. } => {
                    // 使用内置模型生成向量
                    model2vec::embed(model, field_value).await
                }
                Inference::External { .. } => {
                    // 使用外部提供的向量
                    decode_vector(field_value)
                }
            };

            // 量化并存储
            let quantized = quantize_vector(&embedding, self.meta.quantization);
            self.store_vector(docid_local, schema_field.indexed_field_id, quantized);
        }
    }
}
```

### 5.2 向量量化

```rust
// 标量量化 (ScalarQuantizationI8)
pub(crate) fn quantize_vector(
    vector: &[f32],
    quantization: Quantization,
) -> QuantizedVector {
    match quantization {
        Quantization::ScalarQuantizationI8 { scale, zero_point } => {
            vector.iter()
                .map(|&x| {
                    let quantized = ((x - zero_point) * scale) as i8;
                    quantized.clamp(i8::MIN, i8::MAX)
                })
                .collect()
        }
        Quantization::TurboQuantI8 { qjl_seed } => {
            // TurboQuant 使用 QJL (Quantized Johnson-Lindenstrauss)
            // 结合 Fast Walsh-Hadamard Transform
            turbo_quantize(vector, qjl_seed)
        }
        Quantization::None => {
            vector.to_vec()
        }
    }
}
```

### 5.3 IVF (Inverted File) 聚类构建

```rust
// 位置: seekstorm/src/clustering.rs:229-778
pub(crate) async fn cluster_vector_shard(&mut self, sort: bool) -> Vec<Medoid> {
    // 1. 确定聚类数量
    let vector_count_block = self.block_vector_buffer.len();
    let cluster_number = match self.meta.clustering {
        Clustering::Auto => (vector_count_block.sqrt() * 2).max(1),  // 自动: sqrt(n) * 2
        Clustering::None => 1,                                       // 无聚类
        Clustering::Fixed(n) => n.min(vector_count_block).max(1),    // 固定数量
    };

    // 2. 选择第一个聚类中心 (Medoid)
    // 计算所有向量的全局均值，选择与均值最相似的向量作为第一个中心
    let mut sum_vector = vec![0f32; self.vector_dimensions];
    for i in (0..vector_count_block).step_by(vector_step) {
        accumulate_simd(&mut sum_vector, &self.block_vector_buffer[i].embedding);
    }
    let mean_vector = compute_mean(&sum_vector, vector_count_block_step);
    
    // 选择与均值最相似的向量作为第一个 Medoid
    let mut best_similarity = f32::MIN;
    for (i, candidate) in self.block_vector_buffer.iter().enumerate() {
        let similarity = similarity_embedding_simd(&mean_vector, &candidate.embedding, ...);
        if similarity > best_similarity {
            medoid.medoid_index = i;
            best_similarity = similarity;
        }
    }

    // 3. 迭代选择剩余聚类中心 (贪婪策略)
    // 每次选择能够最大化相似度增益的点作为新的聚类中心
    for cluster_id in 1..cluster_number {
        let mut best_medoid_similarity_sum = f32::MIN;
        
        // 采样候选中心 (跳过已选择的 Medoid)
        for i in (0..vector_count_block).skip(cluster_id).step_by(medoid_step) {
            if self.block_vector_buffer[i].is_medoid {
                continue;
            }
            
            let record_outer_simd = QuerySimd::new(&self.block_vector_buffer[i].embedding);
            let mut similarity_sum = 0.0;
            
            // 计算作为新中心能带来的相似度增益
            for j in (0..vector_count_block).skip(cluster_id).step_by(vector_step) {
                if i != j && !self.block_vector_buffer[j].is_medoid {
                    let similarity = similarity_embedding_simd(&record_outer_simd, ...);
                    if similarity > self.block_vector_buffer[j].similarity {
                        similarity_sum += similarity - self.block_vector_buffer[j].similarity;
                    }
                }
            }
            
            if similarity_sum > best_medoid_similarity_sum {
                medoid.medoid_index = i;
                best_medoid_similarity_sum = similarity_sum;
            }
        }
    }

    // 4. K-Means 迭代优化聚类
    loop {
        // 4.1 计算每个聚类的质心 (所有向量的均值)
        for (_medoid_index, centroid) in centroid_map.iter_mut() {
            let sum_vector = compute_sum(&cluster.vectors);
            centroid.centroid = sum_vector / centroid.child_count;
            centroid.query_simd = QuerySimd::new(&centroid.centroid);
        }

        // 4.2 重新分配向量到最近的质心
        for i in 0..vector_count_block {
            let old_medoid_index = self.block_vector_buffer[i].medoid_index;
            let new_medoid_index = find_nearest_medoid(&block_vector_buffer[i], &centroid_map);
            
            if old_medoid_index != new_medoid_index {
                // 重新分配
                self.block_vector_buffer[i].medoid_index = new_medoid_index;
                // 更新聚类计数
            }
        }

        // 4.3 更新聚类中心为 Medoid (距离其他点总和最小的点)
        for (medoid_index, centroid) in centroid_map.iter_mut() {
            let medoid_index_new = find_medoid(&centroid.vectors);
            centroid.medoid_index_new = medoid_index_new;
            centroid.has_changed = centroid.medoid_index != medoid_index_new;
        }

        // 4.4 检查收敛 (如果没有改变则停止)
        let changed_count = centroid_map.values().filter(|c| c.has_changed).count();
        if changed_count == 0 {
            break;
        }
    }

    medoids_vec
}

// IVF 聚类头结构
pub(crate) struct ClusterHeader {
    pub start_index: u32,      // 聚类起始索引
    pub child_count: u32,      // 聚类中向量数量
}

// IVF 搜索时的 Nprobe 模式
pub enum AnnMode {
    Nprobe(usize),  // 搜索 Nprobe 个最近的聚类
    Nscan(usize),   // 扫描 Nscan 个向量
}
```

---

## 6. 文档存储

### 6.1 文档压缩存储

```rust
// 位置: seekstorm/src/doc_store.rs
pub(crate) fn store_document(&mut self, docid_local: usize, mut document: Document) {
    // 1. 过滤存储字段
    let keys: Vec<String> = document.keys().cloned().collect();
    for key in keys.into_iter() {
        if !self.schema_map.contains_key(&key) || 
           !self.schema_map.get(&key).unwrap().store {
            document.shift_remove(&key);
        }
    }

    // 2. 序列化为 JSON
    let serialized = serde_json::to_vec(&document).unwrap();

    // 3. 压缩
    let compressed = match self.meta.document_compression {
        DocumentCompression::None => serialized,
        DocumentCompression::Snappy => {
            snap::raw::Encoder::new().compress_vec(&serialized).unwrap()
        }
        DocumentCompression::Lz4 => {
            lz4_flex::compress_prepend_size(&serialized)
        }
        DocumentCompression::Zstd => {
            zstd::encode_all(serialized.as_slice(), 1).unwrap()
        }
    };

    // 4. 写入文档存储
    self.write_document(docid_local, &compressed);
}
```

### 6.2 文档存储结构

```
docstore.bin:
┌─────────────────────────────────────────────────────────────┐
│ Document 0 (压缩后)                                          │
│ - offset: 0                                                 │
│ - length: len_0                                             │
├─────────────────────────────────────────────────────────────┤
│ Document 1 (压缩后)                                          │
│ - offset: len_0                                             │
│ - length: len_1                                             │
├─────────────────────────────────────────────────────────────┤
│ ...                                                          │
├─────────────────────────────────────────────────────────────┤
│ Document N (压缩后)                                          │
│ - offset: len_0 + ... + len_{N-1}                           │
│ - length: len_N                                             │
└─────────────────────────────────────────────────────────────┘

docstore_index.bin:
┌─────────────────────────────────────────────────────────────┐
│ DocID 0: offset_0 (8 bytes)                                 │
│ DocID 1: offset_1 (8 bytes)                                 │
│ ...                                                          │
│ DocID N: offset_N (8 bytes)                                 │
└─────────────────────────────────────────────────────────────┘
```

---

## 7. 索引数据结构

### 7.1 整体结构

```
Index (全局索引)
│
├── Shard 0 (分片 0)
│   ├── segments_level0 (Level 0 段 - 可变)
│   │   └── PostingListObject0 (每个词项一个)
│   │       ├── pointer_first: u32
│   │       ├── pointer_last: u32
│   │       ├── posting_count: u16
│   │       ├── position_count: usize
│   │       ├── ngram_type: NgramType
│   │       └── ...
│   │
│   ├── segments_index (Level 1+ 段 - 不可变)
│   │   └── PostingListObjectIndex
│   │       ├── byte_array_blocks: Vec<Vec<u8>>
│   │       ├── byte_array_blocks_pointer: Vec<(usize, usize, u32)>
│   │       └── segment: HashMap<u64, PostingListObjectIndexEntry>
│   │
│   ├── vector_clusters (IVF 向量聚类)
│   │   └── ClusterHeader
│   │       ├── start_index: u32   // 聚类起始索引
│   │       ├── child_count: u32    // 聚类中向量数量
│   │       └── medoid_index: usize // 聚类中心索引
│   │
│   ├── docstore (文档存储)
│   ├── facets (分面)
│   └── ...
│
├── Shard 1 (分片 1)
│   └── ...
│
└── Shard N (分片 N)
```

### 7.2 Posting List 结构

```
Posting Buffer (Level 0 内存缓冲区):
┌─────────────────────────────────────────────────────────────┐
│ DocID Term 1 Posting:                                        │
│ │ ┌───────────────────────────────────────────────────────┐ │
│ │ │ prev_pointer: u32 (4 bytes)                          │ │
│ │ │ docid_lsb: u16 (2 bytes)                             │ │
│ │ │ position_size: u16 (2 bytes, 带嵌入标志)             │ │
│ │ │ [embedded_positions | non_embedded_pointer]          │ │
│ │ └───────────────────────────────────────────────────────┘ │
├─────────────────────────────────────────────────────────────┤
│ DocID Term 1 Posting (下一个文档):                            │
│ │ ...                                                        │
├─────────────────────────────────────────────────────────────┤
│ DocID Term 2 Posting:                                        │
│ │ ...                                                        │
└─────────────────────────────────────────────────────────────┘
```

### 7.3 N-gram 结构

```rust
pub struct TermObject {
    pub term: String,
    pub term_hash: u64,
    pub key_hash: u128,
    pub key0: u8,
    pub ngram_type: NgramType,
    
    // 单词 n-gram
    pub term_ngram_0: Option<String>,
    pub term_ngram_1: Option<String>,
    pub term_ngram_2: Option<String>,
    
    // n-gram 位置数量
    pub field_vec_ngram1: Vec<(usize, u32)>,
    pub field_vec_ngram2: Vec<(usize, u32)>,
    pub field_vec_ngram3: Vec<(usize, u32)>,
    
    // 位置列表
    pub field_positions_vec: Vec<Vec<u16>>,
}

pub enum NgramType {
    SingleTerm = 0,          // 单词
    NgramFF = 1,             // 频繁-频繁
    NgramFR = 2,             // 频繁-稀有
    NgramRF = 3,             // 稀有-频繁
    NgramFFF = 4,            // 频繁-频繁-频繁
    NgramRFF = 5,            // 稀有-频繁-频繁
    NgramFRF = 6,            // 频繁-稀有-频繁
}
```

---

## 8. 压缩与编码

### 8.1 位置编码

```
嵌入模式 (embed_flag = true):
┌─────────────────────────────────────────────────────────────┐
│ 单字段:                                                       │
│   [position_0 | position_1 | ... | count_bits]             │
│                                                               │
│ 多字段 (最长字段):                                             │
│   [position_0 | position_1 | ... | count_bits]             │
│                                                               │
│ 多字段 (多字段):                                               │
│   [field_id | position_0 | position_1 | ...]               │
└─────────────────────────────────────────────────────────────┘

非嵌入模式 (embed_flag = false):
┌─────────────────────────────────────────────────────────────┐
│ 字段向量元数据:                                               │
│   [field_0_count | field_1_count | ...]                   │
│                                                               │
│ 位置数据 (VLE 压缩):                                          │
│   [delta_0 | delta_1 | delta_2 | ...]                      │
│                                                               │
│ VLE 编码:                                                     │
│   < 128:   0xxxxxxx (1 byte)                                │
│   < 16384: 1xxxxxxx 0xxxxxxx (2 bytes)                     │
│   < 2M:    1xxxxxxx 1xxxxxxx 0xxxxxxx (3 bytes)           │
└─────────────────────────────────────────────────────────────┘
```

### 8.2 文档 ID 压缩

```
Block 0 (DocID 0-65535):
  - 使用 16 位存储文档 ID
  - 使用 Array/Bitmap/RLE 编码

Array 编码:
  [docid_0, docid_1, ...]  // 每个 2 字节

Bitmap 编码:
  [bitmap_0, bitmap_1, ...]  // 每个 8192 字节 (65,536 bits)

RLE (Run Length Encoding):
  [run_length_0, run_length_1, ...]  // 变长
```

### 8.3 文档长度压缩

```rust
// 文档长度使用查表压缩
pub const DOCUMENT_LENGTH_COMPRESSION: [u32; 256] = [
    1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
    17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32,
    34, 36, 38, 40, 42, 44, 46, 48, 50, 52, 54, 56, 58, 60, 62, 64,
    68, 72, 76, 80, 84, 88, 92, 96, 100, 104, 108, 112, 116, 120, 124, 128,
    // ... 渐进增长
];

// 压缩: 1 字节 → 还原: 1-65535 之间的数值
let compressed = int_to_byte4(actual_length);
let normalized = DOCUMENT_LENGTH_COMPRESSION[compressed as usize];
```

---

## 9. 关键优化点

### 9.1 并发控制

```rust
// 信号量控制并发索引
pub struct Shard {
    pub(crate) semaphore: Arc<Semaphore>,  // 控制并发数
    // ...
}

// 获取许可后再执行索引
let permit = semaphore.acquire_owned().await.unwrap();
// ... 执行索引
drop(permit);  // 释放许可
```

### 9.2 内存缓冲区

```rust
// Posting Buffer 预分配
pub struct Shard {
    postings_buffer: Vec<u8>,              // 预分配的缓冲区
    postings_buffer_pointer: usize,         // 当前写入位置
    // ...
}

// 自动扩容
if self.postings_buffer_pointer > self.postings_buffer.len() - (POSTING_BUFFER_SIZE >> 4) {
    self.postings_buffer.resize(
        self.postings_buffer.len() + (POSTING_BUFFER_SIZE >> 2), 
        0
    );
}
```

### 9.3 位置嵌入

```rust
// 小位置列表直接嵌入到 Posting 中，减少指针跳转
let embed_flag = positions_sum <= 4 && 
    positions_values_are_small_enough();

if embed_flag {
    // 位置直接存储在 Posting 元数据中
    data |= (position_0 << position_bits) | (position_1 << ...) | ...;
} else {
    // 位置存储到独立的压缩缓冲区
    compress_positions(&field_positions, &mut positions_compressed);
}
```

### 9.4 自动提交

```rust
// 每 65,536 文档自动提交
let do_commit = shard_mut.block_id != docid_local >> 16;
if do_commit {
    // 提交全文索引
    shard_mut.commit_lexical_shard(docid_local).await;
    
    // 提交向量索引
    if shard_mut.is_vector_indexing {
        shard_mut.commit_vector_shard().await;
    }
    
    shard_mut.block_id = docid_local >> 16;
}
```

---

## 10. 文件位置参考

| 功能 | 文件路径 |
|------|----------|
| HTTP 请求处理 | `seekstorm_server/src/http_server.rs` |
| API 端点实现 | `seekstorm_server/src/api_endpoints.rs` |
| 索引主逻辑 | `seekstorm/src/index.rs` |
| Posting 构建 | `seekstorm/src/index_posting.rs` |
| 分词器 | `seekstorm/src/tokenizer.rs` |
| 向量推理 | `seekstorm/src/vector.rs` |
| 向量相似度 | `seekstorm/src/vector_similarity.rs` |
| 文档存储 | `seekstorm/src/doc_store.rs` |
| 压缩逻辑 | `seekstorm/src/compress_postinglist.rs` |
| Commit 逻辑 | `seekstorm/src/commit.rs` |

---

## 附录：完整时序图

```
Client                          HTTP Server       Index Runtime        Shard
  │                                  │                   │            │
  │  POST /api/v1/index/0/doc        │                   │            │
  │  { "title": "...", "body": ... } │                   │            │
  │─────────────────────────────────>│                   │            │
  │                                  │ 验证 API Key     │            │
  │                                  │ 获取 Index       │            │
  │                                  │──────────────────>│            │
  │                                  │ index_document   │            │
  │                                  │───────────────────┼───────────>│
  │                                  │                   │            │
  │                                  │                   │ 分配 DocID │
  │                                  │                   │  选择 Shard│
  │                                  │                   │            │
  │                                  │                   │ 分词处理   │
  │                                  │                   │───────────>│
  │                                  │                   │<───────────│
  │                                  │                   │            │
  │                                  │                   │ 构建 N-gram│
  │                                  │                   │            │
  │                                  │                   │ 向量推理   │
  │                                  │                   │            │
  │                                  │                   │ 同义词扩展 │
  │                                  │                   │            │
  │                                  │                   │ 压缩位置   │
  │                                  │                   │            │
  │                                  │                   │ 写入 Buffer│
  │                                  │                   │            │
  │                                  │                   │ 存储文档   │
  │                                  │                   │            │
  │                                  │                   │ 检查 Commit│
  │                                  │                   │            │
  │                                  │  200 OK           │            │
  │<─────────────────────────────────│                   │            │
  │                                  │                   │            │
  │     (异步完成后续工作)            │                   │            │
  │                                  │                   │ 释放信号量 │
  │                                  │                   │            │
  │                                  │  如需: Commit     │            │
  │                                  │                   │───────────>│
  │                                  │                   │  持久化磁盘 │
```

---

**总结**: SeekStorm 的文档索引流程是一个高度优化的管道系统，通过多阶段处理、智能压缩、并发控制和自动提交等机制，实现了高性能的全文和向量搜索索引功能。