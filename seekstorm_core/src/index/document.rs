//! Schemaless 文档（Phase 2 §2.2）。
//!
//! [`SchemalessDoc`] 是写入 [`Index`] 的最小单位。字段以 `IndexMap<String, serde_json::Value>`
//! 保存，保留插入顺序便于稳定序列化与回填。序列化采用 MessagePack 风格的自描述格式：
//! 头部 `[magic:4][field_count:u32 BE]`，之后每个字段 `[name_len:u32][name:bytes][value_json_len:u32][value_json:bytes]`。
//!
//! 选择 JSON 而非 bincode / postcard：Phase 2 仅作 PoC，serde_json 已在依赖中，
//! 字段值语义透明，便于跨语言/调试。后续若需更紧凑可替换为 bincode。

use anyhow::Result;
use indexmap::IndexMap;
use serde_json::Value;

/// 文档魔数：`"SKMD"`（SeekStorm Meta Doc）。
const DOC_MAGIC: &[u8; 4] = b"SKMD";

/// Schemaless 文档。
#[derive(Clone, Debug, Default)]
pub struct SchemalessDoc {
    pub fields: IndexMap<String, Value>,
}

impl SchemalessDoc {
    /// 从 JSON 字符串构造。顶层必须是对象。
    pub fn from_json(json: &str) -> Result<Self> {
        let value: Value = serde_json::from_str(json)?;
        Self::from_value(&value)
    }

    /// 从已解析的 JSON Value 构造。
    pub fn from_value(value: &Value) -> Result<Self> {
        match value {
            Value::Object(map) => {
                let mut fields = IndexMap::with_capacity(map.len());
                for (k, v) in map {
                    fields.insert(k.clone(), v.clone());
                }
                Ok(Self { fields })
            }
            _ => anyhow::bail!("SchemalessDoc expects a JSON object, got {:?}", value),
        }
    }

    /// 从 JSON Value 构造（消费）。
    pub fn from_value_owned(value: Value) -> Result<Self> {
        match value {
            Value::Object(map) => {
                let mut fields = IndexMap::with_capacity(map.len());
                for (k, v) in map {
                    fields.insert(k, v);
                }
                Ok(Self { fields })
            }
            _ => anyhow::bail!("SchemalessDoc expects a JSON object, got {:?}", value),
        }
    }

    /// 序列化为字节。
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::with_capacity(8 + self.fields.len() * 32);
        buf.extend_from_slice(DOC_MAGIC);
        buf.extend_from_slice(&(self.fields.len() as u32).to_be_bytes());
        for (name, value) in &self.fields {
            let name_bytes = name.as_bytes();
            buf.extend_from_slice(&(name_bytes.len() as u32).to_be_bytes());
            buf.extend_from_slice(name_bytes);
            let value_json = serde_json::to_vec(value)?;
            buf.extend_from_slice(&(value_json.len() as u32).to_be_bytes());
            buf.extend_from_slice(&value_json);
        }
        Ok(buf)
    }

    /// 从字节反序列化。
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < 8 {
            anyhow::bail!("SchemalessDoc: too short ({} bytes)", bytes.len());
        }
        if &bytes[0..4] != DOC_MAGIC {
            anyhow::bail!("SchemalessDoc: bad magic {:?}", &bytes[0..4]);
        }
        let field_count = u32::from_be_bytes(bytes[4..8].try_into().unwrap()) as usize;
        let mut fields = IndexMap::with_capacity(field_count);
        let mut cursor = 8usize;
        for _ in 0..field_count {
            if cursor + 4 > bytes.len() {
                anyhow::bail!("SchemalessDoc: truncated name_len at field {}", fields.len());
            }
            let name_len = u32::from_be_bytes(bytes[cursor..cursor + 4].try_into().unwrap()) as usize;
            cursor += 4;
            if cursor + name_len > bytes.len() {
                anyhow::bail!("SchemalessDoc: truncated name");
            }
            let name = std::str::from_utf8(&bytes[cursor..cursor + name_len])?.to_string();
            cursor += name_len;
            if cursor + 4 > bytes.len() {
                anyhow::bail!("SchemalessDoc: truncated value_len at field {}", name);
            }
            let value_len = u32::from_be_bytes(bytes[cursor..cursor + 4].try_into().unwrap()) as usize;
            cursor += 4;
            if cursor + value_len > bytes.len() {
                anyhow::bail!("SchemalessDoc: truncated value for field {}", name);
            }
            let value: Value = serde_json::from_slice(&bytes[cursor..cursor + value_len])?;
            cursor += value_len;
            fields.insert(name, value);
        }
        Ok(Self { fields })
    }

    /// 字段迭代。
    pub fn fields(&self) -> impl Iterator<Item = (&String, &Value)> {
        self.fields.iter()
    }

    /// 取首个向量字段（按插入顺序）。返回 `(name, Vec<f32>)`。
    /// 仅识别 `[f64; N]` 或 `[i64; N]` 形态的纯数字数组。
    pub fn get_vector_field(&self) -> Option<(String, Vec<f32>)> {
        for (name, value) in &self.fields {
            if let Value::Array(arr) = value {
                if !arr.is_empty() && arr.iter().all(|v| v.is_number()) {
                    let vec: Vec<f32> = arr
                        .iter()
                        .map(|v| {
                            v.as_f64()
                                .map(|f| f as f32)
                                .unwrap_or_else(|| v.as_i64().map(|i| i as f32).unwrap_or(0.0))
                        })
                        .collect();
                    return Some((name.clone(), vec));
                }
            }
        }
        None
    }

    /// 按字段名取值。
    pub fn get(&self, name: &str) -> Option<&Value> {
        self.fields.get(name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_json_basic() {
        let doc = SchemalessDoc::from_json(r#"{"title":"hello","count":42}"#).unwrap();
        assert_eq!(doc.fields.len(), 2);
        assert_eq!(doc.get("title").unwrap(), &Value::String("hello".into()));
        assert_eq!(doc.get("count").unwrap(), &serde_json::json!(42));
    }

    #[test]
    fn test_to_from_bytes_roundtrip() {
        let doc = SchemalessDoc::from_json(r#"{"title":"hello","count":42,"tags":["a","b"]}"#).unwrap();
        let bytes = doc.to_bytes().unwrap();
        let restored = SchemalessDoc::from_bytes(&bytes).unwrap();
        assert_eq!(restored.fields.len(), 3);
        assert_eq!(restored.get("title").unwrap(), &Value::String("hello".into()));
        assert_eq!(restored.get("count").unwrap(), &serde_json::json!(42));
    }

    #[test]
    fn test_get_vector_field() {
        let doc = SchemalessDoc::from_json(r#"{"title":"x","vec":[1.0,2.0,3.0]}"#).unwrap();
        let (name, vec) = doc.get_vector_field().unwrap();
        assert_eq!(name, "vec");
        assert_eq!(vec, vec![1.0f32, 2.0, 3.0]);
    }

    #[test]
    fn test_get_vector_field_integer() {
        // 整数数组也应被识别（推断为 Vector）。
        let doc = SchemalessDoc::from_json(r#"{"v":[1,2,3]}"#).unwrap();
        let (_name, vec) = doc.get_vector_field().unwrap();
        assert_eq!(vec, vec![1.0f32, 2.0, 3.0]);
    }

    #[test]
    fn test_no_vector_field() {
        let doc = SchemalessDoc::from_json(r#"{"title":"x"}"#).unwrap();
        assert!(doc.get_vector_field().is_none());
    }

    #[test]
    fn test_bad_magic_rejected() {
        let bad = b"XXXX0000";
        assert!(SchemalessDoc::from_bytes(bad).is_err());
    }
}
