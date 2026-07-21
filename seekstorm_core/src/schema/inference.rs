//! 类型推断：把 JSON 值映射到 [`FieldType`]。
//!
//! 规则（与 V4_PHASED_IMPLEMENTATION.md §2.1 一致）：
//! - 字符串：若能解析为 RFC 3339 日期时间则为 `DateTime`，否则 `Text`。
//! - 数字：i64 → `I64`，u64 → `U64`，否则 `F64`。
//! - 布尔：`Bool`。
//! - 数组：若元素全为数字且长度 > 1，视为 `Vector(dim, Cosine)`；
//!   否则取首个元素的类型作为 `Array(Box<...>)`，空数组退化为 `Array(Box::Text>)`。
//! - 其它（null / 对象）：`Bytes`（Phase 2 不支持嵌套对象，落到 Bytes 即可）。

use serde_json::Value;

use super::field::{FieldType, VectorSimilarity};

/// 推断字段类型。
pub fn infer_field_type(value: &Value) -> FieldType {
    match value {
        Value::String(s) => {
            if chrono::DateTime::parse_from_rfc3339(s).is_ok() {
                FieldType::DateTime
            } else {
                FieldType::Text
            }
        }
        Value::Number(n) => {
            if n.is_i64() {
                FieldType::I64
            } else if n.is_u64() {
                FieldType::U64
            } else {
                FieldType::F64
            }
        }
        Value::Bool(_) => FieldType::Bool,
        Value::Array(arr) => {
            if !arr.is_empty() && arr.iter().all(|v| v.is_number()) {
                // 全数字数组视为向量。维度 = 数组长度，相似度默认 Cosine。
                FieldType::Vector(arr.len(), VectorSimilarity::Cosine)
            } else if !arr.is_empty() {
                FieldType::Array(Box::new(infer_field_type(&arr[0])))
            } else {
                // 空数组：无法推断元素类型，退化为 Array<Text>。
                FieldType::Array(Box::new(FieldType::Text))
            }
        }
        // null / Object / 其它：Phase 2 不支持嵌套对象，统一记为 Bytes。
        _ => FieldType::Bytes,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_infer_text() {
        let v = Value::String("hello".into());
        assert!(matches!(infer_field_type(&v), FieldType::Text));
    }

    #[test]
    fn test_infer_datetime() {
        let v = Value::String("2026-07-21T10:30:00Z".into());
        assert!(matches!(infer_field_type(&v), FieldType::DateTime));
    }

    #[test]
    fn test_infer_numbers() {
        assert!(matches!(
            infer_field_type(&serde_json::json!(42)),
            FieldType::I64
        ));
        assert!(matches!(
            infer_field_type(&serde_json::json!(-1)),
            FieldType::I64
        ));
        assert!(matches!(
            infer_field_type(&serde_json::json!(3.14)),
            FieldType::F64
        ));
    }

    #[test]
    fn test_infer_vector() {
        let v = serde_json::json!([1.0, 2.0, 3.0]);
        match infer_field_type(&v) {
            FieldType::Vector(dim, sim) => {
                assert_eq!(dim, 3);
                assert!(matches!(sim, VectorSimilarity::Cosine));
            }
            other => panic!("expected Vector, got {:?}", other),
        }
    }

    #[test]
    fn test_infer_array() {
        let v = serde_json::json!(["a", "b"]);
        match infer_field_type(&v) {
            FieldType::Array(inner) => assert!(matches!(*inner, FieldType::Text)),
            other => panic!("expected Array, got {:?}", other),
        }
    }
}
