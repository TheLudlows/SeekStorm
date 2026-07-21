//! 简单词法查询解析器。
//!
//! 支持语法：
//! - `field:term` → 在指定字段搜索 term
//! - `term` → 在所有索引文本字段搜索 term
//! - 空格分隔 → AND 语义（所有 term 都必须匹配）
//!
//! 示例：
//! - `"hello"` → 搜索所有 text 字段
//! - `"title:hello"` → 搜索 title 字段
//! - `"title:hello world"` → title 包含 hello 且任意字段包含 world

use anyhow::Result;

/// 词法查询。
#[derive(Clone, Debug)]
pub struct LexicalQuery {
    /// 查询项列表（AND 语义）。
    pub terms: Vec<TermQuery>,
}

/// 单个 term 查询。
#[derive(Clone, Debug)]
pub struct TermQuery {
    /// 字段名（若为 None 表示搜索所有 text 字段）。
    pub field: Option<String>,
    /// 查询词。
    pub text: String,
}

/// 解析查询字符串。
pub fn parse_query(query: &str) -> Result<LexicalQuery> {
    let mut terms = Vec::new();

    for part in query.split_whitespace() {
        if part.contains(':') {
            // field:term 格式
            let colon_pos = part.find(':').unwrap();
            let field = &part[..colon_pos];
            let text = &part[colon_pos + 1..];
            if field.is_empty() || text.is_empty() {
                anyhow::bail!("Invalid query term: '{}'", part);
            }
            terms.push(TermQuery {
                field: Some(field.to_string()),
                text: text.to_string(),
            });
        } else {
            // 裸 term：搜索所有 text 字段
            terms.push(TermQuery {
                field: None,
                text: part.to_string(),
            });
        }
    }

    if terms.is_empty() {
        anyhow::bail!("Empty query");
    }

    Ok(LexicalQuery { terms })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_term() {
        let q = parse_query("hello").unwrap();
        assert_eq!(q.terms.len(), 1);
        assert_eq!(q.terms[0].field, None);
        assert_eq!(q.terms[0].text, "hello");
    }

    #[test]
    fn test_parse_field_term() {
        let q = parse_query("title:hello").unwrap();
        assert_eq!(q.terms.len(), 1);
        assert_eq!(q.terms[0].field, Some("title".to_string()));
        assert_eq!(q.terms[0].text, "hello");
    }

    #[test]
    fn test_parse_multiple_terms() {
        let q = parse_query("title:hello world").unwrap();
        assert_eq!(q.terms.len(), 2);
        assert_eq!(q.terms[0].field, Some("title".to_string()));
        assert_eq!(q.terms[0].text, "hello");
        assert_eq!(q.terms[1].field, None);
        assert_eq!(q.terms[1].text, "world");
    }

    #[test]
    fn test_parse_empty_query() {
        assert!(parse_query("").is_err());
    }

    #[test]
    fn test_parse_invalid_field_term() {
        assert!(parse_query("title:").is_err());
        assert!(parse_query(":hello").is_err());
    }
}
