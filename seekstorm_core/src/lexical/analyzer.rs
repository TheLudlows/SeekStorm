//! 分词器构造：按 `TokenizerType` 配置生成 tantivy `TextAnalyzer`。
//!
//! 仅使用 tantivy 的分词能力，不创建 tantivy `Index` / `IndexWriter` / `IndexReader`。

use anyhow::Result;
use tantivy::tokenizer::{LowerCaser, NgramTokenizer, SimpleTokenizer, TextAnalyzer};

use crate::schema::TokenizerType;

/// 按 `TokenizerType` 构造分词器。
pub fn build_analyzer(tt: &TokenizerType) -> Result<TextAnalyzer> {
    match tt {
        TokenizerType::Default => {
            // 默认：SimpleTokenizer + LowerCaser
            Ok(TextAnalyzer::builder(SimpleTokenizer::default())
                .filter(LowerCaser)
                .build())
        }
        TokenizerType::Raw => {
            // 原文：不分词，整个字符串作为一个 token
            Ok(TextAnalyzer::builder(SimpleTokenizer::default()).build())
        }
        TokenizerType::Ngram { n } => {
            // N-gram：n-gram 分词 + LowerCaser
            let ngram = NgramTokenizer::new(*n as usize, *n as usize, false)
                .map_err(|e| anyhow::anyhow!("NgramTokenizer: {}", e))?;
            Ok(TextAnalyzer::builder(ngram).filter(LowerCaser).build())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tantivy::tokenizer::TokenStream;

    #[test]
    fn test_default_analyzer() {
        let mut analyzer = build_analyzer(&TokenizerType::Default).unwrap();
        let mut stream = analyzer.token_stream("Hello World 测试");
        let mut tokens = Vec::new();
        while let Some(token) = stream.next() {
            tokens.push(token.text.clone());
        }
        assert_eq!(tokens, vec!["hello", "world", "测试"]);
    }

    #[test]
    fn test_ngram_analyzer() {
        let mut analyzer = build_analyzer(&TokenizerType::Ngram { n: 2 }).unwrap();
        let mut stream = analyzer.token_stream("hello");
        let mut tokens = Vec::new();
        while let Some(token) = stream.next() {
            tokens.push(token.text.clone());
        }
        assert_eq!(tokens, vec!["he", "el", "ll", "lo"]);
    }
}
