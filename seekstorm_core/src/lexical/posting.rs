//! 词法索引 posting 结构与二进制编解码。
//!
//! 每个 posting 表示一个 (field_id, term, doc_id) 三元组的倒排项，
//! 存储该 term 在该 doc 中的出现位置（positions）。

use anyhow::Result;

/// Posting 魔数："SKLP"（SeekStorm Lexical Posting）。
const POSTING_MAGIC: &[u8; 4] = b"SKLP";

/// 倒排项：(field_id, term, doc_id) 的 posting。
#[derive(Clone, Debug)]
pub struct Posting {
    pub field_id: u32,
    pub term: String,
    pub doc_id: u64,
    pub term_freq: u32,
    pub positions: Vec<u32>,
}

impl Posting {
    /// 构造 posting。
    pub fn new(field_id: u32, term: String, doc_id: u64, positions: Vec<u32>) -> Self {
        let term_freq = positions.len() as u32;
        Self {
            field_id,
            term,
            doc_id,
            term_freq,
            positions,
        }
    }

    /// 序列化为字节。
    ///
    /// 格式：
    /// ```text
    /// [magic: 4 bytes "SKLP"]
    /// [field_id: u32 BE]
    /// [doc_id: u64 BE]
    /// [term_freq: u32 BE]
    /// [term_len: u32 BE]
    /// [term: UTF-8 bytes]
    /// [pos_count: u32 BE]
    /// [positions: u32 BE * pos_count]
    /// ```
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::with_capacity(4 + 4 + 8 + 4 + 4 + self.term.len() + 4 + self.positions.len() * 4);
        buf.extend_from_slice(POSTING_MAGIC);
        buf.extend_from_slice(&self.field_id.to_be_bytes());
        buf.extend_from_slice(&self.doc_id.to_be_bytes());
        buf.extend_from_slice(&self.term_freq.to_be_bytes());

        let term_bytes = self.term.as_bytes();
        buf.extend_from_slice(&(term_bytes.len() as u32).to_be_bytes());
        buf.extend_from_slice(term_bytes);

        buf.extend_from_slice(&(self.positions.len() as u32).to_be_bytes());
        for pos in &self.positions {
            buf.extend_from_slice(&pos.to_be_bytes());
        }

        Ok(buf)
    }

    /// 从字节反序列化。
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < 4 + 4 + 8 + 4 + 4 {
            anyhow::bail!("Posting decode: too short ({} bytes)", bytes.len());
        }
        if &bytes[0..4] != POSTING_MAGIC {
            anyhow::bail!("Posting decode: bad magic {:?}", &bytes[0..4]);
        }

        let mut cursor = 4usize;

        let field_id = u32::from_be_bytes(bytes[cursor..cursor + 4].try_into()?);
        cursor += 4;

        let doc_id = u64::from_be_bytes(bytes[cursor..cursor + 8].try_into()?);
        cursor += 8;

        let term_freq = u32::from_be_bytes(bytes[cursor..cursor + 4].try_into()?);
        cursor += 4;

        if cursor + 4 > bytes.len() {
            anyhow::bail!("Posting decode: truncated term_len");
        }
        let term_len = u32::from_be_bytes(bytes[cursor..cursor + 4].try_into()?) as usize;
        cursor += 4;

        if cursor + term_len > bytes.len() {
            anyhow::bail!("Posting decode: truncated term");
        }
        let term = std::str::from_utf8(&bytes[cursor..cursor + term_len])?.to_string();
        cursor += term_len;

        if cursor + 4 > bytes.len() {
            anyhow::bail!("Posting decode: truncated pos_count");
        }
        let pos_count = u32::from_be_bytes(bytes[cursor..cursor + 4].try_into()?) as usize;
        cursor += 4;

        let mut positions = Vec::with_capacity(pos_count);
        for _ in 0..pos_count {
            if cursor + 4 > bytes.len() {
                anyhow::bail!("Posting decode: truncated positions");
            }
            let pos = u32::from_be_bytes(bytes[cursor..cursor + 4].try_into()?);
            positions.push(pos);
            cursor += 4;
        }

        Ok(Self {
            field_id,
            term,
            doc_id,
            term_freq,
            positions,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_posting_roundtrip() {
        let posting = Posting::new(1, "hello".to_string(), 42, vec![0, 5, 10]);
        let bytes = posting.encode().unwrap();
        let restored = Posting::decode(&bytes).unwrap();

        assert_eq!(restored.field_id, 1);
        assert_eq!(restored.term, "hello");
        assert_eq!(restored.doc_id, 42);
        assert_eq!(restored.term_freq, 3);
        assert_eq!(restored.positions, vec![0, 5, 10]);
    }

    #[test]
    fn test_posting_empty_positions() {
        let posting = Posting::new(2, "world".to_string(), 100, vec![]);
        let bytes = posting.encode().unwrap();
        let restored = Posting::decode(&bytes).unwrap();

        assert_eq!(restored.term_freq, 0);
        assert!(restored.positions.is_empty());
    }

    #[test]
    fn test_posting_bad_magic() {
        let bad = b"XXXX0000000000000000";
        assert!(Posting::decode(bad).is_err());
    }
}
