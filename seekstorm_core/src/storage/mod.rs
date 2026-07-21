//! 存储子系统：简易 LSM 引擎及其依赖。
//!
//! 模块层次：
//! - [`io_backend`]：跨平台异步文件 I/O 抽象。
//! - [`wal`]：预写日志（Write-Ahead Log）。
//! - [`lsm`]：`LsmEngine` 门面、`MemTable`、`LsmKey`/`LsmValue`、`LsmConfig`。
//! - [`sstable`]：磁盘上的有序字符串表（Sorted-String Table）。
//! - [`manifest`]：索引元数据清单。

pub mod io_backend;
pub mod lsm;
pub mod manifest;
pub mod sstable;
pub mod wal;
